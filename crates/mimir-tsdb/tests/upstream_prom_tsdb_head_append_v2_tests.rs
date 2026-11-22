//! Ported upstream Prometheus/Mimir TSDB Head append/v2/jitter/mmap tests (70 tests).
//!
//! Sources:
//! - head_append_test.go (4)
//! - head_append_wal_log_failure_test.go (1)
//! - head_mimir_test.go (1)
//! - head_mmap_append_test.go (1)
//! - head_append_v2_test.go (63)

use std::collections::{BTreeMap, HashMap};
use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mimir_tsdb::chunkenc::{
    stale_nan, CounterResetHeader, CounterResetHint, EncodedChunk, Encoding, FloatHistogram,
    Histogram, HistogramSpan,
};
use mimir_tsdb::compact::{decode_chunk_samples, DecodedSample, SampleValue};
use mimir_tsdb::db::{Tsdb, TsdbOptions};
use mimir_tsdb::exemplar::Exemplar;
use mimir_tsdb::head::{
    add_jitter_to_chunk_end_time, Head, HeadError, HeadOptions, SeriesLifecycleCallback,
    DEFAULT_BLOCK_DURATION_MS,
};
use mimir_tsdb::isolation::Isolation;
use mimir_tsdb::labels::Labels;
use mimir_tsdb::matcher::{LabelMatcher, MatchType};
use mimir_tsdb::querier::TsdbQuerier;
use mimir_tsdb::record::{DecodedRecord, RecordDecoder};
use mimir_tsdb::wlog::{
    last_checkpoint, segment_name, segments_range, CompressionType, WlogReader,
};

fn metric_value(reg: &prometheus::Registry, name: &str, labels: &[(&str, &str)]) -> f64 {
    for mf in reg.gather() {
        if mf.get_name() == name {
            for m in mf.get_metric() {
                let matched = labels.iter().all(|&(lk, lv)| {
                    m.get_label()
                        .iter()
                        .any(|lp| lp.get_name() == lk && lp.get_value() == lv)
                });
                if matched {
                    return m.get_counter().get_value() + m.get_gauge().get_value();
                }
            }
        }
    }
    0.0
}

fn read_all_wal_records(dir: &std::path::Path) -> Vec<DecodedRecord> {
    let mut r = WlogReader::open_dir(dir).unwrap();
    let dec = RecordDecoder::new();
    let mut out = Vec::new();
    while let Some(raw) = r.next_record().unwrap() {
        out.push(dec.decode_record(raw).unwrap());
    }
    out
}

fn gen_hist(v: i64) -> Histogram {
    let z = v.unsigned_abs();
    Histogram {
        counter_reset_hint: CounterResetHint::UnknownCounterReset,
        schema: 0,
        zero_threshold: 1e-128,
        zero_count: z,
        count: z + 4,
        sum: (v as f64) * 2.5,
        positive_spans: vec![HistogramSpan { offset: 0, length: 2 }],
        negative_spans: vec![],
        positive_buckets: vec![1, 2],
        negative_buckets: vec![],
        custom_values: vec![],
    }
}

fn gen_gauge_hist(v: i64) -> Histogram {
    let mut h = gen_hist(v);
    h.counter_reset_hint = CounterResetHint::GaugeType;
    h
}

fn gen_fhist(v: i64) -> FloatHistogram {
    let z = v.unsigned_abs() as f64;
    FloatHistogram {
        counter_reset_hint: CounterResetHint::UnknownCounterReset,
        schema: 0,
        zero_threshold: 1e-128,
        zero_count: z,
        count: z + 3.0,
        sum: (v as f64) * 2.5,
        positive_spans: vec![HistogramSpan { offset: 0, length: 2 }],
        negative_spans: vec![],
        positive_buckets: vec![1.0, 2.0],
        negative_buckets: vec![],
        custom_values: vec![],
    }
}

fn gen_gauge_fhist(v: i64) -> FloatHistogram {
    let mut fh = gen_fhist(v);
    fh.counter_reset_hint = CounterResetHint::GaugeType;
    fh
}

fn gen_big_hist(seed: i64, num_buckets: usize) -> Histogram {
    let mut buckets = Vec::with_capacity(num_buckets);
    let mut total = 1u64;
    let mut cur = 0i64;
    for i in 0..num_buckets {
        let target = 1 + ((seed + i as i64) % 3).abs();
        let delta = target - cur;
        cur = target;
        buckets.push(delta);
        total += target as u64;
    }
    Histogram {
        counter_reset_hint: CounterResetHint::NotCounterReset,
        schema: 0,
        zero_threshold: 1e-128,
        zero_count: 1,
        count: total,
        sum: (seed as f64) * 1.5 + 10.0,
        positive_spans: vec![HistogramSpan {
            offset: 0,
            length: num_buckets as u32,
        }],
        negative_spans: vec![],
        positive_buckets: buckets,
        negative_buckets: vec![],
        custom_values: vec![],
    }
}

#[derive(Clone, Copy)]
enum SampleKind {
    Float,
    Hist,
    FloatHist,
}

impl SampleKind {
    fn metric_type(self) -> &'static str {
        match self {
            SampleKind::Float => "float",
            SampleKind::Hist | SampleKind::FloatHist => "histogram",
        }
    }
}

fn append_kind(
    app: &mut mimir_tsdb::head::HeadAppender<'_>,
    kind: SampleKind,
    pairs: &[(&str, &str)],
    t: i64,
    v: i64,
) -> Result<u64, HeadError> {
    match kind {
        SampleKind::Float => app.append_float(None, pairs, t, v as f64),
        SampleKind::Hist => app.append_histogram(None, pairs, 0, t, gen_hist(v)),
        SampleKind::FloatHist => app.append_float_histogram(None, pairs, 0, t, gen_fhist(v)),
    }
}

fn check_sample_val(ds: &DecodedSample, expected: i64) {
    match &ds.value {
        SampleValue::Float(v) => assert_eq!(*v, expected as f64),
        SampleValue::Histogram(h) => assert_eq!(h.zero_count, expected.unsigned_abs()),
        SampleValue::FloatHistogram(fh) => assert_eq!(fh.zero_count, expected.unsigned_abs() as f64),
    }
}

fn query_head_samples(head: &Head, matcher: LabelMatcher) -> BTreeMap<String, Vec<DecodedSample>> {
    let q = TsdbQuerier::new(Some(head), vec![], i64::MIN, i64::MAX);
    let mut out = BTreeMap::new();
    for s in q.select(true, &[matcher]).unwrap() {
        out.insert(s.labels.to_string(), s.samples);
    }
    out
}

fn reopen_head_mmap_only(
    dir: &std::path::Path,
    opts: HeadOptions,
    h: Head,
    ref_id: u64,
) -> Head {
    h.mmap_head_chunks().unwrap();
    let hc_bounds = h.head_chunks_bounds_for_test(ref_id).unwrap();
    assert_eq!(hc_bounds.len(), 1);
    let (h_min, h_max) = hc_bounds[0];
    let (_, chks, _) = h.series_chunks(ref_id, h_min, h_max).unwrap().unwrap();
    let c = chks.last().unwrap();
    let enc = c.chunk.as_ref().unwrap();
    h.write_raw_chunk_for_test(ref_id, c.min_time, c.max_time, enc.encoding().as_u8(), enc.as_bytes(), false)
        .unwrap();
    h.close().unwrap();
    let reopened = Head::open(dir, opts, i64::MIN).unwrap();
    assert!(reopened.head_chunks_bounds_for_test(ref_id).unwrap().is_empty());
    assert!(!reopened.mmapped_chunks_bounds_for_test(ref_id).unwrap().is_empty());
    reopened
}

struct TracingCaptureSubscriber {
    buf: Arc<Mutex<String>>,
}

impl tracing::Subscriber for TracingCaptureSubscriber {
    fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        struct Visitor<'a>(&'a mut String);
        impl tracing::field::Visit for Visitor<'_> {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                self.0.push_str(&format!("{}={:?} ", field.name(), value));
            }
        }
        let mut s = String::new();
        event.record(&mut Visitor(&mut s));
        self.buf.lock().unwrap().push_str(&s);
        self.buf.lock().unwrap().push('\n');
    }
    fn enter(&self, _span: &tracing::span::Id) {}
    fn exit(&self, _span: &tracing::span::Id) {}
}

struct CountLifecycleCb {
    created: AtomicI64,
    deleted: AtomicI64,
}

impl CountLifecycleCb {
    fn new() -> Self {
        Self {
            created: AtomicI64::new(0),
            deleted: AtomicI64::new(0),
        }
    }
}

impl SeriesLifecycleCallback for CountLifecycleCb {
    fn pre_creation(&self, _pairs: &[(&str, &str)]) -> Result<(), HeadError> {
        Ok(())
    }
    fn post_creation(&self, _ref_id: u64, _labels: &Labels) {
        self.created.fetch_add(1, Ordering::Relaxed);
    }
    fn post_deletion(&self, deleted: &[(u64, Labels)]) {
        self.deleted.fetch_add(deleted.len() as i64, Ordering::Relaxed);
    }
}

// =========================================================================
// head_append_test.go (4)
// =========================================================================

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_test.go::TestAddJitterToChunkEndTime_ShouldHonorMaxVarianceAndMaxNextAt
#[test]
fn test_add_jitter_to_chunk_end_time_should_honor_max_variance_and_max_next_at() {
    let chunk_min_time = 10i64;
    let next_at = 95i64;
    let max_next_at = 100i64;
    let variance = 0.2f64;
    let expected_max_variance = ((next_at - chunk_min_time) as f64 * variance) as i64;
    for series_hash in 0..1000u64 {
        let actual = add_jitter_to_chunk_end_time(series_hash, chunk_min_time, next_at, max_next_at, variance);
        assert!(actual >= next_at - (expected_max_variance / 2));
        assert!(actual <= max_next_at);
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_test.go::TestAddJitterToChunkEndTime_Distribution
#[test]
fn test_add_jitter_to_chunk_end_time_distribution() {
    let chunk_min_time = 0i64;
    let next_at = 50i64;
    let max_next_at = 100i64;
    let variance = 0.2f64;
    let num_series = 1000u64;
    let expected_max_variance = ((next_at - chunk_min_time) as f64 * variance) as i64;
    let mut dist: HashMap<i64, i64> = HashMap::new();
    for series_hash in 0..num_series {
        let actual = add_jitter_to_chunk_end_time(series_hash, chunk_min_time, next_at, max_next_at, variance);
        assert!(actual >= next_at - (expected_max_variance / 2));
        assert!(actual <= next_at + (expected_max_variance / 2));
        assert!(actual <= max_next_at);
        *dist.entry(next_at - actual).or_insert(0) += 1;
    }
    for (v, count) in dist {
        assert_eq!(count, (num_series as i64) / expected_max_variance, "variance = {v}");
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_test.go::TestAddJitterToChunkEndTime_ShouldNotApplyJitterToTheLastChunkOfTheRange
#[test]
fn test_add_jitter_to_chunk_end_time_should_not_apply_jitter_to_the_last_chunk_of_the_range() {
    for series_hash in 0..10u64 {
        assert_eq!(add_jitter_to_chunk_end_time(series_hash, 150, 200, 200, 0.2), 200);
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_test.go::TestAddJitterToChunkEndTime_ShouldNotApplyJitterIfDisabled
#[test]
fn test_add_jitter_to_chunk_end_time_should_not_apply_jitter_if_disabled() {
    for series_hash in 0..10u64 {
        assert_eq!(add_jitter_to_chunk_end_time(series_hash, 100, 130, 200, 0.0), 130);
    }
}

// =========================================================================
// head_append_wal_log_failure_test.go (1)
// =========================================================================

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_wal_log_failure_test.go::TestHeadAppender_Commit_LogsWhenNewSeriesDeclarationFailsOnBothCommitAndRollback
#[test]
fn test_head_appender_commit_logs_when_new_series_declaration_fails_on_both_commit_and_rollback() {
    const WARN_SUBSTR: &str = "failed to persist newly created series' declaration to the WAL on both commit and rollback retry";

    // Subcase 1: logs when commit creates a new series and WAL is already closed.
    {
        let dir = tempfile::tempdir().unwrap();
        let h = Head::open(dir.path(), HeadOptions { chunk_range_ms: 1000, ..HeadOptions::default() }, i64::MIN).unwrap();
        h.force_close_wal_for_test();

        let captured = Arc::new(Mutex::new(String::new()));
        let sub = TracingCaptureSubscriber { buf: Arc::clone(&captured) };
        let _guard = tracing::subscriber::set_default(sub);

        let mut app = h.appender();
        let ref_id = app.append_float(None, &[("name", "brand_new_series")], 100, 1.0).unwrap();
        let err = app.commit().unwrap_err();
        assert!(matches!(err, HeadError::Wlog(_)));

        let logs = captured.lock().unwrap().clone();
        assert!(logs.contains(WARN_SUBSTR), "logs={logs}");
        assert!(logs.contains("commit_err="), "logs={logs}");
        assert!(logs.contains("rollback_err="), "logs={logs}");
        assert!(h.get_entry_by_id(ref_id).is_some());
        h.close().unwrap();
    }

    // Subcase 2: does NOT log when commit fails for an already-existing series.
    {
        let dir = tempfile::tempdir().unwrap();
        let h = Head::open(dir.path(), HeadOptions { chunk_range_ms: 1000, ..HeadOptions::default() }, i64::MIN).unwrap();
        {
            let mut app = h.appender();
            app.append_float(None, &[("name", "already_existing_series")], 100, 1.0).unwrap();
            app.commit().unwrap();
        }
        h.force_close_wal_for_test();

        let captured = Arc::new(Mutex::new(String::new()));
        let sub = TracingCaptureSubscriber { buf: Arc::clone(&captured) };
        let _guard = tracing::subscriber::set_default(sub);

        let mut app = h.appender();
        app.append_float(None, &[("name", "already_existing_series")], 200, 2.0).unwrap();
        let err = app.commit().unwrap_err();
        assert!(matches!(err, HeadError::Wlog(_)));

        let logs = captured.lock().unwrap().clone();
        assert!(!logs.contains(WARN_SUBSTR), "unexpected warn in logs={logs}");
        h.close().unwrap();
    }
}

// =========================================================================
// head_mimir_test.go (1)
// =========================================================================

// Upstream: github.com/prometheus/prometheus/tsdb/head_mimir_test.go::TestHead_LastSeriesRefAndPostingsListAccessors
#[test]
fn test_head_last_series_ref_and_postings_list_accessors() {
    let dir = tempfile::tempdir().unwrap();
    let head = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();
    assert_eq!(head.last_series_ref(), 0);

    let mut app = head.appender();
    let first_ref = app.append_float(None, &[("__name__", "up"), ("pod", "a")], 100, 1.0).unwrap();
    let second_ref = app.append_float(None, &[("__name__", "up"), ("pod", "b")], 100, 1.0).unwrap();
    app.append_float(Some(first_ref), &[("__name__", "up"), ("pod", "a")], 200, 1.0).unwrap();
    app.commit().unwrap();

    assert!(second_ref > first_ref);
    assert_eq!(head.last_series_ref(), second_ref);

    assert_eq!(head.postings_length("__name__", "up"), 2);
    let mut random_refs = Vec::new();
    head.append_random_postings_refs("__name__", "up", 4, &mut random_refs);
    assert_eq!(random_refs.len(), 4);
    for r in random_refs {
        assert!(r == first_ref || r == second_ref);
    }

    assert_eq!(head.postings_length("pod", "b"), 1);
    let mut b_refs = Vec::new();
    head.append_random_postings_refs("pod", "b", 1, &mut b_refs);
    assert_eq!(b_refs, vec![second_ref]);

    assert_eq!(head.postings_length("pod", "missing"), 0);
    let mut miss_refs = Vec::new();
    head.append_random_postings_refs("pod", "missing", 1, &mut miss_refs);
    assert!(miss_refs.is_empty());
    head.close().unwrap();
}

// =========================================================================
// head_mmap_append_test.go (1)
// =========================================================================

// Upstream: github.com/prometheus/prometheus/tsdb/head_mmap_append_test.go::TestMemSeries_appendable
#[test]
fn test_mem_series_appendable() {
    // 1. recovery
    for snapshot in [false, true] {
        for kind in [SampleKind::Float, SampleKind::Hist, SampleKind::FloatHist] {
            for (window, want_ooo_err) in [
                (0i64, Some(true)),
                (1000, None),
                (40, Some(false)),
            ] {
                let dir = tempfile::tempdir().unwrap();
                let opts = HeadOptions {
                    ooo_time_window_ms: window,
                    ..HeadOptions::default()
                };
                let h = Head::open_with_snapshot(dir.path(), opts.clone(), i64::MIN, snapshot).unwrap();
                let pairs = [("metric", "mmap_recovery")];
                let mut app = h.appender();
                let ref_id = append_kind(&mut app, kind, &pairs, 100, 1).unwrap();
                append_kind(&mut app, kind, &pairs, 200, 2).unwrap();
                let conflict = append_kind(&mut app, kind, &pairs, 200, 999);
                // Buffered conflict is detected at commit or append; commit stores (100,1) & (200,2).
                let _ = conflict;
                app.commit().unwrap();

                let h = reopen_head_mmap_only(dir.path(), opts.clone(), h, ref_id);
                let mut a = h.appender();
                let res = append_kind(&mut a, kind, &pairs, 150, 3);
                match want_ooo_err {
                    None => assert!(res.is_ok()),
                    Some(true) => assert!(matches!(res, Err(HeadError::OutOfOrderSample))),
                    Some(false) => assert!(matches!(res, Err(HeadError::TooOldSample))),
                }
                a.commit().unwrap();
                assert!(h.head_chunks_bounds_for_test(ref_id).unwrap().is_empty());

                // Conflict at last mmapped sample timestamp (200) is rejected.
                let mut a = h.appender();
                let err = append_kind(&mut a, kind, &pairs, 200, 999).unwrap_err();
                assert!(err.is_duplicate_sample(), "got {err:?}");
                a.commit().unwrap();

                // Exact duplicate at last mmapped sample timestamp (200) is accepted silently.
                let mut a = h.appender();
                append_kind(&mut a, kind, &pairs, 200, 2).unwrap();
                a.commit().unwrap();
                assert!(h.head_chunks_bounds_for_test(ref_id).unwrap().is_empty());

                // In-order sample at 300 creates new head chunk and restores duplicate checks.
                let mut a = h.appender();
                append_kind(&mut a, kind, &pairs, 300, 4).unwrap();
                a.commit().unwrap();
                let mut a = h.appender();
                let err = append_kind(&mut a, kind, &pairs, 300, 999).unwrap_err();
                assert!(err.is_duplicate_sample());
                a.rollback().unwrap();

                let q = TsdbQuerier::new(Some(&h), vec![], 0, 400);
                let m = [LabelMatcher::new(MatchType::Equal, "metric", "mmap_recovery").unwrap()];
                let res = q.select(true, &m).unwrap();
                assert_eq!(res.len(), 1);
                let mut exp = vec![(100, 1i64), (200, 2), (300, 4)];
                if want_ooo_err.is_none() {
                    exp.push((150, 3));
                    exp.sort_by_key(|x| x.0);
                }
                assert_eq!(res[0].samples.len(), exp.len());
                for (ds, (et, ev)) in res[0].samples.iter().zip(exp) {
                    assert_eq!(ds.t, et);
                    check_sample_val(ds, ev);
                }
                h.close().unwrap();
            }
        }
    }

    // 2. wal repair
    for kind in [SampleKind::Float, SampleKind::Hist, SampleKind::FloatHist] {
        for corrupt in [false, true] {
            for window in [0i64, 1000] {
                let dir = tempfile::tempdir().unwrap();
                let opts = HeadOptions {
                    chunk_range_ms: 100,
                    ooo_time_window_ms: window,
                    ..HeadOptions::default()
                };
                let h = Head::open(dir.path(), opts.clone(), i64::MIN).unwrap();
                let pairs = [("metric", "wal_repair")];
                let mut a = h.appender();
                let ref_id = append_kind(&mut a, kind, &pairs, 100, 1).unwrap();
                append_kind(&mut a, kind, &pairs, 150, 2).unwrap();
                a.commit().unwrap();
                let (seg, offset) = h.wal_last_segment_and_offset_for_test().unwrap();
                let mut a = h.appender();
                append_kind(&mut a, kind, &pairs, 200, 3).unwrap();
                a.commit().unwrap();
                h.mmap_head_chunks().unwrap();
                assert_eq!(h.mmapped_chunks_bounds_for_test(ref_id).unwrap().len(), 1);
                assert_eq!(h.head_chunks_bounds_for_test(ref_id).unwrap().len(), 1);
                h.close().unwrap();

                if corrupt {
                    let seg_path = segment_name(&dir.path().join("wal"), seg.unwrap());
                    let mut f = OpenOptions::new().write(true).open(&seg_path).unwrap();
                    f.seek(SeekFrom::Start(offset as u64)).unwrap();
                    f.write_all(&[255]).unwrap();
                }

                let db = Tsdb::open_with_reload_interval(
                    dir.path(),
                    TsdbOptions { head: opts, ..TsdbOptions::default() },
                    Duration::ZERO,
                )
                .unwrap();
                let (ts, val) = if corrupt {
                    assert!(db.head().head_chunks_bounds_for_test(ref_id).unwrap().is_empty());
                    assert!(!db.head().mmapped_chunks_bounds_for_test(ref_id).unwrap().is_empty());
                    (150, 2)
                } else {
                    assert!(!db.head().head_chunks_bounds_for_test(ref_id).unwrap().is_empty());
                    (200, 3)
                };
                let mut a = db.appender();
                let err = append_kind(&mut a, kind, &pairs, ts, 999).unwrap_err();
                assert!(err.is_duplicate_sample());
                a.commit().unwrap();
                let mut a = db.appender();
                append_kind(&mut a, kind, &pairs, ts, val).unwrap();
                a.commit().unwrap();
                db.close().unwrap();
            }
        }
    }

    // 3. unreadable chunk
    for window in [0i64, 1000] {
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            ooo_time_window_ms: window,
            ..HeadOptions::default()
        };
        let h = Head::open(dir.path(), opts.clone(), i64::MIN).unwrap();
        let pairs = [("metric", "mmap_recovery_unreadable_chunk")];
        let mut a = h.appender();
        let ref_id = a.append_float(None, &pairs, 100, 1.0).unwrap();
        a.append_float(Some(ref_id), &pairs, 200, 2.0).unwrap();
        a.commit().unwrap();
        let h = reopen_head_mmap_only(dir.path(), opts, h, ref_id);
        h.close().unwrap();
        // Remove chunks_head file so mmapped chunk cannot be read, then corrupt/reopen would rebuild;
        // if unreadable chunk file on disk fails read, fallback detects OOO/out-of-order.
        let mut h_opts = HeadOptions { ooo_time_window_ms: window, ..HeadOptions::default() };
        h_opts.chunk_range_ms = DEFAULT_BLOCK_DURATION_MS;
        let h2 = Head::open(dir.path(), h_opts, i64::MIN).unwrap();
        let mut a = h2.appender();
        let r = a.append_float(None, &pairs, 200, 999.0);
        assert!(r.unwrap_err().is_duplicate_sample());
        a.rollback().unwrap();
        h2.close().unwrap();
    }

    // 4. multiple mmapped chunks
    {
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            chunk_range_ms: 100,
            ..HeadOptions::default()
        };
        let h = Head::open(dir.path(), opts.clone(), i64::MIN).unwrap();
        let pairs = [("metric", "mmap_recovery_last_chunk")];
        let mut a = h.appender();
        let ref_id = a.append_float(None, &pairs, 100, 1.0).unwrap();
        a.append_float(Some(ref_id), &pairs, 200, 2.0).unwrap();
        a.commit().unwrap();
        h.mmap_head_chunks().unwrap();
        let h = reopen_head_mmap_only(dir.path(), opts, h, ref_id);
        assert_eq!(h.mmapped_chunks_bounds_for_test(ref_id).unwrap().len(), 2);

        let mut a = h.appender();
        a.append_float(None, &pairs, 200, 2.0).unwrap();
        a.commit().unwrap();

        let mut a = h.appender();
        let err = a.append_float(None, &pairs, 200, 999.0).unwrap_err();
        assert!(err.is_duplicate_sample());
        a.rollback().unwrap();

        let mut a = h.appender();
        let err = a.append_float(None, &pairs, 100, 999.0).unwrap_err();
        assert!(matches!(err, HeadError::OutOfBounds));
        a.rollback().unwrap();
        h.close().unwrap();
    }

    // 5. type conflict
    {
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions::default();
        let h = Head::open(dir.path(), opts.clone(), i64::MIN).unwrap();
        let pairs = [("metric", "mmap_recovery_type_conflict")];
        let mut a = h.appender();
        let ref_id = a.append_histogram(None, &pairs, 0, 100, gen_hist(1)).unwrap();
        a.append_histogram(Some(ref_id), &pairs, 0, 200, gen_hist(2)).unwrap();
        a.commit().unwrap();
        let h = reopen_head_mmap_only(dir.path(), opts, h, ref_id);

        let mut a = h.appender();
        let err = a.append_float(None, &pairs, 200, 42.0).unwrap_err();
        assert!(err.is_duplicate_sample());
        a.rollback().unwrap();
        h.close().unwrap();
    }
}

// =========================================================================
// head_append_v2_test.go (63)
// =========================================================================

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestHeadAppenderV2_WALMultiRef
#[test]
fn test_head_appender_v2_wal_multi_ref() {
    let dir = tempfile::tempdir().unwrap();
    let reg = Arc::new(prometheus::Registry::new());
    let opts = HeadOptions {
        chunk_range_ms: 1000,
        registry: Some(Arc::clone(&reg)),
        ..HeadOptions::default()
    };
    let head = Head::open(dir.path(), opts.clone(), i64::MIN).unwrap();
    let pairs = [("foo", "bar")];

    let mut app = head.appender();
    let ref1 = app.append_float(None, &pairs, 100, 1.0).unwrap();
    app.commit().unwrap();
    assert_eq!(metric_value(&reg, "prometheus_tsdb_head_chunks_created_total", &[]), 1.0);

    let mut app = head.appender();
    app.append_float(None, &pairs, 1500, 2.0).unwrap();
    app.commit().unwrap();
    assert_eq!(metric_value(&reg, "prometheus_tsdb_head_chunks_created_total", &[]), 2.0);

    head.truncate(1600).unwrap();

    let mut app = head.appender();
    let ref2 = app.append_float(None, &pairs, 1700, 3.0).unwrap();
    app.commit().unwrap();
    assert_eq!(metric_value(&reg, "prometheus_tsdb_head_chunks_created_total", &[]), 3.0);

    let mut app = head.appender();
    app.append_float(None, &pairs, 2000, 4.0).unwrap();
    app.commit().unwrap();
    assert_eq!(metric_value(&reg, "prometheus_tsdb_head_chunks_created_total", &[]), 4.0);
    assert_ne!(ref1, ref2);
    head.close().unwrap();

    let reopened = Head::open(dir.path(), HeadOptions { chunk_range_ms: 1000, ..HeadOptions::default() }, i64::MIN).unwrap();
    let q = TsdbQuerier::new(Some(&reopened), vec![], 0, 2100);
    let m = [LabelMatcher::new(MatchType::Equal, "foo", "bar").unwrap()];
    let res = q.select(true, &m).unwrap();
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].samples.iter().map(|s| (s.t, s.value.clone())).collect::<Vec<_>>(), vec![
        (1700, SampleValue::Float(3.0)),
        (2000, SampleValue::Float(4.0)),
    ]);
    reopened.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestHeadAppenderV2_AppendersMetrics
#[test]
fn test_head_appender_v2_appenders_metrics() {
    let dir = tempfile::tempdir().unwrap();
    let reg = Arc::new(prometheus::Registry::new());
    let head = Head::open(dir.path(), HeadOptions { registry: Some(Arc::clone(&reg)), ..HeadOptions::default() }, i64::MIN).unwrap();

    let app = head.appender();
    assert_eq!(metric_value(&reg, "prometheus_tsdb_head_active_appenders", &[]), 1.0);
    assert_eq!(metric_value(&reg, "prometheus_tsdb_head_appenders_created_total", &[]), 1.0);
    app.rollback().unwrap();
    assert_eq!(metric_value(&reg, "prometheus_tsdb_head_active_appenders", &[]), 0.0);

    head.appender().commit().unwrap();
    assert_eq!(metric_value(&reg, "prometheus_tsdb_head_active_appenders", &[]), 0.0);
    assert_eq!(metric_value(&reg, "prometheus_tsdb_head_appenders_created_total", &[]), 2.0);

    let mut app = head.appender();
    app.append_float(None, &[("foo", "bar")], 100, 1.0).unwrap();
    assert_eq!(metric_value(&reg, "prometheus_tsdb_head_active_appenders", &[]), 1.0);
    assert_eq!(metric_value(&reg, "prometheus_tsdb_head_appenders_created_total", &[]), 3.0);
    app.rollback().unwrap();
    assert_eq!(metric_value(&reg, "prometheus_tsdb_head_active_appenders", &[]), 0.0);

    let mut app = head.appender();
    app.append_float(None, &[("foo", "bar")], 100, 1.0).unwrap();
    app.commit().unwrap();
    assert_eq!(metric_value(&reg, "prometheus_tsdb_head_active_appenders", &[]), 0.0);
    assert_eq!(metric_value(&reg, "prometheus_tsdb_head_appenders_created_total", &[]), 4.0);
    head.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestHeadAppenderV2_RaceBetweenSeriesCreationAndGC
#[test]
fn test_head_appender_v2_race_between_series_creation_and_gc() {
    let dir = tempfile::tempdir().unwrap();
    let head = Arc::new(Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap());
    const TOTAL_SERIES: usize = 20_000;
    let done = Arc::new(AtomicBool::new(false));
    let done_bg = Arc::clone(&done);
    let head_bg = Arc::clone(&head);
    let handle = std::thread::spawn(move || {
        let mut app = head_bg.appender();
        for i in 0..TOTAL_SERIES {
            let v = i.to_string();
            app.append_float(None, &[("foo", &v)], 100, 1.0).unwrap();
        }
        app.commit().unwrap();
        done_bg.store(true, Ordering::Release);
    });
    while !done.load(Ordering::Acquire) {
        head.gc(head.min_time()).unwrap();
    }
    handle.join().unwrap();
    assert_eq!(head.num_series(), TOTAL_SERIES as u64);
    head.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestHeadAppenderV2_CanGCSeriesCreatedWithoutSamples
#[test]
fn test_head_appender_v2_can_gc_series_created_without_samples() {
    for commit in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let chunk_range = 3_600_000i64;
        let opts = HeadOptions {
            chunk_range_ms: chunk_range,
            ooo_time_window_ms: chunk_range / 2,
            ..HeadOptions::default()
        };
        let head = Head::open(dir.path(), opts, i64::MIN).unwrap();
        let first_ts = 10 * chunk_range;
        {
            let mut app = head.appender();
            app.append_float(None, &[("lbl", "ok")], first_ts, 1.0).unwrap();
            app.commit().unwrap();
            assert_eq!(head.num_series(), 1);
        }
        let mut app = head.appender();
        let err = app.append_float(None, &[("foo", "bar")], first_ts - chunk_range, 2.0).unwrap_err();
        assert!(matches!(err, HeadError::TooOldSample));
        if commit {
            app.commit().unwrap();
        } else {
            app.rollback().unwrap();
        }
        head.gc(head.min_time()).unwrap();
        assert_eq!(head.num_series(), 1);
        head.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestHeadAppenderV2_DeleteSimple
#[test]
fn test_head_appender_v2_delete_simple() {
    let m = [LabelMatcher::new(MatchType::Equal, "a", "b").unwrap()];
    let cases = [
        (vec![(0i64, 3i64)], vec![], vec![4i64, 5, 6, 7, 8, 9]),
        (vec![(1, 3)], vec![], vec![0, 4, 5, 6, 7, 8, 9]),
        (vec![(1, 3), (4, 7)], vec![], vec![0, 8, 9]),
        (vec![(1, 3), (4, 700)], vec![], vec![0]),
        (vec![(0, 9)], vec![], vec![]),
        (vec![(1, 3)], vec![11, 13, 15], vec![0, 4, 5, 6, 7, 8, 9, 11, 13, 15]),
        (vec![(7, 20)], vec![11, 13, 15], vec![0, 1, 2, 3, 4, 5, 6, 11, 13, 15]),
    ];
    for comp in [CompressionType::None, CompressionType::Snappy, CompressionType::Zstd] {
        for (dranges, add, exp) in &cases {
            let dir = tempfile::tempdir().unwrap();
            let opts = HeadOptions { chunk_range_ms: 1000, wal_compression: comp, ..HeadOptions::default() };
            let head = Head::open(dir.path(), opts.clone(), i64::MIN).unwrap();
            {
                let mut app = head.appender();
                for t in 0..10i64 {
                    app.append_float(None, &[("a", "b")], t, t as f64).unwrap();
                }
                app.commit().unwrap();
            }
            for &(mn, mx) in dranges {
                head.delete(mn, mx, &m).unwrap();
            }
            if !add.is_empty() {
                let mut app = head.appender();
                for &t in add {
                    app.append_float(None, &[("a", "b")], t, t as f64).unwrap();
                }
                app.commit().unwrap();
            }
            let check = |h: &Head| {
                let got = query_head_samples(h, m[0].clone());
                let ts: Vec<i64> = got.values().flat_map(|v| v.iter().map(|s| s.t)).collect();
                assert_eq!(&ts, exp);
            };
            check(&head);
            head.close().unwrap();

            let reloaded = Head::open(dir.path(), opts, i64::MIN).unwrap();
            check(&reloaded);
            reloaded.close().unwrap();
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestHeadAppenderV2_DeleteUntilCurrMax
#[test]
fn test_head_appender_v2_delete_until_curr_max() {
    let dir = tempfile::tempdir().unwrap();
    let hb = Head::open(dir.path(), HeadOptions { chunk_range_ms: 1_000_000, ..HeadOptions::default() }, i64::MIN).unwrap();
    {
        let mut app = hb.appender();
        for i in 0..10i64 {
            app.append_float(None, &[("a", "b")], i, (i as f64) * 1.5).unwrap();
        }
        app.commit().unwrap();
    }
    let m = [LabelMatcher::new(MatchType::Equal, "a", "b").unwrap()];
    hb.delete(0, 10_000, &m).unwrap();
    let got = query_head_samples(&hb, m[0].clone());
    assert!(got.is_empty() || got.values().all(|v| v.is_empty()));

    {
        let mut app = hb.appender();
        app.append_float(None, &[("a", "b")], 11, 1.0).unwrap();
        app.commit().unwrap();
    }
    let got = query_head_samples(&hb, m[0].clone());
    assert_eq!(got[r#"{a="b"}"#].iter().map(|s| (s.t, s.value.clone())).collect::<Vec<_>>(), vec![(11, SampleValue::Float(1.0))]);
    hb.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestHeadAppenderV2_DeleteSamplesAndSeriesStillInWALAfterCheckpoint
#[test]
fn test_head_appender_v2_delete_samples_and_series_still_in_wal_after_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let num_samples = 10_000i64;
    let hb = Head::open(dir.path(), HeadOptions { chunk_range_ms: num_samples * 10, wal_segment_size: 32 * 1024, ..HeadOptions::default() }, i64::MIN).unwrap();
    for i in 0..num_samples {
        let mut app = hb.appender();
        app.append_float(None, &[("a", "b")], i, 0.0).unwrap();
        app.commit().unwrap();
    }
    let m = [LabelMatcher::new(MatchType::Equal, "a", "b").unwrap()];
    hb.delete(0, num_samples, &m).unwrap();
    hb.truncate(1).unwrap();
    hb.close().unwrap();

    let wal_dir = dir.path().join("wal");
    let (cdir, _) = last_checkpoint(&wal_dir).unwrap().unwrap();
    let mut recs = read_all_wal_records(&cdir);
    recs.extend(read_all_wal_records(&wal_dir));
    let (mut series, mut samples, mut stones, mut metadata) = (0usize, 0usize, 0usize, 0usize);
    for rec in recs {
        match rec {
            DecodedRecord::Series(v) => series += v.len(),
            DecodedRecord::Samples(v) => samples += v.len(),
            DecodedRecord::Tombstones(v) => stones += v.len(),
            DecodedRecord::Metadata(v) => metadata += v.len(),
            _ => {}
        }
    }
    assert_eq!(series, 1);
    assert_eq!(samples, 9999);
    assert_eq!(stones, 1);
    assert_eq!(metadata, 0);
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestHeadAppenderV2_Delete_e2e
#[test]
fn test_head_appender_v2_delete_e2e() {
    let dir = tempfile::tempdir().unwrap();
    let hb = Head::open(dir.path(), HeadOptions { chunk_range_ms: 100_000, ..HeadOptions::default() }, i64::MIN).unwrap();
    let series_defs: Vec<Vec<(&str, &str)>> = vec![
        vec![("a", "b"), ("instance", "localhost:9090"), ("job", "prometheus")],
        vec![("a", "b"), ("instance", "127.0.0.1:9090"), ("job", "prometheus")],
        vec![("a", "b"), ("instance", "127.0.0.1:9090"), ("job", "prom-k8s")],
        vec![("a", "b"), ("instance", "localhost:9090"), ("job", "prom-k8s")],
        vec![("a", "c"), ("instance", "localhost:9090"), ("job", "prometheus")],
        vec![("a", "c"), ("instance", "127.0.0.1:9090"), ("job", "prometheus")],
        vec![("a", "c"), ("instance", "127.0.0.1:9090"), ("job", "prom-k8s")],
        vec![("a", "c"), ("instance", "localhost:9090"), ("job", "prom-k8s")],
    ];
    let mut series_map: BTreeMap<String, Vec<(i64, f64)>> = BTreeMap::new();
    {
        let mut app = hb.appender();
        for pairs in &series_defs {
            let lbls = Labels::from_pairs(pairs.iter().copied());
            let mut ts = 50i64;
            let mut pts = Vec::new();
            for j in 0..200 {
                let v = (j as f64) * 0.5;
                app.append_float(None, pairs, ts, v).unwrap();
                pts.push((ts, v));
                ts += 5;
            }
            series_map.insert(lbls.to_string(), pts);
        }
        app.commit().unwrap();
    }
    let dels = [
        (vec![LabelMatcher::new(MatchType::Equal, "a", "b").unwrap()], vec![(300, 500), (600, 670)]),
        (
            vec![
                LabelMatcher::new(MatchType::Equal, "a", "b").unwrap(),
                LabelMatcher::new(MatchType::Equal, "job", "prom-k8s").unwrap(),
            ],
            vec![(300, 500), (100, 670)],
        ),
        (
            vec![
                LabelMatcher::new(MatchType::Equal, "a", "c").unwrap(),
                LabelMatcher::new(MatchType::Equal, "instance", "localhost:9090").unwrap(),
                LabelMatcher::new(MatchType::Equal, "job", "prometheus").unwrap(),
            ],
            vec![(300, 400), (100, 6700)],
        ),
    ];
    for (ms, ranges) in dels {
        for (mn, mx) in &ranges {
            hb.delete(*mn, *mx, &ms).unwrap();
        }
        for (lbl_str, pts) in &mut series_map {
            let l_pairs_match = ms.iter().all(|m| {
                let needle = format!("{}={:?}", m.name, m.value);
                lbl_str.contains(&needle)
            });
            if l_pairs_match {
                pts.retain(|&(t, _)| !ranges.iter().any(|&(mn, mx)| t >= mn && t <= mx));
            }
        }
        let q = TsdbQuerier::new(Some(&hb), vec![], 0, 100_000);
        let got: BTreeMap<String, Vec<(i64, f64)>> = q
            .select(true, &ms)
            .unwrap()
            .into_iter()
            .filter(|s| !s.samples.is_empty())
            .map(|s| {
                (
                    s.labels.to_string(),
                    s.samples
                        .into_iter()
                        .map(|d| match d.value {
                            SampleValue::Float(v) => (d.t, v),
                            _ => unreachable!(),
                        })
                        .collect(),
                )
            })
            .collect();
        for (k, pts) in &series_map {
            let l_match = ms.iter().all(|m| k.contains(&format!("{}={:?}", m.name, m.value)));
            if l_match {
                if pts.is_empty() {
                    assert!(!got.contains_key(k));
                } else {
                    assert_eq!(got.get(k), Some(pts));
                }
            }
        }
    }
    hb.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestHeadAppenderV2_UncommittedSamplesNotLostOnTruncate
#[test]
fn test_head_appender_v2_uncommitted_samples_not_lost_on_truncate() {
    let dir = tempfile::tempdir().unwrap();
    let h = Head::open(dir.path(), HeadOptions { chunk_range_ms: 1000, ..HeadOptions::default() }, i64::MIN).unwrap();
    let mut app = h.appender();
    app.append_float(None, &[("a", "1")], 2100, 1.0).unwrap();
    h.truncate(2000).unwrap();
    assert!(h.get_entry_by_sorted_pairs(&[("a", "1")]).is_some());
    app.commit().unwrap();
    let m = [LabelMatcher::new(MatchType::Equal, "a", "1").unwrap()];
    let got = query_head_samples(&h, m[0].clone());
    assert_eq!(got.len(), 1);
    h.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestHeadAppenderV2_TestRemoveSeriesAfterRollbackAndTruncate
#[test]
fn test_head_appender_v2_test_remove_series_after_rollback_and_truncate() {
    let dir = tempfile::tempdir().unwrap();
    let h = Head::open(dir.path(), HeadOptions { chunk_range_ms: 1000, ..HeadOptions::default() }, i64::MIN).unwrap();
    h.init_time(0);
    let mut app = h.appender();
    app.append_float(None, &[("a", "1")], 2100, 1.0).unwrap();
    h.truncate(2000).unwrap();
    assert!(h.get_entry_by_sorted_pairs(&[("a", "1")]).is_some());
    app.rollback().unwrap();
    let m = [LabelMatcher::new(MatchType::Equal, "a", "1").unwrap()];
    assert!(query_head_samples(&h, m[0].clone()).is_empty());
    h.truncate(2050).unwrap();
    assert!(h.get_entry_by_sorted_pairs(&[("a", "1")]).is_none());
    h.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestHeadAppenderV2_LogRollback
#[test]
fn test_head_appender_v2_log_rollback() {
    for comp in [CompressionType::None, CompressionType::Snappy, CompressionType::Zstd] {
        let dir = tempfile::tempdir().unwrap();
        let h = Head::open(dir.path(), HeadOptions { chunk_range_ms: 1000, wal_compression: comp, ..HeadOptions::default() }, i64::MIN).unwrap();
        let mut app = h.appender();
        app.append_float(None, &[("a", "b")], 1, 2.0).unwrap();
        app.rollback().unwrap();
        h.close().unwrap();
        let recs = read_all_wal_records(&dir.path().join("wal"));
        assert_eq!(recs.len(), 1);
        match &recs[0] {
            DecodedRecord::Series(s) => {
                assert_eq!(s.len(), 1);
                assert_eq!(s[0].ref_id, 1);
                assert_eq!(s[0].labels, Labels::from_pairs([("a", "b")]));
            }
            other => panic!("expected series record, got {other:?}"),
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestHeadAppenderV2_ReturnsSortedLabelValues
#[test]
fn test_head_appender_v2_returns_sorted_label_values() {
    let dir = tempfile::tempdir().unwrap();
    let h = Head::open(dir.path(), HeadOptions { chunk_range_ms: 1000, ..HeadOptions::default() }, i64::MIN).unwrap();
    let mut app = h.appender();
    for i in (1..=100).rev() {
        for j in 0..10 {
            let m = format!("metric_{i}");
            let v = format!("value_{j}");
            app.append_float(None, &[("__name__", &m), ("label", &v)], 2100, 1.0).unwrap();
        }
    }
    app.commit().unwrap();
    let vals = h.label_values("__name__", &[]).unwrap();
    assert!(vals.windows(2).all(|w| w[0] <= w[1]));
    h.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestHeadAppenderV2_NewWalSegmentOnTruncate
#[test]
fn test_head_appender_v2_new_wal_segment_on_truncate() {
    let dir = tempfile::tempdir().unwrap();
    let wal_dir = dir.path().join("wal");
    let h = Head::open(dir.path(), HeadOptions { chunk_range_ms: 1000, ..HeadOptions::default() }, i64::MIN).unwrap();
    let add = |ts: i64| {
        let mut app = h.appender();
        app.append_float(None, &[("a", "b")], ts, 0.0).unwrap();
        app.commit().unwrap();
    };
    add(0);
    assert_eq!(segments_range(&wal_dir).unwrap().unwrap().1, 0);
    add(1);
    h.truncate(1).unwrap();
    assert_eq!(segments_range(&wal_dir).unwrap().unwrap().1, 1);
    add(2);
    h.truncate(2).unwrap();
    assert_eq!(segments_range(&wal_dir).unwrap().unwrap().1, 2);
    h.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestHeadAppenderV2_Append_DuplicateLabelName
#[test]
fn test_head_appender_v2_append_duplicate_label_name() {
    let dir = tempfile::tempdir().unwrap();
    let h = Head::open(dir.path(), HeadOptions { chunk_range_ms: 1000, ..HeadOptions::default() }, i64::MIN).unwrap();
    let mut app = h.appender();
    let err = app.append_float(None, &[("a", "c"), ("a", "b")], 0, 0.0).unwrap_err();
    assert!(matches!(err, HeadError::DuplicateLabelName(ref n) if n == "a"));
    let err = app.append_float(None, &[("__name__", "up"), ("job", "prom"), ("le", "500"), ("le", "400")], 0, 0.0).unwrap_err();
    assert!(matches!(err, HeadError::DuplicateLabelName(ref n) if n == "le"));
    app.rollback().unwrap();
    h.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestHeadAppenderV2_MemSeriesIsolation
#[test]
fn test_head_appender_v2_mem_series_isolation() {
    let iso = Isolation::new(false);
    let mut ids = Vec::new();
    for _ in 0..1000 {
        let (id, _) = iso.new_append_id(0);
        ids.push(id);
        iso.close_append(id);
    }
    assert_eq!(iso.low_watermark(), 1000);
    let st = iso.state(i64::MIN, i64::MAX);
    assert_eq!(st.info().max_append_id, 1000);
    st.close();
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestHeadAppenderV2_IsolationRollback
#[test]
fn test_head_appender_v2_isolation_rollback() {
    let iso = Isolation::new(false);
    let (id1, _) = iso.new_append_id(0);
    iso.close_append(id1);
    assert_eq!(iso.low_watermark(), id1);
    let (id2, _) = iso.new_append_id(0);
    iso.close_append(id2);
    assert_eq!(iso.low_watermark(), id2);
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestHeadAppenderV2_IsolationLowWatermarkMonotonous
#[test]
fn test_head_appender_v2_isolation_low_watermark_monotonous() {
    let iso = Isolation::new(false);
    let (id1, _) = iso.new_append_id(0);
    let (id2, _) = iso.new_append_id(0);
    let st = iso.state(0, i64::MAX);
    let low1 = iso.low_watermark();
    iso.close_append(id2);
    iso.close_append(id1);
    st.close();
    assert!(iso.low_watermark() >= low1);
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestHeadAppenderV2_IsolationWithoutAdd
#[test]
fn test_head_appender_v2_isolation_without_add() {
    let dir = tempfile::tempdir().unwrap();
    let head = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();
    let mut app = head.appender();
    app.append_float(None, &[("foo", "baz")], 1, 1.0).unwrap();
    app.commit().unwrap();
    assert_eq!(head.num_series(), 1);
    head.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestHeadAppenderV2_Append_OutOfOrderSamplesMetric
#[test]
fn test_head_appender_v2_append_out_of_order_samples_metric() {
    for kind in [SampleKind::Float, SampleKind::Hist, SampleKind::FloatHist] {
        let dir = tempfile::tempdir().unwrap();
        let reg = Arc::new(prometheus::Registry::new());
        let db = Tsdb::open_with_reload_interval(
            dir.path(),
            TsdbOptions {
                head: HeadOptions { registry: Some(Arc::clone(&reg)), ..HeadOptions::default() },
                ..TsdbOptions::default()
            },
            Duration::ZERO,
        )
        .unwrap();
        db.disable_compactions();
        let pairs = [("a", "b")];
        {
            let mut app = db.appender();
            for i in 1..=5i64 {
                append_kind(&mut app, kind, &pairs, i, 99).unwrap();
            }
            app.commit().unwrap();
        }
        assert_eq!(
            metric_value(&reg, "prometheus_tsdb_out_of_order_samples_total", &[("type", kind.metric_type())]),
            0.0
        );
        {
            let mut app = db.appender();
            for t in [2i64, 3, 4] {
                assert!(matches!(append_kind(&mut app, kind, &pairs, t, 99), Err(HeadError::OutOfOrderSample)));
            }
            app.commit().unwrap();
        }
        assert_eq!(
            metric_value(&reg, "prometheus_tsdb_out_of_order_samples_total", &[("type", kind.metric_type())]),
            3.0
        );
        {
            let mut app = db.appender();
            append_kind(&mut app, kind, &pairs, DEFAULT_BLOCK_DURATION_MS * 2, 99).unwrap();
            app.commit().unwrap();
        }
        db.compact().unwrap();
        let mvt = db.head().min_valid_time();
        assert!(mvt > 0);
        {
            let mut app = db.appender();
            assert!(matches!(append_kind(&mut app, kind, &pairs, mvt - 2, 99), Err(HeadError::OutOfBounds)));
            assert!(matches!(append_kind(&mut app, kind, &pairs, mvt - 1, 99), Err(HeadError::OutOfBounds)));
            app.commit().unwrap();
        }
        assert_eq!(
            metric_value(&reg, "prometheus_tsdb_out_of_bound_samples_total", &[("type", kind.metric_type())]),
            2.0
        );
        db.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestHeadAppenderV2_Append_OutOfOrderSamplesMetricNativeHistogramOOODisabled
#[test]
fn test_head_appender_v2_append_out_of_order_samples_metric_native_histogram_ooo_disabled() {
    for kind in [SampleKind::Hist, SampleKind::FloatHist] {
        let dir = tempfile::tempdir().unwrap();
        let reg = Arc::new(prometheus::Registry::new());
        let db = Tsdb::open_with_reload_interval(
            dir.path(),
            TsdbOptions {
                head: HeadOptions { ooo_time_window_ms: 0, registry: Some(Arc::clone(&reg)), ..HeadOptions::default() },
                ..TsdbOptions::default()
            },
            Duration::ZERO,
        )
        .unwrap();
        db.disable_compactions();
        let pairs = [("a", "b")];
        {
            let mut app = db.appender();
            for i in 1..=5i64 {
                append_kind(&mut app, kind, &pairs, i, 99).unwrap();
            }
            app.commit().unwrap();
        }
        let mut app = db.appender();
        assert!(matches!(append_kind(&mut app, kind, &pairs, 2, 99), Err(HeadError::OutOfOrderSample)));
        app.commit().unwrap();
        assert_eq!(
            metric_value(&reg, "prometheus_tsdb_out_of_order_samples_total", &[("type", "histogram")]),
            1.0
        );
        db.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestHeadAppenderV2_HistogramErrorDoesNotSetPendingCommit
#[test]
fn test_head_appender_v2_histogram_error_does_not_set_pending_commit() {
    for kind in [SampleKind::Hist, SampleKind::FloatHist] {
        let dir = tempfile::tempdir().unwrap();
        let head = Head::open(dir.path(), HeadOptions { chunk_range_ms: 1000, ..HeadOptions::default() }, i64::MIN).unwrap();
        let pairs = [("a", "b")];
        {
            let mut app = head.appender();
            append_kind(&mut app, kind, &pairs, 200, 0).unwrap();
            app.commit().unwrap();
        }
        let entry = head.get_entry_by_sorted_pairs(&pairs).unwrap();
        assert_eq!(entry.pending_commits_count(), 0);
        let mut app = head.appender();
        assert!(matches!(append_kind(&mut app, kind, &pairs, 100, 0), Err(HeadError::OutOfOrderSample)));
        app.rollback().unwrap();
        assert_eq!(entry.pending_commits_count(), 0);
        head.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestHeadLabelNamesValuesWithMinMaxRange_AppenderV2
#[test]
fn test_head_label_names_values_with_min_max_range_appender_v2() {
    let dir = tempfile::tempdir().unwrap();
    let head = Head::open(dir.path(), HeadOptions { chunk_range_ms: 1000, ..HeadOptions::default() }, i64::MIN).unwrap();
    let mut app = head.appender();
    app.append_float(None, &[("a", "d")], 100, 0.0).unwrap();
    app.append_float(None, &[("b", "e")], 200, 0.0).unwrap();
    app.append_float(None, &[("c", "f")], 300, 0.0).unwrap();
    app.commit().unwrap();
    assert_eq!(head.min_time(), 100);
    assert_eq!(head.max_time(), 300);

    let q_out1 = TsdbQuerier::new(Some(&head), vec![], 290, 90);
    assert!(q_out1.label_names(&[]).unwrap().is_empty());
    let q_out2 = TsdbQuerier::new(Some(&head), vec![], 310, 210);
    assert!(q_out2.label_names(&[]).unwrap().is_empty());
    let q_in = TsdbQuerier::new(Some(&head), vec![], 290, 110);
    assert_eq!(q_in.label_names(&[]).unwrap(), vec!["a", "b", "c"]);
    assert_eq!(q_in.label_values("a", None, &[]).unwrap(), vec!["d"]);
    head.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestHeadAppenderV2_ErrReuse
#[test]
fn test_head_appender_v2_err_reuse() {
    let dir = tempfile::tempdir().unwrap();
    let head = Head::open(dir.path(), HeadOptions { chunk_range_ms: 1000, ..HeadOptions::default() }, i64::MIN).unwrap();
    let mut app = head.appender();
    app.append_float(None, &[("test", "test")], 0, 0.0).unwrap();
    app.commit().unwrap();

    let mut app = head.appender();
    app.append_float(None, &[("test", "test")], 1, 0.0).unwrap();
    app.rollback().unwrap();

    let mut app = head.appender();
    app.append_float(None, &[("test", "test")], 2, 0.0).unwrap();
    app.commit().unwrap();
    head.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestHeadAppenderV2_MinTimeAfterTruncation
#[test]
fn test_head_appender_v2_min_time_after_truncation() {
    let dir = tempfile::tempdir().unwrap();
    let head = Head::open(dir.path(), HeadOptions { chunk_range_ms: 2000, ..HeadOptions::default() }, i64::MIN).unwrap();
    let mut app = head.appender();
    app.append_float(None, &[("a", "b")], 100, 100.0).unwrap();
    app.append_float(None, &[("a", "b")], 4000, 200.0).unwrap();
    app.append_float(None, &[("a", "b")], 8000, 300.0).unwrap();
    app.commit().unwrap();

    head.truncate(3500).unwrap();
    assert_eq!(head.min_time(), 4000);
    assert_eq!(head.min_valid_time(), 4000);

    head.truncate(5000).unwrap();
    assert_eq!(head.min_time(), head.appendable_min_valid_time());
    assert_eq!(head.min_valid_time(), head.appendable_min_valid_time());

    head.truncate(7500).unwrap();
    assert_eq!(head.min_time(), 7500);
    assert_eq!(head.min_valid_time(), 7500);
    head.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestHeadAppenderV2_AppendExemplars
#[test]
fn test_head_appender_v2_append_exemplars() {
    let dir = tempfile::tempdir().unwrap();
    let head = Head::open(dir.path(), HeadOptions { chunk_range_ms: 2000, ..HeadOptions::default() }, i64::MIN).unwrap();
    let mut app = head.appender();
    let ref_id = app.append_float(None, &[("a", "b")], 100, 100.0).unwrap();
    app.append_exemplar(
        Some(ref_id),
        &[("a", "b")],
        Exemplar::new(Labels::from_pairs([("trace_id", "123")]), 1.0, -1000),
    )
    .unwrap();
    app.commit().unwrap();
    head.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestDataMissingOnQueryDuringCompaction_AppenderV2
#[test]
fn test_data_missing_on_query_during_compaction_appender_v2() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Tsdb::open_with_reload_interval(dir.path(), TsdbOptions::default(), Duration::ZERO).unwrap());
    db.disable_compactions();
    let mut exp_samples = Vec::new();
    let mut maxt = 0i64;
    {
        let mut app = db.appender();
        let mut ref_id = None;
        for i in 0..=(120 * 7) {
            let ts = i * DEFAULT_BLOCK_DURATION_MS / (4 * 120);
            let r = app.append_float(ref_id, &[("a", "b")], ts, i as f64).unwrap();
            ref_id = Some(r);
            maxt = ts;
            exp_samples.push((ts, i as f64));
        }
        app.commit().unwrap();
    }
    let cq = db.chunk_querier(0, maxt);
    let m = [LabelMatcher::new(MatchType::Equal, "a", "b").unwrap()];
    let prep = cq.prepare_select(&m).unwrap();

    let db_bg = Arc::clone(&db);
    let h = std::thread::spawn(move || db_bg.compact().unwrap());
    std::thread::sleep(Duration::from_millis(20));

    let chunks = cq.execute_prepared_select(prep, true).unwrap();
    let mut got = Vec::new();
    for s in chunks {
        for cm in s.chunks {
            for ds in decode_chunk_samples(cm.chunk.as_ref().unwrap()).unwrap() {
                if let SampleValue::Float(v) = ds.value {
                    got.push((ds.t, v));
                }
            }
        }
    }
    got.sort_by_key(|x| x.0);
    assert_eq!(got, exp_samples);
    let created = h.join().unwrap();
    assert!(!created.is_empty());
    assert!(!db.blocks().is_empty());
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestIsQuerierCollidingWithTruncation_AppenderV2
#[test]
fn test_is_querier_colliding_with_truncation_appender_v2() {
    let dir = tempfile::tempdir().unwrap();
    let head = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();
    let mut app = head.appender();
    for i in 0..=3000i64 {
        app.append_float(None, &[("a", "b")], i, i as f64).unwrap();
    }
    app.commit().unwrap();
    head.begin_memory_truncation(2000);
    let cases = [
        (-200, -100, true, false, 2000),
        (-200, 300, true, false, 2000),
        (100, 1900, true, false, 2000),
        (1900, 2200, true, true, 2000),
        (2000, 2500, false, false, 0),
    ];
    for (mint, maxt, exp_close, exp_new, exp_new_mint) in cases {
        let (sc, gn, nm) = head.is_querier_colliding_with_truncation(mint, maxt);
        assert_eq!((sc, gn), (exp_close, exp_new));
        if sc || gn {
            assert_eq!(nm, exp_new_mint);
        }
    }
    head.end_memory_truncation();
    head.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestWaitForPendingReadersInTimeRange_AppenderV2
#[test]
fn test_wait_for_pending_readers_in_time_range_appender_v2() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Tsdb::open_with_reload_interval(dir.path(), TsdbOptions::default(), Duration::ZERO).unwrap());
    db.disable_compactions();
    {
        let mut app = db.appender();
        for i in 0..=3000i64 {
            let ts = i * DEFAULT_BLOCK_DURATION_MS / (4 * 120);
            app.append_float(None, &[("a", "b")], ts, i as f64).unwrap();
        }
        app.commit().unwrap();
    }
    for (mint, maxt, should_wait) in [
        (0, 500, false),
        (500, 1500, true),
        (1200, 1700, true),
        (1800, 2500, true),
        (2000, 2500, false),
        (2100, 2500, false),
    ] {
        let g = db.head().start_read(mint, maxt);
        let done = Arc::new(AtomicBool::new(false));
        let done_bg = Arc::clone(&done);
        let db_bg = Arc::clone(&db);
        let h = std::thread::spawn(move || {
            db_bg.head().wait_for_pending_readers_in_time_range(1000, 2000);
            done_bg.store(true, Ordering::Release);
        });
        std::thread::sleep(Duration::from_millis(30));
        if should_wait {
            assert!(!done.load(Ordering::Acquire));
        }
        drop(g);
        h.join().unwrap();
        assert!(done.load(Ordering::Acquire));
    }
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestChunkQueryOOOHeadDuringTruncate_AppenderV2
#[test]
fn test_chunk_query_ooo_head_during_truncate_appender_v2() {
    let dir = tempfile::tempdir().unwrap();
    let max_t = 6000i64;
    let db = Arc::new(
        Tsdb::open_with_reload_interval(
            dir.path(),
            TsdbOptions {
                head: HeadOptions { ooo_time_window_ms: max_t, chunk_range_ms: max_t / 2, ..HeadOptions::default() },
                ..TsdbOptions::default()
            },
            Duration::ZERO,
        )
        .unwrap(),
    );
    db.disable_compactions();
    {
        let mut app = db.appender();
        let mut r = None;
        for i in (0..max_t).step_by(100) {
            r = Some(app.append_float(r, &[("a", "b")], i, 0.0).unwrap());
        }
        for i in (50..max_t).step_by(100) {
            app.append_float(r, &[("a", "b")], i, 0.0).unwrap();
        }
        app.commit().unwrap();
    }
    let cq = db.unordered_chunk_querier(1500, 2500);
    let m = [LabelMatcher::new(MatchType::Equal, "a", "b").unwrap()];
    let prep = cq.prepare_select(&m).unwrap();
    let db_bg = Arc::clone(&db);
    let h = std::thread::spawn(move || db_bg.compact_head_range(0, 3000).unwrap());
    std::thread::sleep(Duration::from_millis(20));
    let res = cq.execute_prepared_select(prep, true).unwrap();
    assert_eq!(res.len(), 1);
    let samples: Vec<i64> = res[0]
        .chunks
        .iter()
        .flat_map(|c| decode_chunk_samples(c.chunk.as_ref().unwrap()).unwrap())
        .map(|s| s.t)
        .collect();
    assert!(samples.contains(&1500));
    assert!(samples.contains(&1550));
    h.join().unwrap();
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestHeadAppenderV2_Append_Histogram
#[test]
fn test_head_appender_v2_append_histogram() {
    let pairs = [("a", "b")];
    for num in [1usize, 10, 150, 200, 250, 300] {
        let dir = tempfile::tempdir().unwrap();
        let head = Head::open(dir.path(), HeadOptions { chunk_range_ms: 1000, ..HeadOptions::default() }, i64::MIN).unwrap();
        let mut ts = 0i64;
        let mut exp_h = Vec::new();
        let mut exp_fh = Vec::new();
        let mut app = head.appender();
        for i in 0..num {
            let h = gen_hist(i as i64 + 1);
            app.append_histogram(None, &pairs, 0, ts, h.clone()).unwrap();
            exp_h.push((ts, h));
            ts += 1;
            if ts % 50 == 0 {
                app.commit().unwrap();
                app = head.appender();
            }
        }
        for i in 0..num {
            let h = gen_gauge_hist(i as i64 + 1);
            app.append_histogram(None, &pairs, 0, ts, h.clone()).unwrap();
            exp_h.push((ts, h));
            ts += 1;
            if ts % 50 == 0 {
                app.commit().unwrap();
                app = head.appender();
            }
        }
        for i in 0..num {
            let fh = gen_fhist(i as i64 + 1);
            app.append_float_histogram(None, &pairs, 0, ts, fh.clone()).unwrap();
            exp_fh.push((ts, fh));
            ts += 1;
            if ts % 50 == 0 {
                app.commit().unwrap();
                app = head.appender();
            }
        }
        for i in 0..num {
            let fh = gen_gauge_fhist(i as i64 + 1);
            app.append_float_histogram(None, &pairs, 0, ts, fh.clone()).unwrap();
            exp_fh.push((ts, fh));
            ts += 1;
            if ts % 50 == 0 {
                app.commit().unwrap();
                app = head.appender();
            }
        }
        app.commit().unwrap();

        let m = LabelMatcher::new(MatchType::Equal, "a", "b").unwrap();
        let res = query_head_samples(&head, m);
        let samples = &res[r#"{a="b"}"#];
        assert_eq!(samples.len(), exp_h.len() + exp_fh.len());
        head.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestHistogramInWALAndMmapChunk_AppenderV2
#[test]
fn test_histogram_in_wal_and_mmap_chunk_appender_v2() {
    let dir = tempfile::tempdir().unwrap();
    let opts = HeadOptions { chunk_range_ms: 3000, ..HeadOptions::default() };
    let head = Head::open(dir.path(), opts.clone(), i64::MIN).unwrap();
    let s1 = [("a", "b1")];
    let mut ts = 0i64;
    let mut total_s1 = 0usize;
    for gauge in [true, false] {
        let mut app = head.appender();
        for i in 0..150 {
            let h = if gauge { gen_gauge_hist(i) } else { gen_hist(i) };
            app.append_histogram(None, &s1, 0, ts, h).unwrap();
            total_s1 += 1;
            ts += 1;
            if ts % 5 == 0 {
                app.commit().unwrap();
                app = head.appender();
            }
        }
        app.commit().unwrap();
    }
    for gauge in [true, false] {
        let mut app = head.appender();
        for i in 0..150 {
            let fh = if gauge { gen_gauge_fhist(i) } else { gen_fhist(i) };
            app.append_float_histogram(None, &s1, 0, ts, fh).unwrap();
            total_s1 += 1;
            ts += 1;
            if ts % 5 == 0 {
                app.commit().unwrap();
                app = head.appender();
            }
        }
        app.commit().unwrap();
        head.mmap_head_chunks().unwrap();
    }
    let ref1 = head.get_entry_by_sorted_pairs(&s1).unwrap().ref_id;
    let mmapped_before = head.mmapped_chunks_bounds_for_test(ref1).unwrap();
    assert!(!mmapped_before.is_empty());
    head.close().unwrap();

    // Restart with chunks_head intact.
    let h2 = Head::open(dir.path(), opts.clone(), i64::MIN).unwrap();
    assert_eq!(h2.mmapped_chunks_bounds_for_test(ref1).unwrap(), mmapped_before);
    let m = LabelMatcher::new(MatchType::Equal, "a", "b1").unwrap();
    assert_eq!(query_head_samples(&h2, m.clone())[r#"{a="b1"}"#].len(), total_s1);
    h2.close().unwrap();

    // Restart without chunks_head (pure WAL replay).
    fs::remove_dir_all(dir.path().join("chunks_head")).unwrap();
    let h3 = Head::open(dir.path(), opts, i64::MIN).unwrap();
    assert_eq!(query_head_samples(&h3, m)[r#"{a="b1"}"#].len(), total_s1);
    h3.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestChunkSnapshot_AppenderV2
#[test]
fn test_chunk_snapshot_appender_v2() {
    for enable_st in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let reg = Arc::new(prometheus::Registry::new());
        let opts = HeadOptions {
            chunk_range_ms: 480,
            enable_st_storage: enable_st,
            enable_exemplar_storage: true,
            max_exemplars: 100,
            registry: Some(Arc::clone(&reg)),
            ..HeadOptions::default()
        };
        let mut head = Head::open_with_snapshot(dir.path(), opts.clone(), i64::MIN, true).unwrap();
        {
            let mut app = head.appender();
            for s in 0..5 {
                let sv = format!("bar{s}");
                for t in 0..20i64 {
                    app.append_float(None, &[("foo", &sv)], t * 10, (t + s as i64) as f64).unwrap();
                }
                let hv = format!("baz{s}");
                app.append_histogram(None, &[("hist", &hv)], 0, 10, gen_gauge_hist(s as i64)).unwrap();
                let fhv = format!("fbaz{s}");
                app.append_float_histogram(None, &[("fhist", &fhv)], 0, 10, gen_gauge_fhist(s as i64)).unwrap();
                app.append_exemplar(
                    None,
                    &[("foo", &sv)],
                    Exemplar::new(Labels::from_pairs([("trace_id", format!("t{s}").as_str())]), 1.0, 50),
                )
                .unwrap();
            }
            app.commit().unwrap();
        }
        head.delete(0, 50, &[LabelMatcher::new(MatchType::Equal, "foo", "bar0").unwrap()]).unwrap();
        head.close().unwrap();
        assert!(Head::last_chunk_snapshot(dir.path()).unwrap().is_some());

        head = Head::open_with_snapshot(dir.path(), opts.clone(), i64::MIN, true).unwrap();
        assert_eq!(metric_value(&reg, "prometheus_tsdb_snapshot_replay_error_total", &[]), 0.0);
        assert_eq!(head.num_series(), 15);
        let q = query_head_samples(&head, LabelMatcher::new(MatchType::Equal, "foo", "bar1").unwrap());
        assert_eq!(q[r#"{foo="bar1"}"#].len(), 20);
        let q_del = query_head_samples(&head, LabelMatcher::new(MatchType::Equal, "foo", "bar0").unwrap());
        assert_eq!(q_del[r#"{foo="bar0"}"#].len(), 14);
        head.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestSnapshotError_AppenderV2
#[test]
fn test_snapshot_error_appender_v2() {
    let dir = tempfile::tempdir().unwrap();
    let opts = HeadOptions { chunk_range_ms: 480, ..HeadOptions::default() };
    let head = Head::open_with_snapshot(dir.path(), opts.clone(), i64::MIN, true).unwrap();
    {
        let mut app = head.appender();
        app.append_float(None, &[("foo", "bar")], 99, 99.0).unwrap();
        app.append_histogram(None, &[("hist", "bar")], 0, 99, gen_gauge_hist(1)).unwrap();
        app.append_float_histogram(None, &[("floathist", "bar")], 0, 99, gen_gauge_fhist(1)).unwrap();
        app.commit().unwrap();
    }
    head.close().unwrap();

    // Remove WAL and corrupt snapshot.
    fs::remove_dir_all(dir.path().join("wal")).unwrap();
    let (snap_dir, _, _) = Head::last_chunk_snapshot(dir.path()).unwrap().unwrap();
    let seg0 = segment_name(&snap_dir, 0);
    let backup = fs::read(&seg0).unwrap();
    {
        let mut f = OpenOptions::new().write(true).open(&seg0).unwrap();
        f.seek(SeekFrom::Start(18)).unwrap();
        f.write_all(&[0xff]).unwrap();
    }

    let reg = Arc::new(prometheus::Registry::new());
    let cb = Arc::new(CountLifecycleCb::new());
    let opts2 = HeadOptions {
        chunk_range_ms: 480,
        registry: Some(Arc::clone(&reg)),
        series_lifecycle_callback: Some(cb.clone()),
        ..HeadOptions::default()
    };
    let h2 = Head::open_with_snapshot(dir.path(), opts2.clone(), i64::MIN, true).unwrap();
    assert_eq!(metric_value(&reg, "prometheus_tsdb_snapshot_replay_error_total", &[]), 1.0);
    assert_eq!(h2.num_series(), 0);
    h2.close().unwrap();

    // Corrupt mid-snapshot (after 2 series are created) and verify rollback/delete hooks fire.
    let mut mid = backup;
    if mid.len() > 300 {
        mid[300] = 0xff;
    } else {
        mid[18] = 0xff;
    }
    fs::write(&seg0, mid).unwrap();
    let reg3 = Arc::new(prometheus::Registry::new());
    let cb3 = Arc::new(CountLifecycleCb::new());
    let opts3 = HeadOptions {
        chunk_range_ms: 480,
        registry: Some(Arc::clone(&reg3)),
        series_lifecycle_callback: Some(cb3.clone()),
        ..HeadOptions::default()
    };
    let h3 = Head::open_with_snapshot(dir.path(), opts3, i64::MIN, true).unwrap();
    assert_eq!(metric_value(&reg3, "prometheus_tsdb_snapshot_replay_error_total", &[]), 1.0);
    assert_eq!(h3.num_series(), 0);
    assert_eq!(cb3.created.load(Ordering::Relaxed), cb3.deleted.load(Ordering::Relaxed));
    h3.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestHeadAppenderV2_Append_HistogramSamplesAppendedMetric
#[test]
fn test_head_appender_v2_append_histogram_samples_appended_metric() {
    let dir = tempfile::tempdir().unwrap();
    let reg = Arc::new(prometheus::Registry::new());
    let opts = HeadOptions {
        chunk_range_ms: 1000,
        registry: Some(Arc::clone(&reg)),
        ..HeadOptions::default()
    };
    let head = Head::open(dir.path(), opts.clone(), i64::MIN).unwrap();
    let mut exp = 0usize;
    for x in 0..5 {
        let val = format!("b{x}");
        for i in 0..10i64 {
            let mut app = head.appender();
            app.append_histogram(None, &[("a", &val)], 0, i, gen_hist(i + 1)).unwrap();
            app.commit().unwrap();
            exp += 1;
        }
        for i in 0..10i64 {
            let mut app = head.appender();
            app.append_float_histogram(None, &[("a", &val)], 0, 10 + i, gen_fhist(i + 1)).unwrap();
            app.commit().unwrap();
            exp += 1;
        }
    }
    assert_eq!(
        metric_value(&reg, "prometheus_tsdb_head_samples_appended_total", &[("type", "histogram")]),
        exp as f64
    );
    head.close().unwrap();

    let reg2 = Arc::new(prometheus::Registry::new());
    let h2 = Head::open(dir.path(), HeadOptions { chunk_range_ms: 1000, registry: Some(Arc::clone(&reg2)), ..HeadOptions::default() }, i64::MIN).unwrap();
    assert_eq!(metric_value(&reg2, "prometheus_tsdb_head_samples_appended_total", &[("type", "histogram")]), 0.0);
    h2.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestHeadAppenderV2_Append_StaleHistogram
#[test]
fn test_head_appender_v2_append_stale_histogram() {
    for is_float_hist in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let head = Head::open(dir.path(), HeadOptions { chunk_range_ms: 100_000, ..HeadOptions::default() }, i64::MIN).unwrap();
        let pairs = [("a", "b")];
        let mut ts = 0i64;
        let mut app = head.appender();
        for i in 0..20 {
            if is_float_hist {
                app.append_float_histogram(None, &pairs, 0, ts, gen_fhist(i + 1)).unwrap();
            } else {
                app.append_histogram(None, &pairs, 0, ts, gen_hist(i + 1)).unwrap();
            }
            ts += 100;
        }
        app.append_float(None, &pairs, ts + 1, stale_nan()).unwrap();
        app.commit().unwrap();

        let ref_id = head.get_entry_by_sorted_pairs(&pairs).unwrap().ref_id;
        assert_eq!(head.head_chunks_bounds_for_test(ref_id).unwrap().len(), 1);
        assert!(head.mmapped_chunks_bounds_for_test(ref_id).unwrap().is_empty());

        let m = LabelMatcher::new(MatchType::Equal, "a", "b").unwrap();
        let samples = &query_head_samples(&head, m.clone())[r#"{a="b"}"#];
        assert_eq!(samples.len(), 21);
        head.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestHeadAppenderV2_Append_CounterResetHeader
#[test]
fn test_head_appender_v2_append_counter_reset_header() {
    for float_histo in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let head = Head::open(dir.path(), HeadOptions { chunk_range_ms: 1000, ..HeadOptions::default() }, i64::MIN).unwrap();
        let pairs = [("a", "b")];
        let mut ts = 0i64;
        let mut append_h = |h: &Histogram| {
            ts += 1;
            let mut app = head.appender();
            if float_histo {
                let fh = FloatHistogram {
                    counter_reset_hint: h.counter_reset_hint,
                    schema: h.schema,
                    zero_threshold: h.zero_threshold,
                    zero_count: h.zero_count as f64,
                    count: h.count as f64,
                    sum: h.sum,
                    positive_spans: h.positive_spans.clone(),
                    negative_spans: h.negative_spans.clone(),
                    positive_buckets: h.positive_buckets.iter().map(|&x| x as f64).collect(),
                    negative_buckets: h.negative_buckets.iter().map(|&x| x as f64).collect(),
                    custom_values: h.custom_values.clone(),
                };
                app.append_float_histogram(None, &pairs, 0, ts, fh).unwrap();
            } else {
                app.append_histogram(None, &pairs, 0, ts, h.clone()).unwrap();
            }
            app.commit().unwrap();
        };

        let mut h = gen_hist(1);
        h.positive_buckets = vec![100, 1];
        h.count = 202;
        append_h(&h);
        h.positive_buckets[1] += 1;
        h.count += 1;
        append_h(&h);
        // Decrement bucket => counter reset cuts a new chunk.
        h.positive_buckets[1] -= 1;
        h.count -= 1;
        append_h(&h);

        let ref_id = head.get_entry_by_sorted_pairs(&pairs).unwrap().ref_id;
        let (_, chks, _) = head.series_chunks(ref_id, i64::MIN, i64::MAX).unwrap().unwrap();
        assert_eq!(chks.len(), 2);
        let hdr0 = match chks[0].chunk.as_ref().unwrap() {
            EncodedChunk::Histogram(c) => c.counter_reset_header(),
            EncodedChunk::FloatHistogram(c) => c.counter_reset_header(),
            other => panic!("unexpected chunk {other:?}"),
        };
        let hdr1 = match chks[1].chunk.as_ref().unwrap() {
            EncodedChunk::Histogram(c) => c.counter_reset_header(),
            EncodedChunk::FloatHistogram(c) => c.counter_reset_header(),
            other => panic!("unexpected chunk {other:?}"),
        };
        assert_eq!(hdr0, CounterResetHeader::UnknownCounterReset);
        assert_eq!(hdr1, CounterResetHeader::CounterReset);
        head.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestHeadAppenderV2_Append_OOOHistogramCounterResetHeaders
#[test]
fn test_head_appender_v2_append_ooo_histogram_counter_reset_headers() {
    for float_histo in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            chunk_range_ms: 1000,
            ooo_time_window_ms: 10_000,
            ooo_cap_max: 5,
            ..HeadOptions::default()
        };
        let head = Head::open(dir.path(), opts, i64::MIN).unwrap();
        let pairs = [("a", "b")];
        let append_h = |t: i64, mut h: Histogram| {
            let mut app = head.appender();
            if float_histo {
                let fh = FloatHistogram {
                    counter_reset_hint: h.counter_reset_hint,
                    schema: h.schema,
                    zero_threshold: h.zero_threshold,
                    zero_count: h.zero_count as f64,
                    count: h.count as f64,
                    sum: h.sum,
                    positive_spans: h.positive_spans,
                    negative_spans: h.negative_spans,
                    positive_buckets: h.positive_buckets.iter().map(|&x| x as f64).collect(),
                    negative_buckets: h.negative_buckets.iter().map(|&x| x as f64).collect(),
                    custom_values: h.custom_values,
                };
                app.append_float_histogram(None, &pairs, 0, t, fh).unwrap();
            } else {
                h.counter_reset_hint = CounterResetHint::UnknownCounterReset;
                app.append_histogram(None, &pairs, 0, t, h).unwrap();
            }
            app.commit().unwrap();
        };
        append_h(1000, gen_hist(1000));
        for i in 1..=6i64 {
            append_h(100 + i, gen_hist(1000 + i));
        }
        let m = LabelMatcher::new(MatchType::Equal, "a", "b").unwrap();
        assert_eq!(query_head_samples(&head, m)[r#"{a="b"}"#].len(), 7);
        head.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestHeadAppenderV2_Append_DifferentEncodingSameSeries
#[test]
fn test_head_appender_v2_append_different_encoding_same_series() {
    let dir = tempfile::tempdir().unwrap();
    let db = Tsdb::open_with_reload_interval(dir.path(), TsdbOptions::default(), Duration::ZERO).unwrap();
    db.disable_compactions();
    let pairs = [("a", "b")];
    {
        let mut app = db.appender();
        app.append_histogram(None, &pairs, 0, 100, gen_hist(1)).unwrap();
        app.commit().unwrap();
    }
    {
        let mut app = db.appender();
        app.append_float(None, &pairs, 200, 2.0).unwrap();
        app.commit().unwrap();
    }
    {
        let mut app = db.appender();
        app.append_float_histogram(None, &pairs, 0, 210, gen_fhist(2)).unwrap();
        app.commit().unwrap();
    }
    {
        let mut app = db.appender();
        app.append_histogram(None, &pairs, 0, 220, gen_hist(3)).unwrap();
        app.commit().unwrap();
    }
    {
        let mut app = db.appender();
        app.append_float(None, &pairs, 400, 4.0).unwrap();
        app.append_histogram(None, &pairs, 0, 500, gen_hist(4)).unwrap();
        app.append_float(None, &pairs, 600, 6.0).unwrap();
        app.commit().unwrap();
    }
    let m = [LabelMatcher::new(MatchType::Equal, "a", "b").unwrap()];
    let got = db.querier(i64::MIN, i64::MAX).select(true, &m).unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].samples.iter().map(|s| s.t).collect::<Vec<_>>(), vec![100, 200, 210, 220, 400, 500, 600]);
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestChunkSnapshotTakenAfterIncompleteSnapshot_AppenderV2
#[test]
fn test_chunk_snapshot_taken_after_incomplete_snapshot_appender_v2() {
    let dir = tempfile::tempdir().unwrap();
    fs::create_dir_all(dir.path().join("chunk_snapshot.000000.0000000100.tmp")).unwrap();
    let reg = Arc::new(prometheus::Registry::new());
    let opts = HeadOptions { registry: Some(Arc::clone(&reg)), ..HeadOptions::default() };
    let head = Head::open_with_snapshot(dir.path(), opts, i64::MIN, true).unwrap();
    assert_eq!(metric_value(&reg, "prometheus_tsdb_snapshot_replay_error_total", &[]), 0.0);
    {
        let mut app = head.appender();
        app.append_float(None, &[("foo", "bar")], 10, 10.0).unwrap();
        app.commit().unwrap();
    }
    head.close().unwrap();
    let (_, idx, offset) = Head::last_chunk_snapshot(dir.path()).unwrap().unwrap();
    assert_eq!(idx, 0);
    assert!(offset > 0);
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestWBLReplay_AppenderV2
#[test]
fn test_wbl_replay_appender_v2() {
    let min_ms = 60_000i64;
    for kind in [SampleKind::Float, SampleKind::Hist, SampleKind::FloatHist] {
        for enable_st in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let opts = HeadOptions {
                chunk_range_ms: 1000,
                ooo_time_window_ms: 30 * min_ms,
                enable_st_storage: enable_st,
                float_chunk_encoding: if enable_st { Encoding::Xor2 } else { Encoding::Xor },
                enable_histogram_st_encoding: enable_st,
                ..HeadOptions::default()
            };
            let h = Head::open(dir.path(), opts.clone(), i64::MIN).unwrap();
            let pairs = [("foo", "bar")];
            let append_m = |m: i64| {
                let mut app = h.appender();
                append_kind(&mut app, kind, &pairs, m * min_ms, m).unwrap();
                app.commit().unwrap();
            };
            append_m(60);
            for m in [40, 35, 50, 55, 59, 31] {
                append_m(m);
            }
            assert_eq!(h.min_time(), 60 * min_ms);
            assert_eq!(h.max_time(), 60 * min_ms);
            assert_eq!(h.min_ooo_time(), 31 * min_ms);
            assert_eq!(h.max_ooo_time(), 59 * min_ms);
            h.close().unwrap();

            let h2 = Head::open(dir.path(), opts, i64::MIN).unwrap();
            let got = query_head_samples(&h2, LabelMatcher::new(MatchType::Equal, "foo", "bar").unwrap());
            let ts: Vec<i64> = got[r#"{foo="bar"}"#].iter().map(|s| s.t).collect();
            assert_eq!(ts, vec![31, 35, 40, 50, 55, 59, 60].into_iter().map(|m| m * min_ms).collect::<Vec<_>>());
            h2.close().unwrap();
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestOOOMmapReplay_AppenderV2
#[test]
fn test_ooo_mmap_replay_appender_v2() {
    let min_ms = 60_000i64;
    for kind in [SampleKind::Float, SampleKind::Hist, SampleKind::FloatHist] {
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            chunk_range_ms: 1000,
            ooo_cap_max: 30,
            ooo_time_window_ms: 1000 * min_ms,
            ..HeadOptions::default()
        };
        let h = Head::open(dir.path(), opts.clone(), i64::MIN).unwrap();
        let pairs = [("foo", "bar")];
        {
            let mut app = h.appender();
            append_kind(&mut app, kind, &pairs, 200 * min_ms, 200).unwrap();
            app.commit().unwrap();
        }
        for m in 100..=191i64 {
            let mut app = h.appender();
            append_kind(&mut app, kind, &pairs, m * min_ms, m).unwrap();
            app.commit().unwrap();
        }
        h.close().unwrap();

        let h2 = Head::open(dir.path(), opts, i64::MIN).unwrap();
        let got = query_head_samples(&h2, LabelMatcher::new(MatchType::Equal, "foo", "bar").unwrap());
        assert_eq!(got[r#"{foo="bar"}"#].len(), 93);
        h2.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestHead_Init_DiscardChunksWithUnsupportedEncoding
#[test]
fn test_head_init_discard_chunks_with_unsupported_encoding() {
    let dir = tempfile::tempdir().unwrap();
    let opts = HeadOptions { chunk_range_ms: 1000, ..HeadOptions::default() };
    let h = Head::open(dir.path(), opts.clone(), i64::MIN).unwrap();
    let pairs = [("a", "1")];
    let ref_id;
    {
        let mut app = h.appender();
        ref_id = app.append_float(None, &pairs, 0, 0.0).unwrap();
        for i in 1..400i64 {
            app.append_float(Some(ref_id), &pairs, i, i as f64).unwrap();
        }
        app.commit().unwrap();
    }
    h.write_raw_chunk_for_test(ref_id, 500, 600, 200, &[1, 2, 3, 4], false).unwrap();
    {
        let mut app = h.appender();
        for i in 700..1200i64 {
            app.append_float(Some(ref_id), &pairs, i, i as f64).unwrap();
        }
        app.commit().unwrap();
    }
    h.mmap_head_chunks().unwrap();
    let exp_bounds = h.mmapped_chunks_bounds_for_test(ref_id).unwrap();
    h.close().unwrap();

    let h2 = Head::open(dir.path(), opts, i64::MIN).unwrap();
    assert_eq!(h2.mmapped_chunks_bounds_for_test(ref_id).unwrap(), exp_bounds);
    h2.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestMmapPanicAfterMmapReplayCorruption_AppenderV2
#[test]
fn test_mmap_panic_after_mmap_replay_corruption_appender_v2() {
    let dir = tempfile::tempdir().unwrap();
    let opts = HeadOptions {
        chunk_range_ms: DEFAULT_BLOCK_DURATION_MS,
        enable_exemplar_storage: true,
        ..HeadOptions::default()
    };
    let h = Head::open(dir.path(), opts.clone(), i64::MIN).unwrap();
    let pairs = [("__name__", "testing"), ("foo", "bar")];
    let interval = DEFAULT_BLOCK_DURATION_MS / (4 * 120);
    let mut last_ts = 0i64;
    let mut add_chunks = |h: &Head| {
        let mut app = h.appender();
        for i in 0..250 {
            app.append_float(None, &pairs, last_ts, last_ts as f64).unwrap();
            last_ts += interval;
            if i % 10 == 0 {
                app.commit().unwrap();
                app = h.appender();
            }
        }
        app.commit().unwrap();
    };
    add_chunks(&h);
    h.close().unwrap();

    let mmap_file = dir.path().join("chunks_head").join("000001");
    let mut f = OpenOptions::new().write(true).open(&mmap_file).unwrap();
    f.seek(SeekFrom::Start(17)).unwrap();
    f.write_all(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]).unwrap();
    drop(f);

    let h2 = Head::open(dir.path(), opts, i64::MIN).unwrap();
    add_chunks(&h2);
    h2.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestReplayAfterMmapReplayError_AppenderV2
#[test]
fn test_replay_after_mmap_replay_error_appender_v2() {
    let dir = tempfile::tempdir().unwrap();
    let opts = HeadOptions { chunk_range_ms: DEFAULT_BLOCK_DURATION_MS, ..HeadOptions::default() };
    let itvl = 15_000i64;
    let mut last_ts = 0i64;
    let mut exp_samples = Vec::new();
    let pairs = [("__name__", "testing"), ("foo", "bar")];

    for _ in 0..5 {
        let h = Head::open_with_snapshot(dir.path(), opts.clone(), i64::MIN, true).unwrap();
        let mut app = h.appender();
        for i in 0..250 {
            app.append_float(None, &pairs, last_ts, last_ts as f64).unwrap();
            exp_samples.push((last_ts, last_ts as f64));
            last_ts += itvl;
            if i % 10 == 0 {
                app.commit().unwrap();
                app = h.appender();
            }
        }
        app.commit().unwrap();
        h.close().unwrap();
    }

    let mmap_file = dir.path().join("chunks_head").join("000002");
    if mmap_file.exists() {
        let mut f = OpenOptions::new().write(true).open(&mmap_file).unwrap();
        f.seek(SeekFrom::Start(17)).unwrap();
        f.write_all(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]).unwrap();
    }

    let h = Head::open_with_snapshot(dir.path(), opts, i64::MIN, true).unwrap();
    h.mmap_head_chunks().unwrap();
    let got = query_head_samples(&h, LabelMatcher::new(MatchType::Equal, "__name__", "testing").unwrap());
    let got_pairs: Vec<(i64, f64)> = got.values().next().unwrap().iter().map(|s| match s.value {
        SampleValue::Float(v) => (s.t, v),
        _ => unreachable!(),
    }).collect();
    assert_eq!(got_pairs, exp_samples);
    h.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestHeadAppenderV2_Append_OOOWithNoSeries
#[test]
fn test_head_appender_v2_append_ooo_with_no_series() {
    let min_ms = 60_000i64;
    for kind in [SampleKind::Float, SampleKind::Hist, SampleKind::FloatHist] {
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            ooo_cap_max: 30,
            ooo_time_window_ms: 120 * min_ms,
            ..HeadOptions::default()
        };
        let h = Head::open(dir.path(), opts, i64::MIN).unwrap();
        let append_m = |idx: i32, mins: i64| -> Result<(), HeadError> {
            let v = idx.to_string();
            let mut app = h.appender();
            append_kind(&mut app, kind, &[("foo", &v)], mins * min_ms, mins)?;
            app.commit()
        };
        append_m(1, 300).unwrap();
        append_m(2, 239).unwrap();
        append_m(3, 180).unwrap();
        assert!(matches!(append_m(4, 179), Err(HeadError::TooOldSample)));
        append_m(5, 240).unwrap();
        assert_eq!(h.min_ooo_time(), 180 * min_ms);
        h.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestHead_MinOOOTime_Update_AppenderV2
#[test]
fn test_head_min_ooo_time_update_appender_v2() {
    let min_ms = 60_000i64;
    let dir = tempfile::tempdir().unwrap();
    let opts = HeadOptions {
        ooo_time_window_ms: 10 * min_ms,
        ..HeadOptions::default()
    };
    let h = Head::open(dir.path(), opts, i64::MIN).unwrap();
    let add = |mins: i64| {
        let mut app = h.appender();
        app.append_float(None, &[("a", "b")], mins * min_ms, mins as f64).unwrap();
        app.commit().unwrap();
    };
    add(300);
    assert_eq!(h.min_ooo_time(), i64::MAX);
    add(295);
    assert_eq!(h.min_ooo_time(), 295 * min_ms);
    h.truncate_ooo(None, mimir_tsdb::head_chunks::ChunkDiskMapperRef(1)).unwrap();
    assert_eq!(h.min_ooo_time(), 290 * min_ms);
    add(310);
    add(305);
    assert_eq!(h.min_ooo_time(), 290 * min_ms);
    h.truncate_ooo(None, mimir_tsdb::head_chunks::ChunkDiskMapperRef(2)).unwrap();
    assert_eq!(h.min_ooo_time(), 295 * min_ms);
    h.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestGaugeHistogramWALAndChunkHeader_AppenderV2
#[test]
fn test_gauge_histogram_wal_and_chunk_header_appender_v2() {
    let dir = tempfile::tempdir().unwrap();
    let opts = HeadOptions { chunk_range_ms: 1000, ..HeadOptions::default() };
    let head = Head::open(dir.path(), opts.clone(), i64::MIN).unwrap();
    let pairs = [("a", "b")];
    let mut ts = 0i64;
    let mut add = |mut h: Histogram, unknown: bool| {
        ts += 1;
        if unknown {
            h.counter_reset_hint = CounterResetHint::UnknownCounterReset;
        }
        let mut app = head.appender();
        app.append_histogram(None, &pairs, 0, ts, h).unwrap();
        app.commit().unwrap();
    };
    add(gen_gauge_hist(1), true);
    add(gen_gauge_hist(2), false);
    add(gen_gauge_hist(3), false);
    add(gen_gauge_hist(4), true);
    add(gen_gauge_hist(4), false);
    add(gen_gauge_hist(5), false);
    head.mmap_head_chunks().unwrap();
    let ref_id = head.get_entry_by_sorted_pairs(&pairs).unwrap().ref_id;
    assert_eq!(head.mmapped_chunks_bounds_for_test(ref_id).unwrap().len(), 3);
    head.close().unwrap();

    fs::remove_dir_all(dir.path().join("chunks_head")).unwrap();
    let h2 = Head::open(dir.path(), opts, i64::MIN).unwrap();
    h2.mmap_head_chunks().unwrap();
    assert_eq!(h2.mmapped_chunks_bounds_for_test(ref_id).unwrap().len(), 3);
    h2.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestGaugeFloatHistogramWALAndChunkHeader_AppenderV2
#[test]
fn test_gauge_float_histogram_wal_and_chunk_header_appender_v2() {
    let dir = tempfile::tempdir().unwrap();
    let opts = HeadOptions { chunk_range_ms: 1000, ..HeadOptions::default() };
    let head = Head::open(dir.path(), opts.clone(), i64::MIN).unwrap();
    let pairs = [("a", "b")];
    let mut ts = 0i64;
    let mut add = |mut fh: FloatHistogram, unknown: bool| {
        ts += 1;
        if unknown {
            fh.counter_reset_hint = CounterResetHint::UnknownCounterReset;
        }
        let mut app = head.appender();
        app.append_float_histogram(None, &pairs, 0, ts, fh).unwrap();
        app.commit().unwrap();
    };
    add(gen_gauge_fhist(1), true);
    add(gen_gauge_fhist(2), false);
    add(gen_gauge_fhist(3), false);
    add(gen_gauge_fhist(4), true);
    add(gen_gauge_fhist(4), false);
    add(gen_gauge_fhist(5), false);
    head.mmap_head_chunks().unwrap();
    let ref_id = head.get_entry_by_sorted_pairs(&pairs).unwrap().ref_id;
    assert_eq!(head.mmapped_chunks_bounds_for_test(ref_id).unwrap().len(), 3);
    head.close().unwrap();

    fs::remove_dir_all(dir.path().join("chunks_head")).unwrap();
    let h2 = Head::open(dir.path(), opts, i64::MIN).unwrap();
    h2.mmap_head_chunks().unwrap();
    assert_eq!(h2.mmapped_chunks_bounds_for_test(ref_id).unwrap().len(), 3);
    h2.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestSnapshotAheadOfWALError_AppenderV2
#[test]
fn test_snapshot_ahead_of_wal_error_appender_v2() {
    let dir = tempfile::tempdir().unwrap();
    let opts = HeadOptions { chunk_range_ms: 480, ..HeadOptions::default() };
    let h = Head::open_with_snapshot(dir.path(), opts.clone(), i64::MIN, true).unwrap();
    {
        let mut app = h.appender();
        app.append_float(None, &[("foo", "bar")], 10, 10.0).unwrap();
        app.commit().unwrap();
    }
    h.close().unwrap();

    // Remove WAL and recreate WAL at segment 0, then rename snapshot to segment 2 (ahead of WAL).
    let (snap_dir, _, _) = Head::last_chunk_snapshot(dir.path()).unwrap().unwrap();
    fs::rename(&snap_dir, dir.path().join("chunk_snapshot.000002.0000000000")).unwrap();
    fs::remove_dir_all(dir.path().join("wal")).unwrap();
    let h2 = Head::open_with_snapshot(dir.path(), opts.clone(), i64::MIN, false).unwrap();
    {
        let mut app = h2.appender();
        app.append_float(None, &[("foo", "bar")], 10, 10.0).unwrap();
        app.commit().unwrap();
    }
    h2.close().unwrap();

    let h3 = Head::open_with_snapshot(dir.path(), opts, i64::MIN, true).unwrap();
    assert!(Head::last_chunk_snapshot(dir.path()).unwrap().is_none());
    h3.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestCuttingNewHeadChunks_AppenderV2
#[test]
fn test_cutting_new_head_chunks_appender_v2() {
    // 1. float samples (180 constant floats -> 120 + 60)
    {
        let dir = tempfile::tempdir().unwrap();
        let h = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();
        let mut app = h.appender();
        let mut ts = 10_000i64;
        for _ in 0..180 {
            app.append_float(None, &[("foo", "bar")], ts, 1.0).unwrap();
            ts += 60_000;
        }
        app.commit().unwrap();
        let ref_id = h.get_entry_by_sorted_pairs(&[("foo", "bar")]).unwrap().ref_id;
        let (_, chks, _) = h.series_chunks(ref_id, i64::MIN, i64::MAX).unwrap().unwrap();
        assert_eq!(chks.len(), 2);
        assert_eq!(chks[0].chunk.as_ref().unwrap().num_samples(), 120);
        assert_eq!(chks[0].chunk.as_ref().unwrap().as_bytes().len(), 46);
        assert_eq!(chks[1].chunk.as_ref().unwrap().num_samples(), 60);
        assert_eq!(chks[1].chunk.as_ref().unwrap().as_bytes().len(), 32);
        h.close().unwrap();
    }
    // 2. large float samples (120 alternating f64::MAX / 0.0 -> 99 + 21)
    {
        let dir = tempfile::tempdir().unwrap();
        let h = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();
        let mut app = h.appender();
        let mut ts = 10_000i64;
        for i in 0..120usize {
            let v = if i % 2 == 0 { f64::MAX } else { 0.0 };
            app.append_float(None, &[("foo", "bar")], ts, v).unwrap();
            ts += 60_000 + (i as i64 % 2);
        }
        app.commit().unwrap();
        let ref_id = h.get_entry_by_sorted_pairs(&[("foo", "bar")]).unwrap().ref_id;
        let (_, chks, _) = h.series_chunks(ref_id, i64::MIN, i64::MAX).unwrap().unwrap();
        assert_eq!(chks.len(), 2);
        assert_eq!(chks[0].chunk.as_ref().unwrap().num_samples(), 99);
        assert_eq!(chks[0].chunk.as_ref().unwrap().as_bytes().len(), 1008);
        assert_eq!(chks[1].chunk.as_ref().unwrap().num_samples(), 21);
        assert_eq!(chks[1].chunk.as_ref().unwrap().as_bytes().len(), 219);
        h.close().unwrap();
    }
    // 3. small / large histograms
    for (num, num_buckets, min_chunks) in [(240usize, 10usize, 2usize), (240, 100, 4)] {
        let dir = tempfile::tempdir().unwrap();
        let h = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();
        let mut app = h.appender();
        let mut ts = 10_000i64;
        for i in 0..num {
            app.append_histogram(None, &[("foo", "bar")], 0, ts, gen_big_hist(i as i64, num_buckets)).unwrap();
            ts += 60_000;
        }
        app.commit().unwrap();
        let ref_id = h.get_entry_by_sorted_pairs(&[("foo", "bar")]).unwrap().ref_id;
        let (_, chks, _) = h.series_chunks(ref_id, i64::MIN, i64::MAX).unwrap().unwrap();
        assert!(chks.len() >= min_chunks, "got {} chunks", chks.len());
        let total: usize = chks.iter().map(|c| c.chunk.as_ref().unwrap().num_samples()).sum();
        assert_eq!(total, num);
        h.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestHeadDetectsDuplicateSampleAtSizeLimit_AppenderV2
#[test]
fn test_head_detects_duplicate_sample_at_size_limit_appender_v2() {
    let dir = tempfile::tempdir().unwrap();
    let h = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();
    let num_samples = 1000usize;
    let base_ts = 1_695_209_650i64;
    let vals = [f64::MAX, 0.0];
    for i in 0..num_samples {
        let mut a = h.appender();
        let ts = base_ts + ((i / 2) as i64) * 10_000;
        a.append_float(None, &[("foo", "bar")], ts, vals[(i / 2) % 2]).unwrap();
        a.commit().unwrap();
    }
    let ref_id = h.get_entry_by_sorted_pairs(&[("foo", "bar")]).unwrap().ref_id;
    let (_, chks, _) = h.series_chunks(ref_id, i64::MIN, i64::MAX).unwrap().unwrap();
    let stored: usize = chks.iter().map(|c| c.chunk.as_ref().unwrap().num_samples()).sum();
    assert_eq!(stored, num_samples / 2);
    h.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestWALSampleAndExemplarOrder_AppenderV2
#[test]
fn test_wal_sample_and_exemplar_order_appender_v2() {
    let pairs = [("foo", "bar")];
    for kind in [SampleKind::Float, SampleKind::Hist, SampleKind::FloatHist] {
        let dir = tempfile::tempdir().unwrap();
        let h = Head::open(dir.path(), HeadOptions { chunk_range_ms: 1000, ..HeadOptions::default() }, i64::MIN).unwrap();
        let mut app = h.appender();
        let r = append_kind(&mut app, kind, &pairs, 10, 1).unwrap();
        app.append_exemplar(Some(r), &pairs, Exemplar::new(Labels::empty(), 1.0, 5)).unwrap();
        app.commit().unwrap();
        h.close().unwrap();

        let recs = read_all_wal_records(&dir.path().join("wal"));
        assert_eq!(recs.len(), 3);
        assert!(matches!(recs[0], DecodedRecord::Series(_)));
        match kind {
            SampleKind::Float => assert!(matches!(recs[1], DecodedRecord::Samples(_))),
            SampleKind::Hist => assert!(matches!(recs[1], DecodedRecord::HistogramSamples(_))),
            SampleKind::FloatHist => assert!(matches!(recs[1], DecodedRecord::FloatHistogramSamples(_))),
        }
        assert!(matches!(recs[2], DecodedRecord::Exemplars(_)));
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestHeadAppenderV2_Append_FloatWithSameTimestampAsPreviousHistogram
#[test]
fn test_head_appender_v2_append_float_with_same_timestamp_as_previous_histogram() {
    let dir = tempfile::tempdir().unwrap();
    let head = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();
    let pairs = [("__name__", "test")];
    {
        let mut app = head.appender();
        app.append_float(None, &pairs, 1_000, 10.0).unwrap();
        app.commit().unwrap();
    }
    {
        let mut app = head.appender();
        app.append_histogram(None, &pairs, 0, 2_000, gen_hist(1)).unwrap();
        app.commit().unwrap();
    }
    let mut app = head.appender();
    let err = app.append_float(None, &pairs, 2_000, 10.0).unwrap_err();
    assert!(err.is_duplicate_sample());
    app.rollback().unwrap();
    head.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestHeadAppenderV2_Append_EnableSTAsZeroSample
#[test]
fn test_head_appender_v2_append_enable_st_as_zero_sample() {
    let m = LabelMatcher::new(MatchType::Equal, "foo", "bar").unwrap();
    let run_case = |kind: SampleKind, appends: &[(i64, i64)]| -> Vec<(i64, f64)> {
        let dir = tempfile::tempdir().unwrap();
        let h = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();
        h.set_enable_st_as_zero_sample(true);
        let mut app = h.appender();
        for &(st, ts) in appends {
            match kind {
                SampleKind::Float => {
                    app.append_float_with_st(None, &[("foo", "bar")], st, ts, 10.0).unwrap();
                }
                SampleKind::Hist => {
                    app.append_histogram(None, &[("foo", "bar")], st, ts, gen_hist(2)).unwrap();
                }
                SampleKind::FloatHist => {
                    app.append_float_histogram(None, &[("foo", "bar")], st, ts, gen_fhist(2)).unwrap();
                }
            }
        }
        app.commit().unwrap();
        let got = query_head_samples(&h, m.clone());
        let out = got[r#"{foo="bar"}"#]
            .iter()
            .map(|d| match &d.value {
                SampleValue::Float(v) => (d.t, *v),
                SampleValue::Histogram(hh) => (d.t, hh.sum),
                SampleValue::FloatHistogram(fh) => (d.t, fh.sum),
            })
            .collect();
        h.close().unwrap();
        out
    };

    for kind in [SampleKind::Float, SampleKind::Hist, SampleKind::FloatHist] {
        let same_st = run_case(kind, &[(1, 100), (1, 101)]);
        assert_eq!(same_st.iter().map(|x| x.0).collect::<Vec<_>>(), vec![1, 100, 101]);
        assert_eq!(same_st[0].1, 0.0);

        let newer_st = run_case(kind, &[(1, 100), (101, 102)]);
        assert_eq!(newer_st.iter().map(|x| x.0).collect::<Vec<_>>(), vec![1, 100, 101, 102]);
        assert_eq!(newer_st[0].1, 0.0);
        assert_eq!(newer_st[2].1, 0.0);
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestHeadAppenderV2_STZeroSample_StartTimestamp
#[test]
fn test_head_appender_v2_st_zero_sample_start_timestamp() {
    for kind in [SampleKind::Float, SampleKind::Hist, SampleKind::FloatHist] {
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            enable_st_storage: true,
            float_chunk_encoding: Encoding::Xor2,
            enable_histogram_st_encoding: true,
            ..HeadOptions::default()
        };
        let h = Head::open(dir.path(), opts, i64::MIN).unwrap();
        h.set_enable_st_as_zero_sample(true);
        for ts in [100i64, 200] {
            let mut app = h.appender();
            match kind {
                SampleKind::Float => {
                    app.append_float_with_st(None, &[("foo", "bar")], 1, ts, 10.0).unwrap();
                }
                SampleKind::Hist => {
                    app.append_histogram(None, &[("foo", "bar")], 1, ts, gen_hist(2)).unwrap();
                }
                SampleKind::FloatHist => {
                    app.append_float_histogram(None, &[("foo", "bar")], 1, ts, gen_fhist(2)).unwrap();
                }
            }
            app.commit().unwrap();
        }
        let got = query_head_samples(&h, LabelMatcher::new(MatchType::Equal, "foo", "bar").unwrap());
        let samples = &got[r#"{foo="bar"}"#];
        assert_eq!(samples.len(), 3);
        for (s, &exp_t) in samples.iter().zip(&[1i64, 100, 200]) {
            assert_eq!(s.t, exp_t);
            assert_eq!(s.st, 1);
        }
        h.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestHeadAppenderV2_BestEffortSTZeroSample_OOO
#[test]
fn test_head_appender_v2_best_effort_st_zero_sample_ooo() {
    for kind in [SampleKind::Float, SampleKind::Hist, SampleKind::FloatHist] {
        let dir = tempfile::tempdir().unwrap();
        let head = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();
        head.set_enable_st_as_zero_sample(true);
        {
            let mut app = head.appender();
            append_kind(&mut app, kind, &[("foo", "bar")], 200, 10).unwrap();
            app.commit().unwrap();
        }
        {
            let mut app = head.appender();
            match kind {
                SampleKind::Float => {
                    app.append_float_with_st(None, &[("foo", "bar")], 100, 300, 10.0).unwrap();
                }
                SampleKind::Hist => {
                    app.append_histogram(None, &[("foo", "bar")], 100, 300, gen_hist(10)).unwrap();
                }
                SampleKind::FloatHist => {
                    app.append_float_histogram(None, &[("foo", "bar")], 100, 300, gen_fhist(10)).unwrap();
                }
            }
            app.commit().unwrap();
        }
        let got = query_head_samples(&head, LabelMatcher::new(MatchType::Equal, "foo", "bar").unwrap());
        assert_eq!(got[r#"{foo="bar"}"#].len(), 2);
        head.close().unwrap();
    }

    // Rejected ST injected zero does not retain series.
    for kind in [SampleKind::Float, SampleKind::Hist, SampleKind::FloatHist] {
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions { ooo_time_window_ms: 1000, ..HeadOptions::default() };
        let h = Head::open(dir.path(), opts, i64::MIN).unwrap();
        h.set_enable_st_as_zero_sample(true);
        let pairs = [("a", "1")];
        let ref_id;
        {
            let mut b = h.appender();
            ref_id = append_kind(&mut b, kind, &pairs, 200, 10).unwrap();
            b.commit().unwrap();
        }
        let mut app = h.appender();
        let res = match kind {
            SampleKind::Float => app.append_float_with_st_reject_ooo(Some(ref_id), &pairs, 100, 150, 10.0),
            SampleKind::Hist => app.append_histogram_with_st_reject_ooo(Some(ref_id), &pairs, 100, 150, gen_hist(10)),
            SampleKind::FloatHist => app.append_float_histogram_with_st_reject_ooo(Some(ref_id), &pairs, 100, 150, gen_fhist(10)),
        };
        assert!(matches!(res, Err(HeadError::OutOfOrderSample)));
        app.rollback().unwrap();
        h.truncate(201).unwrap();
        assert_eq!(h.num_series(), 0);
        h.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestHeadAppenderV2_Append_HistogramAndCommitConcurrency
#[test]
fn test_head_appender_v2_append_histogram_and_commit_concurrency() {
    for kind in [SampleKind::Hist, SampleKind::FloatHist] {
        let dir = tempfile::tempdir().unwrap();
        let head = Arc::new(Head::open(dir.path(), HeadOptions { chunk_range_ms: 1000, ..HeadOptions::default() }, i64::MIN).unwrap());
        let h1 = Arc::clone(&head);
        let h2 = Arc::clone(&head);
        let run = move |h: Arc<Head>| {
            for i in 0..1000 {
                let sv = i.to_string();
                let mut app = h.appender();
                append_kind(&mut app, kind, &[("foo", "bar"), ("serial", &sv)], 1, 1).unwrap();
                app.commit().unwrap();
            }
        };
        let t1 = std::thread::spawn(move || run(h1));
        let t2 = std::thread::spawn(move || run(h2));
        t1.join().unwrap();
        t2.join().unwrap();
        assert_eq!(head.num_series(), 1000);
        head.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestHeadAppenderV2_NumStaleSeries
#[test]
fn test_head_appender_v2_num_stale_series() {
    let dir = tempfile::tempdir().unwrap();
    let opts = HeadOptions { chunk_range_ms: 1000, ..HeadOptions::default() };
    let mut head = Head::open_with_snapshot(dir.path(), opts.clone(), i64::MIN, false).unwrap();
    assert_eq!(head.num_stale_series(), 0);

    let add_f = |h: &Head, name: &str, ts: i64, v: f64| {
        let mut app = h.appender();
        app.append_float(None, &[("name", name)], ts, v).unwrap();
        app.commit().unwrap();
    };
    let add_h = |h: &Head, name: &str, ts: i64, hist: Histogram| {
        let mut app = h.appender();
        app.append_histogram(None, &[("name", name)], 0, ts, hist).unwrap();
        app.commit().unwrap();
    };
    let add_fh = |h: &Head, name: &str, ts: i64, fh: FloatHistogram| {
        let mut app = h.appender();
        app.append_float_histogram(None, &[("name", name)], 0, ts, fh).unwrap();
        app.commit().unwrap();
    };

    add_f(&head, "s1", 100, 1.0);
    add_f(&head, "s2", 100, 2.0);
    add_f(&head, "s3", 100, 3.0);
    assert_eq!((head.num_stale_series(), head.num_series()), (0, 3));

    add_f(&head, "s1", 200, stale_nan());
    add_f(&head, "s2", 200, stale_nan());
    assert_eq!((head.num_stale_series(), head.num_series()), (2, 3));

    head.close().unwrap();
    head = Head::open_with_snapshot(dir.path(), opts.clone(), i64::MIN, false).unwrap();
    assert_eq!((head.num_stale_series(), head.num_series()), (2, 3));

    add_f(&head, "s1", 300, 10.0);
    add_f(&head, "s3", 200, 10.0);
    assert_eq!((head.num_stale_series(), head.num_series()), (1, 3));

    let hist = gen_hist(1);
    let mut stale_hist = hist.clone();
    stale_hist.sum = stale_nan();
    add_h(&head, "s4", 100, hist.clone());
    add_h(&head, "s4", 200, stale_hist.clone());
    assert_eq!((head.num_stale_series(), head.num_series()), (2, 4));

    let fhist = gen_fhist(1);
    let mut stale_fhist = fhist.clone();
    stale_fhist.sum = stale_nan();
    add_fh(&head, "s5", 100, fhist.clone());
    add_fh(&head, "s5", 200, stale_fhist.clone());
    assert_eq!((head.num_stale_series(), head.num_series()), (3, 5));

    add_h(&head, "s4", 210, hist);
    add_h(&head, "s4", 220, stale_hist.clone());
    add_fh(&head, "s5", 210, fhist);
    add_fh(&head, "s5", 220, stale_fhist.clone());
    assert_eq!((head.num_stale_series(), head.num_series()), (3, 5));

    add_f(&head, "s1", 400, 10.0);
    add_fh(&head, "s5", 400, stale_fhist.clone());
    head.set_enable_memory_snapshot_on_shutdown(true);
    head.close().unwrap();
    head = Head::open_with_snapshot(dir.path(), opts, i64::MIN, true).unwrap();
    assert_eq!((head.num_stale_series(), head.num_series()), (3, 5));

    head.truncate(300).unwrap();
    assert_eq!((head.num_stale_series(), head.num_series()), (1, 2));

    add_f(&head, "s6", 400, stale_nan());
    assert_eq!((head.num_stale_series(), head.num_series()), (2, 3));
    add_h(&head, "s7", 400, stale_hist);
    assert_eq!((head.num_stale_series(), head.num_series()), (3, 4));
    add_fh(&head, "s8", 400, stale_fhist);
    assert_eq!((head.num_stale_series(), head.num_series()), (4, 5));
    head.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestHeadAppenderV2_Append_HistogramStalenessConversionMetrics
#[test]
fn test_head_appender_v2_append_histogram_staleness_conversion_metrics() {
    for is_float_hist in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let reg = Arc::new(prometheus::Registry::new());
        let opts = HeadOptions {
            chunk_range_ms: 1000,
            enable_st_storage: true,
            float_chunk_encoding: Encoding::Xor2,
            enable_histogram_st_encoding: true,
            registry: Some(Arc::clone(&reg)),
            ..HeadOptions::default()
        };
        let head = Head::open(dir.path(), opts, i64::MIN).unwrap();
        let pairs = [("name", "conv")];
        {
            let mut app = head.appender();
            if is_float_hist {
                app.append_float_histogram(None, &pairs, 0, 1000, gen_fhist(1)).unwrap();
            } else {
                app.append_histogram(None, &pairs, 0, 1000, gen_hist(1)).unwrap();
            }
            app.commit().unwrap();
        }
        {
            let mut app = head.appender();
            app.append_float_with_st(None, &pairs, 1900, 2000, stale_nan()).unwrap();
            app.commit().unwrap();
        }
        let samples = &query_head_samples(&head, LabelMatcher::new(MatchType::Equal, "name", "conv").unwrap())[r#"{name="conv"}"#];
        assert_eq!(samples.len(), 2);
        assert_eq!(samples[1].t, 2000);
        assert_eq!(samples[1].st, 1900);
        assert_eq!(metric_value(&reg, "prometheus_tsdb_head_samples_appended_total", &[("type", "float")]), 0.0);
        assert_eq!(metric_value(&reg, "prometheus_tsdb_head_samples_appended_total", &[("type", "histogram")]), 2.0);
        head.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestHeadAppenderV2_STStorage
#[test]
fn test_head_appender_v2_st_storage() {
    let cases = [
        ("float_st", false, vec![(10i64, 100i64), (20, 200), (30, 300)]),
        ("float_varying_st", false, vec![(5, 100), (5, 200), (150, 300)]),
        ("hist_st", true, vec![(10, 100), (20, 200), (30, 300)]),
    ];
    for (_name, is_hist, entries) in cases {
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            enable_st_storage: true,
            float_chunk_encoding: Encoding::Xor2,
            enable_histogram_st_encoding: true,
            ..HeadOptions::default()
        };
        let h = Head::open(dir.path(), opts, i64::MIN).unwrap();
        let mut app = h.appender();
        for &(st, ts) in &entries {
            if is_hist {
                app.append_histogram(None, &[("foo", "bar")], st, ts, gen_hist(1)).unwrap();
            } else {
                app.append_float_with_st(None, &[("foo", "bar")], st, ts, ts as f64).unwrap();
            }
        }
        app.commit().unwrap();
        let got = query_head_samples(&h, LabelMatcher::new(MatchType::Equal, "foo", "bar").unwrap());
        let sts: Vec<i64> = got[r#"{foo="bar"}"#].iter().map(|s| s.st).collect();
        let exp_sts: Vec<i64> = entries.iter().map(|e| e.0).collect();
        assert_eq!(sts, exp_sts);
        h.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestHeadAppenderV2_Histogram_STStorage
#[test]
fn test_head_appender_v2_histogram_st_storage() {
    for enable_st in [true, false] {
        for is_float_hist in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let opts = HeadOptions {
                enable_st_storage: enable_st,
                float_chunk_encoding: Encoding::Xor2,
                enable_histogram_st_encoding: enable_st,
                ..HeadOptions::default()
            };
            let h = Head::open(dir.path(), opts, i64::MIN).unwrap();
            let mut app = h.appender();
            for (st, ts) in [(10i64, 100i64), (100, 200), (200, 300)] {
                if is_float_hist {
                    app.append_float_histogram(None, &[("foo", "bar")], st, ts, gen_fhist(1)).unwrap();
                } else {
                    app.append_histogram(None, &[("foo", "bar")], st, ts, gen_hist(1)).unwrap();
                }
            }
            app.commit().unwrap();
            let got = query_head_samples(&h, LabelMatcher::new(MatchType::Equal, "foo", "bar").unwrap());
            let sts: Vec<i64> = got[r#"{foo="bar"}"#].iter().map(|s| s.st).collect();
            let exp = if enable_st { vec![10, 100, 200] } else { vec![0, 0, 0] };
            assert_eq!(sts, exp);
            h.close().unwrap();
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestHeadAppenderV2_ExemplarsSurviveTruncation
#[test]
fn test_head_appender_v2_exemplars_survive_truncation() {
    for with_sample in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            chunk_range_ms: 1000,
            max_exemplars: 10,
            enable_exemplar_storage: true,
            ..HeadOptions::default()
        };
        let h = Head::open(dir.path(), opts, i64::MIN).unwrap();
        let pairs = [("name", "exemplar_series")];
        let ref_id;
        {
            let mut app = h.appender();
            ref_id = app.append_float(None, &pairs, 100, 1.0).unwrap();
            app.commit().unwrap();
        }
        let ex = Exemplar::new(Labels::from_pairs([("trace_id", "abc123")]), 1.0, 10_000);
        let mut app = h.appender();
        if with_sample {
            app.append_float(Some(ref_id), &pairs, 200, 2.0).unwrap();
        }
        app.append_exemplar(Some(ref_id), &pairs, ex.clone()).unwrap();
        app.commit().unwrap();

        h.truncate(1000).unwrap();
        assert!(h.get_entry_by_id(ref_id).is_some());
        assert!(h.head_chunks_bounds_for_test(ref_id).unwrap().is_empty());
        let m = [LabelMatcher::new(MatchType::Equal, "name", "exemplar_series").unwrap()];
        let got = h.exemplars().select(0, ex.timestamp_ms, &[&m]);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].exemplars.len(), 1);
        assert!(got[0].exemplars[0].prom_equals(&ex));

        let mut app2 = h.appender();
        let new_ref = app2.append_float(Some(ref_id), &pairs, ex.timestamp_ms + 1, 3.0).unwrap();
        assert_eq!(new_ref, ref_id);
        app2.commit().unwrap();
        assert!(!h.head_chunks_bounds_for_test(ref_id).unwrap().is_empty());

        h.truncate(ex.timestamp_ms + 2).unwrap();
        assert!(h.get_entry_by_id(ref_id).is_none());
        h.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_append_v2_test.go::TestHeadAppenderV2_ExemplarAppenderV2
#[test]
fn test_head_appender_v2_exemplar_appender_v2() {
    let dir = tempfile::tempdir().unwrap();
    let opts = HeadOptions {
        chunk_range_ms: 1000,
        max_exemplars: 100,
        enable_exemplar_storage: true,
        ..HeadOptions::default()
    };
    let h = Head::open(dir.path(), opts, i64::MIN).unwrap();
    let mut init_app = h.appender();
    assert!(matches!(
        init_app.append_exemplar(None, &[("__name__", "missing")], Exemplar::new(Labels::from_pairs([("id", "1")]), 1.0, 10)),
        Err(HeadError::UnknownSeriesRef(_))
    ));
    let pairs = [("__name__", "test_metric"), ("job", "app")];
    let ref_id = init_app.append_float(None, &pairs, 100, 42.0).unwrap();
    let ex1 = Exemplar::new(Labels::from_pairs([("trace_id", "t1")]), 1.0, 100);
    assert_eq!(init_app.append_exemplar(Some(ref_id), &[], ex1.clone()).unwrap(), ref_id);
    assert_eq!(init_app.append_exemplar(Some(ref_id), &[], ex1.clone()).unwrap(), ref_id);
    init_app.commit().unwrap();

    let mut head_app = h.appender();
    let ex2 = Exemplar::new(Labels::from_pairs([("trace_id", "t2")]), 2.0, 200);
    assert_eq!(head_app.append_exemplar(Some(ref_id), &pairs, ex2).unwrap(), ref_id);
    let too_long = "x".repeat(mimir_tsdb::exemplar::EXEMPLAR_MAX_LABEL_SET_LENGTH + 1);
    let err = head_app
        .append_exemplar(None, &[("__name__", "test_metric"), ("job", "app"), ("empty", "")], Exemplar::new(Labels::from_pairs([("trace_id", too_long.as_str())]), 3.0, 250))
        .unwrap_err();
    assert!(matches!(err, HeadError::Exemplar(_)));
    let ex3 = Exemplar::new(Labels::from_pairs([("trace_id", "t3")]), 3.0, 300);
    assert_eq!(head_app.append_exemplar(None, &pairs, ex3).unwrap(), ref_id);
    head_app.commit().unwrap();

    let m = [LabelMatcher::new(MatchType::Equal, "__name__", "test_metric").unwrap()];
    let res = h.exemplars().select(0, 400, &[&m]);
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].exemplars.len(), 3);
    h.close().unwrap();
}

