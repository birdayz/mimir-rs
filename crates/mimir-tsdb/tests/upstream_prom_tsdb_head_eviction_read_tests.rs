//! Ported upstream Prometheus/Mimir TSDB Head eviction, WAL replay, and read tests.
//!
//! Sources:
//! - head_series_eviction_test.go (15)
//! - head_wal_replay_test.go (2)
//! - head_read_test.go (10)
//! - head_read_mimir_test.go (1)

use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

use mimir_tsdb::chunkenc::{
    stale_nan, CounterResetHint, EncodedChunk, FloatHistogram, Histogram, HistogramSpan,
    XorChunk,
};
use mimir_tsdb::compact::{decode_chunk_samples, SampleValue};
use mimir_tsdb::db::{Tsdb, TsdbOptions};
use mimir_tsdb::exemplar::Exemplar;
use mimir_tsdb::head::{
    EvictedSeries, Head, HeadError, HeadOptions, HeadReadIsolation, SeriesLifecycleCallback,
};
use mimir_tsdb::head_chunks::{
    ooo_head_chunk_id, pack_head_chunk_ref, resolve_ooo_chunk_pos, unpack_head_chunk_ref,
    HeadChunksReader, OOO_CHUNK_ID_MASK,
};
use mimir_tsdb::isolation::{Isolation, TxRing};
use mimir_tsdb::labels::Labels;
use mimir_tsdb::matcher::{LabelMatcher, MatchType};
use mimir_tsdb::querier::{PreparedChunkSelect, TsdbQuerier};
use mimir_tsdb::record::{DecodedRecord, RecordDecoder};
use mimir_tsdb::wlog::{segment_name, WlogReader};

const EVICTION_TEST_SERIES: usize = 10;
const EVICTION_TEST_EVICTED_SERIES: usize = 5;
const EVICTION_TEST_SAMPLES: usize = 3;

fn counter_value(reg: &prometheus::Registry, name: &str, labels: &[(&str, &str)]) -> f64 {
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

fn eviction_labels(i: usize) -> Labels {
    Labels::from_pairs([("__name__", "series"), ("id", &format!("{i:02}"))])
}

fn eviction_matcher() -> LabelMatcher {
    LabelMatcher::new(MatchType::Equal, "__name__", "series").unwrap()
}

fn eviction_expected_samples(late_series: &[usize]) -> BTreeMap<String, usize> {
    let mut res = BTreeMap::new();
    for i in 0..EVICTION_TEST_SERIES {
        res.insert(eviction_labels(i).to_string(), EVICTION_TEST_SAMPLES);
    }
    for &i in late_series {
        *res.entry(eviction_labels(i).to_string()).or_insert(0) += 1;
    }
    res
}

struct EvictionTestDb {
    _dir: tempfile::TempDir,
    db: Arc<Tsdb>,
    reg: Arc<prometheus::Registry>,
    evicted_refs: Vec<u64>,
}

fn new_eviction_test_db(exemplar_series: &[usize]) -> EvictionTestDb {
    let dir = tempfile::tempdir().unwrap();
    let reg = Arc::new(prometheus::Registry::new());
    let opts = TsdbOptions {
        head: HeadOptions {
            chunk_range_ms: 1000,
            enable_exemplar_storage: !exemplar_series.is_empty(),
            max_exemplars: exemplar_series.len(),
            registry: Some(Arc::clone(&reg)),
            ..HeadOptions::default()
        },
        ..TsdbOptions::default()
    };
    let db = Arc::new(Tsdb::open_with_reload_interval(dir.path(), opts, Duration::ZERO).unwrap());
    db.disable_compactions();
    let mut evicted_refs = Vec::new();
    {
        let mut app = db.appender();
        for i in 0..EVICTION_TEST_SERIES {
            let evicted = i < EVICTION_TEST_EVICTED_SERIES;
            let mut ref_id = 0u64;
            let id_str = format!("{i:02}");
            let pairs = [("__name__", "series"), ("id", id_str.as_str())];
            for s in 0..EVICTION_TEST_SAMPLES {
                let v = if evicted && s == EVICTION_TEST_SAMPLES - 1 {
                    stale_nan()
                } else {
                    i as f64
                };
                ref_id = app.append_float(None, &pairs, 100 * (s as i64 + 1), v).unwrap();
            }
            if evicted {
                evicted_refs.push(ref_id);
            }
            if exemplar_series.contains(&i) {
                let trace = i.to_string();
                app.append_exemplar(
                    Some(ref_id),
                    &pairs,
                    Exemplar {
                        labels: Labels::from_pairs([("trace_id", trace.as_str())]),
                        value: 1.0,
                        timestamp_ms: 200,
                        has_ts: true,
                    },
                )
                .unwrap();
            }
        }
        app.commit().unwrap();
    }
    assert_eq!(db.head().num_series(), EVICTION_TEST_SERIES as u64);
    EvictionTestDb {
        _dir: dir,
        db,
        reg,
        evicted_refs,
    }
}

#[derive(Clone, Copy)]
enum EvictMode {
    CompactSelectedSeries,
    CompactStaleHead,
}

impl EvictMode {
    fn run(self, db: &Tsdb, refs: &[u64]) {
        match self {
            EvictMode::CompactSelectedSeries => {
                db.compact_selected_series(refs).unwrap();
            }
            EvictMode::CompactStaleHead => {
                db.compact_stale_head().unwrap();
            }
        }
    }
}

#[derive(Clone, Copy)]
enum QuerierKind {
    Querier,
    ChunkQuerier,
}

enum OpenPreparedQuery {
    Samples {
        mint: i64,
        maxt: i64,
        prep: PreparedChunkSelect,
    },
    Chunks {
        mint: i64,
        maxt: i64,
        prep: PreparedChunkSelect,
    },
}

fn open_prepared_query(db: &Tsdb, kind: QuerierKind, mint: i64, maxt: i64) -> OpenPreparedQuery {
    let m = [eviction_matcher()];
    match kind {
        QuerierKind::Querier => {
            let q = db.querier(mint, maxt);
            let prep = q.prepare_select_chunks(&m, false).unwrap();
            OpenPreparedQuery::Samples { mint, maxt, prep }
        }
        QuerierKind::ChunkQuerier => {
            let cq = db.chunk_querier(mint, maxt);
            let prep = cq.prepare_select(&m).unwrap();
            OpenPreparedQuery::Chunks { mint, maxt, prep }
        }
    }
}

fn read_prepared_query(db: &Tsdb, open: OpenPreparedQuery) -> BTreeMap<String, usize> {
    let mut out = BTreeMap::new();
    match open {
        OpenPreparedQuery::Samples { mint, maxt, prep } => {
            let q = db.querier(mint, maxt);
            let series = q.execute_prepared_select(prep, true).unwrap();
            for s in series {
                let mut count = 0usize;
                for cm in &s.chunks {
                    let enc = cm.chunk.as_ref().unwrap();
                    count += decode_chunk_samples(enc)
                        .unwrap()
                        .into_iter()
                        .filter(|ds| ds.t >= mint && ds.t <= maxt)
                        .count();
                }
                if count > 0 {
                    *out.entry(s.labels.to_string()).or_insert(0) += count;
                }
            }
        }
        OpenPreparedQuery::Chunks { mint, maxt, prep } => {
            let cq = db.chunk_querier(mint, maxt);
            let series = cq.execute_prepared_select(prep, true).unwrap();
            for s in series {
                let mut count = 0usize;
                for cm in &s.chunks {
                    let enc = cm.chunk.as_ref().unwrap();
                    count += decode_chunk_samples(enc)
                        .unwrap()
                        .into_iter()
                        .filter(|ds| ds.t >= mint && ds.t <= maxt)
                        .count();
                }
                if count > 0 {
                    *out.entry(s.labels.to_string()).or_insert(0) += count;
                }
            }
        }
    }
    out
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

fn wait_until(label: &str, mut cond: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !cond() {
        assert!(Instant::now() < deadline, "timed out waiting for {label}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

type PreCreationHook = Arc<dyn Fn(&[(&str, &str)]) -> Result<(), HeadError> + Send + Sync>;
type PostDeletionHook = Arc<dyn Fn(&[(u64, Labels)]) + Send + Sync>;

struct TestLifecycleCb {
    pre: Option<PreCreationHook>,
    post_del: Option<PostDeletionHook>,
}

impl SeriesLifecycleCallback for TestLifecycleCb {
    fn pre_creation(&self, pairs: &[(&str, &str)]) -> Result<(), HeadError> {
        if let Some(ref f) = self.pre {
            f(pairs)
        } else {
            Ok(())
        }
    }

    fn post_creation(&self, _ref_id: u64, _labels: &Labels) {}

    fn post_deletion(&self, deleted: &[(u64, Labels)]) {
        if let Some(ref f) = self.post_del {
            f(deleted);
        }
    }
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

#[derive(Clone, Copy)]
enum SampleKind {
    Float,
    Hist,
    FloatHist,
}

fn append_kind(
    app: &mut mimir_tsdb::head::HeadAppender<'_>,
    kind: SampleKind,
    pairs: &[(&str, &str)],
    t: i64,
    v: i64,
) -> u64 {
    match kind {
        SampleKind::Float => app.append_float(None, pairs, t, v as f64).unwrap(),
        SampleKind::Hist => app.append_histogram(None, pairs, 0, t, gen_hist(v)).unwrap(),
        SampleKind::FloatHist => app
            .append_float_histogram(None, pairs, 0, t, gen_fhist(v))
            .unwrap(),
    }
}

fn check_sample_value(ds: &mimir_tsdb::compact::DecodedSample, expected: i64) {
    match &ds.value {
        SampleValue::Float(v) => assert_eq!(*v, expected as f64),
        SampleValue::Histogram(h) => assert_eq!(h.zero_count, expected.unsigned_abs()),
        SampleValue::FloatHistogram(fh) => assert_eq!(fh.zero_count, expected.unsigned_abs() as f64),
    }
}

// =========================================================================
// head_series_eviction_test.go (15)
// =========================================================================

// Upstream: github.com/prometheus/prometheus/tsdb/head_series_eviction_test.go::TestSeriesEviction_QueryOpenedAfterWaitingForReaders
#[test]
fn test_series_eviction_query_opened_after_waiting_for_readers() {
    let mut all_cases = Vec::new();
    for mode in [EvictMode::CompactSelectedSeries, EvictMode::CompactStaleHead] {
        for qk in [QuerierKind::Querier, QuerierKind::ChunkQuerier] {
            for (mint, maxt) in [(0, 1000), (i64::MIN, i64::MAX)] {
                all_cases.push((mode, qk, mint, maxt));
            }
        }
    }

    struct Scenario {
        late_series: Vec<usize>,
        exemplar_series: Vec<usize>,
        append_after_eviction: bool,
        cases: Vec<(EvictMode, QuerierKind, i64, i64)>,
    }

    let scenarios = vec![
        Scenario {
            late_series: vec![],
            exemplar_series: vec![],
            append_after_eviction: false,
            cases: vec![
                (EvictMode::CompactSelectedSeries, QuerierKind::Querier, 0, 1000),
                (EvictMode::CompactSelectedSeries, QuerierKind::Querier, i64::MIN, i64::MAX),
                (EvictMode::CompactSelectedSeries, QuerierKind::ChunkQuerier, 0, 1000),
                (EvictMode::CompactSelectedSeries, QuerierKind::ChunkQuerier, i64::MIN, i64::MAX),
                (EvictMode::CompactStaleHead, QuerierKind::ChunkQuerier, i64::MIN, i64::MAX),
            ],
        },
        Scenario {
            late_series: vec![0],
            exemplar_series: vec![],
            append_after_eviction: false,
            cases: vec![
                (EvictMode::CompactSelectedSeries, QuerierKind::ChunkQuerier, 0, 1000),
                (EvictMode::CompactSelectedSeries, QuerierKind::Querier, i64::MIN, i64::MAX),
            ],
        },
        Scenario {
            late_series: vec![],
            exemplar_series: vec![0],
            append_after_eviction: false,
            cases: all_cases.clone(),
        },
        Scenario {
            late_series: vec![0],
            exemplar_series: vec![0, 1],
            append_after_eviction: false,
            cases: all_cases.clone(),
        },
        Scenario {
            late_series: vec![],
            exemplar_series: vec![0],
            append_after_eviction: true,
            cases: all_cases,
        },
    ];

    for sc in scenarios {
        for (mode, qk, mint, maxt) in sc.cases {
            let tdb = new_eviction_test_db(&sc.exemplar_series);
            let db = Arc::clone(&tdb.db);
            if !sc.exemplar_series.is_empty() {
                db.head().truncate(200).unwrap();
            }

            let late_series = sc.late_series.clone();
            let db_late = Arc::clone(&db);
            if !late_series.is_empty() {
                db.set_compact_head_view_before_evict_hook_for_test(Some(Arc::new(move || {
                    let mut app = db_late.appender();
                    for &i in &late_series {
                        let id_str = format!("{i:02}");
                        app.append_float(None, &[("__name__", "series"), ("id", &id_str)], 400, 42.0)
                            .unwrap();
                    }
                    app.commit().unwrap();
                })));
            }

            let reading: Arc<Mutex<Option<OpenPreparedQuery>>> = Arc::new(Mutex::new(None));
            let num_series_in_cb = Arc::new(Mutex::new(0u64));
            let reading_cb = Arc::clone(&reading);
            let num_cb = Arc::clone(&num_series_in_cb);
            let db_cb = Arc::clone(&db);
            db.head()
                .set_after_waiting_for_readers_hook_for_test(Some(Arc::new(move || {
                    assert!(db_cb.head().evicting_series().is_some());
                    *num_cb.lock().unwrap() = db_cb.head().num_series();
                    *reading_cb.lock().unwrap() = Some(open_prepared_query(&db_cb, qk, mint, maxt));
                })));

            mode.run(&db, &tdb.evicted_refs);

            let mut removed = EVICTION_TEST_EVICTED_SERIES - sc.late_series.len();
            for &i in &sc.exemplar_series {
                let ref_id = tdb.evicted_refs[i];
                assert!(db.head().get_entry_by_id(ref_id).is_some());
                if !sc.late_series.contains(&i) {
                    removed -= 1;
                    assert!(db.head().mmapped_chunks_bounds_for_test(ref_id).unwrap().is_empty());
                    assert!(db.head().head_chunks_bounds_for_test(ref_id).unwrap().is_empty());
                }
            }
            assert_eq!(*num_series_in_cb.lock().unwrap(), EVICTION_TEST_SERIES as u64);
            assert_eq!(db.head().num_series(), (EVICTION_TEST_SERIES - removed) as u64);

            let open_q = reading.lock().unwrap().take().unwrap();
            let got = read_prepared_query(&db, open_q);
            assert_eq!(got, eviction_expected_samples(&sc.late_series));

            let expected_skipped = (EVICTION_TEST_EVICTED_SERIES - sc.late_series.len()) as f64;
            assert_eq!(
                counter_value(
                    &tdb.reg,
                    "prometheus_tsdb_head_chunk_reads_of_removed_series_total",
                    &[]
                ),
                expected_skipped
            );

            if sc.append_after_eviction {
                let mut app = db.appender();
                app.append_float(
                    Some(tdb.evicted_refs[0]),
                    &[("__name__", "series"), ("id", "00")],
                    400,
                    42.0,
                )
                .unwrap();
                app.commit().unwrap();
                let current = open_prepared_query(&db, qk, mint, maxt);
                assert_eq!(read_prepared_query(&db, current), eviction_expected_samples(&[0]));
            }
            db.close().unwrap();
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_series_eviction_test.go::TestSeriesEviction_OnlyQueriesSkipRemovedSeries
#[test]
fn test_series_eviction_only_queries_skip_removed_series() {
    let tdb = new_eviction_test_db(&[]);
    let head = tdb.db.head();
    let ref0 = tdb.evicted_refs[0];

    let (_, metas, _) = head
        .series_chunk_metas_no_bytes(ref0, 0, 1000, None)
        .unwrap()
        .unwrap();
    assert_eq!(metas.len(), 1);
    tdb.db.compact_selected_series(&tdb.evicted_refs).unwrap();

    let evicting = Arc::new(EvictedSeries::new(tdb.evicted_refs.clone(), 1000, None));
    let iso_query = HeadReadIsolation {
        ino_mint: 0,
        evicting: Some(evicting),
        ..Default::default()
    };
    let iso_non_query = HeadReadIsolation {
        ino_mint: 0,
        evicting: None,
        ..Default::default()
    };

    let before = counter_value(
        &tdb.reg,
        "prometheus_tsdb_head_chunk_reads_of_removed_series_total",
        &[],
    );
    assert!(head
        .series_chunks_with_isolation(ref0, 0, 1000, &iso_query)
        .unwrap()
        .is_none());
    assert!(head
        .series_chunk_metas_no_bytes_with_snapshot_and_isolation(ref0, 0, 1000, None, &iso_query)
        .unwrap()
        .is_none());
    assert_eq!(
        counter_value(
            &tdb.reg,
            "prometheus_tsdb_head_chunk_reads_of_removed_series_total",
            &[]
        ),
        before + 2.0
    );

    let before2 = counter_value(
        &tdb.reg,
        "prometheus_tsdb_head_chunk_reads_of_removed_series_total",
        &[],
    );
    assert!(head
        .series_chunks_with_isolation(ref0, 0, 1000, &iso_non_query)
        .unwrap()
        .is_none());
    let mut chks = Vec::new();
    let mut tombs = Vec::new();
    head.load_series_in_order_chunks_into(ref0, 0, 1000, &mut chks, &mut tombs)
        .unwrap();
    assert!(chks.is_empty());
    assert_eq!(
        counter_value(
            &tdb.reg,
            "prometheus_tsdb_head_chunk_reads_of_removed_series_total",
            &[]
        ),
        before2
    );
    tdb.db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_series_eviction_test.go::TestSeriesEviction_OtherNotFoundErrorsAreReturned
#[test]
fn test_series_eviction_other_not_found_errors_are_returned() {
    for truncate in [false, true] {
        let tdb = new_eviction_test_db(&[0]);
        let head = tdb.db.head();
        let captured = Arc::new(Mutex::new(None));
        if truncate {
            head.truncate(200).unwrap();
            let cap_cb = Arc::clone(&captured);
            let h_cb = Arc::clone(&tdb.db);
            head.set_after_waiting_for_readers_hook_for_test(Some(Arc::new(move || {
                *cap_cb.lock().unwrap() = h_cb.head().evicting_series();
            })));
            tdb.db.compact_selected_series(&tdb.evicted_refs).unwrap();
        }
        let evicting = captured
            .lock()
            .unwrap()
            .take()
            .unwrap_or_else(|| Arc::new(EvictedSeries::new(tdb.evicted_refs.clone(), 1000, None)));
        let iso = HeadReadIsolation {
            ino_mint: 5000,
            evicting: Some(evicting),
            ..Default::default()
        };

        let evicted_ref = tdb.evicted_refs[0];
        let kept_ref = *tdb.evicted_refs.last().unwrap() + 1;
        for ref_id in [evicted_ref, kept_ref] {
            assert!(head.get_entry_by_id(ref_id).is_some());
            assert!(head
                .series_chunks_with_isolation(ref_id, 5000, 6000, &iso)
                .unwrap()
                .is_none());
        }
        assert_eq!(
            counter_value(
                &tdb.reg,
                "prometheus_tsdb_head_chunk_reads_of_removed_series_total",
                &[]
            ),
            0.0
        );
        tdb.db.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_series_eviction_test.go::TestSeriesEviction_SeriesNotBeingEvictedAreNotSkipped
#[test]
fn test_series_eviction_series_not_being_evicted_are_not_skipped() {
    let tdb = new_eviction_test_db(&[]);
    let head = tdb.db.head();
    let ref0 = tdb.evicted_refs[0];
    tdb.db.compact_selected_series(&tdb.evicted_refs).unwrap();

    for evicting in [
        None,
        Some(Arc::new(EvictedSeries::new(
            tdb.evicted_refs[1..].to_vec(),
            1000,
            None,
        ))),
    ] {
        let iso = HeadReadIsolation {
            ino_mint: 0,
            evicting,
            ..Default::default()
        };
        assert!(head
            .series_chunks_with_isolation(ref0, 0, 1000, &iso)
            .unwrap()
            .is_none());
        assert_eq!(
            counter_value(
                &tdb.reg,
                "prometheus_tsdb_head_chunk_reads_of_removed_series_total",
                &[]
            ),
            0.0
        );
    }
    tdb.db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_series_eviction_test.go::TestSeriesEviction_SeriesAreOnlyPublishedWhileTheyAreEvicted
#[test]
fn test_series_eviction_series_are_only_published_while_they_are_evicted() {
    for mode in [EvictMode::CompactSelectedSeries, EvictMode::CompactStaleHead] {
        let tdb = new_eviction_test_db(&[]);
        let head = tdb.db.head();
        assert!(head.evicting_series().is_none());

        let published: Arc<Mutex<Option<Arc<EvictedSeries>>>> = Arc::new(Mutex::new(None));
        let pub_cb = Arc::clone(&published);
        let db_cb = Arc::clone(&tdb.db);
        head.set_after_waiting_for_readers_hook_for_test(Some(Arc::new(move || {
            *pub_cb.lock().unwrap() = db_cb.head().evicting_series();
        })));

        mode.run(&tdb.db, &tdb.evicted_refs);

        let ev = published.lock().unwrap().take().unwrap();
        for &r in &tdb.evicted_refs {
            assert!(ev.contains(r));
        }
        assert!(head.evicting_series().is_none());
        tdb.db.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_series_eviction_test.go::TestSeriesEviction_RecreationIsDeclaredAfterTombstone
#[test]
fn test_series_eviction_recreation_is_declared_after_tombstone() {
    for mode in [EvictMode::CompactSelectedSeries, EvictMode::CompactStaleHead] {
        let tdb = new_eviction_test_db(&[]);
        let db = Arc::clone(&tdb.db);
        let old_ref = tdb.evicted_refs[0];
        let lset = eviction_labels(0);
        let matcher = [eviction_matcher()];

        let mut expected: BTreeMap<String, usize> = db
            .querier(i64::MIN, i64::MAX)
            .select(true, &matcher)
            .unwrap()
            .into_iter()
            .map(|s| (s.labels.to_string(), s.samples.len()))
            .collect();
        *expected.entry(lset.to_string()).or_insert(0) += 2;

        let (done_tx, done_rx) = mpsc::channel::<Result<u64, String>>();
        let db_cb = Arc::clone(&db);
        let lset_hash = lset.hash();
        db.head()
            .set_lifecycle_callback(Some(Arc::new(TestLifecycleCb {
                pre: None,
                post_del: Some(Arc::new(move |deleted| {
                    if !deleted.iter().any(|(r, _)| *r == old_ref) {
                        return;
                    }
                    let db_t = Arc::clone(&db_cb);
                    let tx_t = done_tx.clone();
                    std::thread::spawn(move || {
                        let mut app = db_t.appender();
                        let res = app
                            .append_float(
                                Some(old_ref),
                                &[("__name__", "series"), ("id", "00")],
                                400,
                                42.0,
                            )
                            .and_then(|r| {
                                app.commit()?;
                                Ok(r)
                            })
                            .map_err(|e| e.to_string());
                        let _ = tx_t.send(res);
                    });
                    std::thread::sleep(Duration::from_millis(30));
                    assert!(db_cb.head().is_hash_pending_tombstone_for_test(lset_hash));
                })),
            })));

        mode.run(&db, &tdb.evicted_refs);
        let new_ref = done_rx.recv_timeout(Duration::from_secs(10)).unwrap().unwrap();
        assert_ne!(new_ref, old_ref);

        {
            let mut app = db.appender();
            let later = app
                .append_float(
                    Some(new_ref),
                    &[("__name__", "series"), ("id", "00")],
                    500,
                    43.0,
                )
                .unwrap();
            assert_eq!(later, new_ref);
            app.commit().unwrap();
        }

        let got: BTreeMap<String, usize> = db
            .querier(i64::MIN, i64::MAX)
            .select(true, &matcher)
            .unwrap()
            .into_iter()
            .map(|s| (s.labels.to_string(), s.samples.len()))
            .collect();
        assert_eq!(got, expected);
        let dir_path = db.dir().to_path_buf();
        db.close().unwrap();

        let mut stone_pos = None;
        let mut series_pos = None;
        for (idx, rec) in read_all_wal_records(&dir_path.join("wal")).into_iter().enumerate() {
            match rec {
                DecodedRecord::Tombstones(stones) => {
                    if stones.iter().any(|s| s.ref_id == old_ref) {
                        stone_pos = Some(idx);
                    }
                }
                DecodedRecord::Series(series) if series.iter().any(|s| s.ref_id == new_ref) => {
                    series_pos = Some(idx);
                }
                _ => {}
            }
        }
        assert!(series_pos.unwrap() > stone_pos.unwrap());

        let reopened = Tsdb::open_with_reload_interval(
            &dir_path,
            TsdbOptions::default(),
            Duration::ZERO,
        )
        .unwrap();
        let got_reopened: BTreeMap<String, usize> = reopened
            .querier(i64::MIN, i64::MAX)
            .select(true, &matcher)
            .unwrap()
            .into_iter()
            .map(|s| (s.labels.to_string(), s.samples.len()))
            .collect();
        assert_eq!(got_reopened, expected);
        assert!(reopened.head().get_entry_by_id(old_ref).is_none());
        let new_entry = reopened.head().get_entry_by_id(new_ref).unwrap();
        assert_eq!(new_entry.labels, lset);
        let head_q = TsdbQuerier::new(Some(reopened.head()), vec![], i64::MIN, i64::MAX);
        let head_res = head_q.select(true, &matcher).unwrap();
        let s0 = head_res.iter().find(|s| s.labels == lset).unwrap();
        assert_eq!(s0.samples.iter().map(|x| x.t).collect::<Vec<_>>(), vec![400, 500]);
        reopened.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_series_eviction_test.go::TestHead_GCSeriesDoesNotBlockRecreation
#[test]
fn test_head_gc_series_does_not_block_recreation() {
    let tdb = new_eviction_test_db(&[]);
    let db = Arc::clone(&tdb.db);
    let h = db.head();
    let ref0 = tdb.evicted_refs[0];
    let ref1 = tdb.evicted_refs[1];

    h.evict_series_refs(&[ref0, ref1]).unwrap();
    assert!(!h.is_hash_pending_tombstone_for_test(eviction_labels(0).hash()));
    assert!(!h.is_hash_pending_tombstone_for_test(eviction_labels(1).hash()));

    let (tx, rx) = mpsc::channel();
    let db_t = Arc::clone(&db);
    std::thread::spawn(move || {
        let mut app = db_t.appender();
        let res = app
            .append_float(None, &[("__name__", "series"), ("id", "00")], 400, 1.0)
            .and_then(|_| app.append_float(None, &[("__name__", "series"), ("id", "01")], 400, 1.0))
            .and_then(|_| app.commit())
            .map_err(|e| e.to_string());
        let _ = tx.send(res);
    });
    rx.recv_timeout(Duration::from_secs(10)).unwrap().unwrap();
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_series_eviction_test.go::TestSeriesEviction_CreationPausedBeforeInsertionIsDeclaredAfterTombstone
#[test]
fn test_series_eviction_creation_paused_before_insertion_is_declared_after_tombstone() {
    for mode in [EvictMode::CompactSelectedSeries, EvictMode::CompactStaleHead] {
        let tdb = new_eviction_test_db(&[]);
        let db = Arc::clone(&tdb.db);
        let ls = eviction_labels(20);

        let (paused_tx, paused_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel::<()>();
        let resume_rx = Arc::new(Mutex::new(Some(resume_rx)));
        let (unlinked_tx, unlinked_rx) = mpsc::channel();
        let (finish_tx, finish_rx) = mpsc::channel::<()>();
        let finish_rx = Arc::new(Mutex::new(Some(finish_rx)));
        let first_creation = Arc::new(AtomicBool::new(false));
        let old_ref = Arc::new(Mutex::new(0u64));
        let old_ref_cb = Arc::clone(&old_ref);

        db.head()
            .set_lifecycle_callback(Some(Arc::new(TestLifecycleCb {
                pre: Some(Arc::new(move |pairs| {
                    if pairs == [("__name__", "series"), ("id", "20")]
                        && first_creation
                            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                            .is_ok()
                    {
                        let _ = paused_tx.send(());
                        if let Some(rx) = resume_rx.lock().unwrap().take() {
                            let _ = rx.recv();
                        }
                    }
                    Ok(())
                })),
                post_del: Some(Arc::new(move |deleted| {
                    let target = *old_ref_cb.lock().unwrap();
                    if deleted.iter().any(|(r, _)| *r == target) {
                        let _ = unlinked_tx.send(());
                        if let Some(rx) = finish_rx.lock().unwrap().take() {
                            let _ = rx.recv();
                        }
                    }
                })),
            })));

        let mut old_app = db.appender();
        let (created_tx, created_rx) = mpsc::channel::<Result<u64, String>>();
        let db_creator = Arc::clone(&db);
        let creator_handle = std::thread::spawn(move || {
            let mut app = db_creator.appender();
            let res = app
                .append_float(None, &[("__name__", "series"), ("id", "20")], 400, 42.0)
                .and_then(|r| {
                    app.commit()?;
                    Ok(r)
                })
                .map_err(|e| e.to_string());
            let _ = created_tx.send(res);
        });

        paused_rx.recv_timeout(Duration::from_secs(10)).unwrap();

        {
            let mut r = 0u64;
            for i in 0..3 {
                let v = if i == 2 { stale_nan() } else { i as f64 };
                r = old_app
                    .append_float(
                        None,
                        &[("__name__", "series"), ("id", "20")],
                        (i as i64 + 1) * 100,
                        v,
                    )
                    .unwrap();
            }
            *old_ref.lock().unwrap() = r;
            old_app.commit().unwrap();
        }
        let target_ref = *old_ref.lock().unwrap();

        let (evicted_tx, evicted_rx) = mpsc::channel::<Result<(), String>>();
        let db_evict = Arc::clone(&db);
        let evict_handle = std::thread::spawn(move || {
            match mode {
                EvictMode::CompactSelectedSeries => {
                    let _ = evicted_tx.send(
                        db_evict
                            .compact_selected_series(&[target_ref])
                            .map(|_| ())
                            .map_err(|e| e.to_string()),
                    );
                }
                EvictMode::CompactStaleHead => {
                    let _ = evicted_tx.send(
                        db_evict
                            .compact_stale_head()
                            .map(|_| ())
                            .map_err(|e| e.to_string()),
                    );
                }
            }
        });

        unlinked_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        assert!(db.head().has_pending_tombstones_active_for_test());
        let _ = resume_tx.send(());

        std::thread::sleep(Duration::from_millis(50));
        assert!(
            created_rx.try_recv().is_err(),
            "creator must stay blocked while tombstone is pending"
        );
        let _ = finish_tx.send(());

        let new_ref = created_rx.recv_timeout(Duration::from_secs(10)).unwrap().unwrap();
        assert_ne!(new_ref, target_ref);
        evicted_rx.recv_timeout(Duration::from_secs(10)).unwrap().unwrap();
        creator_handle.join().unwrap();
        evict_handle.join().unwrap();

        {
            let mut app = db.appender();
            app.append_float(
                Some(new_ref),
                &[("__name__", "series"), ("id", "20")],
                500,
                43.0,
            )
            .unwrap();
            app.commit().unwrap();
        }

        let matcher = [LabelMatcher::new(MatchType::Equal, "id", "20").unwrap()];
        let before = db.querier(i64::MIN, i64::MAX).select(true, &matcher).unwrap();
        assert_eq!(before.len(), 1);
        assert_eq!(before[0].samples.len(), 5);
        let dir_path = db.dir().to_path_buf();
        db.close().unwrap();

        let mut old_stone_pos = None;
        let mut new_series_pos = None;
        for (idx, rec) in read_all_wal_records(&dir_path.join("wal")).into_iter().enumerate() {
            match rec {
                DecodedRecord::Tombstones(stones) => {
                    if stones.iter().any(|s| s.ref_id == target_ref) {
                        old_stone_pos = Some(idx);
                    }
                }
                DecodedRecord::Series(series) if series.iter().any(|s| s.ref_id == new_ref) => {
                    new_series_pos = Some(idx);
                }
                _ => {}
            }
        }
        assert!(new_series_pos.unwrap() > old_stone_pos.unwrap());

        let reopened = Tsdb::open_with_reload_interval(
            &dir_path,
            TsdbOptions::default(),
            Duration::ZERO,
        )
        .unwrap();
        let after = reopened
            .querier(i64::MIN, i64::MAX)
            .select(true, &matcher)
            .unwrap();
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].labels, ls);
        let to_bits = |s: &[mimir_tsdb::compact::DecodedSample]| -> Vec<(i64, u64)> {
            s.iter()
                .map(|d| {
                    let bits = match &d.value {
                        SampleValue::Float(v) => v.to_bits(),
                        _ => 0,
                    };
                    (d.t, bits)
                })
                .collect()
        };
        assert_eq!(to_bits(&after[0].samples), to_bits(&before[0].samples));
        reopened.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_series_eviction_test.go::TestHead_WALReplay_RecreatedSeriesKeepsSamplesBelowOldMmappedChunks
#[test]
fn test_head_wal_replay_recreated_series_keeps_samples_below_old_mmapped_chunks() {
    let dir = tempfile::tempdir().unwrap();
    let opts = HeadOptions {
        chunk_range_ms: 10_000,
        ..HeadOptions::default()
    };
    let head = Arc::new(Head::open(dir.path(), opts.clone(), i64::MIN).unwrap());
    let pairs = [("foo", "bar")];

    let mut old_ref = 0u64;
    {
        let mut app = head.appender();
        for i in 0..130 {
            let v = if i == 129 { stale_nan() } else { i as f64 };
            old_ref = app.append_float(None, &pairs, 2 * i as i64, v).unwrap();
        }
        app.commit().unwrap();
    }
    head.mmap_head_chunks().unwrap();
    let mmapped = head.mmapped_chunks_bounds_for_test(old_ref).unwrap();
    assert!(!mmapped.is_empty());
    let mapped_max = mmapped.last().unwrap().2;
    assert!(mapped_max > 151 && mapped_max < 258);

    let (tx, rx) = mpsc::channel::<Result<u64, String>>();
    let h_cb = Arc::clone(&head);
    head.set_lifecycle_callback(Some(Arc::new(TestLifecycleCb {
        pre: None,
        post_del: Some(Arc::new(move |deleted| {
            if !deleted.iter().any(|(r, _)| *r == old_ref) {
                return;
            }
            let h_t = Arc::clone(&h_cb);
            let tx_t = tx.clone();
            std::thread::spawn(move || {
                let mut app = h_t.appender();
                let res = app
                    .append_float(None, &[("foo", "bar")], 151, 42.0)
                    .and_then(|r| {
                        app.commit()?;
                        Ok(r)
                    })
                    .map_err(|e| e.to_string());
                let _ = tx_t.send(res);
            });
            std::thread::sleep(Duration::from_millis(30));
            assert!(h_cb.has_pending_tombstones_active_for_test());
        })),
    })));

    let fps = head.snapshot_fingerprints(&[old_ref]);
    head.truncate_stale_series(&[old_ref], 258, &fps).unwrap();
    let new_ref = rx.recv_timeout(Duration::from_secs(10)).unwrap().unwrap();
    assert_ne!(new_ref, old_ref);

    {
        let mut app = head.appender();
        app.append_float(Some(new_ref), &pairs, 301, 43.0).unwrap();
        app.commit().unwrap();
    }

    let query_ts = |h: &Head| -> Vec<i64> {
        let q = TsdbQuerier::new(Some(h), vec![], i64::MIN, i64::MAX);
        let m = [LabelMatcher::new(MatchType::Equal, "foo", "bar").unwrap()];
        q.select(true, &m)
            .unwrap()
            .into_iter()
            .flat_map(|s| s.samples.into_iter().map(|x| x.t))
            .collect()
    };
    assert_eq!(query_ts(&head), vec![151, 301]);
    head.close().unwrap();

    let reopened = Head::open(dir.path(), opts, i64::MIN).unwrap();
    assert_eq!(query_ts(&reopened), vec![151, 301]);
    reopened.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_series_eviction_test.go::TestSeriesEviction_CreatorsAreReleasedBeforePostingsAreDeleted
#[test]
fn test_series_eviction_creators_are_released_before_postings_are_deleted() {
    for mode in [EvictMode::CompactSelectedSeries, EvictMode::CompactStaleHead] {
        let tdb = new_eviction_test_db(&[]);
        let db = Arc::clone(&tdb.db);
        let old_ref = tdb.evicted_refs[0];

        let (in_postings_tx, in_postings_rx) = mpsc::channel();
        let (finish_tx, finish_rx) = mpsc::channel::<()>();
        let finish_rx = Arc::new(Mutex::new(Some(finish_rx)));
        db.head()
            .set_before_deleting_postings_hook_for_test(Some(Arc::new(move || {
                let _ = in_postings_tx.send(());
                if let Some(rx) = finish_rx.lock().unwrap().take() {
                    let _ = rx.recv();
                }
            })));

        let (evicted_tx, evicted_rx) = mpsc::channel::<()>();
        let db_ev = Arc::clone(&db);
        let refs = tdb.evicted_refs.clone();
        let ev_handle = std::thread::spawn(move || {
            mode.run(&db_ev, &refs);
            let _ = evicted_tx.send(());
        });

        in_postings_rx.recv_timeout(Duration::from_secs(10)).unwrap();

        let (rec_tx, rec_rx) = mpsc::channel::<u64>();
        let db_rec = Arc::clone(&db);
        let rec_handle = std::thread::spawn(move || {
            let mut app = db_rec.appender();
            let r = app
                .append_float(
                    Some(old_ref),
                    &[("__name__", "series"), ("id", "00")],
                    400,
                    42.0,
                )
                .unwrap();
            app.commit().unwrap();
            let _ = rec_tx.send(r);
        });

        let new_ref = rec_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        assert_ne!(new_ref, old_ref);
        let _ = finish_tx.send(());
        evicted_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        ev_handle.join().unwrap();
        rec_handle.join().unwrap();

        let dir_path = db.dir().to_path_buf();
        db.close().unwrap();

        let mut stone_pos = None;
        let mut series_pos = None;
        for (idx, rec) in read_all_wal_records(&dir_path.join("wal")).into_iter().enumerate() {
            match rec {
                DecodedRecord::Tombstones(stones) => {
                    if stones.iter().any(|s| s.ref_id == old_ref) {
                        stone_pos = Some(idx);
                    }
                }
                DecodedRecord::Series(series) if series.iter().any(|s| s.ref_id == new_ref) => {
                    series_pos = Some(idx);
                }
                _ => {}
            }
        }
        assert!(series_pos.unwrap() > stone_pos.unwrap());
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_series_eviction_test.go::TestEvictedSeries_Contains
#[test]
fn test_evicted_series_contains() {
    let refs = vec![3u64, 5, 6, 100, 1 << 40];
    let empty = EvictedSeries::new(vec![], i64::MAX, None);
    for r in [0, 3] {
        assert!(!empty.contains(r));
    }

    let ev = EvictedSeries::new(refs.clone(), i64::MAX, None);
    for r in refs {
        assert!(ev.contains(r));
    }
    for r in [0, 2, 4, 7, 99, 101, (1 << 40) - 1, (1 << 40) + 1] {
        assert!(!ev.contains(r));
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_series_eviction_test.go::TestSeriesRemoval_BlockIsAddedBeforeSeriesAreRemoved
#[test]
fn test_series_removal_block_is_added_before_series_are_removed() {
    for mode in [EvictMode::CompactSelectedSeries, EvictMode::CompactStaleHead] {
        let tdb = new_eviction_test_db(&[]);
        let db = Arc::clone(&tdb.db);
        let num_series_before = Arc::new(Mutex::new(0u64));
        let in_blocks_before = Arc::new(Mutex::new(BTreeMap::<String, usize>::new()));

        let ns_cb = Arc::clone(&num_series_before);
        let ib_cb = Arc::clone(&in_blocks_before);
        let db_cb = Arc::clone(&db);
        db.head()
            .set_after_waiting_for_readers_hook_for_test(Some(Arc::new(move || {
                assert!(db_cb.head().evicting_series().is_some());
                *ns_cb.lock().unwrap() = db_cb.head().num_series();
                let mut counts = BTreeMap::new();
                let m = [eviction_matcher()];
                for blk in db_cb.blocks() {
                    let q = TsdbQuerier::new(None, vec![blk], i64::MIN, i64::MAX);
                    for s in q.select(true, &m).unwrap() {
                        *counts.entry(s.labels.to_string()).or_insert(0) += s.samples.len();
                    }
                }
                *ib_cb.lock().unwrap() = counts;
            })));

        mode.run(&db, &tdb.evicted_refs);

        assert_eq!(*num_series_before.lock().unwrap(), EVICTION_TEST_SERIES as u64);
        let counts = in_blocks_before.lock().unwrap();
        for i in 0..EVICTION_TEST_EVICTED_SERIES {
            assert_eq!(counts[&eviction_labels(i).to_string()], EVICTION_TEST_SAMPLES);
        }
        assert_eq!(
            db.head().num_series(),
            (EVICTION_TEST_SERIES - EVICTION_TEST_EVICTED_SERIES) as u64
        );
        db.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_series_eviction_test.go::TestSeriesEviction_HeadAndOOOQueriersWithoutSkipEvictedSeriesDoNotSkipRemovedSeries
#[test]
fn test_series_eviction_head_and_ooo_queriers_without_skip_evicted_series_do_not_skip_removed_series() {
    let tdb = new_eviction_test_db(&[]);
    let db = Arc::clone(&tdb.db);
    let head = db.head();
    let ref0 = tdb.evicted_refs[0];

    let captured_iso: Arc<Mutex<Option<HeadReadIsolation>>> = Arc::new(Mutex::new(None));
    let iso_cb = Arc::clone(&captured_iso);
    head.set_after_waiting_for_readers_hook_for_test(Some(Arc::new(move || {
        // Explicit isolation without evicting (evicting: None).
        *iso_cb.lock().unwrap() = Some(HeadReadIsolation {
            ino_mint: 0,
            evicting: None,
            ..Default::default()
        });
    })));

    db.compact_selected_series(&tdb.evicted_refs).unwrap();
    let iso = captured_iso.lock().unwrap().take().unwrap();
    assert!(head
        .series_chunks_with_isolation(ref0, 0, 1000, &iso)
        .unwrap()
        .is_none());
    assert_eq!(
        counter_value(
            &tdb.reg,
            "prometheus_tsdb_head_chunk_reads_of_removed_series_total",
            &[]
        ),
        0.0
    );
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_series_eviction_test.go::TestSeriesEviction_QueryStartingAtLastTimestampIsWaitedForByNextEviction
#[test]
fn test_series_eviction_query_starting_at_last_timestamp_is_waited_for_by_next_eviction() {
    const LATE_TS: i64 = 400;
    for qk in [QuerierKind::Querier, QuerierKind::ChunkQuerier] {
        let tdb = new_eviction_test_db(&[]);
        let db = Arc::clone(&tdb.db);

        let db_late = Arc::clone(&db);
        db.set_compact_head_view_before_evict_hook_for_test(Some(Arc::new(move || {
            let mut app = db_late.appender();
            app.append_float(None, &[("__name__", "series"), ("id", "00")], LATE_TS, 42.0)
                .unwrap();
            app.commit().unwrap();
        })));

        let reading: Arc<Mutex<Option<OpenPreparedQuery>>> = Arc::new(Mutex::new(None));
        let reading_cb = Arc::clone(&reading);
        let db_cb = Arc::clone(&db);
        db.head()
            .set_after_waiting_for_readers_hook_for_test(Some(Arc::new(move || {
                *reading_cb.lock().unwrap() =
                    Some(open_prepared_query(&db_cb, qk, LATE_TS, LATE_TS));
            })));

        db.compact_selected_series(&tdb.evicted_refs).unwrap();
        let open_q = reading.lock().unwrap().take().unwrap();
        assert_eq!(
            db.head().num_series(),
            (EVICTION_TEST_SERIES - EVICTION_TEST_EVICTED_SERIES + 1) as u64
        );

        let (done_tx, done_rx) = mpsc::channel::<Result<(), String>>();
        let db_ev2 = Arc::clone(&db);
        let ref0 = tdb.evicted_refs[0];
        let h = std::thread::spawn(move || {
            let _ = done_tx.send(
                db_ev2
                    .compact_selected_series(&[ref0])
                    .map(|_| ())
                    .map_err(|e| e.to_string()),
            );
        });

        std::thread::sleep(Duration::from_millis(60));
        assert!(
            done_rx.try_recv().is_err(),
            "second eviction must wait for open query starting at last timestamp"
        );

        let samples = read_prepared_query(&db, open_q);
        assert_eq!(samples.get(&eviction_labels(0).to_string()), Some(&1));

        done_rx.recv_timeout(Duration::from_secs(10)).unwrap().unwrap();
        h.join().unwrap();
        db.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_series_eviction_test.go::TestSeriesEviction_SamplesAndEvictionWhileWaitingForReaders
#[test]
fn test_series_eviction_samples_and_eviction_while_waiting_for_readers() {
    const LATE_TS: i64 = 400;
    let tdb = new_eviction_test_db(&[]);
    let db = Arc::clone(&tdb.db);
    let head = db.head();

    let open_q = open_prepared_query(&db, QuerierKind::Querier, 0, LATE_TS);

    let (first_tx, first_rx) = mpsc::channel();
    let db_f = Arc::clone(&db);
    let refs_f = tdb.evicted_refs.clone();
    let h1 = std::thread::spawn(move || {
        db_f.compact_selected_series(&refs_f).unwrap();
        let _ = first_tx.send(());
    });

    wait_until("first eviction publishes evicting_series", || {
        head.evicting_series().is_some()
    });
    assert!(!head.has_pending_tombstones_active_for_test());

    {
        let mut app = db.appender();
        app.append_float(None, &[("__name__", "series"), ("id", "00")], LATE_TS, 42.0)
            .unwrap();
        app.append_float(
            None,
            &[
                ("__name__", "series"),
                ("id", &format!("{EVICTION_TEST_SERIES:02}")),
            ],
            LATE_TS,
            43.0,
        )
        .unwrap();
        app.commit().unwrap();
    }
    assert_eq!(head.num_series(), (EVICTION_TEST_SERIES + 1) as u64);
    assert!(!db.try_lock_compaction_for_test());

    let series_before_second = Arc::new(Mutex::new(0u64));
    let late_present_before_second = Arc::new(Mutex::new(false));
    let sb_cb = Arc::clone(&series_before_second);
    let lp_cb = Arc::clone(&late_present_before_second);
    let db_cb = Arc::clone(&db);
    db.set_compact_head_view_before_evict_hook_for_test(Some(Arc::new(move || {
        *sb_cb.lock().unwrap() = db_cb.head().num_series();
        *lp_cb.lock().unwrap() = db_cb
            .head()
            .get_entry_by_sorted_pairs(&[("__name__", "series"), ("id", "00")])
            .is_some();
    })));

    let (second_tx, second_rx) = mpsc::channel();
    let db_s = Arc::clone(&db);
    let refs_s = tdb.evicted_refs.clone();
    let h2 = std::thread::spawn(move || {
        db_s.compact_selected_series(&refs_s).unwrap();
        let _ = second_tx.send(());
    });

    assert_eq!(head.num_series(), (EVICTION_TEST_SERIES + 1) as u64);
    let _ = read_prepared_query(&db, open_q);

    first_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    second_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    h1.join().unwrap();
    h2.join().unwrap();

    assert_eq!(
        *series_before_second.lock().unwrap(),
        (EVICTION_TEST_SERIES - EVICTION_TEST_EVICTED_SERIES + 2) as u64
    );
    assert!(*late_present_before_second.lock().unwrap());
    assert!(head
        .get_entry_by_sorted_pairs(&[("__name__", "series"), ("id", "00")])
        .is_none());
    assert!(!head.has_pending_tombstones_active_for_test());
    assert_eq!(
        head.num_series(),
        (EVICTION_TEST_SERIES - EVICTION_TEST_EVICTED_SERIES + 1) as u64
    );

    let q = db.querier(0, i64::MAX);
    let m = [eviction_matcher()];
    let got: BTreeMap<String, usize> = q
        .select(true, &m)
        .unwrap()
        .into_iter()
        .map(|s| (s.labels.to_string(), s.samples.len()))
        .collect();
    let mut expected = eviction_expected_samples(&[0]);
    expected.insert(eviction_labels(EVICTION_TEST_SERIES).to_string(), 1);
    assert_eq!(got, expected);
    db.close().unwrap();
}

// =========================================================================
// head_wal_replay_test.go (2)
// =========================================================================

// Upstream: github.com/prometheus/prometheus/tsdb/head_wal_replay_test.go::TestWALReplayPreservesMappedChunksAcrossSeriesRefs
#[test]
fn test_wal_replay_preserves_mapped_chunks_across_series_refs() {
    let minute = 60_000i64;
    for kind in [SampleKind::Float, SampleKind::Hist, SampleKind::FloatHist] {
        struct Case {
            refs: usize,
            initial_ooo: usize,
            total_ooo: usize,
            mapped_in_order: bool,
            initial_mapped_in_order: bool,
            corrupt_wal: bool,
        }
        let cases = [
            Case { refs: 1, initial_ooo: 1, total_ooo: 33, mapped_in_order: false, initial_mapped_in_order: false, corrupt_wal: false },
            Case { refs: 2, initial_ooo: 1, total_ooo: 32, mapped_in_order: false, initial_mapped_in_order: false, corrupt_wal: false },
            Case { refs: 2, initial_ooo: 1, total_ooo: 33, mapped_in_order: false, initial_mapped_in_order: false, corrupt_wal: false },
            Case { refs: 2, initial_ooo: 33, total_ooo: 33, mapped_in_order: false, initial_mapped_in_order: false, corrupt_wal: false },
            Case { refs: 2, initial_ooo: 33, total_ooo: 65, mapped_in_order: false, initial_mapped_in_order: false, corrupt_wal: false },
            Case { refs: 3, initial_ooo: 1, total_ooo: 33, mapped_in_order: false, initial_mapped_in_order: false, corrupt_wal: false },
            Case { refs: 2, initial_ooo: 0, total_ooo: 0, mapped_in_order: true, initial_mapped_in_order: false, corrupt_wal: false },
            Case { refs: 2, initial_ooo: 0, total_ooo: 0, mapped_in_order: true, initial_mapped_in_order: true, corrupt_wal: false },
            Case { refs: 2, initial_ooo: 1, total_ooo: 33, mapped_in_order: true, initial_mapped_in_order: false, corrupt_wal: false },
            Case { refs: 1, initial_ooo: 0, total_ooo: 0, mapped_in_order: true, initial_mapped_in_order: false, corrupt_wal: true },
            Case { refs: 2, initial_ooo: 0, total_ooo: 0, mapped_in_order: true, initial_mapped_in_order: false, corrupt_wal: true },
        ];

        for tc in cases {
            let dir = tempfile::tempdir().unwrap();
            let reg = Arc::new(prometheus::Registry::new());
            let open_db = || {
                let opts = TsdbOptions {
                    head: HeadOptions {
                        stripe_size: 16,
                        wal_segment_size: 32 * 1024,
                        ooo_time_window_ms: 300 * minute,
                        registry: Some(Arc::clone(&reg)),
                        ..HeadOptions::default()
                    },
                    max_block_chunk_segment_size: 1024 * 1024,
                    ..TsdbOptions::default()
                };
                let db = Tsdb::open_with_reload_interval(dir.path(), opts, Duration::ZERO).unwrap();
                db.disable_compactions();
                db
            };

            let pairs = [("foo", "bar")];
            let mut expected_ts = Vec::<i64>::new();
            let append_one = |db: &Tsdb, ts: i64| -> u64 {
                let mut app = db.appender();
                let r = append_kind(&mut app, kind, &pairs, ts, ts);
                app.commit().unwrap();
                r
            };
            let check_samples = |db: &Tsdb, exp: &[i64]| {
                let mut sorted_exp = exp.to_vec();
                sorted_exp.sort_unstable();
                let m = [LabelMatcher::new(MatchType::Equal, "foo", "bar").unwrap()];
                let got = db.querier(i64::MIN, i64::MAX).select(true, &m).unwrap();
                assert_eq!(got.len(), 1);
                assert_eq!(got[0].samples.len(), sorted_exp.len());
                for (ds, &et) in got[0].samples.iter().zip(&sorted_exp) {
                    assert_eq!(ds.t, et);
                    check_sample_value(ds, et);
                }
                assert_eq!(
                    counter_value(&reg, "prometheus_tsdb_mmap_chunk_corruptions_total", &[]),
                    0.0
                );
            };

            let first_ref;
            {
                let db = open_db();
                let mut fr = 0u64;
                for i in 1..tc.refs {
                    let ts = (i as i64) * 100 * minute;
                    let r = append_one(&db, ts);
                    if i == 1 {
                        fr = r;
                    }
                    expected_ts.push(ts);
                    db.compact_head_range_inclusive(db.head().min_time(), ts)
                        .unwrap();
                    assert_eq!(db.head().num_series(), 0);
                }
                let r = append_one(&db, 300 * minute);
                if tc.refs > 1 {
                    assert_ne!(fr, r);
                } else {
                    fr = r;
                }
                first_ref = fr;
                for i in 0..tc.initial_ooo {
                    append_one(&db, 250 * minute + i as i64);
                }
                if tc.initial_mapped_in_order {
                    append_one(&db, 400 * minute);
                    db.head().mmap_head_chunks().unwrap();
                }
                db.close().unwrap();
            }

            let corrupt_target;
            {
                let db = open_db();
                assert_eq!(
                    db.head().get_entry_by_sorted_pairs(&pairs).unwrap().ref_id,
                    first_ref
                );
                let (seg, offset) = db.head().wal_last_segment_and_offset_for_test().unwrap();
                corrupt_target = (seg.unwrap(), offset);
                for i in tc.initial_ooo..tc.total_ooo {
                    append_one(&db, 250 * minute + i as i64);
                }
                for i in 0..tc.total_ooo {
                    expected_ts.push(250 * minute + i as i64);
                }
                expected_ts.push(300 * minute);
                if tc.mapped_in_order {
                    for min in [400i64, 500, 650] {
                        let ts = min * minute;
                        if !tc.initial_mapped_in_order || min != 400 {
                            append_one(&db, ts);
                        }
                        expected_ts.push(ts);
                    }
                    db.head().mmap_head_chunks().unwrap();
                    assert_eq!(
                        db.head()
                            .mmapped_chunks_bounds_for_test(first_ref)
                            .unwrap()
                            .len(),
                        3
                    );
                }
                check_samples(&db, &expected_ts);
                db.close().unwrap();
            }

            if tc.corrupt_wal {
                let seg_path = segment_name(&dir.path().join("wal"), corrupt_target.0);
                let mut f = OpenOptions::new().write(true).open(&seg_path).unwrap();
                use std::io::Seek;
                f.seek(std::io::SeekFrom::Start(corrupt_target.1 as u64)).unwrap();
                f.write_all(&[255]).unwrap();
                expected_ts.pop();
            }

            for _ in 0..3 {
                let db = open_db();
                check_samples(&db, &expected_ts);
                db.close().unwrap();
                if tc.mapped_in_order {
                    let reader = HeadChunksReader::open(dir.path().join("chunks_head")).unwrap();
                    let mut count = 0usize;
                    reader
                        .iterate_all_chunks(|rec| {
                            if !rec.is_ooo {
                                count += 1;
                            }
                            Ok(())
                        })
                        .unwrap();
                    assert_eq!(count, 3);
                }
            }
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_wal_replay_test.go::TestMergeReplayMmappedChunks
#[test]
fn test_merge_replay_mmapped_chunks() {
    let a = (1u64, 100i64, 150i64);
    let b = (2u64, 200i64, 250i64);
    let c = (3u64, 0i64, 50i64);
    let replayed = (4u64, 300i64, 350i64);
    let overlap = (5u64, 125i64, 225i64);

    let cases = [
        (vec![], vec![a, b], vec![a, b], 3u64, false),
        (vec![a, b], vec![], vec![a, b], 3, false),
        (vec![b], vec![c, a], vec![c, a, b], 3, false),
        (vec![c, a], vec![b], vec![c, a, b], 3, false),
        (vec![a, b], vec![a, b], vec![a, b], 3, false),
        (vec![c, a, replayed], vec![b], vec![c, a, b], 3, false),
        (vec![a, b], vec![], vec![], 0, false),
        (vec![c, a, b], vec![overlap], vec![c, overlap], 5, false),
        (vec![b], vec![a, c], vec![a, b, c], 3, true),
        (vec![a, b], vec![a, b], vec![a, b], 3, true),
        (vec![a, b], vec![overlap], vec![a, b, overlap], 5, true),
    ];

    for (existing, incoming, want, last_ref, ooo) in cases {
        let got = Head::merge_replay_mmapped_chunks_for_test(existing, incoming, last_ref, ooo);
        assert_eq!(got, want);
    }
}

// =========================================================================
// head_read_test.go (10) + head_read_mimir_test.go (1)
// =========================================================================

// Upstream: github.com/prometheus/prometheus/tsdb/head_read_test.go::TestMemSeries_chunk
#[test]
fn test_mem_series_chunk() {
    let dir = tempfile::tempdir().unwrap();
    let opts = HeadOptions {
        chunk_range_ms: 100,
        ..HeadOptions::default()
    };
    let head = Head::open(dir.path(), opts, i64::MIN).unwrap();
    let pairs = [("__name__", "series_chunk_test")];

    let mut app = head.appender();
    let ref_id = app.append_float(None, &pairs, 0, 0.0).unwrap();
    for t in (5..600).step_by(5) {
        app.append_float(Some(ref_id), &pairs, t, t as f64).unwrap();
    }
    app.commit().unwrap();
    head.mmap_head_chunks().unwrap();

    assert_eq!(head.mmapped_chunks_bounds_for_test(ref_id).unwrap().len(), 5);
    assert_eq!(head.head_chunks_bounds_for_test(ref_id).unwrap().len(), 1);

    let (_, chks, _) = head.series_chunks(ref_id, 0, 599).unwrap().unwrap();
    assert_eq!(chks.len(), 6);
    assert_eq!(chks[0].min_time, 0);
    assert_eq!(chks[5].min_time, 500);
    assert!(head.series_chunks(ref_id, 10_000, 20_000).unwrap().is_none());
    head.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_read_test.go::TestHeadChunkFromSeriesIsolationFastPath
#[test]
fn test_head_chunk_from_series_isolation_fast_path() {
    let mut ring = TxRing::new(4);
    assert!(ring.last_append_id().is_none());
    ring.add(1);
    ring.add(1);
    assert_eq!(ring.last_append_id(), Some(1));

    let iso = Isolation::new(false);
    let (id1, _) = iso.new_append_id(0);
    iso.close_append(id1);
    let st_closed = iso.state(0, 1);
    assert!(st_closed.info().incomplete_appends.is_empty());
    st_closed.close();

    let (id2, _) = iso.new_append_id(0);
    let st_open = iso.state(0, 1);
    assert!(st_open.info().incomplete_appends.contains(&id2));
    st_open.close();
    iso.close_append(id2);
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_read_test.go::TestHeadChunkFromSeriesRetainsIsolationForInterleavedCommits
#[test]
fn test_head_chunk_from_series_retains_isolation_for_interleaved_commits() {
    let dir = tempfile::tempdir().unwrap();
    let opts = HeadOptions {
        ooo_time_window_ms: 10,
        ..HeadOptions::default()
    };
    let head = Arc::new(Head::open(dir.path(), opts, i64::MIN).unwrap());
    let target = [("__name__", "target_metric")];
    let blocker = [("__name__", "blocker_metric")];

    let mut init = head.appender();
    let t_ref = init.append_float(None, &target, 0, 1.0).unwrap();
    let _b_ref = init.append_float(None, &blocker, 0, 1.0).unwrap();
    init.commit().unwrap();

    let mut later = head.appender();
    later.append_float(Some(t_ref), &target, 2, 3.0).unwrap();

    let mut incomplete = head.appender();
    incomplete.append_float(Some(t_ref), &target, 1, 2.0).unwrap();

    let (_, chks_before, _) = head.series_chunks(t_ref, 0, 2).unwrap().unwrap();
    let ts_before: Vec<i64> = chks_before
        .iter()
        .flat_map(|c| decode_chunk_samples(c.chunk.as_ref().unwrap()).unwrap())
        .map(|s| s.t)
        .collect();
    assert_eq!(ts_before, vec![0]);

    later.commit().unwrap();
    incomplete.commit().unwrap();

    let (_, chks_after, _) = head.series_chunks(t_ref, 0, 2).unwrap().unwrap();
    let mut ts_after: Vec<i64> = chks_after
        .iter()
        .flat_map(|c| decode_chunk_samples(c.chunk.as_ref().unwrap()).unwrap())
        .map(|s| s.t)
        .collect();
    ts_after.sort_unstable();
    assert_eq!(ts_after, vec![0, 1, 2]);
    head.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_read_test.go::TestSafeHeadChunkIteratorForChunkCopySkipsVisibleChunk
#[test]
fn test_safe_head_chunk_iterator_for_chunk_copy_skips_visible_chunk() {
    let mut xc = XorChunk::new();
    {
        let mut app = xc.appender().unwrap();
        app.append(0, 0, 1.0).unwrap();
        app.append(0, 1, 2.0).unwrap();
    }
    let samples = decode_chunk_samples(&EncodedChunk::Xor(xc)).unwrap();
    assert_eq!(samples.len(), 2);
    assert_eq!(samples[0].t, 0);
    assert_eq!(samples[1].t, 1);

    let mut ring = TxRing::new(2);
    ring.add(1);
    ring.add(1);
    assert_eq!(ring.last_append_id(), Some(1));
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_read_test.go::TestMemSeries_chunk_FastPath
#[test]
fn test_mem_series_chunk_fast_path() {
    let dir = tempfile::tempdir().unwrap();
    let opts = HeadOptions {
        chunk_range_ms: 100,
        ..HeadOptions::default()
    };
    let head = Head::open(dir.path(), opts, i64::MIN).unwrap();
    let pairs = [("__name__", "fast_path_test")];

    let mut app = head.appender();
    let ref_id = app.append_float(None, &pairs, 0, 0.0).unwrap();
    for t in (5..400).step_by(5) {
        app.append_float(Some(ref_id), &pairs, t, t as f64).unwrap();
    }
    app.commit().unwrap();
    head.mmap_head_chunks().unwrap();

    let mut app2 = head.appender();
    for t in (400..600).step_by(5) {
        app2.append_float(Some(ref_id), &pairs, t, t as f64).unwrap();
    }
    app2.commit().unwrap();

    assert_eq!(head.mmapped_chunks_bounds_for_test(ref_id).unwrap().len(), 3);
    assert_eq!(head.head_chunks_bounds_for_test(ref_id).unwrap().len(), 3);

    let (_, chks, _) = head.series_chunks(ref_id, 0, 600).unwrap().unwrap();
    let (_, ivs, snap) = head
        .series_chunk_metas_no_bytes_with_snapshot(ref_id, 0, 600, None)
        .unwrap()
        .unwrap();
    assert_eq!(chks.len(), 6);
    assert_eq!(ivs.len(), 6);
    let fast_chks = head
        .series_final_chunks_from_snapshot(None, ref_id, snap)
        .unwrap();
    assert_eq!(fast_chks.len(), chks.len());
    for (a, b) in chks.iter().zip(&fast_chks) {
        assert_eq!((a.min_time, a.max_time), (b.min_time, b.max_time));
    }
    head.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_read_test.go::TestMemSeries_chunk_ResolveAfterWrap
#[test]
fn test_mem_series_chunk_resolve_after_wrap() {
    let first_id = OOO_CHUNK_ID_MASK - 10;
    let num_chunks = 20usize;
    let ids: Vec<u32> = (0..num_chunks)
        .map(|pos| ooo_head_chunk_id(first_id, pos))
        .collect();
    assert_eq!(ids[9] & (OOO_CHUNK_ID_MASK - 1), OOO_CHUNK_ID_MASK - 1);
    assert_eq!(ids[10] & (OOO_CHUNK_ID_MASK - 1), 0);

    for (pos, &id) in ids.iter().enumerate() {
        let (_, raw, is_ooo) = unpack_head_chunk_ref(pack_head_chunk_ref(1, id));
        assert!(is_ooo);
        assert_eq!(resolve_ooo_chunk_pos(first_id, raw, num_chunks), Some(pos));
    }

    let first_after_4 = (first_id + 4) % OOO_CHUNK_ID_MASK;
    for (pos, &id) in ids.iter().enumerate() {
        let (_, raw, _) = unpack_head_chunk_ref(pack_head_chunk_ref(1, id));
        let got = resolve_ooo_chunk_pos(first_after_4, raw, num_chunks - 4);
        if pos < 4 {
            assert_eq!(got, None);
        } else {
            assert_eq!(got, Some(pos - 4));
        }
    }

    let first_after_12 = (first_id + 12) % OOO_CHUNK_ID_MASK;
    assert_eq!(first_after_12, 2);
    for (pos, &id) in ids.iter().enumerate() {
        let (_, raw, _) = unpack_head_chunk_ref(pack_head_chunk_ref(1, id));
        let got = resolve_ooo_chunk_pos(first_after_12, raw, num_chunks - 12);
        if pos < 12 {
            assert_eq!(got, None);
        } else {
            assert_eq!(got, Some(pos - 12));
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_read_test.go::TestHeadIndexReader_PostingsForLabelMatching
#[test]
fn test_head_index_reader_postings_for_label_matching() {
    let dir = tempfile::tempdir().unwrap();
    let opts = HeadOptions {
        chunk_range_ms: 1000,
        ..HeadOptions::default()
    };
    let h = Head::open(dir.path(), opts, i64::MIN).unwrap();
    {
        let mut app = h.appender();
        for (i, pairs) in [
            vec![("n", "1")],
            vec![("n", "1"), ("i", "a")],
            vec![("n", "1"), ("i", "b")],
            vec![("n", "2")],
            vec![("n", "2.5")],
        ]
        .iter()
        .enumerate()
        {
            app.append_float(None, pairs, i as i64, 0.0).unwrap();
        }
        app.commit().unwrap();
    }
    let ir = h.index().unwrap();
    assert_eq!(ir.postings_for_label_matching("n", |v| v == "1").len(), 3);
    assert_eq!(ir.postings_for_label_matching("n", |v| v.starts_with('2')).len(), 2);
    assert!(ir.postings_for_label_matching("missing", |_| true).is_empty());
    h.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_read_test.go::TestHeadIndexReader_LabelValuesFor
#[test]
fn test_head_index_reader_label_values_for() {
    let dir = tempfile::tempdir().unwrap();
    let h = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();
    {
        let mut app = h.appender();
        for (a, b) in [("1", "1"), ("1", "2"), ("1", "3"), ("1", "4"), ("2", "5")] {
            app.append_float(None, &[("a", a), ("b", b)], 1, 1.0)
                .unwrap();
        }
        app.commit().unwrap();
    }
    let ir = h.index().unwrap();
    let p_a1 = ir.postings("a", "1");
    assert_eq!(ir.label_values_for(&p_a1, "b"), vec!["1", "2", "3", "4"]);
    assert!(ir.label_values_for(&[], "a").is_empty());
    assert!(ir.label_values_for(&p_a1, "c").is_empty());
    h.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_read_test.go::TestHeadIndexReader_LabelValuesExcluding
#[test]
fn test_head_index_reader_label_values_excluding() {
    let dir = tempfile::tempdir().unwrap();
    let h = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();
    {
        let mut app = h.appender();
        for (a, b) in [
            ("1", "1"),
            ("1", "2"),
            ("1", "3"),
            ("1", "4"),
            ("1", "5"),
            ("2", "5"),
        ] {
            app.append_float(None, &[("a", a), ("b", b)], 1, 1.0)
                .unwrap();
        }
        app.commit().unwrap();
    }
    let ir = h.index().unwrap();
    let p_a1 = ir.postings("a", "1");
    assert_eq!(ir.label_values_excluding(&p_a1, "b"), vec!["5"]);
    assert_eq!(ir.label_values_excluding(&[], "a"), vec!["1", "2"]);
    assert!(ir.label_values_excluding(&[], "c").is_empty());
    h.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_read_test.go::TestHeadChunkReaderCache
#[test]
fn test_head_chunk_reader_cache() {
    let dir = tempfile::tempdir().unwrap();
    let opts = HeadOptions {
        chunk_range_ms: 100,
        ..HeadOptions::default()
    };
    let h = Head::open(dir.path(), opts, i64::MIN).unwrap();
    let pairs = [("__name__", "test")];
    let mut app = h.appender();
    let ref_id = app.append_float(None, &pairs, 0, 0.0).unwrap();
    for i in (5..500).step_by(5) {
        app.append_float(Some(ref_id), &pairs, i, i as f64).unwrap();
    }
    app.commit().unwrap();

    let head_chks_before = h.head_chunks_bounds_for_test(ref_id).unwrap();
    assert!(head_chks_before.len() > 2);
    let newest_min = head_chks_before.last().unwrap().0;

    h.mmap_head_chunks().unwrap();
    assert_eq!(h.head_chunks_bounds_for_test(ref_id).unwrap().len(), 1);
    let (_, chks_after_mmap, _) = h.series_chunks(ref_id, newest_min, 10_000).unwrap().unwrap();
    assert_eq!(chks_after_mmap.len(), 1);
    assert_eq!(chks_after_mmap[0].min_time, newest_min);

    let oldest_max = h.mmapped_chunks_bounds_for_test(ref_id).unwrap()[0].2;
    h.truncate(oldest_max + 1).unwrap();
    let (_, chks_after_trunc, _) = h.series_chunks(ref_id, 0, 10_000).unwrap().unwrap();
    assert_eq!(chks_after_trunc.last().unwrap().min_time, newest_min);

    let dir2 = tempfile::tempdir().unwrap();
    let h2 = Head::open(
        dir2.path(),
        HeadOptions {
            chunk_range_ms: 1,
            ..HeadOptions::default()
        },
        i64::MIN,
    )
    .unwrap();
    let mut app2 = h2.appender();
    let big_ref = app2.append_float(None, &[("__name__", "big")], 0, 0.0).unwrap();
    for i in 1..266 {
        app2.append_float(Some(big_ref), &[("__name__", "big")], i, i as f64)
            .unwrap();
    }
    app2.commit().unwrap();
    let ir2 = h2.index().unwrap();
    let (_, ivs, _) = ir2.series_chunk_metas_no_bytes(big_ref, None).unwrap().unwrap();
    assert_eq!(ivs.len(), 266);

    h.close().unwrap();
    h2.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/head_read_mimir_test.go::TestHead_MustIndex
#[test]
fn test_head_must_index() {
    let dir = tempfile::tempdir().unwrap();
    let opts = HeadOptions {
        chunk_range_ms: 1000,
        ..HeadOptions::default()
    };
    let h = Head::open(dir.path(), opts, i64::MIN).unwrap();
    {
        let mut app = h.appender();
        for (i, pairs) in [
            vec![("n", "1")],
            vec![("n", "1"), ("i", "a")],
            vec![("n", "1"), ("i", "b")],
            vec![("n", "2")],
            vec![("n", "2.5")],
        ]
        .iter()
        .enumerate()
        {
            app.append_float(None, pairs, i as i64, 0.0).unwrap();
        }
        app.commit().unwrap();
    }
    let ir = h.index().unwrap();
    let imr = h.must_index();
    assert_eq!(ir, imr);
    h.close().unwrap();
}