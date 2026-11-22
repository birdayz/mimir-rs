#![allow(dead_code)]

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};
use mimir_tsdb::compact::encode_iterable_chunks;
use mimir_tsdb::head::{Head, HeadError, SeriesLifecycleCallback};
use mimir_tsdb::wlog::last_checkpoint;

use mimir_tsdb::block::{BlockMetaCompaction, BlockStats};
use mimir_tsdb::chunkenc::{
    is_stale_nan, stale_nan, CounterResetHint, EncodedChunk, FloatHistogram, Histogram,
    HistogramSpan, XorChunk, CUSTOM_BUCKETS_SCHEMA,
};
use mimir_tsdb::chunks_block::ChunkMeta;
use mimir_tsdb::compact::{
    decode_chunk_samples, write_block_from_series, CompactionSeries, DecodedSample, SampleValue,
};
use mimir_tsdb::db::{Tsdb, TsdbOptions, TsdbRuntimeConfig};
use mimir_tsdb::head::HeadAppender;
use mimir_tsdb::labels::Labels;
use mimir_tsdb::matcher::{LabelMatcher, MatchType};
use mimir_tsdb::record::{DecodedRecord, RecordDecoder};
use mimir_tsdb::wlog::WlogReader;

pub const HOUR_MS: i64 = 60 * 60 * 1000;
pub const DAY_MS: i64 = 24 * HOUR_MS;
pub const DEFAULT_BLOCK_DURATION_MS: i64 = 2 * HOUR_MS;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SampleKind {
    Float,
    IntHist,
    FloatHist,
    CustomBucketsIntHist,
    CustomBucketsFloatHist,
    GaugeIntHist,
    GaugeFloatHist,
}

impl SampleKind {
    pub fn name(self) -> &'static str {
        match self {
            Self::Float => "float",
            Self::IntHist => "integer histogram",
            Self::FloatHist => "float histogram",
            Self::CustomBucketsIntHist => "custom buckets int histogram",
            Self::CustomBucketsFloatHist => "custom buckets float histogram",
            Self::GaugeIntHist => "gauge int histogram",
            Self::GaugeFloatHist => "gauge float histogram",
        }
    }

    pub fn metric_type(self) -> &'static str {
        match self {
            Self::Float => "float",
            _ => "histogram",
        }
    }
}

pub const ALL_SAMPLE_KINDS: [SampleKind; 7] = [
    SampleKind::Float,
    SampleKind::IntHist,
    SampleKind::FloatHist,
    SampleKind::CustomBucketsIntHist,
    SampleKind::CustomBucketsFloatHist,
    SampleKind::GaugeIntHist,
    SampleKind::GaugeFloatHist,
];

pub fn gen_test_hist(i: i64) -> Histogram {
    Histogram {
        counter_reset_hint: CounterResetHint::UnknownCounterReset,
        schema: 1,
        zero_threshold: 0.001,
        zero_count: (2 + i).max(0) as u64,
        count: (12 + i * 9).max(0) as u64,
        sum: 18.4 * ((i + 1) as f64),
        positive_spans: vec![
            HistogramSpan { offset: 0, length: 2 },
            HistogramSpan { offset: 1, length: 2 },
        ],
        negative_spans: vec![
            HistogramSpan { offset: 0, length: 2 },
            HistogramSpan { offset: 1, length: 2 },
        ],
        positive_buckets: vec![i + 1, 1, -1, 0],
        negative_buckets: vec![i + 1, 1, -1, 0],
        custom_values: vec![],
    }
}

pub fn gen_test_custom_buckets_hist(i: i64) -> Histogram {
    Histogram {
        counter_reset_hint: CounterResetHint::UnknownCounterReset,
        schema: CUSTOM_BUCKETS_SCHEMA,
        zero_threshold: 0.0,
        zero_count: 0,
        count: (5 + i * 4).max(0) as u64,
        sum: 18.4 * ((i + 1) as f64),
        positive_spans: vec![
            HistogramSpan { offset: 0, length: 2 },
            HistogramSpan { offset: 1, length: 2 },
        ],
        negative_spans: vec![],
        positive_buckets: vec![i + 1, 1, -1, 0],
        negative_buckets: vec![],
        custom_values: vec![0.0, 1.0, 2.0, 3.0, 4.0],
    }
}

pub fn gen_test_gauge_hist(i: i64) -> Histogram {
    let mut h = gen_test_hist(i);
    h.counter_reset_hint = CounterResetHint::GaugeType;
    h
}

pub fn gen_test_fhist(i: i64) -> FloatHistogram {
    FloatHistogram {
        counter_reset_hint: CounterResetHint::UnknownCounterReset,
        schema: 1,
        zero_threshold: 0.001,
        zero_count: (2 + i) as f64,
        count: (12 + i * 9) as f64,
        sum: 18.4 * ((i + 1) as f64),
        positive_spans: vec![
            HistogramSpan { offset: 0, length: 2 },
            HistogramSpan { offset: 1, length: 2 },
        ],
        negative_spans: vec![
            HistogramSpan { offset: 0, length: 2 },
            HistogramSpan { offset: 1, length: 2 },
        ],
        positive_buckets: vec![(i + 1) as f64, (i + 2) as f64, (i + 1) as f64, (i + 1) as f64],
        negative_buckets: vec![(i + 1) as f64, (i + 2) as f64, (i + 1) as f64, (i + 1) as f64],
        custom_values: vec![],
    }
}

pub fn gen_test_custom_buckets_fhist(i: i64) -> FloatHistogram {
    FloatHistogram {
        counter_reset_hint: CounterResetHint::UnknownCounterReset,
        schema: CUSTOM_BUCKETS_SCHEMA,
        zero_threshold: 0.0,
        zero_count: 0.0,
        count: (5 + i * 4) as f64,
        sum: 18.4 * ((i + 1) as f64),
        positive_spans: vec![
            HistogramSpan { offset: 0, length: 2 },
            HistogramSpan { offset: 1, length: 2 },
        ],
        negative_spans: vec![],
        positive_buckets: vec![(i + 1) as f64, (i + 2) as f64, (i + 1) as f64, (i + 1) as f64],
        negative_buckets: vec![],
        custom_values: vec![0.0, 1.0, 2.0, 3.0, 4.0],
    }
}

pub fn gen_test_gauge_fhist(i: i64) -> FloatHistogram {
    let mut fh = gen_test_fhist(i);
    fh.counter_reset_hint = CounterResetHint::GaugeType;
    fh
}

pub fn make_sample_kind(kind: SampleKind, ts: i64, value: i64) -> DecodedSample {
    let v = match kind {
        SampleKind::Float => SampleValue::Float(value as f64),
        SampleKind::IntHist => SampleValue::Histogram(Box::new(gen_test_hist(value))),
        SampleKind::FloatHist => SampleValue::FloatHistogram(Box::new(gen_test_fhist(value))),
        SampleKind::CustomBucketsIntHist => {
            SampleValue::Histogram(Box::new(gen_test_custom_buckets_hist(value)))
        }
        SampleKind::CustomBucketsFloatHist => {
            SampleValue::FloatHistogram(Box::new(gen_test_custom_buckets_fhist(value)))
        }
        SampleKind::GaugeIntHist => SampleValue::Histogram(Box::new(gen_test_gauge_hist(value))),
        SampleKind::GaugeFloatHist => {
            SampleValue::FloatHistogram(Box::new(gen_test_gauge_fhist(value)))
        }
    };
    DecodedSample { st: 0, t: ts, value: v }
}

pub fn append_kind_v2(
    app: &mut HeadAppender<'_>,
    series_ref: Option<u64>,
    kind: SampleKind,
    pairs: &[(&str, &str)],
    ts: i64,
    value: i64,
) -> Result<(u64, DecodedSample), mimir_tsdb::head::HeadError> {
    let sample = make_sample_kind(kind, ts, value);
    let r = match &sample.value {
        SampleValue::Float(f) => app.append_float(series_ref, pairs, ts, *f)?,
        SampleValue::Histogram(h) => app.append_histogram(series_ref, pairs, 0, ts, (**h).clone())?,
        SampleValue::FloatHistogram(fh) => {
            app.append_float_histogram(series_ref, pairs, 0, ts, (**fh).clone())?
        }
    };
    Ok((r, sample))
}

pub fn metric_val(reg: &prometheus::Registry, name: &str, labels: &[(&str, &str)]) -> f64 {
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

pub fn labels_str(l: &Labels) -> String {
    let mut out = String::from("{");
    for (i, (k, v)) in l.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        out.push_str(k);
        out.push_str("=\"");
        out.push_str(v);
        out.push('"');
    }
    out.push('}');
    out
}

pub fn query_db_impl(
    db: &Tsdb,
    mint: i64,
    maxt: i64,
    matchers: &[LabelMatcher],
    replace_stale_nan: bool,
) -> BTreeMap<String, Vec<DecodedSample>> {
    let res = db.querier(mint, maxt).select(true, matchers).unwrap();
    let mut out = BTreeMap::new();
    for s in res {
        let mut samples = s.samples;
        if replace_stale_nan {
            samples.retain(|ds| match &ds.value {
                SampleValue::Float(f) => !is_stale_nan(*f),
                SampleValue::Histogram(h) => !is_stale_nan(h.sum),
                SampleValue::FloatHistogram(fh) => !is_stale_nan(fh.sum),
            });
        }
        if !samples.is_empty() {
            out.insert(labels_str(&s.labels), samples);
        }
    }
    out
}

pub fn query_db(
    db: &Tsdb,
    mint: i64,
    maxt: i64,
    matchers: &[LabelMatcher],
) -> BTreeMap<String, Vec<DecodedSample>> {
    query_db_impl(db, mint, maxt, matchers, true)
}

pub fn query_db_raw_nans(
    db: &Tsdb,
    mint: i64,
    maxt: i64,
    matchers: &[LabelMatcher],
) -> BTreeMap<String, Vec<DecodedSample>> {
    query_db_impl(db, mint, maxt, matchers, false)
}

pub fn query_chunks_expanded(
    db: &Tsdb,
    mint: i64,
    maxt: i64,
    matchers: &[LabelMatcher],
) -> BTreeMap<String, Vec<Vec<DecodedSample>>> {
    let res = db.chunk_querier(mint, maxt).select(true, matchers).unwrap();
    let mut out = BTreeMap::new();
    for s in res {
        let mut chks = Vec::new();
        for cm in s.chunks {
            if let Some(enc) = cm.chunk {
                chks.push(decode_chunk_samples(&enc).unwrap());
            }
        }
        if !chks.is_empty() {
            out.insert(labels_str(&s.labels), chks);
        }
    }
    out
}

pub fn compare_samples(
    name: &str,
    expected: &[DecodedSample],
    actual: &[DecodedSample],
    ignore_counter_resets: bool,
) {
    assert_eq!(
        actual.len(),
        expected.len(),
        "sample count mismatch for {name}: got {}, want {}",
        actual.len(),
        expected.len()
    );
    for (i, (e, a)) in expected.iter().zip(actual.iter()).enumerate() {
        assert_eq!(e.t, a.t, "timestamp mismatch for {name}[{i}]");
        match (&e.value, &a.value) {
            (SampleValue::Float(ef), SampleValue::Float(af)) => {
                if ef.is_nan() && af.is_nan() {
                    assert_eq!(
                        is_stale_nan(*ef),
                        is_stale_nan(*af),
                        "NaN stale mismatch for {name}[{i}]"
                    );
                } else {
                    assert_eq!(ef, af, "float mismatch for {name}[{i}] at t={}", e.t);
                }
            }
            (SampleValue::Histogram(eh), SampleValue::Histogram(ah)) => {
                let mut eh_cmp = (**eh).clone();
                let mut ah_cmp = (**ah).clone();
                if ignore_counter_resets && eh_cmp.counter_reset_hint != CounterResetHint::GaugeType
                {
                    eh_cmp.counter_reset_hint = CounterResetHint::UnknownCounterReset;
                    ah_cmp.counter_reset_hint = CounterResetHint::UnknownCounterReset;
                } else if ah_cmp.counter_reset_hint == CounterResetHint::UnknownCounterReset {
                    eh_cmp.counter_reset_hint = CounterResetHint::UnknownCounterReset;
                }
                assert_eq!(eh_cmp, ah_cmp, "hist mismatch for {name}[{i}] at t={}", e.t);
            }
            (SampleValue::FloatHistogram(efh), SampleValue::FloatHistogram(afh)) => {
                let mut efh_cmp = (**efh).clone();
                let mut afh_cmp = (**afh).clone();
                if ignore_counter_resets
                    && efh_cmp.counter_reset_hint != CounterResetHint::GaugeType
                {
                    efh_cmp.counter_reset_hint = CounterResetHint::UnknownCounterReset;
                    afh_cmp.counter_reset_hint = CounterResetHint::UnknownCounterReset;
                } else if afh_cmp.counter_reset_hint == CounterResetHint::UnknownCounterReset {
                    efh_cmp.counter_reset_hint = CounterResetHint::UnknownCounterReset;
                }
                assert_eq!(efh_cmp, afh_cmp, "fhist mismatch for {name}[{i}] at t={}", e.t);
            }
            _ => panic!("sample type mismatch for {name}[{i}]: {e:?} vs {a:?}"),
        }
    }
}

pub fn compare_series(
    expected: &BTreeMap<String, Vec<DecodedSample>>,
    actual: &BTreeMap<String, Vec<DecodedSample>>,
    ignore_counter_resets: bool,
) {
    assert_eq!(
        expected.len(),
        actual.len(),
        "series count mismatch: expected {:?}, got {:?}",
        expected.keys().collect::<Vec<_>>(),
        actual.keys().collect::<Vec<_>>()
    );
    for (k, exp) in expected {
        let act = actual
            .get(k)
            .unwrap_or_else(|| panic!("expected series {k} missing"));
        compare_samples(k, exp, act, ignore_counter_resets);
    }
}

pub fn create_xor_block(dir: &Path, num_series: usize, num_samples: usize, mint: i64, maxt: i64) -> PathBuf {
    let mut series = Vec::with_capacity(num_series);
    let step = if num_samples > 1 && maxt > mint + 1 {
        ((maxt - 1 - mint) / (num_samples as i64 - 1)).max(1)
    } else {
        1
    };
    for i in 0..num_series {
        let mut samples = Vec::with_capacity(num_samples);
        for j in 0..num_samples {
            let t = (mint + (j as i64) * step).min(maxt - 1);
            if samples.last().is_some_and(|&(pt, _)| pt == t) {
                continue;
            }
            samples.push((t, (i + j) as f64));
        }
        let mut xc = XorChunk::new();
        {
            let mut app = xc.appender().unwrap();
            for &(t, v) in &samples {
                app.append(0, t, v).unwrap();
            }
        }
        let id_s = i.to_string();
        series.push(CompactionSeries {
            labels: Labels::from_pairs([("i", id_s.as_str())]),
            chunks: vec![ChunkMeta {
                ref_id: 0,
                min_time: samples[0].0,
                max_time: samples.last().unwrap().0,
                chunk: Some(EncodedChunk::Xor(xc)),
            }],
            tombstones: Vec::new(),
        });
    }
    let meta = write_block_from_series(
        dir,
        mint,
        maxt,
        series,
        BlockMetaCompaction::default(),
        false,
        0,
    )
    .unwrap()
    .unwrap();
    dir.join(meta.ulid.to_string())
}

pub fn dir_size(dir: &Path) -> u64 {
    mimir_tsdb::encoding::fileutil::dir_size(dir).unwrap_or(0)
}

pub fn dir_hash(dir: &Path) -> u64 {
    let mut files = Vec::new();
    fn collect(root: &Path, cur: &Path, out: &mut Vec<(PathBuf, Vec<u8>)>) {
        let mut entries: Vec<_> = fs::read_dir(cur).unwrap().map(|e| e.unwrap()).collect();
        entries.sort_by_key(|e| e.file_name());
        for e in entries {
            let p = e.path();
            if e.file_type().unwrap().is_dir() {
                collect(root, &p, out);
            } else {
                let rel = p.strip_prefix(root).unwrap().to_path_buf();
                let bytes = fs::read(&p).unwrap_or_default();
                out.push((rel, bytes));
            }
        }
    }
    collect(dir, dir, &mut files);
    files.sort_by(|a, b| a.0.cmp(&b.0));
    let mut h = 14695981039346656037u64;
    for (rel, bytes) in files {
        for &b in rel.to_string_lossy().as_bytes() {
            h ^= u64::from(b);
            h = h.wrapping_mul(1099511628211);
        }
        h ^= bytes.len() as u64;
        h = h.wrapping_mul(1099511628211);
        for &b in &bytes {
            h ^= u64::from(b);
            h = h.wrapping_mul(1099511628211);
        }
    }
    h
}

pub fn copy_dir_recursive(src: &Path, dst: &Path) {
    fs::create_dir_all(dst).unwrap();
    for entry in fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let to = dst.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir_recursive(&entry.path(), &to);
        } else {
            fs::copy(entry.path(), &to).unwrap();
        }
    }
}

pub fn read_all_wal_records(dir: &Path) -> Vec<DecodedRecord> {
    let mut r = WlogReader::open_dir(dir).unwrap();
    let dec = RecordDecoder::new();
    let mut out = Vec::new();
    while let Some(raw) = r.next_record().unwrap() {
        out.push(dec.decode_record(raw).unwrap());
    }
    out
}

pub fn match_eq(name: &str, value: &str) -> LabelMatcher {
    LabelMatcher::new(MatchType::Equal, name, value).unwrap()
}

pub fn match_re(name: &str, value: &str) -> LabelMatcher {
    LabelMatcher::new(MatchType::RegexMatch, name, value).unwrap()
}

pub fn dummy_stale_nan() -> f64 {
    stale_nan()
}

pub fn dummy_block_stats() -> BlockStats {
    BlockStats::default()
}

pub fn open_test_db(dir: &Path, opts: TsdbOptions) -> Tsdb {
    Tsdb::open(dir, opts).unwrap()
}

pub fn opts_with_reg(reg: &Arc<prometheus::Registry>, chunk_range_ms: i64) -> TsdbOptions {
    let mut opts = TsdbOptions::default();
    opts.head.registry = Some(Arc::clone(reg));
    opts.head.chunk_range_ms = chunk_range_ms;
    opts
}

pub fn opts_range(chunk_range_ms: i64) -> TsdbOptions {
    let mut o = TsdbOptions::default();
    o.head.chunk_range_ms = chunk_range_ms;
    o
}

pub fn open_db_range(dir: &Path, chunk_range_ms: i64) -> Tsdb {
    let db = Tsdb::open(dir, opts_range(chunk_range_ms)).unwrap();
    db.disable_compactions();
    db
}

pub fn xor_chunk_pts(pts: &[(i64, f64)]) -> ChunkMeta {
    let mut xc = XorChunk::new();
    {
        let mut a = xc.appender().unwrap();
        for &(t, v) in pts {
            a.append(0, t, v).unwrap();
        }
    }
    ChunkMeta {
        ref_id: 0,
        min_time: pts[0].0,
        max_time: pts[pts.len() - 1].0,
        chunk: Some(EncodedChunk::Xor(xc)),
    }
}

pub fn write_single_series_block(
    dir: &Path,
    name_label: &str,
    pts: &[(i64, f64)],
    hint: Option<&str>,
) -> PathBuf {
    let mut comp = BlockMetaCompaction::default();
    match hint {
        Some("stale") => comp.set_stale_series(),
        Some("selected") => comp.set_selected_series(),
        Some("ooo") => comp.set_out_of_order(),
        _ => {}
    }
    let meta = write_block_from_series(
        dir,
        pts[0].0,
        pts[pts.len() - 1].0 + 1,
        vec![CompactionSeries {
            labels: Labels::from_pairs([("name", name_label)]),
            chunks: vec![xor_chunk_pts(pts)],
            tombstones: Vec::new(),
        }],
        comp,
        false,
        0,
    )
    .unwrap()
    .unwrap();
    dir.join(meta.ulid.to_string())
}

pub fn query_head_range(db: &Tsdb, mint: i64, maxt: i64, matchers: &[LabelMatcher], raw_nans: bool) -> BTreeMap<String, Vec<DecodedSample>> {
    let q = mimir_tsdb::querier::TsdbQuerier::new(Some(db.head()), Vec::new(), mint, maxt);
    let res = q.select(true, matchers).unwrap();
    let mut out = BTreeMap::new();
    for s in res {
        let mut samples = s.samples;
        if !raw_nans {
            samples.retain(|ds| match &ds.value {
                SampleValue::Float(f) => !is_stale_nan(*f),
                SampleValue::Histogram(h) => !is_stale_nan(h.sum),
                SampleValue::FloatHistogram(fh) => !is_stale_nan(fh.sum),
            });
        }
        if !samples.is_empty() {
            out.insert(labels_str(&s.labels), samples);
        }
    }
    out
}

#[derive(Default)]
pub struct CountLifecycleCb {
    pub created: AtomicI64,
    pub deleted: AtomicI64,
}
impl SeriesLifecycleCallback for CountLifecycleCb {
    fn pre_creation(&self, _pairs: &[(&str, &str)]) -> Result<(), HeadError> { Ok(()) }
    fn post_creation(&self, _ref_id: u64, _labels: &Labels) {
        self.created.fetch_add(1, Ordering::Relaxed);
    }
    fn post_deletion(&self, deleted: &[(u64, Labels)]) {
        self.deleted.fetch_add(deleted.len() as i64, Ordering::Relaxed);
    }
}

pub fn force_checkpoint_at(head: &Head, mint: i64) {
    let wal_dir = head.dir().join("wal");
    for _ in 0..10 {
        head.reset_last_wal_truncation_time_for_test();
        head.truncate_wal(mint).unwrap();
        if last_checkpoint(&wal_dir).unwrap().is_some() {
            return;
        }
    }
    panic!("a checkpoint must have been produced");
}

pub fn checkpoint_has_series_ref(head: &Head, target_ref: u64) -> bool {
    let wal_dir = head.dir().join("wal");
    let (cp_dir, _) = last_checkpoint(&wal_dir).unwrap().expect("checkpoint must exist");
    read_all_wal_records(&cp_dir).into_iter().any(|rec| match rec {
        DecodedRecord::Series(series) => series.iter().any(|s| s.ref_id == target_ref),
        _ => false,
    })
}

pub fn assert_sample_vals(got: &[DecodedSample], want: &[(i64, f64)]) {
    assert_eq!(got.len(), want.len());
    for (g, &(wt, wv)) in got.iter().zip(want) {
        assert_eq!(g.t, wt);
        let SampleValue::Float(gf) = g.value else { panic!("expected float") };
        if is_stale_nan(wv) {
            assert!(is_stale_nan(gf));
        } else {
            assert_eq!(gf, wv);
        }
    }
}

pub fn make_hist_block_series(
    from_m: i64,
    to_m: i64,
    step_m: i64,
    include_hist: bool,
    include_float: bool,
    float_hist: bool,
) -> Vec<CompactionSeries> {
    let mut out = Vec::new();
    for i in 0..5usize {
        let mut samples = Vec::new();
        let mut m = from_m;
        while m <= to_m {
            let ts = m * 60_000;
            if include_hist && (!include_float || (m / step_m) % 2 == 0) {
                let v = if float_hist {
                    SampleValue::FloatHistogram(Box::new(gen_test_fhist(m)))
                } else {
                    SampleValue::Histogram(Box::new(gen_test_hist(m)))
                };
                samples.push(DecodedSample { st: 0, t: ts, value: v });
            } else if include_float {
                samples.push(DecodedSample { st: 0, t: ts, value: SampleValue::Float(m as f64) });
            }
            m += step_m;
        }
        let chunks = encode_iterable_chunks(&samples).unwrap();
        let s_id = i.to_string();
        out.push(CompactionSeries {
            labels: Labels::from_pairs([("i", s_id.as_str())]),
            chunks,
            tombstones: Vec::new(),
        });
    }
    out
}

pub fn run_selected_mutation_race(erase_evidence_by_cleanup: bool) {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db_range(dir.path(), 1000);
    let mut app = db.appender();
    let sel_ref = app.append_float(None, &[("name", "selected")], 100, 1.0).unwrap();
    let filler_ref = app.append_float(None, &[("name", "filler")], 700, 1.0).unwrap();
    app.commit().unwrap();

    let mut pending = db.appender();
    pending.append_float(Some(sel_ref), &[("name", "selected")], 400, 2.0).unwrap();
    pending.append_float(Some(filler_ref), &[("name", "filler")], 700, 1.0).unwrap();

    let pause = db.head().pause_series_commit_for_test(filler_ref);
    let (hook_tx, hook_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let done_rx = std::sync::Mutex::new(done_rx);
    db.set_compact_head_view_before_evict_hook_for_test(Some(Arc::new(move || {
        hook_tx.send(()).unwrap();
        done_rx.lock().unwrap().recv().unwrap();
    })));

    std::thread::scope(|s| {
        let h_commit = s.spawn(|| pending.commit());
        let deadline = Instant::now() + Duration::from_secs(5);
        while (db.head().series_max_time_for_test(sel_ref) != Some(400)
            || db.head().has_pending_commit_for_test(sel_ref) != Some(false))
            && Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(db.head().series_max_time_for_test(sel_ref), Some(400));

        let h_compact = s.spawn(|| db.compact_selected_series(&[sel_ref]).unwrap());
        hook_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        pause.resume();
        h_commit.join().unwrap().unwrap();
        if erase_evidence_by_cleanup {
            let mut cleanup = db.appender();
            cleanup.append_float(Some(sel_ref), &[("name", "selected")], 400, 2.0).unwrap();
            cleanup.commit().unwrap();
            assert_eq!(db.head().series_txs_len_for_test(sel_ref), Some(0));
        }
        done_tx.send(()).unwrap();
        h_compact.join().unwrap();
    });

    let got = query_db(&db, 0, 1000, &[match_eq("name", "selected")]);
    assert_sample_vals(&got[r#"{name="selected"}"#], &[(100, 1.0), (400, 2.0)]);
    db.close().unwrap();
}

pub fn lcg_next(s: &mut u64) -> u64 {
    *s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    *s >> 16
}

pub fn xor_block(dir: &Path, mint: i64, maxt: i64, n_series: usize, n_samples: usize) {
    let mut series = Vec::with_capacity(n_series);
    let step = ((maxt - mint - 1).max(1) / (n_samples.max(2) as i64 - 1)).max(1);
    for i in 0..n_series {
        let mut smpls = Vec::new();
        for j in 0..n_samples {
            let t = (mint + (j as i64) * step).min(maxt - 1);
            if smpls.last().is_some_and(|x: &DecodedSample| x.t == t) {
                continue;
            }
            smpls.push(DecodedSample { st: 0, t, value: SampleValue::Float((i + j) as f64) });
        }
        let s_id = i.to_string();
        series.push(CompactionSeries {
            labels: Labels::from_pairs([("i", s_id.as_str())]),
            chunks: encode_iterable_chunks(&smpls).unwrap(),
            tombstones: Vec::new(),
        });
    }
    write_block_from_series(dir, mint, maxt, series, BlockMetaCompaction::default(), false, 0).unwrap();
}

pub fn set_crh(mut s: DecodedSample, hint: CounterResetHint) -> DecodedSample {
    match &mut s.value {
        SampleValue::Histogram(h) => h.counter_reset_hint = hint,
        SampleValue::FloatHistogram(fh) => fh.counter_reset_hint = hint,
        SampleValue::Float(_) => {}
    }
    s
}

pub fn count_chunk_samples_for_series(db: &Tsdb, mint: i64, maxt: i64, matchers: &[LabelMatcher]) -> usize {
    let res = db.chunk_querier(mint, maxt).select(true, matchers).unwrap();
    let mut total = 0usize;
    for s in res {
        for cm in s.chunks {
            if let Some(enc) = cm.chunk {
                total += enc.num_samples();
            }
        }
    }
    total
}

pub fn run_ooo_querier_cases(kind: SampleKind, use_chunk_querier: bool, force_reset: bool) {
    let min_ms = |m: i64| m * 60_000;
    type OooCase<'a> = (i64, i64, i64, &'a [(i64, i64, bool, i64)]);
    let cases: &[OooCase<'_>] = &[
        (30, min_ms(0), min_ms(200), &[(100, 200, false, 1), (0, 99, false, 1)]),
        (30, min_ms(20), min_ms(180), &[(100, 200, false, 1), (0, 99, false, 1)]),
        (30, min_ms(0), min_ms(200), &[(100, 200, false, 1), (0, 99, false, 2), (1, 99, true, 2)]),
        (30, min_ms(0), min_ms(200), &[(100, 200, false, 2), (171, 180, false, 2)]),
        (30, min_ms(0), min_ms(200), &[(100, 200, false, 2), (101, 110, false, 2)]),
        (5, min_ms(0), min_ms(200), &[(100, 200, false, 2), (101, 109, false, 2), (191, 193, false, 2)]),
        (30, min_ms(0), min_ms(200), &[(100, 200, false, 2), (101, 159, false, 2), (191, 193, false, 2)]),
    ];
    for &(ooo_cap, qmin, qmax, batches) in cases {
        let dir = tempfile::tempdir().unwrap();
        let mut opts = TsdbOptions::default();
        opts.head.ooo_time_window_ms = 24 * HOUR_MS;
        opts.head.ooo_cap_max = ooo_cap as usize;
        let db = Tsdb::open(dir.path(), opts).unwrap();
        db.disable_compactions();
        let mut exp = Vec::new();
        for &(from_m, to_m, reset, step) in batches {
            let mut app = db.appender();
            let mut m = from_m;
            while m <= to_m {
                let ts = min_ms(m);
                let mut smpl = make_sample_kind(kind, ts, m);
                if reset || force_reset {
                    smpl = set_crh(smpl, CounterResetHint::CounterReset);
                }
                match &smpl.value {
                    SampleValue::Float(f) => { app.append_float(None, &[("foo", "bar1")], ts, *f).unwrap(); }
                    SampleValue::Histogram(h) => { app.append_histogram(None, &[("foo", "bar1")], 0, ts, (**h).clone()).unwrap(); }
                    SampleValue::FloatHistogram(fh) => { app.append_float_histogram(None, &[("foo", "bar1")], 0, ts, (**fh).clone()).unwrap(); }
                }
                if ts >= qmin && ts <= qmax {
                    exp.push(smpl);
                }
                m += step;
            }
            app.commit().unwrap();
        }
        exp.sort_by_key(|s| s.t);
        let mut exp_map = BTreeMap::new();
        exp_map.insert(r#"{foo="bar1"}"#.to_string(), exp);
        if use_chunk_querier {
            let chks = query_chunks_expanded(&db, qmin, qmax, &[match_eq("foo", "bar1")]);
            let mut flat = Vec::new();
            for c in &chks[r#"{foo="bar1"}"#] {
                assert!(c[0].t <= qmax && c.last().unwrap().t >= qmin);
                for s in c {
                    assert!(s.t >= c[0].t && s.t <= c.last().unwrap().t);
                    if s.t >= qmin && s.t <= qmax { flat.push(s.clone()); }
                }
            }
            let mut flat_map = BTreeMap::new();
            flat_map.insert(r#"{foo="bar1"}"#.to_string(), flat);
            compare_series(&exp_map, &flat_map, true);
        } else {
            compare_series(&exp_map, &query_db(&db, qmin, qmax, &[match_eq("foo", "bar1")]), true);
        }
        db.close().unwrap();
    }
}

pub fn run_ooo_compaction_race_v2(mode: u8) {
    let dir = tempfile::tempdir().unwrap();
    let reg = Arc::new(prometheus::Registry::new());
    let mut opts = opts_with_reg(&reg, DEFAULT_BLOCK_DURATION_MS);
    opts.head.ooo_time_window_ms = 3 * DEFAULT_BLOCK_DURATION_MS;
    let db = Arc::new(Tsdb::open(dir.path(), opts).unwrap());
    db.disable_compactions();
    let interval = 15_000i64;
    let mut ts = interval;
    for _ in 1..10 {
        let mut app = db.appender();
        app.append_float(None, &[("__name__", "test_metric")], ts, ts as f64).unwrap();
        app.commit().unwrap();
        ts += interval;
    }
    let mut app = db.appender();
    app.append_float(None, &[("__name__", "test_metric")], 0, ts as f64).unwrap();
    app.commit().unwrap();

    let cq_before = db.chunk_querier(0, i64::MAX);
    let prepared_or_res = if mode == 1 {
        Some(cq_before.select(true, &[match_eq("__name__", "test_metric")]).unwrap())
    } else {
        None
    };
    let read_guard = db.head().start_read(0, i64::MAX);

    let done = Arc::new(AtomicBool::new(false));
    let done_c = Arc::clone(&done);
    let db_c = Arc::clone(&db);
    let h = std::thread::spawn(move || {
        let res = db_c.compact_ooo_head();
        done_c.store(true, Ordering::Release);
        res
    });
    let deadline = Instant::now() + Duration::from_secs(10);
    while !done.load(Ordering::Acquire) && db.last_garbage_collected_mmap_ref_for_test() == 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(!done.load(Ordering::Acquire));

    let cq_after = db.chunk_querier(0, i64::MAX);
    let n_before: usize = if let Some(res) = prepared_or_res {
        res.iter().flat_map(|s| &s.chunks).map(|c| c.chunk.as_ref().unwrap().num_samples()).sum()
    } else {
        let res = cq_before.select(true, &[match_eq("__name__", "test_metric")]).unwrap();
        res.iter().flat_map(|s| &s.chunks).map(|c| c.chunk.as_ref().unwrap().num_samples()).sum()
    };
    assert_eq!(n_before, 10);
    assert!(!done.load(Ordering::Acquire));
    drop(read_guard);
    drop(cq_before);

    h.join().unwrap().unwrap();
    assert!(metric_val(&reg, "prometheus_tsdb_head_chunks_removed_total", &[]) >= 1.0);
    assert_eq!(count_chunk_samples_for_series(&db, 0, i64::MAX, &[match_eq("__name__", "test_metric")]), 10);
    drop(cq_after);
    db.close().unwrap();
}

pub fn run_ooo_runtime_config_test(kind: SampleKind) {
    let min_ms = |m: i64| m * 60_000;
    let cfg = |w_min: i64| TsdbRuntimeConfig { ooo_time_window_ms: Some(w_min * 60_000), ..Default::default() };
    {
        let dir = tempfile::tempdir().unwrap();
        let mut opts = TsdbOptions::default();
        opts.head.ooo_time_window_ms = min_ms(30);
        let db = Tsdb::open(dir.path(), opts).unwrap();
        let mut app = db.appender();
        for m in 300..=310 { append_kind_v2(&mut app, None, kind, &[("foo", "bar1")], min_ms(m), m).unwrap(); }
        for m in 281..=290 { append_kind_v2(&mut app, None, kind, &[("foo", "bar1")], min_ms(m), m).unwrap(); }
        app.commit().unwrap();
        let mut app = db.appender();
        assert!(append_kind_v2(&mut app, None, kind, &[("foo", "bar1")], min_ms(251), 251).is_err());
        app.rollback().unwrap();
        db.apply_config(&cfg(60)).unwrap();
        let mut app = db.appender();
        for m in 251..=260 { append_kind_v2(&mut app, None, kind, &[("foo", "bar1")], min_ms(m), m).unwrap(); }
        app.commit().unwrap();
        db.compact_ooo_head().unwrap();
        assert!(!db.blocks().is_empty());
        db.close().unwrap();
    }
    {
        let dir = tempfile::tempdir().unwrap();
        let mut opts = TsdbOptions::default();
        opts.head.ooo_time_window_ms = 0;
        let db = Tsdb::open(dir.path(), opts).unwrap();
        let mut app = db.appender();
        for m in 300..=310 { append_kind_v2(&mut app, None, kind, &[("foo", "bar1")], min_ms(m), m).unwrap(); }
        app.commit().unwrap();
        assert!(!db.head().has_wbl());
        db.apply_config(&cfg(60)).unwrap();
        assert!(db.head().has_wbl());
        let mut app = db.appender();
        for m in 251..=260 { append_kind_v2(&mut app, None, kind, &[("foo", "bar1")], min_ms(m), m).unwrap(); }
        app.commit().unwrap();
        db.compact_ooo_head().unwrap();
        assert!(!db.blocks().is_empty());
        db.close().unwrap();
    }
    {
        let dir = tempfile::tempdir().unwrap();
        let mut opts = TsdbOptions::default();
        opts.head.ooo_time_window_ms = min_ms(60);
        let db = Tsdb::open(dir.path(), opts).unwrap();
        let mut app = db.appender();
        for m in 300..=310 { append_kind_v2(&mut app, None, kind, &[("foo", "bar1")], min_ms(m), m).unwrap(); }
        for m in 251..=260 { append_kind_v2(&mut app, None, kind, &[("foo", "bar1")], min_ms(m), m).unwrap(); }
        app.commit().unwrap();
        db.apply_config(&cfg(0)).unwrap();
        let mut app = db.appender();
        assert!(append_kind_v2(&mut app, None, kind, &[("foo", "bar1")], min_ms(290), 290).is_err());
        app.rollback().unwrap();
        db.compact_ooo_head().unwrap();
        assert!(!db.blocks().is_empty());
        db.close().unwrap();
    }
}

pub fn run_histogram_append_and_query_test(float_hist: bool) {
    let min_ms = |m: i64| m * 60_000;
    let dir = tempfile::tempdir().unwrap();
    let db = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
    let mut h = Histogram {
        count: 15, zero_count: 4, zero_threshold: 0.001, sum: 35.5, schema: 1,
        positive_spans: vec![HistogramSpan { offset: 0, length: 2 }, HistogramSpan { offset: 2, length: 2 }],
        positive_buckets: vec![1, 1, -1, 0],
        negative_spans: vec![HistogramSpan { offset: 0, length: 1 }, HistogramSpan { offset: 1, length: 2 }],
        negative_buckets: vec![1, 2, -1],
        ..Histogram::default()
    };
    let mut exp1 = Vec::new();
    let add_h = |pairs: &[(&str, &str)], m: i64, cur_h: &Histogram, exp: &mut Vec<DecodedSample>| {
        let mut app = db.appender();
        if float_hist {
            let fh = cur_h.to_float();
            app.append_float_histogram(None, pairs, 0, min_ms(m), fh.clone()).unwrap();
            exp.push(DecodedSample { st: 0, t: min_ms(m), value: SampleValue::FloatHistogram(Box::new(fh)) });
        } else {
            app.append_histogram(None, pairs, 0, min_ms(m), cur_h.clone()).unwrap();
            exp.push(DecodedSample { st: 0, t: min_ms(m), value: SampleValue::Histogram(Box::new(cur_h.clone())) });
        }
        app.commit().unwrap();
    };
    add_h(&[("foo", "bar1")], 100, &h, &mut exp1);
    h.positive_buckets[0] += 1;
    h.negative_buckets[0] += 2;
    h.count += 10;
    add_h(&[("foo", "bar1")], 101, &h, &mut exp1);
    h.schema = 2;
    add_h(&[("foo", "bar1")], 102, &h, &mut exp1);
    h.schema = 1;
    add_h(&[("foo", "bar1")], 103, &h, &mut exp1);
    h.positive_spans[1].length += 1;
    h.positive_buckets.push(1);
    h.count += 3;
    add_h(&[("foo", "bar1")], 104, &h, &mut exp1);
    h.positive_spans[0].length += 1;
    h.positive_spans[1].offset -= 1;
    h.positive_buckets.insert(2, 0);
    h.count += 3;
    for m in 105..=109 { add_h(&[("foo", "bar1")], m, &h, &mut exp1); }
    for idx in [exp1.len() - 7, exp1.len() - 6] {
        let is_6 = idx == exp1.len() - 6;
        match &mut exp1[idx].value {
            SampleValue::Histogram(eh) => {
                eh.positive_spans = h.positive_spans.clone();
                eh.positive_buckets = if is_6 { vec![2, 1, -3, 2, 0, 1] } else { vec![2, 1, -3, 2, 0, -2] };
            }
            SampleValue::FloatHistogram(efh) => {
                efh.positive_spans = h.positive_spans.clone();
                efh.positive_buckets = if is_6 { vec![2.0, 3.0, 0.0, 2.0, 2.0, 3.0] } else { vec![2.0, 3.0, 0.0, 2.0, 2.0, 0.0] };
            }
            _ => {}
        }
    }
    h.positive_spans[1].length -= 1;
    h.positive_buckets.pop();
    h.count -= 3;
    add_h(&[("foo", "bar1")], 110, &h, &mut exp1);
    let mut exp_map = BTreeMap::new();
    exp_map.insert(r#"{foo="bar1"}"#.into(), exp1.clone());
    compare_series(&exp_map, &query_db(&db, i64::MIN, i64::MAX, &[match_eq("foo", "bar1")]), true);

    let mut exp2 = Vec::new();
    let mut app = db.appender();
    for m in 100..=102 { app.append_float(None, &[("foo", "bar2")], min_ms(m), m as f64).unwrap(); exp2.push(DecodedSample { st: 0, t: min_ms(m), value: SampleValue::Float(m as f64) }); }
    app.commit().unwrap();
    for m in 103..=105 { add_h(&[("foo", "bar2")], m, &h, &mut exp2); }
    let mut exp_all = BTreeMap::new();
    exp_all.insert(r#"{foo="bar1"}"#.into(), exp1);
    exp_all.insert(r#"{foo="bar2"}"#.into(), exp2);
    compare_series(&exp_all, &query_db(&db, i64::MIN, i64::MAX, &[match_re("foo", "bar.*")]), true);
    db.close().unwrap();
}
