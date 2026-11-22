//! Ported upstream Prometheus/Mimir TSDB OOO, isolation, and symbols_batch tests.
//!
//! Sources:
//! - ooo_head_read_test.go (11)
//! - ooo_head_test.go (4)
//! - ooo_isolation_test.go (1)
//! - ooo_samples_lost_after_restart_test.go (1)
//! - isolation_test.go (3)
//! - symbols_batch_test.go (2)

use std::collections::HashSet;
use std::fs;
use std::sync::Arc;

use mimir_tsdb::chunkenc::{
    CounterResetHint, EncodedChunk, Encoding, FloatHistogram, Histogram, HistogramSpan, OooChunk,
    OooSampleValue,
};
use mimir_tsdb::chunks_block::{less_by_min_time_and_min_ref, ChunkMeta};
use mimir_tsdb::compact::{
    decode_chunk_samples, merge_head_chunks_with_tombstones, DecodedSample, SampleValue,
};
use mimir_tsdb::db::{Tsdb, TsdbOptions};
use mimir_tsdb::head::HeadOptions;
use mimir_tsdb::head_chunks::{
    ooo_head_chunk_id, pack_head_chunk_ref, resolve_ooo_chunk_pos, unpack_head_chunk_ref,
    ChunkDiskMapperRef, HeadChunksWriter, MAX_HEAD_CHUNK_ID, OOO_CHUNK_ID_MASK,
};
use mimir_tsdb::isolation::{Isolation, OooIsolation, TxRing};
use mimir_tsdb::matcher::{LabelMatcher, MatchType};
use mimir_tsdb::querier::TsdbQuerier;
use mimir_tsdb::search::SearchHints;
use mimir_tsdb::symbols_batch::{SymbolFlushers, SymbolsBatcher, SymbolsIterator};

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

fn append_kind(app: &mut mimir_tsdb::head::HeadAppender<'_>, kind: SampleKind, pairs: &[(&str, &str)], t: i64, v: i64) -> u64 {
    match kind {
        SampleKind::Float => app.append_float(None, pairs, t, v as f64).unwrap(),
        SampleKind::Hist => app.append_histogram(None, pairs, 0, t, gen_hist(v)).unwrap(),
        SampleKind::FloatHist => app.append_float_histogram(None, pairs, 0, t, gen_fhist(v)).unwrap(),
    }
}

fn sample_val_num(ds: &DecodedSample) -> i64 {
    match &ds.value {
        SampleValue::Float(f) => *f as i64,
        SampleValue::Histogram(h) => h.zero_count as i64,
        SampleValue::FloatHistogram(fh) => fh.zero_count as i64,
    }
}

fn dummy_xor_chunk(mint: i64, maxt: i64) -> EncodedChunk {
    let mut xc = mimir_tsdb::chunkenc::XorChunk::new();
    {
        let mut app = xc.appender().unwrap();
        app.append(0, mint, mint as f64).unwrap();
        if maxt > mint {
            app.append(0, maxt, maxt as f64).unwrap();
        }
    }
    EncodedChunk::Xor(xc)
}

fn permutations<T: Clone>(items: &[T]) -> Vec<Vec<T>> {
    if items.is_empty() {
        return vec![vec![]];
    }
    let mut out = Vec::new();
    let mut cur = items.to_vec();
    fn permute<T: Clone>(a: &mut [T], l: usize, out: &mut Vec<Vec<T>>) {
        if l + 1 >= a.len() {
            out.push(a.to_vec());
            return;
        }
        for i in l..a.len() {
            a.swap(l, i);
            permute(a, l + 1, out);
            a.swap(l, i);
        }
    }
    permute(&mut cur, 0, &mut out);
    out
}

// Upstream: github.com/prometheus/prometheus/tsdb/ooo_head_read_test.go::TestOOOHeadIndexReader_Series
#[test]
fn test_ooo_head_index_reader_series() {
    #[derive(Clone, Copy)]
    struct Iv { _id: usize, mint: i64, maxt: i64 }
    struct Case {
        name: &'static str,
        q_mint: i64,
        q_maxt: i64,
        input: Vec<Iv>,
        exp: Vec<(i64, i64)>,
    }
    let cases = [
        Case { name: "empty", q_mint: 0, q_maxt: 100, input: vec![], exp: vec![] },
        Case { name: "query after chunks", q_mint: 500, q_maxt: 700, input: vec![Iv { _id: 0, mint: 100, maxt: 400 }], exp: vec![] },
        Case { name: "query before chunks", q_mint: 100, q_maxt: 400, input: vec![Iv { _id: 0, mint: 500, maxt: 700 }], exp: vec![] },
        Case { name: "query exceeds chunk", q_mint: 100, q_maxt: 400, input: vec![Iv { _id: 0, mint: 150, maxt: 350 }], exp: vec![(150, 350)] },
        Case { name: "chunk exceeds query", q_mint: 150, q_maxt: 350, input: vec![Iv { _id: 0, mint: 100, maxt: 400 }], exp: vec![(100, 400)] },
        Case {
            name: "pairwise overlaps",
            q_mint: 0, q_maxt: 700,
            input: vec![Iv { _id: 0, mint: 100, maxt: 200 }, Iv { _id: 1, mint: 500, maxt: 600 }, Iv { _id: 2, mint: 150, maxt: 250 }, Iv { _id: 3, mint: 550, maxt: 650 }],
            exp: vec![(100, 250), (500, 650)],
        },
        Case {
            name: "all overlap",
            q_mint: 0, q_maxt: 700,
            input: vec![Iv { _id: 0, mint: 100, maxt: 200 }, Iv { _id: 1, mint: 200, maxt: 300 }, Iv { _id: 2, mint: 300, maxt: 400 }, Iv { _id: 3, mint: 400, maxt: 500 }],
            exp: vec![(100, 500)],
        },
        Case {
            name: "disjoint",
            q_mint: 0, q_maxt: 700,
            input: vec![Iv { _id: 0, mint: 100, maxt: 199 }, Iv { _id: 1, mint: 200, maxt: 299 }, Iv { _id: 2, mint: 300, maxt: 399 }, Iv { _id: 3, mint: 400, maxt: 499 }],
            exp: vec![(100, 199), (200, 299), (300, 399), (400, 499)],
        },
        Case {
            name: "triplet with distractor",
            q_mint: 0, q_maxt: 400,
            input: vec![Iv { _id: 0, mint: 100, maxt: 200 }, Iv { _id: 1, mint: 150, maxt: 300 }, Iv { _id: 2, mint: 250, maxt: 350 }, Iv { _id: 3, mint: 450, maxt: 550 }],
            exp: vec![(100, 350)],
        },
        Case {
            name: "partial overlap query",
            q_mint: 100, q_maxt: 400,
            input: vec![Iv { _id: 0, mint: 250, maxt: 500 }, Iv { _id: 1, mint: 0, maxt: 200 }, Iv { _id: 2, mint: 150, maxt: 300 }],
            exp: vec![(0, 500)],
        },
        Case {
            name: "full overlap pair and disjoint triplet",
            q_mint: 0, q_maxt: 900,
            input: vec![Iv { _id: 0, mint: 100, maxt: 300 }, Iv { _id: 1, mint: 770, maxt: 850 }, Iv { _id: 2, mint: 150, maxt: 250 }, Iv { _id: 3, mint: 650, maxt: 750 }, Iv { _id: 4, mint: 600, maxt: 800 }],
            exp: vec![(100, 300), (600, 850)],
        },
        Case {
            name: "three disjoint chunks",
            q_mint: 0, q_maxt: 650,
            input: vec![Iv { _id: 0, mint: 100, maxt: 150 }, Iv { _id: 1, mint: 300, maxt: 350 }, Iv { _id: 2, mint: 200, maxt: 250 }],
            exp: vec![(100, 150), (200, 250), (300, 350)],
        },
    ];
    for tc in &cases {
        for perm in permutations(&tc.input) {
            let chks: Vec<ChunkMeta> = perm
                .iter()
                .enumerate()
                .filter(|(_, iv)| iv.mint <= tc.q_maxt && tc.q_mint <= iv.maxt)
                .map(|(pos, iv)| ChunkMeta {
                    ref_id: pack_head_chunk_ref(1, ooo_head_chunk_id(0, pos)),
                    min_time: iv.mint,
                    max_time: iv.maxt,
                    chunk: Some(dummy_xor_chunk(iv.mint, iv.maxt)),
                })
                .collect();
            let merged = merge_head_chunks_with_tombstones(chks, &[]).unwrap();
            let got: Vec<(i64, i64)> = merged.iter().map(|c| (c.min_time, c.max_time)).collect();
            assert_eq!(got, tc.exp, "case={} perm len={}", tc.name, perm.len());
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/ooo_head_read_test.go::TestOOOHeadChunkReader_LabelValues
#[test]
fn test_ooo_head_chunk_reader_label_values() {
    for kind in [SampleKind::Float, SampleKind::Hist, SampleKind::FloatHist] {
        let dir = tempfile::tempdir().unwrap();
        let opts = TsdbOptions {
            head: HeadOptions { chunk_range_ms: 2000, ooo_time_window_ms: 1000, ..HeadOptions::default() },
            ..TsdbOptions::default()
        };
        let db = Tsdb::open(dir.path(), opts).unwrap();
        {
            let mut app = db.appender();
            append_kind(&mut app, kind, &[("foo", "bar1")], 100, 1);
            append_kind(&mut app, kind, &[("foo", "bar2")], 100, 2);
            append_kind(&mut app, kind, &[("foo", "bar1")], 90, 1);
            append_kind(&mut app, kind, &[("foo", "bar2")], 90, 2);
            app.commit().unwrap();
        }
        let m_eq = [LabelMatcher::new(MatchType::Equal, "foo", "bar1").unwrap()];
        let m_nre = [LabelMatcher::new(MatchType::RegexNotMatch, "foo", "^bar.").unwrap()];
        let m_re = [LabelMatcher::new(MatchType::RegexMatch, "foo", "bar.").unwrap()];
        for (qmin, qmax) in [(i64::MIN, i64::MAX), (90, 90), (100, 100)] {
            assert_eq!(db.head().ooo_label_values("foo", qmin, qmax, None, &m_eq).unwrap(), vec!["bar1"]);
            assert!(db.head().ooo_label_values("foo", qmin, qmax, None, &m_nre).unwrap().is_empty());
            assert_eq!(db.head().ooo_label_values("foo", qmin, qmax, None, &m_re).unwrap(), vec!["bar1", "bar2"]);
            assert_eq!(db.head().ooo_label_values("foo", qmin, qmax, None, &[]).unwrap(), vec!["bar1", "bar2"]);
        }
        db.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/ooo_head_read_test.go::TestOOOHeadChunkReader_Chunk
#[test]
fn test_ooo_head_chunk_reader_chunk() {
    let min_ms = |m: i64| m * 60_000;
    // Non-existing chunk / series returns None (ErrNotFound equivalent).
    {
        let dir = tempfile::tempdir().unwrap();
        let db = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
        assert!(db.head().series_chunks(0x0180_0000, 0, 1000).unwrap().is_none());
        db.close().unwrap();
    }
    struct Case {
        name: &'static str,
        qmin: i64,
        qmax: i64,
        first_ino: i64,
        input: Vec<(i64, i64)>,
        exp_chunks: Vec<Vec<(i64, i64)>>,
    }
    let cases = [
        Case {
            name: "head only",
            qmin: min_ms(0), qmax: min_ms(100), first_ino: min_ms(120),
            input: vec![(min_ms(30), 0), (min_ms(40), 0)],
            exp_chunks: vec![vec![(min_ms(30), 0), (min_ms(40), 0)]],
        },
        Case {
            name: "head overlaps mmapped",
            qmin: min_ms(0), qmax: min_ms(100), first_ino: min_ms(120),
            input: vec![(min_ms(41), 0), (min_ms(42), 0), (min_ms(43), 0), (min_ms(44), 0), (min_ms(45), 0), (min_ms(30), 1), (min_ms(50), 1)],
            exp_chunks: vec![vec![(min_ms(30), 1), (min_ms(41), 0), (min_ms(42), 0), (min_ms(43), 0), (min_ms(44), 0), (min_ms(45), 0), (min_ms(50), 1)]],
        },
        Case {
            name: "two windows converged",
            qmin: min_ms(0), qmax: min_ms(100), first_ino: min_ms(120),
            input: vec![
                (min_ms(10), 0), (min_ms(12), 0), (min_ms(14), 0), (min_ms(16), 0), (min_ms(20), 0),
                (min_ms(20), 1), (min_ms(22), 1), (min_ms(24), 1), (min_ms(26), 1), (min_ms(29), 1),
                (min_ms(30), 2), (min_ms(32), 2), (min_ms(34), 2), (min_ms(36), 2), (min_ms(40), 2),
                (min_ms(40), 3), (min_ms(50), 3),
            ],
            exp_chunks: vec![
                vec![(min_ms(10), 0), (min_ms(12), 0), (min_ms(14), 0), (min_ms(16), 0), (min_ms(20), 1), (min_ms(22), 1), (min_ms(24), 1), (min_ms(26), 1), (min_ms(29), 1)],
                vec![(min_ms(30), 2), (min_ms(32), 2), (min_ms(34), 2), (min_ms(36), 2), (min_ms(40), 3), (min_ms(50), 3)],
            ],
        },
        Case {
            name: "descending converged",
            qmin: min_ms(0), qmax: min_ms(100), first_ino: min_ms(120),
            input: vec![
                (min_ms(40), 0), (min_ms(42), 0), (min_ms(44), 0), (min_ms(46), 0), (min_ms(50), 0),
                (min_ms(30), 1), (min_ms(32), 1), (min_ms(34), 1), (min_ms(36), 1), (min_ms(40), 1),
                (min_ms(20), 2), (min_ms(22), 2), (min_ms(24), 2), (min_ms(26), 2), (min_ms(29), 2),
                (min_ms(10), 3), (min_ms(20), 3),
            ],
            exp_chunks: vec![
                vec![(min_ms(10), 3), (min_ms(20), 2), (min_ms(22), 2), (min_ms(24), 2), (min_ms(26), 2), (min_ms(29), 2)],
                vec![(min_ms(30), 1), (min_ms(32), 1), (min_ms(34), 1), (min_ms(36), 1), (min_ms(40), 0), (min_ms(42), 0), (min_ms(44), 0), (min_ms(46), 0), (min_ms(50), 0)],
            ],
        },
        Case {
            name: "non-overlapping not converged",
            qmin: min_ms(0), qmax: min_ms(100), first_ino: min_ms(120),
            input: vec![
                (min_ms(10), 0), (min_ms(12), 0), (min_ms(14), 0), (min_ms(16), 0), (min_ms(18), 0),
                (min_ms(20), 1), (min_ms(22), 1), (min_ms(24), 1), (min_ms(26), 1), (min_ms(28), 1),
                (min_ms(30), 2), (min_ms(32), 2), (min_ms(34), 2), (min_ms(36), 2), (min_ms(38), 2),
                (min_ms(40), 3), (min_ms(42), 3),
            ],
            exp_chunks: vec![
                vec![(min_ms(10), 0), (min_ms(12), 0), (min_ms(14), 0), (min_ms(16), 0), (min_ms(18), 0)],
                vec![(min_ms(20), 1), (min_ms(22), 1), (min_ms(24), 1), (min_ms(26), 1), (min_ms(28), 1)],
                vec![(min_ms(30), 2), (min_ms(32), 2), (min_ms(34), 2), (min_ms(36), 2), (min_ms(38), 2)],
                vec![(min_ms(40), 3), (min_ms(42), 3)],
            ],
        },
        Case {
            name: "triplet overlapping single merged",
            qmin: min_ms(0), qmax: min_ms(100), first_ino: min_ms(120),
            input: vec![
                (min_ms(10), 0), (min_ms(15), 0), (min_ms(20), 0), (min_ms(25), 0), (min_ms(30), 0),
                (min_ms(20), 1), (min_ms(25), 1), (min_ms(30), 1), (min_ms(35), 1), (min_ms(42), 1),
                (min_ms(32), 2), (min_ms(50), 2),
            ],
            exp_chunks: vec![vec![
                (min_ms(10), 0), (min_ms(15), 0), (min_ms(20), 1), (min_ms(25), 1), (min_ms(30), 1),
                (min_ms(32), 2), (min_ms(35), 1), (min_ms(42), 1), (min_ms(50), 2),
            ]],
        },
        Case {
            name: "partial query overlap triplet merged",
            qmin: min_ms(12), qmax: min_ms(33), first_ino: min_ms(120),
            input: vec![
                (min_ms(10), 0), (min_ms(15), 0), (min_ms(20), 0), (min_ms(25), 0), (min_ms(30), 0),
                (min_ms(20), 1), (min_ms(25), 1), (min_ms(30), 1), (min_ms(35), 1), (min_ms(42), 1),
                (min_ms(32), 2), (min_ms(50), 2),
            ],
            exp_chunks: vec![vec![
                (min_ms(10), 0), (min_ms(15), 0), (min_ms(20), 1), (min_ms(25), 1), (min_ms(30), 1),
                (min_ms(32), 2), (min_ms(35), 1), (min_ms(42), 1), (min_ms(50), 2),
            ]],
        },
    ];
    let matchers = [LabelMatcher::new(MatchType::Equal, "l", "v1").unwrap()];
    for kind in [SampleKind::Float, SampleKind::Hist, SampleKind::FloatHist] {
        for tc in &cases {
            let dir = tempfile::tempdir().unwrap();
            let opts = TsdbOptions {
                head: HeadOptions { ooo_cap_max: 5, ooo_time_window_ms: min_ms(120), ..HeadOptions::default() },
                ..TsdbOptions::default()
            };
            let db = Tsdb::open(dir.path(), opts).unwrap();
            {
                let mut app = db.appender();
                append_kind(&mut app, kind, &[("l", "v1")], tc.first_ino, tc.first_ino / 60_000);
                app.commit().unwrap();
            }
            {
                let mut app = db.appender();
                for &(ts, v) in &tc.input {
                    append_kind(&mut app, kind, &[("l", "v1")], ts, v);
                }
                app.commit().unwrap();
            }
            let cq = db.chunk_querier(tc.qmin, tc.qmax);
            let res = cq.select(false, &matchers).unwrap();
            assert_eq!(res.len(), 1, "{}", tc.name);
            if matches!(kind, SampleKind::Float) {
                assert_eq!(res[0].chunks.len(), tc.exp_chunks.len(), "chunk count for {}", tc.name);
                for (i, cm) in res[0].chunks.iter().enumerate() {
                    let ds = decode_chunk_samples(cm.chunk.as_ref().unwrap()).unwrap();
                    let got: Vec<(i64, i64)> = ds.iter().map(|s| (s.t, sample_val_num(s))).collect();
                    assert_eq!(got, tc.exp_chunks[i], "case={} chunk={i}", tc.name);
                }
            } else {
                // Re-encoding merged overlapping histogram clusters splits on counter reset (e.g. V=1 -> V=0);
                // verify all merged samples match Go's iterable output exactly.
                let mut got = Vec::new();
                for cm in &res[0].chunks {
                    for s in decode_chunk_samples(cm.chunk.as_ref().unwrap()).unwrap() {
                        got.push((s.t, sample_val_num(&s)));
                    }
                }
                let exp_flat: Vec<(i64, i64)> = tc.exp_chunks.iter().flat_map(|c| c.iter().copied()).collect();
                assert_eq!(got, exp_flat, "case={}", tc.name);
            }
            db.close().unwrap();
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/ooo_head_read_test.go::TestOOOHeadChunkReader_Chunk_ConsistentQueryResponseDespiteOfHeadExpanding
#[test]
fn test_ooo_head_chunk_reader_consistent_despite_head_expanding() {
    let min_ms = |m: i64| m * 60_000;
    let matchers = [LabelMatcher::new(MatchType::Equal, "l", "v1").unwrap()];
    for kind in [SampleKind::Float, SampleKind::Hist, SampleKind::FloatHist] {
        for after_samples in [
            vec![(min_ms(10), 1), (min_ms(32), 1), (min_ms(50), 1)],
            vec![(min_ms(10), 1), (min_ms(32), 1), (min_ms(50), 1), (min_ms(25), 2), (min_ms(31), 2)],
        ] {
            let dir = tempfile::tempdir().unwrap();
            let opts = TsdbOptions {
                head: HeadOptions { ooo_cap_max: 5, ooo_time_window_ms: min_ms(120), ..HeadOptions::default() },
                ..TsdbOptions::default()
            };
            let db = Tsdb::open(dir.path(), opts).unwrap();
            let s_ref = {
                let mut app = db.appender();
                let r = append_kind(&mut app, kind, &[("l", "v1")], min_ms(120), 120);
                app.commit().unwrap();
                r
            };
            {
                let mut app = db.appender();
                for (ts, v) in [
                    (min_ms(20), 0), (min_ms(22), 0), (min_ms(24), 0), (min_ms(26), 0), (min_ms(30), 0),
                    (min_ms(25), 1), (min_ms(35), 1),
                ] {
                    append_kind(&mut app, kind, &[("l", "v1")], ts, v);
                }
                app.commit().unwrap();
            }
            let _ = &matchers;
            let (_, _, snap) = db
                .head()
                .series_chunk_metas_no_bytes_with_snapshot(s_ref, min_ms(0), min_ms(100), None)
                .unwrap()
                .unwrap();
            {
                let mut app = db.appender();
                for (ts, v) in after_samples {
                    append_kind(&mut app, kind, &[("l", "v1")], ts, v);
                }
                app.commit().unwrap();
            }
            let chks = db.head().series_final_chunks_from_snapshot(None, s_ref, snap).unwrap();
            if matches!(kind, SampleKind::Float) {
                assert_eq!(chks.len(), 1);
            }
            let mut got = Vec::new();
            for cm in &chks {
                for s in decode_chunk_samples(cm.chunk.as_ref().unwrap()).unwrap() {
                    got.push((s.t, sample_val_num(&s)));
                }
            }
            assert_eq!(got, vec![
                (min_ms(20), 0), (min_ms(22), 0), (min_ms(24), 0), (min_ms(25), 1),
                (min_ms(26), 0), (min_ms(30), 0), (min_ms(35), 1),
            ]);
            db.close().unwrap();
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/ooo_head_read_test.go::TestSortMetaByMinTimeAndMinRef
#[test]
fn test_sort_meta_by_min_time_and_min_ref() {
    let mut ordered = [
        ChunkMeta { ref_id: 0, min_time: 0, max_time: 0, chunk: None },
        ChunkMeta { ref_id: 1, min_time: 1, max_time: 1, chunk: None },
    ];
    ordered.sort_by(less_by_min_time_and_min_ref);
    assert_eq!(ordered.iter().map(|m| m.ref_id).collect::<Vec<_>>(), vec![0, 1]);

    let mut same_min = [
        ChunkMeta { ref_id: 10, min_time: 0, max_time: 0, chunk: None },
        ChunkMeta { ref_id: 5, min_time: 0, max_time: 0, chunk: None },
    ];
    same_min.sort_by(less_by_min_time_and_min_ref);
    assert_eq!(same_min.iter().map(|m| m.ref_id).collect::<Vec<_>>(), vec![5, 10]);
}

// Upstream: github.com/prometheus/prometheus/tsdb/ooo_head_read_test.go::TestHeadAndOOOQuerierSearch
#[test]
fn test_head_and_ooo_querier_search() {
    let empty_q = TsdbQuerier::new(None, Vec::new(), 0, 100);
    assert!(empty_q.search_label_names(&SearchHints::default(), &[]).unwrap().is_empty());
    assert!(empty_q.search_label_values("env", &SearchHints::default(), &[]).unwrap().is_empty());

    let dir = tempfile::tempdir().unwrap();
    let db = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
    {
        let mut app = db.appender();
        app.append_float(None, &[("__name__", "m"), ("env", "prod"), ("job", "api")], 10, 1.0).unwrap();
        app.append_float(None, &[("__name__", "m"), ("env", "dev"), ("job", "api")], 10, 2.0).unwrap();
        app.commit().unwrap();
    }
    let q = db.querier(0, 100);
    let names: Vec<String> = q.search_label_names(&SearchHints::default(), &[]).unwrap().into_iter().map(|r| r.value).collect();
    assert_eq!(names, vec!["__name__", "env", "job"]);
    let vals: Vec<String> = q.search_label_values("env", &SearchHints::default(), &[]).unwrap().into_iter().map(|r| r.value).collect();
    assert_eq!(vals, vec!["dev", "prod"]);
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/ooo_head_read_test.go::TestNewOOOCompactionHead_panicReleasesLock
#[test]
fn test_new_ooo_compaction_head_error_releases_lock() {
    let dir = tempfile::tempdir().unwrap();
    let opts = TsdbOptions {
        head: HeadOptions { ooo_time_window_ms: 120 * 60_000, ..HeadOptions::default() },
        ..TsdbOptions::default()
    };
    let db = Tsdb::open(dir.path(), opts).unwrap();
    let in_order_ts = 60 * 60_000i64;
    let ref_id = {
        let mut app = db.appender();
        let r = app.append_float(None, &[("__name__", "series")], in_order_ts, 0.0).unwrap();
        app.append_float(Some(r), &[("__name__", "series")], in_order_ts - 5 * 60_000, 1.0).unwrap();
        app.commit().unwrap();
        r
    };
    fs::remove_dir_all(dir.path().join("chunks_head")).unwrap();
    let res = db.head().collect_ooo_compaction_candidates_with_watermark();
    assert!(res.is_err(), "expected error when chunks_head directory was removed");
    assert_eq!(db.head().is_series_locked(ref_id), Some(false), "series lock leaked after failure");
    let _ = db.close();
}

// Upstream: github.com/prometheus/prometheus/tsdb/ooo_head_read_test.go::TestOOOQueryAcrossChunkIDWrap
#[test]
fn test_ooo_query_across_chunk_id_wrap() {
    let min_ms = |m: i64| m * 60_000;
    let matchers = [LabelMatcher::new(MatchType::Equal, "l", "v1").unwrap()];
    for kind in [SampleKind::Float, SampleKind::Hist, SampleKind::FloatHist] {
        let dir = tempfile::tempdir().unwrap();
        let opts = TsdbOptions {
            head: HeadOptions { ooo_cap_max: 5, ooo_time_window_ms: min_ms(120), ..HeadOptions::default() },
            ..TsdbOptions::default()
        };
        let db = Tsdb::open(dir.path(), opts).unwrap();
        let mut exp = Vec::new();
        {
            let mut app = db.appender();
            append_kind(&mut app, kind, &[("l", "v1")], min_ms(120), 120);
            exp.push((min_ms(120), 120));
            for i in 1..=10i64 {
                append_kind(&mut app, kind, &[("l", "v1")], min_ms(i), i);
                exp.push((min_ms(i), i));
            }
            app.commit().unwrap();
        }
        {
            let mut app = db.appender();
            for i in 11..=20i64 {
                append_kind(&mut app, kind, &[("l", "v1")], min_ms(i), i);
                exp.push((min_ms(i), i));
            }
            app.commit().unwrap();
        }
        exp.sort_by_key(|&(t, _)| t);
        let series = db.querier(0, min_ms(200)).select(false, &matchers).unwrap();
        assert_eq!(series.len(), 1);
        let got: Vec<(i64, i64)> = series[0].samples.iter().map(|s| {
            let v = match &s.value {
                SampleValue::Float(f) => *f as i64,
                SampleValue::Histogram(h) => h.zero_count as i64,
                SampleValue::FloatHistogram(fh) => fh.zero_count as i64,
            };
            (s.t, v)
        }).collect();
        assert_eq!(got, exp);
        db.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/ooo_head_read_test.go::TestOOOHeadChunkID_Wrap
#[test]
fn test_ooo_head_chunk_id_wrap() {
    let first_id = OOO_CHUNK_ID_MASK - 5;
    for pos in 0..=10usize {
        let id = ooo_head_chunk_id(first_id, pos);
        let expected = (pos as u32 + OOO_CHUNK_ID_MASK - 5) % OOO_CHUNK_ID_MASK;
        assert_ne!(id & OOO_CHUNK_ID_MASK, 0);
        assert!(id < MAX_HEAD_CHUNK_ID);
        assert_eq!(id & (OOO_CHUNK_ID_MASK - 1), expected);
        let packed = pack_head_chunk_ref(1, id);
        let (_, chunk_id, is_ooo) = unpack_head_chunk_ref(packed);
        assert!(is_ooo);
        assert_eq!(chunk_id, expected);
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/ooo_head_read_test.go::TestOOOChunk_ResolveAfterWrap
#[test]
fn test_ooo_chunk_resolve_after_wrap() {
    let dir = tempfile::tempdir().unwrap();
    let mut cw = HeadChunksWriter::open(dir.path()).unwrap();
    let first_id_before = OOO_CHUNK_ID_MASK - 5;
    let mut raw_ids = Vec::with_capacity(10);
    let mut refs = Vec::with_capacity(10);
    for i in 0..10usize {
        let r = cw.write_chunk(1, (i * 100) as i64, (i * 100 + 99) as i64, &dummy_xor_chunk(0, 1), true).unwrap();
        refs.push(r);
        let id = ooo_head_chunk_id(first_id_before, i);
        let (_, raw_id, _) = unpack_head_chunk_ref(pack_head_chunk_ref(1, id));
        raw_ids.push(raw_id);
    }
    cw.close().unwrap();
    let surviving = &refs[8..];
    let first_id_after = (first_id_before + 8) % OOO_CHUNK_ID_MASK;
    assert_eq!(first_id_after, 3);
    for pos in [8usize, 9] {
        let resolved = resolve_ooo_chunk_pos(first_id_after, raw_ids[pos], surviving.len());
        assert_eq!(resolved, Some(pos - 8));
    }
    for &raw_id in raw_ids.iter().take(8) {
        assert_eq!(resolve_ooo_chunk_pos(first_id_after, raw_id, surviving.len()), None);
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/ooo_head_read_test.go::TestOOOMmapGuardPreventsAliasing
#[test]
fn test_ooo_mmap_guard_prevents_aliasing() {
    assert_eq!(OOO_CHUNK_ID_MASK - 1, (1 << 23) - 1);
    let first = OOO_CHUNK_ID_MASK - 100;
    let mut seen = HashSet::new();
    for pos in 0..200usize {
        let id = ooo_head_chunk_id(first, pos);
        assert!(id < MAX_HEAD_CHUNK_ID);
        assert!(seen.insert(id), "position {pos} produced duplicate HeadChunkID");
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/ooo_head_test.go::TestOOOInsert
#[test]
fn test_ooo_insert() {
    const TEST_MAX: usize = 32;
    let val_even = |pos: usize| -> i64 { (pos as i64) * 2 + 2 };
    let val_odd = |pos: usize| -> i64 { (pos as i64) * 2 + 1 };
    let make_val = |kind: usize, st_mode: usize, ts: i64| -> (i64, OooSampleValue) {
        let st = match st_mode {
            0 => 0,
            1 => ts,
            2 => ts - 100,
            _ => 1000 - ts,
        };
        let v = match kind {
            0 => OooSampleValue::Float(ts as f64),
            1 => OooSampleValue::Histogram(gen_hist(ts)),
            _ => OooSampleValue::FloatHistogram(gen_fhist(ts)),
        };
        (st, v)
    };
    for (kind, st_mode) in [(0, 0), (0, 1), (0, 2), (0, 3), (1, 0), (1, 1), (2, 0), (2, 1)] {
        for num_pre in 0..=TEST_MAX {
            for ins_pos in 0..=num_pre {
                let mut chunk = OooChunk::new();
                for i in 0..num_pre {
                    let ts = val_even(i);
                    let (st, v) = make_val(kind, st_mode, ts);
                    assert!(chunk.insert(st, ts, v));
                }
                let new_ts = val_odd(ins_pos);
                let (new_st, new_v) = make_val(kind, st_mode, new_ts);
                assert!(chunk.insert(new_st, new_ts, new_v.clone()));
                let got = chunk.samples();
                assert_eq!(got.len(), num_pre + 1);
                let mut idx = 0;
                for i in 0..ins_pos {
                    let ts = val_even(i);
                    let (st, v) = make_val(kind, st_mode, ts);
                    assert_eq!((got[idx].st, got[idx].t), (st, ts));
                    assert!(got[idx].value.prom_equals(&v));
                    idx += 1;
                }
                assert_eq!((got[idx].st, got[idx].t), (new_st, new_ts));
                assert!(got[idx].value.prom_equals(&new_v));
                idx += 1;
                for i in ins_pos..num_pre {
                    let ts = val_even(i);
                    let (st, v) = make_val(kind, st_mode, ts);
                    assert_eq!((got[idx].st, got[idx].t), (st, ts));
                    assert!(got[idx].value.prom_equals(&v));
                    idx += 1;
                }
            }
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/ooo_head_test.go::TestOOOInsertDuplicate
#[test]
fn test_ooo_insert_duplicate() {
    const TEST_MAX: usize = 32;
    let val_even = |pos: usize| -> i64 { (pos as i64) * 2 + 2 };
    for (kind, st_mode) in [(0, 0), (0, 1), (0, 2), (0, 3), (1, 0), (1, 1), (2, 0), (2, 1)] {
        for num in 1..=TEST_MAX {
            for dup_pos in 0..num {
                let mut chunk = OooChunk::new();
                for i in 0..num {
                    let ts = val_even(i);
                    let st = match st_mode { 0 => 0, 1 => ts, 2 => ts - 100, _ => 1000 - ts };
                    let v = match kind {
                        0 => OooSampleValue::Float(ts as f64),
                        1 => OooSampleValue::Histogram(gen_hist(ts)),
                        _ => OooSampleValue::FloatHistogram(gen_fhist(ts)),
                    };
                    assert!(chunk.insert(st, ts, v));
                }
                let before = chunk.samples();
                let dup_ts = val_even(dup_pos);
                let ok = chunk.insert(0, dup_ts, OooSampleValue::Float(0.123));
                assert!(!ok);
                assert_eq!(chunk.samples(), before);
            }
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/ooo_head_test.go::TestOOOChunks_ToEncodedChunks
#[test]
fn test_ooo_chunks_to_encoded_chunks() {
    let h1 = gen_hist(1);
    let mut h2 = h1.clone();
    h2.positive_spans.push(HistogramSpan { offset: 1, length: 1 });
    h2.positive_buckets.push(12);
    let mut h2_explicit = h2.clone();
    h2_explicit.counter_reset_hint = CounterResetHint::CounterReset;

    // Empty
    assert!(OooChunk::new().to_encoded_chunks(i64::MIN, i64::MAX, false, false).unwrap().is_empty());

    // Floats
    {
        let mut c = OooChunk::new();
        c.insert_float(0, 1000, 43.0);
        c.insert_float(0, 1100, 42.0);
        let cut = c.to_encoded_chunks(i64::MIN, i64::MAX, false, false).unwrap();
        assert_eq!(cut.len(), 1);
        assert_eq!((cut[0].chunk.encoding(), cut[0].min_time, cut[0].max_time), (Encoding::Xor, 1000, 1100));
    }

    // Mix of floats and histograms
    {
        let mut c = OooChunk::new();
        c.insert_float(0, 1000, 43.0);
        c.insert_histogram(0, 1100, h1.clone());
        c.insert_float(0, 1200, 42.0);
        let cut = c.to_encoded_chunks(i64::MIN, i64::MAX, false, false).unwrap();
        assert_eq!(cut.len(), 3);
        assert_eq!((cut[0].chunk.encoding(), cut[0].min_time, cut[0].max_time), (Encoding::Xor, 1000, 1000));
        assert_eq!((cut[1].chunk.encoding(), cut[1].min_time, cut[1].max_time), (Encoding::Histogram, 1100, 1100));
        assert_eq!((cut[2].chunk.encoding(), cut[2].min_time, cut[2].max_time), (Encoding::Xor, 1200, 1200));
    }

    // Implicit counter reset
    {
        let mut c = OooChunk::new();
        c.insert_histogram(0, 1000, h2.clone());
        c.insert_histogram(0, 1100, h1.clone());
        let cut = c.to_encoded_chunks(i64::MIN, i64::MAX, false, false).unwrap();
        assert_eq!(cut.len(), 2);
        assert_eq!((cut[0].min_time, cut[0].max_time), (1000, 1000));
        assert_eq!((cut[1].min_time, cut[1].max_time), (1100, 1100));
    }

    // Explicit counter reset single & inside
    {
        let mut c1 = OooChunk::new();
        c1.insert_histogram(0, 1100, h2_explicit.clone());
        let cut1 = c1.to_encoded_chunks(i64::MIN, i64::MAX, false, false).unwrap();
        assert_eq!(cut1.len(), 1);
        assert_eq!((cut1[0].min_time, cut1[0].max_time), (1100, 1100));

        let mut c2 = OooChunk::new();
        c2.insert_histogram(0, 1000, h1.clone());
        c2.insert_histogram(0, 1100, h2_explicit);
        let cut2 = c2.to_encoded_chunks(i64::MIN, i64::MAX, false, false).unwrap();
        assert_eq!(cut2.len(), 2);
        assert_eq!((cut2[0].min_time, cut2[0].max_time), (1000, 1000));
        assert_eq!((cut2[1].min_time, cut2[1].max_time), (1100, 1100));
    }

    // Recoded histogram keeps min_time=0, max_time=1 in single chunk
    {
        let mut c = OooChunk::new();
        c.insert_histogram(0, 0, h1);
        c.insert_histogram(0, 1, h2);
        let cut = c.to_encoded_chunks(i64::MIN, i64::MAX, false, false).unwrap();
        assert_eq!(cut.len(), 1);
        assert_eq!((cut[0].chunk.encoding(), cut[0].min_time, cut[0].max_time), (Encoding::Histogram, 0, 1));
        let ds = decode_chunk_samples(&cut[0].chunk).unwrap();
        assert_eq!(ds.len(), 2);
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/ooo_head_test.go::TestOOOChunks_ToEncodedChunks_WithST
#[test]
fn test_ooo_chunks_to_encoded_chunks_with_st() {
    let h1 = gen_hist(1);
    let h2 = gen_hist(2);
    let h3 = gen_hist(3);
    let fh1 = gen_fhist(1);
    let fh2 = gen_fhist(2);
    let fh3 = gen_fhist(3);
    let cases = [
        (Encoding::Xor2, Encoding::Xor, vec![(0, 1000, OooSampleValue::Float(43.0)), (0, 1100, OooSampleValue::Float(42.0))]),
        (Encoding::Xor2, Encoding::Xor, vec![(1000, 1000, OooSampleValue::Float(43.0)), (1100, 1100, OooSampleValue::Float(42.0))]),
        (Encoding::Xor2, Encoding::Xor, vec![(900, 1000, OooSampleValue::Float(43.0)), (1000, 1100, OooSampleValue::Float(42.0))]),
        (Encoding::Xor2, Encoding::Xor, vec![(500, 1000, OooSampleValue::Float(43.0)), (1100, 1100, OooSampleValue::Float(42.0)), (0, 1200, OooSampleValue::Float(41.0))]),
        (Encoding::HistogramSt, Encoding::Histogram, vec![(0, 1000, OooSampleValue::Histogram(h1.clone())), (0, 1100, OooSampleValue::Histogram(h2.clone()))]),
        (Encoding::HistogramSt, Encoding::Histogram, vec![(900, 1000, OooSampleValue::Histogram(h1.clone())), (1000, 1100, OooSampleValue::Histogram(h2.clone()))]),
        (Encoding::HistogramSt, Encoding::Histogram, vec![(500, 1000, OooSampleValue::Histogram(h1)), (1100, 1100, OooSampleValue::Histogram(h2)), (0, 1200, OooSampleValue::Histogram(h3))]),
        (Encoding::FloatHistogramSt, Encoding::FloatHistogram, vec![(0, 1000, OooSampleValue::FloatHistogram(fh1.clone())), (0, 1100, OooSampleValue::FloatHistogram(fh2.clone()))]),
        (Encoding::FloatHistogramSt, Encoding::FloatHistogram, vec![(900, 1000, OooSampleValue::FloatHistogram(fh1.clone())), (1000, 1100, OooSampleValue::FloatHistogram(fh2.clone()))]),
        (Encoding::FloatHistogramSt, Encoding::FloatHistogram, vec![(500, 1000, OooSampleValue::FloatHistogram(fh1)), (1100, 1100, OooSampleValue::FloatHistogram(fh2)), (0, 1200, OooSampleValue::FloatHistogram(fh3))]),
    ];
    for (st_enc, reg_enc, samples) in &cases {
        for store_st in [true, false] {
            let mut c = OooChunk::new();
            for (st, t, v) in samples {
                c.insert(*st, *t, v.clone());
            }
            let cut = c.to_encoded_chunks(i64::MIN, i64::MAX, store_st, store_st).unwrap();
            assert_eq!(cut.len(), 1);
            assert_eq!(cut[0].chunk.encoding(), if store_st { *st_enc } else { *reg_enc });
            assert_eq!(cut[0].min_time, samples[0].1);
            assert_eq!(cut[0].max_time, samples.last().unwrap().1);
            let ds = decode_chunk_samples(&cut[0].chunk).unwrap();
            assert_eq!(ds.len(), samples.len());
            for (idx, s) in ds.iter().enumerate() {
                assert_eq!(s.st, if store_st { samples[idx].0 } else { 0 });
                assert_eq!(s.t, samples[idx].1);
            }
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/ooo_isolation_test.go::TestOOOIsolation
#[test]
fn test_ooo_isolation() {
    let iso = OooIsolation::new();
    for r in 0..=3u64 {
        assert!(!iso.has_open_reads_at_or_before(ChunkDiskMapperRef(r)));
    }
    let read1 = iso.track_read_after(ChunkDiskMapperRef(1));
    assert!(!iso.has_open_reads_at_or_before(ChunkDiskMapperRef(0)));
    assert!(!iso.has_open_reads_at_or_before(ChunkDiskMapperRef(1)));
    assert!(iso.has_open_reads_at_or_before(ChunkDiskMapperRef(2)));

    let read2 = iso.track_read_after(ChunkDiskMapperRef(0));
    assert!(!iso.has_open_reads_at_or_before(ChunkDiskMapperRef(0)));
    assert!(iso.has_open_reads_at_or_before(ChunkDiskMapperRef(1)));
    assert!(iso.has_open_reads_at_or_before(ChunkDiskMapperRef(2)));

    read2.close();
    assert!(!iso.has_open_reads_at_or_before(ChunkDiskMapperRef(0)));
    assert!(!iso.has_open_reads_at_or_before(ChunkDiskMapperRef(1)));
    assert!(iso.has_open_reads_at_or_before(ChunkDiskMapperRef(2)));

    // Idempotent double close.
    read2.close();
    assert!(!iso.has_open_reads_at_or_before(ChunkDiskMapperRef(0)));
    assert!(!iso.has_open_reads_at_or_before(ChunkDiskMapperRef(1)));
    assert!(iso.has_open_reads_at_or_before(ChunkDiskMapperRef(2)));

    read1.close();
    assert!(!iso.has_open_reads_at_or_before(ChunkDiskMapperRef(0)));
    assert!(!iso.has_open_reads_at_or_before(ChunkDiskMapperRef(1)));
    assert!(!iso.has_open_reads_at_or_before(ChunkDiskMapperRef(2)));
}

// Upstream: github.com/prometheus/prometheus/tsdb/ooo_samples_lost_after_restart_test.go::TestOOOSamplesLostAfterRestart
#[test]
fn test_ooo_samples_lost_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let hour = 3_600_000i64;
    let min = 60_000i64;
    let open_db = || {
        let opts = TsdbOptions {
            head: HeadOptions { ooo_time_window_ms: 6 * hour, ..HeadOptions::default() },
            ..TsdbOptions::default()
        };
        Tsdb::open(dir.path(), opts).unwrap()
    };
    let add = |db: &Tsdb, name: &str, ts: &[i64]| {
        let mut app = db.appender();
        for &x in ts {
            app.append_float(None, &[("__name__", name)], x, 1.0).unwrap();
        }
        app.commit().unwrap();
    };
    let run = |db: &Tsdb, from: i64, to: i64| {
        let mut ts = from;
        while ts < to {
            add(db, "scraped", &[ts]);
            db.head().mmap_head_chunks().unwrap();
            db.compact().unwrap();
            ts += min;
        }
    };
    let late = |db: &Tsdb, now: i64| {
        let ts: Vec<i64> = (0..40i64).map(|i| now - 150 * min + i * 15_000).collect();
        add(db, "late", &ts);
    };
    let count = |db: &Tsdb| -> usize {
        let m = [LabelMatcher::new(MatchType::Equal, "__name__", "late").unwrap()];
        db.querier(i64::MIN, i64::MAX)
            .select(false, &m)
            .unwrap()
            .into_iter()
            .map(|s| s.samples.len())
            .sum()
    };

    let db = open_db();
    run(&db, 0, 3 * hour);
    late(&db, 3 * hour);
    run(&db, 3 * hour, 4 * hour + 20 * min);
    db.close().unwrap();

    let db = open_db();
    run(&db, 4 * hour + 20 * min, 5 * hour + 5 * min);
    late(&db, 5 * hour + 5 * min);
    run(&db, 5 * hour + 5 * min, 10 * hour);
    db.close().unwrap();

    let db = open_db();
    assert_eq!(count(&db), 80);
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/isolation_test.go::TestIsolation
#[test]
fn test_isolation() {
    let iso = Isolation::new(false);
    assert_eq!(iso.low_watermark(), 0);
    assert_eq!(iso.lowest_append_time(), i64::MAX);

    let append_a = iso.new_append_id(10);
    assert_eq!(append_a, (1, 1));
    assert_eq!(iso.low_watermark(), 1);
    assert_eq!(iso.open_reads_count(), 0);
    assert_eq!(iso.lowest_append_time(), 10);

    let state_a = iso.state(10, 20);
    assert_eq!(iso.open_reads_count(), 1);

    let append_b = iso.new_append_id(20);
    assert_eq!(append_b, (2, 1));
    assert_eq!(iso.low_watermark(), 1);
    assert_eq!(iso.lowest_append_time(), 10);

    iso.close_append(append_a.0);
    assert_eq!(iso.low_watermark(), 1);
    assert_eq!(iso.open_reads_count(), 1);
    assert_eq!(iso.lowest_append_time(), 20);

    state_a.close();
    assert_eq!(iso.low_watermark(), 2);
    assert_eq!(iso.open_reads_count(), 0);

    iso.close_append(append_b.0);
    assert_eq!(iso.low_watermark(), 2);
    assert_eq!(iso.lowest_append_time(), i64::MAX);
}

// Upstream: github.com/prometheus/prometheus/tsdb/isolation_test.go::TestTxRingLastAppendID
#[test]
fn test_tx_ring_last_append_id() {
    let empty = TxRing::new(0);
    assert_eq!(empty.last_append_id(), None);

    let mut ring = TxRing::new(2);
    assert_eq!(ring.last_append_id(), None);

    ring.add(1);
    assert_eq!(ring.last_append_id(), Some(1));

    ring.add(2);
    assert_eq!(ring.last_append_id(), Some(2));

    ring.cleanup_append_ids_below(2);
    ring.add(3);
    assert_eq!(ring.last_append_id(), Some(3));
}

// Upstream: github.com/prometheus/prometheus/tsdb/isolation_test.go::TestCommittedAppendID
#[test]
fn test_committed_append_id() {
    // Disabled isolation always returns 0.
    {
        let iso = Isolation::new(true);
        assert_eq!(iso.committed_append_id(), 0);
        let _ = iso.new_append_id(0);
        assert_eq!(iso.committed_append_id(), 0);
    }
    // No appenders ever opened.
    {
        let iso = Isolation::new(false);
        assert_eq!(iso.committed_append_id(), 0);
    }
    // All issued IDs closed returns last_append_id.
    {
        let iso = Isolation::new(false);
        let (id_a, _) = iso.new_append_id(0);
        let (id_b, _) = iso.new_append_id(0);
        iso.close_append(id_a);
        iso.close_append(id_b);
        assert_eq!(iso.committed_append_id(), id_b);
        assert_eq!(iso.last_append_id(), iso.committed_append_id());
    }
    // Single open appender caps committed at id - 1.
    {
        let iso = Isolation::new(false);
        let (id, _) = iso.new_append_id(0);
        assert_eq!(iso.committed_append_id(), id - 1);
    }
    // Multiple open appenders track the lowest.
    {
        let iso = Isolation::new(false);
        let (id_a, _) = iso.new_append_id(0);
        let (id_b, _) = iso.new_append_id(0);
        let (id_c, _) = iso.new_append_id(0);
        assert_eq!(iso.committed_append_id(), id_a - 1);
        iso.close_append(id_c);
        assert_eq!(iso.committed_append_id(), id_a - 1);
        iso.close_append(id_a);
        assert_eq!(iso.committed_append_id(), id_b - 1);
        iso.close_append(id_b);
        assert_eq!(iso.committed_append_id(), id_c);
    }
    // Open reads do not influence committed.
    {
        let iso = Isolation::new(false);
        let (id_a, _) = iso.new_append_id(0);
        let state = iso.state(0, i64::MAX);
        iso.close_append(id_a);
        assert_eq!(iso.committed_append_id(), id_a);
        state.close();
        assert_eq!(iso.committed_append_id(), id_a);
    }
}

fn run_symbols_batch_and_iteration(flushers_concurrency: usize) {
    let flushers = SymbolFlushers::new(flushers_concurrency);
    let dir = tempfile::tempdir().unwrap();
    let mut b = SymbolsBatcher::new(100, dir.path(), Arc::clone(&flushers));
    let mut all_words = HashSet::new();
    for i in 0..(10 * flushers_concurrency) {
        b.add_symbol("").unwrap();
        all_words.insert(String::new());
        for j in 0..123usize {
            let w = format!("word_{}_{}", i % 3, j);
            b.add_symbol(&w).unwrap();
            all_words.insert(w);
        }
    }
    b.flush_symbols(true).unwrap();
    b.flush_symbols(true).unwrap();
    flushers.close().unwrap();

    let mut it = SymbolsIterator::open(b.symbol_files()).unwrap();
    let mut first = true;
    let mut prev = String::new();
    while let Some(w) = it.next_symbol().unwrap() {
        if !first {
            assert!(!w.is_empty());
            assert!(prev < w);
        }
        first = false;
        assert!(all_words.remove(&w), "unexpected symbol {w:?}");
        prev = w;
    }
    it.close().unwrap();
    assert!(all_words.is_empty());
}

// Upstream: github.com/prometheus/prometheus/tsdb/symbols_batch_test.go::TestSymbolsBatchAndIteration1
#[test]
fn test_symbols_batch_and_iteration_1() {
    run_symbols_batch_and_iteration(1);
}

// Upstream: github.com/prometheus/prometheus/tsdb/symbols_batch_test.go::TestSymbolsBatchAndIteration5
#[test]
fn test_symbols_batch_and_iteration_5() {
    run_symbols_batch_and_iteration(5);
}
