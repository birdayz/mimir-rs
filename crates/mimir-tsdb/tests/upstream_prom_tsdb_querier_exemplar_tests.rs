//! Ported upstream Prometheus/Mimir TSDB querier, exemplar, postings-cache, lookup-planner, and query-cost tests.
//!
//! Sources:
//! - querier_test.go (30)
//! - exemplar_test.go (11)
//! - postings_for_matchers_cache_test.go (6)
//! - querier_lookup_planner_test.go (2)
//! - query_cost_test.go (1)

use std::collections::HashSet;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use mimir_tsdb::block::{Block, BlockMetaCompaction};
use mimir_tsdb::chunkenc::{
    CounterResetHint, EncodedChunk, Encoding, FloatHistogram, Histogram, HistogramSpan, Xor2Chunk,
    XorChunk,
};
use mimir_tsdb::chunks_block::ChunkMeta;
use mimir_tsdb::compact::{
    decode_chunk_samples, trim_and_merge_chunks, write_block_from_series, CompactionSeries,
    DecodedSample, SampleValue,
};
use mimir_tsdb::db::{Tsdb, TsdbOptions};
use mimir_tsdb::exemplar::{
    CircularExemplarStorage, Exemplar, ExemplarError, EXEMPLAR_MAX_LABEL_SET_LENGTH,
};
use mimir_tsdb::head::{Head, HeadOptions};
use mimir_tsdb::index::{ConcreteLookupPlan, IndexError, IndexLookupPlanner, LookupPlan};
use mimir_tsdb::labels::{append_stringlabel_pair, Labels};
use mimir_tsdb::matcher::{LabelMatcher, MatchType};
use mimir_tsdb::postings::{
    postings_for_matchers, ContextCheck, EmptyPostingsIter, ListPostings, PostingsError,
    PostingsIter, PostingsSource, PrependPostings,
};
use mimir_tsdb::postings_cache::{
    next_postings_cache_source_id, shared_cache_key_string, PagedMetricVersions,
    PostingsForMatchersCache, PostingsForMatchersCacheConfig, PostingsForMatchersCacheMetrics,
};
use mimir_tsdb::querier::{
    DeletedIterator, QuerierError, TsdbChunkQuerier, TsdbQuerier,
};
use mimir_tsdb::search::{SearchHints, SearchOrdering};
use mimir_tsdb::tombstones::{write_tombstones_file, Interval, MemTombstones};

fn xor_chunk_samples(samples: &[(i64, i64, f64)], use_xor2: bool) -> EncodedChunk {
    if use_xor2 {
        let mut xc = Xor2Chunk::new();
        {
            let mut app = xc.appender().unwrap();
            for &(st, t, v) in samples {
                app.append(st, t, v).unwrap();
            }
        }
        EncodedChunk::Xor2(xc)
    } else {
        let mut xc = XorChunk::new();
        {
            let mut app = xc.appender().unwrap();
            for &(_, t, v) in samples {
                app.append(0, t, v).unwrap();
            }
        }
        EncodedChunk::Xor(xc)
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

fn mk_block_testdata(dir: &Path) -> Arc<Block> {
    let series = vec![
        CompactionSeries {
            labels: Labels::from_pairs([("a", "a")]),
            chunks: vec![
                ChunkMeta { ref_id: 0, min_time: 1, max_time: 3, chunk: Some(xor_chunk_samples(&[(0, 1, 2.0), (0, 2, 3.0), (0, 3, 4.0)], false)) },
                ChunkMeta { ref_id: 1, min_time: 5, max_time: 7, chunk: Some(xor_chunk_samples(&[(0, 5, 2.0), (0, 6, 3.0), (0, 7, 4.0)], false)) },
            ],
            tombstones: Vec::new(),
        },
        CompactionSeries {
            labels: Labels::from_pairs([("a", "a"), ("b", "b")]),
            chunks: vec![
                ChunkMeta { ref_id: 0, min_time: 1, max_time: 3, chunk: Some(xor_chunk_samples(&[(0, 1, 1.0), (0, 2, 2.0), (0, 3, 3.0)], false)) },
                ChunkMeta { ref_id: 1, min_time: 5, max_time: 6, chunk: Some(xor_chunk_samples(&[(0, 5, 3.0), (0, 6, 6.0)], false)) },
            ],
            tombstones: Vec::new(),
        },
        CompactionSeries {
            labels: Labels::from_pairs([("b", "b")]),
            chunks: vec![
                ChunkMeta { ref_id: 0, min_time: 1, max_time: 3, chunk: Some(xor_chunk_samples(&[(0, 1, 3.0), (0, 2, 2.0), (0, 3, 6.0)], false)) },
                ChunkMeta { ref_id: 1, min_time: 5, max_time: 7, chunk: Some(xor_chunk_samples(&[(0, 5, 1.0), (0, 6, 7.0), (0, 7, 2.0)], false)) },
            ],
            tombstones: Vec::new(),
        },
    ];
    let meta = write_block_from_series(dir, 0, 10, series, BlockMetaCompaction::default(), false, 0).unwrap().unwrap();
    Arc::new(Block::open(dir.join(meta.ulid.to_string())).unwrap())
}

fn float_pairs(samples: &[DecodedSample]) -> Vec<(i64, f64)> {
    samples.iter().map(|s| match s.value { SampleValue::Float(v) => (s.t, v), _ => panic!() }).collect()
}

// Upstream: github.com/prometheus/prometheus/tsdb/querier_test.go::TestBlockQuerier
#[test]
fn test_block_querier() {
    let dir = tempfile::tempdir().unwrap();
    let blk = mk_block_testdata(dir.path());
    assert!(TsdbQuerier::new(None, vec![Arc::clone(&blk)], 0, 0).select(true, &[]).unwrap().is_empty());
    assert!(TsdbQuerier::new(None, vec![Arc::clone(&blk)], 0, 0).select(true, &[LabelMatcher::new(MatchType::Equal, "a", "a").unwrap()]).unwrap().is_empty());
    assert!(TsdbQuerier::new(None, vec![Arc::clone(&blk)], 1, 0).select(true, &[LabelMatcher::new(MatchType::Equal, "a", "a").unwrap()]).unwrap().is_empty());
    assert!(TsdbQuerier::new(None, vec![Arc::clone(&blk)], i64::MIN, i64::MAX).select(true, &[LabelMatcher::new(MatchType::Equal, "a", "x").unwrap()]).unwrap().is_empty());

    let all = TsdbQuerier::new(None, vec![Arc::clone(&blk)], i64::MIN, i64::MAX).select(true, &[LabelMatcher::new(MatchType::RegexMatch, "a", ".*").unwrap()]).unwrap();
    assert_eq!(all.len(), 3);
    assert_eq!(float_pairs(&all[0].samples), vec![(1, 2.0), (2, 3.0), (3, 4.0), (5, 2.0), (6, 3.0), (7, 4.0)]);

    let sub = TsdbQuerier::new(None, vec![Arc::clone(&blk)], 2, 6).select(true, &[LabelMatcher::new(MatchType::Equal, "a", "a").unwrap()]).unwrap();
    assert_eq!(sub.len(), 2);
    assert_eq!(float_pairs(&sub[0].samples), vec![(2, 3.0), (3, 4.0), (5, 2.0), (6, 3.0)]);
    assert_eq!(float_pairs(&sub[1].samples), vec![(2, 2.0), (3, 3.0), (5, 3.0), (6, 6.0)]);

    // ChunkQuerier returns untrimmed chunks overlapping [mint, maxt].
    let chks = TsdbChunkQuerier::new(None, vec![blk], 5, 6).select(true, &[LabelMatcher::new(MatchType::Equal, "a", "a").unwrap()]).unwrap();
    assert_eq!(chks.len(), 2);
    assert_eq!(chks[0].chunks.len(), 1);
    assert_eq!(float_pairs(&decode_chunk_samples(chks[0].chunks[0].chunk.as_ref().unwrap()).unwrap()), vec![(5, 2.0), (6, 3.0), (7, 4.0)]);
}

// Upstream: github.com/prometheus/prometheus/tsdb/querier_test.go::TestBlockBaseQuerier_LabelNamesLimit
#[test]
fn test_block_base_querier_label_names_limit() {
    let dir = tempfile::tempdir().unwrap();
    let s = vec![CompactionSeries {
        labels: Labels::from_pairs([("aaa", "1"), ("bbb", "2"), ("ccc", "3")]),
        chunks: vec![ChunkMeta { ref_id: 0, min_time: 1, max_time: 1, chunk: Some(xor_chunk_samples(&[(0, 1, 1.0)], false)) }],
        tombstones: Vec::new(),
    }];
    let meta = write_block_from_series(dir.path(), 0, 10, s, BlockMetaCompaction::default(), false, 0).unwrap().unwrap();
    let blk = Arc::new(Block::open(dir.path().join(meta.ulid.to_string())).unwrap());
    let q = TsdbQuerier::new(None, vec![blk], 0, 10);
    assert_eq!(q.label_names(&[]).unwrap(), vec!["aaa", "bbb", "ccc"]);
    assert_eq!(q.label_names_with_limit(Some(2), &[]).unwrap(), vec!["aaa", "bbb"]);
    assert_eq!(q.label_names_with_limit(Some(10), &[]).unwrap(), vec!["aaa", "bbb", "ccc"]);
    assert_eq!(q.label_names_with_limit(Some(0), &[]).unwrap(), vec!["aaa", "bbb", "ccc"]);
}

// Upstream: github.com/prometheus/prometheus/tsdb/querier_test.go::TestBlockQuerier_AgainstHeadWithOpenChunks
#[test]
fn test_block_querier_against_head_with_open_chunks() {
    let dir = tempfile::tempdir().unwrap();
    let head = Head::open(dir.path(), HeadOptions { chunk_range_ms: 7_200_000, ..HeadOptions::default() }, i64::MIN).unwrap();
    {
        let mut app = head.appender();
        for (t, v) in [(1, 2.0), (2, 3.0), (3, 4.0), (5, 2.0), (6, 3.0), (7, 4.0)] {
            app.append_float(None, &[("a", "a")], t, v).unwrap();
        }
        for (t, v) in [(1, 1.0), (2, 2.0), (3, 3.0), (5, 3.0), (6, 6.0)] {
            app.append_float(None, &[("a", "a"), ("b", "b")], t, v).unwrap();
        }
        for (t, v) in [(1, 3.0), (2, 2.0), (3, 6.0), (5, 1.0), (6, 7.0), (7, 2.0)] {
            app.append_float(None, &[("b", "b")], t, v).unwrap();
        }
        app.commit().unwrap();
    }
    let sub = TsdbQuerier::new(Some(&head), Vec::new(), 2, 6).select(true, &[LabelMatcher::new(MatchType::Equal, "a", "a").unwrap()]).unwrap();
    assert_eq!(sub.len(), 2);
    assert_eq!(float_pairs(&sub[0].samples), vec![(2, 3.0), (3, 4.0), (5, 2.0), (6, 3.0)]);
    assert_eq!(float_pairs(&sub[1].samples), vec![(2, 2.0), (3, 3.0), (5, 3.0), (6, 6.0)]);
    head.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/querier_test.go::TestBlockQuerier_TrimmingDoesNotModifyOriginalTombstoneIntervals
#[test]
fn test_block_querier_trimming_does_not_modify_original_tombstones() {
    let dir = tempfile::tempdir().unwrap();
    let blk = mk_block_testdata(dir.path());
    blk.delete(1, 2, &[LabelMatcher::new(MatchType::RegexMatch, "a", "a").unwrap()]).unwrap();
    let refs = blk.postings_for_matchers(&[LabelMatcher::new(MatchType::RegexMatch, "a", "a").unwrap()]).unwrap();
    let before: Vec<_> = refs.iter().map(|&r| blk.tombstones().get(r)).collect();
    let q = TsdbQuerier::new(None, vec![Arc::clone(&blk)], 2, 6);
    let res = q.select(true, &[LabelMatcher::new(MatchType::RegexMatch, "a", "a").unwrap()]).unwrap();
    assert_eq!(float_pairs(&res[0].samples), vec![(3, 4.0), (5, 2.0), (6, 3.0)]);
    let after: Vec<_> = refs.iter().map(|&r| blk.tombstones().get(r)).collect();
    assert_eq!(before, after);
}

// Upstream: github.com/prometheus/prometheus/tsdb/querier_test.go::TestBlockQuerierDelete
#[test]
fn test_block_querier_delete() {
    let dir = tempfile::tempdir().unwrap();
    let blk = mk_block_testdata(dir.path());
    let refs = blk.postings_for_matchers(&[LabelMatcher::new(MatchType::RegexMatch, "", ".+").unwrap()]).unwrap();
    assert_eq!(refs.len(), 3);
    let tombs = MemTombstones::new();
    tombs.add_interval(refs[0], Interval::new(1, 3));
    tombs.add_interval(refs[1], Interval::new(1, 3));
    tombs.add_interval(refs[1], Interval::new(6, 10));
    tombs.add_interval(refs[2], Interval::new(6, 10));
    write_tombstones_file(blk.dir(), &tombs).unwrap();
    let blk = Arc::new(Block::open(blk.dir()).unwrap());

    let res = TsdbQuerier::new(None, vec![Arc::clone(&blk)], i64::MIN, i64::MAX)
        .select(true, &[LabelMatcher::new(MatchType::RegexMatch, "a", ".*").unwrap()])
        .unwrap();
    assert_eq!(res.len(), 3);
    assert_eq!(float_pairs(&res[0].samples), vec![(5, 2.0), (6, 3.0), (7, 4.0)]);
    assert_eq!(float_pairs(&res[1].samples), vec![(5, 3.0)]);
    assert_eq!(float_pairs(&res[2].samples), vec![(1, 3.0), (2, 2.0), (3, 6.0), (5, 1.0)]);
}

// Upstream: github.com/prometheus/prometheus/tsdb/querier_test.go::TestPopulateWithTombSeriesIterators
#[test]
fn test_populate_with_tomb_series_iterators() {
    let dir = tempfile::tempdir().unwrap();
    let mut series = Vec::new();
    for i in 0..3usize {
        let lbl = Labels::from_pairs([("a", format!("{i}").as_str())]);
        let mut chks = Vec::new();
        for c in 0..3i64 {
            let base = c * 10 + 1;
            chks.push(ChunkMeta {
                ref_id: c as u64,
                min_time: base,
                max_time: base + 2,
                chunk: Some(xor_chunk_samples(&[(0, base, base as f64), (0, base + 1, (base + 1) as f64), (0, base + 2, (base + 2) as f64)], false)),
            });
        }
        series.push(CompactionSeries { labels: lbl, chunks: chks, tombstones: Vec::new() });
    }
    let meta = write_block_from_series(dir.path(), 0, 40, series, BlockMetaCompaction::default(), false, 0).unwrap().unwrap();
    let blk = Block::open(dir.path().join(meta.ulid.to_string())).unwrap();
    blk.delete(10, 22, &[LabelMatcher::new(MatchType::Equal, "a", "1").unwrap()]).unwrap();
    let q = TsdbQuerier::new(None, vec![Arc::new(blk)], 0, 40);
    let res = q.select(true, &[LabelMatcher::new(MatchType::Equal, "a", "1").unwrap()]).unwrap();
    assert_eq!(res.len(), 1);
    let ts: Vec<i64> = res[0].samples.iter().map(|s| s.t).collect();
    assert_eq!(ts, vec![1, 2, 3, 23]);
}

// Upstream: github.com/prometheus/prometheus/tsdb/querier_test.go::TestPopulateWithDelSeriesIterator_DoubleSeek
#[test]
fn test_populate_with_del_series_iterator_double_seek() {
    let chk = xor_chunk_samples(&[(0, 1, 1.0), (0, 2, 2.0), (0, 3, 3.0)], false);
    let mut it = DeletedIterator::new(&chk, Vec::new()).unwrap();
    assert_eq!(it.seek(1).unwrap().t, 1);
    assert_eq!(it.seek(2).unwrap().t, 2);
    assert_eq!(it.seek(2).unwrap().t, 2);
}

// Upstream: github.com/prometheus/prometheus/tsdb/querier_test.go::TestPopulateWithDelSeriesIterator_SeekInCurrentChunk
#[test]
fn test_populate_with_del_series_iterator_seek_in_current_chunk() {
    let chk = xor_chunk_samples(&[(0, 1, 2.0), (0, 3, 4.0), (0, 5, 6.0), (0, 7, 8.0)], false);
    let mut it = DeletedIterator::new(&chk, Vec::new()).unwrap();
    assert_eq!(it.next_sample().unwrap().t, 1);
    assert_eq!(it.seek(4).unwrap().t, 5);
}

// Upstream: github.com/prometheus/prometheus/tsdb/querier_test.go::TestPopulateWithDelSeriesIterator_SeekWithMinTime
#[test]
fn test_populate_with_del_series_iterator_seek_with_min_time() {
    let chk = xor_chunk_samples(&[(0, 1, 6.0), (0, 5, 6.0), (0, 6, 8.0)], false);
    let mut it = DeletedIterator::new(&chk, vec![Interval::new(i64::MIN, 2), Interval::new(4, i64::MAX)]).unwrap();
    assert!(it.seek(1).is_none());
}

// Upstream: github.com/prometheus/prometheus/tsdb/querier_test.go::TestPopulateWithDelSeriesIterator_NextWithMinTime
#[test]
fn test_populate_with_del_series_iterator_next_with_min_time() {
    let chk = xor_chunk_samples(&[(0, 1, 6.0), (0, 5, 6.0), (0, 7, 8.0)], false);
    let mut it = DeletedIterator::new(&chk, vec![Interval::new(i64::MIN, 2), Interval::new(4, i64::MAX)]).unwrap();
    assert!(it.next_sample().is_none());
}

// Upstream: github.com/prometheus/prometheus/tsdb/querier_test.go::TestPopulateWithDelSeriesIterator_WithST
#[test]
fn test_populate_with_del_series_iterator_with_st() {
    let chk = xor_chunk_samples(&[(0, 1000, 1.0), (1500, 1500, 1.5), (1900, 2000, 2.0), (500, 3000, 3.0)], true);
    let mut it = DeletedIterator::new(&chk, Vec::new()).unwrap();
    let mut got = Vec::new();
    while let Some(s) = it.next_sample() {
        got.push((s.st, s.t));
    }
    assert_eq!(got, vec![(0, 1000), (1500, 1500), (1900, 2000), (500, 3000)]);
}

// Upstream: github.com/prometheus/prometheus/tsdb/querier_test.go::TestPopulateWithDelChunkSeriesIterator_WithST
#[test]
fn test_populate_with_del_chunk_series_iterator_with_st() {
    let chk = xor_chunk_samples(&[(100, 1000, 1.0), (200, 2000, 2.0), (300, 3000, 3.0)], true);
    let trimmed = trim_and_merge_chunks(
        vec![ChunkMeta { ref_id: 0, min_time: 1000, max_time: 3000, chunk: Some(chk) }],
        &[Interval::new(1500, 2500)],
        1000,
        3000,
        120,
    ).unwrap();
    assert_eq!(trimmed.len(), 1);
    let ds = decode_chunk_samples(trimmed[0].chunk.as_ref().unwrap()).unwrap();
    let got: Vec<(i64, i64)> = ds.iter().map(|s| (s.st, s.t)).collect();
    assert_eq!(got, vec![(100, 1000), (300, 3000)]);
}

// Upstream: github.com/prometheus/prometheus/tsdb/querier_test.go::TestDeletedIterator
#[test]
fn test_deleted_iterator() {
    let samples: Vec<(i64, i64, f64)> = (0..1000).map(|i| (i / 2, i, i as f64)).collect();
    let chk = xor_chunk_samples(&samples, true);
    let iv_sets = vec![
        vec![Interval::new(1, 20)],
        vec![Interval::new(1, 10), Interval::new(12, 20), Interval::new(21, 23), Interval::new(25, 30)],
        vec![Interval::new(1, 10), Interval::new(12, 23), Interval::new(25, 30)],
        vec![Interval::new(0, 2000)],
        vec![Interval::new(500, 2000)],
        vec![Interval::new(1000, 20000)],
    ];
    let mut it = DeletedIterator::new(&chk, Vec::new()).unwrap();
    for ivs in iv_sets {
        it.reset(&chk, ivs.clone()).unwrap();
        let mut got = Vec::new();
        while let Some(s) = it.next_sample() {
            assert_eq!(s.st, s.t / 2);
            got.push(s.t);
        }
        let exp: Vec<i64> = (0..1000).filter(|&t| !ivs.iter().any(|iv| iv.in_bounds(t))).collect();
        assert_eq!(got, exp);
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/querier_test.go::TestDeletedIterator_WithSeek
#[test]
fn test_deleted_iterator_with_seek() {
    let samples: Vec<(i64, i64, f64)> = (0..1000).map(|i| (0, i, i as f64)).collect();
    let chk = xor_chunk_samples(&samples, false);
    let cases = [
        (vec![Interval::new(1, 20)], 1, Some(21)),
        (vec![Interval::new(1, 20)], 20, Some(21)),
        (vec![Interval::new(1, 20)], 10, Some(21)),
        (vec![Interval::new(1, 20)], 999, Some(999)),
        (vec![Interval::new(1, 20)], 1000, None),
        (vec![Interval::new(1, 1000)], 5, None),
    ];
    for (ivs, seek_t, exp_t) in cases {
        let mut it = DeletedIterator::new(&chk, ivs).unwrap();
        assert_eq!(it.seek(seek_t).map(|s| s.t), exp_t);
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/querier_test.go::TestPostingsForMatchers
#[test]
fn test_postings_for_matchers() {
    let dir = tempfile::tempdir().unwrap();
    let head = Head::open(dir.path(), HeadOptions { chunk_range_ms: 1000, ..HeadOptions::default() }, i64::MIN).unwrap();
    {
        let mut app = head.appender();
        app.append_float(None, &[("n", "1")], 0, 0.0).unwrap();
        app.append_float(None, &[("n", "1"), ("i", "a")], 0, 0.0).unwrap();
        app.append_float(None, &[("n", "1"), ("i", "b")], 0, 0.0).unwrap();
        app.append_float(None, &[("n", "1"), ("i", "\n")], 0, 0.0).unwrap();
        app.append_float(None, &[("n", "2")], 0, 0.0).unwrap();
        app.append_float(None, &[("n", "2.5")], 0, 0.0).unwrap();
        app.commit().unwrap();
    }
    let q = TsdbQuerier::new(Some(&head), Vec::new(), 0, 10);
    let cases = [
        (vec![LabelMatcher::new(MatchType::Equal, "n", "1").unwrap()], 4),
        (vec![LabelMatcher::new(MatchType::Equal, "n", "1").unwrap(), LabelMatcher::new(MatchType::Equal, "i", "a").unwrap()], 1),
        (vec![LabelMatcher::new(MatchType::Equal, "n", "1").unwrap(), LabelMatcher::new(MatchType::Equal, "i", "missing").unwrap()], 0),
        (vec![LabelMatcher::new(MatchType::Equal, "missing", "").unwrap()], 6),
        (vec![LabelMatcher::new(MatchType::NotEqual, "n", "1").unwrap()], 2),
        (vec![LabelMatcher::new(MatchType::NotEqual, "i", "").unwrap()], 3),
        (vec![LabelMatcher::new(MatchType::NotEqual, "missing", "").unwrap()], 0),
        (vec![LabelMatcher::new(MatchType::Equal, "n", "1").unwrap(), LabelMatcher::new(MatchType::NotEqual, "i", "a").unwrap()], 3),
        (vec![LabelMatcher::new(MatchType::RegexMatch, "n", "^1$").unwrap()], 4),
        (vec![LabelMatcher::new(MatchType::RegexMatch, "i", "^$").unwrap()], 3),
    ];
    for (ms, exp_len) in cases {
        assert_eq!(q.select(true, &ms).unwrap().len(), exp_len);
    }
    head.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/querier_test.go::TestQuerierIndexQueriesRace
#[test]
fn test_querier_index_queries_race() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Tsdb::open(dir.path(), TsdbOptions::default()).unwrap());
    let stop = Arc::new(AtomicBool::new(false));
    let db_w = Arc::clone(&db);
    let stop_w = Arc::clone(&stop);
    let h = std::thread::spawn(move || {
        let mut i = 0usize;
        while !stop_w.load(Ordering::Acquire) {
            let mut app = db_w.appender();
            let seq = i.to_string();
            app.append_float(None, &[("__name__", "metric"), ("seq", &seq), ("always_0", "0")], 0, 0.0).unwrap();
            app.commit().unwrap();
            i += 1;
        }
    });
    let m1 = [LabelMatcher::new(MatchType::NotEqual, "__name__", "metric").unwrap()];
    let m2 = [
        LabelMatcher::new(MatchType::NotEqual, "always_0", "0").unwrap(),
        LabelMatcher::new(MatchType::Equal, "__name__", "metric").unwrap(),
    ];
    for _ in 0..100 {
        let q = db.querier(i64::MIN, i64::MAX);
        assert!(q.label_values("seq", None, &m1).unwrap().is_empty());
        assert!(q.label_values("seq", None, &m2).unwrap().is_empty());
    }
    stop.store(true, Ordering::Release);
    h.join().unwrap();
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/querier_test.go::TestClose
#[test]
fn test_close() {
    let dir = tempfile::tempdir().unwrap();
    let db = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
    db.close().unwrap();
    assert!(db.is_closed());
    assert!(matches!(db.querier(0, 20).select(false, &[]), Err(QuerierError::Closed)));
}

// Upstream: github.com/prometheus/prometheus/tsdb/querier_test.go::TestPostingsForMatcher
#[test]
fn test_postings_for_matcher() {
    struct FailingLvalsSource;
    impl PostingsSource for FailingLvalsSource {
        fn all_postings(&self) -> Result<Vec<u64>, PostingsError> {
            Ok(Vec::new())
        }
        fn postings_for(&self, _name: &str, _values: &[&str]) -> Result<Vec<u64>, PostingsError> {
            Ok(Vec::new())
        }
        fn postings_for_all_values(&self, _name: &str) -> Result<Vec<u64>, PostingsError> {
            Err(PostingsError::Index("label_values error".into()))
        }
        fn postings_for_label_matching(&self, _name: &str, _pred: &mut dyn FnMut(&str) -> bool) -> Result<Vec<u64>, PostingsError> {
            Err(PostingsError::Index("label_values error".into()))
        }
    }
    let src = FailingLvalsSource;
    assert!(postings_for_matchers(&src, &[LabelMatcher::new(MatchType::Equal, "test", "test").unwrap()]).is_ok());
    assert!(postings_for_matchers(&src, &[LabelMatcher::new(MatchType::RegexMatch, "test", "a|b").unwrap()]).is_ok());
    assert!(postings_for_matchers(&src, &[LabelMatcher::new(MatchType::RegexMatch, "test", "^(?:a|b)$").unwrap()]).is_ok());
}

// Upstream: github.com/prometheus/prometheus/tsdb/querier_test.go::TestBlockBaseSeriesSet
#[test]
fn test_block_base_series_set() {
    let dir = tempfile::tempdir().unwrap();
    let series = vec![
        CompactionSeries { labels: Labels::from_pairs([("a", "a")]), chunks: vec![ChunkMeta { ref_id: 0, min_time: 1, max_time: 1, chunk: Some(xor_chunk_samples(&[(0, 1, 1.0)], false)) }], tombstones: Vec::new() },
        CompactionSeries { labels: Labels::from_pairs([("a", "a"), ("b", "b")]), chunks: vec![ChunkMeta { ref_id: 0, min_time: 1, max_time: 1, chunk: Some(xor_chunk_samples(&[(0, 1, 2.0)], false)) }], tombstones: Vec::new() },
        CompactionSeries { labels: Labels::from_pairs([("b", "b")]), chunks: vec![ChunkMeta { ref_id: 0, min_time: 1, max_time: 1, chunk: Some(xor_chunk_samples(&[(0, 1, 3.0)], false)) }], tombstones: Vec::new() },
    ];
    let meta = write_block_from_series(dir.path(), 0, 10, series, BlockMetaCompaction::default(), false, 0).unwrap().unwrap();
    let blk = Arc::new(Block::open(dir.path().join(meta.ulid.to_string())).unwrap());
    let q = TsdbQuerier::new(None, vec![blk], 0, 10);
    let res = q.select(true, &[LabelMatcher::new(MatchType::Equal, "a", "a").unwrap()]).unwrap();
    assert_eq!(res.len(), 2);
    assert_eq!(res[0].labels, Labels::from_pairs([("a", "a")]));
    assert_eq!(res[1].labels, Labels::from_pairs([("a", "a"), ("b", "b")]));
}

// Upstream: github.com/prometheus/prometheus/tsdb/querier_test.go::TestQueryWithDeletedHistograms
#[test]
fn test_query_with_deleted_histograms() {
    for (idx, is_float) in [false, true].into_iter().enumerate() {
        let dir = tempfile::tempdir().unwrap();
        let db = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
        {
            let mut app = db.appender();
            for i in 0..100i64 {
                if is_float {
                    app.append_float_histogram(None, &[("__name__", "test"), ("type", &format!("t{idx}"))], 0, i, gen_fhist(i)).unwrap();
                } else {
                    app.append_histogram(None, &[("__name__", "test"), ("type", &format!("t{idx}"))], 0, i, gen_hist(i)).unwrap();
                }
            }
            app.commit().unwrap();
        }
        let m = [LabelMatcher::new(MatchType::Equal, "__name__", "test").unwrap()];
        db.delete(80, 100, &m).unwrap();
        let res = db.chunk_querier(0, 100).select(false, &m).unwrap();
        assert_eq!(res.len(), 1);
        let sample_count: usize = res[0].chunks.iter().map(|c| decode_chunk_samples(c.chunk.as_ref().unwrap()).unwrap().len()).sum();
        assert_eq!(sample_count, 80);
        db.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/querier_test.go::TestPrependPostings
#[test]
fn test_prepend_postings() {
    let mut empty = PrependPostings::new(Vec::new(), Box::new(EmptyPostingsIter));
    assert!(!empty.next());

    let mut p = PrependPostings::new(vec![10, 20, 30], Box::new(ListPostings::new(vec![200, 300, 500])));
    for exp in [10, 20, 30, 200, 300, 500] {
        assert!(p.next());
        assert_eq!(p.at(), exp);
        assert_eq!(p.at(), exp);
    }
    assert!(!p.next());

    let mut p2 = PrependPostings::new(vec![10, 20, 30], Box::new(ListPostings::new(vec![200, 300, 500])));
    assert!(p2.seek(5)); assert_eq!(p2.at(), 10);
    assert!(p2.seek(15)); assert_eq!(p2.at(), 20);
    assert!(p2.seek(20)); assert_eq!(p2.at(), 20);
    assert!(p2.seek(50)); assert_eq!(p2.at(), 200);
    assert!(!p2.seek(1000));
    assert!(!p2.next());
}

// Upstream: github.com/prometheus/prometheus/tsdb/querier_test.go::TestLabelsValuesWithMatchersOptimization
#[test]
fn test_labels_values_with_matchers_optimization() {
    let dir = tempfile::tempdir().unwrap();
    let blk = mk_block_testdata(dir.path());
    let q = TsdbQuerier::new(None, vec![blk], 0, 10);
    let vals = q.label_values("b", None, &[LabelMatcher::new(MatchType::Equal, "a", "a").unwrap()]).unwrap();
    assert_eq!(vals, vec!["b"]);
}

// Upstream: github.com/prometheus/prometheus/tsdb/querier_test.go::TestQueryWithOneChunkCompletelyDeleted
#[test]
fn test_query_with_one_chunk_completely_deleted() {
    let dir = tempfile::tempdir().unwrap();
    let db = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
    {
        let mut app = db.appender();
        for i in 0..20i64 {
            app.append_histogram(None, &[("__name__", "test")], 0, i, gen_hist(1)).unwrap();
        }
        for i in 30..40i64 {
            app.append_histogram(None, &[("__name__", "test")], 0, i, gen_hist(1)).unwrap();
        }
        for i in 60..100i64 {
            app.append_float_histogram(None, &[("__name__", "test")], 0, i, gen_fhist(1)).unwrap();
        }
        app.commit().unwrap();
    }
    let m = [LabelMatcher::new(MatchType::Equal, "__name__", "test").unwrap()];
    db.delete(0, 20, &m).unwrap();
    db.delete(30, 40, &m).unwrap();
    let res = db.chunk_querier(0, 100).select(false, &m).unwrap();
    assert_eq!(res.len(), 1);
    let mut sample_count = 0usize;
    for cm in &res[0].chunks {
        let ds = decode_chunk_samples(cm.chunk.as_ref().unwrap()).unwrap();
        for s in &ds {
            assert!(matches!(s.value, SampleValue::FloatHistogram(_)));
            sample_count += 1;
        }
    }
    assert_eq!(sample_count, 40);
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/querier_test.go::TestChunkQuerier_OverlappingInOrderAndOOOChunks
#[test]
fn test_chunk_querier_overlapping_in_order_and_ooo_chunks() {
    const OOO_CAP: usize = 32;
    const OOO_SAMPLES: i64 = 1000;
    for store_st in [false, true] {
        for kind in [0usize, 1, 2] {
            let dir = tempfile::tempdir().unwrap();
            let opts = TsdbOptions {
                head: HeadOptions {
                    ooo_cap_max: OOO_CAP,
                    ooo_time_window_ms: 86_400_000,
                    enable_st_storage: store_st,
                    float_chunk_encoding: if store_st { Encoding::Xor2 } else { Encoding::Xor },
                    ..HeadOptions::default()
                },
                ..TsdbOptions::default()
            };
            let db = Tsdb::open(dir.path(), opts).unwrap();
            let append_s = |app: &mut mimir_tsdb::head::HeadAppender<'_>, ts: i64| {
                match kind {
                    0 => { app.append_float(None, &[("foo", "bar")], ts, ts as f64).unwrap(); }
                    1 => { app.append_histogram(None, &[("foo", "bar")], 0, ts, gen_hist(ts)).unwrap(); }
                    _ => { app.append_float_histogram(None, &[("foo", "bar")], 0, ts, gen_fhist(ts)).unwrap(); }
                }
            };
            {
                let mut app = db.appender();
                append_s(&mut app, 10_000);
                append_s(&mut app, 10_000 + (OOO_SAMPLES + 1) * 10);
                app.commit().unwrap();
            }
            {
                let mut app = db.appender();
                for i in 1..=OOO_SAMPLES {
                    append_s(&mut app, 10_000 + i * 10);
                }
                app.commit().unwrap();
            }
            let m = [LabelMatcher::new(MatchType::Equal, "foo", "bar").unwrap()];
            let res = db.chunk_querier(i64::MIN, i64::MAX).select(false, &m).unwrap();
            assert_eq!(res.len(), 1);
            let mut last_ts = i64::MIN;
            let mut count = 0i64;
            for cm in &res[0].chunks {
                for s in decode_chunk_samples(cm.chunk.as_ref().unwrap()).unwrap() {
                    assert!(s.t > last_ts);
                    last_ts = s.t;
                    count += 1;
                }
            }
            assert_eq!(count, OOO_SAMPLES + 2);
            db.close().unwrap();
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/querier_test.go::TestReader_PostingsForLabelMatchingHonorsContextCancel
#[test]
fn test_reader_postings_for_label_matching_honors_context_cancel() {
    struct CtxAfter { cnt: AtomicUsize, fail_after: usize }
    impl ContextCheck for CtxAfter {
        fn err(&self) -> Option<String> {
            let n = self.cnt.fetch_add(1, Ordering::Relaxed) + 1;
            if n > self.fail_after { Some("context canceled".into()) } else { None }
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let mut series = Vec::new();
    for i in 0..300usize {
        let v = format!("v_{i:04}");
        series.push(CompactionSeries {
            labels: Labels::from_pairs([("__name__", v.as_str())]),
            chunks: vec![ChunkMeta { ref_id: 0, min_time: 1, max_time: 1, chunk: Some(xor_chunk_samples(&[(0, 1, 1.0)], false)) }],
            tombstones: Vec::new(),
        });
    }
    let meta = write_block_from_series(dir.path(), 0, 10, series, BlockMetaCompaction::default(), false, 0).unwrap().unwrap();
    let blk = Block::open(dir.path().join(meta.ulid.to_string())).unwrap();
    let ctx = CtxAfter { cnt: AtomicUsize::new(0), fail_after: 1 };
    let mut it = blk.index().postings_for_label_matching_iter_ctx(&ctx, "__name__", &mut |_| true);
    while it.next() {}
    assert!(it.err().is_some());
}

// Upstream: github.com/prometheus/prometheus/tsdb/querier_test.go::TestMergeQuerierConcurrentSelectMatchers
#[test]
fn test_merge_querier_concurrent_select_matchers() {
    let dir = tempfile::tempdir().unwrap();
    let blk = mk_block_testdata(dir.path());
    let q1 = TsdbQuerier::new(None, vec![Arc::clone(&blk)], 0, 10);
    let q2 = TsdbQuerier::new(None, vec![blk], 0, 10);
    let ms = vec![
        LabelMatcher::new(MatchType::RegexMatch, "baz", ".*").unwrap(),
        LabelMatcher::new(MatchType::Equal, "foo", "bar").unwrap(),
    ];
    let _ = std::thread::scope(|s| {
        let h1 = s.spawn(|| q1.select(false, &ms).unwrap());
        let h2 = s.spawn(|| q2.select(false, &ms).unwrap());
        (h1.join().unwrap(), h2.join().unwrap())
    });
    assert_eq!(ms[0].name, "baz");
    assert_eq!(ms[1].name, "foo");
}

// Upstream: github.com/prometheus/prometheus/tsdb/querier_test.go::TestBlockBaseQuerierSearchLabelNames
#[test]
fn test_block_base_querier_search_label_names() {
    let dir = tempfile::tempdir().unwrap();
    let series = vec![
        CompactionSeries { labels: Labels::from_pairs([("env", "prod"), ("job", "api")]), chunks: vec![ChunkMeta { ref_id: 0, min_time: 1, max_time: 1, chunk: Some(xor_chunk_samples(&[(0, 1, 1.0)], false)) }], tombstones: Vec::new() },
        CompactionSeries { labels: Labels::from_pairs([("env", "dev"), ("job", "api")]), chunks: vec![ChunkMeta { ref_id: 0, min_time: 1, max_time: 1, chunk: Some(xor_chunk_samples(&[(0, 1, 1.0)], false)) }], tombstones: Vec::new() },
        CompactionSeries { labels: Labels::from_pairs([("region", "eu")]), chunks: vec![ChunkMeta { ref_id: 0, min_time: 1, max_time: 1, chunk: Some(xor_chunk_samples(&[(0, 1, 1.0)], false)) }], tombstones: Vec::new() },
    ];
    let meta = write_block_from_series(dir.path(), 0, 10, series, BlockMetaCompaction::default(), false, 0).unwrap().unwrap();
    let blk = Arc::new(Block::open(dir.path().join(meta.ulid.to_string())).unwrap());
    let q = TsdbQuerier::new(None, vec![blk], 0, 10);
    let all: Vec<String> = q.search_label_names(&SearchHints::default(), &[]).unwrap().into_iter().map(|r| r.value).collect();
    assert_eq!(all, vec!["env", "job", "region"]);
    let filtered: Vec<String> = q.search_label_names(&SearchHints { terms: vec!["e".into()], ..SearchHints::default() }, &[]).unwrap().into_iter().map(|r| r.value).collect();
    assert_eq!(filtered, vec!["env", "region"]);
    let lim = q.search_label_names(&SearchHints { limit: 1, ..SearchHints::default() }, &[]).unwrap();
    assert_eq!(lim.len(), 1);
}

// Upstream: github.com/prometheus/prometheus/tsdb/querier_test.go::TestBlockBaseQuerierSearchLabelValues
#[test]
fn test_block_base_querier_search_label_values() {
    let dir = tempfile::tempdir().unwrap();
    let series: Vec<_> = ["prod", "dev", "staging"].into_iter().map(|e| CompactionSeries {
        labels: Labels::from_pairs([("env", e)]),
        chunks: vec![ChunkMeta { ref_id: 0, min_time: 1, max_time: 1, chunk: Some(xor_chunk_samples(&[(0, 1, 1.0)], false)) }],
        tombstones: Vec::new(),
    }).collect();
    let meta = write_block_from_series(dir.path(), 0, 10, series, BlockMetaCompaction::default(), false, 0).unwrap().unwrap();
    let blk = Arc::new(Block::open(dir.path().join(meta.ulid.to_string())).unwrap());
    let q = TsdbQuerier::new(None, vec![blk], 0, 10);
    let all: Vec<String> = q.search_label_values("env", &SearchHints::default(), &[]).unwrap().into_iter().map(|r| r.value).collect();
    assert_eq!(all, vec!["dev", "prod", "staging"]);
    let lim = q.search_label_values("env", &SearchHints { limit: 2, ..SearchHints::default() }, &[]).unwrap();
    assert_eq!(lim.len(), 2);
    assert!(q.search_label_values("unknown", &SearchHints::default(), &[]).unwrap().is_empty());
    let desc: Vec<String> = q.search_label_values("env", &SearchHints { order_by: SearchOrdering::ValueDesc, ..SearchHints::default() }, &[]).unwrap().into_iter().map(|r| r.value).collect();
    assert_eq!(desc, vec!["staging", "prod", "dev"]);
}

// Upstream: github.com/prometheus/prometheus/tsdb/querier_test.go::TestSearchLabelValuesLimitAppliedAfterOrdering
#[test]
fn test_search_label_values_limit_applied_after_ordering() {
    let dir = tempfile::tempdir().unwrap();
    let db = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
    {
        let mut app = db.appender();
        for (m, p) in [("z_metric", "z_pod"), ("m_metric", "m_pod"), ("a_metric", "a_pod")] {
            app.append_float(None, &[("__name__", m), ("job", "api"), ("pod", p)], 2100, 1.0).unwrap();
        }
        app.commit().unwrap();
    }
    let q = db.querier(1500, 2500);
    let job_api = [LabelMatcher::new(MatchType::Equal, "job", "api").unwrap()];
    for lim in 1..=3 {
        let got: Vec<String> = q.search_label_values("__name__", &SearchHints { order_by: SearchOrdering::ValueAsc, limit: lim, ..SearchHints::default() }, &[]).unwrap().into_iter().map(|r| r.value).collect();
        assert_eq!(got, vec!["a_metric", "m_metric", "z_metric"][..lim]);
        let pods: Vec<String> = q.search_label_values("pod", &SearchHints { order_by: SearchOrdering::ValueAsc, limit: lim, ..SearchHints::default() }, &job_api).unwrap().into_iter().map(|r| r.value).collect();
        assert_eq!(pods, vec!["a_pod", "m_pod", "z_pod"][..lim]);
    }
    let desc: Vec<String> = q.search_label_values("__name__", &SearchHints { order_by: SearchOrdering::ValueDesc, limit: 2, ..SearchHints::default() }, &[]).unwrap().into_iter().map(|r| r.value).collect();
    assert_eq!(desc, vec!["z_metric", "m_metric"]);
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/querier_test.go::TestSearchLabelValuesLimitAcrossMergedQueriers
#[test]
fn test_search_label_values_limit_across_merged_queriers() {
    let blk_dir = tempfile::tempdir().unwrap();
    let blk_series = ["n_metric", "p_metric", "r_metric"].into_iter().map(|m| CompactionSeries {
        labels: Labels::from_pairs([("__name__", m), ("job", "api")]),
        chunks: vec![ChunkMeta { ref_id: 0, min_time: 2100, max_time: 2100, chunk: Some(xor_chunk_samples(&[(0, 2100, 1.0)], false)) }],
        tombstones: Vec::new(),
    }).collect();
    let meta = write_block_from_series(blk_dir.path(), 2000, 3000, blk_series, BlockMetaCompaction::default(), false, 0).unwrap().unwrap();
    let blk = Arc::new(Block::open(blk_dir.path().join(meta.ulid.to_string())).unwrap());

    let head_dir = tempfile::tempdir().unwrap();
    let head = Head::open(head_dir.path(), HeadOptions::default(), i64::MIN).unwrap();
    {
        let mut app = head.appender();
        for m in ["z_metric", "m_metric", "a_metric"] {
            app.append_float(None, &[("__name__", m), ("job", "api")], 2100, 1.0).unwrap();
        }
        app.commit().unwrap();
    }
    let q = TsdbQuerier::new(Some(&head), vec![blk], 1500, 2500);
    let got: Vec<String> = q.search_label_values("__name__", &SearchHints { order_by: SearchOrdering::ValueAsc, limit: 2, ..SearchHints::default() }, &[]).unwrap().into_iter().map(|r| r.value).collect();
    assert_eq!(got, vec!["a_metric", "m_metric"]);
    head.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/exemplar_test.go::TestValidateExemplar
#[test]
fn test_validate_exemplar() {
    let es = CircularExemplarStorage::new(2, 0);
    let l = Labels::from_pairs([("service", "asdf")]);
    let e = Exemplar::new(Labels::from_pairs([("trace_id", "qwerty")]), 0.1, 1);
    es.validate_exemplar(&l, &e).unwrap();
    es.add_exemplar(&l, e).unwrap();

    let e2 = Exemplar {
        labels: Labels::from_pairs([("trace_id", "zxcvb")]),
        value: 0.1,
        timestamp_ms: 2,
        has_ts: false,
    };
    es.validate_exemplar(&l, &e2).unwrap();
    es.add_exemplar(&l, e2.clone()).unwrap();
    assert_eq!(es.validate_exemplar(&l, &e2), Err(ExemplarError::DuplicateExemplar));

    let mut e3 = e2.clone();
    e3.timestamp_ms = 3;
    assert_eq!(es.validate_exemplar(&l, &e3), Err(ExemplarError::DuplicateExemplar));
    e3.timestamp_ms = 1;
    e3.value = 0.3;
    assert_eq!(es.validate_exemplar(&l, &e3), Err(ExemplarError::OutOfOrderExemplar));

    let long_v = "b".repeat(EXEMPLAR_MAX_LABEL_SET_LENGTH);
    let e4 = Exemplar::new(Labels::from_pairs([("a", long_v.as_str())]), 0.1, 2);
    assert_eq!(es.validate_exemplar(&l, &e4), Err(ExemplarError::LabelLengthExceeded));

    let mut raw = Vec::new();
    append_stringlabel_pair(&mut raw, b"trace_id", b"aaaa");
    append_stringlabel_pair(&mut raw, b"trace_id", b"bbbb");
    let dup_lbls = Labels::from_raw_stringlabels_bytes(raw.into());
    let e5 = Exemplar::new(dup_lbls, 0.1, 4);
    assert!(matches!(es.validate_exemplar(&l, &e5), Err(ExemplarError::DuplicateLabelName(_))));
    assert!(matches!(es.add_exemplar(&l, e5), Err(ExemplarError::DuplicateLabelName(_))));
}

// Upstream: github.com/prometheus/prometheus/tsdb/exemplar_test.go::TestCircularExemplarStorage_AddExemplar
#[test]
fn test_circular_exemplar_storage_add_exemplar() {
    let s1 = Labels::from_pairs([("trace_id", "foo")]);
    let s2 = Labels::from_pairs([("trace_id", "bar")]);
    let m1 = [LabelMatcher::new(MatchType::Equal, "trace_id", "foo").unwrap()];

    // Insert after newest.
    {
        let es = CircularExemplarStorage::new(3, 0);
        es.add_exemplar(&s1, Exemplar::new(s1.clone(), 0.1, 1)).unwrap();
        es.add_exemplar(&s1, Exemplar::new(s1.clone(), 0.2, 2)).unwrap();
        let got = es.select(0, 10, &[&m1]);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].exemplars.len(), 2);
    }
    // Out of order rejected when ooo window is 0.
    {
        let es = CircularExemplarStorage::new(3, 0);
        es.add_exemplar(&s1, Exemplar::new(s1.clone(), 0.1, 2)).unwrap();
        assert_eq!(es.add_exemplar(&s1, Exemplar::new(s1.clone(), 0.2, 1)), Err(ExemplarError::OutOfOrderExemplar));
    }
    // Out of order accepted within ooo window.
    {
        let es = CircularExemplarStorage::new(3, 100);
        es.add_exemplar(&s1, Exemplar::new(s1.clone(), 0.1, 50)).unwrap();
        es.add_exemplar(&s1, Exemplar::new(s1.clone(), 0.2, 20)).unwrap();
        es.add_exemplar(&s2, Exemplar::new(s2.clone(), 0.3, 60)).unwrap();
        let got = es.select(0, 100, &[&m1]);
        assert_eq!(got[0].exemplars.iter().map(|e| e.timestamp_ms).collect::<Vec<_>>(), vec![20, 50]);
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/exemplar_test.go::TestCircularExemplarStorage_Resize
#[test]
fn test_circular_exemplar_storage_resize() {
    let es = CircularExemplarStorage::new(5, 0);
    let l = Labels::from_pairs([("service", "asdf")]);
    for i in 0..5i64 {
        let tid = format!("{i}");
        es.add_exemplar(&l, Exemplar::new(Labels::from_pairs([("trace_id", tid.as_str())]), 0.1, 100 + i)).unwrap();
    }
    assert_eq!(es.resize(3), 3);
    assert_eq!(es.capacity(), 3);
    let m = [LabelMatcher::new(MatchType::Equal, "service", "asdf").unwrap()];
    let got = es.select(0, 200, &[&m]);
    assert_eq!(got[0].exemplars.iter().map(|e| e.timestamp_ms).collect::<Vec<_>>(), vec![102, 103, 104]);
}

// Upstream: github.com/prometheus/prometheus/tsdb/exemplar_test.go::TestStorageOverflow
#[test]
fn test_storage_overflow() {
    let es = CircularExemplarStorage::new(5, 0);
    let l = Labels::from_pairs([("service", "asdf")]);
    let mut added = Vec::new();
    for i in 0..6i64 {
        let e = Exemplar::new(Labels::from_pairs([("trace_id", "a")]), (i + 1) as f64 / 10.0, 101 + i);
        es.add_exemplar(&l, e.clone()).unwrap();
        added.push(e);
    }
    let m = [LabelMatcher::new(MatchType::Equal, "service", "asdf").unwrap()];
    let ret = es.select(100, 110, &[&m]);
    assert_eq!(ret.len(), 1);
    assert_eq!(ret[0].exemplars, added[1..]);
}

// Upstream: github.com/prometheus/prometheus/tsdb/exemplar_test.go::TestSelectExemplar
#[test]
fn test_select_exemplar() {
    let es = CircularExemplarStorage::new(5, 0);
    let l = Labels::from_pairs([("service", "asdf")]);
    let e = Exemplar::new(Labels::from_pairs([("trace_id", "querty")]), 0.1, 12);
    es.add_exemplar(&l, e.clone()).unwrap();
    let m = [LabelMatcher::new(MatchType::Equal, "service", "asdf").unwrap()];
    let ret = es.select(0, 100, &[&m]);
    assert_eq!(ret.len(), 1);
    assert_eq!(ret[0].exemplars, vec![e]);
}

// Upstream: github.com/prometheus/prometheus/tsdb/exemplar_test.go::TestSelectExemplar_MultiSeries
#[test]
fn test_select_exemplar_multi_series() {
    let es = CircularExemplarStorage::new(5, 0);
    let l1 = Labels::from_pairs([("__name__", "test_metric"), ("service", "asdf")]);
    let l2 = Labels::from_pairs([("__name__", "test_metric2"), ("service", "qwer")]);
    for i in 0..5i64 {
        es.add_exemplar(&l1, Exemplar::new(Labels::from_pairs([("trace_id", "a")]), (i + 1) as f64 / 10.0, 101 + i)).unwrap();
        es.add_exemplar(&l2, Exemplar::new(Labels::from_pairs([("trace_id", "b")]), (i + 1) as f64 / 10.0, 101 + i)).unwrap();
    }
    let m2 = [LabelMatcher::new(MatchType::Equal, "__name__", "test_metric2").unwrap()];
    let m1 = [LabelMatcher::new(MatchType::Equal, "__name__", "test_metric").unwrap()];
    assert_eq!(es.select(100, 200, &[&m2])[0].exemplars.len(), 3);
    assert_eq!(es.select(100, 200, &[&m1])[0].exemplars.len(), 2);
}

// Upstream: github.com/prometheus/prometheus/tsdb/exemplar_test.go::TestSelectExemplar_TimeRange
#[test]
fn test_select_exemplar_time_range() {
    let es = CircularExemplarStorage::new(5, 0);
    let l = Labels::from_pairs([("service", "asdf")]);
    for i in 0..5i64 {
        let tid = i.to_string();
        es.add_exemplar(&l, Exemplar::new(Labels::from_pairs([("trace_id", tid.as_str())]), 0.1, 101 + i)).unwrap();
    }
    let m = [LabelMatcher::new(MatchType::Equal, "service", "asdf").unwrap()];
    let ret = es.select(102, 104, &[&m]);
    assert_eq!(ret.len(), 1);
    assert_eq!(ret[0].exemplars.len(), 3);
}

// Upstream: github.com/prometheus/prometheus/tsdb/exemplar_test.go::TestSelectExemplar_DuplicateSeries
#[test]
fn test_select_exemplar_duplicate_series() {
    let es = CircularExemplarStorage::new(4, 0);
    let l = Labels::from_pairs([("service", "asdf"), ("cluster", "us-central1")]);
    let e = Exemplar::new(Labels::from_pairs([("trace_id", "qwerty")]), 0.1, 12);
    es.add_exemplar(&l, e).unwrap();
    let m1 = [LabelMatcher::new(MatchType::Equal, "service", "asdf").unwrap()];
    let m2 = [LabelMatcher::new(MatchType::Equal, "cluster", "us-central1").unwrap()];
    let ret = es.select(0, 100, &[&m1, &m2]);
    assert_eq!(ret.len(), 1);
}

// Upstream: github.com/prometheus/prometheus/tsdb/exemplar_test.go::TestIndexOverwrite
#[test]
fn test_index_overwrite() {
    let es = CircularExemplarStorage::new(2, 0);
    let l1 = Labels::from_pairs([("service", "asdf")]);
    let l2 = Labels::from_pairs([("service", "qwer")]);
    es.add_exemplar(&l1, Exemplar::new(Labels::empty(), 1.0, 1)).unwrap();
    es.add_exemplar(&l2, Exemplar::new(Labels::empty(), 2.0, 2)).unwrap();
    es.add_exemplar(&l2, Exemplar::new(Labels::empty(), 3.0, 3)).unwrap();
    let m1 = [LabelMatcher::new(MatchType::Equal, "service", "asdf").unwrap()];
    let m2 = [LabelMatcher::new(MatchType::Equal, "service", "qwer").unwrap()];
    assert!(es.select(0, 10, &[&m1]).is_empty());
    assert_eq!(es.select(0, 10, &[&m2])[0].exemplars.len(), 2);
    es.add_exemplar(&l1, Exemplar::new(Labels::empty(), 4.0, 4)).unwrap();
    assert_eq!(es.select(0, 10, &[&m1])[0].exemplars.len(), 1);
    assert_eq!(es.select(0, 10, &[&m2])[0].exemplars.len(), 1);
}

// Upstream: github.com/prometheus/prometheus/tsdb/exemplar_test.go::TestResize
#[test]
fn test_resize() {
    let cases = [
        (100usize, 200usize, 100usize, vec![99, 98, 1, 0]),
        (100, 50, 50, vec![99, 98, 50]),
        (100, 0, 0, vec![]),
        (0, 10, 0, vec![]),
    ];
    for (start_sz, new_sz, exp_migrated, exp_present) in cases {
        let es = CircularExemplarStorage::new(start_sz, 0);
        for i in 0..start_sz {
            let s = i.to_string();
            let l = Labels::from_pairs([("s", s.as_str())]);
            es.add_exemplar(&l, Exemplar::new(Labels::empty(), i as f64, i as i64)).unwrap();
        }
        assert_eq!(es.resize(new_sz), exp_migrated);
        let mut seen = HashSet::new();
        es.iterate_exemplars(|lbls, _| {
            seen.insert(lbls.get("s").parse::<usize>().unwrap());
        });
        for i in exp_present {
            assert!(seen.contains(&i));
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/exemplar_test.go::TestCircularExemplarStorage_Concurrent_AddExemplar_Resize
#[test]
fn test_circular_exemplar_storage_concurrent_add_exemplar_resize() {
    let es = Arc::new(CircularExemplarStorage::new(0, 0));
    let l = Labels::from_pairs([("service", "asdf")]);
    let e = Exemplar::new(Labels::from_pairs([("trace_id", "qwerty")]), 0.1, 1);
    let es_w = Arc::clone(&es);
    let l_w = l.clone();
    let e_w = e.clone();
    let h = std::thread::spawn(move || {
        for _ in 0..100 {
            let _ = es_w.add_exemplar(&l_w, e_w.clone());
        }
    });
    for i in 0..100usize {
        es.resize(i + 1);
    }
    h.join().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/postings_for_matchers_cache_test.go::TestPostingsForMatchersCacheFactory
#[test]
fn test_postings_for_matchers_cache_factory() {
    for shared in [true, false] {
        let reg = prometheus::Registry::new();
        let cfg = PostingsForMatchersCacheConfig {
            ttl: Duration::from_secs(1),
            max_items: 5,
            max_bytes: 500,
            metrics: Some(PostingsForMatchersCacheMetrics::new("test_", Some(&reg))),
            ..PostingsForMatchersCacheConfig::default()
        };
        let c = PostingsForMatchersCache::new(cfg);
        c.set_eligibility(false, shared);
        assert_eq!(c.shared(), shared);
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/postings_for_matchers_cache_test.go::TestPostingsForMatchersCache
#[test]
fn test_postings_for_matchers_cache() {
    let reg = prometheus::Registry::new();
    let cache = PostingsForMatchersCache::new(PostingsForMatchersCacheConfig {
        ttl: Duration::from_secs(10),
        max_items: 16,
        max_bytes: 4096,
        enable_invalidation: true,
        metrics: Some(PostingsForMatchersCacheMetrics::new("t_", Some(&reg))),
        ..PostingsForMatchersCacheConfig::default()
    });
    cache.set_eligibility(true, true);
    let src = next_postings_cache_source_id();
    let m = [LabelMatcher::new(MatchType::Equal, "__name__", "foo").unwrap()];
    let calls = AtomicUsize::new(0);
    let p1 = cache.get_or_compute_for_source(src, &m, true, || -> Result<Vec<u64>, PostingsError> {
        calls.fetch_add(1, Ordering::Relaxed);
        Ok(vec![1, 2, 3])
    }).unwrap();
    let p2 = cache.get_or_compute_for_source(src, &m, true, || -> Result<Vec<u64>, PostingsError> {
        calls.fetch_add(1, Ordering::Relaxed);
        Ok(vec![9])
    }).unwrap();
    assert_eq!(&*p1, &[1, 2, 3]);
    assert_eq!(&*p2, &[1, 2, 3]);
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    cache.invalidate_metric_for_source(src, "foo");
    let p3 = cache.get_or_compute_for_source(src, &m, true, || -> Result<Vec<u64>, PostingsError> {
        calls.fetch_add(1, Ordering::Relaxed);
        Ok(vec![4, 5])
    }).unwrap();
    assert_eq!(&*p3, &[4, 5]);
    assert_eq!(calls.load(Ordering::Relaxed), 2);
}

// Upstream: github.com/prometheus/prometheus/tsdb/postings_for_matchers_cache_test.go::TestPostingsForMatchersCache_ShouldNotReturnStaleEntriesWhileAnotherGoroutineIsEvictingTheCache
#[test]
fn test_postings_for_matchers_cache_no_stale_during_eviction() {
    let cache = Arc::new(PostingsForMatchersCache::new(PostingsForMatchersCacheConfig {
        ttl: Duration::from_millis(20),
        max_items: 1000,
        max_bytes: 1024 * 1024,
        ..PostingsForMatchersCacheConfig::default()
    }));
    cache.set_eligibility(true, false);
    let src = next_postings_cache_source_id();
    let m = [LabelMatcher::new(MatchType::Equal, "foo", "bar").unwrap()];
    let call_id = Arc::new(AtomicUsize::new(0));
    let v1 = cache.get_or_compute_for_source(src, &m, true, || -> Result<Vec<u64>, PostingsError> {
        Ok(vec![call_id.fetch_add(1, Ordering::Relaxed) as u64 + 1])
    }).unwrap();
    assert_eq!(&*v1, &[1]);
    std::thread::sleep(Duration::from_millis(30));
    let (r1, r2) = std::thread::scope(|s| {
        let h1 = s.spawn(|| cache.get_or_compute_for_source(src, &m, true, || -> Result<Vec<u64>, PostingsError> {
            Ok(vec![call_id.fetch_add(1, Ordering::Relaxed) as u64 + 1])
        }).unwrap());
        let h2 = s.spawn(|| cache.get_or_compute_for_source(src, &m, true, || -> Result<Vec<u64>, PostingsError> {
            Ok(vec![call_id.fetch_add(1, Ordering::Relaxed) as u64 + 1])
        }).unwrap());
        (h1.join().unwrap(), h2.join().unwrap())
    });
    assert!(r1[0] >= 2 && r2[0] >= 2);
}

// Upstream: github.com/prometheus/prometheus/tsdb/postings_for_matchers_cache_test.go::TestPostingsForMatchersCache_RaceConditionBetweenExecutionContextCancellationAndNewRequest
#[test]
fn test_postings_for_matchers_cache_race_between_cancel_and_new_request() {
    let cache = Arc::new(PostingsForMatchersCache::new(PostingsForMatchersCacheConfig {
        ttl: Duration::from_secs(60),
        max_items: 8,
        max_bytes: 1024,
        ..PostingsForMatchersCacheConfig::default()
    }));
    cache.set_eligibility(true, false);
    let src = next_postings_cache_source_id();
    let m = [LabelMatcher::new(MatchType::Equal, "foo", "bar").unwrap()];
    let calls = Arc::new(AtomicUsize::new(0));
    let r_err = cache.get_or_compute_for_source(src, &m, true, || -> Result<Vec<u64>, PostingsError> {
        calls.fetch_add(1, Ordering::Relaxed);
        Err(PostingsError::Index("canceled".into()))
    });
    assert!(r_err.is_err());
    let r_ok = cache.get_or_compute_for_source(src, &m, true, || -> Result<Vec<u64>, PostingsError> {
        calls.fetch_add(1, Ordering::Relaxed);
        Ok(vec![1, 2, 3])
    }).unwrap();
    assert_eq!(&*r_ok, &[1, 2, 3]);
    assert_eq!(calls.load(Ordering::Relaxed), 2);
}

// Upstream: github.com/prometheus/prometheus/tsdb/postings_for_matchers_cache_test.go::Test_sharedCacheKey
#[test]
fn test_shared_cache_key() {
    let ms = [
        LabelMatcher::new(MatchType::Equal, "bar", "baz").unwrap(),
        LabelMatcher::new(MatchType::Equal, "__name__", "foo").unwrap(),
    ];
    let key = shared_cache_key_string(0, "user", "0000000000XXXXXXXXXXXXHEAD", &ms);
    assert_eq!(key, "0\x00user\x000000000000XXXXXXXXXXXXHEAD\x00__name__=foo\x00bar=baz");
}

// Upstream: github.com/prometheus/prometheus/tsdb/postings_for_matchers_cache_test.go::TestMetricVersions
#[test]
fn test_metric_versions() {
    let mv = PagedMetricVersions::new(100);
    assert_eq!(mv.load(1, ""), 0);
    mv.fetch_add(1, "");
    assert_eq!(mv.load(1, ""), 1);
    assert_eq!(mv.load(1, "foo"), 0);
    mv.fetch_add(1, "foo");
    mv.fetch_add(1, "foo");
    assert_eq!(mv.load(1, "foo"), 2);
    assert_eq!(mv.load(2, "foo"), 0);
    assert_eq!(mv.load(1, "bar"), 0);
}

fn run_lookup_planner_cases(chunk_mode: bool) {
    struct MockPlanner {
        return_error: bool,
        index_matchers: Vec<LabelMatcher>,
        scan_matchers: Vec<LabelMatcher>,
    }
    impl IndexLookupPlanner for MockPlanner {
        fn plan_index_lookup(&self, _plan: &dyn LookupPlan) -> Result<ConcreteLookupPlan, IndexError> {
            if self.return_error {
                return Err(IndexError::Corrupt("test planner error".into()));
            }
            Ok(ConcreteLookupPlan {
                index_matchers: self.index_matchers.clone(),
                scan_matchers: self.scan_matchers.clone(),
            })
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let series = vec![
        CompactionSeries { labels: Labels::from_pairs([("__name__", "metric1"), ("job", "test"), ("instance", "a")]), chunks: vec![ChunkMeta { ref_id: 0, min_time: 1, max_time: 1, chunk: Some(xor_chunk_samples(&[(0, 1, 1.0)], false)) }], tombstones: Vec::new() },
        CompactionSeries { labels: Labels::from_pairs([("__name__", "metric1"), ("job", "test"), ("instance", "b")]), chunks: vec![ChunkMeta { ref_id: 0, min_time: 1, max_time: 1, chunk: Some(xor_chunk_samples(&[(0, 1, 1.0)], false)) }], tombstones: Vec::new() },
        CompactionSeries { labels: Labels::from_pairs([("__name__", "metric2"), ("job", "prod"), ("instance", "a")]), chunks: vec![ChunkMeta { ref_id: 0, min_time: 1, max_time: 1, chunk: Some(xor_chunk_samples(&[(0, 1, 1.0)], false)) }], tombstones: Vec::new() },
        CompactionSeries { labels: Labels::from_pairs([("__name__", "metric2"), ("job", "prod"), ("instance", "b")]), chunks: vec![ChunkMeta { ref_id: 0, min_time: 1, max_time: 1, chunk: Some(xor_chunk_samples(&[(0, 1, 1.0)], false)) }], tombstones: Vec::new() },
        CompactionSeries { labels: Labels::from_pairs([("__name__", "metric3"), ("instance", "a")]), chunks: vec![ChunkMeta { ref_id: 0, min_time: 1, max_time: 1, chunk: Some(xor_chunk_samples(&[(0, 1, 1.0)], false)) }], tombstones: Vec::new() },
    ];
    let meta = write_block_from_series(dir.path(), 0, 100, series, BlockMetaCompaction::default(), false, 0).unwrap().unwrap();
    let blk = Arc::new(Block::open(dir.path().join(meta.ulid.to_string())).unwrap());
    let run = |ms: &[LabelMatcher], p: &dyn IndexLookupPlanner| -> Vec<Labels> {
        if chunk_mode {
            TsdbChunkQuerier::new(None, vec![Arc::clone(&blk)], 0, 100).select_with_planner(false, ms, p).unwrap().into_iter().map(|s| s.labels).collect()
        } else {
            TsdbQuerier::new(None, vec![Arc::clone(&blk)], 0, 100).select_with_planner(false, ms, p).unwrap().into_iter().map(|s| s.labels).collect()
        }
    };
    // Planner error falls back to all matchers.
    let p_err = MockPlanner { return_error: true, index_matchers: vec![], scan_matchers: vec![] };
    assert_eq!(run(&[LabelMatcher::new(MatchType::Equal, "__name__", "metric1").unwrap()], &p_err).len(), 2);

    // Planner splits index + scan matchers.
    let p_split = MockPlanner {
        return_error: false,
        index_matchers: vec![LabelMatcher::new(MatchType::Equal, "job", "test").unwrap()],
        scan_matchers: vec![LabelMatcher::new(MatchType::Equal, "instance", "a").unwrap()],
    };
    let got = run(&[LabelMatcher::new(MatchType::Equal, "job", "test").unwrap(), LabelMatcher::new(MatchType::Equal, "instance", "a").unwrap()], &p_split);
    assert_eq!(got.len(), 1);
    assert_eq!((got[0].get("__name__"), got[0].get("job"), got[0].get("instance")), ("metric1", "test", "a"));

    // Empty string scan matcher filters correctly.
    let p_empty = MockPlanner {
        return_error: false,
        index_matchers: vec![LabelMatcher::new(MatchType::Equal, "__name__", "metric3").unwrap()],
        scan_matchers: vec![LabelMatcher::new(MatchType::Equal, "job", "").unwrap()],
    };
    let got_empty = run(&[LabelMatcher::new(MatchType::Equal, "__name__", "metric3").unwrap(), LabelMatcher::new(MatchType::Equal, "job", "").unwrap()], &p_empty);
    assert_eq!(got_empty.len(), 1);
    assert_eq!((got_empty[0].get("__name__"), got_empty[0].get("job"), got_empty[0].get("instance")), ("metric3", "", "a"));
}

// Upstream: github.com/prometheus/prometheus/tsdb/querier_lookup_planner_test.go::TestSelectSeriesSetWithIndexLookupPlanner
#[test]
fn test_select_series_set_with_index_lookup_planner() {
    run_lookup_planner_cases(false);
}

// Upstream: github.com/prometheus/prometheus/tsdb/querier_lookup_planner_test.go::TestSelectChunkSeriesSetWithIndexLookupPlanner
#[test]
fn test_select_chunk_series_set_with_index_lookup_planner() {
    run_lookup_planner_cases(true);
}

// Upstream: github.com/prometheus/prometheus/tsdb/query_cost_test.go::TestDB_CostSamplingQueryable
#[test]
fn test_db_cost_sampling_queryable() {
    let cases = [
        (vec![], false, false, false),
        (vec![], true, false, false),
        (vec![(0, 100)], false, false, false),
        (vec![(0, 50), (50, 100)], false, false, true),
        (vec![(0, 80), (20, 100)], false, false, true),
        (vec![(0, 50)], true, false, true),
        (vec![], true, true, true),
    ];
    for (blocks, head, ooo, unsupported) in cases {
        let dir = tempfile::tempdir().unwrap();
        for (a, b) in blocks {
            let s = vec![CompactionSeries {
                labels: Labels::from_pairs([("__name__", "0")]),
                chunks: vec![ChunkMeta { ref_id: 0, min_time: a, max_time: b - 1, chunk: Some(xor_chunk_samples(&[(0, a, 1.0)], false)) }],
                tombstones: Vec::new(),
            }];
            write_block_from_series(dir.path(), a, b, s, BlockMetaCompaction::default(), false, 0).unwrap();
        }
        let opts = TsdbOptions { head: HeadOptions { ooo_time_window_ms: 1000, ..HeadOptions::default() }, ..TsdbOptions::default() };
        let db = Tsdb::open(dir.path(), opts).unwrap();
        if head {
            let mut app = db.appender();
            app.append_float(None, &[("__name__", "0")], 80, 1.0).unwrap();
            if ooo {
                app.append_float(None, &[("__name__", "0")], 40, 2.0).unwrap();
            }
            app.commit().unwrap();
        }
        let (source, supported) = db.cost_sampling_queryable();
        assert!(supported);
        let rq = source.querier(25, 90);
        let rcq = source.chunk_querier(25, 90);
        if unsupported {
            assert!(matches!(rq, Err(QuerierError::CostSamplingUnsupported)));
            assert!(matches!(rcq, Err(QuerierError::CostSamplingUnsupported)));
        } else {
            assert!(rq.is_ok());
            assert!(rcq.is_ok());
        }
        db.close().unwrap();
    }

    // Layout changes: pinned querier remains usable while new openings reject OOO.
    let dir = tempfile::tempdir().unwrap();
    let opts = TsdbOptions { head: HeadOptions { ooo_time_window_ms: 1000, ..HeadOptions::default() }, ..TsdbOptions::default() };
    let db = Tsdb::open(dir.path(), opts).unwrap();
    {
        let mut app = db.appender();
        app.append_float(None, &[("__name__", "0")], 20, 1.0).unwrap();
        app.append_float(None, &[("__name__", "0")], 80, 2.0).unwrap();
        app.commit().unwrap();
    }
    let (source, _) = db.cost_sampling_queryable();
    let pinned = source.chunk_querier(25, 75).unwrap();
    {
        let mut app = db.appender();
        app.append_float(None, &[("__name__", "0")], 40, 3.0).unwrap();
        app.commit().unwrap();
    }
    let _ = pinned.select(false, &[LabelMatcher::new(MatchType::Equal, "__name__", "0").unwrap()]).unwrap();
    assert!(matches!(source.chunk_querier(25, 75), Err(QuerierError::CostSamplingUnsupported)));
    assert!(matches!(source.querier(25, 75), Err(QuerierError::CostSamplingUnsupported)));
    db.close().unwrap();
}

