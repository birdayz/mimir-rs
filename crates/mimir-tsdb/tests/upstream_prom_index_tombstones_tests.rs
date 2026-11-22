//! Ported upstream Prometheus `tsdb/index` (48) and `tsdb/tombstones` (6) tests.
//!
//! Source: github.com/grafana/mimir-prometheus (tsdb/index/*_test.go, tsdb/tombstones/*_test.go).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

use bytes::Bytes;
use mimir_tsdb::chunks_block::ChunkMeta;
use mimir_tsdb::encoding::Encbuf;
use mimir_tsdb::index::{
    checked_uvarint_decbuf_at, decode_postings_raw, IndexError, IndexOnlyLookupPlan, IndexReader,
    IndexWriter, ScanEmptyMatchersLookupPlanner, SeriesHashCache, Symbols, INDEX_FILENAME,
    INDEX_FORMAT_V2,
};
use mimir_tsdb::labels::{Labels, SymbolLabels};
use mimir_tsdb::matcher::{LabelMatcher, MatchType};
use mimir_tsdb::postings::{
    expand_postings, find_intersecting_postings, intersect_iters, merge_iters, without_iters,
    BigEndianPostings, ContextCheck, EmptyPostingsIter, ErrPostingsIter, LabelHints, ListPostings,
    MemPostings, PostingsCloner, PostingsError, PostingsIter, PostingsSource,
    PostingsStatMaxHeap, PostingsWithIndex, PostingsWithIndexHeap, RemovedPostings, Stat,
    ALL_POSTINGS_KEY, CHECK_CONTEXT_EVERY_N_ITERATIONS, LABEL_VALUE_BYTES_MIN_LENGTH,
};
use mimir_tsdb::symbols::SymbolTable;
use mimir_tsdb::tombstones::{
    add_interval, read_tombstones_file, write_tombstones_file, Interval, Intervals, MemTombstones,
};

struct Lcg(u64);

impl Lcg {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0
    }

    fn next_u32(&mut self, bound: u32) -> u32 {
        ((self.next_u64() >> 32) as u32) % bound
    }

    fn next_i64(&mut self, bound: i64) -> i64 {
        ((self.next_u64() >> 1) as i64) % bound
    }
}

struct MockContextErrAfter {
    count: AtomicU64,
    fail_after: u64,
}

impl MockContextErrAfter {
    fn new(fail_after: u64) -> Self {
        Self {
            count: AtomicU64::new(0),
            fail_after,
        }
    }

    fn count(&self) -> u64 {
        self.count.load(Ordering::SeqCst)
    }
}

impl ContextCheck for MockContextErrAfter {
    fn err(&self) -> Option<String> {
        let c = self.count.fetch_add(1, Ordering::SeqCst) + 1;
        if c >= self.fail_after {
            Some("context canceled".to_string())
        } else {
            None
        }
    }
}

struct PostingsFailingAfterNthCall {
    ttl: isize,
    inner: Box<dyn PostingsIter>,
    err: PostingsError,
}

impl PostingsFailingAfterNthCall {
    fn new(ttl: isize, inner: Box<dyn PostingsIter>) -> Self {
        Self {
            ttl,
            inner,
            err: PostingsError::Custom("ttl exceeded".to_string()),
        }
    }
}

impl PostingsIter for PostingsFailingAfterNthCall {
    fn next(&mut self) -> bool {
        self.ttl -= 1;
        if self.ttl <= 0 {
            return false;
        }
        self.inner.next()
    }

    fn seek(&mut self, v: u64) -> bool {
        self.ttl -= 1;
        if self.ttl <= 0 {
            return false;
        }
        self.inner.seek(v)
    }

    fn at(&self) -> u64 {
        self.inner.at()
    }

    fn err(&self) -> Option<&PostingsError> {
        if self.ttl <= 0 {
            Some(&self.err)
        } else {
            self.inner.err()
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

fn create_file_reader(
    dir: &std::path::Path,
    input: &[(Labels, Vec<ChunkMeta>)],
) -> (IndexReader, PathBuf, HashSet<String>) {
    let fn_path = dir.join(INDEX_FILENAME);
    let mut iw = IndexWriter::new(&fn_path).unwrap();

    let mut symbols = HashSet::new();
    for (lbls, _) in input {
        for (n, v) in lbls.iter() {
            symbols.insert(n.to_string());
            symbols.insert(v.to_string());
        }
    }
    let mut syms: Vec<String> = symbols.iter().cloned().collect();
    syms.sort();
    for s in &syms {
        iw.add_symbol(s).unwrap();
    }
    for (i, (lbls, chunks)) in input.iter().enumerate() {
        iw.add_series(i as u64, lbls, chunks).unwrap();
    }
    iw.close().unwrap();

    let ir = IndexReader::open(&fn_path).unwrap();
    (ir, fn_path, symbols)
}

fn find_20k_series_path() -> Option<PathBuf> {
    let rel = "github.com/grafana/mimir-prometheus@v1.8.2-0.20261113142535-4819bfb31f7c/tsdb/testdata/20kseries.json";
    if let Ok(gomodcache) = std::env::var("GOMODCACHE") {
        let p = PathBuf::from(gomodcache).join(rel);
        if p.exists() {
            return Some(p);
        }
    }
    if let Ok(home) = std::env::var("HOME") {
        let p = PathBuf::from(home).join("go/pkg/mod").join(rel);
        if p.exists() {
            return Some(p);
        }
    }
    None
}

// ============================================================================
// tsdb/index/index_test.go (16 tests)
// ============================================================================

// Upstream: github.com/prometheus/prometheus/tsdb/index/index_test.go::TestIndexRW_Create_Open
#[test]
fn test_index_rw_create_open() {
    let dir = tempfile::tempdir().unwrap();
    let fn_path = dir.path().join(INDEX_FILENAME);

    // An empty index must still result in a readable file.
    let iw = IndexWriter::new(&fn_path).unwrap();
    iw.close().unwrap();

    let ir = IndexReader::open(&fn_path).unwrap();
    drop(ir);

    // Modify magic header must cause open to fail.
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut f = fs::OpenOptions::new().write(true).open(&fn_path).unwrap();
        f.seek(SeekFrom::Start(0)).unwrap();
        f.write_all(&[0, 0]).unwrap();
    }

    assert!(IndexReader::open(dir.path()).is_err());
    assert!(IndexReader::open(&fn_path).is_err());
}

// Upstream: github.com/prometheus/prometheus/tsdb/index/index_test.go::TestIndexRW_Postings
#[test]
fn test_index_rw_postings() {
    let dir = tempfile::tempdir().unwrap();
    let mut input = Vec::new();
    for i in 1..5 {
        let b = i.to_string();
        input.push((Labels::from_pairs([("a", "1"), ("b", b.as_str())]), vec![]));
    }
    let (ir, _, _) = create_file_reader(dir.path(), &input);

    let mut p = ir.postings_iter("a", &["1"]).unwrap();
    let mut i = 0usize;
    while p.next() {
        let (lbls, chks) = ir.series(p.at()).unwrap();
        assert!(chks.is_empty());
        assert_eq!(input[i].0, lbls);
        i += 1;
    }
    assert!(p.err().is_none());
    assert_eq!(i, input.len());

    // Test ShardedPostings() with and without series hash cache.
    for cache_enabled in [false, true] {
        let cache = if cache_enabled {
            Some(SeriesHashCache::new())
        } else {
            None
        };

        let mut p = ir.postings_iter("a", &["1"]).unwrap();
        let expected = expand_postings(&mut *p).unwrap();
        assert!(!expected.is_empty());

        const SHARD_COUNT: u64 = 4;
        let mut actual_shards: HashMap<u64, Vec<u64>> = HashMap::new();
        let mut actual_postings = Vec::with_capacity(expected.len());

        for shard_index in 0..SHARD_COUNT {
            let mut p = ir.postings_iter("a", &["1"]).unwrap();
            let mut sharded = ir.sharded_postings(&mut *p, shard_index, SHARD_COUNT, cache.as_ref());
            while sharded.next() {
                let r = sharded.at();
                actual_shards.entry(shard_index).or_default().push(r);
                actual_postings.push(r);
            }
            assert!(sharded.err().is_none());
        }

        actual_postings.sort_unstable();
        let mut exp_sorted = expected.clone();
        exp_sorted.sort_unstable();
        assert_eq!(exp_sorted, actual_postings);

        for (shard_index, ids) in &actual_shards {
            for &id in ids {
                let lbls = ir.series_labels(id).unwrap();
                assert_eq!(*shard_index, lbls.stable_hash() % SHARD_COUNT);
            }
        }

        for refs in [Vec::new(), expected] {
            let mut merged = merge_iters(vec![
                Box::new(ListPostings::new(refs)),
                Box::new(ErrPostingsIter::new(PostingsError::Custom(
                    "postings error".to_string(),
                ))),
            ]);
            let mut sharded = ir.sharded_postings(&mut *merged, 0, SHARD_COUNT, cache.as_ref());
            assert!(!sharded.next());
            let err = sharded.err().unwrap();
            assert!(err.to_string().contains("postings error"), "got {err}");
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/index/index_test.go::TestPostingsMany
#[test]
fn test_postings_many() {
    let dir = tempfile::tempdir().unwrap();
    let mut input = Vec::new();
    for i in 1..1000 {
        let v = format!("{i:03}");
        input.push((Labels::from_pairs([("i", v.as_str()), ("foo", "bar")]), vec![]));
    }
    let (ir, _, symbols) = create_file_reader(dir.path(), &input);

    let cases: &[&[&str]] = &[
        // Simple cases, everything is present.
        &["002"],
        &["031", "032", "033"],
        &["032", "033"],
        &["127", "128"],
        &["127", "128", "129"],
        &["127", "129"],
        &["128", "129"],
        &["998", "999"],
        &["999"],
        // Before actual values.
        &["000"],
        &["000", "001"],
        &["000", "002"],
        // After actual values.
        &["999a"],
        &["999", "999a"],
        &["998", "999", "999a"],
        // In the middle of actual values.
        &["126a", "127", "128"],
        &["127", "127a", "128"],
        &["127", "127a", "128", "128a", "129"],
        &["127", "128a", "129"],
        &["128", "128a", "129"],
        &["128", "129", "129a"],
        &[
            "126a", "126b", "127", "127a", "127b", "128", "128a", "128b", "129", "129a", "129b",
        ],
    ];

    for &case in cases {
        let refs = ir.postings("i", case).unwrap();
        let mut got = Vec::with_capacity(refs.len());
        for r in refs {
            let lbls = ir.series_labels(r).unwrap();
            got.push(lbls.get("i").to_string());
        }
        let exp: Vec<String> = case
            .iter()
            .filter(|&&e| symbols.contains(e) && e != "l")
            .map(|&s| s.to_string())
            .collect();
        assert_eq!(exp, got, "input: {case:?}");
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/index/index_test.go::TestPersistence_index_e2e
#[test]
fn test_persistence_index_e2e() {
    let mut lbls_vec: Vec<Labels> = if let Some(path) = find_20k_series_path() {
        let f = fs::File::open(path).unwrap();
        let reader = BufReader::new(f);
        let mut out = Vec::with_capacity(20_000);
        for line in reader.lines().take(20_000) {
            let line = line.unwrap();
            if line.trim().is_empty() {
                continue;
            }
            let map: BTreeMap<String, String> = serde_json::from_str(&line).unwrap();
            let pairs: Vec<(&str, &str)> =
                map.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
            out.push(Labels::from_sorted_slice(&pairs));
        }
        out
    } else {
        (0..20_000)
            .map(|i| {
                let m = format!("metric_{:03}", i % 500);
                let inst = format!("inst_{i:05}");
                let job = format!("job_{:02}", i % 20);
                Labels::from_pairs([("__name__", m.as_str()), ("instance", inst.as_str()), ("job", job.as_str())])
            })
            .collect()
    };

    lbls_vec.sort();
    lbls_vec.dedup();

    let mut input = Vec::with_capacity(lbls_vec.len());
    let mut chunk_ref = 0u64;
    for (i, lset) in lbls_vec.into_iter().enumerate() {
        let mut metas = Vec::new();
        for j in 0..=(i % 20) {
            chunk_ref += 1;
            metas.push(ChunkMeta {
                min_time: (j as i64) * 10_000,
                max_time: ((j + 1) as i64) * 10_000 - 1,
                ref_id: chunk_ref,
                chunk: None,
            });
        }
        input.push((lset, metas));
    }

    let dir = tempfile::tempdir().unwrap();
    let (ir, _, _) = create_file_reader(dir.path(), &input);

    // Build mock index expectations.
    let mut mock_postings: HashMap<(String, String), Vec<usize>> = HashMap::new();
    let mut mock_symbols: HashSet<String> = HashSet::new();
    let mut label_pairs: HashMap<String, Vec<String>> = HashMap::new();
    for (i, (lset, _)) in input.iter().enumerate() {
        for (n, v) in lset.iter() {
            mock_symbols.insert(n.to_string());
            mock_symbols.insert(v.to_string());
            mock_postings
                .entry((n.to_string(), v.to_string()))
                .or_insert_with(|| {
                    label_pairs
                        .entry(n.to_string())
                        .or_default()
                        .push(v.to_string());
                    Vec::new()
                })
                .push(i);
        }
        mock_postings
            .entry((String::new(), String::new()))
            .or_default()
            .push(i);
    }

    for ((name, value), expected_indices) in &mock_postings {
        let got_refs = ir.postings(name, &[value.as_str()]).unwrap();
        assert_eq!(
            got_refs.len(),
            expected_indices.len(),
            "postings count mismatch for {name:?}={value:?}"
        );
        for (&ref_id, &exp_idx) in got_refs.iter().zip(expected_indices.iter()) {
            let (got_lbls, got_chks) = ir.series(ref_id).unwrap();
            assert_eq!(input[exp_idx].0, got_lbls);
            assert_eq!(input[exp_idx].1, got_chks);
        }
    }

    for (k, mut vals) in label_pairs {
        vals.sort();
        let res = ir.label_values(&k).unwrap();
        assert_eq!(vals, res);
    }

    let got_symbols = ir.symbols().unwrap();
    let mut exp_symbols: Vec<String> = mock_symbols.into_iter().collect();
    exp_symbols.sort();
    assert_eq!(exp_symbols, got_symbols);
}

// Upstream: github.com/prometheus/prometheus/tsdb/index/index_test.go::TestWriter_ShouldReturnErrorOnSeriesWithDuplicatedLabelNames
#[test]
fn test_writer_should_return_error_on_series_with_duplicated_label_names() {
    let dir = tempfile::tempdir().unwrap();
    let mut w = IndexWriter::new(dir.path().join(INDEX_FILENAME)).unwrap();

    w.add_symbol("__name__").unwrap();
    w.add_symbol("metric_1").unwrap();
    w.add_symbol("metric_2").unwrap();

    let dup_labels =
        Labels::from_sorted_slice(&[("__name__", "metric_1"), ("__name__", "metric_2")]);
    w.add_series(0, &dup_labels, &[]).unwrap();

    let err = w.close().unwrap_err();
    assert!(
        err.to_string()
            .contains("corruption detected when writing postings to index"),
        "unexpected error: {err}"
    );
}

// Upstream: github.com/prometheus/prometheus/tsdb/index/index_test.go::TestDecbufUvarintWithInvalidBuffer
#[test]
fn test_decbuf_uvarint_with_invalid_buffer() {
    let b = [0x81u8, 0x81, 0x81, 0x81, 0x81, 0x81];
    assert!(checked_uvarint_decbuf_at(&b, 0).is_err());
}

// Upstream: github.com/prometheus/prometheus/tsdb/index/index_test.go::TestReaderWithInvalidBuffer
#[test]
fn test_reader_with_invalid_buffer() {
    let b = vec![0x81u8, 0x81, 0x81, 0x81, 0x81, 0x81];
    assert!(IndexReader::from_bytes(b).is_err());
}

// Upstream: github.com/prometheus/prometheus/tsdb/index/index_test.go::TestNewFileReaderErrorNoOpenFiles
#[test]
fn test_new_file_reader_error_no_open_files() {
    let dir = tempfile::tempdir().unwrap();
    let idx_name = dir.path().join(INDEX_FILENAME);
    fs::write(&idx_name, b"corrupted contents").unwrap();

    assert!(IndexReader::open(&idx_name).is_err());
    // Ensure file is not locked/held open and directory removes cleanly.
    fs::remove_file(&idx_name).unwrap();
    dir.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/index/index_test.go::TestSymbols
#[test]
fn test_symbols() {
    let mut buf = Encbuf::new();

    // Add prefix to the buffer to simulate symbols as part of larger buffer.
    buf.put_uvarint_str("something");

    let symbols_start = buf.len();
    buf.put_be32(204); // Length of symbols table.
    buf.put_be32(100); // Number of symbols.
    for i in 0..100u32 {
        let ch = char::from_u32(i).unwrap();
        let mut s = [0u8; 4];
        buf.put_uvarint_str(ch.encode_utf8(&mut s));
    }
    let crc = crc32c::crc32c(&buf.as_slice()[symbols_start + 4..]);
    buf.put_be32(crc);

    let s = Symbols::new(Bytes::from(buf.into_vec()), INDEX_FORMAT_V2, symbols_start).unwrap();

    // We store only 4 offsets to symbols (4 * 8 = 32).
    assert_eq!(32, s.size());

    for i in (0..100u32).rev() {
        let got = s.lookup(i).unwrap();
        let ch = char::from_u32(i).unwrap();
        assert_eq!(ch.to_string(), got);
    }
    assert!(s.lookup(100).is_err());

    for i in (0..100u32).rev() {
        let ch = char::from_u32(i).unwrap();
        let mut tmp = [0u8; 4];
        let r = s.reverse_lookup(ch.encode_utf8(&mut tmp)).unwrap();
        assert_eq!(i, r);
    }
    let ch100 = char::from_u32(100).unwrap();
    let mut tmp = [0u8; 4];
    assert!(s.reverse_lookup(ch100.encode_utf8(&mut tmp)).is_err());

    let iter_syms = s.iter().unwrap();
    assert_eq!(100, iter_syms.len());
    for (i, sym) in iter_syms.into_iter().enumerate() {
        let ch = char::from_u32(i as u32).unwrap();
        assert_eq!(ch.to_string(), sym);
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/index/index_test.go::TestDecoder_Postings_WrongInput
#[test]
fn test_decoder_postings_wrong_input() {
    assert!(decode_postings_raw(b"the cake is a lie").is_err());
}

// Upstream: github.com/prometheus/prometheus/tsdb/index/index_test.go::TestChunksRefOrdering
#[test]
fn test_chunks_ref_ordering() {
    let dir = tempfile::tempdir().unwrap();
    let mut idx = IndexWriter::new(dir.path().join(INDEX_FILENAME)).unwrap();

    idx.add_symbol("1").unwrap();
    idx.add_symbol("2").unwrap();
    idx.add_symbol("__name__").unwrap();

    let c50 = ChunkMeta {
        ref_id: 50,
        min_time: 0,
        max_time: 0,
        chunk: None,
    };
    let c100 = ChunkMeta {
        ref_id: 100,
        min_time: 0,
        max_time: 0,
        chunk: None,
    };
    let c200 = ChunkMeta {
        ref_id: 200,
        min_time: 0,
        max_time: 0,
        chunk: None,
    };

    idx.add_series(1, &Labels::from_pairs([("__name__", "1")]), &[c100])
        .unwrap();
    let err = idx
        .add_series(2, &Labels::from_pairs([("__name__", "2")]), &[c50])
        .unwrap_err();
    assert!(matches!(err, IndexError::InvalidChunkOrder(_)));
    idx.add_series(2, &Labels::from_pairs([("__name__", "2")]), &[c200])
        .unwrap();
    idx.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/index/index_test.go::TestChunksTimeOrdering
#[test]
fn test_chunks_time_ordering() {
    let dir = tempfile::tempdir().unwrap();
    let mut idx = IndexWriter::new(dir.path().join(INDEX_FILENAME)).unwrap();

    idx.add_symbol("1").unwrap();
    idx.add_symbol("2").unwrap();
    idx.add_symbol("__name__").unwrap();

    idx.add_series(
        1,
        &Labels::from_pairs([("__name__", "1")]),
        &[
            ChunkMeta {
                ref_id: 1,
                min_time: 0,
                max_time: 10,
                chunk: None,
            },
            ChunkMeta {
                ref_id: 2,
                min_time: 11,
                max_time: 20,
                chunk: None,
            },
            ChunkMeta {
                ref_id: 3,
                min_time: 21,
                max_time: 30,
                chunk: None,
            },
        ],
    )
    .unwrap();

    let err1 = idx
        .add_series(
            1,
            &Labels::from_pairs([("__name__", "2")]),
            &[
                ChunkMeta {
                    ref_id: 10,
                    min_time: 0,
                    max_time: 10,
                    chunk: None,
                },
                ChunkMeta {
                    ref_id: 20,
                    min_time: 10,
                    max_time: 20,
                    chunk: None,
                },
            ],
        )
        .unwrap_err();
    assert!(matches!(err1, IndexError::InvalidChunkOrder(_)));

    let err2 = idx
        .add_series(
            1,
            &Labels::from_pairs([("__name__", "2")]),
            &[ChunkMeta {
                ref_id: 10,
                min_time: 100,
                max_time: 30,
                chunk: None,
            }],
        )
        .unwrap_err();
    assert!(matches!(err2, IndexError::InvalidChunkOrder(_)));

    idx.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/index/index_test.go::TestReader_PostingsForLabelMatching
#[test]
fn test_reader_postings_for_label_matching() {
    let dir = tempfile::tempdir().unwrap();
    let mut input = Vec::new();
    for i in 1..=9 {
        let v = i.to_string();
        input.push((
            Labels::from_pairs([("__name__", v.as_str())]),
            vec![ChunkMeta {
                ref_id: 1,
                min_time: 0,
                max_time: 10,
                chunk: None,
            }],
        ));
    }
    let (ir, _, _) = create_file_reader(dir.path(), &input);

    let refs = ir
        .postings_for_label_matching("__name__", &mut |v| v.parse::<i32>().unwrap() % 2 == 0)
        .unwrap();
    assert_eq!(vec![4, 6, 8, 10], refs);
}

// Upstream: github.com/prometheus/prometheus/tsdb/index/index_test.go::TestReader_PostingsForAllLabelValues
#[test]
fn test_reader_postings_for_all_label_values() {
    let dir = tempfile::tempdir().unwrap();
    let mut input = Vec::new();
    for i in 1..=9 {
        let v = i.to_string();
        input.push((
            Labels::from_pairs([("__name__", v.as_str())]),
            vec![ChunkMeta {
                ref_id: 1,
                min_time: 0,
                max_time: 10,
                chunk: None,
            }],
        ));
    }
    let (ir, _, _) = create_file_reader(dir.path(), &input);

    let refs = ir.postings_for_all_values("__name__").unwrap();
    assert_eq!(vec![3, 4, 5, 6, 7, 8, 9, 10, 11], refs);
}

// Upstream: github.com/prometheus/prometheus/tsdb/index/index_test.go::TestReader_PostingsForLabelMatchingHonorsContextCancel
#[test]
fn test_reader_postings_for_label_matching_honors_context_cancel() {
    const SERIES_COUNT: usize = 1000;
    let dir = tempfile::tempdir().unwrap();
    let mut input = Vec::with_capacity(SERIES_COUNT);
    for i in 1..=SERIES_COUNT {
        let v = format!("{i:4}");
        input.push((
            Labels::from_pairs([("__name__", v.as_str())]),
            vec![ChunkMeta {
                ref_id: 1,
                min_time: 0,
                max_time: 10,
                chunk: None,
            }],
        ));
    }
    let (ir, _, _) = create_file_reader(dir.path(), &input);

    let fail_after = (SERIES_COUNT / 2) as u64;
    let ctx = MockContextErrAfter::new(fail_after);
    let mut p = ir.postings_for_label_matching_iter_ctx(&ctx, "__name__", &mut |_| true);
    assert!(!p.next());
    assert!(p.err().is_some());
    assert_eq!(fail_after, ctx.count());
}

// Upstream: github.com/prometheus/prometheus/tsdb/index/index_test.go::TestReader_LabelNamesForHonorsContextCancel
#[test]
fn test_reader_label_names_for_honors_context_cancel() {
    const SERIES_COUNT: usize = 1000;
    let dir = tempfile::tempdir().unwrap();
    let mut input = Vec::with_capacity(SERIES_COUNT);
    for i in 1..=SERIES_COUNT {
        let v = format!("{i:4}");
        input.push((
            Labels::from_pairs([("__name__", v.as_str())]),
            vec![ChunkMeta {
                ref_id: 1,
                min_time: 0,
                max_time: 10,
                chunk: None,
            }],
        ));
    }
    let (ir, _, _) = create_file_reader(dir.path(), &input);

    let mut p = ir.postings_iter("", &[""]).unwrap();
    let fail_after = 3u64;
    let ctx = MockContextErrAfter::new(fail_after);
    let res = ir.label_names_for_ctx(&ctx, &mut *p);
    assert!(res.is_err());
    assert_eq!(fail_after, ctx.count());
}

// ============================================================================
// tsdb/index/labelvalues_test.go (4 tests)
// ============================================================================

// Upstream: github.com/prometheus/prometheus/tsdb/index/labelvalues_test.go::TestReader_LabelValuesFor
#[test]
fn test_reader_label_values_for() {
    let dir = tempfile::tempdir().unwrap();
    let fn_path = dir.path().join(INDEX_FILENAME);
    let mut iw = IndexWriter::new(&fn_path).unwrap();

    let series = [
        Labels::from_pairs([("a", "1"), ("b", "1")]),
        Labels::from_pairs([("a", "1"), ("b", "2")]),
        Labels::from_pairs([("a", "1"), ("b", "3")]),
        Labels::from_pairs([("a", "1"), ("b", "4")]),
        Labels::from_pairs([("a", "2"), ("b", "5")]),
        Labels::from_pairs([("d", "1")]),
    ];

    for sym in ["1", "2", "3", "4", "5", "a", "b", "d"] {
        iw.add_symbol(sym).unwrap();
    }
    for (i, s) in series.iter().enumerate() {
        iw.add_series((i + 1) as u64, s, &[]).unwrap();
    }
    iw.close().unwrap();

    let ir = IndexReader::open(&fn_path).unwrap();

    // filtering based on non-empty postings
    {
        let mut p = ir.postings_iter("a", &["1"]).unwrap();
        let vals = ir.label_values_for(&mut *p, "b").unwrap();
        assert_eq!(vec!["1", "2", "3", "4"], vals);
    }
    // requesting a non-existent label value
    {
        let mut p = ir.postings_iter("a", &["1"]).unwrap();
        let vals = ir.label_values_for(&mut *p, "c").unwrap();
        assert!(vals.is_empty());
    }
    // filtering based on empty postings
    {
        let mut p = EmptyPostingsIter;
        let vals = ir.label_values_for(&mut p, "a").unwrap();
        assert!(vals.is_empty());
    }
    // filtering based on a postings set missing the label
    {
        let mut p = ir.postings_iter("d", &["1"]).unwrap();
        let vals = ir.label_values_for(&mut *p, "a").unwrap();
        assert!(vals.is_empty());
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/index/labelvalues_test.go::TestReader_LabelValuesExcluding
#[test]
fn test_reader_label_values_excluding() {
    let dir = tempfile::tempdir().unwrap();
    let fn_path = dir.path().join(INDEX_FILENAME);
    let mut iw = IndexWriter::new(&fn_path).unwrap();

    let series = [
        Labels::from_pairs([("a", "1"), ("b", "1")]),
        Labels::from_pairs([("a", "1"), ("b", "2")]),
        Labels::from_pairs([("a", "1"), ("b", "3")]),
        Labels::from_pairs([("a", "1"), ("b", "4")]),
        Labels::from_pairs([("a", "1"), ("b", "5")]),
        Labels::from_pairs([("a", "2"), ("b", "5")]),
        Labels::from_pairs([("d", "1")]),
    ];

    for sym in ["1", "2", "3", "4", "5", "a", "b", "d"] {
        iw.add_symbol(sym).unwrap();
    }
    for (i, s) in series.iter().enumerate() {
        iw.add_series((i + 1) as u64, s, &[]).unwrap();
    }
    iw.close().unwrap();

    let ir = IndexReader::open(&fn_path).unwrap();

    // filtering based on non-empty postings
    {
        let mut p = ir.postings_iter("a", &["1"]).unwrap();
        let vals = ir.label_values_excluding(&mut *p, "b").unwrap();
        assert_eq!(vec!["5"], vals);
    }
    // requesting a non-existent label value
    {
        let mut p = ir.postings_iter("a", &["1"]).unwrap();
        let vals = ir.label_values_excluding(&mut *p, "c").unwrap();
        assert!(vals.is_empty());
    }
    // filtering based on empty postings
    {
        let mut p = EmptyPostingsIter;
        let vals = ir.label_values_excluding(&mut p, "a").unwrap();
        assert_eq!(vec!["1", "2"], vals);
    }
    // filtering based on a postings set missing the label
    {
        let mut p = ir.postings_iter("d", &["1"]).unwrap();
        let vals = ir.label_values_excluding(&mut *p, "a").unwrap();
        assert_eq!(vec!["1", "2"], vals);
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/index/labelvalues_test.go::TestMemPostings_LabelValuesFor
#[test]
fn test_mem_postings_label_values_for() {
    let mut syms = SymbolTable::new();
    let mp = MemPostings::new();
    mp.add(1, &SymbolLabels::from_pairs(&mut syms, [("a", "1"), ("b", "1")]));
    mp.add(2, &SymbolLabels::from_pairs(&mut syms, [("a", "1"), ("b", "2")]));
    mp.add(3, &SymbolLabels::from_pairs(&mut syms, [("a", "1"), ("b", "3")]));
    mp.add(4, &SymbolLabels::from_pairs(&mut syms, [("a", "1"), ("b", "4")]));
    mp.add(5, &SymbolLabels::from_pairs(&mut syms, [("a", "2"), ("b", "5")]));
    mp.add(6, &SymbolLabels::from_pairs(&mut syms, [("d", "1")]));

    {
        let mut p = mp.postings_iter("a", &["1"], &syms);
        let vals = mp.label_values_for(&syms, &mut *p, "b").unwrap();
        assert_eq!(vec!["1", "2", "3", "4"], vals);
    }
    {
        let mut p = mp.postings_iter("a", &["1"], &syms);
        let vals = mp.label_values_for(&syms, &mut *p, "c").unwrap();
        assert!(vals.is_empty());
    }
    {
        let mut p = EmptyPostingsIter;
        let vals = mp.label_values_for(&syms, &mut p, "a").unwrap();
        assert!(vals.is_empty());
    }
    {
        let mut p = mp.postings_iter("d", &["1"], &syms);
        let vals = mp.label_values_for(&syms, &mut *p, "a").unwrap();
        assert!(vals.is_empty());
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/index/labelvalues_test.go::TestMemPostings_LabelValuesExcluding
#[test]
fn test_mem_postings_label_values_excluding() {
    let mut syms = SymbolTable::new();
    let mp = MemPostings::new();
    mp.add(1, &SymbolLabels::from_pairs(&mut syms, [("a", "1"), ("b", "1")]));
    mp.add(2, &SymbolLabels::from_pairs(&mut syms, [("a", "1"), ("b", "2")]));
    mp.add(3, &SymbolLabels::from_pairs(&mut syms, [("a", "1"), ("b", "3")]));
    mp.add(4, &SymbolLabels::from_pairs(&mut syms, [("a", "1"), ("b", "4")]));
    mp.add(5, &SymbolLabels::from_pairs(&mut syms, [("a", "1"), ("b", "5")]));
    mp.add(6, &SymbolLabels::from_pairs(&mut syms, [("a", "2"), ("b", "5")]));
    mp.add(7, &SymbolLabels::from_pairs(&mut syms, [("d", "1")]));

    {
        let mut p = mp.postings_iter("a", &["1"], &syms);
        let vals = mp.label_values_excluding(&syms, &mut *p, "b").unwrap();
        assert_eq!(vec!["5"], vals);
    }
    {
        let mut p = mp.postings_iter("a", &["1"], &syms);
        let vals = mp.label_values_excluding(&syms, &mut *p, "c").unwrap();
        assert!(vals.is_empty());
    }
    {
        let mut p = EmptyPostingsIter;
        let vals = mp.label_values_excluding(&syms, &mut p, "a").unwrap();
        assert_eq!(vec!["1", "2"], vals);
    }
    {
        let mut p = mp.postings_iter("d", &["1"], &syms);
        let vals = mp.label_values_excluding(&syms, &mut *p, "a").unwrap();
        assert_eq!(vec!["1", "2"], vals);
    }
}

// ============================================================================
// tsdb/index/lookup_test.go (1 test)
// ============================================================================

fn matchers_to_string(matchers: &[LabelMatcher]) -> String {
    matchers
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

// Upstream: github.com/prometheus/prometheus/tsdb/index/lookup_test.go::TestScanEmptyMatchersLookupPlanner
#[test]
fn test_scan_empty_matchers_lookup_planner() {
    let m = |t: MatchType, n: &str, v: &str| LabelMatcher::new(t, n, v).unwrap();
    let planner = ScanEmptyMatchersLookupPlanner;

    type PlannerCase<'a> = (&'a str, Vec<LabelMatcher>, Vec<LabelMatcher>, Vec<LabelMatcher>);
    let cases: Vec<PlannerCase<'_>> = vec![
        (
            "splits matchers between index and scan",
            vec![
                m(MatchType::Equal, "job", "prometheus"),
                m(MatchType::RegexMatch, "instance", ".*"),
                m(MatchType::Equal, "env", ""),
                m(MatchType::NotEqual, "status", "down"),
                m(MatchType::RegexMatch, "region", ".+"),
            ],
            vec![
                m(MatchType::Equal, "job", "prometheus"),
                m(MatchType::NotEqual, "status", "down"),
            ],
            vec![
                m(MatchType::Equal, "env", ""),
                m(MatchType::RegexMatch, "region", ".+"),
            ],
        ),
        (
            "all selective matchers go to index",
            vec![
                m(MatchType::Equal, "job", "prometheus"),
                m(MatchType::NotEqual, "instance", "localhost"),
                m(MatchType::RegexMatch, "env", "prod|staging"),
            ],
            vec![
                m(MatchType::Equal, "job", "prometheus"),
                m(MatchType::NotEqual, "instance", "localhost"),
                m(MatchType::RegexMatch, "env", "prod|staging"),
            ],
            vec![],
        ),
        (
            "even with only non-selective matchers, there is still one which stays as index matcher",
            vec![
                m(MatchType::Equal, "job", ""),
                m(MatchType::RegexMatch, "instance", ".*"),
                m(MatchType::RegexMatch, "env", ".+"),
            ],
            vec![m(MatchType::Equal, "job", "")],
            vec![m(MatchType::RegexMatch, "env", ".+")],
        ),
        (
            "all non-selective matchers go to scan",
            vec![
                m(MatchType::Equal, "job", "prometheus"),
                m(MatchType::Equal, "job", ""),
                m(MatchType::RegexMatch, "instance", ".*"),
                m(MatchType::RegexMatch, "env", ".+"),
            ],
            vec![m(MatchType::Equal, "job", "prometheus")],
            vec![
                m(MatchType::Equal, "job", ""),
                m(MatchType::RegexMatch, "env", ".+"),
            ],
        ),
        (
            "single matcher goes to index",
            vec![m(MatchType::Equal, "job", "prometheus")],
            vec![m(MatchType::Equal, "job", "prometheus")],
            vec![],
        ),
        (
            "single empty matcher goes to index",
            vec![m(MatchType::Equal, "job", "")],
            vec![m(MatchType::Equal, "job", "")],
            vec![],
        ),
        (
            "single match-all regex goes to index",
            vec![m(MatchType::RegexMatch, "job", ".*")],
            vec![m(MatchType::RegexMatch, "job", ".*")],
            vec![],
        ),
        (
            "single match-non-empty regex goes to index",
            vec![m(MatchType::RegexMatch, "job", ".+")],
            vec![m(MatchType::RegexMatch, "job", ".+")],
            vec![],
        ),
        (
            "single empty matcher goes to index; invalid on API but internal code uses it",
            vec![m(MatchType::Equal, "", "")],
            vec![m(MatchType::Equal, "", "")],
            vec![],
        ),
        ("empty matchers slice", vec![], vec![], vec![]),
    ];

    for (name, matchers, expected_index, expected_scan) in cases {
        let initial = IndexOnlyLookupPlan(matchers);
        let plan = planner.plan_index_lookup(&initial);
        assert_eq!(
            matchers_to_string(&expected_scan),
            matchers_to_string(plan.scan_matchers()),
            "scan mismatch in {name}"
        );
        assert_eq!(
            matchers_to_string(&expected_index),
            matchers_to_string(plan.index_matchers()),
            "index mismatch in {name}"
        );
    }
}

// ============================================================================
// tsdb/index/postings_cloner_test.go (1 test)
// ============================================================================

// Upstream: github.com/prometheus/prometheus/tsdb/index/postings_cloner_test.go::TestPostingsCloner
#[test]
fn test_postings_cloner() {
    // seek beyond highest value of postings, then other clone seeks higher
    {
        let mut src = ListPostings::new(vec![1, 2, 4, 8]);
        let pc = PostingsCloner::new(&mut src);
        let mut p1 = pc.clone_iter();
        assert!(!p1.seek(9));
        let mut p2 = pc.clone_iter();
        assert!(!p2.seek(10));
    }
    // seek beyond highest value of postings, then other clone seeks lower
    {
        let mut src = ListPostings::new(vec![1, 2, 4, 8]);
        let pc = PostingsCloner::new(&mut src);
        let mut p1 = pc.clone_iter();
        assert!(!p1.seek(9));
        let mut p2 = pc.clone_iter();
        assert!(p2.seek(2));
        assert_eq!(2, p2.at());
    }
    // seek to posting with value 3 or higher
    {
        let mut src = ListPostings::new(vec![1, 2, 4, 8]);
        let pc = PostingsCloner::new(&mut src);
        let mut p = pc.clone_iter();
        assert!(p.seek(3));
        assert_eq!(4, p.at());
        assert!(p.seek(4));
        assert_eq!(4, p.at());
    }
    // seek alternatively on different postings
    {
        let mut src = ListPostings::new(vec![1, 2, 4, 8]);
        let pc = PostingsCloner::new(&mut src);
        let mut p1 = pc.clone_iter();
        assert!(p1.seek(1));
        assert_eq!(1, p1.at());
        let mut p2 = pc.clone_iter();
        assert!(p2.seek(2));
        assert_eq!(2, p2.at());
        let mut p3 = pc.clone_iter();
        assert!(p3.seek(4));
        assert_eq!(4, p3.at());
        let mut p4 = pc.clone_iter();
        assert!(p4.seek(5));
        assert_eq!(8, p4.at());

        assert!(p1.seek(3));
        assert_eq!(4, p1.at());
        assert!(p1.seek(4));
        assert_eq!(4, p1.at());
    }
    // iterate through the postings
    {
        let mut src = ListPostings::new(vec![1, 2, 4, 8]);
        let pc = PostingsCloner::new(&mut src);
        let mut p1 = pc.clone_iter();
        let mut p2 = pc.clone_iter();

        assert!(p1.next());
        assert_eq!(1, p1.at());
        assert!(p2.next());
        assert_eq!(1, p2.at());

        assert!(p1.next());
        assert_eq!(2, p1.at());
        assert!(p1.next());
        assert_eq!(4, p1.at());
        assert!(p1.next());
        assert_eq!(8, p1.at());
        assert!(!p1.next());

        assert!(p2.next());
        assert_eq!(2, p2.at());
        assert!(p2.next());
        assert_eq!(4, p2.at());
    }
    // ensure a failed seek doesn't allow more next calls
    {
        let mut src = ListPostings::new(vec![1, 2, 4, 8]);
        let pc = PostingsCloner::new(&mut src);
        let mut p = pc.clone_iter();
        assert!(!p.seek(9));
        assert!(!p.next());
    }
    // cloning an err postings
    {
        let mut err_src = ErrPostingsIter::new(PostingsError::Custom("foobar".to_string()));
        let pc = PostingsCloner::new(&mut err_src);
        let mut p = pc.clone_iter();
        assert!(!p.next());
        assert_eq!("foobar", p.err().unwrap().to_string());
    }
}

// ============================================================================
// tsdb/index/postings_mimir_test.go (1 test)
// ============================================================================

// Upstream: github.com/prometheus/prometheus/tsdb/index/postings_mimir_test.go::TestMemPostings_PostingsListAccessors
#[test]
fn test_mem_postings_postings_list_accessors() {
    let mut syms = SymbolTable::new();
    let mp = MemPostings::new();
    mp.add(5, &SymbolLabels::from_pairs(&mut syms, [("pod", "a"), ("job", "z")]));
    mp.add(1, &SymbolLabels::from_pairs(&mut syms, [("pod", "a"), ("job", "x")]));
    mp.add(2, &SymbolLabels::from_pairs(&mut syms, [("pod", "a"), ("job", "y")]));
    mp.add(3, &SymbolLabels::from_pairs(&mut syms, [("pod", "b"), ("job", "y")]));

    let cases: &[(&str, &str, &[u64])] = &[
        ("pod", "a", &[1, 2, 5]),
        ("pod", "b", &[3]),
        ("job", "y", &[2, 3]),
        ("pod", "missing", &[]),
        ("missing", "a", &[]),
    ];

    for &(name, value, all_refs) in cases {
        assert_eq!(all_refs.len(), mp.postings_length(name, value, &syms));

        let mut random_refs = Vec::new();
        mp.append_random_postings_refs(name, value, 10, &syms, &mut random_refs);
        if all_refs.is_empty() {
            assert!(random_refs.is_empty());
            continue;
        }
        assert_eq!(10, random_refs.len());
        for r in &random_refs {
            assert!(all_refs.contains(r));
        }
    }

    let mut random_refs = vec![42];
    mp.append_random_postings_refs("pod", "b", 2, &syms, &mut random_refs);
    assert_eq!(vec![42, 3, 3], random_refs);
}

// ============================================================================
// tsdb/index/postingsstats_test.go (2 tests)
// ============================================================================

// Upstream: github.com/prometheus/prometheus/tsdb/index/postingsstats_test.go::TestPostingsStats
#[test]
fn test_postings_stats() {
    const MAX_COUNT: u64 = 3_000_000;
    const HEAP_LENGTH: usize = 10;
    let mut stats = PostingsStatMaxHeap::default();
    stats.init(HEAP_LENGTH);
    for i in 0..MAX_COUNT {
        stats.push(Stat {
            name: "Label-da".to_string(),
            count: i,
        });
    }
    stats.push(Stat {
        name: "Stuff".to_string(),
        count: 3_000_000,
    });

    let data = stats.get();
    assert_eq!(10, data.len());
    for (i, item) in data.iter().enumerate().take(HEAP_LENGTH) {
        assert_eq!(MAX_COUNT - (i as u64), item.count);
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/index/postingsstats_test.go::TestPostingsStats2
#[test]
fn test_postings_stats2() {
    const HEAP_LENGTH: usize = 10;
    let mut stats = PostingsStatMaxHeap::default();
    stats.init(HEAP_LENGTH);
    stats.push(Stat {
        name: "Stuff".to_string(),
        count: 10,
    });
    stats.push(Stat {
        name: "Stuff".to_string(),
        count: 11,
    });
    stats.push(Stat {
        name: "Stuff".to_string(),
        count: 1,
    });
    stats.push(Stat {
        name: "Stuff".to_string(),
        count: 6,
    });

    let data = stats.get();
    assert_eq!(4, data.len());
    assert_eq!(11, data[0].count);
}

// ============================================================================
// tsdb/index/postings_test.go (23 tests)
// ============================================================================

// Upstream: github.com/prometheus/prometheus/tsdb/index/postings_test.go::TestMemPostings_addFor
#[test]
fn test_mem_postings_add_for() {
    let p = MemPostings::new();
    for id in [1, 2, 3, 4, 6, 7, 8] {
        p.add_for_pair(id, ALL_POSTINGS_KEY.0, ALL_POSTINGS_KEY.1);
    }
    p.add_for_pair(5, ALL_POSTINGS_KEY.0, ALL_POSTINGS_KEY.1);
    assert_eq!(vec![1, 2, 3, 4, 5, 6, 7, 8], p.all_postings());
}

// Upstream: github.com/prometheus/prometheus/tsdb/index/postings_test.go::TestMemPostings_ensureOrder
#[test]
fn test_mem_postings_ensure_order() {
    let mut syms = SymbolTable::new();
    let p = MemPostings::new_unordered();
    let a_id = syms.intern("a");
    let mut rng = Lcg::new(12345);

    for i in 0..100 {
        let v_id = syms.intern(&i.to_string());
        for _ in 0..100 {
            let id = rng.next_u64();
            p.add_for_pair(id, a_id, v_id);
        }
    }

    p.ensure_order();
    assert!(p.is_all_sorted());
}

// Upstream: github.com/prometheus/prometheus/tsdb/index/postings_test.go::TestIntersect
#[test]
fn test_intersect() {
    let lp = |xs: &[u64]| -> Box<dyn PostingsIter> { Box::new(ListPostings::new(xs.to_vec())) };

    type IterCase = (Vec<Box<dyn PostingsIter>>, Option<Vec<u64>>);
    let cases: Vec<IterCase> = vec![
        (vec![], None),
        (vec![lp(&[1, 2, 3]), lp(&[2, 3, 4]), Box::new(EmptyPostingsIter)], None),
        (vec![lp(&[2, 3, 4]), lp(&[1, 2, 3]), Box::new(EmptyPostingsIter)], None),
        (vec![Box::new(EmptyPostingsIter), lp(&[2, 3, 4]), lp(&[1, 2, 3])], None),
        (vec![Box::new(EmptyPostingsIter), lp(&[1, 2, 3]), lp(&[2, 3, 4])], None),
        (vec![lp(&[1, 2, 3]), Box::new(EmptyPostingsIter), lp(&[2, 3, 4])], None),
        (vec![lp(&[2, 3, 4]), Box::new(EmptyPostingsIter), lp(&[1, 2, 3])], None),
        (
            vec![
                lp(&[2, 3, 4]),
                Box::new(EmptyPostingsIter),
                lp(&[1, 2, 3]),
                lp(&[1, 2, 3]),
                lp(&[2, 3, 4]),
                lp(&[1, 2, 3]),
                lp(&[1, 2, 3]),
                lp(&[1, 2, 3]),
            ],
            None,
        ),
        (vec![lp(&[1, 2, 3, 4, 5]), lp(&[6, 7, 8, 9, 10])], Some(vec![])),
        (vec![lp(&[1, 2, 3, 4, 5]), lp(&[4, 5, 6, 7, 8])], Some(vec![4, 5])),
        (
            vec![lp(&[1, 2, 3, 4, 9, 10]), lp(&[1, 4, 5, 6, 7, 8, 10, 11])],
            Some(vec![1, 4, 10]),
        ),
        (vec![lp(&[1]), lp(&[0, 1])], Some(vec![1])),
        (vec![lp(&[1])], Some(vec![1])),
        (vec![lp(&[1]), lp(&[])], Some(vec![])),
        (vec![lp(&[]), lp(&[])], Some(vec![])),
    ];

    for (its, exp) in cases {
        let mut i = intersect_iters(its);
        match exp {
            None => assert!(i.is_empty_sentinel()),
            Some(expected) => {
                assert!(!i.is_empty_sentinel(), "unexpected EmptyPostings sentinel");
                let res = expand_postings(&mut *i).unwrap();
                assert_eq!(expected, res);
            }
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/index/postings_test.go::TestMultiIntersect
#[test]
fn test_multi_intersect() {
    let cases: &[(&[&[u64]], &[u64])] = &[
        (
            &[
                &[1, 2, 3, 4, 5, 6, 1000, 1001],
                &[2, 4, 5, 6, 7, 8, 999, 1001],
                &[1, 2, 5, 6, 7, 8, 1001, 1200],
            ],
            &[2, 5, 6, 1001],
        ),
        (&[&[1, 2], &[1, 2], &[1, 2], &[2]], &[2]),
    ];

    for &(p, expected) in cases {
        let ps: Vec<Box<dyn PostingsIter>> = p
            .iter()
            .map(|l| Box::new(ListPostings::new(l.to_vec())) as Box<dyn PostingsIter>)
            .collect();
        let mut it = intersect_iters(ps);
        let res = expand_postings(&mut *it).unwrap();
        assert_eq!(expected, res.as_slice());
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/index/postings_test.go::TestMultiMerge
#[test]
fn test_multi_merge() {
    let i1 = Box::new(ListPostings::new(vec![1, 2, 3, 4, 5, 6, 1000, 1001]));
    let i2 = Box::new(ListPostings::new(vec![2, 4, 5, 6, 7, 8, 999, 1001]));
    let i3 = Box::new(ListPostings::new(vec![1, 2, 5, 6, 7, 8, 1001, 1200]));

    let mut m = merge_iters(vec![i1, i2, i3]);
    let res = expand_postings(&mut *m).unwrap();
    assert_eq!(vec![1, 2, 3, 4, 5, 6, 7, 8, 999, 1000, 1001, 1200], res);
}

// Upstream: github.com/prometheus/prometheus/tsdb/index/postings_test.go::TestMergedPostings
#[test]
fn test_merged_postings() {
    let lp = |xs: &[u64]| -> Box<dyn PostingsIter> { Box::new(ListPostings::new(xs.to_vec())) };

    type MergeCase = (Vec<Box<dyn PostingsIter>>, Option<Vec<u64>>);
    let cases: Vec<MergeCase> = vec![
        (vec![], None),
        (vec![lp(&[]), lp(&[])], None),
        (vec![lp(&[])], Some(vec![])),
        (
            vec![
                Box::new(EmptyPostingsIter),
                Box::new(EmptyPostingsIter),
                Box::new(EmptyPostingsIter),
                Box::new(EmptyPostingsIter),
            ],
            None,
        ),
        (
            vec![lp(&[1, 2, 3, 4, 5]), lp(&[6, 7, 8, 9, 10])],
            Some(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
        ),
        (
            vec![lp(&[1, 2, 3, 4, 5]), lp(&[4, 5, 6, 7, 8])],
            Some(vec![1, 2, 3, 4, 5, 6, 7, 8]),
        ),
        (
            vec![lp(&[1, 2, 3, 4, 9, 10]), lp(&[1, 4, 5, 6, 7, 8, 10, 11])],
            Some(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]),
        ),
        (
            vec![
                lp(&[1, 2, 3, 4, 9, 10]),
                Box::new(EmptyPostingsIter),
                lp(&[1, 4, 5, 6, 7, 8, 10, 11]),
            ],
            Some(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]),
        ),
        (vec![lp(&[1, 2]), lp(&[])], Some(vec![1, 2])),
        (
            vec![lp(&[1, 2]), Box::new(EmptyPostingsIter)],
            Some(vec![1, 2]),
        ),
    ];

    for (its, exp) in cases {
        let mut m = merge_iters(its);
        match exp {
            None => assert!(!m.next()),
            Some(expected) => {
                assert!(!m.is_empty_sentinel(), "unexpected EmptyPostings sentinel");
                let res = expand_postings(&mut *m).unwrap();
                assert_eq!(expected, res);
            }
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/index/postings_test.go::TestMergedPostingsSeek
#[test]
fn test_merged_postings_seek() {
    type SeekCase<'a> = (&'a [u64], &'a [u64], u64, bool, &'a [u64]);
    let cases: &[SeekCase<'_>] = &[
        (&[2, 3, 4, 5], &[6, 7, 8, 9, 10], 1, true, &[2, 3, 4, 5, 6, 7, 8, 9, 10]),
        (&[1, 2, 3, 4, 5], &[6, 7, 8, 9, 10], 2, true, &[2, 3, 4, 5, 6, 7, 8, 9, 10]),
        (&[1, 2, 3, 4, 5], &[4, 5, 6, 7, 8], 9, false, &[]),
        (&[1, 2, 3, 4, 9, 10], &[1, 4, 5, 6, 7, 8, 10, 11], 10, true, &[10, 11]),
    ];

    for &(a, b, seek, success, res) in cases {
        let mut p = merge_iters(vec![
            Box::new(ListPostings::new(a.to_vec())),
            Box::new(ListPostings::new(b.to_vec())),
        ]);
        assert_eq!(success, p.seek(seek));
        if success {
            let start = p.at();
            let mut lst = vec![start];
            lst.extend(expand_postings(&mut *p).unwrap());
            assert_eq!(res, lst.as_slice());
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/index/postings_test.go::TestRemovedPostings
#[test]
fn test_removed_postings() {
    let cases: &[(&[u64], &[u64], &[u64])] = &[
        (&[], &[], &[]),
        (&[1, 2, 3, 4], &[], &[1, 2, 3, 4]),
        (&[], &[1, 2, 3, 4], &[]),
        (&[1, 2, 3, 4, 5], &[6, 7, 8, 9, 10], &[1, 2, 3, 4, 5]),
        (&[1, 2, 3, 4, 5], &[4, 5, 6, 7, 8], &[1, 2, 3]),
        (&[1, 2, 3, 4, 9, 10], &[1, 4, 5, 6, 7, 8, 10, 11], &[2, 3, 9]),
        (&[1, 2, 3, 4, 9, 10], &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11], &[]),
    ];

    for &(a, b, expected) in cases {
        let mut rp = RemovedPostings::new(
            Box::new(ListPostings::new(a.to_vec())),
            Box::new(ListPostings::new(b.to_vec())),
        );
        let res = expand_postings(&mut rp).unwrap();
        assert_eq!(expected, res.as_slice());
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/index/postings_test.go::TestRemovedNextStackoverflow
#[test]
fn test_removed_next_stackoverflow() {
    let count: u64 = 10_000_000;
    let full: Vec<u64> = (0..count).collect();
    let remove: Vec<u64> = (0..count).collect();

    let mut rp = RemovedPostings::new(
        Box::new(ListPostings::new(full)),
        Box::new(ListPostings::new(remove)),
    );
    let mut got_elem = false;
    while rp.next() {
        got_elem = true;
    }
    assert!(rp.err().is_none());
    assert!(!got_elem);
}

// Upstream: github.com/prometheus/prometheus/tsdb/index/postings_test.go::TestRemovedPostingsSeek
#[test]
fn test_removed_postings_seek() {
    type RemovedSeekCase<'a> = (&'a [u64], &'a [u64], u64, bool, &'a [u64]);
    let cases: &[RemovedSeekCase<'_>] = &[
        (&[2, 3, 4, 5], &[6, 7, 8, 9, 10], 1, true, &[2, 3, 4, 5]),
        (&[1, 2, 3, 4, 5], &[6, 7, 8, 9, 10], 2, true, &[2, 3, 4, 5]),
        (&[1, 2, 3, 4, 5], &[4, 5, 6, 7, 8], 9, false, &[]),
        (&[1, 2, 3, 4, 9, 10], &[1, 4, 5, 6, 7, 8, 10, 11], 10, false, &[]),
        (&[1, 2, 3, 4, 9, 10], &[1, 4, 5, 6, 7, 8, 11], 4, true, &[9, 10]),
        (&[1, 2, 3, 4, 9, 10], &[1, 4, 5, 6, 7, 8, 11], 5, true, &[9, 10]),
        (&[1, 2, 3, 4, 9, 10], &[1, 4, 5, 6, 7, 8, 11], 10, true, &[10]),
    ];

    for &(a, b, seek, success, res) in cases {
        let mut p = RemovedPostings::new(
            Box::new(ListPostings::new(a.to_vec())),
            Box::new(ListPostings::new(b.to_vec())),
        );
        assert_eq!(success, p.seek(seek));
        if success {
            let start = p.at();
            let mut lst = vec![start];
            lst.extend(expand_postings(&mut p).unwrap());
            assert_eq!(res, lst.as_slice());
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/index/postings_test.go::TestBigEndian
#[test]
fn test_big_endian() {
    const NUM: usize = 1000;
    let mut rng = Lcg::new(98765);
    let mut ls = vec![0u32; NUM];
    ls[0] = 2;
    for i in 1..NUM {
        ls[i] = ls[i - 1] + rng.next_u32(25) + 2;
    }

    let mut be_lst = vec![0u8; NUM * 4];
    for i in 0..NUM {
        be_lst[i * 4..i * 4 + 4].copy_from_slice(&ls[i].to_be_bytes());
    }

    // Iteration
    {
        let mut bep = BigEndianPostings::from_slice(&be_lst);
        for &expected in &ls {
            assert!(bep.next());
            assert_eq!(u64::from(expected), bep.at());
        }
        assert!(!bep.next());
        assert!(bep.err().is_none());
    }

    // Seek beyond uint32
    {
        for target in [1u64 << 32, (1u64 << 32) + 1, (1u64 << 33) + 2, u64::MAX] {
            for positioned in [false, true] {
                let mut bep = BigEndianPostings::from_slice(&[0, 0, 0, 1, 0xff, 0xff, 0xff, 0xff]);
                if positioned {
                    assert!(bep.next());
                    assert_eq!(1, bep.at());
                }
                assert!(!bep.seek(target), "target={target} positioned={positioned}");
                assert!(!bep.next());
                assert!(bep.err().is_none());
            }
        }
    }

    // Seek
    {
        let table = [
            (ls[0] - 1, ls[0], true),
            (ls[4], ls[4], true),
            (ls[500] - 1, ls[500], true),
            (ls[600] + 1, ls[601], true),
            (ls[600] + 1, ls[601], true),
            (ls[600] + 1, ls[601], true),
            (ls[0], ls[601], true),
            (ls[600], ls[601], true),
            (ls[999], ls[999], true),
            (ls[999] + 10, ls[999], false),
        ];

        let mut bep = BigEndianPostings::from_slice(&be_lst);
        for (seek, val, found) in table {
            assert_eq!(found, bep.seek(u64::from(seek)));
            assert_eq!(u64::from(val), bep.at());
            assert!(bep.err().is_none());
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/index/postings_test.go::TestIntersectWithMerge
#[test]
fn test_intersect_with_merge() {
    let a = Box::new(ListPostings::new(vec![21, 22, 23, 24, 25, 30]));
    let b = merge_iters(vec![
        Box::new(ListPostings::new(vec![10, 20, 30])),
        Box::new(ListPostings::new(vec![15, 26, 30])),
    ]);
    let mut p = intersect_iters(vec![a, b]);
    let res = expand_postings(&mut *p).unwrap();
    assert_eq!(vec![30], res);
}

// Upstream: github.com/prometheus/prometheus/tsdb/index/postings_test.go::TestWithoutPostings
#[test]
fn test_without_postings() {
    let lp = |xs: &[u64]| -> Box<dyn PostingsIter> { Box::new(ListPostings::new(xs.to_vec())) };

    type WithoutCase = (Box<dyn PostingsIter>, Box<dyn PostingsIter>, Option<Vec<u64>>);
    let cases: Vec<WithoutCase> = vec![
        (Box::new(EmptyPostingsIter), Box::new(EmptyPostingsIter), None),
        (Box::new(EmptyPostingsIter), lp(&[1, 2]), None),
        (lp(&[1, 2]), Box::new(EmptyPostingsIter), Some(vec![1, 2])),
        (lp(&[]), lp(&[]), Some(vec![])),
        (lp(&[1, 2, 3]), lp(&[]), Some(vec![1, 2, 3])),
        (lp(&[1, 2, 3]), lp(&[4, 5, 6]), Some(vec![1, 2, 3])),
        (lp(&[1, 2, 3]), lp(&[3, 4, 5]), Some(vec![1, 2])),
    ];

    for (base, drop_it, exp) in cases {
        let mut w = without_iters(base, drop_it);
        match exp {
            None => assert!(w.is_empty_sentinel()),
            Some(expected) => {
                assert!(!w.is_empty_sentinel(), "unexpected EmptyPostings sentinel");
                let res = expand_postings(&mut *w).unwrap();
                assert_eq!(expected, res);
            }
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/index/postings_test.go::TestMemPostingsStats
#[test]
fn test_mem_postings_stats() {
    let mut syms = SymbolTable::new();
    let p = MemPostings::new();
    p.add(1, &SymbolLabels::from_pairs(&mut syms, [("label", "value1")]));
    p.add(1, &SymbolLabels::from_pairs(&mut syms, [("label", "value2")]));
    p.add(1, &SymbolLabels::from_pairs(&mut syms, [("label", "value3")]));
    p.add(2, &SymbolLabels::from_pairs(&mut syms, [("label", "value1")]));

    let stats = p.stats(&syms, "label", 10, |name, value, n| {
        ((name.len() + value.len()) as u64) * n
    });

    assert_eq!(2, stats.cardinality_metrics_stats[0].count);
    assert_eq!("value1", stats.cardinality_metrics_stats[0].name);

    assert_eq!(3, stats.cardinality_label_stats[0].count);
    assert_eq!("label", stats.cardinality_label_stats[0].name);

    assert_eq!(44, stats.label_value_stats[0].count);
    assert_eq!("label", stats.label_value_stats[0].name);

    assert_eq!(2, stats.label_value_pairs_stats[0].count);
    assert_eq!("label=value1", stats.label_value_pairs_stats[0].name);

    assert_eq!(3, stats.num_label_pairs);
}

// Upstream: github.com/prometheus/prometheus/tsdb/index/postings_test.go::TestMemPostings_Delete
#[test]
fn test_mem_postings_delete() {
    let mut syms = SymbolTable::new();
    let p = MemPostings::new();
    p.add(1, &SymbolLabels::from_pairs(&mut syms, [("lbl1", "a")]));
    p.add(2, &SymbolLabels::from_pairs(&mut syms, [("lbl1", "b")]));
    p.add(3, &SymbolLabels::from_pairs(&mut syms, [("lbl2", "a")]));

    let mut before = p.postings_iter("", &[""], &syms);
    let mut deleted_refs = HashSet::new();
    deleted_refs.insert(2);
    let lbl1_id = syms.intern("lbl1");
    let b_id = syms.intern("b");
    p.delete(&deleted_refs, &[(lbl1_id, b_id)]);
    let mut after = p.postings_iter("", &[""], &syms);

    let expanded_before = expand_postings(&mut *before).unwrap();
    assert_eq!(vec![1, 2, 3], expanded_before);

    let expanded_after = expand_postings(&mut *after).unwrap();
    assert_eq!(vec![1, 3], expanded_after);

    let mut deleted = p.postings_iter("lbl1", &["b"], &syms);
    let expanded_deleted = expand_postings(&mut *deleted).unwrap();
    assert!(expanded_deleted.is_empty());
}

// Upstream: github.com/prometheus/prometheus/tsdb/index/postings_test.go::TestFindIntersectingPostings
#[test]
fn test_find_intersecting_postings() {
    // multiple intersections
    {
        let mut p = ListPostings::new(vec![10, 15, 20, 25, 30, 35, 40, 45, 50]);
        let mut candidates: Vec<Box<dyn PostingsIter>> = vec![
            Box::new(ListPostings::new(vec![7, 13, 14, 27])),
            Box::new(ListPostings::new(vec![10, 20])),
            Box::new(ListPostings::new(vec![29, 30, 31])),
            Box::new(ListPostings::new(vec![29, 30, 31])),
            Box::new(ListPostings::new(vec![60])),
            Box::new(ListPostings::new(vec![45])),
            Box::new(EmptyPostingsIter),
        ];
        let mut indexes = find_intersecting_postings(&mut p, &mut candidates).unwrap();
        indexes.sort_unstable();
        assert_eq!(vec![1, 2, 3, 5], indexes);
    }
    // no intersections
    {
        let mut p = ListPostings::new(vec![10, 15, 20, 25, 30, 35, 40, 45, 50]);
        let mut candidates: Vec<Box<dyn PostingsIter>> = vec![
            Box::new(ListPostings::new(vec![7, 13, 14, 27])),
            Box::new(ListPostings::new(vec![60])),
            Box::new(EmptyPostingsIter),
        ];
        let indexes = find_intersecting_postings(&mut p, &mut candidates).unwrap();
        assert!(indexes.is_empty());
    }
    // p is ErrPostings
    {
        let mut p = ErrPostingsIter::new(PostingsError::ContextCanceled("canceled".to_string()));
        let mut candidates: Vec<Box<dyn PostingsIter>> =
            vec![Box::new(ListPostings::new(vec![1]))];
        assert!(find_intersecting_postings(&mut p, &mut candidates).is_err());
    }
    // one of the candidates is ErrPostings
    {
        let mut p = ListPostings::new(vec![1]);
        let mut candidates: Vec<Box<dyn PostingsIter>> = vec![
            Box::new(ListPostings::new(vec![1])),
            Box::new(ErrPostingsIter::new(PostingsError::ContextCanceled(
                "canceled".to_string(),
            ))),
        ];
        assert!(find_intersecting_postings(&mut p, &mut candidates).is_err());
    }
    // one of the candidates fails on nth call
    {
        let mut p = ListPostings::new(vec![10, 20, 30, 40, 50, 60, 70]);
        let mut candidates: Vec<Box<dyn PostingsIter>> = vec![
            Box::new(ListPostings::new(vec![7, 13, 14, 27])),
            Box::new(PostingsFailingAfterNthCall::new(
                2,
                Box::new(ListPostings::new(vec![29, 30, 31, 40])),
            )),
        ];
        assert!(find_intersecting_postings(&mut p, &mut candidates).is_err());
    }
    // p fails on the nth call
    {
        let mut p = PostingsFailingAfterNthCall::new(
            2,
            Box::new(ListPostings::new(vec![10, 20, 30, 40, 50, 60, 70])),
        );
        let mut candidates: Vec<Box<dyn PostingsIter>> = vec![
            Box::new(ListPostings::new(vec![7, 13, 14, 27])),
            Box::new(ListPostings::new(vec![29, 30, 31, 40])),
        ];
        assert!(find_intersecting_postings(&mut p, &mut candidates).is_err());
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/index/postings_test.go::TestPostingsWithIndexHeap
#[test]
fn test_postings_with_index_heap() {
    // seekHead
    {
        let mut h = PostingsWithIndexHeap(vec![
            PostingsWithIndex {
                index: 0,
                p: Box::new(ListPostings::new(vec![10, 20, 30])),
                popped: false,
            },
            PostingsWithIndex {
                index: 1,
                p: Box::new(ListPostings::new(vec![1, 5])),
                popped: false,
            },
            PostingsWithIndex {
                index: 2,
                p: Box::new(ListPostings::new(vec![25, 50])),
                popped: false,
            },
        ]);
        for node in &mut h.0 {
            node.p.next();
        }
        h.init();

        for expected in [1u64, 5, 10, 20, 25, 30, 50] {
            assert_eq!(expected, h.at());
            h.seek_head(h.at() + 1).unwrap();
        }
        assert!(h.empty());
    }
    // pop
    {
        let mut h = PostingsWithIndexHeap(vec![
            PostingsWithIndex {
                index: 0,
                p: Box::new(ListPostings::new(vec![10, 20, 30])),
                popped: false,
            },
            PostingsWithIndex {
                index: 1,
                p: Box::new(ListPostings::new(vec![1, 5])),
                popped: false,
            },
            PostingsWithIndex {
                index: 2,
                p: Box::new(ListPostings::new(vec![25, 50])),
                popped: false,
            },
        ]);
        for node in &mut h.0 {
            node.p.next();
        }
        h.init();

        for expected in [1u64, 5, 10, 20] {
            assert_eq!(expected, h.at());
            h.seek_head(h.at() + 1).unwrap();
        }
        assert_eq!(25, h.at());
        let node = h.pop_node().unwrap();
        assert_eq!(2, node.index);
        assert_eq!(25, node.p.at());
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/index/postings_test.go::TestListPostings
#[test]
fn test_list_postings() {
    // empty list
    {
        let mut p = ListPostings::new(vec![]);
        assert_eq!(0, p.len());
        assert!(!p.next());
        assert!(!p.seek(10));
        assert!(!p.next());
        assert!(p.err().is_none());
        assert_eq!(0, p.len());
    }

    // one posting
    {
        let mut p = ListPostings::new(vec![10]);
        assert_eq!(1, p.len());
        assert!(p.next());
        assert_eq!(10, p.at());
        assert!(!p.next());
        assert!(p.err().is_none());
        assert_eq!(0, p.len());
    }
    {
        let mut p = ListPostings::new(vec![10]);
        assert_eq!(1, p.len());
        assert!(p.seek(5));
        assert_eq!(10, p.at());
        assert!(p.seek(5));
        assert_eq!(10, p.at());
        assert!(!p.next());
        assert!(p.err().is_none());
        assert_eq!(0, p.len());
    }
    {
        let mut p = ListPostings::new(vec![10]);
        assert_eq!(1, p.len());
        assert!(p.seek(10));
        assert_eq!(10, p.at());
        assert!(!p.next());
        assert!(p.err().is_none());
        assert_eq!(0, p.len());
    }
    {
        let mut p = ListPostings::new(vec![10]);
        assert_eq!(1, p.len());
        assert!(!p.seek(15));
        assert!(!p.next());
        assert!(p.err().is_none());
        assert_eq!(0, p.len());
    }
    {
        let mut p = ListPostings::new(vec![10]);
        assert_eq!(1, p.len());
        assert!(p.next());
        assert!(!p.seek(15));
        assert!(!p.next());
        assert!(p.err().is_none());
        assert_eq!(0, p.len());
    }

    // multiple postings
    {
        let mut p = ListPostings::new(vec![10, 20]);
        assert_eq!(2, p.len());
        assert!(p.next());
        assert_eq!(10, p.at());
        assert!(p.next());
        assert_eq!(20, p.at());
        assert!(!p.next());
        assert!(p.err().is_none());
        assert_eq!(0, p.len());
    }
    {
        let mut p = ListPostings::new(vec![10, 20]);
        assert_eq!(2, p.len());
        assert!(p.seek(5));
        assert_eq!(10, p.at());
        assert!(p.seek(5));
        assert_eq!(10, p.at());
        assert!(p.seek(10));
        assert_eq!(10, p.at());
        assert!(p.next());
        assert_eq!(20, p.at());
        assert!(p.seek(10));
        assert_eq!(20, p.at());
        assert!(p.seek(20));
        assert_eq!(20, p.at());
        assert!(!p.next());
        assert!(p.err().is_none());
        assert_eq!(0, p.len());
    }
    {
        let mut p = ListPostings::new(vec![10, 20, 30, 40, 50]);
        assert_eq!(5, p.len());
        assert!(p.seek(45));
        assert_eq!(50, p.at());
        assert!(!p.next());
        assert_eq!(0, p.len());
    }
    {
        let mut p = ListPostings::new(vec![10, 20, 30, 40, 50]);
        assert_eq!(5, p.len());
        assert!(p.seek(50));
        assert_eq!(50, p.at());
        assert!(!p.next());
        assert_eq!(0, p.len());
    }
    {
        let mut p = ListPostings::new(vec![10, 20, 30, 40, 50]);
        assert_eq!(5, p.len());
        assert!(!p.seek(60));
        assert!(!p.next());
        assert_eq!(0, p.len());
    }

    for c in [2u64, 8, 9, 10] {
        let list: Vec<u64> = (0..c).map(|i| i * 10).collect();

        // all one by one
        let mut p = ListPostings::new(list.clone());
        for i in 0..c {
            assert!(p.seek(i * 10));
            assert_eq!(i * 10, p.at());
        }
        assert!(!p.seek(c * 10));

        // each one
        for &r in &list {
            let mut p = ListPostings::new(list.clone());
            assert!(p.seek(r));
            assert_eq!(r, p.at());
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/index/postings_test.go::TestMemPostings_PostingsForLabelMatching
#[test]
fn test_mem_postings_postings_for_label_matching() {
    let mut syms = SymbolTable::new();
    let mp = MemPostings::new();
    mp.add(1, &SymbolLabels::from_pairs(&mut syms, [("foo", "1")]));
    mp.add(2, &SymbolLabels::from_pairs(&mut syms, [("foo", "2")]));
    mp.add(3, &SymbolLabels::from_pairs(&mut syms, [("foo", "3")]));
    mp.add(4, &SymbolLabels::from_pairs(&mut syms, [("foo", "4")]));

    let refs = mp
        .as_source(&syms)
        .postings_for_label_matching("foo", &mut |v| v.parse::<i32>().unwrap() % 2 == 0)
        .unwrap();
    assert_eq!(vec![2, 4], refs);
}

// Upstream: github.com/prometheus/prometheus/tsdb/index/postings_test.go::TestMemPostings_PostingsForAllLabelValues
#[test]
fn test_mem_postings_postings_for_all_label_values() {
    let mut syms = SymbolTable::new();
    let mp = MemPostings::new();
    mp.add(1, &SymbolLabels::from_pairs(&mut syms, [("foo", "1")]));
    mp.add(2, &SymbolLabels::from_pairs(&mut syms, [("foo", "2")]));
    mp.add(3, &SymbolLabels::from_pairs(&mut syms, [("foo", "3")]));
    mp.add(4, &SymbolLabels::from_pairs(&mut syms, [("foo", "4")]));

    let mut p = mp.postings_for_all_values_iter("foo", &syms);
    assert!(p.err().is_none());
    let refs = expand_postings(&mut *p).unwrap();
    assert_eq!(vec![1, 2, 3, 4], refs);
}

// Upstream: github.com/prometheus/prometheus/tsdb/index/postings_test.go::TestMemPostings_PostingsForLabelMatchingHonorsContextCancel
#[test]
fn test_mem_postings_postings_for_label_matching_honors_context_cancel() {
    let mut syms = SymbolTable::new();
    let mem_p = MemPostings::new();
    let series_count = 10 * CHECK_CONTEXT_EVERY_N_ITERATIONS;
    for i in 1..=series_count {
        let v = format!("{i:4}");
        mem_p.add(
            i as u64,
            &SymbolLabels::from_pairs(&mut syms, [("__name__", v.as_str())]),
        );
    }

    let fail_after = (series_count / 2 / CHECK_CONTEXT_EVERY_N_ITERATIONS) as u64;
    let ctx = MockContextErrAfter::new(fail_after);
    let p = mem_p.postings_for_label_matching_iter_ctx(&ctx, "__name__", &syms, &mut |_| true);
    assert!(p.err().is_some());
    assert_eq!(fail_after + 1, ctx.count());
}

// Upstream: github.com/prometheus/prometheus/tsdb/index/postings_test.go::TestMemPostings_LabelValuesLimitSmallest
#[test]
fn test_mem_postings_label_values_limit_smallest() {
    let mut syms = SymbolTable::new();
    let p = MemPostings::new();
    let all = ["a", "b", "c", "d", "e"];
    for (i, &v) in all.iter().enumerate().rev() {
        p.add(
            (i + 1) as u64,
            &SymbolLabels::from_pairs(&mut syms, [("lbl", v)]),
        );
    }

    let shared = p.label_values("lbl", &syms);
    assert_eq!(vec!["e", "d", "c", "b", "a"], shared);

    type LimitCase<'a> = (&'a str, &'a str, Option<LabelHints>, Option<&'a [&'a str]>);
    let cases: &[LimitCase<'_>] = &[
        ("nil hints", "lbl", None, None),
        (
            "no limit",
            "lbl",
            Some(LabelHints {
                limit: 0,
                limit_smallest: true,
            }),
            None,
        ),
        (
            "limit below count",
            "lbl",
            Some(LabelHints {
                limit: 2,
                limit_smallest: true,
            }),
            Some(&["a", "b"]),
        ),
        (
            "limit of one",
            "lbl",
            Some(LabelHints {
                limit: 1,
                limit_smallest: true,
            }),
            Some(&["a"]),
        ),
        (
            "limit at count",
            "lbl",
            Some(LabelHints {
                limit: 5,
                limit_smallest: true,
            }),
            None,
        ),
        (
            "limit above count",
            "lbl",
            Some(LabelHints {
                limit: 10,
                limit_smallest: true,
            }),
            None,
        ),
        (
            "limit without flag",
            "lbl",
            Some(LabelHints {
                limit: 2,
                limit_smallest: false,
            }),
            Some(&["e", "d"]),
        ),
        (
            "unknown label name",
            "missing",
            Some(LabelHints {
                limit: 2,
                limit_smallest: true,
            }),
            Some(&[]),
        ),
    ];

    for (name, label, hints, want) in cases {
        let mut got = p.label_values_with_hints(label, &syms, hints.as_ref());
        if *label == "missing" {
            assert!(got.is_empty(), "{name}");
        } else if let Some(w) = want {
            let expected: Vec<String> = w.iter().map(|s| s.to_string()).collect();
            assert_eq!(expected, got, "{name}");
        } else {
            let mut sorted_got = got.clone();
            sorted_got.sort();
            assert_eq!(vec!["a", "b", "c", "d", "e"], sorted_got, "{name}");
        }

        assert_eq!(shared, p.label_values("lbl", &syms));
        if !got.is_empty() {
            got[0] = "zzz".to_string();
            assert_eq!(shared, p.label_values("lbl", &syms));
        }
    }

    let mut deleted = HashSet::new();
    deleted.insert(3u64);
    let lbl_id = syms.intern("lbl");
    let c_id = syms.intern("c");
    p.delete(&deleted, &[(lbl_id, c_id)]);
    assert_eq!(
        vec!["a", "b", "d"],
        p.label_values_with_hints(
            "lbl",
            &syms,
            Some(&LabelHints {
                limit: 3,
                limit_smallest: true,
            }),
        )
    );
}

// Upstream: github.com/prometheus/prometheus/tsdb/index/postings_test.go::TestMemPostings_LabelValuesBytes
#[test]
fn test_mem_postings_label_values_bytes() {
    let value = |prefix: &str, length: usize| -> String {
        format!("{prefix}{}", "x".repeat(length - prefix.len()))
    };
    let long1 = value("a", 200);
    let long2 = value("b", 300);

    let mut syms = SymbolTable::new();
    let p = MemPostings::new();
    assert!(p.label_values_bytes(&syms).is_empty());

    // Values at or below the minimum length are ignored entirely.
    for i in 0..100 {
        let v = value(&i.to_string(), LABEL_VALUE_BYTES_MIN_LENGTH);
        p.add(
            i as u64,
            &SymbolLabels::from_pairs(&mut syms, [("short", v.as_str())]),
        );
    }
    assert!(p.label_values_bytes(&syms).is_empty());

    p.add(
        1000,
        &SymbolLabels::from_pairs(
            &mut syms,
            [("lbl1", long1.as_str()), ("lbl2", long2.as_str())],
        ),
    );
    p.add(
        1001,
        &SymbolLabels::from_pairs(&mut syms, [("lbl1", long2.as_str())]),
    );
    let expected1 = HashMap::from([
        ("lbl1".to_string(), 500u64),
        ("lbl2".to_string(), 300u64),
    ]);
    assert_eq!(expected1, p.label_values_bytes(&syms));

    // Repeating a value on another series must not count it again.
    p.add(
        1002,
        &SymbolLabels::from_pairs(&mut syms, [("lbl1", long1.as_str())]),
    );
    assert_eq!(expected1, p.label_values_bytes(&syms));

    // Deleting series 1001 empties lbl1=long2, releasing its bytes, while lbl1=long1 stays.
    let mut del1001 = HashSet::new();
    del1001.insert(1001);
    let lbl1_id = syms.intern("lbl1");
    let lbl2_id = syms.intern("lbl2");
    let long1_id = syms.intern(&long1);
    let long2_id = syms.intern(&long2);
    p.delete(&del1001, &[(lbl1_id, long2_id)]);
    let expected2 = HashMap::from([
        ("lbl1".to_string(), 200u64),
        ("lbl2".to_string(), 300u64),
    ]);
    assert_eq!(expected2, p.label_values_bytes(&syms));

    // Deleting every series carrying lbl2 drops the label name entirely.
    let mut del1000 = HashSet::new();
    del1000.insert(1000);
    p.delete(&del1000, &[(lbl1_id, long1_id), (lbl2_id, long2_id)]);
    let expected3 = HashMap::from([("lbl1".to_string(), 200u64)]);
    assert_eq!(expected3, p.label_values_bytes(&syms));

    // Re-adding a released value counts it again.
    p.add(
        1003,
        &SymbolLabels::from_pairs(&mut syms, [("lbl2", long2.as_str())]),
    );
    assert_eq!(expected2, p.label_values_bytes(&syms));

    // Returned map is owned/independent copy.
    let mut got = p.label_values_bytes(&syms);
    got.insert("lbl1".to_string(), 999);
    assert_eq!(200, p.label_values_bytes(&syms)["lbl1"]);
}

// ============================================================================
// tsdb/tombstones/tombstones_test.go (6 tests)
// ============================================================================

// Upstream: github.com/prometheus/prometheus/tsdb/tombstones/tombstones_test.go::TestWriteAndReadbackTombstones
#[test]
fn test_write_and_readback_tombstones() {
    let tmpdir = tempfile::tempdir().unwrap();
    let mut rng = Lcg::new(54321);
    let mut ref_id = 0u64;
    let stones = MemTombstones::new();

    for _ in 0..100 {
        ref_id += u64::from(rng.next_u32(10)) + 1;
        let num_ranges = rng.next_u32(5) + 1;
        let mut dranges = Intervals::with_capacity(num_ranges as usize);
        let mut mint = rng.next_i64(1_000_000_000_000);
        for _ in 0..num_ranges {
            let maxt = mint + rng.next_i64(1000);
            add_interval(&mut dranges, Interval::new(mint, maxt));
            mint += rng.next_i64(1000) + 1;
        }
        for iv in dranges {
            stones.add_interval(ref_id, iv);
        }
    }

    write_tombstones_file(tmpdir.path(), &stones).unwrap();
    let restr = read_tombstones_file(tmpdir.path()).unwrap();
    assert_eq!(stones, restr);
}

// Upstream: github.com/prometheus/prometheus/tsdb/tombstones/tombstones_test.go::TestDeletingTombstones
#[test]
fn test_deleting_tombstones() {
    let stones = MemTombstones::new();
    let mut rng = Lcg::new(777);
    let ref_id = 42u64;
    let mint = rng.next_i64(1_000_000_000);
    let mut dranges = Intervals::new();
    add_interval(&mut dranges, Interval::new(mint, mint + rng.next_i64(1000)));
    for &iv in &dranges {
        stones.add_interval(ref_id, iv);
        stones.add_interval(43, iv);
    }

    let intervals = stones.get(ref_id);
    assert_eq!(dranges, intervals);

    let mut del = HashSet::new();
    del.insert(ref_id);
    stones.delete_tombstones(&del);

    let intervals_after = stones.get(ref_id);
    assert!(intervals_after.is_empty());
}

// Upstream: github.com/prometheus/prometheus/tsdb/tombstones/tombstones_test.go::TestTombstonesGetWithCopy
#[test]
fn test_tombstones_get_with_copy() {
    let stones = MemTombstones::new();
    for iv in [
        Interval::new(1, 2),
        Interval::new(7, 8),
        Interval::new(11, 12),
    ] {
        stones.add_interval(1, iv);
    }

    let mut intervals0 = stones.get(1);
    assert_eq!(
        vec![
            Interval::new(1, 2),
            Interval::new(7, 8),
            Interval::new(11, 12)
        ],
        intervals0
    );
    add_interval(&mut intervals0, Interval::new(4, 6));
    assert_eq!(
        vec![
            Interval::new(1, 2),
            Interval::new(4, 8),
            Interval::new(11, 12)
        ],
        intervals0
    );

    let intervals2 = stones.get(1);
    assert_eq!(
        vec![
            Interval::new(1, 2),
            Interval::new(7, 8),
            Interval::new(11, 12)
        ],
        intervals2
    );
}

// Upstream: github.com/prometheus/prometheus/tsdb/tombstones/tombstones_test.go::TestTruncateBefore
#[test]
fn test_truncate_before() {
    type TruncateCase<'a> = (&'a [(i64, i64)], i64, &'a [(i64, i64)]);
    let cases: &[TruncateCase<'_>] = &[
        (&[(1, 2), (4, 10), (12, 100)], 3, &[(4, 10), (12, 100)]),
        (
            &[(1, 2), (4, 10), (12, 100), (200, 1000)],
            900,
            &[(200, 1000)],
        ),
        (&[(1, 2), (4, 10), (12, 100), (200, 1000)], 2000, &[]),
        (
            &[(1, 2), (4, 10), (12, 100), (200, 1000)],
            0,
            &[(1, 2), (4, 10), (12, 100), (200, 1000)],
        ),
    ];

    for &(before, before_t, after) in cases {
        let ref_id = 42u64;
        let stones = MemTombstones::new();
        for &(mint, maxt) in before {
            stones.add_interval(ref_id, Interval::new(mint, maxt));
        }
        stones.truncate_before(before_t);
        let ts = stones.get(ref_id);
        let exp: Vec<Interval> = after
            .iter()
            .map(|&(mint, maxt)| Interval::new(mint, maxt))
            .collect();
        assert_eq!(exp, ts);
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/tombstones/tombstones_test.go::TestAddingNewIntervals
#[test]
fn test_adding_new_intervals() {
    type AddIntervalCase<'a> = (&'a [(i64, i64)], (i64, i64), &'a [(i64, i64)]);
    let cases: &[AddIntervalCase<'_>] = &[
        (&[], (1, 2), &[(1, 2)]),
        (&[(1, 2)], (1, 2), &[(1, 2)]),
        (&[(1, 4), (6, 6)], (5, 6), &[(1, 6)]),
        (&[(1, 10), (12, 20), (25, 30)], (21, 25), &[(1, 10), (12, 30)]),
        (
            &[(1, 10), (12, 20), (25, 30)],
            (22, 23),
            &[(1, 10), (12, 20), (22, 23), (25, 30)],
        ),
        (&[(1, 2), (3, 5), (7, 7)], (6, 7), &[(1, 2), (3, 7)]),
        (&[(1, 10), (12, 20), (25, 30)], (18, 23), &[(1, 10), (12, 23), (25, 30)]),
        (&[(1, 10), (12, 20), (25, 30)], (9, 23), &[(1, 23), (25, 30)]),
        (&[(1, 10), (12, 20), (25, 30)], (9, 230), &[(1, 230)]),
        (&[(5, 10), (12, 20), (25, 30)], (1, 4), &[(1, 10), (12, 20), (25, 30)]),
        (&[(5, 10), (12, 20), (25, 30)], (11, 14), &[(5, 20), (25, 30)]),
        (
            &[(5, 10), (12, 20), (25, 30)],
            (1, 3),
            &[(1, 3), (5, 10), (12, 20), (25, 30)],
        ),
        (
            &[(5, 10), (12, 20), (25, 30)],
            (35, 40),
            &[(5, 10), (12, 20), (25, 30), (35, 40)],
        ),
        (&[], (i64::MIN, 2), &[(i64::MIN, 2)]),
        (&[(i64::MIN, 2)], (9, i64::MAX), &[(i64::MIN, 2), (9, i64::MAX)]),
        (&[(9, i64::MAX)], (i64::MIN, 2), &[(i64::MIN, 2), (9, i64::MAX)]),
        (&[(9, i64::MAX)], (i64::MIN, 10), &[(i64::MIN, i64::MAX)]),
        (&[(9, 10)], (i64::MIN, 7), &[(i64::MIN, 7), (9, 10)]),
        (&[(9, 10)], (12, i64::MAX), &[(9, 10), (12, i64::MAX)]),
        (&[(9, 10)], (i64::MIN, 8), &[(i64::MIN, 10)]),
        (&[(9, 10)], (11, i64::MAX), &[(9, i64::MAX)]),
        (&[(1, 2), (5, 6)], (5, i64::MAX), &[(1, 2), (5, i64::MAX)]),
        (&[(1, 2), (5, 6), (10, 11)], (4, i64::MAX), &[(1, 2), (4, i64::MAX)]),
    ];

    for &(exist, new_itv, exp) in cases {
        let mut ivs: Intervals = exist.iter().map(|&(a, b)| Interval::new(a, b)).collect();
        add_interval(&mut ivs, Interval::new(new_itv.0, new_itv.1));
        let expected: Intervals = exp.iter().map(|&(a, b)| Interval::new(a, b)).collect();
        assert_eq!(expected, ivs);
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/tombstones/tombstones_test.go::TestMemTombstonesConcurrency
#[test]
fn test_mem_tombstones_concurrency() {
    let tomb = Arc::new(MemTombstones::new());
    let total_runs = 100i64;
    let t1 = {
        let tomb = Arc::clone(&tomb);
        thread::spawn(move || {
            for x in 0..total_runs {
                tomb.add_interval(x as u64, Interval::new(x, x));
            }
        })
    };
    let t2 = {
        let tomb = Arc::clone(&tomb);
        thread::spawn(move || {
            for x in 0..total_runs {
                let _ = tomb.get(x as u64);
            }
        })
    };
    t1.join().unwrap();
    t2.join().unwrap();
}
