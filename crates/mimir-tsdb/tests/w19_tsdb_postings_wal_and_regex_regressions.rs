//! Wave 19 TSDB regression tests:
//! - W19-DTM-01: Atomic subtracting-matcher ordering in MemPostings
//! - W19-DTM-02: Non-UTF-8 label bytes in WAL & checkpoint replay (record::decode_labels)
//! - W19-DTM-03: FastRegexMatcher compile limits & large literal alternations
//! - W19-RPC-01: Reversed time range (end < start) parity & OOO tombstone filter in Head
//! - W19-RPC-02: effective_head range check on chunk queries vs label queries
//! - W19-RPC-04: unexpected all postings on empty-label LabelValues with other matchers
//! - W19-RPC-05: SearchError::InvalidThreshold message formatting

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use mimir_tsdb::encoding::{Decbuf, Encbuf};
use mimir_tsdb::head::{Head, HeadOptions};
use mimir_tsdb::labels::{Labels, SymbolLabels};
use mimir_tsdb::matcher::{FastRegexMatcher, LabelMatcher, MatchType};
use mimir_tsdb::postings::{MemPostings, PostingsError};
use mimir_tsdb::querier::{BlockSelectStatsSink, TsdbQuerier};
use mimir_tsdb::record::{decode_labels, RecordEncoder, RecordType, RefSample, RefSeries, Tombstone};
use mimir_tsdb::search::SearchHints;
use mimir_tsdb::symbols::SymbolTable;
use mimir_tsdb::wlog::{CompressionType, WlogWriter};

#[test]
fn w19_dtm_01_atomic_subtract_matcher_under_concurrent_add() {
    let postings = Arc::new(MemPostings::new());
    let mut syms = SymbolTable::new();

    // Pre-intern all label names & values so concurrent writers never block on symbol lock,
    // and insert an initial baseline series '{__name__="up", l="y"}'.
    let up_labels = SymbolLabels::from_pairs(&mut syms, [("__name__", "up"), ("l", "y")]);
    let x_labels = SymbolLabels::from_pairs(&mut syms, [("__name__", "up"), ("l", "x")]);
    let symbols = Arc::new(syms);
    postings.add(1, &up_labels);

    let stop = Arc::new(AtomicBool::new(false));
    let postings_w = Arc::clone(&postings);
    let stop_w = Arc::clone(&stop);
    let writer = std::thread::spawn(move || {
        for r in 2u64..4000 {
            postings_w.add(r, &x_labels);
        }
        stop_w.store(true, Ordering::Release);
    });

    let m_up = LabelMatcher::new(MatchType::Equal, "__name__", "up").unwrap();
    let m_not_x = LabelMatcher::new(MatchType::NotEqual, "l", "x").unwrap();
    let m_not_rx = LabelMatcher::new(MatchType::RegexNotMatch, "l", "x.*").unwrap();
    let m_empty_l = LabelMatcher::new(MatchType::Equal, "l", "").unwrap();

    while !stop.load(Ordering::Acquire) {
        let res1 = postings
            .postings_for_matchers(&symbols, &[m_up.clone(), m_not_x.clone()])
            .unwrap();
        assert_eq!(
            res1,
            vec![1],
            "concurrent add must never leak series with l=x into {{__name__=\"up\", l!=\"x\"}}"
        );

        let res2 = postings
            .postings_for_matchers(&symbols, &[m_up.clone(), m_not_rx.clone()])
            .unwrap();
        assert_eq!(
            res2,
            vec![1],
            "concurrent add must never leak series with l=x into {{__name__=\"up\", l!~\"x.*\"}}"
        );

        let res3 = postings
            .postings_for_matchers(&symbols, std::slice::from_ref(&m_empty_l))
            .unwrap();
        assert!(
            res3.is_empty(),
            "subtract-only query {{l=\"\"}} must never observe half-added series: got {res3:?}"
        );
    }

    writer.join().unwrap();
}

fn encode_raw_series_record(ref_id: u64, pairs: &[(&[u8], &[u8])]) -> Vec<u8> {
    let mut buf = Encbuf::new();
    buf.put_byte(RecordType::Series as u8);
    buf.put_be64(ref_id);
    buf.put_uvarint(pairs.len());
    for (k, v) in pairs {
        buf.put_uvarint(k.len());
        buf.put_bytes(k);
        buf.put_uvarint(v.len());
        buf.put_bytes(v);
    }
    buf.into_vec()
}

fn encode_raw_exemplar_record(ref_id: u64, t: i64, v: f64, pairs: &[(&[u8], &[u8])]) -> Vec<u8> {
    let mut buf = Encbuf::new();
    buf.put_byte(RecordType::Exemplars as u8);
    buf.put_be64(ref_id);
    buf.put_be64_i64(t);
    buf.put_varint64(0);
    buf.put_varint64(0);
    buf.put_be_f64(v);
    buf.put_uvarint(pairs.len());
    for (k, v) in pairs {
        buf.put_uvarint(k.len());
        buf.put_bytes(k);
        buf.put_uvarint(v.len());
        buf.put_bytes(v);
    }
    buf.into_vec()
}

#[test]
fn w19_dtm_02_decode_labels_non_utf8_wal_and_checkpoint_replay() {
    // 1. Direct record::decode_labels test with non-UTF-8 bytes.
    let mut raw = Encbuf::new();
    raw.put_uvarint(2);
    raw.put_uvarint(8);
    raw.put_bytes(b"__name__");
    raw.put_uvarint(4);
    raw.put_bytes(b"a\xff\xfeb");
    raw.put_uvarint(3);
    raw.put_bytes(b"job");
    raw.put_uvarint(3);
    raw.put_bytes(b"api");
    let mut dec = Decbuf::new(raw.as_slice());
    let decoded = decode_labels(&mut dec).expect("non-UTF-8 label bytes must decode lossily");
    assert_eq!(decoded.get("__name__"), "a\u{FFFD}\u{FFFD}b");
    assert_eq!(decoded.get("job"), "api");

    // 1b. U+FFFD replacement can reorder names that were sorted as raw bytes: 0x80 < 0xE0, but
    // 0x80 decodes to U+FFFD (EF BF BD), which sorts after the valid U+0800 (E0 A0 80). The
    // decoded labels must be re-sorted, never handed to `from_sorted_slice` in raw-byte order.
    let mut flipped = Encbuf::new();
    flipped.put_uvarint(2);
    flipped.put_uvarint(1);
    flipped.put_bytes(b"\x80");
    flipped.put_uvarint(1);
    flipped.put_bytes(b"1");
    flipped.put_uvarint(3);
    flipped.put_bytes("\u{0800}".as_bytes());
    flipped.put_uvarint(1);
    flipped.put_bytes(b"2");
    let mut dec = Decbuf::new(flipped.as_slice());
    let decoded = decode_labels(&mut dec).unwrap();
    let pairs: Vec<(String, String)> = decoded
        .iter()
        .map(|(n, v)| (n.to_string(), v.to_string()))
        .collect();
    assert_eq!(
        pairs,
        vec![
            ("\u{0800}".to_string(), "2".to_string()),
            ("\u{FFFD}".to_string(), "1".to_string()),
        ]
    );
    assert_eq!(decoded, Labels::from_pairs([("\u{FFFD}", "1"), ("\u{0800}", "2")]));

    // 1c. Distinct raw names collapsing onto the same replaced name keep one label (last wins).
    let mut collide = Encbuf::new();
    collide.put_uvarint(2);
    collide.put_uvarint(1);
    collide.put_bytes(b"\x80");
    collide.put_uvarint(1);
    collide.put_bytes(b"1");
    collide.put_uvarint(1);
    collide.put_bytes(b"\x81");
    collide.put_uvarint(1);
    collide.put_bytes(b"2");
    let mut dec = Decbuf::new(collide.as_slice());
    let decoded = decode_labels(&mut dec).unwrap();
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded.get("\u{FFFD}"), "2");

    // 2. Live WAL replay: Go-written exemplar record with non-UTF-8 label value followed by valid
    // series + sample records must replay all subsequent records without truncation/corruption.
    {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let mut w = WlogWriter::open(&wal_dir, CompressionType::None).unwrap();
        let enc = RecordEncoder::new(false);

        w.log(&[&encode_raw_series_record(1, &[(b"__name__", b"first_m"), (b"job", b"api")])])
            .unwrap();
        // Exemplar with invalid UTF-8 bytes in trace_id value.
        let ex_rec = encode_raw_exemplar_record(1, 1_000, 1.0, &[(b"trace_id", b"bad\xff\xfeid")]);
        w.log(&[&ex_rec]).unwrap();

        // Subsequent series + sample record that MUST NOT be truncated!
        let s2 = Labels::from_pairs([("__name__", "after_non_utf8"), ("job", "api")]);
        let series_rec = enc.series(
            &[RefSeries {
                ref_id: 2,
                labels: s2,
            }],
            Vec::new(),
        );
        w.log(&[&series_rec]).unwrap();
        let samples_rec = enc.samples(
            &[
                RefSample {
                    ref_id: 1,
                    t: 1_000,
                    st: 0,
                    v: 10.0,
                },
                RefSample {
                    ref_id: 2,
                    t: 2_000,
                    st: 0,
                    v: 20.0,
                },
            ],
            Vec::new(),
        );
        w.log(&[&samples_rec]).unwrap();
        w.close().unwrap();

        let head = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();
        assert_eq!(head.num_series(), 2, "both series must survive WAL replay");
        assert!(head
            .get_entry_by_sorted_pairs(&[("__name__", "first_m"), ("job", "api")])
            .is_some());
        let e2 = head
            .get_entry_by_sorted_pairs(&[("__name__", "after_non_utf8"), ("job", "api")])
            .expect("series after non-UTF-8 exemplar must replay cleanly");
        let (_, chks, _) = head.series_chunks(e2.ref_id, 0, 10_000).unwrap().unwrap();
        assert_eq!(chks.len(), 1);
        head.close().unwrap();
    }

    // 3. Checkpoint replay: checkpoint.000000 containing non-UTF-8 exemplar label followed by
    // live WAL segment 00000001 must replay cleanly without HeadError::Repair.
    {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let ckpt_dir = wal_dir.join("checkpoint.000000");
        let mut cw = WlogWriter::open(&ckpt_dir, CompressionType::None).unwrap();
        cw.log(&[&encode_raw_series_record(1, &[(b"__name__", b"ckpt_m"), (b"job", b"api")])])
            .unwrap();
        cw.log(&[&encode_raw_exemplar_record(
            1,
            1_000,
            1.0,
            &[(b"trace_id", b"\xff\xfe")],
        )])
        .unwrap();
        cw.close().unwrap();

        let mut w = WlogWriter::open(&wal_dir, CompressionType::None).unwrap();
        w.next_segment().unwrap();
        let enc = RecordEncoder::new(false);
        let samples_rec = enc.samples(
            &[RefSample {
                ref_id: 1,
                t: 5_000,
                st: 0,
                v: 42.0,
            }],
            Vec::new(),
        );
        w.log(&[&samples_rec]).unwrap();
        w.close().unwrap();
        std::fs::remove_file(wal_dir.join("00000000")).unwrap();

        let head = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();
        let e1 = head
            .get_entry_by_sorted_pairs(&[("__name__", "ckpt_m"), ("job", "api")])
            .unwrap();
        let (_, chks, _) = head.series_chunks(e1.ref_id, 0, 10_000).unwrap().unwrap();
        assert_eq!(chks.len(), 1);
        head.close().unwrap();
    }
}

#[test]
fn w19_dtm_03_fast_regex_matcher_compile_limits_and_large_alternation() {
    // 1. \pL{1000} exceeds regex crate's default 10 MiB NFA size limit, allowed under Go's 128 MiB budget.
    let unicode_rep = FastRegexMatcher::new(r"\pL{1000}")
        .expect(r"\pL{1000} must compile with raised regex size limits");
    let matching_1000 = "a".repeat(1000);
    let too_short = "a".repeat(999);
    assert!(unicode_rep.is_match(&matching_1000));
    assert!(!unicode_rep.is_match(&too_short));

    // 2. >250 nested groups (up to 1000, matching Go maxHeight = 1000).
    let deep = format!("{}a{}", "(".repeat(300), ")".repeat(300));
    let deep_matcher = FastRegexMatcher::new(&deep)
        .expect("300 nested groups must compile with nest_limit(1000)");
    assert!(deep_matcher.is_match("a"));
    assert!(!deep_matcher.is_match("b"));

    // 3. Large escaped alternation (>256 branches) uses HIR Alternation -> build_multi_strategy
    // instead of hitting find_set_matches cap or NFA compiler.
    let mut branches = Vec::with_capacity(400);
    for i in 0..400 {
        branches.push(format!(r"10\.0\.{}\.{}:9100", i / 256, i % 256));
    }
    let pattern = branches.join("|");
    let large_alt = FastRegexMatcher::new(&pattern).expect("400-branch escaped alternation");
    assert!(
        large_alt.is_optimized(),
        "400-branch literal alternation must optimize via MultiSet without falling back to NFA Regex"
    );
    assert!(large_alt.is_match("10.0.0.1:9100"));
    assert!(large_alt.is_match("10.0.1.100:9100"));
    assert!(!large_alt.is_match("10.0.0.1:9200"));
    assert!(!large_alt.is_match("10x0x0x1:9100"));
}

#[test]
fn w19_rpc_01_head_reversed_time_range_and_ooo_tombstones() {
    let dir = tempfile::tempdir().unwrap();
    let opts = HeadOptions {
        chunk_range_ms: 10_000,
        ooo_time_window_ms: 100_000,
        ..HeadOptions::default()
    };
    let head = Head::open(dir.path(), opts.clone(), i64::MIN).unwrap();
    let pairs = [("__name__", "up"), ("job", "api")];

    let mut app = head.appender();
    let r = app.append_float(None, &pairs, 1_000, 1.0).unwrap();
    app.append_float(Some(r), &pairs, 2_000, 2.0).unwrap();
    app.commit().unwrap();

    // Reversed time range [1800, 1200] overlaps chunk [1000, 2000]:
    // 1000 <= 1200 && 1800 <= 2000 == true.
    assert!(head.series_has_chunks_in_range(r, 1_800, 1_200).is_some());
    let (_, chks, _) = head.series_chunks(r, 1_800, 1_200).unwrap().unwrap();
    assert_eq!(chks.len(), 1);
    assert_eq!((chks[0].min_time, chks[0].max_time), (1_000, 2_000));

    let (_, cnt, _) = head
        .series_lazy_chunk_count_with_snapshot(r, 1_800, 1_200, None)
        .unwrap()
        .unwrap();
    assert_eq!(cnt, 1);

    let (_, ivs, _) = head
        .series_chunk_metas_no_bytes_with_snapshot(r, 1_800, 1_200, None)
        .unwrap()
        .unwrap();
    assert_eq!(ivs.as_slice(), &[(1_000, 2_000)]);

    let q = TsdbQuerier::new(Some(&head), Vec::new(), 1_800, 1_200);
    let m = [LabelMatcher::new(MatchType::Equal, "__name__", "up").unwrap()];
    assert_eq!(q.select_raw_labels(true, &m).unwrap().len(), 1);
    assert_eq!(q.select_chunks(true, &m).unwrap().len(), 1);

    // Non-overlapping reversed range [2500, 2100] must still return nothing.
    assert!(head.series_has_chunks_in_range(r, 2_500, 2_100).is_none());
    assert!(head.series_chunks(r, 2_500, 2_100).unwrap().is_none());
    head.close().unwrap();

    // OOO chunk covered by a tombstone (replayed from WAL) must be filtered out by
    // series_has_chunks_in_range.
    let dir2 = tempfile::tempdir().unwrap();
    let enc = RecordEncoder::new(false);
    let mut wal_w = WlogWriter::open(dir2.path().join("wal"), CompressionType::None).unwrap();
    wal_w
        .log(&[&enc.series(
            &[RefSeries {
                ref_id: 1,
                labels: Labels::from_pairs([("__name__", "ooo_tomb"), ("job", "api")]),
            }],
            Vec::new(),
        )])
        .unwrap();
    wal_w
        .log(&[&enc.samples(
            &[RefSample {
                ref_id: 1,
                t: 200,
                st: 0,
                v: 1.0,
            }],
            Vec::new(),
        )])
        .unwrap();
    wal_w
        .log(&[&enc.tombstones(
            &[Tombstone {
                ref_id: 1,
                mint: 0,
                maxt: 1_000,
            }],
            Vec::new(),
        )])
        .unwrap();
    wal_w.close().unwrap();

    let mut wbl_w = WlogWriter::open(dir2.path().join("wbl"), CompressionType::None).unwrap();
    wbl_w
        .log(&[&enc.samples(
            &[RefSample {
                ref_id: 1,
                t: 500,
                st: 0,
                v: 2.0,
            }],
            Vec::new(),
        )])
        .unwrap();
    wbl_w.close().unwrap();

    let head2 = Head::open(dir2.path(), opts, i64::MIN).unwrap();
    let e_ooo = head2
        .get_entry_by_sorted_pairs(&[("__name__", "ooo_tomb"), ("job", "api")])
        .unwrap();
    assert!(
        head2
            .series_has_chunks_in_range(e_ooo.ref_id, 400, 600)
            .is_none(),
        "tombstoned OOO chunk must be filtered in series_has_chunks_in_range"
    );
    head2.close().unwrap();
}

#[test]
fn w19_rpc_02_querier_effective_head_past_max_time_and_empty_head() {
    let dir = tempfile::tempdir().unwrap();
    let head = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();
    let m = [LabelMatcher::new(MatchType::Equal, "__name__", "up").unwrap()];

    // Trigger B: empty Head with maxt == i64::MAX still queries Head and records stats.
    {
        let sink: BlockSelectStatsSink = Arc::default();
        let mut q = TsdbQuerier::new(Some(&head), Vec::new(), 0, i64::MAX);
        q.set_select_stats(Arc::clone(&sink), None);
        assert!(q.select_chunks(true, &m).unwrap().is_empty());
        let stats = sink.lock();
        assert_eq!(stats.len(), 1);
        assert!(stats[0].head);
        assert_eq!(stats[0].selected_postings, 0);
        assert_eq!(stats[0].series, 0);
        assert!(q.label_values("__name__", None, &[]).unwrap().is_empty());
        assert!(q.label_names(&[]).unwrap().is_empty());
    }

    // Trigger A: populated Head queried after head.max_time() ([2000, 5000]).
    {
        let mut app = head.appender();
        app.append_float(None, &[("__name__", "up"), ("job", "api")], 1_000, 1.0)
            .unwrap();
        app.commit().unwrap();
    }
    {
        let sink: BlockSelectStatsSink = Arc::default();
        let mut q = TsdbQuerier::new(Some(&head), Vec::new(), 2_000, 5_000);
        q.set_select_stats(Arc::clone(&sink), None);
        assert!(q.select_chunks(true, &m).unwrap().is_empty());
        let stats = sink.lock();
        assert_eq!(stats.len(), 1);
        assert!(stats[0].head);
        assert_eq!(stats[0].selected_postings, 1);
        assert_eq!(stats[0].series, 0);
        // LabelNames / LabelValues / SearchLabelValues must still use effective_in_order_head!
        assert!(q.label_values("__name__", None, &[]).unwrap().is_empty());
        assert!(q.label_names(&[]).unwrap().is_empty());
        assert!(q
            .search_label_values("__name__", &SearchHints::default(), &[])
            .unwrap()
            .is_empty());
    }

    head.close().unwrap();
}

#[test]
fn w19_rpc_04_empty_label_with_all_postings_and_other_matcher_errors() {
    let dir = tempfile::tempdir().unwrap();
    let head = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();
    {
        let mut app = head.appender();
        app.append_float(None, &[("__name__", "up"), ("job", "api")], 1_000, 1.0)
            .unwrap();
        app.commit().unwrap();
    }

    let all_m = LabelMatcher::new(MatchType::Equal, "", "").unwrap();
    let job_m = LabelMatcher::new(MatchType::Equal, "job", "api").unwrap();

    // Single [{}] for name="" succeeds with [""]
    assert_eq!(
        head.label_values("", std::slice::from_ref(&all_m)).unwrap(),
        vec![String::new()]
    );

    // [{}, {job="api"}] for name="" must fail with UnexpectedAllPostings!
    let err = head
        .label_values("", &[all_m.clone(), job_m.clone()])
        .expect_err("empty label name with [{}] + {job=\"api\"} must fail with UnexpectedAllPostings");
    assert!(
        matches!(
            err,
            mimir_tsdb::head::HeadError::Postings(PostingsError::UnexpectedAllPostings)
        ),
        "unexpected error: {err:?}"
    );

    // Same via TsdbQuerier (head-only and block-present paths).
    let q = TsdbQuerier::new(Some(&head), Vec::new(), 0, i64::MAX);
    assert!(q
        .label_values("", None, &[all_m.clone(), job_m.clone()])
        .is_err());

    head.close().unwrap();
}

#[test]
fn w19_rpc_05_search_invalid_threshold_message_format() {
    let hints = SearchHints {
        fuzz_threshold: -1,
        ..SearchHints::default()
    };
    assert_eq!(
        hints.validate().unwrap_err().to_string(),
        "fuzz threshold -1 out of [0,100]"
    );
}
