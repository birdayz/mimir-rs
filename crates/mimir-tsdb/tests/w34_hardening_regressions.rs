//! Regression tests for w34 TSDB hardening audit fixes (CRITICAL #1-#6, MEDIUM #1-#7).

use std::fs::OpenOptions;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use mimir_tsdb::chunkenc::{EncodedChunk, XorChunk};
use mimir_tsdb::compact::SampleValue;
use mimir_tsdb::head::{Head, HeadAppender, HeadOptions};
use mimir_tsdb::head_chunks::{
    head_chunk_file_path, ChunkDiskMapper, HeadChunksError, HeadChunksReader, HeadChunksWriter,
    WriteFaultAction,
};
use mimir_tsdb::isolation::TxRing;
use mimir_tsdb::labels::prom_regexp::{parse_go_syntax, FastRegexMatcher};
use mimir_tsdb::labels::size_of_labels;
use mimir_tsdb::matcher::{LabelMatcher, MatchType};
use mimir_tsdb::querier::TsdbQuerier;
use mimir_tsdb::wlog::{
    SegmentTracker, WlogError, WlogReader, MAX_WAL_RECORD_DECOMPRESSED_BYTES, PAGE_SIZE, REC_FIRST,
    REC_MIDDLE, REC_PAGE_TERM, RECORD_HEADER_SIZE,
};

fn append_float(app: &mut HeadAppender<'_>, ref_opt: Option<u64>, pairs: &[(&str, &str)], t: i64, v: f64) -> u64 {
    app.append_v2(ref_opt, pairs, 0, t, v, None, None, false, None, &[])
        .unwrap()
}

fn make_xor_chunk(seed: i64, samples: usize) -> EncodedChunk {
    let mut xc = XorChunk::new();
    {
        let mut app = xc.appender().unwrap();
        for i in 0..samples {
            let t = seed * 10_000 + (i as i64) * 15;
            let v = (seed as f64) + (i as f64) * 0.37 + 0.125;
            app.append(0, t, v).unwrap();
        }
    }
    EncodedChunk::Xor(xc)
}

fn metric_val(reg: &prometheus::Registry, name: &str) -> f64 {
    for mf in reg.gather() {
        if mf.get_name() == name {
            if let Some(m) = mf.get_metric().first() {
                if m.has_gauge() {
                    return m.get_gauge().get_value();
                }
                if m.has_counter() {
                    return m.get_counter().get_value();
                }
            }
        }
    }
    0.0
}

fn query_series(head: &Head, mint: i64, maxt: i64, metric: &str) -> Vec<(i64, f64)> {
    let q = TsdbQuerier::new(Some(head), vec![], mint, maxt);
    let m = LabelMatcher::new(MatchType::Equal, "__name__", metric).unwrap();
    let mut samples = Vec::new();
    for s in q.select(true, &[m]).unwrap() {
        for ds in s.samples {
            if let SampleValue::Float(v) = ds.value {
                samples.push((ds.t, v));
            }
        }
    }
    samples.sort_by_key(|&(t, _)| t);
    samples
}

// ============================================================================
// 1. CRITICAL #2 + MEDIUM #7: chunk_snapshot + load_mmapped_chunks restart
// ============================================================================

#[test]
fn w34_chunk_snapshot_flushes_sealed_chunks_and_attaches_mmapped_in_order_and_ooo_on_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let opts = HeadOptions {
        chunk_range_ms: 1000,
        ooo_time_window_ms: 100_000,
        ..HeadOptions::default()
    };

    {
        let h = Head::open_with_snapshot(dir.path(), opts.clone(), i64::MIN, true).unwrap();
        let pairs = [("__name__", "snap_m"), ("job", "t")];
        // Append enough in-order chunks to create sealed extra_head_chunks + active chunk,
        // WITHOUT manually calling mmap_head_chunks() first.
        for i in 0..5i64 {
            let mut app = h.appender();
            append_float(&mut app, None, &pairs, i * 2000 + 100, i as f64 + 1.0);
            append_float(&mut app, None, &pairs, i * 2000 + 500, i as f64 + 1.5);
            app.commit().unwrap();
        }
        // Also append an OOO sample and compact it into chunks_head so OOO mmapped chunks exist too.
        {
            let mut app = h.appender();
            append_float(&mut app, None, &pairs, 50, 99.0);
            app.commit().unwrap();
        }
        h.collect_ooo_compaction_candidates().unwrap();

        // Directly call chunk_snapshot() while sealed extra_head_chunks are still in memory:
        // must acquire chunk_snapshot_mtx before WAL position AND flush sealed chunks first.
        let snap = h.chunk_snapshot().unwrap();
        assert!(snap.is_some());
        // Drop without calling close() (simulates crash/restart with existing snapshot).
        std::mem::forget(h);
    }

    let reg = Arc::new(prometheus::Registry::new());
    let opts_reopen = HeadOptions {
        registry: Some(Arc::clone(&reg)),
        ..opts
    };
    let h2 = Head::open_with_snapshot(dir.path(), opts_reopen, i64::MIN, true).unwrap();
    assert_eq!(metric_val(&reg, "prometheus_tsdb_snapshot_replay_error_total"), 0.0);

    // Every in-order chunk (mmapped + snapshot active) AND OOO mmapped chunk must be present!
    let got = query_series(&h2, 0, 20_000, "snap_m");
    let expected: Vec<(i64, f64)> = vec![
        (50, 99.0),
        (100, 1.0),
        (500, 1.5),
        (2100, 2.0),
        (2500, 2.5),
        (4100, 3.0),
        (4500, 3.5),
        (6100, 4.0),
        (6500, 4.5),
        (8100, 5.0),
        (8500, 5.5),
    ];
    assert_eq!(got, expected);
    assert_eq!(h2.min_time(), 100);
    assert_eq!(h2.max_time(), 8500);
    assert_eq!(h2.min_ooo_time(), 50);
    assert_eq!(h2.max_ooo_time(), 50);
    assert!(metric_val(&reg, "prometheus_tsdb_head_chunks") >= 6.0);
    h2.close().unwrap();
}

#[test]
fn w34_snapshot_active_chunk_superseded_by_later_mmapped_chunk_drops_without_dup_or_error() {
    let dir = tempfile::tempdir().unwrap();
    let opts = HeadOptions {
        chunk_range_ms: 1000,
        ..HeadOptions::default()
    };

    let ref_id;
    {
        let h = Head::open_with_snapshot(dir.path(), opts.clone(), i64::MIN, true).unwrap();
        let pairs = [("__name__", "superseded_m")];
        {
            let mut app = h.appender();
            ref_id = append_float(&mut app, None, &pairs, 1000, 1.0);
            app.commit().unwrap();
        }
        // Snapshot captures active chunk [1000..=1000].
        h.chunk_snapshot().unwrap().unwrap();

        // Now advance the series so chunk [1000..=1000] is sealed & mmapped into chunks_head/
        // AFTER the snapshot was taken, without updating the snapshot.
        {
            let mut app = h.appender();
            append_float(&mut app, Some(ref_id), &pairs, 3000, 2.0);
            app.commit().unwrap();
        }
        h.mmap_head_chunks().unwrap();
        // Leak h so close() does not overwrite the older snapshot.
        std::mem::forget(h);
    }

    let reg = Arc::new(prometheus::Registry::new());
    let h2 = Head::open_with_snapshot(
        dir.path(),
        HeadOptions {
            registry: Some(Arc::clone(&reg)),
            ..opts
        },
        i64::MIN,
        true,
    )
    .unwrap();
    assert_eq!(metric_val(&reg, "prometheus_tsdb_mmap_chunk_corruptions_total"), 0.0);
    assert_eq!(metric_val(&reg, "prometheus_tsdb_snapshot_replay_error_total"), 0.0);

    let got = query_series(&h2, 0, 10_000, "superseded_m");
    assert_eq!(got, vec![(1000, 1.0), (3000, 2.0)]);
    // Active chunk from snapshot was dropped cleanly when mmapped chunk superseded it:
    // 1 mmapped chunk + 1 active chunk (from WAL tail at t=3000) = 2 chunks total.
    let (hc_cnt, mm_cnt) = h2.series_head_chunk_count_for_test(ref_id).unwrap();
    assert_eq!(mm_cnt, 1);
    assert_eq!(hc_cnt, 1);
    assert_eq!(metric_val(&reg, "prometheus_tsdb_head_chunks"), 2.0);
    h2.close().unwrap();
}

// ============================================================================
// 2. CRITICAL #1 + CRITICAL #5: active chunks_head non-Mmap + active OOM guard
// ============================================================================

#[test]
fn w34_head_chunks_reader_uses_active_pread_across_writes_rollback_and_cut() {
    let dir = tempfile::tempdir().unwrap();
    let chunk1 = make_xor_chunk(1, 2);
    let chunk2 = make_xor_chunk(2, 2);
    let chunk3 = make_xor_chunk(3, 2);

    let mut writer = HeadChunksWriter::with_options(dir.path(), 4096, 64).unwrap();
    let r1 = writer.write_chunk(10, 10_000, 10_015, &chunk1, false).unwrap();
    writer.flush().unwrap();

    // Open reader knowing seq=1 is active: it must read via ActiveReadFile, not Mmap.
    let mut reader =
        HeadChunksReader::open_no_repair_with_active(dir.path(), writer.active_file_seq()).unwrap();
    assert_eq!(reader.file_count(), 1);
    assert!(reader.has_file(1));
    assert_eq!(reader.max_seq(), 1);
    assert_eq!(reader.chunk(r1).unwrap(), chunk1);

    // Write chunk2 into the same active seq=1, flush, and read via ActiveReadFile without remapping.
    let r2 = writer.write_chunk(10, 20_000, 20_015, &chunk2, false).unwrap();
    writer.flush().unwrap();
    assert_eq!(reader.chunk(r2).unwrap(), chunk2);

    // Cut new file seq=2 (sealing seq=1), write chunk3 in seq=2, then refresh with new active seq.
    writer.cut_new_file().unwrap();
    let r3 = writer.write_chunk(10, 30_000, 30_015, &chunk3, false).unwrap();
    writer.flush().unwrap();

    reader
        .refresh_in_place_with_active(writer.active_file_seq())
        .unwrap();
    assert_eq!(reader.file_count(), 2);
    assert_eq!(reader.max_seq(), 2);
    assert_eq!(reader.chunk(r1).unwrap(), chunk1);
    assert_eq!(reader.chunk(r2).unwrap(), chunk2);
    assert_eq!(reader.chunk(r3).unwrap(), chunk3);
    assert_eq!(reader.collect_all_chunks().unwrap().len(), 3);
    writer.close().unwrap();
}

#[test]
fn w34_active_file_corrupt_uvarint_returns_corruption_without_oom() {
    // 1) HeadChunksReader active file path
    {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = HeadChunksWriter::with_options(dir.path(), 4096, 64).unwrap();
        let valid = make_xor_chunk(1, 2);
        let r1 = writer.write_chunk(1, 10_000, 10_015, &valid, false).unwrap();
        writer.flush().unwrap();

        let reader =
            HeadChunksReader::open_no_repair_with_active(dir.path(), writer.active_file_seq())
                .unwrap();
        assert_eq!(reader.chunk(r1).unwrap(), valid);

        // Overwrite the uvarint length of r1 on disk with a multi-GiB varint (0xff..0x0f).
        let p = head_chunk_file_path(dir.path(), 1);
        let (_, off) = r1.unpack();
        let mut f = OpenOptions::new().write(true).open(&p).unwrap();
        f.seek(SeekFrom::Start(off as u64 + 25)).unwrap();
        f.write_all(&[0xffu8, 0xff, 0xff, 0xff, 0x0f]).unwrap();
        f.flush().unwrap();

        let err = reader.chunk(r1).unwrap_err();
        assert!(
            matches!(err, HeadChunksError::Corruption { .. }),
            "expected Corruption, got {err:?}"
        );

        // Also test data_len = 1 (< 2)
        f.seek(SeekFrom::Start(off as u64 + 25)).unwrap();
        f.write_all(&[0x01u8]).unwrap();
        f.flush().unwrap();
        let err2 = reader.chunk(r1).unwrap_err();
        assert!(
            matches!(err2, HeadChunksError::Corruption { .. }),
            "expected Corruption for data_len=1, got {err2:?}"
        );
        writer.close().unwrap();
    }

    // 2) ChunkDiskMapper active file path (chunk >= write_buffer_size bypasses chunk_buffer
    //    and writes straight into active_file so chunk() exercises decode_chunk_from_active_file).
    {
        let dir = tempfile::tempdir().unwrap();
        let cdm = ChunkDiskMapper::open(dir.path(), 1024, 0, None).unwrap();
        let large = make_xor_chunk(7, 300);
        assert!(large.as_bytes().len() > 1024);
        let r = cdm.write_chunk(7, 70_000, 74_485, large, false, |res| res.unwrap());

        // Corrupt the uvarint length on disk in the active segment to ~4 GiB.
        let (seq, off) = r.unpack();
        let p = head_chunk_file_path(dir.path(), seq as usize);
        let mut f = OpenOptions::new().write(true).open(&p).unwrap();
        f.seek(SeekFrom::Start(off as u64 + 25)).unwrap();
        f.write_all(&[0xffu8, 0xff, 0xff, 0xff, 0x0f]).unwrap();
        f.flush().unwrap();

        let err = cdm.chunk(r).unwrap_err();
        assert!(
            matches!(err, HeadChunksError::Corruption { .. }),
            "expected Corruption from ChunkDiskMapper active file, got {err:?}"
        );
        cdm.close().unwrap();
    }
}

// ============================================================================
// 3. CRITICAL #3 + CRITICAL #4: FastRegexMatcher / parse_go_syntax hardening
// ============================================================================

#[test]
fn w34_fast_regex_matcher_repeat_leading_zeros_posix_unicode_and_surrogates() {
    // Leading zeros in {01}, {00}, a{02}, a{1,02} must be treated as literals, not repetitions.
    for (pat, yes, no) in [
        ("a{02}", vec!["a{02}"], vec!["aa", "a", "a{2}"]),
        ("a{00}", vec!["a{00}"], vec!["", "a"]),
        ("a{01}", vec!["a{01}"], vec!["a"]),
        ("a{1,02}", vec!["a{1,02}"], vec!["a", "aa"]),
    ] {
        let m = FastRegexMatcher::new(pat).unwrap();
        for s in yes {
            assert!(m.is_match(s), "{pat} should match {s:?}");
        }
        for s in no {
            assert!(!m.is_match(s), "{pat} must NOT match {s:?}");
        }
    }

    // All 14 positive + 14 negated POSIX classes inside [...]
    let posix_cases: &[(&str, &[&str], &[&str])] = &[
        ("[[:digit:]]", &["0", "9"], &["a", " "]),
        ("[[:^digit:]]", &["a", " ", "~"], &["0", "5", "9"]),
        ("[[:xdigit:]]", &["0", "9", "a", "f", "A", "F"], &["g", "G", " ", "~"]),
        ("[[:^xdigit:]]", &["g", "G", " ", "~"], &["0", "a", "F"]),
        ("[[:word:]]", &["0", "A", "_", "z"], &["-", " ", "!"]),
        ("[[:^word:]]", &["-", " ", "!"], &["0", "A", "_", "z"]),
        ("[[:blank:]]", &[" ", "\t"], &["\n", "a", "0"]),
        ("[[:^blank:]]", &["\n", "a", "0"], &[" ", "\t"]),
        ("[[:punct:]]", &["!", "/", ":", "@", "[", "`", "{", "~"], &["a", "0", " "]),
        ("[[:^punct:]]", &["a", "0", " "], &["!", "~", "["]),
        ("[[:cntrl:]]", &["\x00", "\x1f", "\x7f"], &[" ", "a"]),
        ("[[:^cntrl:]]", &[" ", "a", "~"], &["\x00", "\x1f", "\x7f"]),
        ("[[:graph:]]", &["!", "a", "~"], &[" ", "\x00", "\x7f"]),
        ("[[:^graph:]]", &[" ", "\x00", "\x7f"], &["!", "a", "~"]),
        ("[[:print:]]", &[" ", "!", "a", "~"], &["\x00", "\x1f", "\x7f"]),
        ("[[:^print:]]", &["\x00", "\x1f", "\x7f"], &[" ", "a", "~"]),
        ("[[:alpha:]]", &["A", "z"], &["0", "_", " "]),
        ("[[:^alpha:]]", &["0", "_", " "], &["A", "z"]),
        ("[[:alnum:]]", &["0", "A", "z"], &["_", " ", "!"]),
        ("[[:^alnum:]]", &["_", " ", "!"], &["0", "A", "z"]),
        ("[[:lower:]]", &["a", "z"], &["A", "0"]),
        ("[[:^lower:]]", &["A", "0"], &["a", "z"]),
        ("[[:upper:]]", &["A", "Z"], &["a", "0"]),
        ("[[:^upper:]]", &["a", "0"], &["A", "Z"]),
        ("[[:space:]]", &["\t", "\n", "\r", " "], &["a", "0"]),
        ("[[:^space:]]", &["a", "0"], &["\t", "\n", " "]),
        ("[[:ascii:]]", &["\x00", "a", "\x7f"], &["\u{0080}", "é"]),
        ("[[:^ascii:]]", &["\u{0080}", "é"], &["\x00", "a", "\x7f"]),
    ];
    for &(pat, yes, no) in posix_cases {
        let m = FastRegexMatcher::new(pat).unwrap();
        for s in yes {
            assert!(m.is_match(s), "{pat} should match {s:?}");
        }
        for s in no {
            assert!(!m.is_match(s), "{pat} must NOT match {s:?}");
        }
    }

    // Unicode classes inside [...] and outside must fall back cleanly to Regex (never wrong literal set)
    for pat in [r"[\p{Nd}]", r"[\pL]", r"[a\p{Greek}]", r"[\P{Nd}]"] {
        let m = FastRegexMatcher::new(pat).unwrap();
        assert!(
            m.set_matches().is_none(),
            "{pat} should not synthesize set_matches"
        );
    }
    let nd = FastRegexMatcher::new(r"[\p{Nd}]").unwrap();
    assert!(nd.is_match("5"));
    assert!(nd.is_match("٥")); // U+0665 Arabic-Indic digit five
    assert!(!nd.is_match("p"));
    assert!(!nd.is_match("{"));
    assert!(!nd.is_match("N"));

    // Surrogate range underflow (?i)[\x{D800}-\x{F000}] must not panic/underflow
    let surr = FastRegexMatcher::new(r"(?i)[\x{D800}-\x{F000}]").unwrap();
    assert!(surr.is_match("\u{E123}"));
    assert!(!surr.is_match("a"));

    // Direct parse_go_syntax malformed inputs return Err, never panic
    for bad in ["(", ")", "*a", "+a", "?a", "a{2,1}", "[abc", "\\"] {
        assert!(parse_go_syntax(bad).is_err(), "{bad:?} must fail cleanly");
    }
}

// ============================================================================
// 4. CRITICAL #6 + MEDIUM #5: WlogReader torn multi-fragment + page/size caps
// ============================================================================

struct MemSegmentReader {
    data: Vec<u8>,
    pos: usize,
}

impl Read for MemSegmentReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = (self.data.len() - self.pos).min(buf.len());
        buf[..n].copy_from_slice(&self.data[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

impl SegmentTracker for MemSegmentReader {
    fn tracker_info(&self) -> (Option<&Path>, Option<usize>, Option<usize>) {
        (None, Some(0), Some(self.pos))
    }
}

fn encode_wal_fragment(typ: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(RECORD_HEADER_SIZE + payload.len());
    out.push(typ);
    out.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    out.extend_from_slice(&crc32c::crc32c(payload).to_be_bytes());
    out.extend_from_slice(payload);
    out
}

#[test]
fn w34_wlog_reader_detects_torn_record_at_eof() {
    let _ = REC_PAGE_TERM;
    for second_typ in [None, Some(REC_MIDDLE)] {
        let mut stream = Vec::new();
        stream.extend_from_slice(&encode_wal_fragment(REC_FIRST, b"first-part"));
        if let Some(mid) = second_typ {
            stream.extend_from_slice(&encode_wal_fragment(mid, b"mid-part"));
        }

        let mut reader = WlogReader::from_reader(MemSegmentReader {
            data: stream,
            pos: 0,
        });
        let err = reader.next_raw_record().unwrap_err();
        assert!(
            matches!(err, WlogError::Corruption { ref message, .. } if message.contains("torn")),
            "expected torn record Corruption, got {err:?}"
        );
    }
}

#[test]
fn w34_wlog_reader_rejects_oversized_fragment_and_incremental_128mib_cap() {
    // 1) Fragment length > PAGE_SIZE - RECORD_HEADER_SIZE returns Corruption immediately.
    {
        let mut hdr = Vec::with_capacity(RECORD_HEADER_SIZE);
        hdr.push(1);
        hdr.extend_from_slice(&((PAGE_SIZE - RECORD_HEADER_SIZE + 1) as u16).to_be_bytes());
        hdr.extend_from_slice(&0u32.to_be_bytes());

        let mut reader = WlogReader::from_reader(MemSegmentReader {
            data: hdr,
            pos: 0,
        });
        let err = reader.next_raw_record().unwrap_err();
        assert!(
            matches!(err, WlogError::Corruption { .. }),
            "expected Corruption for oversized fragment, got {err:?}"
        );
    }

    // 2) Streaming > MAX_WAL_RECORD_DECOMPRESSED_BYTES across fragments fails incrementally
    // BEFORE buffering > 128 MiB and BEFORE REC_LAST arrives.
    struct InfiniteFragmentsReader {
        frag: Vec<u8>,
        frag_off: usize,
    }
    impl Read for InfiniteFragmentsReader {
        fn read(&mut self, mut buf: &mut [u8]) -> io::Result<usize> {
            let mut written = 0;
            while !buf.is_empty() {
                if self.frag_off >= self.frag.len() {
                    self.frag_off = 0;
                    self.frag[0] = REC_MIDDLE;
                }
                let n = (self.frag.len() - self.frag_off).min(buf.len());
                buf[..n].copy_from_slice(&self.frag[self.frag_off..self.frag_off + n]);
                self.frag_off += n;
                written += n;
                buf = &mut buf[n..];
            }
            Ok(written)
        }
    }
    impl SegmentTracker for InfiniteFragmentsReader {
        fn tracker_info(&self) -> (Option<&Path>, Option<usize>, Option<usize>) {
            (None, Some(0), None)
        }
    }

    let max_frag_payload = PAGE_SIZE - RECORD_HEADER_SIZE;
    let payload = vec![0xABu8; max_frag_payload];
    let frag = encode_wal_fragment(REC_FIRST, &payload);
    let mut reader = WlogReader::from_reader(InfiniteFragmentsReader { frag, frag_off: 0 });
    let err = reader.next_raw_record().unwrap_err();
    assert!(
        matches!(err, WlogError::Corruption { ref message, .. } if message.contains("maximum")),
        "expected Corruption on incremental 128MiB cap, got {err:?}"
    );
    let _ = MAX_WAL_RECORD_DECOMPRESSED_BYTES;
}

// ============================================================================
// 5. MEDIUM #1, #2, #3, #4: truncate_memory race, mmap rollback, TxRing, labels
// ============================================================================

#[test]
fn w34_truncate_memory_does_not_overshoot_concurrent_append_sample() {
    use std::sync::Barrier;

    let dir = tempfile::tempdir().unwrap();
    let opts = HeadOptions {
        chunk_range_ms: 4000,
        ..HeadOptions::default()
    };
    let head = Arc::new(Head::open(dir.path(), opts, i64::MIN).unwrap());

    // Seed initial series across many stripes so gc_collect_and_unlink takes measurable work.
    {
        let mut app = head.appender();
        for i in 0..200 {
            let name = format!("seed_s_{i}");
            append_float(&mut app, None, &[("__name__", name.as_str())], 5000, 1.0);
        }
        app.commit().unwrap();
    }

    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(3));

    // Writer 1: repeatedly commits samples starting with pre-truncation timestamps (t=2500..=3500)
    // concurrently with truncate_memory(2000..=4000).
    let h1 = Arc::clone(&head);
    let s1 = Arc::clone(&stop);
    let b1 = Arc::clone(&barrier);
    let committed_min_after_2000 = Arc::new(std::sync::atomic::AtomicI64::new(i64::MAX));
    let cm1 = Arc::clone(&committed_min_after_2000);
    let t1 = std::thread::spawn(move || {
        b1.wait();
        for i in 0..100i64 {
            let t = 2500 + (i % 5) * 100;
            let name = format!("conc_low_{i}");
            let mut app = h1.appender();
            if app
                .append_v2(None, &[("__name__", name.as_str())], 0, t, i as f64, None, None, false, None, &[])
                .is_ok()
                && app.commit().is_ok()
            {
                cm1.fetch_min(t, Ordering::AcqRel);
            }
        }
        s1.store(true, Ordering::Release);
    });

    // Writer 2: advances max_time monotonically so appendable_min_valid_time() moves upward.
    let h2 = Arc::clone(&head);
    let s2 = Arc::clone(&stop);
    let b2 = Arc::clone(&barrier);
    let t2 = std::thread::spawn(move || {
        b2.wait();
        let mut t = 5500i64;
        while !s2.load(Ordering::Acquire) {
            let mut app = h2.appender();
            let _ = app.append_v2(
                None,
                &[("__name__", "conc_high")],
                0,
                t,
                t as f64,
                None,
                None,
                false,
                None,
                &[],
            );
            let _ = app.commit();
            t += 100;
        }
    });

    barrier.wait();
    while !stop.load(Ordering::Acquire) {
        head.truncate_memory(2000).unwrap();
    }
    t1.join().unwrap();
    t2.join().unwrap();

    let min_committed = committed_min_after_2000.load(Ordering::Acquire);
    if min_committed != i64::MAX {
        assert!(
            head.min_time() <= min_committed,
            "head.min_time()={} overshot concurrently committed sample t={}",
            head.min_time(),
            min_committed
        );
    }
    head.close().unwrap();
}

#[test]
fn w34_mmap_head_chunks_partial_failure_rolls_back_without_duplicate_chunks() {
    let dir = tempfile::tempdir().unwrap();
    let opts = HeadOptions {
        chunk_range_ms: 1000,
        max_head_chunk_file_size: 160,
        head_chunks_write_buffer_size: 64,
        ..HeadOptions::default()
    };
    {
        let head = Head::open(dir.path(), opts.clone(), i64::MIN).unwrap();
        for i in 0..3i64 {
            for s in 0..4 {
                let m = format!("mmap_fail_s_{s}");
                let mut app = head.appender();
                append_float(
                    &mut app,
                    None,
                    &[("__name__", m.as_str())],
                    i * 2000 + 100,
                    (i + s) as f64,
                );
                app.commit().unwrap();
            }
        }

        // Fail on the 3rd data write (after earlier series chunks were written & flushed across files).
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_c = Arc::clone(&calls);
        head.set_chunk_writer_write_fault_hook_for_test(Some(Arc::new(move |len| {
            if len > 8 {
                let n = calls_c.fetch_add(1, Ordering::AcqRel);
                if n >= 2 {
                    return WriteFaultAction::Error(io::ErrorKind::StorageFull);
                }
            }
            WriteFaultAction::PassThrough
        })));

        assert!(head.mmap_head_chunks().is_err());
        head.set_chunk_writer_write_fault_hook_for_test(None);

        // Retry must succeed and never produce duplicate chunks in chunks_head!
        let mmapped = head.mmap_head_chunks().unwrap();
        assert_eq!(mmapped, 8);
        head.close().unwrap();
    }

    let reg = Arc::new(prometheus::Registry::new());
    let reopened = Head::open(
        dir.path(),
        HeadOptions {
            registry: Some(Arc::clone(&reg)),
            ..opts
        },
        i64::MIN,
    )
    .unwrap();
    assert_eq!(metric_val(&reg, "prometheus_tsdb_mmap_chunk_corruptions_total"), 0.0);
    for s in 0..4 {
        let m = format!("mmap_fail_s_{s}");
        assert_eq!(
            query_series(&reopened, 0, 10_000, &m),
            vec![
                (100, s as f64),
                (2100, (1 + s) as f64),
                (4100, (2 + s) as f64),
            ]
        );
    }
    reopened.close().unwrap();
}

#[test]
fn w34_tx_ring_shrinks_and_delete_series_entries_owned_removes_txs() {
    // 1) TxRing shrink when len > 64 && count * 4 < len
    let mut ring = TxRing::new(8);
    for id in 1..=300u64 {
        ring.add(id);
    }
    assert!(ring.capacity() >= 256);
    ring.cleanup_append_ids_below(295);
    assert_eq!(ring.len(), 6);
    assert!(
        ring.capacity() <= 64,
        "expected TxRing to shrink below 64, got {}",
        ring.capacity()
    );
    let mut iter = ring.iterator();
    let mut collected = Vec::new();
    for _ in 0..ring.len() {
        collected.push(iter.at());
        iter.next_pos();
    }
    assert_eq!(collected, vec![295, 296, 297, 298, 299, 300]);

    // 2) delete_series_entries_owned removes entry from series_txs
    let dir = tempfile::tempdir().unwrap();
    let head = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();
    let ref_id = {
        let mut app = head.appender();
        let r = append_float(&mut app, None, &[("__name__", "tx_del")], 1000, 1.0);
        app.commit().unwrap();
        r
    };
    assert!(head.txs_contains_for_test(ref_id));
    head.delete_series_entry_for_test(ref_id);
    assert!(!head.txs_contains_for_test(ref_id));
    head.close().unwrap();
}

#[test]
fn w34_size_of_labels_rejects_exact_1_shl_24_boundary() {
    let max_ok = "a".repeat((1 << 24) - 1);
    assert!(size_of_labels(&max_ok, "v", 1) > 0);
    let too_long = "a".repeat(1 << 24);
    let res = std::panic::catch_unwind(|| size_of_labels(&too_long, "v", 1));
    assert!(res.is_err(), "len == 1 << 24 must panic as unencodable");
}
