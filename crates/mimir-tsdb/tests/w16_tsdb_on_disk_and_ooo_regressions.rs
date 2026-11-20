//! Wave 16 regression tests for OEM-01..03 and W16-ICW-01..05.

use std::fs;
use std::io::{self, Read};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use mimir_tsdb::block::Block;
use mimir_tsdb::chunkenc::{EncodedChunk, Encoding, Xor2Chunk, XorChunk};
use mimir_tsdb::chunks_block::ChunkMeta;
use mimir_tsdb::compact::{
    compact_blocks, compact_blocks_with_float_encoding, compact_chunk_series_merge,
    decode_chunk_samples, write_block_from_series, CompactionSeries, SampleValue,
    SERIES_TO_CHUNK_ENCODER_SPLIT,
};
use mimir_tsdb::db::{Tsdb, TsdbOptions};
use mimir_tsdb::encoding::{Decbuf, Encbuf, EncodingError};
use mimir_tsdb::head::{Head, HeadOptions};
use mimir_tsdb::head_chunks::WriteFaultAction;
use mimir_tsdb::index::{IndexError, IndexReader, IndexWriter};
use mimir_tsdb::labels::{decode_stringlabel_size, encode_stringlabel_size, Labels};
use mimir_tsdb::matcher::{LabelMatcher, MatchType};
use mimir_tsdb::querier::TsdbQuerier;
use mimir_tsdb::record::decode_labels;
use mimir_tsdb::search::SearchHints;
use mimir_tsdb::wlog::{CompressionType, SegmentBufReader, WlogReader, WlogWriter, PAGE_SIZE};

fn metric_value(reg: &prometheus::Registry, name: &str, labels: &[(&str, &str)]) -> f64 {
    for mf in reg.gather() {
        if mf.get_name() == name {
            for m in mf.get_metric() {
                let mut matched = true;
                for &(lk, lv) in labels {
                    if !m
                        .get_label()
                        .iter()
                        .any(|lp| lp.get_name() == lk && lp.get_value() == lv)
                    {
                        matched = false;
                        break;
                    }
                }
                if matched {
                    return m.get_counter().get_value() + m.get_gauge().get_value();
                }
            }
        }
    }
    0.0
}

// -----------------------------------------------------------------------------
// OEM-01: HeadActiveReads::wait_for_range only waits for pre-existing readers.
// -----------------------------------------------------------------------------

#[test]
fn oem_01_wait_for_range_does_not_starve_on_post_cutoff_readers() {
    let dir = tempfile::tempdir().unwrap();
    let opts = HeadOptions {
        chunk_range_ms: 10_000,
        ooo_time_window_ms: 60_000,
        ooo_cap_max: 2,
        ..HeadOptions::default()
    };
    let head = Arc::new(Head::open(dir.path(), opts, i64::MIN).unwrap());
    let pairs = [("__name__", "oem01_metric"), ("job", "api")];

    // Establish in-order high-water mark so earlier samples go to OOO.
    {
        let mut app = head.appender();
        app.append_float(None, &pairs, 50_000, 1.0).unwrap();
        app.commit().unwrap();
    }
    // Insert 3 OOO samples with ooo_cap_max=2 so 1 OOO chunk is mmapped.
    {
        let mut app = head.appender();
        for t in [1_000i64, 2_000, 3_000] {
            app.append_float(None, &pairs, t, t as f64).unwrap();
        }
        app.commit().unwrap();
    }

    // Start pre-existing reader overlapping the OOO range.
    let pre_guard = head.start_read(500, 10_000);

    let done = Arc::new(AtomicBool::new(false));
    let done_c = Arc::clone(&done);
    let head_c = Arc::clone(&head);
    let handle = std::thread::spawn(move || {
        head_c.clear_ooo_data_before(4_000).unwrap();
        done_c.store(true, Ordering::Release);
    });

    // Give the background thread time to block in wait_for_range on pre_guard.
    std::thread::sleep(Duration::from_millis(50));
    assert!(
        !done.load(Ordering::Acquire),
        "clear_ooo_data_before must wait for pre-existing reader"
    );

    // Start a post-cutoff overlapping reader BEFORE dropping pre_guard!
    let post_guard = head.start_read(500, 10_000);
    // Drop only the pre-existing reader while post_guard stays alive.
    drop(pre_guard);

    let deadline = Instant::now() + Duration::from_secs(3);
    while !done.load(Ordering::Acquire) && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        done.load(Ordering::Acquire),
        "wait_for_range must not starve on post-cutoff overlapping reader"
    );

    drop(post_guard);
    handle.join().unwrap();
    head.close().unwrap();
}

fn wait_until(what: &str, cond: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(3);
    while !cond() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// A clone of a reader registered before the cutoff is still that reader: the waiter must not
/// treat it as a new, unprotected one.
#[test]
fn oem_01_clone_of_pre_cutoff_reader_is_still_waited_for() {
    let dir = tempfile::tempdir().unwrap();
    let opts = HeadOptions {
        chunk_range_ms: 10_000,
        ooo_time_window_ms: 60_000,
        ooo_cap_max: 2,
        ..HeadOptions::default()
    };
    let head = Arc::new(Head::open(dir.path(), opts, i64::MIN).unwrap());
    let pairs = [("__name__", "oem01_clone_metric"), ("job", "api")];
    {
        let mut app = head.appender();
        app.append_float(None, &pairs, 50_000, 1.0).unwrap();
        app.commit().unwrap();
    }
    {
        let mut app = head.appender();
        for t in [1_000i64, 2_000, 3_000] {
            app.append_float(None, &pairs, t, t as f64).unwrap();
        }
        app.commit().unwrap();
    }

    let pre_guard = head.start_read(500, 10_000);
    let done = Arc::new(AtomicBool::new(false));
    let done_c = Arc::clone(&done);
    let head_c = Arc::clone(&head);
    let handle = std::thread::spawn(move || {
        head_c.clear_ooo_data_before(4_000).unwrap();
        done_c.store(true, Ordering::Release);
    });
    std::thread::sleep(Duration::from_millis(50));
    assert!(!done.load(Ordering::Acquire));

    let cloned_guard = pre_guard.clone();
    drop(pre_guard);
    std::thread::sleep(Duration::from_millis(100));
    assert!(
        !done.load(Ordering::Acquire),
        "clear_ooo_data_before must wait for the clone of a pre-existing reader"
    );
    drop(cloned_guard);
    wait_until("clear_ooo_data_before", || done.load(Ordering::Acquire));

    handle.join().unwrap();
    head.close().unwrap();
}

/// A reader registered while `truncate_memory` waits for older readers still sees the old
/// `min_time`, so a lazy `QueryStream` may already have announced chunks below the truncation
/// time. The GC must not drop them before that reader is done, while readers registered after
/// `min_time` is published are clamped and never delay the truncation.
#[test]
fn oem_01_truncate_memory_protects_readers_registered_during_the_wait() {
    let dir = tempfile::tempdir().unwrap();
    let opts = HeadOptions {
        chunk_range_ms: 10_000,
        ..HeadOptions::default()
    };
    let head = Arc::new(Head::open(dir.path(), opts, i64::MIN).unwrap());
    let pairs = [("__name__", "oem01_truncate_metric"), ("job", "api")];
    let ref_id = {
        let mut app = head.appender();
        let r = app.append_float(None, &pairs, 1_000, 1.0).unwrap();
        app.append_float(Some(r), &pairs, 2_000, 2.0).unwrap();
        app.commit().unwrap();
        r
    };
    {
        // Cuts the [0, 10_000) chunk.
        let mut app = head.appender();
        app.append_float(Some(ref_id), &pairs, 11_000, 3.0).unwrap();
        app.append_float(Some(ref_id), &pairs, 12_000, 4.0).unwrap();
        app.commit().unwrap();
    }
    assert_eq!(head.min_time(), 1_000);

    let pre_guard = head.start_read(0, 5_000);
    let done = Arc::new(AtomicBool::new(false));
    let done_c = Arc::clone(&done);
    let head_c = Arc::clone(&head);
    let handle = std::thread::spawn(move || {
        head_c.truncate_memory(10_000).unwrap();
        done_c.store(true, Ordering::Release);
    });
    std::thread::sleep(Duration::from_millis(50));
    assert!(!done.load(Ordering::Acquire), "truncate_memory must wait for pre_guard");

    // Registered during the wait: phase 1 counts both chunks.
    let mid_guard = head.start_read(0, 20_000);
    let (_, mid_count, mid_snapshot) = head
        .series_lazy_chunk_count_with_snapshot(ref_id, 0, 20_000, None)
        .unwrap()
        .expect("series in range");
    assert_eq!(mid_count, 2);

    drop(pre_guard);
    wait_until("min_time to be published", || head.min_time() == 10_000);

    // Registered after min_time was published: clamped to the truncation time.
    let late_guard = head.start_read(0, 20_000);
    let (_, late_count, late_snapshot) = head
        .series_lazy_chunk_count_with_snapshot(ref_id, 0, 20_000, None)
        .unwrap()
        .expect("series in range");
    assert_eq!(late_count, 1);

    std::thread::sleep(Duration::from_millis(100));
    assert!(
        !done.load(Ordering::Acquire),
        "the GC must wait for the reader registered during the first wait"
    );
    let mid_chunks = head
        .series_final_chunks_from_snapshot(None, ref_id, mid_snapshot)
        .unwrap();
    assert_eq!(mid_chunks.len(), mid_count as usize);

    // The late reader stays open: it must not hold the truncation back.
    drop(mid_guard);
    wait_until("truncate_memory", || done.load(Ordering::Acquire));
    let late_chunks = head
        .series_final_chunks_from_snapshot(None, ref_id, late_snapshot)
        .unwrap();
    assert_eq!(late_chunks.len(), late_count as usize);
    assert_eq!(late_chunks[0].min_time, 11_000);

    drop(late_guard);
    handle.join().unwrap();
    head.close().unwrap();
}

// -----------------------------------------------------------------------------
// OEM-02: Best-effort WBL logging + unconditional OOO commit tail.
// -----------------------------------------------------------------------------

#[test]
fn oem_02_wbl_error_is_best_effort_and_partial_ooo_commit_still_logs_wbl() {
    // Part A: WBL write failure does NOT fail commit (Go head_append.go:1994-2001).
    {
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            chunk_range_ms: 10_000,
            ooo_time_window_ms: 60_000,
            ooo_cap_max: 8,
            ..HeadOptions::default()
        };
        let head = Head::open(dir.path(), opts, i64::MIN).unwrap();
        let pairs = [("__name__", "oem02_wbl_err"), ("job", "api")];

        {
            let mut app = head.appender();
            app.append_float(None, &pairs, 50_000, 1.0).unwrap();
            app.commit().unwrap();
        }

        head.set_wbl_write_fault_hook_for_test(Some(Arc::new(|_len| {
            WriteFaultAction::Error(io::ErrorKind::StorageFull)
        })));

        {
            let mut app = head.appender();
            app.append_float(None, &pairs, 1_000, 42.0).unwrap();
            app.commit()
                .expect("WBL failure must be best-effort and not fail commit");
        }
        head.set_wbl_write_fault_hook_for_test(None);

        let entry = head.get_entry_by_sorted_pairs(&pairs).unwrap();
        let (_, chks) = head
            .series_final_chunks(entry.ref_id, i64::MIN, i64::MAX)
            .unwrap()
            .unwrap();
        let got: Vec<(i64, f64)> = chks
            .iter()
            .flat_map(|c| decode_chunk_samples(c.chunk.as_ref().unwrap()).unwrap())
            .map(|s| match s.value {
                SampleValue::Float(v) => (s.t, v),
                _ => panic!("expected float"),
            })
            .collect();
        assert!(got.contains(&(1_000, 42.0)));
        assert_eq!(head.max_ooo_time(), 1_000);
        head.close().unwrap();
    }

    // Part B: Partial OOO commit where S1 inserts into ooo_head_chunk and S2 fails on chunk cut
    // (ENOSPC) MUST still flush S1 to WBL and update OOO bounds, so crash/restart preserves S1!
    {
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            chunk_range_ms: 10_000,
            ooo_time_window_ms: 60_000,
            ooo_cap_max: 2,
            ..HeadOptions::default()
        };
        let s1 = [("__name__", "oem02_s1"), ("job", "api")];
        let s2 = [("__name__", "oem02_s2"), ("job", "api")];

        {
            let head = Head::open(dir.path(), opts.clone(), i64::MIN).unwrap();
            // Establish in-order high-water mark for both series, and fill s2's OOO head chunk to cap (2).
            {
                let mut app = head.appender();
                app.append_float(None, &s1, 50_000, 1.0).unwrap();
                app.append_float(None, &s2, 50_000, 1.0).unwrap();
                app.append_float(None, &s2, 1_000, 10.0).unwrap();
                app.append_float(None, &s2, 2_000, 20.0).unwrap();
                app.commit().unwrap();
            }

            // Inject ENOSPC on chunk_writer so s2 cutting its full OOO head chunk fails.
            head.set_chunk_writer_write_fault_hook_for_test(Some(Arc::new(|_len| {
                WriteFaultAction::Error(io::ErrorKind::StorageFull)
            })));

            // Single commit: s1 inserts OOO (succeeds in memory) BEFORE s2 hits ENOSPC!
            {
                let mut app = head.appender();
                app.append_float(None, &s1, 3_000, 33.0).unwrap();
                app.append_float(None, &s2, 4_000, 44.0).unwrap();
                assert!(app.commit().is_err());
            }

            head.set_chunk_writer_write_fault_hook_for_test(None);
            // OOO bounds must reflect s1's OOO insert (3_000) despite s2's error.
            assert!(head.max_ooo_time() >= 3_000);
            assert!(head.min_ooo_time() <= 3_000);
            head.close().unwrap();
        }

        // Reopen Head from WAL + WBL: s1's OOO sample (3_000, 33.0) MUST be recovered from WBL!
        let head2 = Head::open(dir.path(), opts, i64::MIN).unwrap();
        let entry1 = head2.get_entry_by_sorted_pairs(&s1).unwrap();
        let (_, chks1) = head2
            .series_final_chunks(entry1.ref_id, i64::MIN, i64::MAX)
            .unwrap()
            .unwrap();
        let got1: Vec<(i64, f64)> = chks1
            .iter()
            .flat_map(|c| decode_chunk_samples(c.chunk.as_ref().unwrap()).unwrap())
            .map(|s| match s.value {
                SampleValue::Float(v) => (s.t, v),
                _ => panic!("expected float"),
            })
            .collect();
        assert!(
            got1.contains(&(3_000, 33.0)),
            "s1 OOO sample must be logged to WBL and survive restart: {got1:?}"
        );
        head2.close().unwrap();
    }
}

// -----------------------------------------------------------------------------
// OEM-03: truncate_ooo / clear_ooo_data_before must never lower max_ooo_time.
// -----------------------------------------------------------------------------

#[test]
fn oem_03_truncate_ooo_and_clear_ooo_never_lower_max_ooo_time() {
    let dir = tempfile::tempdir().unwrap();
    let opts = HeadOptions {
        chunk_range_ms: 10_000,
        ooo_time_window_ms: 60_000,
        ooo_cap_max: 2,
        ..HeadOptions::default()
    };
    let head = Head::open(dir.path(), opts, i64::MIN).unwrap();
    let pairs = [("__name__", "oem03_metric"), ("job", "api")];

    {
        let mut app = head.appender();
        app.append_float(None, &pairs, 50_000, 1.0).unwrap();
        app.commit().unwrap();
    }
    {
        let mut app = head.appender();
        app.append_float(None, &pairs, 1_000, 1.0).unwrap();
        app.append_float(None, &pairs, 2_000, 2.0).unwrap();
        app.append_float(None, &pairs, 5_000, 5.0).unwrap();
        app.commit().unwrap();
    }
    assert_eq!(head.max_ooo_time(), 5_000);

    // Drain all OOO data via clear_ooo_data_before: max_ooo_time must NOT drop to i64::MIN.
    head.clear_ooo_data_before(5_000).unwrap();
    assert_eq!(
        head.max_ooo_time(),
        5_000,
        "clear_ooo_data_before must never lower max_ooo_time"
    );

    // Now test truncate_ooo directly with an mmapped OOO chunk at t=6_000..7_000.
    {
        let mut app = head.appender();
        app.append_float(None, &pairs, 6_000, 6.0).unwrap();
        app.append_float(None, &pairs, 7_000, 7.0).unwrap();
        app.append_float(None, &pairs, 8_000, 8.0).unwrap();
        app.commit().unwrap();
    }
    assert_eq!(head.max_ooo_time(), 8_000);
    let max_ref = mimir_tsdb::head_chunks::ChunkDiskMapperRef::new(u32::MAX, u32::MAX);
    head.truncate_ooo(None, max_ref).unwrap();
    assert_eq!(
        head.max_ooo_time(),
        8_000,
        "truncate_ooo must never lower max_ooo_time"
    );

    head.close().unwrap();
}

// -----------------------------------------------------------------------------
// W16-ICW-01: 1 << 24 stringlabels bound & >= 16 MiB symbol/label rejection.
// -----------------------------------------------------------------------------

#[test]
fn w16_icw_01_encode_stringlabel_size_max_and_reject_16mib() {
    // Max valid 24-bit unsigned size: (1 << 24) - 1 = 16_777_215.
    let max_valid = (1 << 24) - 1;
    let mut buf = Vec::new();
    encode_stringlabel_size(&mut buf, max_valid);
    assert_eq!(buf, vec![255, 0xff, 0xff, 0xff]);
    let (dec_len, next) = decode_stringlabel_size(&buf, 0);
    assert_eq!(dec_len, max_valid);
    assert_eq!(next, 4);

    // Panics at 1 << 24 instead of wrapping to [255, 0, 0, 0].
    let res = std::panic::catch_unwind(|| {
        let mut b = Vec::new();
        encode_stringlabel_size(&mut b, 1 << 24);
    });
    assert!(res.is_err(), "encode_stringlabel_size must panic at 1 << 24");

    // IndexReader::open must reject a >= 16 MiB symbol cleanly with IndexError::Corrupt.
    let dir = tempfile::tempdir().unwrap();
    let idx_path = dir.path().join("index");
    let huge_sym = "é".repeat(1 << 23); // 16_777_216 bytes (1 << 24)
    assert_eq!(huge_sym.len(), 1 << 24);
    {
        let mut w = IndexWriter::new(&idx_path).unwrap();
        w.add_symbol("").unwrap();
        w.add_symbol("__name__").unwrap();
        w.add_symbol(&huge_sym).unwrap();
        w.close().unwrap();
    }
    let open_res = IndexReader::open(&idx_path);
    assert!(
        matches!(open_res, Err(IndexError::Corrupt(_))),
        "expected IndexError::Corrupt for 16 MiB symbol"
    );

    // record::decode_labels must reject a >= 16 MiB label string cleanly with EncodingError::InvalidSize.
    let mut enc = Encbuf::new();
    enc.put_uvarint(1);
    enc.put_uvarint_str("k");
    enc.put_uvarint_str(&huge_sym);
    let mut dec = Decbuf::new(enc.as_slice());
    let dec_res = decode_labels(&mut dec);
    assert!(
        matches!(dec_res, Err(EncodingError::InvalidSize)),
        "expected EncodingError::InvalidSize, got {dec_res:?}"
    );
}

// -----------------------------------------------------------------------------
// W16-ICW-02: CompactingChunkSeriesMerger + seriesToChunkEncoder 1:1 parity.
// -----------------------------------------------------------------------------

#[test]
fn w16_icw_02_compacting_chunk_series_merger_tie_break_dedup_and_120_split() {
    let dir = tempfile::tempdir().unwrap();
    let lbls = Labels::from_pairs([("__name__", "icw02_metric"), ("job", "api")]);

    // Case 1: Tie-break & (min_time, max_time) sort order in compact_blocks + TsdbQuerier.
    // Block A: [150, 200] -> (150, 1.0), (200, 1.9)
    // Block B: [150, 300] -> (150, 2.0), (300, 3.0)
    // c0 = [150, 200] sorts before c1 = [150, 300] and is passed LAST to chain_sample_merge,
    // so (150, 1.0) wins!
    let mut ca = XorChunk::new();
    {
        let mut app = ca.appender().unwrap();
        app.append(0, 150, 1.0).unwrap();
        app.append(0, 200, 1.9).unwrap();
    }
    let mut cb = XorChunk::new();
    {
        let mut app = cb.appender().unwrap();
        app.append(0, 150, 2.0).unwrap();
        app.append(0, 300, 3.0).unwrap();
    }

    let meta_a = write_block_from_series(
        dir.path(),
        100,
        400,
        vec![CompactionSeries {
            labels: lbls.clone(),
            chunks: vec![ChunkMeta {
                ref_id: 0,
                min_time: 150,
                max_time: 200,
                chunk: Some(EncodedChunk::Xor(ca)),
            }],
            tombstones: Vec::new(),
        }],
        Default::default(),
        false,
        0,
    )
    .unwrap()
    .unwrap();
    let meta_b = write_block_from_series(
        dir.path(),
        100,
        400,
        vec![CompactionSeries {
            labels: lbls.clone(),
            chunks: vec![ChunkMeta {
                ref_id: 0,
                min_time: 150,
                max_time: 300,
                chunk: Some(EncodedChunk::Xor(cb)),
            }],
            tombstones: Vec::new(),
        }],
        Default::default(),
        false,
        0,
    )
    .unwrap()
    .unwrap();

    let blk_a = Arc::new(Block::open(dir.path().join(meta_a.ulid.to_string())).unwrap());
    let blk_b = Arc::new(Block::open(dir.path().join(meta_b.ulid.to_string())).unwrap());

    // Multi-block TsdbQuerier (!unordered)
    let q = TsdbQuerier::new(None, vec![Arc::clone(&blk_b), Arc::clone(&blk_a)], 100, 400);
    let q_res = q
        .select_chunks_with_mode(
            true,
            &[LabelMatcher::new(MatchType::Equal, "__name__", "icw02_metric").unwrap()],
            false,
        )
        .unwrap();
    assert_eq!(q_res.len(), 1);
    let q_samples: Vec<(i64, f64)> = q_res[0]
        .chunks
        .iter()
        .flat_map(|c| decode_chunk_samples(c.chunk.as_ref().unwrap()).unwrap())
        .map(|s| match s.value {
            SampleValue::Float(v) => (s.t, v),
            _ => panic!("expected float"),
        })
        .collect();
    assert_eq!(q_samples, vec![(150, 1.0), (200, 1.9), (300, 3.0)]);

    // compact_blocks
    let out_dir = tempfile::tempdir().unwrap();
    let merged_meta = compact_blocks(out_dir.path(), &[blk_b.as_ref(), blk_a.as_ref()])
        .unwrap()
        .unwrap();
    let blk_m = Block::open(out_dir.path().join(merged_meta.ulid.to_string())).unwrap();
    let q_m = TsdbQuerier::new(None, vec![Arc::new(blk_m)], 100, 400);
    let m_res = q_m
        .select_chunks_with_mode(
            true,
            &[LabelMatcher::new(MatchType::Equal, "__name__", "icw02_metric").unwrap()],
            false,
        )
        .unwrap();
    let m_samples: Vec<(i64, f64)> = m_res[0]
        .chunks
        .iter()
        .flat_map(|c| decode_chunk_samples(c.chunk.as_ref().unwrap()).unwrap())
        .map(|s| match s.value {
            SampleValue::Float(v) => (s.t, v),
            _ => panic!("expected float"),
        })
        .collect();
    assert_eq!(m_samples, vec![(150, 1.0), (200, 1.9), (300, 3.0)]);

    // Case 2: Byte-identical chunk elision preserves EncXOR2 verbatim without re-encoding.
    let mut c_xor2 = Xor2Chunk::new();
    {
        let mut app = c_xor2.appender().unwrap();
        app.append(0, 100, 7.0).unwrap();
        app.append(0, 200, 8.0).unwrap();
    }
    let xor2_meta = ChunkMeta {
        ref_id: 1,
        min_time: 100,
        max_time: 200,
        chunk: Some(EncodedChunk::Xor2(c_xor2)),
    };
    let deduped = compact_chunk_series_merge(
        vec![xor2_meta.clone(), xor2_meta.clone()],
        Encoding::Xor,
    )
    .unwrap();
    assert_eq!(deduped.len(), 1);
    assert_eq!(deduped[0].chunk.as_ref().unwrap().encoding(), Encoding::Xor2);

    // Case 3: 120-sample split & EncXOR2 plumbing in compact_blocks_with_float_encoding.
    let split_dir = tempfile::tempdir().unwrap();
    let mut c_left = XorChunk::new();
    {
        let mut app = c_left.appender().unwrap();
        for i in 0..100i64 {
            app.append(0, 1000 + i * 2, i as f64).unwrap();
        }
    }
    let mut c_right = XorChunk::new();
    {
        let mut app = c_right.appender().unwrap();
        for i in 0..100i64 {
            app.append(0, 1001 + i * 2, (100 + i) as f64).unwrap();
        }
    }
    let bm1 = write_block_from_series(
        split_dir.path(),
        1000,
        1300,
        vec![CompactionSeries {
            labels: lbls.clone(),
            chunks: vec![ChunkMeta {
                ref_id: 0,
                min_time: 1000,
                max_time: 1198,
                chunk: Some(EncodedChunk::Xor(c_left)),
            }],
            tombstones: Vec::new(),
        }],
        Default::default(),
        false,
        0,
    )
    .unwrap()
    .unwrap();
    let bm2 = write_block_from_series(
        split_dir.path(),
        1000,
        1300,
        vec![CompactionSeries {
            labels: lbls,
            chunks: vec![ChunkMeta {
                ref_id: 0,
                min_time: 1001,
                max_time: 1199,
                chunk: Some(EncodedChunk::Xor(c_right)),
            }],
            tombstones: Vec::new(),
        }],
        Default::default(),
        false,
        0,
    )
    .unwrap()
    .unwrap();
    let sb1 = Block::open(split_dir.path().join(bm1.ulid.to_string())).unwrap();
    let sb2 = Block::open(split_dir.path().join(bm2.ulid.to_string())).unwrap();
    let split_out = tempfile::tempdir().unwrap();
    let smeta =
        compact_blocks_with_float_encoding(split_out.path(), &[&sb1, &sb2], Encoding::Xor2)
            .unwrap()
            .unwrap();
    let sblk = Block::open(split_out.path().join(smeta.ulid.to_string())).unwrap();
    let all_ids = sblk.index().postings("", &[""]).unwrap();
    assert_eq!(all_ids.len(), 1);
    let (_, out_chks) = sblk.index().series(all_ids[0]).unwrap();
    assert_eq!(out_chks.len(), 2, "200 overlapping samples must split at 120 + 80");
    let ch0 = sblk.chunks().chunk(out_chks[0].ref_id).unwrap();
    let ch1 = sblk.chunks().chunk(out_chks[1].ref_id).unwrap();
    assert_eq!(ch0.encoding(), Encoding::Xor2);
    assert_eq!(ch1.encoding(), Encoding::Xor2);
    assert_eq!(ch0.num_samples(), SERIES_TO_CHUNK_ENCODER_SPLIT);
    assert_eq!(ch1.num_samples(), 80);
}

// -----------------------------------------------------------------------------
// W16-ICW-03: Head::open mmaps sealed WAL-replayed chunks without inflating mmap_chunks_total.
// -----------------------------------------------------------------------------

#[test]
fn w16_icw_03_head_open_mmaps_wal_replayed_chunks_without_inflating_mmap_metrics() {
    let dir = tempfile::tempdir().unwrap();
    let pairs = [("__name__", "icw03_metric"), ("job", "api")];

    {
        let opts = HeadOptions {
            chunk_range_ms: 10_000,
            samples_per_chunk: 120,
            ..HeadOptions::default()
        };
        let head = Head::open(dir.path(), opts, i64::MIN).unwrap();
        let mut app = head.appender();
        // 300 samples across 3 chunks (2 sealed + 1 active) in a single chunk_range or spanning ranges.
        for i in 0..300i64 {
            app.append_float(None, &pairs, 1_000 + i * 10, i as f64)
                .unwrap();
        }
        app.commit().unwrap();
        head.close().unwrap();
    }

    // Delete chunks_head/ to simulate missing or corrupt-repaired head chunks on startup.
    let chunks_dir = dir.path().join("chunks_head");
    if chunks_dir.exists() {
        fs::remove_dir_all(&chunks_dir).unwrap();
    }

    let reg = Arc::new(prometheus::Registry::new());
    let opts = HeadOptions {
        chunk_range_ms: 10_000,
        samples_per_chunk: 120,
        head_chunks_write_queue_size: 1024,
        registry: Some(Arc::clone(&reg)),
        ..HeadOptions::default()
    };
    let head2 = Head::open(dir.path(), opts, i64::MIN).unwrap();

    // chunks_head/000001 must have been written during Head::open!
    assert!(
        chunks_dir.join("000001").exists(),
        "Head::open must flush sealed WAL-replayed chunks to chunks_head"
    );
    // Live mmap_chunks_total & head_chunks_max_mmapped must remain 0!
    assert_eq!(
        metric_value(&reg, "prometheus_tsdb_mmap_chunks_total", &[]),
        0.0
    );
    assert_eq!(
        metric_value(&reg, "prometheus_tsdb_head_chunks_max_mmapped", &[]),
        0.0
    );
    // chunk_write_queue recorded the 2 sealed chunks written on open.
    assert_eq!(
        metric_value(
            &reg,
            "prometheus_tsdb_chunk_write_queue_operations_total",
            &[("operation", "add")]
        ),
        2.0
    );
    assert_eq!(
        metric_value(
            &reg,
            "prometheus_tsdb_chunk_write_queue_operations_total",
            &[("operation", "complete")]
        ),
        2.0
    );

    // Subsequent live mmap_head_chunks() has 0 extra sealed chunks left to mmap.
    assert_eq!(head2.mmap_head_chunks().unwrap(), 0);
    assert_eq!(
        metric_value(&reg, "prometheus_tsdb_mmap_chunks_total", &[]),
        0.0
    );

    let entry = head2.get_entry_by_sorted_pairs(&pairs).unwrap();
    let (_, chks) = head2
        .series_final_chunks(entry.ref_id, i64::MIN, i64::MAX)
        .unwrap()
        .unwrap();
    let count: usize = chks
        .iter()
        .map(|c| decode_chunk_samples(c.chunk.as_ref().unwrap()).unwrap().len())
        .sum();
    assert_eq!(count, 300);
    head2.close().unwrap();
}

// -----------------------------------------------------------------------------
// W16-ICW-04: AllPostingsKey ("") LabelValues parity across Head, Block, Querier.
// -----------------------------------------------------------------------------

#[test]
fn w16_icw_04_all_postings_key_label_values_parity() {
    let dir = tempfile::tempdir().unwrap();
    let db = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();

    // Empty Head/DB -> []
    {
        let q = db.querier(i64::MIN, i64::MAX);
        assert_eq!(q.label_values("", None, &[]).unwrap(), Vec::<String>::new());
        assert_eq!(
            q.label_values(
                "",
                None,
                &[LabelMatcher::new(MatchType::Equal, "__name__", "up").unwrap()],
            )
            .unwrap(),
            Vec::<String>::new()
        );
        assert_eq!(q.label_values_cardinality("", &[]).unwrap(), 0);
    }

    // Push series to Head
    {
        let mut app = db.appender();
        app.append_float(None, &[("__name__", "up"), ("job", "api")], 10_000, 1.0)
            .unwrap();
        app.commit().unwrap();
    }

    let check_non_empty = |db: &Tsdb| {
        let q = db.querier(i64::MIN, i64::MAX);
        assert_eq!(q.label_values("", None, &[]).unwrap(), vec![String::new()]);
        assert_eq!(
            q.label_values(
                "",
                None,
                &[LabelMatcher::new(MatchType::Equal, "__name__", "up").unwrap()],
            )
            .unwrap(),
            vec![String::new()]
        );
        assert_eq!(
            q.label_values(
                "",
                None,
                &[LabelMatcher::new(MatchType::Equal, "__name__", "missing").unwrap()],
            )
            .unwrap(),
            Vec::<String>::new()
        );
        let sr: Vec<String> = q
            .search_label_values("", &SearchHints::default(), &[])
            .unwrap()
            .into_iter()
            .map(|r| r.value)
            .collect();
        assert_eq!(sr, vec![String::new()]);
        assert_eq!(q.label_values_cardinality("", &[]).unwrap(), 1);
    };

    check_non_empty(&db);

    // Compact Head into ULID block + truncate Head, then re-verify on Block-only data!
    db.compact_head_all().unwrap();
    check_non_empty(&db);

    db.close().unwrap();
}

// -----------------------------------------------------------------------------
// W16-ICW-05: Lazy 1-FD / 1-buffer SegmentBufReader.
// -----------------------------------------------------------------------------

#[test]
fn w16_icw_05_segment_buf_reader_opens_only_one_fd_lazily() {
    let dir = tempfile::tempdir().unwrap();
    let num_segments = 24usize;
    let payload = vec![b'x'; 200];

    {
        let mut w = WlogWriter::with_segment_size(dir.path(), PAGE_SIZE, CompressionType::None)
            .unwrap();
        // Write enough records to create >20 segments (1 page per segment).
        for i in 0..num_segments {
            let mut rec = Vec::with_capacity(8 + payload.len());
            rec.extend_from_slice(&(i as u64).to_be_bytes());
            rec.extend_from_slice(&payload);
            w.log(&[&rec]).unwrap();
            if i + 1 < num_segments {
                w.next_segment().unwrap();
            }
        }
        w.close().unwrap();
    }

    #[cfg(target_os = "linux")]
    let dir_canon = fs::canonicalize(dir.path()).unwrap();
    #[cfg(target_os = "linux")]
    let fd_count = || -> usize {
        fs::read_dir("/proc/self/fd")
            .unwrap()
            .filter_map(Result::ok)
            .filter(|e| {
                fs::read_link(e.path())
                    .map(|target| target.starts_with(&dir_canon))
                    .unwrap_or(false)
            })
            .count()
    };

    #[cfg(target_os = "linux")]
    assert_eq!(fd_count(), 0);

    let sbr = SegmentBufReader::open_dir(dir.path()).unwrap();

    #[cfg(target_os = "linux")]
    {
        let fds_after_open = fd_count();
        // Lazy open: zero segment FDs opened in open_ranges!
        assert_eq!(
            fds_after_open, 0,
            "SegmentBufReader::open_dir opened segment FDs eagerly: after={fds_after_open}"
        );
    }

    let mut wr = WlogReader::from_reader(sbr);
    let mut read_count = 0usize;
    while let Some(rec) = wr.next_record().unwrap() {
        let idx = u64::from_be_bytes(rec[..8].try_into().unwrap()) as usize;
        assert_eq!(idx, read_count);
        read_count += 1;

        #[cfg(target_os = "linux")]
        {
            let fds_during = fd_count();
            assert!(
                fds_during <= 1,
                "SegmentBufReader held >1 segment FD open during streaming: during={fds_during}"
            );
        }
    }
    assert_eq!(read_count, num_segments);

    // Also confirm reading directly via std::io::Read drains all segments cleanly.
    let mut sbr2 = SegmentBufReader::open_dir(dir.path()).unwrap();
    let mut raw = Vec::new();
    sbr2.read_to_end(&mut raw).unwrap();
    assert!(!raw.is_empty());
}

/// Go `segmentBufReader.Read` never advances `cur` past the last segment, so
/// `Reader.Err()` still reports `segs[cur]` / `off` when the final record is torn at a page
/// boundary. `Head::open` needs that position to run `wlog.Repair`.
#[test]
fn w16_icw_05_torn_last_record_at_page_boundary_keeps_last_segment_position() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut w = WlogWriter::open(dir.path(), CompressionType::None).unwrap();
        w.log(&[b"first-segment-record"]).unwrap();
        w.next_segment().unwrap();
        w.log(&[b"intact"]).unwrap();
        // Spans three pages: REC_FIRST fills the rest of page 0, REC_MIDDLE page 1, REC_LAST page 2.
        let big = vec![b'y'; 2 * PAGE_SIZE + 100];
        w.log(&[&big]).unwrap();
        w.close().unwrap();
    }
    // Crash after only the first page reached disk: the file ends on a page boundary mid-record.
    let seg1 = dir.path().join(format!("{:08}", 1));
    let f = fs::OpenOptions::new().write(true).open(&seg1).unwrap();
    f.set_len(PAGE_SIZE as u64).unwrap();
    drop(f);

    let mut r = WlogReader::open_dir(dir.path()).unwrap();
    assert_eq!(r.next_record().unwrap().unwrap(), b"first-segment-record");
    assert_eq!(r.next_record().unwrap().unwrap(), b"intact");
    let err = r.next_record().unwrap_err();
    match err {
        mimir_tsdb::wlog::WlogError::Corruption {
            dir: err_dir,
            segment,
            offset,
            ..
        } => {
            assert_eq!(segment, Some(1));
            assert_eq!(offset, PAGE_SIZE);
            assert_eq!(err_dir.as_deref(), Some(dir.path()));
        }
        other => panic!("expected corruption error, got {other:?}"),
    }
    // EOF is sticky and keeps reporting the last segment without re-reading it.
    assert_eq!(r.segment(), Some(1));
    assert_eq!(r.offset(), PAGE_SIZE);

    // Clean EOF on an intact log also stays on the last segment.
    let mut sbr = SegmentBufReader::open_dir(dir.path()).unwrap();
    let mut raw = Vec::new();
    sbr.read_to_end(&mut raw).unwrap();
    assert_eq!(raw.len(), 2 * PAGE_SIZE);
    assert_eq!(sbr.current_segment(), Some(1));
    assert_eq!(sbr.current_offset(), PAGE_SIZE);
    let mut one = [0u8; 1];
    assert_eq!(sbr.read(&mut one).unwrap(), 0);
}
