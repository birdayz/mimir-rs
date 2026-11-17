//! Destructive ENOSPC & torn-write durability tests for WlogWriter, HeadChunksWriter,
//! BlockMeta, and Tombstones atomic writers (RISK-4B, RISK-4C).

use std::fs;
use std::io::ErrorKind;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use mimir_tsdb::block::{write_meta_file, BlockMeta, BlockMetaCompaction, BlockStats, META_FILENAME};
use mimir_tsdb::chunkenc::{EncodedChunk, XorChunk};
use mimir_tsdb::head_chunks::{
    head_chunk_file_path, HeadChunksError, HeadChunksReader, HeadChunksWriter, WriteFaultAction,
    HEAD_CHUNK_FILE_HEADER_SIZE,
};
use mimir_tsdb::tombstones::{write_tombstones_file, Interval, MemTombstones, TOMBSTONES_FILENAME};
use mimir_tsdb::wlog::{
    segment_name, CompressionType, SegmentBufReader, WlogError, WlogReader, WlogWriter, PAGE_SIZE,
};
use ulid::Ulid;

fn make_xor_chunk(series_seed: i64) -> EncodedChunk {
    let mut xc = XorChunk::new();
    {
        let mut app = xc.appender().unwrap();
        app.append(0, series_seed * 1000, series_seed as f64 + 0.5)
            .unwrap();
        app.append(0, series_seed * 1000 + 500, series_seed as f64 + 1.5)
            .unwrap();
    }
    EncodedChunk::Xor(xc)
}

fn replay_all_wlog(dir: &std::path::Path) -> Vec<Vec<u8>> {
    let seg_reader = SegmentBufReader::open_dir(dir).unwrap();
    let mut reader = WlogReader::from_reader(seg_reader);
    let mut out = Vec::new();
    while let Some(rec) = reader.next_record().unwrap() {
        out.push(rec.to_vec());
    }
    out
}

#[test]
fn wlog_enospc_mid_single_page_record_rolls_back_torn_bytes_and_recovers() {
    for compression in [CompressionType::None, CompressionType::Snappy] {
        let dir = tempfile::tempdir().unwrap();
        let mut w = WlogWriter::open(dir.path(), compression).unwrap();

        let mut expected = Vec::new();
        for i in 0..10usize {
            let rec = format!("pre-record-{i:04}-payload-data").into_bytes();
            w.log(&[&rec]).unwrap();
            expected.push(rec);
        }

        let seg_path = segment_name(dir.path(), 0);
        let len_before_fault = fs::metadata(&seg_path).unwrap().len();

        // Inject a torn partial write (17 bytes written to OS file) followed by ENOSPC.
        w.set_write_fault_hook(Some(Arc::new(|_| WriteFaultAction::PartialThenError {
            prefix_len: 17,
            err_kind: ErrorKind::StorageFull,
        })));

        let err = w
            .log(&[b"must-not-appear-torn-single-page-record"])
            .unwrap_err();
        assert!(
            matches!(err, WlogError::Io(ref io_err) if io_err.kind() == ErrorKind::StorageFull),
            "expected StorageFull error, got {err:?}"
        );

        // Segment file on disk must be truncated back to exact pre-fault length.
        let len_after_rollback = fs::metadata(&seg_path).unwrap().len();
        assert_eq!(
            len_after_rollback, len_before_fault,
            "torn bytes must be truncated back to saved_file_pos"
        );

        // Recover disk space and write 1,000 more records.
        w.set_write_fault_hook(None);
        for i in 0..1000usize {
            let rec = format!("post-record-{i:04}-durable-payload").into_bytes();
            w.log(&[&rec]).unwrap();
            expected.push(rec);
        }
        w.close().unwrap();

        let replayed = replay_all_wlog(dir.path());
        assert_eq!(replayed, expected);
    }
}

#[test]
fn wlog_enospc_mid_multipage_rec_first_to_rec_middle_leaves_no_orphan() {
    let dir = tempfile::tempdir().unwrap();
    let mut w = WlogWriter::open(dir.path(), CompressionType::None).unwrap();

    let mut expected = Vec::new();
    for i in 0..5usize {
        let rec = format!("initial-small-{i:02}").into_bytes();
        w.log(&[&rec]).unwrap();
        expected.push(rec);
    }

    let seg_path = segment_name(dir.path(), 0);
    let len_before_multipage = fs::metadata(&seg_path).unwrap().len();

    // Multi-page record (> 3 pages = > 96 KiB): REC_FIRST -> REC_MIDDLE -> REC_MIDDLE -> REC_LAST.
    let mut huge_rec = vec![0xABu8; PAGE_SIZE * 3 + 1024];
    for (idx, b) in huge_rec.iter_mut().enumerate() {
        *b = (idx % 251) as u8;
    }

    // Allow REC_FIRST (1st page flush) to succeed on disk, then fail on REC_MIDDLE (2nd page flush)
    // with 512 torn bytes before ENOSPC!
    let write_calls = Arc::new(AtomicUsize::new(0));
    let wc = Arc::clone(&write_calls);
    w.set_write_fault_hook(Some(Arc::new(move |_| {
        let call = wc.fetch_add(1, Ordering::SeqCst);
        if call == 0 {
            WriteFaultAction::PassThrough
        } else {
            WriteFaultAction::PartialThenError {
                prefix_len: 512,
                err_kind: ErrorKind::StorageFull,
            }
        }
    })));

    let err = w.log(&[&huge_rec]).unwrap_err();
    assert!(matches!(err, WlogError::Io(_)));
    assert!(
        write_calls.load(Ordering::SeqCst) >= 2,
        "expected REC_FIRST flush to succeed and REC_MIDDLE flush to fail"
    );

    // Physical file MUST be truncated all the way back BEFORE REC_FIRST!
    assert_eq!(
        fs::metadata(&seg_path).unwrap().len(),
        len_before_multipage,
        "REC_FIRST and torn REC_MIDDLE must both be rolled back"
    );

    // Also verify batch log(&[small_keep, huge_rec]) where small_keep is in memory (not yet flushed)
    // and huge_rec fails on REC_MIDDLE: small_keep must survive in Page!
    write_calls.store(0, Ordering::SeqCst);
    let small_keep = b"batch-first-record-must-survive".to_vec();
    let err2 = w.log(&[&small_keep, &huge_rec]).unwrap_err();
    assert!(matches!(err2, WlogError::Io(_)));
    expected.push(small_keep);
    assert_eq!(
        fs::metadata(&seg_path).unwrap().len(),
        len_before_multipage,
        "segment must still be at pre-batch length"
    );

    // Recover disk space and write 1,000 more records (including multi-page ones).
    w.set_write_fault_hook(None);
    for i in 0..1000usize {
        let rec = if i % 100 == 0 {
            vec![(i % 200) as u8; PAGE_SIZE * 2 + 333]
        } else {
            format!("recovered-record-{i:04}-end-to-end").into_bytes()
        };
        w.log(&[&rec]).unwrap();
        expected.push(rec);
    }
    w.close().unwrap();

    let replayed = replay_all_wlog(dir.path());
    assert_eq!(replayed.len(), expected.len());
    assert_eq!(replayed, expected);
}

#[test]
fn head_chunks_writer_enospc_direct_and_buffered_rollback_preserves_offsets() {
    // 1. Unbuffered / direct chunk write ENOSPC with torn partial bytes.
    {
        let dir = tempfile::tempdir().unwrap();
        let mut w = HeadChunksWriter::with_options(dir.path(), 4096, 1).unwrap();
        let mut refs = Vec::new();
        let mut chunks = Vec::new();

        for i in 0..10i64 {
            let c = make_xor_chunk(i + 1);
            let r = w.write_chunk(i as u64 + 1, i * 1000, i * 1000 + 500, &c, false).unwrap();
            refs.push(r);
            chunks.push(c);
        }

        let seq1_path = head_chunk_file_path(dir.path(), w.current_seq());
        let len_before_fault = fs::metadata(&seq1_path).unwrap().len();

        w.set_write_fault_hook(Some(Arc::new(|_| WriteFaultAction::PartialThenError {
            prefix_len: 12,
            err_kind: ErrorKind::StorageFull,
        })));

        let bad_chunk = make_xor_chunk(9999);
        let err = w.write_chunk(9999, 100, 200, &bad_chunk, false).unwrap_err();
        assert!(matches!(err, HeadChunksError::Io(ref io_e) if io_e.kind() == ErrorKind::StorageFull));

        // Physical file length must be rolled back to exact pre-fault length.
        assert_eq!(fs::metadata(&seq1_path).unwrap().len(), len_before_fault);

        // Recover disk space and write 1,000 more chunks across multiple file rotations.
        w.set_write_fault_hook(None);
        for i in 10..1010i64 {
            let c = make_xor_chunk(i + 1);
            let r = w
                .write_chunk(i as u64 + 1, i * 1000, i * 1000 + 500, &c, i % 7 == 0)
                .unwrap();
            refs.push(r);
            chunks.push(c);
        }
        w.close().unwrap();

        let reader = HeadChunksReader::open(dir.path()).unwrap();
        let all = reader.collect_all_chunks().unwrap();
        assert_eq!(all.len(), 1010);
        for (idx, (r, expected_chunk)) in refs.iter().zip(chunks.iter()).enumerate() {
            assert_eq!(all[idx].chunk_ref, *r);
            assert_eq!(reader.chunk(*r).unwrap(), *expected_chunk);
        }
    }

    // 2. Buffered flush ENOSPC: previously-acknowledged chunks remain intact and flush on recovery.
    {
        let dir = tempfile::tempdir().unwrap();
        let mut w = HeadChunksWriter::with_options(dir.path(), 1024 * 1024, 512).unwrap();
        let mut refs = Vec::new();
        let mut chunks = Vec::new();

        // Write 5 chunks (~200 bytes total < 512B buffer), all acknowledged into write_buf.
        for i in 0..5i64 {
            let c = make_xor_chunk(i + 1);
            let r = w.write_chunk(i as u64 + 1, i * 1000, i * 1000 + 500, &c, false).unwrap();
            refs.push(r);
            chunks.push(c);
        }

        // Fail flush with torn partial write + ENOSPC.
        w.set_write_fault_hook(Some(Arc::new(|_| WriteFaultAction::PartialThenError {
            prefix_len: 23,
            err_kind: ErrorKind::StorageFull,
        })));
        let err = w.flush().unwrap_err();
        assert!(matches!(err, HeadChunksError::Io(_)));

        // Physical file still only has the 8-byte header (all 23 torn bytes rolled back).
        let seq1_path = head_chunk_file_path(dir.path(), 1);
        assert_eq!(
            fs::metadata(&seq1_path).unwrap().len(),
            HEAD_CHUNK_FILE_HEADER_SIZE as u64
        );

        // Recover disk space, write 1,000 more chunks, close, and verify all 1,005 chunks!
        w.set_write_fault_hook(None);
        for i in 5..1005i64 {
            let c = make_xor_chunk(i + 1);
            let r = w
                .write_chunk(i as u64 + 1, i * 1000, i * 1000 + 500, &c, false)
                .unwrap();
            refs.push(r);
            chunks.push(c);
        }
        w.close().unwrap();

        let reader = HeadChunksReader::open(dir.path()).unwrap();
        let all = reader.collect_all_chunks().unwrap();
        assert_eq!(all.len(), 1005);
        for (r, expected_chunk) in refs.iter().zip(chunks.iter()) {
            assert_eq!(reader.chunk(*r).unwrap(), *expected_chunk);
        }
    }

    // 3. cut_new_file header write ENOSPC cleans up empty file and preserves seq.
    {
        let dir = tempfile::tempdir().unwrap();
        let mut w = HeadChunksWriter::with_options(dir.path(), 1024, 64).unwrap();
        let c0 = make_xor_chunk(1);
        let r0 = w.write_chunk(1, 0, 500, &c0, false).unwrap();
        assert_eq!(w.current_seq(), 1);

        w.set_write_fault_hook(Some(Arc::new(|len| {
            if len == HEAD_CHUNK_FILE_HEADER_SIZE {
                WriteFaultAction::PartialThenError {
                    prefix_len: 3,
                    err_kind: ErrorKind::StorageFull,
                }
            } else {
                WriteFaultAction::PassThrough
            }
        })));
        let err = w.cut_new_file().unwrap_err();
        assert!(matches!(err, HeadChunksError::Io(_)));
        assert_eq!(w.current_seq(), 1);
        assert!(!head_chunk_file_path(dir.path(), 2).exists());

        w.set_write_fault_hook(None);
        let c1 = make_xor_chunk(2);
        let r1 = w.write_chunk(2, 1000, 1500, &c1, false).unwrap();
        assert_eq!(r1.unpack().0, 2);
        w.close().unwrap();

        let reader = HeadChunksReader::open(dir.path()).unwrap();
        assert_eq!(reader.chunk(r0).unwrap(), c0);
        assert_eq!(reader.chunk(r1).unwrap(), c1);
    }
}

#[test]
fn meta_and_tombstones_clean_up_orphan_tmp_on_rename_error() {
    // 1. write_meta_file cleans up meta.json.tmp when rename fails.
    {
        let dir = tempfile::tempdir().unwrap();
        // Pre-create meta.json as a non-empty directory so rename(&tmp, &path) fails.
        fs::create_dir_all(dir.path().join(META_FILENAME).join("blocker")).unwrap();

        let mut meta = BlockMeta {
            ulid: Ulid::from_parts(1000, 1),
            min_time: 0,
            max_time: 1000,
            stats: BlockStats::default(),
            compaction: BlockMetaCompaction::default(),
            version: 1,
            out_of_order: false,
            thanos: None,
        };
        assert!(write_meta_file(dir.path(), &mut meta).is_err());
        assert!(
            !dir.path().join(format!("{META_FILENAME}.tmp")).exists(),
            "orphan meta.json.tmp must be removed on error"
        );
    }

    // 2. write_tombstones_file cleans up tombstones.tmp when rename fails.
    {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir_all(dir.path().join(TOMBSTONES_FILENAME).join("blocker")).unwrap();

        let ts = MemTombstones::new();
        ts.add_interval(1, Interval::new(100, 200));
        assert!(write_tombstones_file(dir.path(), &ts).is_err());
        assert!(
            !dir.path().join(format!("{TOMBSTONES_FILENAME}.tmp")).exists(),
            "orphan tombstones.tmp must be removed on error"
        );
    }
}
