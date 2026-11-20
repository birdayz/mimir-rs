//! Startup repair of `chunks_head`, WAL and WBL corruption.
//!
//! Ported from github.com/prometheus/prometheus/tsdb/head_test.go
//! (`TestHeadReadWriterRepair`, `TestWalRepair_DecodingError`, `TestWblRepair_DecodingError`),
//! github.com/prometheus/prometheus/tsdb/head_append_v2_test.go
//! (`TestHead_Init_DiscardChunksWithUnsupportedEncoding`) and the `Head.Init` /
//! `DB.Open` repair paths of github.com/prometheus/prometheus/tsdb/{head.go,db.go}.

use std::fs::{self, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;

use crate::chunkenc::{EncodedChunk, XorChunk};
use crate::db::{Tsdb, TsdbOptions};
use crate::head_chunks::{HeadChunksWriter, HEAD_CHUNK_FILE_HEADER_SIZE};
use crate::labels::Labels;
use crate::record::{RecordEncoder, RefSample, RefSeries, Tombstone};
use crate::wlog::{CompressionType, WlogReader, WlogWriter};

fn counter(reg: &prometheus::Registry, name: &str) -> f64 {
    reg.gather()
        .into_iter()
        .find(|mf| mf.get_name() == name)
        .map(|mf| mf.get_metric().iter().map(|m| m.get_counter().get_value()).sum())
        .unwrap_or_else(|| panic!("metric {name} not registered"))
}

fn opts_with_registry(reg: &Arc<prometheus::Registry>) -> TsdbOptions {
    let mut opts = TsdbOptions::default();
    opts.head.registry = Some(Arc::clone(reg));
    opts
}

fn chunk_files(dir: &Path) -> Vec<String> {
    let mut v: Vec<String> = fs::read_dir(dir.join("chunks_head"))
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    v.sort();
    v
}

fn xor_chunk(ts: &[i64]) -> EncodedChunk {
    let mut xc = XorChunk::new();
    {
        let mut app = xc.appender().unwrap();
        for &t in ts {
            app.append(0, t, t as f64).unwrap();
        }
    }
    EncodedChunk::Xor(xc)
}

fn write_series_wal(dir: &Path, compress: CompressionType) {
    let mut w = WlogWriter::open(dir.join("wal"), compress).unwrap();
    let enc = RecordEncoder::new(false);
    let rec = enc.series(
        &[RefSeries {
            ref_id: 1,
            labels: Labels::from_pairs([("a", "1")]),
        }],
        Vec::new(),
    );
    w.log(&[&rec]).unwrap();
    w.close().unwrap();
}

/// Go `TestHeadReadWriterRepair`: a CRC mismatch in the 4th of 6 head chunk files makes
/// `Open` delete files 4..6, count one m-map chunk corruption and still succeed.
#[test]
fn head_read_writer_repair_deletes_corrupt_and_later_files() {
    const CHUNK_RANGE: i64 = 1000;
    let dir = tempfile::tempdir().unwrap();
    write_series_wal(dir.path(), CompressionType::None);
    {
        // One chunk per file (max file size smaller than two chunks).
        let mut w = HeadChunksWriter::with_max_file_size(dir.path().join("chunks_head"), 60)
            .unwrap();
        for i in 0..6i64 {
            let c = xor_chunk(&[i * CHUNK_RANGE, i * CHUNK_RANGE + CHUNK_RANGE - 1]);
            w.write_chunk(1, i * CHUNK_RANGE, i * CHUNK_RANGE + CHUNK_RANGE - 1, &c, false)
                .unwrap();
        }
        w.close().unwrap();
    }
    let files = chunk_files(dir.path());
    assert_eq!(files.len(), 6);

    // Corrupt the 4th file by writing random bytes to the series ref.
    let f = OpenOptions::new()
        .write(true)
        .open(dir.path().join("chunks_head").join(&files[3]))
        .unwrap();
    f.write_all_at(&[67, 88], (HEAD_CHUNK_FILE_HEADER_SIZE + 2) as u64)
        .unwrap();
    drop(f);

    let reg = Arc::new(prometheus::Registry::new());
    let db = Tsdb::open(dir.path(), opts_with_registry(&reg)).expect("open must repair");
    assert_eq!(counter(&reg, "prometheus_tsdb_mmap_chunk_corruptions_total"), 1.0);
    // The 3 intact chunks are still attached to the series replayed from the WAL.
    assert_eq!(db.head().num_series(), 1);
    assert_eq!(db.head().max_time(), 3 * CHUNK_RANGE - 1);
    db.close().unwrap();
    drop(db);

    // The segments from the corrupt segment on are removed.
    assert_eq!(chunk_files(dir.path()), files[..3].to_vec());
}

/// Audit repro (TSDB-1): a torn tail (valid header + non-zero garbage too short for a chunk
/// header), as left by an OOMKill during a buffered write, must not prevent startup.
#[test]
fn torn_chunks_head_tail_is_repaired_on_open() {
    let dir = tempfile::tempdir().unwrap();
    fs::create_dir_all(dir.path().join("wal")).unwrap();
    fs::create_dir_all(dir.path().join("chunks_head")).unwrap();
    let mut data = vec![0x01, 0x30, 0xBC, 0x91, 0x01, 0, 0, 0];
    data.extend((1..=30u8).map(|b| b.wrapping_mul(37) | 1));
    fs::write(dir.path().join("chunks_head").join("000001"), &data).unwrap();

    let reg = Arc::new(prometheus::Registry::new());
    let db = Tsdb::open(dir.path(), opts_with_registry(&reg)).expect("open must repair");
    assert_eq!(counter(&reg, "prometheus_tsdb_mmap_chunk_corruptions_total"), 1.0);
    assert_eq!(counter(&reg, "prometheus_tsdb_wal_corruptions_total"), 0.0);
    assert!(chunk_files(dir.path()).is_empty());

    // The head is writable and new chunks are written after the repair.
    let mut app = db.appender();
    app.append_float(None, &[("__name__", "m")], 1000, 1.0).unwrap();
    app.commit().unwrap();
    db.close().unwrap();
}

/// Go `Head.loadMmappedChunks`: an out-of-sequence in-order m-mapped chunk is a plain error
/// (not a `CorruptionErr`): the counter is incremented once in the iteration callback,
/// `DeleteCorrupted` cannot handle it and every chunk file is discarded (`Truncate(MaxUint32)`).
#[test]
fn out_of_sequence_mmapped_chunk_discards_all_chunk_files() {
    let dir = tempfile::tempdir().unwrap();
    write_series_wal(dir.path(), CompressionType::None);
    {
        let mut w = HeadChunksWriter::with_max_file_size(dir.path().join("chunks_head"), 60)
            .unwrap();
        w.write_chunk(1, 0, 999, &xor_chunk(&[0, 999]), false).unwrap();
        w.write_chunk(1, 2000, 2999, &xor_chunk(&[2000, 2999]), false).unwrap();
        // Overlaps the previous chunk of the same series.
        w.write_chunk(1, 2500, 3500, &xor_chunk(&[2500, 3500]), false).unwrap();
        w.close().unwrap();
    }
    assert_eq!(chunk_files(dir.path()).len(), 3);

    let reg = Arc::new(prometheus::Registry::new());
    let db = Tsdb::open(dir.path(), opts_with_registry(&reg)).expect("open must repair");
    assert_eq!(counter(&reg, "prometheus_tsdb_mmap_chunk_corruptions_total"), 1.0);
    assert!(chunk_files(dir.path()).is_empty());
    // Go Head.Init runs gc(h.MinTime()) after replay; since WAL only had a series record
    // with no samples and all mmapped chunks were discarded, the chunkless series is GCed.
    assert_eq!(db.head().num_series(), 0);
    db.close().unwrap();
}

/// Go `TestHead_Init_DiscardChunksWithUnsupportedEncoding`: chunks with an unknown encoding
/// are skipped (not a corruption, not fatal).
#[test]
fn chunks_with_unsupported_encoding_are_discarded() {
    let dir = tempfile::tempdir().unwrap();
    write_series_wal(dir.path(), CompressionType::None);
    let p = dir.path().join("chunks_head").join("000001");
    {
        let mut w = HeadChunksWriter::open(dir.path().join("chunks_head")).unwrap();
        w.write_chunk(1, 0, 999, &xor_chunk(&[0, 999]), false).unwrap();
        w.write_chunk(1, 1000, 1999, &xor_chunk(&[1000, 1999]), false).unwrap();
        w.close().unwrap();
    }
    // Rewrite the encoding byte of the second chunk to an unknown value (0x7f) and fix
    // its CRC so that only the encoding is "unsupported".
    let mut data = fs::read(&p).unwrap();
    let first_len = {
        let data_len = data[HEAD_CHUNK_FILE_HEADER_SIZE + 25] as usize; // 1-byte uvarint
        25 + 1 + data_len + 4
    };
    let start = HEAD_CHUNK_FILE_HEADER_SIZE + first_len;
    data[start + 24] = 0x7f;
    let data_len = data[start + 25] as usize;
    let end = start + 26 + data_len;
    let crc = crc32c::crc32c(&data[start..end]);
    data[end..end + 4].copy_from_slice(&crc.to_be_bytes());
    fs::write(&p, &data).unwrap();

    let reg = Arc::new(prometheus::Registry::new());
    let db = Tsdb::open(dir.path(), opts_with_registry(&reg)).expect("open");
    assert_eq!(counter(&reg, "prometheus_tsdb_mmap_chunk_corruptions_total"), 0.0);
    assert_eq!(db.head().max_time(), 999);
    db.close().unwrap();
    assert_eq!(chunk_files(dir.path()), vec!["000001".to_string()]);
}

/// Go `openMMapFiles`: an invalid file header (bad magic) is NOT a `CorruptionErr`; the
/// head cannot be created and `Open` fails without deleting anything.
#[test]
fn invalid_chunk_file_magic_is_fatal() {
    let dir = tempfile::tempdir().unwrap();
    fs::create_dir_all(dir.path().join("wal")).unwrap();
    fs::create_dir_all(dir.path().join("chunks_head")).unwrap();
    fs::write(
        dir.path().join("chunks_head").join("000001"),
        [0xde, 0xad, 0xbe, 0xef, 0x01, 0, 0, 0],
    )
    .unwrap();
    let err = Tsdb::open(dir.path(), TsdbOptions::default())
        .err()
        .expect("open must fail");
    assert!(err.to_string().contains("invalid magic number deadbeef"), "{err}");
    assert_eq!(chunk_files(dir.path()), vec!["000001".to_string()]);
}

fn count_records(dir: &Path) -> usize {
    let mut r = WlogReader::open_dir(dir).unwrap();
    let mut n = 0;
    while r.next_record().unwrap().is_some() {
        n += 1;
    }
    n
}

/// Go `TestWalRepair_DecodingError`: a record that cannot be decoded is a repairable
/// corruption; `Open` succeeds, counts one WAL corruption and keeps the 5 intact records.
#[test]
fn wal_repair_decoding_error() {
    let enc = RecordEncoder::new(false);
    let cases: Vec<(&str, Vec<u8>)> = vec![
        (
            "decode_series",
            enc.series(
                &[RefSeries {
                    ref_id: 1,
                    labels: Labels::from_pairs([("a", "b")]),
                }],
                Vec::new(),
            ),
        ),
        (
            "decode_samples",
            enc.samples(
                &[RefSample {
                    ref_id: 0,
                    st: 0,
                    t: 99,
                    v: 1.0,
                }],
                Vec::new(),
            ),
        ),
        (
            "decode_tombstone",
            enc.tombstones(
                &[Tombstone {
                    ref_id: 1,
                    mint: 0,
                    maxt: 0,
                }],
                Vec::new(),
            ),
        ),
    ];
    const TOTAL_RECS: usize = 9;
    const EXP_RECS: usize = 5;
    for (name, rec) in &cases {
        for compress in [CompressionType::None, CompressionType::Snappy, CompressionType::Zstd] {
            for concurrency in [1usize, 4] {
                let dir = tempfile::tempdir().unwrap();
                {
                    let mut w = WlogWriter::open(dir.path().join("wal"), compress).unwrap();
                    for i in 1..=TOTAL_RECS {
                        if i - 1 == EXP_RECS {
                            w.log(&[&rec[..3]]).unwrap();
                            continue;
                        }
                        w.log(&[rec]).unwrap();
                    }
                    w.close().unwrap();
                }
                let reg = Arc::new(prometheus::Registry::new());
                let mut opts = opts_with_registry(&reg);
                opts.head.chunk_range_ms = 1;
                opts.head.wal_replay_concurrency = concurrency;
                let db = Tsdb::open(dir.path(), opts)
                    .unwrap_or_else(|e| panic!("{name} {compress:?} c={concurrency}: {e}"));
                assert_eq!(
                    counter(&reg, "prometheus_tsdb_wal_corruptions_total"),
                    1.0,
                    "{name} {compress:?} c={concurrency}"
                );
                db.close().unwrap();
                drop(db);
                assert_eq!(
                    count_records(&dir.path().join("wal")),
                    EXP_RECS,
                    "{name} {compress:?} c={concurrency}: wrong number of intact records"
                );
            }
        }
    }
}

/// Go `TestWblRepair_DecodingError`.
#[test]
fn wbl_repair_decoding_error() {
    let enc = RecordEncoder::new(false);
    let rec = enc.samples(
        &[RefSample {
            ref_id: 0,
            st: 0,
            t: 99,
            v: 1.0,
        }],
        Vec::new(),
    );
    const TOTAL_RECS: usize = 9;
    const EXP_RECS: usize = 5;
    let dir = tempfile::tempdir().unwrap();
    {
        WlogWriter::open(dir.path().join("wal"), CompressionType::None)
            .unwrap()
            .close()
            .unwrap();
        let mut wbl = WlogWriter::open(dir.path().join("wbl"), CompressionType::None).unwrap();
        for i in 1..=TOTAL_RECS {
            if i - 1 == EXP_RECS {
                wbl.log(&[&rec[..3]]).unwrap();
                continue;
            }
            wbl.log(&[&rec]).unwrap();
        }
        wbl.close().unwrap();
    }
    let reg = Arc::new(prometheus::Registry::new());
    let mut opts = opts_with_registry(&reg);
    opts.head.ooo_time_window_ms = 1000 * 60_000;
    let db = Tsdb::open(dir.path(), opts).expect("open must repair the WBL");
    assert_eq!(counter(&reg, "prometheus_tsdb_wal_corruptions_total"), 1.0);
    db.close().unwrap();
    drop(db);
    assert_eq!(count_records(&dir.path().join("wbl")), EXP_RECS);
}

/// Go `Head.Init` returns at the first WAL error, so the WBL is NOT replayed in the boot
/// that repairs the WAL (`DB.Open` repairs and continues without re-running `Init`).
#[test]
fn wal_corruption_skips_wbl_replay_in_same_boot() {
    for concurrency in [1usize, 4] {
        wal_corruption_skips_wbl_replay_in_same_boot_case(concurrency);
    }
}

fn wal_corruption_skips_wbl_replay_in_same_boot_case(concurrency: usize) {
    let dir = tempfile::tempdir().unwrap();
    let enc = RecordEncoder::new(false);
    {
        let mut w = WlogWriter::open(dir.path().join("wal"), CompressionType::None).unwrap();
        let series = enc.series(
            &[RefSeries {
                ref_id: 1,
                labels: Labels::from_pairs([("a", "1")]),
            }],
            Vec::new(),
        );
        w.log(&[&series]).unwrap();
        let s = enc.samples(
            &[RefSample {
                ref_id: 1,
                st: 0,
                t: 10_000,
                v: 1.0,
            }],
            Vec::new(),
        );
        w.log(&[&s]).unwrap();
        w.log(&[&s[..3]]).unwrap();
        w.close().unwrap();

        let mut wbl = WlogWriter::open(dir.path().join("wbl"), CompressionType::None).unwrap();
        let ooo = enc.samples(
            &[RefSample {
                ref_id: 1,
                st: 0,
                t: 5_000,
                v: 2.0,
            }],
            Vec::new(),
        );
        wbl.log(&[&ooo]).unwrap();
        wbl.close().unwrap();
    }
    let reg = Arc::new(prometheus::Registry::new());
    let mut opts = opts_with_registry(&reg);
    opts.head.ooo_time_window_ms = 60 * 60_000;
    opts.head.wal_replay_concurrency = concurrency;
    let db = Tsdb::open(dir.path(), opts).expect("open must repair the WAL");
    assert_eq!(counter(&reg, "prometheus_tsdb_wal_corruptions_total"), 1.0);
    assert_eq!(db.head().num_series(), 1);
    assert_eq!(db.head().max_time(), 10_000);
    assert_eq!(db.head().min_ooo_time(), i64::MAX, "WBL must not be replayed");
    db.close().unwrap();
    drop(db);
    // The WBL itself is untouched.
    assert_eq!(count_records(&dir.path().join("wbl")), 1);
}
