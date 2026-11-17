//! DB-level behaviour ported from upstream Prometheus TSDB tests.
//!
//! Ported from github.com/prometheus/prometheus/tsdb/db_test.go (`TestTimeRetention`,
//! `TestOpen_VariousBlockStates`, `TestBiggerBlocksForOldOOOData`) and
//! github.com/prometheus/prometheus/tsdb/head_test.go (`TestHeadCompactable`,
//! `TestHead_TruncateWAL_IncrementsCorruptionMetricOnCorruptedSegment`), plus the OOO
//! compaction block boundaries of `DB.compactOOO`.

use std::fs;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ulid::Ulid;

use crate::block::{read_meta_file, write_meta_file, BlockDesc, BlockMetaCompaction};
use crate::chunkenc::{EncodedChunk, XorChunk};
use crate::chunks_block::ChunkMeta;
use crate::compact::{write_block_from_series, CompactionSeries, SampleValue};
use crate::db::{Tsdb, TsdbOptions};
use crate::head::{Head, HeadOptions};
use crate::labels::Labels;
use crate::matcher::{LabelMatcher, MatchType};
use crate::record::{RecordEncoder, RefSeries};
use crate::wlog::{CompressionType, WlogWriter};

const HOUR: i64 = 60 * 60 * 1000;
const DAY: i64 = 24 * HOUR;

fn metric(reg: &prometheus::Registry, name: &str) -> f64 {
    reg.gather()
        .into_iter()
        .find(|mf| mf.get_name() == name)
        .map(|mf| {
            mf.get_metric()
                .iter()
                .map(|m| {
                    if mf.get_field_type() == prometheus::proto::MetricType::GAUGE {
                        m.get_gauge().get_value()
                    } else {
                        m.get_counter().get_value()
                    }
                })
                .sum()
        })
        .unwrap_or_else(|| panic!("metric {name} not registered"))
}

/// Go `createBlock(t, dir, genSeries(1, 1, mint, maxt))`: one series with samples at
/// `mint` and `maxt - 1`, block meta `[mint, maxt)`.
fn create_block(dir: &Path, mint: i64, maxt: i64) -> PathBuf {
    let mut xc = XorChunk::new();
    {
        let mut app = xc.appender().unwrap();
        app.append(0, mint, 1.0).unwrap();
        if maxt - 1 > mint {
            app.append(0, maxt - 1, 2.0).unwrap();
        }
    }
    let meta = write_block_from_series(
        dir,
        mint,
        maxt,
        vec![CompactionSeries {
            labels: Labels::from_pairs([("__name__", "m")]),
            chunks: vec![ChunkMeta {
                ref_id: 0,
                min_time: mint,
                max_time: maxt - 1,
                chunk: Some(EncodedChunk::Xor(xc)),
            }],
            tombstones: Vec::new(),
        }],
        BlockMetaCompaction::default(),
        false,
        0,
    )
    .unwrap()
    .unwrap();
    dir.join(meta.ulid.to_string())
}

fn opts_with_registry(reg: &Arc<prometheus::Registry>) -> TsdbOptions {
    let mut opts = TsdbOptions::default();
    opts.head.registry = Some(Arc::clone(reg));
    opts
}

/// Go `TestTimeRetention`.
#[test]
fn time_retention() {
    type Case<'a> = (&'a str, &'a [(i64, i64)], &'a [(i64, i64)], i64);
    let cases: [Case; 2] = [
        (
            "Block max time delta greater than retention duration",
            &[(500, 900), (1000, 1500), (1500, 2000)],
            &[(1000, 1500), (1500, 2000)],
            1000,
        ),
        (
            "Block max time delta equal to retention duration",
            &[(500, 900), (1000, 1500), (1500, 2000)],
            &[(1500, 2000)],
            500,
        ),
    ];
    for (name, blocks, exp, retention) in cases {
        let dir = tempfile::tempdir().unwrap();
        for &(mint, maxt) in blocks {
            create_block(dir.path(), mint, maxt);
        }
        let reg = Arc::new(prometheus::Registry::new());
        let mut opts = opts_with_registry(&reg);
        opts.retention_duration_ms = retention;
        opts.head.chunk_range_ms = 1000;
        let db = Tsdb::open(dir.path(), opts).unwrap();
        let act: Vec<(i64, i64)> = db
            .blocks()
            .iter()
            .map(|b| (b.min_time(), b.max_time()))
            .collect();
        assert_eq!(act, exp.to_vec(), "{name}");
        assert_eq!(metric(&reg, "prometheus_tsdb_time_retentions_total"), 1.0, "{name}");
        assert_eq!(metric(&reg, "prometheus_tsdb_blocks_loaded"), exp.len() as f64, "{name}");
        assert_eq!(metric(&reg, "prometheus_tsdb_reloads_total"), 1.0, "{name}");
        let bytes: u64 = db.blocks().iter().map(|b| b.size()).sum();
        assert_eq!(metric(&reg, "prometheus_tsdb_storage_blocks_bytes"), bytes as f64);
        assert!(bytes > 0);
        // Deleted blocks are gone from disk, without leftover `.tmp-for-deletion` dirs.
        let names: Vec<String> = fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .filter(|n| n != "wal" && n != "chunks_head")
            .collect();
        assert_eq!(names.len(), exp.len(), "{name}: {names:?}");
        db.close().unwrap();
    }
}

/// Go `TestOpen_VariousBlockStates` (without the Prometheus 2.1 index-version repair case).
#[test]
fn open_various_block_states() {
    let dir = tempfile::tempdir().unwrap();
    let tmp = dir.path();
    let mut expected_loaded = vec![create_block(tmp, 0, 10), create_block(tmp, 10, 20)];
    let mut expected_removed = Vec::new();

    // Missing meta.json: ignored and only logged.
    let ignored = create_block(tmp, 20, 30);
    fs::remove_file(ignored.join("meta.json")).unwrap();

    // Tmp blocks during creation / deletion and pre-2.21 tmp blocks are removed on start.
    for (mint, suffix) in [(30, ".tmp-for-creation"), (40, ".tmp-for-deletion"), (50, ".tmp")] {
        let d = create_block(tmp, mint, mint + 10);
        let mut t = d.clone().into_os_string();
        t.push(suffix);
        fs::rename(&d, &t).unwrap();
        expected_removed.push(PathBuf::from(t));
    }

    // One ok block, but two should be replaced (parents).
    let ok = create_block(tmp, 50, 60);
    expected_loaded.push(ok.clone());
    let compacted = create_block(tmp, 50, 55);
    expected_removed.push(compacted.clone());
    let mut m = read_meta_file(&ok).unwrap();
    let compacted_id: Ulid = compacted.file_name().unwrap().to_str().unwrap().parse().unwrap();
    for id in [
        compacted_id,
        Ulid::from_parts(1, 0),
        Ulid::from_parts(123, 0),
        compacted_id,
        compacted_id,
    ] {
        m.compaction.parents.push(BlockDesc {
            ulid: id,
            min_time: 0,
            max_time: 0,
        });
    }
    write_meta_file(&ok, &mut m).unwrap();

    let tmp_checkpoint = tmp.join("wal/checkpoint.00000001.tmp");
    fs::create_dir_all(&tmp_checkpoint).unwrap();
    let tmp_chunk_snapshot = tmp.join("chunk_snapshot.0000.00000001.tmp");
    fs::create_dir_all(&tmp_chunk_snapshot).unwrap();

    let db = Tsdb::open(tmp, TsdbOptions::default()).unwrap();
    let mut loaded: Vec<PathBuf> = db.blocks().iter().map(|b| tmp.join(b.ulid().to_string())).collect();
    loaded.sort();
    expected_loaded.sort();
    assert_eq!(loaded, expected_loaded);
    db.close().unwrap();
    drop(db);

    for r in &expected_removed {
        assert!(!r.exists(), "{} should have been removed", r.display());
    }
    assert!(ignored.exists(), "dir without meta.json is ignored, not removed");
    assert!(!tmp_checkpoint.exists());
    assert!(!tmp_chunk_snapshot.exists());
}

/// Go `TestHeadCompactable` (chunk range 20).
#[test]
fn head_compactable() {
    let cases = [
        ("with timely compaction, compaction allowed", true, 19, 31, true),
        ("with timely compaction, compaction not allowed", true, 9, 17, false),
        ("without timely compaction, compaction allowed", false, 1, 32, true),
        ("without timely compaction, compaction not allowed", false, 3, 19, false),
    ];
    for (name, timely, mint, maxt, exp) in cases {
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            chunk_range_ms: 20,
            timely_compaction: timely,
            ..HeadOptions::default()
        };
        let head = Head::open(dir.path(), opts, i64::MIN).unwrap();
        let mut app = head.appender();
        app.append_float(None, &[("a", "b")], mint, 0.0).unwrap();
        app.append_float(None, &[("a", "b")], maxt, 0.0).unwrap();
        app.commit().unwrap();
        assert_eq!(head.compactable(), exp, "{name}");
        head.close().unwrap();
    }
}

fn select_all(db: &Tsdb, mint: i64, maxt: i64) -> Vec<(i64, f64)> {
    let m = LabelMatcher::new(MatchType::Equal, "foo", "bar").unwrap();
    let res = db.querier(mint, maxt).select(true, &[m]).unwrap();
    res.into_iter()
        .flat_map(|s| s.samples)
        .map(|s| match s.value {
            SampleValue::Float(v) => (s.t, v),
            _ => unreachable!(),
        })
        .collect()
}

/// `DB.compactOOO`: one OOO block per aligned block range between the OOO head's min and
/// max time (not a single block over the whole OOO range).
#[test]
fn ooo_compaction_writes_one_block_per_aligned_range() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = TsdbOptions::default();
    opts.head.ooo_time_window_ms = 10 * DAY;
    let db = Tsdb::open(dir.path(), opts).unwrap();
    let base = 100 * DAY;
    {
        let mut app = db.appender();
        app.append_float(None, &[("foo", "bar")], base + 10 * HOUR, 0.0).unwrap();
        app.commit().unwrap();
    }
    // OOO samples crossing three 2h block boundaries: [base+1h, base+6h30m].
    let mut exp = Vec::new();
    let mut t = base + HOUR;
    while t <= base + 6 * HOUR + 30 * 60 * 1000 {
        let mut app = db.appender();
        app.append_float(None, &[("foo", "bar")], t, t as f64).unwrap();
        app.commit().unwrap();
        exp.push((t, t as f64));
        t += 30 * 60 * 1000;
    }
    let uids = db.compact_ooo_head().unwrap();
    assert_eq!(uids.len(), 4);
    let mut ranges: Vec<(i64, i64)> = db
        .blocks()
        .iter()
        .map(|b| {
            assert!(b.meta().compaction.from_out_of_order());
            (b.min_time(), b.max_time())
        })
        .collect();
    ranges.sort();
    assert_eq!(
        ranges,
        vec![
            (base, base + 2 * HOUR),
            (base + 2 * HOUR, base + 4 * HOUR),
            (base + 4 * HOUR, base + 6 * HOUR),
            (base + 6 * HOUR, base + 8 * HOUR),
        ]
    );
    let mut got = select_all(&db, base, base + 7 * HOUR);
    got.sort_by_key(|s| s.0);
    assert_eq!(got, exp);
    db.close().unwrap();
}

/// Go `TestBiggerBlocksForOldOOOData`.
#[test]
fn bigger_blocks_for_old_ooo_data() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = TsdbOptions::default();
    opts.head.ooo_time_window_ms = 10 * DAY;
    opts.enable_bigger_ooo_block_for_old_samples = true;
    let curr_ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let curr_day_start = DAY * (curr_ts / DAY);
    let db = Tsdb::open(dir.path(), opts).unwrap();

    // 1 in-order sample.
    let in_order_ts = curr_day_start + 6 * HOUR;
    {
        let mut app = db.appender();
        app.append_float(None, &[("foo", "bar")], in_order_ts, in_order_ts as f64)
            .unwrap();
        app.commit().unwrap();
    }
    // OOO samples till 5 days ago.
    let mut exp = Vec::new();
    let mut ts = curr_day_start - 5 * DAY;
    while ts < in_order_ts {
        let mut app = db.appender();
        app.append_float(None, &[("foo", "bar")], ts, ts as f64).unwrap();
        app.commit().unwrap();
        exp.push((ts, ts as f64));
        ts += HOUR;
    }
    assert!(db.blocks().is_empty());
    db.compact_ooo_head().unwrap();
    // 5 OOO blocks from the last 5 days + 3 OOO blocks for the 6h of curr day (2h blocks).
    let blocks = db.blocks();
    assert_eq!(blocks.len(), 8);
    let day_blocks = blocks.iter().filter(|b| b.max_time() - b.min_time() == DAY).count();
    assert_eq!(day_blocks, 5);
    for b in &blocks {
        let size = b.max_time() - b.min_time();
        assert_eq!(b.min_time() % size, 0, "block not aligned to its range");
    }
    let mut got = select_all(&db, in_order_ts - 6 * DAY, in_order_ts + 1);
    got.sort_by_key(|s| s.0);
    exp.push((in_order_ts, in_order_ts as f64));
    assert_eq!(got, exp);
    db.close().unwrap();
}

/// Go `TestHead_TruncateWAL_IncrementsCorruptionMetricOnCorruptedSegment`.
#[test]
fn truncate_wal_counts_corruption_and_failed_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let reg = Arc::new(prometheus::Registry::new());
    let opts = HeadOptions {
        chunk_range_ms: 1000,
        registry: Some(Arc::clone(&reg)),
        ..HeadOptions::default()
    };
    // Several WAL segments so that truncation has a range to checkpoint.
    {
        let mut w = WlogWriter::open(dir.path().join("wal"), CompressionType::None).unwrap();
        let rec = RecordEncoder::new(false).series(
            &[RefSeries {
                ref_id: 1,
                labels: Labels::from_pairs([("a", "b")]),
            }],
            Vec::new(),
        );
        for _ in 0..4 {
            w.log(&[&rec]).unwrap();
            w.next_segment().unwrap();
        }
        w.close().unwrap();
    }
    let head = Head::open(dir.path(), opts, 0).unwrap();
    assert_eq!(metric(&reg, "prometheus_tsdb_wal_corruptions_total"), 0.0);

    // Corrupt segment 0 by flipping a byte inside the first record's data.
    let f = fs::OpenOptions::new()
        .write(true)
        .open(dir.path().join("wal/00000000"))
        .unwrap();
    f.write_all_at(&[0xff], 7).unwrap();
    drop(f);

    let err = head.truncate_wal(100).expect_err("checkpoint over a corrupted segment");
    assert!(err.to_string().contains("corruption"), "{err}");
    assert_eq!(metric(&reg, "prometheus_tsdb_wal_corruptions_total"), 1.0);
    assert_eq!(metric(&reg, "prometheus_tsdb_checkpoint_creations_total"), 1.0);
    assert_eq!(metric(&reg, "prometheus_tsdb_checkpoint_creations_failed_total"), 1.0);
    assert_eq!(metric(&reg, "prometheus_tsdb_checkpoint_deletions_total"), 0.0);
    head.close().unwrap();
}

/// `truncateWAL` success path: checkpoint + WAL truncation + checkpoint deletion counters.
#[test]
fn truncate_wal_success_metrics() {
    let dir = tempfile::tempdir().unwrap();
    let reg = Arc::new(prometheus::Registry::new());
    let opts = HeadOptions {
        chunk_range_ms: 1000,
        registry: Some(Arc::clone(&reg)),
        ..HeadOptions::default()
    };
    {
        let mut w = WlogWriter::open(dir.path().join("wal"), CompressionType::None).unwrap();
        let rec = RecordEncoder::new(false).series(
            &[RefSeries {
                ref_id: 1,
                labels: Labels::from_pairs([("a", "b")]),
            }],
            Vec::new(),
        );
        for _ in 0..6 {
            w.log(&[&rec]).unwrap();
            w.next_segment().unwrap();
        }
        w.close().unwrap();
    }
    let head = Head::open(dir.path(), opts, 0).unwrap();
    head.truncate_wal(100).unwrap();
    assert_eq!(metric(&reg, "prometheus_tsdb_checkpoint_creations_total"), 1.0);
    assert_eq!(metric(&reg, "prometheus_tsdb_checkpoint_creations_failed_total"), 0.0);
    assert_eq!(metric(&reg, "prometheus_tsdb_checkpoint_deletions_total"), 1.0);
    assert_eq!(metric(&reg, "prometheus_tsdb_wal_truncations_total"), 1.0);
    assert_eq!(metric(&reg, "prometheus_tsdb_wal_truncations_failed_total"), 0.0);
    let s = reg
        .gather()
        .into_iter()
        .find(|mf| mf.get_name() == "prometheus_tsdb_wal_truncate_duration_seconds")
        .unwrap();
    assert_eq!(s.get_metric()[0].get_summary().get_sample_count(), 1);
    // A second truncation to the same time is a no-op.
    head.truncate_wal(100).unwrap();
    assert_eq!(metric(&reg, "prometheus_tsdb_checkpoint_creations_total"), 1.0);
    head.close().unwrap();
}
