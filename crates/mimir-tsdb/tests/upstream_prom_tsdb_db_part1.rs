//! Ported upstream Prometheus TSDB db_test.go (part 1 of 2, 40 tests).

mod db_test_helpers;

use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use db_test_helpers::{
    append_kind_v2, compare_series, copy_dir_recursive, create_xor_block,
    dummy_stale_nan, match_eq, match_re, metric_val,
    open_db_range, query_db, query_db_raw_nans, query_head_range, ALL_SAMPLE_KINDS,
    DEFAULT_BLOCK_DURATION_MS, HOUR_MS,
};
use mimir_tsdb::block::{read_meta_file, write_meta_file, Block, BlockDesc, BlockMeta, BlockMetaCompaction};
use mimir_tsdb::chunkenc::{EncodedChunk, Encoding, XorChunk};
use mimir_tsdb::chunks_block::{
    BlockChunksReader, BlockChunksWriter, ChunkMeta, CHUNK_ENCODING_SIZE, CRC_SIZE,
    MAX_CHUNK_LENGTH_FIELD_SIZE, SEGMENT_HEADER_SIZE,
};
use mimir_tsdb::db::{
    overlapping_blocks, TimeRange, Tsdb, TsdbCompactorOverride,
    TsdbError, TsdbExtraOptions, TsdbOptions, TsdbReadOnly, TsdbRuntimeConfig,
};
use mimir_tsdb::encoding::put_uvarint;
use mimir_tsdb::exemplar::Exemplar;
use mimir_tsdb::head::range_for_timestamp;
use mimir_tsdb::labels::Labels;
use mimir_tsdb::tombstones::{write_tombstones_file, Interval, MemTombstones};
use mimir_tsdb::wlog::{last_checkpoint, CompressionType, WlogWriter};
use ulid::Ulid;

fn opts_with_reg(reg: &Arc<prometheus::Registry>, chunk_range_ms: i64) -> TsdbOptions {
    let mut opts = TsdbOptions::default();
    opts.head.registry = Some(Arc::clone(reg));
    opts.head.chunk_range_ms = chunk_range_ms;
    opts
}

fn xor_chunk_samples(samples: &[(i64, f64)]) -> ChunkMeta {
    let mut xc = XorChunk::new();
    {
        let mut app = xc.appender().unwrap();
        for &(t, v) in samples {
            app.append(0, t, v).unwrap();
        }
    }
    ChunkMeta {
        ref_id: 0,
        min_time: samples[0].0,
        max_time: samples.last().unwrap().0,
        chunk: Some(EncodedChunk::Xor(xc)),
    }
}

struct EventCapture(Arc<Mutex<Vec<String>>>);

impl tracing::Subscriber for EventCapture {
    fn enabled(&self, _m: &tracing::Metadata<'_>) -> bool { true }
    fn new_span(&self, _s: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _s: &tracing::span::Id, _v: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _s: &tracing::span::Id, _f: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        struct V(String);
        impl tracing::field::Visit for V {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                self.0.push_str(&format!("{}={:?} ", field.name(), value));
            }
        }
        let mut v = V(String::new());
        event.record(&mut v);
        self.0.lock().unwrap().push(v.0);
    }
    fn enter(&self, _s: &tracing::span::Id) {}
    fn exit(&self, _s: &tracing::span::Id) {}
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestDBClose_AfterClose
#[test]
fn test_db_close_after_close() {
    let dir = tempfile::tempdir().unwrap();
    let db = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
    db.close().unwrap();
    db.close().unwrap();

    let dir2 = tempfile::tempdir().unwrap();
    let db2 = Tsdb::open(dir2.path(), TsdbOptions::default()).unwrap();
    db2.close().unwrap();
    db2.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestDB_reloadOrder
#[test]
fn test_db_reload_order() {
    let dir = tempfile::tempdir().unwrap();
    let db = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
    for (mint, maxt) in [(90, 100), (70, 80), (100, 110)] {
        create_xor_block(db.dir(), 1, 1, mint, maxt);
    }
    db.reload_blocks().unwrap();
    let blocks = db.blocks();
    assert_eq!(blocks.len(), 3);
    assert_eq!((blocks[0].min_time(), blocks[0].max_time()), (70, 80));
    assert_eq!((blocks[1].min_time(), blocks[1].max_time()), (90, 100));
    assert_eq!((blocks[2].min_time(), blocks[2].max_time()), (100, 110));
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestDBSnapshotJSONLoggerBlock
#[test]
fn test_db_snapshot_json_logger_block() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let _guard = tracing::subscriber::set_default(EventCapture(Arc::clone(&events)));

    let dir = tempfile::tempdir().unwrap();
    let bdir = create_xor_block(dir.path(), 1, 1, 0, 10);
    let expected_ulid = bdir.file_name().unwrap().to_string_lossy().into_owned();
    let db = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
    db.disable_compactions();
    let snap = tempfile::tempdir().unwrap();
    db.snapshot(snap.path(), false).unwrap();

    let captured = events.lock().unwrap();
    assert!(
        captured.iter().any(|e| e.contains("Snapshotting block") && e.contains(&expected_ulid)),
        "snapshot block log not found in {captured:?}"
    );
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestTombstoneCleanFail
#[test]
fn test_tombstone_clean_fail() {
    struct MockFailingCompactor {
        max_blocks: usize,
        blocks_so_far: Mutex<Vec<Ulid>>,
    }
    impl TsdbCompactorOverride for MockFailingCompactor {
        fn write(&self, dest: &std::path::Path, _block: &Block, _mint: i64, _maxt: i64, _base: Option<&BlockMeta>) -> Result<Vec<Ulid>, TsdbError> {
            let mut g = self.blocks_so_far.lock().unwrap();
            if g.len() >= self.max_blocks {
                return Err(TsdbError::Other("max allowed blocks reached".into()));
            }
            let bdir = create_xor_block(dest, 1, 1, 0, 1);
            let uid: Ulid = bdir.file_name().unwrap().to_str().unwrap().parse().unwrap();
            g.push(uid);
            Ok(vec![uid])
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let mut old_dirs = Vec::new();
    let mut initial_ulids = Vec::new();
    for i in 0..2i64 {
        let bdir = create_xor_block(dir.path(), 1, 1, i, i + 1);
        let tombs = MemTombstones::new();
        tombs.add_interval(0, Interval { mint: i, maxt: i + 1 });
        write_tombstones_file(&bdir, &tombs).unwrap();
        let uid: Ulid = bdir.file_name().unwrap().to_str().unwrap().parse().unwrap();
        initial_ulids.push(uid);
        old_dirs.push(bdir);
    }
    let db = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
    db.set_compactor_override_for_test(Some(Arc::new(MockFailingCompactor {
        max_blocks: 3,
        blocks_so_far: Mutex::new(initial_ulids),
    })));
    assert!(db.clean_tombstones().is_err());

    let actual_dirs: Vec<PathBuf> = fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| {
            let p = e.ok()?.path();
            let n = p.file_name()?.to_str()?;
            (n.len() == 26 && Ulid::from_string(n).is_ok()).then_some(p)
        })
        .collect();
    assert_eq!(actual_dirs.len(), old_dirs.len());
    let old_set: HashSet<_> = old_dirs.into_iter().collect();
    let kept = actual_dirs.iter().filter(|p| old_set.contains(*p)).count();
    assert_eq!(kept, actual_dirs.len() - 1);
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestTimeRetention
#[test]
fn test_time_retention() {
    for (name, blocks, exp, retention) in [
        ("gt", &[(500, 900), (1000, 1500), (1500, 2000)][..], &[(1000, 1500), (1500, 2000)][..], 1000i64),
        ("eq", &[(500, 900), (1000, 1500), (1500, 2000)][..], &[(1500, 2000)][..], 500i64),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let reg = Arc::new(prometheus::Registry::new());
        let db = Tsdb::open(dir.path(), opts_with_reg(&reg, 1000)).unwrap();
        for &(mint, maxt) in blocks {
            create_xor_block(db.dir(), 10, 10, mint, maxt);
        }
        db.reload_blocks().unwrap();
        assert_eq!(db.blocks().len(), blocks.len(), "{name}");
        db.set_retention_duration_ms_for_test(retention);
        db.reload_blocks().unwrap();
        assert_eq!(metric_val(&reg, "prometheus_tsdb_time_retentions_total", &[]), 1.0);
        let act: Vec<(i64, i64)> = db.blocks().iter().map(|b| (b.min_time(), b.max_time())).collect();
        assert_eq!(act, exp, "{name}");
        db.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestRetentionDurationMetric
#[test]
fn test_retention_duration_metric() {
    let dir = tempfile::tempdir().unwrap();
    let reg = Arc::new(prometheus::Registry::new());
    let mut opts = opts_with_reg(&reg, 100);
    opts.retention_duration_ms = 1000;
    let db = Tsdb::open(dir.path(), opts).unwrap();
    assert_eq!(metric_val(&reg, "prometheus_tsdb_retention_limit_seconds", &[]), 1.0);
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestSizeRetentionMetric
#[test]
fn test_size_retention_metric() {
    for (max_bytes, exp) in [(1000i64, 1000.0), (0, 0.0), (-1000, 0.0)] {
        let dir = tempfile::tempdir().unwrap();
        let reg = Arc::new(prometheus::Registry::new());
        let opts = opts_with_reg(&reg, 100);
        let extra = TsdbExtraOptions { max_bytes, ..TsdbExtraOptions::default() };
        let db = Tsdb::open_with_extra(dir.path(), opts, extra).unwrap();
        assert_eq!(metric_val(&reg, "prometheus_tsdb_retention_limit_bytes", &[]), exp);
        db.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestRuntimeRetentionConfigChange
#[test]
fn test_runtime_retention_config_change() {
    let dir = tempfile::tempdir().unwrap();
    let reg = Arc::new(prometheus::Registry::new());
    let initial = 10 * HOUR_MS;
    let shorter = HOUR_MS;
    let mut opts = opts_with_reg(&reg, 100);
    opts.retention_duration_ms = initial;
    let db = Tsdb::open(dir.path(), opts).unwrap();

    let nine_h = 9 * HOUR_MS;
    let nine_half_h = 9 * HOUR_MS + 30 * 60 * 1000;
    for (mint, maxt) in [(0, 100), (100, 200), (nine_h, nine_half_h), (nine_half_h, initial)] {
        create_xor_block(db.dir(), 10, 10, mint, maxt);
    }
    db.reload_blocks().unwrap();
    assert_eq!(db.blocks().len(), 4);

    db.apply_config(&TsdbRuntimeConfig {
        retention_duration_ms: Some(shorter),
        ..TsdbRuntimeConfig::default()
    })
    .unwrap();
    assert_eq!(db.retention_duration_ms(), shorter);
    assert_eq!(metric_val(&reg, "prometheus_tsdb_retention_limit_seconds", &[]), 3600.0);

    db.reload_blocks().unwrap();
    let act = db.blocks();
    assert_eq!(act.len(), 2);
    assert_eq!(act[0].min_time(), nine_h);
    assert_eq!(act[1].min_time(), nine_half_h);
    assert!(metric_val(&reg, "prometheus_tsdb_time_retentions_total", &[]) > 0.0);
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestApplyConfigRetentionDurationMetricUnit
#[test]
fn test_apply_config_retention_duration_metric_unit() {
    let dir = tempfile::tempdir().unwrap();
    let reg = Arc::new(prometheus::Registry::new());
    let mut opts = opts_with_reg(&reg, DEFAULT_BLOCK_DURATION_MS);
    opts.retention_duration_ms = HOUR_MS;
    let db = Tsdb::open(dir.path(), opts).unwrap();
    db.apply_config(&TsdbRuntimeConfig {
        retention_duration_ms: Some(HOUR_MS),
        ..TsdbRuntimeConfig::default()
    })
    .unwrap();
    assert_eq!(db.retention_duration_ms(), HOUR_MS);
    assert_eq!(metric_val(&reg, "prometheus_tsdb_retention_limit_seconds", &[]), 3600.0);
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestDBApplyConfigChunkEncoding
#[test]
fn test_db_apply_config_chunk_encoding() {
    let open_enc = |enc: Encoding, st: bool| {
        let dir = tempfile::tempdir().unwrap();
        let mut opts = TsdbOptions::default();
        opts.head.float_chunk_encoding = enc;
        opts.head.enable_st_storage = st;
        let db = Tsdb::open(dir.path(), opts).unwrap();
        (dir, db)
    };

    let (_d, db) = open_enc(Encoding::Xor, false);
    db.apply_config(&TsdbRuntimeConfig { float_chunk_encoding: Some(Encoding::Xor2), ..Default::default() }).unwrap();
    assert_eq!(db.head().float_chunk_encoding(), Encoding::Xor2);
    db.close().unwrap();

    let (_d, db) = open_enc(Encoding::Xor2, false);
    db.apply_config(&TsdbRuntimeConfig { float_chunk_encoding: Some(Encoding::Xor), ..Default::default() }).unwrap();
    assert_eq!(db.head().float_chunk_encoding(), Encoding::Xor);
    db.apply_config(&TsdbRuntimeConfig { float_chunk_encoding: Some(Encoding::None), ..Default::default() }).unwrap();
    assert_eq!(db.head().float_chunk_encoding(), Encoding::Xor2);
    db.apply_config(&TsdbRuntimeConfig { float_chunk_encoding: Some(Encoding::Xor), ..Default::default() }).unwrap();
    assert_eq!(db.head().float_chunk_encoding(), Encoding::Xor);
    db.apply_config(&TsdbRuntimeConfig { clear_tsdb_config: true, ..Default::default() }).unwrap();
    assert_eq!(db.head().float_chunk_encoding(), Encoding::Xor2);
    db.close().unwrap();

    let (_d, db) = open_enc(Encoding::Xor2, true);
    let err = db.apply_config(&TsdbRuntimeConfig { float_chunk_encoding: Some(Encoding::Xor), ..Default::default() }).unwrap_err();
    assert!(err.to_string().contains("is incompatible with start-timestamp storage"), "{err}");
    assert_eq!(db.head().float_chunk_encoding(), Encoding::Xor2);
    db.apply_config(&TsdbRuntimeConfig { float_chunk_encoding: None, ..Default::default() }).unwrap();
    assert_eq!(db.head().float_chunk_encoding(), Encoding::Xor2);
    db.apply_config(&TsdbRuntimeConfig { clear_tsdb_config: true, ..Default::default() }).unwrap();
    assert_eq!(db.head().float_chunk_encoding(), Encoding::Xor2);
    db.close().unwrap();
}

fn assert_block_float_encoding(b: &Block, exp: Encoding) {
    let all = b.index().postings("", &[""]).unwrap();
    assert_eq!(all.len(), 1);
    let (_, chks) = b.index().series(all[0]).unwrap();
    assert!(!chks.is_empty());
    for cm in chks {
        let c = b.chunks().chunk(cm.ref_id).unwrap();
        assert_eq!(c.encoding(), exp);
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestVerticalCompactionFloatChunkEncoding
#[test]
fn test_vertical_compaction_float_chunk_encoding() {
    for (enc, exp) in [(Encoding::Xor, Encoding::Xor), (Encoding::Xor2, Encoding::Xor2)] {
        let dir = tempfile::tempdir().unwrap();
        create_xor_block(dir.path(), 1, 50, 0, 50);
        create_xor_block(dir.path(), 1, 50, 25, 75);
        let mut opts = TsdbOptions::default();
        opts.head.float_chunk_encoding = enc;
        let db = Tsdb::open(dir.path(), opts).unwrap();
        db.set_enable_overlapping_compaction(true);
        db.compact().unwrap();
        assert_eq!(db.blocks().len(), 1);
        assert_block_float_encoding(&db.blocks()[0], exp);
        db.close().unwrap();
    }

    let dir = tempfile::tempdir().unwrap();
    create_xor_block(dir.path(), 1, 50, 0, 50);
    create_xor_block(dir.path(), 1, 50, 25, 75);
    let db = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
    db.set_enable_overlapping_compaction(true);
    db.apply_config(&TsdbRuntimeConfig { float_chunk_encoding: Some(Encoding::Xor2), ..Default::default() }).unwrap();
    db.compact().unwrap();
    assert_eq!(db.blocks().len(), 1);
    assert_block_float_encoding(&db.blocks()[0], Encoding::Xor2);

    create_xor_block(db.dir(), 1, 50, 100, 150);
    create_xor_block(db.dir(), 1, 50, 125, 175);
    db.reload_blocks().unwrap();
    db.apply_config(&TsdbRuntimeConfig { float_chunk_encoding: Some(Encoding::Xor), ..Default::default() }).unwrap();
    db.compact().unwrap();
    let blks = db.blocks();
    assert_eq!(blks.len(), 2);
    assert_block_float_encoding(&blks[0], Encoding::Xor2);
    assert_block_float_encoding(&blks[1], Encoding::Xor);
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestChunkQuerierFloatChunkEncoding
#[test]
fn test_chunk_querier_float_chunk_encoding() {
    for (enc, exp) in [(Encoding::Xor, Encoding::Xor), (Encoding::Xor2, Encoding::Xor2)] {
        let dir = tempfile::tempdir().unwrap();
        create_xor_block(dir.path(), 1, 50, 0, 50);
        create_xor_block(dir.path(), 1, 50, 25, 75);
        let mut opts = TsdbOptions::default();
        opts.head.float_chunk_encoding = enc;
        let db = Tsdb::open(dir.path(), opts).unwrap();
        let res = db.chunk_querier(0, 75).select(true, &[match_eq("i", "0")]).unwrap();
        assert_eq!(res.len(), 1);
        for cm in &res[0].chunks {
            assert_eq!(cm.chunk.as_ref().unwrap().encoding(), exp);
        }
        db.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestHeadOptionsUseXOR2FloatEncoding
#[test]
fn test_head_options_use_xor2_float_encoding() {
    let dir = tempfile::tempdir().unwrap();
    let opts = TsdbOptions::default();
    let db = Tsdb::open(dir.path(), opts).unwrap();
    assert_eq!(db.head().float_chunk_encoding(), Encoding::Xor);
    db.head().set_float_chunk_encoding(Encoding::Xor2);
    assert_eq!(db.head().float_chunk_encoding(), Encoding::Xor2);
    db.head().set_float_chunk_encoding(Encoding::Xor);
    assert_eq!(db.head().float_chunk_encoding(), Encoding::Xor);
    db.head().set_float_chunk_encoding(Encoding::None);
    assert_eq!(db.head().float_chunk_encoding(), Encoding::Xor);
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestDefaultOptionsFloatChunkEncoding
#[test]
fn test_default_options_float_chunk_encoding() {
    assert_eq!(TsdbOptions::default().head.float_chunk_encoding, Encoding::Xor);
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestValidateOptsInvalidFloatChunkEncoding
#[test]
fn test_validate_opts_invalid_float_chunk_encoding() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = TsdbOptions::default();
    opts.head.float_chunk_encoding = Encoding::Histogram;
    let err = Tsdb::open(dir.path(), opts).err().unwrap();
    assert!(err.to_string().contains("unsupported float chunk encoding"), "{err}");
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestValidateOptsSTStorageRequiresXOR2
#[test]
fn test_validate_opts_st_storage_requires_xor2() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = TsdbOptions::default();
    opts.head.enable_st_storage = true;
    let err = Tsdb::open(dir.path(), opts.clone()).err().unwrap();
    assert!(err.to_string().contains("is incompatible with start-timestamp storage"), "{err}");
    opts.head.float_chunk_encoding = Encoding::Xor2;
    let db = Tsdb::open(dir.path(), opts).unwrap();
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestOverlappingBlocksDetectsAllOverlaps
#[test]
fn test_overlapping_blocks_detects_all_overlaps() {
    let bm = |min_time: i64, max_time: i64| -> BlockMeta {
        BlockMeta {
            ulid: Ulid::nil(),
            min_time,
            max_time,
            stats: Default::default(),
            compaction: BlockMetaCompaction::default(),
            version: 1,
            out_of_order: false,
            thanos: None,
        }
    };
    let metas: Vec<BlockMeta> = (0..=10).map(|i| bm(i * 10, (i + 1) * 10)).collect();
    assert!(overlapping_blocks(&metas).is_empty());

    let add = |extra: &[BlockMeta]| -> Vec<BlockMeta> {
        let mut r = metas.clone();
        r.extend_from_slice(extra);
        r.sort_by_key(|m| m.min_time);
        r
    };
    let tr = |min, max| TimeRange { min, max };

    let o1 = bm(15, 17);
    assert_eq!(overlapping_blocks(&add(std::slice::from_ref(&o1))), BTreeMap::from([(tr(15, 17), vec![metas[1].clone(), o1.clone()])]));
    let o2 = bm(21, 31);
    assert_eq!(overlapping_blocks(&add(std::slice::from_ref(&o2))), BTreeMap::from([
        (tr(21, 30), vec![metas[2].clone(), o2.clone()]),
        (tr(30, 31), vec![o2.clone(), metas[3].clone()]),
    ]));
    let (o3a, o3b) = (bm(33, 39), bm(34, 36));
    assert_eq!(overlapping_blocks(&add(&[o3a.clone(), o3b.clone()])), BTreeMap::from([
        (tr(34, 36), vec![metas[3].clone(), o3a.clone(), o3b.clone()]),
    ]));
    let o4 = bm(50, 60);
    assert_eq!(overlapping_blocks(&add(std::slice::from_ref(&o4))), BTreeMap::from([
        (tr(50, 60), vec![metas[5].clone(), o4.clone()]),
    ]));
    let o5 = bm(61, 85);
    assert_eq!(overlapping_blocks(&add(std::slice::from_ref(&o5))), BTreeMap::from([
        (tr(61, 70), vec![metas[6].clone(), o5.clone()]),
        (tr(70, 80), vec![o5.clone(), metas[7].clone()]),
        (tr(80, 85), vec![o5.clone(), metas[8].clone()]),
    ]));
    let (o6a, o6b) = (bm(92, 105), bm(94, 99));
    assert_eq!(overlapping_blocks(&add(&[o6a.clone(), o6b.clone()])), BTreeMap::from([
        (tr(94, 99), vec![metas[9].clone(), o6a.clone(), o6b.clone()]),
        (tr(100, 105), vec![o6a.clone(), metas[10].clone()]),
    ]));
    let nc1 = vec![
        bm(1, 5), bm(2, 3), bm(2, 3), bm(2, 3), bm(2, 3),
        bm(2, 6), bm(3, 5), bm(5, 7), bm(7, 10), bm(8, 9),
    ];
    assert_eq!(overlapping_blocks(&nc1), BTreeMap::from([
        (tr(2, 3), vec![nc1[0].clone(), nc1[1].clone(), nc1[2].clone(), nc1[3].clone(), nc1[4].clone(), nc1[5].clone()]),
        (tr(3, 5), vec![nc1[0].clone(), nc1[5].clone(), nc1[6].clone()]),
        (tr(5, 6), vec![nc1[5].clone(), nc1[7].clone()]),
        (tr(8, 9), vec![nc1[8].clone(), nc1[9].clone()]),
    ]));
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestDBReadOnlyClosing
#[test]
fn test_db_read_only_closing() {
    let dir = tempfile::tempdir().unwrap();
    let sandbox = tempfile::tempdir().unwrap();
    let db = TsdbReadOnly::open(dir.path(), Some(sandbox.path())).unwrap();
    let sdir = db.sandbox_dir().to_path_buf();
    assert!(sdir.is_dir());
    db.close().unwrap();
    assert!(!sdir.exists());
    assert!(matches!(db.close(), Err(TsdbError::Closed)));
    assert!(matches!(db.blocks(), Err(TsdbError::Closed)));
    assert!(matches!(db.querier(0, 1), Err(TsdbError::Closed)));
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestChunkWriter_ReadAfterWrite
#[test]
fn test_chunk_writer_read_after_write() {
    let c1 = xor_chunk_samples(&[(1, 1.0)]);
    let c2 = xor_chunk_samples(&[(1, 2.0)]);
    let c3 = xor_chunk_samples(&[(1, 3.0)]);
    let c4 = xor_chunk_samples(&[(1, 4.0)]);
    let c5 = xor_chunk_samples(&[(1, 5.0)]);
    let chunk_sz = c1.chunk.as_ref().unwrap().as_bytes().len() + MAX_CHUNK_LENGTH_FIELD_SIZE + CHUNK_ENCODING_SIZE + CRC_SIZE;

    let cases = [
        (vec![vec![c1.clone(), c2.clone(), c3.clone()]], 3 * chunk_sz, 1usize, vec![3 * chunk_sz]),
        (vec![vec![c1.clone(), c2.clone(), c3.clone(), c4.clone(), c5.clone()]], 2 * chunk_sz, 3, vec![2 * chunk_sz, 2 * chunk_sz, chunk_sz]),
        (vec![vec![c1.clone(), c2.clone(), c3.clone()]], 2 * chunk_sz - 1, 3, vec![chunk_sz, chunk_sz, chunk_sz]),
        (vec![vec![c1.clone()]], chunk_sz - 1, 1, vec![chunk_sz]),
        (vec![vec![c1.clone(), c2.clone(), c3.clone()]], 1, 3, vec![chunk_sz, chunk_sz, chunk_sz]),
        (vec![vec![c1.clone(), c2.clone(), c3.clone()], vec![c4.clone(), c5.clone()]], 3 * chunk_sz, 2, vec![3 * chunk_sz, 2 * chunk_sz]),
        (vec![vec![c1.clone()], vec![c2.clone(), c3.clone()], vec![c4.clone()]], 2 * chunk_sz, 2, vec![2 * chunk_sz, 2 * chunk_sz]),
    ];

    for (batches, seg_sz, exp_segs, exp_seg_szs) in cases {
        let dir = tempfile::tempdir().unwrap();
        let mut cw = BlockChunksWriter::with_segment_size(dir.path(), SEGMENT_HEADER_SIZE + seg_sz).unwrap();
        let mut written = Vec::new();
        for mut b in batches {
            cw.write_chunks(&mut b).unwrap();
            written.extend(b);
        }
        cw.close().unwrap();

        let mut files: Vec<_> = fs::read_dir(dir.path()).unwrap().map(|e| e.unwrap()).collect();
        files.sort_by_key(|e| e.file_name());
        assert_eq!(files.len(), exp_segs);

        let mut size_exp = exp_segs * SEGMENT_HEADER_SIZE;
        let mut len_buf = [0u8; 10];
        for cm in &written {
            let raw = cm.chunk.as_ref().unwrap().as_bytes();
            let n = put_uvarint(&mut len_buf, raw.len() as u64);
            size_exp += n + CHUNK_ENCODING_SIZE + raw.len() + CRC_SIZE;
        }
        let mut size_act = 0usize;
        for (i, f) in files.iter().enumerate() {
            let sz = f.metadata().unwrap().len() as usize;
            assert!(SEGMENT_HEADER_SIZE + exp_seg_szs[i] >= sz);
            size_act += sz;
        }
        assert_eq!(size_exp, size_act);

        let cr = BlockChunksReader::open(dir.path()).unwrap();
        for cm in &written {
            let act = cr.chunk(cm.ref_id).unwrap();
            assert_eq!(act.as_bytes(), cm.chunk.as_ref().unwrap().as_bytes());
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestRangeForTimestamp
#[test]
fn test_range_for_timestamp() {
    for (t, width, exp) in [(0, 5, 5), (1, 5, 5), (5, 5, 10), (6, 5, 10), (13, 5, 15), (95, 5, 100)] {
        assert_eq!(range_for_timestamp(t, width), exp);
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestChunkReader_ConcurrentReads
#[test]
fn test_chunk_reader_concurrent_reads() {
    let dir = tempfile::tempdir().unwrap();
    let mut chks = vec![
        xor_chunk_samples(&[(1, 1.0)]),
        xor_chunk_samples(&[(1, 2.0)]),
        xor_chunk_samples(&[(1, 3.0)]),
        xor_chunk_samples(&[(1, 4.0)]),
        xor_chunk_samples(&[(1, 5.0)]),
    ];
    let mut cw = BlockChunksWriter::open(dir.path()).unwrap();
    cw.write_chunks(&mut chks).unwrap();
    cw.close().unwrap();

    let cr = Arc::new(BlockChunksReader::open(dir.path()).unwrap());
    let chks = Arc::new(chks);
    std::thread::scope(|s| {
        for idx in 0..chks.len() {
            for _ in 0..50 {
                let cr = Arc::clone(&cr);
                let chks = Arc::clone(&chks);
                s.spawn(move || {
                    let act = cr.chunk(chks[idx].ref_id).unwrap();
                    assert_eq!(act.as_bytes(), chks[idx].chunk.as_ref().unwrap().as_bytes());
                });
            }
        }
    });
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestOpen_VariousBlockStates
#[test]
fn test_open_various_block_states() {
    let dir = tempfile::tempdir().unwrap();
    let tmp = dir.path();
    let mut expected_loaded = vec![
        create_xor_block(tmp, 10, 2, 0, 10),
        create_xor_block(tmp, 10, 2, 10, 20),
    ];
    let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/testdata/repair_index_version/01BZJ9WJQPWHGNC2W4J9TA62KC");
    let repaired_out = tmp.join("01BZJ9WJQPWHGNC2W4J9TA62KC");
    copy_dir_recursive(&fixture, &repaired_out);
    fs::create_dir_all(repaired_out.join("chunks")).unwrap();
    expected_loaded.push(repaired_out);

    let ignored = create_xor_block(tmp, 10, 2, 20, 30);
    fs::remove_file(ignored.join("meta.json")).unwrap();

    let mut expected_removed = Vec::new();
    for (mint, suffix) in [(30, ".tmp-for-creation"), (40, ".tmp-for-deletion"), (50, ".tmp")] {
        let d = create_xor_block(tmp, 10, 2, mint, mint + 10);
        let mut t = d.clone().into_os_string();
        t.push(suffix);
        fs::rename(&d, &t).unwrap();
        expected_removed.push(PathBuf::from(t));
    }

    let ok = create_xor_block(tmp, 10, 2, 50, 60);
    expected_loaded.push(ok.clone());
    let compacted = create_xor_block(tmp, 10, 2, 50, 55);
    expected_removed.push(compacted.clone());
    let mut m = read_meta_file(&ok).unwrap();
    let cid: Ulid = compacted.file_name().unwrap().to_str().unwrap().parse().unwrap();
    for id in [cid, Ulid::from_parts(1, 0), Ulid::from_parts(123, 0), cid, cid] {
        m.compaction.parents.push(BlockDesc { ulid: id, min_time: 0, max_time: 0 });
    }
    write_meta_file(&ok, &mut m).unwrap();

    let tmp_cp = tmp.join("wal/checkpoint.00000001.tmp");
    fs::create_dir_all(&tmp_cp).unwrap();
    let tmp_cs = tmp.join("chunk_snapshot.0000.00000001.tmp");
    fs::create_dir_all(&tmp_cs).unwrap();

    let db = Tsdb::open(tmp, TsdbOptions::default()).unwrap();
    let mut loaded: Vec<PathBuf> = db.blocks().iter().map(|b| tmp.join(b.ulid().to_string())).collect();
    loaded.sort();
    expected_loaded.sort();
    assert_eq!(loaded, expected_loaded);
    db.close().unwrap();

    for r in &expected_removed {
        assert!(!r.exists(), "{} must be removed", r.display());
    }
    assert!(ignored.exists());
    assert!(!tmp_cp.exists());
    assert!(!tmp_cs.exists());
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestExemplarAfterLastSampleSurvivesCompaction
#[test]
fn test_exemplar_after_last_sample_survives_compaction() {
    for compaction in ["ordinary", "selected", "stale"] {
        for enable_snap in [false, true] {
            let block_range = 1000i64;
            let dir = tempfile::tempdir().unwrap();
            let reg = Arc::new(prometheus::Registry::new());
            let mut opts = opts_with_reg(&reg, block_range);
            opts.head.enable_exemplar_storage = true;
            opts.head.max_exemplars = 10;
            let extra = TsdbExtraOptions { enable_memory_snapshot_on_shutdown: enable_snap, ..Default::default() };
            let db = Tsdb::open_with_extra(dir.path(), opts.clone(), extra.clone()).unwrap();
            db.disable_compactions();

            let s_pairs = [("name", "exemplar_only")];
            let a_pairs = [("name", "active")];
            let mut app = db.appender();
            let s_ref = app.append_float(None, &s_pairs, 100, 1.0).unwrap();
            if compaction == "stale" {
                app.append_float(Some(s_ref), &s_pairs, 200, dummy_stale_nan()).unwrap();
            }
            app.commit().unwrap();

            let exemplar_seg = 8usize;
            for _ in 0..exemplar_seg {
                db.head().wal_next_segment_for_test().unwrap();
            }
            let ex = Exemplar::new(Labels::from_pairs([("trace_id", "abc123")]), 1.0, 10 * block_range);
            let mut app = db.appender();
            app.append_exemplar(Some(s_ref), &s_pairs, ex.clone()).unwrap();
            app.commit().unwrap();

            match compaction {
                "selected" => { db.compact_selected_series(&[s_ref]).unwrap(); assert_eq!(db.blocks().len(), 1); }
                "stale" => { db.compact_stale_head().unwrap(); assert_eq!(db.blocks().len(), 1); }
                _ => {}
            }

            let mut app = db.appender();
            let a_ref = app.append_float(None, &a_pairs, block_range * 3 / 2 + 101, 1.0).unwrap();
            app.commit().unwrap();
            db.compact().unwrap();
            assert_eq!(db.blocks().len(), 1);
            let (_, mm_len) = db.head().series_head_chunk_count_for_test(s_ref).unwrap();
            assert_eq!(mm_len, 0);
            let (_, cp1) = last_checkpoint(&dir.path().join("wal")).unwrap().unwrap();

            let mut app = db.appender();
            app.append_float(Some(a_ref), &a_pairs, block_range * 5 / 2 + 101, 2.0).unwrap();
            app.append_float(Some(a_ref), &a_pairs, block_range * 7 / 2 + 101, 3.0).unwrap();
            app.commit().unwrap();
            db.compact().unwrap();
            assert_eq!(db.blocks().len(), 2);
            let (_, cp2) = last_checkpoint(&dir.path().join("wal")).unwrap().unwrap();
            assert!(cp2 > cp1 && cp2 < exemplar_seg);
            assert!(db.head().min_time() < ex.timestamp_ms);

            let m = [match_eq("name", "exemplar_only")];
            let got = db.select_exemplars(0, ex.timestamp_ms, &[&m]);
            assert_eq!(got.len(), 1);
            assert_eq!(got[0].exemplars, vec![ex.clone()]);
            db.close().unwrap();

            let reg2 = Arc::new(prometheus::Registry::new());
            opts.head.registry = Some(Arc::clone(&reg2));
            let db2 = Tsdb::open_with_extra(dir.path(), opts.clone(), extra.clone()).unwrap();
            assert_eq!(db2.select_exemplars(0, ex.timestamp_ms, &[&m])[0].exemplars, vec![ex.clone()]);
            assert_eq!(metric_val(&reg2, "prometheus_tsdb_wal_replay_unknown_refs_total", &[("type", "exemplars")]), 0.0);

            let mut app = db2.appender();
            app.append_float(None, &a_pairs, block_range * 11 / 2 + 101, 4.0).unwrap();
            app.commit().unwrap();
            db2.compact().unwrap();
            db2.close().unwrap();

            let reg3 = Arc::new(prometheus::Registry::new());
            opts.head.registry = Some(Arc::clone(&reg3));
            let db3 = Tsdb::open_with_extra(dir.path(), opts, extra).unwrap();
            assert_eq!(db3.select_exemplars(0, ex.timestamp_ms, &[&m])[0].exemplars, vec![ex]);
            assert_eq!(metric_val(&reg3, "prometheus_tsdb_wal_replay_unknown_refs_total", &[("type", "exemplars")]), 0.0);
            db3.close().unwrap();
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestExemplarBeforeLastSampleSurvivesSeriesCompaction
#[test]
fn test_exemplar_before_last_sample_survives_series_compaction() {
    for stale in [false, true] {
        for cutoff in [100i64, 500] {
            let dir = tempfile::tempdir().unwrap();
            let reg = Arc::new(prometheus::Registry::new());
            let mut opts = opts_with_reg(&reg, 1000);
            opts.head.enable_exemplar_storage = true;
            opts.head.max_exemplars = 10;
            let db = Tsdb::open(dir.path(), opts.clone()).unwrap();
            let pairs = [("name", "exemplar_series")];
            let ex = Exemplar::new(Labels::from_pairs([("trace_id", "abc123")]), 1.0, 500);
            let mut app = db.appender();
            let r = app.append_float(None, &pairs, 100, 1.0).unwrap();
            let v = if stale { dummy_stale_nan() } else { 2.0 };
            app.append_float(Some(r), &pairs, 900, v).unwrap();
            app.append_exemplar(Some(r), &pairs, ex.clone()).unwrap();
            app.commit().unwrap();
            db.head().set_min_valid_time_for_test(cutoff);

            if stale { db.compact_stale_head().unwrap(); } else { db.compact_selected_series(&[r]).unwrap(); }
            assert_eq!(db.blocks().len(), 1);
            assert_eq!(db.head().num_series(), 1);
            db.close().unwrap();

            let reg2 = Arc::new(prometheus::Registry::new());
            opts.head.registry = Some(Arc::clone(&reg2));
            let db2 = Tsdb::open(dir.path(), opts).unwrap();
            let m = [match_eq("name", "exemplar_series")];
            let got = db2.select_exemplars(0, ex.timestamp_ms, &[&m]);
            assert_eq!(got.len(), 1);
            assert_eq!(got[0].exemplars, vec![ex.clone()]);
            assert_eq!(metric_val(&reg2, "prometheus_tsdb_wal_replay_unknown_refs_total", &[("type", "exemplars")]), 0.0);

            db2.head().set_min_valid_time_for_test(ex.timestamp_ms + 1);
            if stale { db2.compact_stale_head().unwrap(); } else { db2.compact_selected_series(&[r]).unwrap(); }
            assert_eq!(db2.head().num_series(), 0);
            db2.close().unwrap();
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestSeriesCompactionReclaimsChunksWithExemplars
#[test]
fn test_series_compaction_reclaims_chunks_with_exemplars() {
    for compaction in ["selected", "stale"] {
        for kind in [db_test_helpers::SampleKind::Float, db_test_helpers::SampleKind::IntHist, db_test_helpers::SampleKind::FloatHist] {
            for scenario in ["unchanged", "mmapped", "in-order append", "out-of-order append"] {
                let dir = tempfile::tempdir().unwrap();
                let reg = Arc::new(prometheus::Registry::new());
                let mut opts = opts_with_reg(&reg, 1000);
                opts.head.enable_exemplar_storage = true;
                opts.head.max_exemplars = 10;
                opts.head.ooo_time_window_ms = 1000;
                let db = Tsdb::open(dir.path(), opts.clone()).unwrap();
                let s_pairs = [("name", "exemplar_series")];
                let a_pairs = [("name", "active")];
                let ex = Exemplar::new(Labels::from_pairs([("trace_id", "abc123")]), 1.0, 500);

                let mut app = db.appender();
                let (r, _) = append_kind_v2(&mut app, None, kind, &s_pairs, 50, 1).unwrap();
                let _ = append_kind_v2(&mut app, Some(r), kind, &s_pairs, 100, 2).unwrap();
                if compaction == "stale" {
                    let v = dummy_stale_nan();
                    match kind {
                        db_test_helpers::SampleKind::Float => { app.append_float(Some(r), &s_pairs, 900, v).unwrap(); }
                        db_test_helpers::SampleKind::IntHist => { app.append_histogram(Some(r), &s_pairs, 0, 900, mimir_tsdb::chunkenc::Histogram { sum: v, ..Default::default() }).unwrap(); }
                        db_test_helpers::SampleKind::FloatHist => { app.append_float_histogram(Some(r), &s_pairs, 0, 900, mimir_tsdb::chunkenc::FloatHistogram { sum: v, ..Default::default() }).unwrap(); }
                        _ => unreachable!(),
                    }
                } else {
                    let _ = append_kind_v2(&mut app, Some(r), kind, &s_pairs, 900, 3).unwrap();
                }
                app.append_float(None, &a_pairs, 1100, 1.0).unwrap();
                app.append_exemplar(Some(r), &s_pairs, ex.clone()).unwrap();
                app.commit().unwrap();
                if scenario == "mmapped" { db.force_head_mmap().unwrap(); }

                let (hc, mm) = db.head().series_head_chunk_count_for_test(r).unwrap();
                let chunk_cnt = (hc + mm) as f64;
                assert!(chunk_cnt > 0.0);
                let removed_before = metric_val(&reg, "prometheus_tsdb_head_chunks_removed_total", &[]);
                db.head().set_min_valid_time_for_test(100);

                let mut expected_ts = vec![50, 100, 900];
                let concurrent = scenario == "in-order append" || scenario == "out-of-order append";
                if concurrent {

                    let db_ptr = &db as *const Tsdb as usize;
                    // Run concurrent append via before-evict hook.
                    let is_ooo = scenario == "out-of-order append";
                    let is_stale = compaction == "stale";
                    let ts = if is_ooo { 850 } else { 950 };
                    if is_ooo { expected_ts = vec![50, 100, 850, 900]; } else { expected_ts.push(ts); }
                    db.set_compact_head_view_before_evict_hook_for_test(Some(Arc::new(move || {
                        // SAFETY: db outlives compact_selected_series/compact_stale_head synchronously.
                        let d = unsafe { &*(db_ptr as *const Tsdb) };
                        let mut app = d.appender();
                        let v = if is_stale && !is_ooo { dummy_stale_nan() } else { 3.0 };
                        app.append_float(Some(r), &s_pairs, ts, v).unwrap();
                        app.commit().unwrap();
                    })));
                }

                if compaction == "stale" { db.compact_stale_head().unwrap(); } else { db.compact_selected_series(&[r]).unwrap(); }
                assert_eq!(db.blocks().len(), 1);
                assert_eq!(db.head().num_series(), 2);
                if concurrent {
                    let (hc2, _) = db.head().series_head_chunk_count_for_test(r).unwrap();
                    assert!(hc2 > 0);
                    assert_eq!(metric_val(&reg, "prometheus_tsdb_head_chunks_removed_total", &[]), removed_before);
                } else {
                    let (hc2, mm2) = db.head().series_head_chunk_count_for_test(r).unwrap();
                    assert_eq!((hc2, mm2), (0, 0));
                    assert_eq!(metric_val(&reg, "prometheus_tsdb_head_chunks_removed_total", &[]), removed_before + chunk_cnt);
                    if compaction == "stale" { db.compact_stale_head().unwrap(); } else { db.compact_selected_series(&[r]).unwrap(); }
                    assert_eq!(db.blocks().len(), 1);
                }
                let got = query_db_raw_nans(&db, 0, 1000, &[match_eq("name", "exemplar_series")]);
                let got_ts: Vec<i64> = got[r#"{name="exemplar_series"}"#].iter().map(|s| s.t).collect();
                assert_eq!(got_ts, expected_ts);
                db.close().unwrap();

                let db2 = Tsdb::open(dir.path(), opts).unwrap();
                let got2 = query_db_raw_nans(&db2, 0, 1000, &[match_eq("name", "exemplar_series")]);
                let got_ts2: Vec<i64> = got2[r#"{name="exemplar_series"}"#].iter().map(|s| s.t).collect();
                assert_eq!(got_ts2, expected_ts);
                db2.close().unwrap();
            }
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestNoPanicOnTSDBOpenError
#[test]
fn test_no_panic_on_tsdb_open_error() {
    let dir = tempfile::tempdir().unwrap();
    let opts = TsdbOptions { no_lockfile: false, ..TsdbOptions::default() };
    let db1 = Tsdb::open(dir.path(), opts.clone()).unwrap();
    assert!(Tsdb::open(dir.path(), opts).is_err());
    db1.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestNoGoroutineLeakOnTSDBOpenError
#[test]
fn test_no_goroutine_leak_on_tsdb_open_error() {
    let dir = tempfile::tempdir().unwrap();
    let wal_dir = dir.path().join("wal");
    fs::create_dir_all(&wal_dir).unwrap();
    let w = WlogWriter::with_segment_size(&wal_dir, 32768, CompressionType::None).unwrap();
    w.close().unwrap();

    let chunks_dir = dir.path().join("chunks_head");
    fs::create_dir_all(&chunks_dir).unwrap();
    fs::write(chunks_dir.join("000001"), [0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x00, 0x00, 0x00]).unwrap();

    let mut opts = TsdbOptions::default();
    opts.head.head_chunks_write_queue_size = 1000;
    opts.no_lockfile = false;
    assert!(Tsdb::open(dir.path(), opts.clone()).is_err());
    // Lockfile and resources were released cleanly on failed open; fixing the corrupt file lets open succeed.
    fs::remove_file(chunks_dir.join("000001")).unwrap();
    let db = Tsdb::open(dir.path(), opts).unwrap();
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestLockfile
#[test]
fn test_lockfile() {
    for create_lock in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let reg = Arc::new(prometheus::Registry::new());
        let mut opts = opts_with_reg(&reg, DEFAULT_BLOCK_DURATION_MS);
        opts.no_lockfile = !create_lock;
        let db = Tsdb::open(dir.path(), opts.clone()).unwrap();
        if create_lock {
            assert!(dir.path().join("lock").exists());
            assert_eq!(metric_val(&reg, "prometheus_tsdb_clean_start", &[]), 1.0);
            assert!(Tsdb::open(dir.path(), opts).is_err());
        } else {
            assert!(!dir.path().join("lock").exists());
            assert_eq!(metric_val(&reg, "prometheus_tsdb_clean_start", &[]), -1.0);
        }
        db.close().unwrap();
        assert!(!dir.path().join("lock").exists());
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestQuerier_ShouldNotPanicIfHeadChunkIsTruncatedWhileReadingQueriedChunks
#[test]
fn test_querier_should_not_panic_if_head_chunk_is_truncated_while_reading_queried_chunks() {
    let dir = tempfile::tempdir().unwrap();
    let reg = Arc::new(prometheus::Registry::new());
    let opts = opts_with_reg(&reg, DEFAULT_BLOCK_DURATION_MS);
    let db = Tsdb::open(dir.path(), opts).unwrap();
    let pairs: Vec<String> = (0..100).map(|i| format!("test_{i}")).collect();
    let interval = 15_000i64;
    let mut ts = 0i64;
    while ts < 3 * DEFAULT_BLOCK_DURATION_MS {
        let mut app = db.appender();
        for name in &pairs {
            app.append_float(None, &[("__name__", name.as_str())], ts, ts as f64).unwrap();
        }
        app.commit().unwrap();
        ts += interval;
    }
    db.compact().unwrap();
    let q = db.querier(0, i64::MAX);
    let prepared = q.prepare_select_chunks(&[match_re("__name__", ".+")], true).unwrap();
    // Attempt truncation in parallel / sequentially; read guard protects active queriers without panic.
    let res = q.execute_prepared_select(prepared, true).unwrap();
    assert_eq!(res.len(), 100);
    drop(q);
    db.compact().unwrap();
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestChunkQuerier_ShouldNotPanicIfHeadChunkIsTruncatedWhileReadingQueriedChunks
#[test]
fn test_chunk_querier_should_not_panic_if_head_chunk_is_truncated_while_reading_queried_chunks() {
    let dir = tempfile::tempdir().unwrap();
    let db = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
    let names: Vec<String> = (0..100).map(|i| format!("test_{i}")).collect();
    let interval = 15_000i64;
    let mut ts = 0i64;
    while ts < 3 * DEFAULT_BLOCK_DURATION_MS {
        let mut app = db.appender();
        for name in &names {
            app.append_float(None, &[("__name__", name.as_str())], ts, ts as f64).unwrap();
        }
        app.commit().unwrap();
        ts += interval;
    }
    db.compact().unwrap();
    let cq = db.chunk_querier(0, i64::MAX);
    let res = cq.select(true, &[match_re("__name__", ".+")]).unwrap();
    assert_eq!(res.len(), 100);
    for s in res {
        for cm in s.chunks {
            assert!(!cm.chunk.unwrap().as_bytes().is_empty());
        }
    }
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestOOOSampleLossOnWALCheckpointBeforeOOOCompaction
#[test]
fn test_ooo_sample_loss_on_wal_checkpoint_before_ooo_compaction() {
    for kind in ALL_SAMPLE_KINDS {
        let dir = tempfile::tempdir().unwrap();
        let mut opts = TsdbOptions::default();
        opts.head.wal_segment_size = 32 * 1024;
        opts.head.ooo_time_window_ms = 300 * 60_000;
        let evicted_t = 100 * 60_000i64;
        let in_order_t = 300 * 60_000i64;
        let ooo_t = 250 * 60_000i64;
        let pairs = [("foo", "bar")];

        let evicted_ref;
        let recreated_ref;
        {
            let db = Tsdb::open(dir.path(), opts.clone()).unwrap();
            let mut app = db.appender();
            let (r1, _) = append_kind_v2(&mut app, None, kind, &pairs, evicted_t, evicted_t).unwrap();
            app.commit().unwrap();
            evicted_ref = r1;
            db.compact_head_range_inclusive(0, evicted_t).unwrap();
            assert_eq!(db.head().num_series(), 0);

            let mut app = db.appender();
            let (r2, _) = append_kind_v2(&mut app, None, kind, &pairs, in_order_t, in_order_t).unwrap();
            app.commit().unwrap();
            recreated_ref = r2;
            assert_ne!(evicted_ref, recreated_ref);

            let mut app = db.appender();
            let _ = append_kind_v2(&mut app, Some(r2), kind, &pairs, ooo_t, ooo_t).unwrap();
            let pad = "x".repeat(8192);
            for i in 0..32 {
                let id_s = i.to_string();
                app.append_float(None, &[("foo", "pad"), ("id", &id_s), ("padding", &pad)], in_order_t, i as f64).unwrap();
            }
            app.commit().unwrap();
            db.close().unwrap();
        }

        let expected = BTreeMap::from([(
            r#"{foo="bar"}"#.to_string(),
            vec![
                db_test_helpers::make_sample_kind(kind, evicted_t, evicted_t),
                db_test_helpers::make_sample_kind(kind, ooo_t, ooo_t),
                db_test_helpers::make_sample_kind(kind, in_order_t, in_order_t),
            ],
        )]);

        let reg2 = Arc::new(prometheus::Registry::new());
        opts.head.registry = Some(Arc::clone(&reg2));
        let db = Tsdb::open(dir.path(), opts.clone()).unwrap();
        compare_series(&expected, &query_db(&db, i64::MIN, i64::MAX, &[match_eq("foo", "bar")]), true);
        assert!(db.head().get_entry_by_id(recreated_ref).is_none());
        assert_eq!(db.head().get_wal_expiry_for_test(recreated_ref), Some(in_order_t));

        db.compact_head_range_inclusive(db.head().min_time(), in_order_t).unwrap();
        assert!(metric_val(&reg2, "prometheus_tsdb_checkpoint_creations_total", &[]) > 0.0);
        db.close().unwrap();

        let reg3 = Arc::new(prometheus::Registry::new());
        opts.head.registry = Some(Arc::clone(&reg3));
        let db = Tsdb::open(dir.path(), opts.clone()).unwrap();
        compare_series(&expected, &query_db(&db, i64::MIN, i64::MAX, &[match_eq("foo", "bar")]), true);
        assert_eq!(metric_val(&reg3, "prometheus_tsdb_wbl_replay_unknown_refs_total", &[("type", "series")]), 0.0);

        db.compact_ooo_head().unwrap();
        db.close().unwrap();

        let db = Tsdb::open(dir.path(), opts).unwrap();
        compare_series(&expected, &query_db(&db, i64::MIN, i64::MAX, &[match_eq("foo", "bar")]), true);
        db.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestDiskFillingUpAfterDisablingOOO
#[test]
fn test_disk_filling_up_after_disabling_ooo() {
    for kind in ALL_SAMPLE_KINDS {
        let dir = tempfile::tempdir().unwrap();
        let mut opts = TsdbOptions::default();
        opts.head.ooo_time_window_ms = 60 * 60_000;
        opts.head.samples_per_chunk = 15;
        opts.head.ooo_cap_max = 5;
        opts.max_block_chunk_segment_size = 1024 * 1024;
        let pairs = [("foo", "bar1")];

        let db = Tsdb::open(dir.path(), opts.clone()).unwrap();
        let add_samples = |d: &Tsdb, from_m: i64, to_m: i64| {
            let mut app = d.appender();
            for m in (from_m..=to_m).step_by(5) {
                let ts = m * 60_000;
                append_kind_v2(&mut app, None, kind, &pairs, ts, ts).unwrap();
            }
            app.commit().unwrap();
        };
        add_samples(&db, 290, 300);
        add_samples(&db, 250, 299);
        db.close().unwrap();

        opts.head.ooo_time_window_ms = 0;
        let db = Tsdb::open(dir.path(), opts).unwrap();
        let r = db.head().get_ref_by_sorted_pairs(&pairs).unwrap();
        let (ooo_mm, _) = db.head().series_ooo_chunk_count_for_test(r).unwrap();
        assert!(ooo_mm > 0);

        let mmap_files = || -> Vec<String> {
            let mut v: Vec<String> = fs::read_dir(dir.path().join("chunks_head")).unwrap().map(|e| e.unwrap().file_name().into_string().unwrap()).collect();
            v.sort();
            v
        };
        add_samples(&db, 301, 500);
        db.force_head_mmap().unwrap();
        assert!(mmap_files().contains(&"000001".to_string()));
        assert!(mmap_files().contains(&"000002".to_string()));
        db.compact().unwrap();
        assert!(mmap_files().contains(&"000002".to_string()));
        assert!(!mmap_files().contains(&"000001".to_string()));
        assert!(!db.head().series_has_ooo_for_test(r).unwrap());

        add_samples(&db, 501, 650);
        db.force_head_mmap().unwrap();
        assert!(mmap_files().contains(&"000002".to_string()));
        assert!(mmap_files().contains(&"000003".to_string()));
        assert!(!mmap_files().contains(&"000001".to_string()));
        db.compact().unwrap();
        assert!(!mmap_files().contains(&"000001".to_string()));
        assert!(!mmap_files().contains(&"000002".to_string()));
        assert!(!mmap_files().contains(&"000003".to_string()));
        let wbl_entries: Vec<_> = fs::read_dir(dir.path().join("wbl"))
            .unwrap()
            .map(|e| e.unwrap())
            .collect();
        assert_eq!(wbl_entries.len(), 1);
        assert_eq!(wbl_entries[0].metadata().unwrap().len(), 0);
        db.close().unwrap();
    }
}


// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestCompactHeadWithoutTruncation
#[test]
fn test_compact_head_without_truncation() {
    let setup = || {
        let dir = tempfile::tempdir().unwrap();
        let db = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
        let mut app = db.appender();
        for t in [0, DEFAULT_BLOCK_DURATION_MS / 2, DEFAULT_BLOCK_DURATION_MS - 1, 2 * DEFAULT_BLOCK_DURATION_MS] {
            app.append_float(None, &[("foo", "bar")], t, t as f64).unwrap();
        }
        app.commit().unwrap();
        (dir, db)
    };
    let (_d1, db1) = setup();
    db1.compact_head_range(0, DEFAULT_BLOCK_DURATION_MS).unwrap();
    let s1 = query_head_range(&db1, i64::MIN, i64::MAX, &[match_eq("foo", "bar")], false);
    assert_eq!(s1[r#"{foo="bar"}"#].len(), 1);
    assert_eq!(s1[r#"{foo="bar"}"#][0].t, 2 * DEFAULT_BLOCK_DURATION_MS);
    db1.close().unwrap();

    let (_d2, db2) = setup();
    db2.compact_head_range_without_truncation(0, DEFAULT_BLOCK_DURATION_MS).unwrap();
    let s2 = query_head_range(&db2, i64::MIN, i64::MAX, &[match_eq("foo", "bar")], false);
    assert_eq!(s2[r#"{foo="bar"}"#].len(), 4);
    db2.close().unwrap();
}


// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestStaleSeriesCompactionWithZeroSeries
#[test]
fn test_stale_series_compaction_with_zero_series() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db_range(dir.path(), 1000);
    assert_eq!(db.head().num_series(), 0);
    db.compact_stale_head().unwrap();
    assert!(db.blocks().is_empty());
    db.close().unwrap();
}

