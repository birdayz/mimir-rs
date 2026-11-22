//! Ported upstream Prometheus/Mimir TSDB compaction, block, blockwriter, and repair tests.
//!
//! Sources:
//! - compact_test.go (27)
//! - block_test.go (13)
//! - blockwriter_test.go (1)
//! - repair_test.go (1)

use std::collections::HashSet;
use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};

use mimir_tsdb::block::{
    list_block_dirs, read_meta_file, write_meta_file, Block, BlockDesc, BlockMeta,
    BlockMetaCompaction, BlockStats, META_VERSION_1,
};
use mimir_tsdb::chunkenc::{
    CounterResetHint, EncodedChunk, FloatHistogram, Histogram, HistogramSpan, XorChunk,
};
use mimir_tsdb::chunks_block::{BlockChunksWriter, ChunkMeta, CHUNKS_FORMAT_VERSION_SIZE, MAGIC_CHUNKS_SIZE};
use mimir_tsdb::compact::{
    compact_block_metas, compact_blocks, compact_blocks_cancellable,
    compact_blocks_with_splitting, open_blocks_for_compaction,
    plan_block_compactions_with_filter, plan_dir_metas, split_by_range, write_block_from_series,
    AsyncBlockWriter, BlockWriter, CompactError, CompactionSeries, DirMeta, SampleValue,
};
use mimir_tsdb::db::{Tsdb, TsdbOptions};
use mimir_tsdb::head::{Head, HeadOptions};
use mimir_tsdb::index::{IndexReader, IndexWriter, INDEX_FILENAME, INDEX_FORMAT_V1};
use mimir_tsdb::labels::Labels;
use mimir_tsdb::matcher::{LabelMatcher, MatchType};
use mimir_tsdb::querier::TsdbQuerier;
use mimir_tsdb::search::SearchHints;
use mimir_tsdb::tombstones::{write_tombstones_file, MemTombstones};
use ulid::Ulid;

fn unhex(s: &str) -> Vec<u8> {
    let b = s.as_bytes();
    assert_eq!(b.len() % 2, 0);
    (0..b.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(std::str::from_utf8(&b[i..i + 2]).unwrap(), 16).unwrap())
        .collect()
}

fn dir_size(dir: &Path) -> u64 {
    let mut total = 0u64;
    for e in fs::read_dir(dir).unwrap() {
        let e = e.unwrap();
        let md = e.metadata().unwrap();
        if md.is_dir() {
            total += dir_size(&e.path());
        } else {
            total += md.len();
        }
    }
    total
}

fn counter_val(reg: &prometheus::Registry, name: &str) -> f64 {
    reg.gather()
        .into_iter()
        .find(|mf| mf.get_name() == name)
        .map(|mf| mf.get_metric().iter().map(|m| m.get_counter().get_value()).sum())
        .unwrap_or(0.0)
}

fn xor_chunk(samples: &[(i64, f64)]) -> EncodedChunk {
    let mut xc = XorChunk::new();
    {
        let mut app = xc.appender().unwrap();
        for &(t, v) in samples {
            app.append(0, t, v).unwrap();
        }
    }
    EncodedChunk::Xor(xc)
}

fn create_block_series(dir: &Path, mint: i64, maxt: i64, series: Vec<CompactionSeries>) -> PathBuf {
    let meta = write_block_from_series(
        dir,
        mint,
        maxt,
        series,
        BlockMetaCompaction::default(),
        false,
        0,
    )
    .unwrap()
    .unwrap();
    dir.join(meta.ulid.to_string())
}

fn gen_series(num_series: usize, num_samples: usize, mint: i64, maxt: i64) -> (i64, i64, Vec<CompactionSeries>) {
    let mut out = Vec::with_capacity(num_series);
    let step = if num_samples > 1 && maxt > mint + 1 {
        ((maxt - 1 - mint) / (num_samples as i64 - 1)).max(1)
    } else {
        1
    };
    for i in 0..num_series {
        let mut samples = Vec::with_capacity(num_samples);
        for j in 0..num_samples {
            let t = (mint + (j as i64) * step).min(maxt - 1);
            if samples.last().is_some_and(|&(pt, _)| pt == t) {
                continue;
            }
            samples.push((t, (i + j) as f64));
        }
        let c = xor_chunk(&samples);
        out.push(CompactionSeries {
            labels: Labels::from_pairs([("__name__", format!("m_{i}").as_str()), ("idx", format!("{i}").as_str())]),
            chunks: vec![ChunkMeta {
                ref_id: 0,
                min_time: samples[0].0,
                max_time: samples.last().unwrap().0,
                chunk: Some(c),
            }],
            tombstones: Vec::new(),
        });
    }
    (mint, maxt, out)
}

fn create_gen_block(dir: &Path, num_series: usize, num_samples: usize, mint: i64, maxt: i64) -> PathBuf {
    let (mn, mx, s) = gen_series(num_series, num_samples, mint, maxt);
    create_block_series(dir, mn, mx, s)
}

fn simple_series(pairs: &[(&str, &str)], samples: &[(i64, f64)]) -> CompactionSeries {
    let c = xor_chunk(samples);
    CompactionSeries {
        labels: Labels::from_pairs(pairs.iter().copied()),
        chunks: vec![ChunkMeta {
            ref_id: 0,
            min_time: samples[0].0,
            max_time: samples.last().unwrap().0,
            chunk: Some(c),
        }],
        tombstones: Vec::new(),
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/blockwriter_test.go::TestBlockWriter_AppenderV2
#[test]
fn test_block_writer_appender_v2() {
    let dir = tempfile::tempdir().unwrap();
    let w = BlockWriter::new(dir.path(), 7_200_000).unwrap();
    {
        let mut app = w.appender();
        app.append_float(None, &[("a", "b")], 44, 7.0).unwrap();
        app.append_float(None, &[("c", "d")], 55, 12.0).unwrap();
        app.commit().unwrap();
    }
    let uid = w.flush().unwrap().unwrap();
    let b = Block::open(dir.path().join(uid.to_string())).unwrap();
    let q = TsdbQuerier::new(None, vec![Arc::new(b)], i64::MIN, i64::MAX);
    let m = [LabelMatcher::new(MatchType::RegexMatch, "", ".*").unwrap()];
    let res = q.select(true, &m).unwrap();
    assert_eq!(res.len(), 2);
    assert_eq!(res[0].labels.get("a"), "b");
    assert_eq!(res[0].samples[0].t, 44);
    assert_eq!(res[1].labels.get("c"), "d");
    assert_eq!(res[1].samples[0].t, 55);
    w.close().unwrap();
}

const REPAIR_INDEX_HEX: &str = "baaad700010000000c000000040131013201610162b37207b6000000000000000602020003000016aac362000000000006020201030000cbef69da000000000c000000020000000200000003a5834390000000040000000048674bc70000000a0000000102016201313c20aae5ca0000000000000005000000000000001900000000000000000000000000000050000000000000003b000000000000005c3c3e2a05";
const REPAIR_META_HEX: &str = "7b0a092276657273696f6e223a20322c0a0922756c6964223a20223031425a4a39574a51505748474e433257344a39544136324b43222c0a09226d696e54696d65223a20313531313336363430303030302c0a09226d617854696d65223a20313531313336383230303030302c0a09227374617473223a207b0a0909226e756d53616d706c6573223a2033313839373536352c0a0909226e756d536572696573223a2038383931302c0a0909226e756d4368756e6b73223a203236363039330a097d2c0a0922636f6d70616374696f6e223a207b0a0909226c6576656c223a20312c0a090922736f7572636573223a205b0a090909223031425a4a39574a51505748474e433257344a39544136324b43220a09095d0a097d0a7d0a";

// Upstream: github.com/prometheus/prometheus/tsdb/repair_test.go::TestRepairBadIndexVersion
#[test]
fn test_repair_bad_index_version() {
    let dir = tempfile::tempdir().unwrap();
    let bdir = dir.path().join("01BZJ9WJQPWHGNC2W4J9TA62KC");
    fs::create_dir_all(bdir.join("chunks")).unwrap();
    fs::write(bdir.join(INDEX_FILENAME), unhex(REPAIR_INDEX_HEX)).unwrap();
    fs::write(bdir.join("meta.json"), unhex(REPAIR_META_HEX)).unwrap();
    write_tombstones_file(&bdir, &MemTombstones::new()).unwrap();

    assert!(read_meta_file(&bdir).is_err());
    {
        let r = IndexReader::open(bdir.join(INDEX_FILENAME)).unwrap();
        let ids = r.postings("b", &["1"]).unwrap();
        for id in ids {
            assert!(r.series(id).is_err());
        }
    }

    let db = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
    db.close().unwrap();

    let r = IndexReader::open(bdir.join(INDEX_FILENAME)).unwrap();
    let ids = r.postings("b", &["1"]).unwrap();
    let got: Vec<Labels> = ids.into_iter().map(|id| r.series(id).unwrap().0).collect();
    assert_eq!(got, vec![Labels::from_pairs([("a", "1"), ("b", "1")]), Labels::from_pairs([("a", "2"), ("b", "1")])]);
    let meta = read_meta_file(&bdir).unwrap();
    assert_eq!(meta.version, META_VERSION_1);
}

// Upstream: github.com/prometheus/prometheus/tsdb/block_test.go::TestBlockMetaCompaction_SetSelectedSeries
#[test]
fn test_block_meta_compaction_set_selected_series() {
    let mut bmc = BlockMetaCompaction::default();
    assert!(!bmc.from_selected_series());
    bmc.set_selected_series();
    assert!(bmc.from_selected_series());
    assert_eq!(bmc.hints.len(), 1);
    bmc.set_selected_series();
    assert!(bmc.from_selected_series());
    assert_eq!(bmc.hints.len(), 1);
    assert!(!bmc.from_stale_series());
    bmc.set_stale_series();
    assert!(bmc.from_stale_series());
    assert!(bmc.from_selected_series());
    assert_eq!(bmc.hints.len(), 2);
}

// Upstream: github.com/prometheus/prometheus/tsdb/block_test.go::TestBlockMetaMustNeverBeVersion2
#[test]
fn test_block_meta_must_never_be_version_2() {
    let dir = tempfile::tempdir().unwrap();
    let mut meta = BlockMeta {
        ulid: Ulid::new(),
        min_time: 0,
        max_time: 10,
        stats: BlockStats::default(),
        compaction: BlockMetaCompaction::default(),
        version: 2,
        out_of_order: false,
        thanos: None,
    };
    write_meta_file(dir.path(), &mut meta).unwrap();
    let read_back = read_meta_file(dir.path()).unwrap();
    assert_ne!(read_back.version, 2);
}

// Upstream: github.com/prometheus/prometheus/tsdb/block_test.go::TestSetCompactionFailed
#[test]
fn test_set_compaction_failed() {
    let dir = tempfile::tempdir().unwrap();
    let bdir = create_gen_block(dir.path(), 1, 1, 0, 1);
    let b = Block::open(&bdir).unwrap();
    assert!(!b.meta().compaction.failed);
    b.set_compaction_failed().unwrap();
    assert!(b.compaction_failed());
    drop(b);
    let b2 = Block::open(&bdir).unwrap();
    assert!(b2.compaction_failed());
}

// Upstream: github.com/prometheus/prometheus/tsdb/block_test.go::TestBlockLogValue
#[test]
fn test_block_log_value() {
    let dir = tempfile::tempdir().unwrap();
    let bdir = create_gen_block(dir.path(), 1, 1, 0, 10);
    let b = Block::open(&bdir).unwrap();
    let base = bdir.file_name().unwrap().to_str().unwrap();
    assert_eq!(b.ulid().to_string(), base);
    assert!(!format!("{:?}", b).is_empty());
}

// Upstream: github.com/prometheus/prometheus/tsdb/block_test.go::TestCreateBlock
#[test]
fn test_create_block() {
    let dir = tempfile::tempdir().unwrap();
    let bdir = create_gen_block(dir.path(), 1, 1, 0, 10);
    let _b = Block::open(&bdir).unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/block_test.go::TestCorruptedChunk
#[test]
fn test_corrupted_chunk() {
    type CorruptCase = (&'static str, fn(&Path), bool, &'static str);
    let cases: [CorruptCase; 6] = [
        ("invalid header size", |p| { fs::OpenOptions::new().write(true).open(p).unwrap().set_len(1).unwrap(); }, true, "header"),
        ("invalid magic", |p| { let mut f = OpenOptions::new().write(true).open(p).unwrap(); f.seek(SeekFrom::Start(0)).unwrap(); f.write_all(&[0; MAGIC_CHUNKS_SIZE]).unwrap(); }, true, "magic"),
        ("invalid version", |p| { let mut f = OpenOptions::new().write(true).open(p).unwrap(); f.seek(SeekFrom::Start(MAGIC_CHUNKS_SIZE as u64)).unwrap(); f.write_all(&[0; CHUNKS_FORMAT_VERSION_SIZE]).unwrap(); }, true, "version"),
        ("truncated length", |p| { OpenOptions::new().write(true).open(p).unwrap().set_len(9).unwrap(); }, false, ""),
        ("truncated data", |p| { let sz = fs::metadata(p).unwrap().len(); OpenOptions::new().write(true).open(p).unwrap().set_len(sz - 1).unwrap(); }, false, ""),
        ("checksum mismatch", |p| { let sz = fs::metadata(p).unwrap().len(); let mut f = OpenOptions::new().write(true).open(p).unwrap(); f.seek(SeekFrom::Start(sz - 5)).unwrap(); f.write_all(b"x").unwrap(); }, false, "checksum"),
    ];
    for (name, corr, exp_open_err, needle) in cases {
        let dir = tempfile::tempdir().unwrap();
        let bdir = create_block_series(dir.path(), 0, 2, vec![simple_series(&[("a", "b")], &[(1, 1.0)])]);
        let seg = bdir.join("chunks").join("000001");
        corr(&seg);
        let open_res = Block::open(&bdir);
        if exp_open_err {
            let err = open_res.unwrap_err().to_string().to_lowercase();
            assert!(err.contains(needle), "{name}: {err}");
            continue;
        }
        let b = open_res.unwrap();
        let q = TsdbQuerier::new(None, vec![Arc::new(b)], 0, 1);
        let m = [LabelMatcher::new(MatchType::Equal, "a", "b").unwrap()];
        let err = q.select(false, &m).unwrap_err().to_string().to_lowercase();
        if !needle.is_empty() {
            assert!(err.contains(needle), "{name}: {err}");
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/block_test.go::TestLabelValuesWithMatchers
#[test]
fn test_label_values_with_matchers() {
    let dir = tempfile::tempdir().unwrap();
    let mut series = Vec::new();
    for i in 0..100usize {
        let t = format!("value{}", i / 10);
        let u = format!("value{i}");
        series.push(simple_series(&[("tens", &t), ("unique", &u)], &[(100, 0.0)]));
    }
    series.push(simple_series(&[("unique", "value99")], &[(100, 0.0)]));
    let bdir = create_block_series(dir.path(), 0, 200, series);
    let b = Arc::new(Block::open(&bdir).unwrap());
    let q = TsdbQuerier::new(None, vec![b], 0, 200);
    let mut without30: Vec<String> = (0..100).filter(|i| i / 10 != 3).map(|i| format!("value{i}")).collect();
    without30.sort();
    let cases = [
        ("tens", vec![LabelMatcher::new(MatchType::Equal, "unique", "value35").unwrap()], vec!["value3".to_string()]),
        ("unique", vec![LabelMatcher::new(MatchType::Equal, "tens", "value1").unwrap()], (10..20).map(|i| format!("value{i}")).collect()),
        ("tens", vec![LabelMatcher::new(MatchType::RegexMatch, "unique", "value[5-7]5").unwrap()], vec!["value5".to_string(), "value6".to_string(), "value7".to_string()]),
        ("tens", vec![LabelMatcher::new(MatchType::NotEqual, "unique", "").unwrap()], (0..10).map(|i| format!("value{i}")).collect()),
        ("unique", vec![LabelMatcher::new(MatchType::NotEqual, "tens", "value3").unwrap(), LabelMatcher::new(MatchType::NotEqual, "tens", "").unwrap()], without30),
        ("unique", vec![LabelMatcher::new(MatchType::Equal, "tens", "").unwrap()], vec!["value99".to_string()]),
    ];
    for (lbl, matchers, exp) in cases {
        assert_eq!(q.label_values(lbl, None, &matchers).unwrap(), exp);
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/block_test.go::TestBlockQuerierReturnsSortedLabelValues
#[test]
fn test_block_querier_returns_sorted_label_values() {
    let dir = tempfile::tempdir().unwrap();
    let series: Vec<_> = (1..=100).rev().map(|i| simple_series(&[("__name__", &format!("value{i}"))], &[(100, 0.0)])).collect();
    let bdir = create_block_series(dir.path(), 0, 200, series);
    let b = Arc::new(Block::open(&bdir).unwrap());
    let q = TsdbQuerier::new(None, vec![b], 0, 100);
    let res = q.label_values("__name__", None, &[]).unwrap();
    assert!(res.windows(2).all(|w| w[0] <= w[1]));
}

// Upstream: github.com/prometheus/prometheus/tsdb/block_test.go::TestBlockSize
#[test]
fn test_block_size() {
    let dir = tempfile::tempdir().unwrap();
    let bdir = create_gen_block(dir.path(), 10, 99, 1, 100);
    let b = Block::open(&bdir).unwrap();
    let exp_init = b.size();
    assert_eq!(exp_init, dir_size(&bdir));

    let m = [LabelMatcher::new(MatchType::RegexMatch, "", ".*").unwrap()];
    b.delete(1, 10, &m).unwrap();
    let exp_after_del = b.size();
    assert!(exp_after_del > exp_init);
    assert_eq!(exp_after_del, dir_size(&bdir));

    let compacted = compact_blocks(dir.path(), &[&b]).unwrap().unwrap();
    let b2 = Block::open(dir.path().join(compacted.ulid.to_string())).unwrap();
    assert!(exp_after_del > b2.size());
    assert_eq!(b2.size(), dir_size(b2.dir()));
}

const V1_META_HEX: &str = "7b0a092276657273696f6e223a20312c0a0922756c6964223a20223031445858465a445944314d515736303739574b304b36454451222c0a09226d696e54696d65223a20302c0a09226d617854696d65223a20373230303030302c0a09227374617473223a207b0a0909226e756d53616d706c6573223a203130322c0a0909226e756d536572696573223a203130322c0a0909226e756d4368756e6b73223a203130320a097d2c0a0922636f6d70616374696f6e223a207b0a0909226c6576656c223a20312c0a090922736f7572636573223a205b0a090909223031445858465a445944314d515736303739574b304b36454451220a09095d0a097d0a7d0a";
const V1_TOMBS_HEX: &str = "0130ba300100000000";
const V1_CHUNK_HEX: &str = "85bd40dd010000000c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c010001000000000000000000001bf86a400c01000102400000000000000000597ad2760c01000106401000000000000000fe61ae45";
const V1_INDEX_HEX: &str = "baaad7000100000136000000680130013102313002313102313202313302313402313502313602313702313802313901320232300232310232320232330232340232350232360232370232380232390133023330023331023332023333023334023335023336023337023338023339013402343002343102343202343302343402343502343602343702343802343901350235300235310235320235330235340235350235360235370235380235390136023630023631023632023633023634023635023636023637023638023639013702373002373102373202373302373402373502373602373702373802373901380238300238310238320238330238340238350238360238370238380238390139023930023931023932023933023934023935023936023937023938023939036261720362617a03666f6f036d65688116a5490801af020d01000008cfca87070801af020f0100001a4e8defc70801af02110100002c82ed4a340801af02140100003eabdc2cb00801af0217010000502e51b0010801af021a01000062e2ba53ca0801af021d010000747c337d090901af02200100008601c33c44b10901af022301000098013c9eec600901af0226010000aa01065092630901af0229010000bc01b615d6620901af022c010000ce0173cc6e650901af022f010000e001cc20d4370901af0231010000f2011dd3aacd0901af0234010000840285d080e20901af02370100009602a9ec8a570901af023a010000a80220ba4e2f0901af023d010000ba02be7373ed0901af0240010000cc021edacf710901af0243010000de0232e6c5c40901af0246010000f002e473e8220901af02490100008203684b6bfa0901af024c0100009403638497270901af024f010000a60330337e900901af0251010000b803325ea20e0901af0254010000ca03f7871a090901af0257010000dc03953171600901af025a010000ee03cff9177c0901af025d0100008004e93d38b60901af0260010000920489869b1b0901af0263010000a40494bb13700901af0266010000b604d1fe8e710901af0269010000c8047c332cce0901af026c010000da043976b1cf0901af026f010000ec04244b39a40901af0271010000fe04f5b8475e0901af027401000090052e512fbf0901af0277010000a2057de6c6080901af027a010000b405162f22ca0901af027d010000c605087a3a0e0a01af028001010000d80538247fc10a01af028301010000ea05e72a28f40a01af028601010000fc057cc3615e0a01af0289010100008e06a4473b190a01af028c01010000a006e2f4a3880a01af028f01010000b206427117bf0a01af029101010000c406337709be0a01af029401010000d606e61421c80a01af029701010000e806ea84d4990a01af029a01010000fa0671ca01bc0a01af029d010100008c0748d2b7830a01af02a0010100009e07729c1bbd0a01af02a301010000b00741c91f6d0a01af02a601010000c2071436121d0a01af02a901010000d40711f4ff740a01af02ac01010000e607bb1c34000a01af02af01010000f807c80722530a01af02b1010100008a08a99861aa0a01af02b4010100009c08327128000a01af02b701010000ae08ed7f7f350a01af02ba01010000c0081af6dcf30a01af02bd01010000d2081f93ad150a01af02c001010000e4086e6b848f0a01af02c301010000f608ceee30b80a01af02c6010100008809bae41caf0a01af02c9010100009a09f1ac901a0a01af02cc01010000ac0915ce3ab20a01af02cf01010000be09b54b8e850a01af02d101010000d009669ca2bd0a01af02d401010000e209cc7469c90a01af02d701010000f409227bbc220a01af02da01010000860a2af9bff50a01af02dd01010000980afc026c770a01af02e001010000aa0ab9c7234b0a01af02e301010000bc0a57c8f6a00a01af02e601010000ce0a0237fbd00a01af02e901010000e00adaafc7820a01af02ec01010000f20a0fcceff40a01af02ef01010000840b93349c1a0a01af02f101010000960b2c24c6c10a01af02f401010000a80b5552afd10a01af02f701010000ba0bf5d71be60a01af02fa01010000cc0ba08f8a190a01af02fd01010000de0ba5eafbff0a01af028002010000f00b427f2b860a01af028302010000820cb6acde5c0a01af028602010000940c2d4597f60a01af028902010000a60c1986f8410a01af028c02010000b80c1f7b72530a01af028f02010000ca0c3f62e3620a01af029102010000dc0ccef8d8650a01af029402010000ee0c641013110a01af029702010000800d5a3952c60a01af029a02010000920dc17787e30a01af029d02010000a40df51374db0a01af02a002010000b60dcf5dd8e50a01af02a302010000c80d3cdaebb00a01af02a602010000da0de9b9c3c60a01af02a902010000ec0d93f0cdad0a01af02ac02010000fe0d4693e5db0a01b702af02010200900eeb9e96320a01b702b302010600a20e8021478800000000019800000001000000640000000d0000000f0000001100000014000000170000001a0000001d000000200000002300000026000000290000002c0000002f0000003100000034000000370000003a0000003d000000400000004300000046000000490000004c0000004f0000005100000054000000570000005a0000005d000000600000006300000066000000690000006c0000006f0000007100000074000000770000007a0000007d000000800000008300000086000000890000008c0000008f0000009100000094000000970000009a0000009d000000a0000000a3000000a6000000a9000000ac000000af000000b1000000b4000000b7000000ba000000bd000000c0000000c3000000c6000000c9000000cc000000cf000000d1000000d4000000d7000000da000000dd000000e0000000e3000000e6000000e9000000ec000000ef000000f1000000f4000000f7000000fa000000fd000001000000010300000106000001090000010c0000010f0000011100000114000001170000011a0000011d000001200000012300000126000001290000012c2d33376c0000001000000001000000020000012f0000013336ab8147000000080000000100000342209e18f40000000800000001000004e30ad3e714000000080000000100000150f6be9f8200000008000000010000048900f75c4e0000000800000001000005e2eb1afc600000000800000001000002c4976b5c1300000008000000010000062d42634e190000000800000001000006ff708bdf450000000800000001000003996ac4526400000008000000010000037b68cf8a890000000800000001000003b7e601639d000000080000000100000420da0e322400000008000000010000020e3f0452ef0000000800000001000006a54a4c2dae0000000800000001000002e0704c45d2000000080000000100000669c482c4ba0000000800000001000005f1e814c8fb000000080000000100000600ddf68c14000000080000000100000246f4a6179c000000080000000100000200937cedc800000008000000010000029a6a3639e700000008000000010000017702c975b700000008000000010000016aadbffe0b0000000800000001000002a8bbee00a10000000800000001000004b66e0729db00000008000000010000064b05043a930000000800000001000006c30d2b59240000000800000001000001d652e943120000000800000001000004111886f896000000080000000100000588e13e473a00000008000000010000067826b780d600000008000000010000030aeb3c5d870000000800000001000003d566fc800800000008000000010000028c5cc919600000000800000001000002b607c878e900000008000000010000033477a7ab110000000800000001000003c665f2b4930000000800000001000006878bcad3870000000800000001000006d2ef1e1d480000000800000001000002d2a1947c940000000800000001000001ba7e6c1fa00000000800000001000003181a59ea1f000000080000000100000350d1fbaf6c00000008000000010000053d66da21ef00000008000000010000043e66284a6c00000008000000010000051fa75cdfc600000008000000010000038a69ca66ff00000008000000010000045ce6d5a9f9000000080000000100000191c7583a450000000800000001000001c8eecf3b5a00000008000000010000044d04e0ed9500000008000000010000060f83e5b0300000000800000001000003e4a4744aba0000000800000001000005a66dfb76c300000008000000010000056ae3359fd7000000080000000100000579e03bab4c00000008000000010000065ae7317eff0000000800000001000004c50ccf8e2200000008000000010000055b21bd55650000019c0000006600000143000001500000015d0000016a0000017700000184000001910000019e000001ac000001ba000001c8000001d6000001e4000001f2000002000000020e0000021c0000022a00000238000002460000025400000262000002700000027e0000028c0000029a000002a8000002b6000002c4000002d2000002e0000002ee000002fc0000030a00000318000003260000033400000342000003500000035e0000036c0000037b0000038a00000399000003a8000003b7000003c6000003d5000003e4000003f30000040200000411000004200000042f0000043e0000044d0000045c0000046b0000047a0000048900000498000004a7000004b6000004c5000004d4000004e3000004f200000501000005100000051f0000052e0000053d0000054c0000055b0000056a000005790000058800000597000005a6000005b5000005c4000005d3000005e2000005f1000006000000060f0000061e0000062d0000063c0000064b0000065a00000669000006780000068700000696000006a5000006b4000006c3000006d2000006e1000006f0000006fff66aaf73000000080000000100000510f94fe3e20000000800000001000002eedc34faf500000008000000010000025405c3a00400000008000000010000047ae0c9c0cf00000008000000010000054ce529f6e10000000800000001000003a8a84c98d60000000800000001000004f2e8e6a37800000008000000010000015d4996d3510000000800000001000004a78c326db70000000800000001000002382946fcb60000000800000001000001ac48933f270000000800000001000005011b7aa78e00000008000000010000052e65d415740000000800000001000005b56ef54258000000080000000100000270e2e4b9c50000000800000001000005c4ed06955600000008000000010000069669ff97eb00000008000000010000042f841d0e0000000008000000010000036cac5b290d0000000800000001000001f2b5ce5ad300000008000000010000022ad8234b2e000000080000000100000184e2f7e9360000000800000001000006b4a87969c200000008000000010000021cce61e5770000000800000001000002fc2d514d6d000000080000000100000597af73bc7100000008000000010000026213810e5d00000008000000010000035e7d83104b0000000800000001000006e1ccada70d0000000800000001000003f360e0e93e0000000800000001000006f02e98e3610000000800000001000005d3299236d20000000800000001000004021b88cc0d00000008000000010000019e994b066100000008000000010000027e4e9c06e2000000080000000100000498e2c2182200000008000000010000061e61d0f45c0000000800000001000001e483317a5400000008000000010000063ca0560a75000000080000000100000143f5b0ab1900000008000000010000046b02fc84a30000000800000001000004d4eefaca4e00000008000000010000032686c21c8900000012000000020103626172900e0103666f6fb011373eca86000003fd000000670203626172023432c8110203626172023638d81102036261720131e8110203626172023632f81102036261720238338812020362617202333498120203626172023838a8120203666f6f0362617ab8120203626172023438c8120203626172023436d81202036261720135e8120203626172023536f81202036261720232328813020362617202393598130203626172023336a8130203626172023931b8130203626172023834c8130203626172023835d8130203626172023236e8130203626172023231f81302036261720233318814020362617202313298140203626172023131a8140203626172023332b8140203626172023635c81402036261720139d8140203626172023937e8140203626172023139f81402036261720235358815020362617202373898150203626172023932a8150203626172023339b8150203626172023531c8150203626172023330d8150203626172023333e8150203626172023431f81502036261720235308816020362617202393398160203626172023938a8160203626172023335b8160203626172023137c81602036261720134d8160203626172023433e8160203626172023733f81602036261720235388817020362617202373198170203626172023437a81702036261720136b8170203626172023134c8170203626172023138d8170203626172023539e8170203626172023836f817020362617202353288180203626172013898180203626172023736a8180203626172023737b8180203626172023930c8180203626172023636d8180203626172023735e818020000f81802036261720237309c1c0203626172023337ac1c0203626172023237bc1c0203626172023631cc1c0203626172023734dc1c0203626172023439ec1c0203626172023639fc1c02036261720231308c1d02036261720236349c1d0203626172023235ac1d0203626172023136bc1d02036261720137cc1d0203626172023732dc1d0203626172023830ec1d0203626172023239fc1d02036261720238318c1e02036261720239349c1e0203626172023537ac1e0203626172023435bc1e0203626172023230cc1e0203626172023234dc1e0203626172023133ec1e0203626172023936fc1e02036261720232338c1f02036261720233389c1f0203626172023739ac1f0203626172023238bc1f0203626172023434cc1f0203626172023939dc1f0203626172023533ec1f0203666f6f03626172fc1f02036261720238328c2002036261720235349c200203626172023135ac2002036261720133bc200203626172023633cc200203626172023837dc2002036261720132ec200203626172023839fc20020362617201308c2102036261720236309c210203626172023637ac210203626172023430bc21f77dab6e00000000000000050000000000000143000000000000070e00000000000010cc00000000000008c800000000000010e6bdcd4114";

fn write_v1_fixture_block(dir: &Path) -> PathBuf {
    let bdir = dir.join("01DXXFZDYD1MQW6079WK0K6EDQ");
    fs::create_dir_all(bdir.join("chunks")).unwrap();
    fs::write(bdir.join("meta.json"), unhex(V1_META_HEX)).unwrap();
    fs::write(bdir.join("tombstones"), unhex(V1_TOMBS_HEX)).unwrap();
    fs::write(bdir.join("chunks").join("000001"), unhex(V1_CHUNK_HEX)).unwrap();
    fs::write(bdir.join(INDEX_FILENAME), unhex(V1_INDEX_HEX)).unwrap();
    bdir
}

// Upstream: github.com/prometheus/prometheus/tsdb/block_test.go::TestReadIndexFormatV1
#[test]
fn test_read_index_format_v1() {
    let dir = tempfile::tempdir().unwrap();
    let bdir = write_v1_fixture_block(dir.path());
    let b = Arc::new(Block::open(&bdir).unwrap());
    let q = TsdbQuerier::new(None, vec![b], 0, 1000);

    let s1 = q.select(true, &[LabelMatcher::new(MatchType::Equal, "foo", "bar").unwrap()]).unwrap();
    assert_eq!(s1.len(), 1);
    assert_eq!(s1[0].labels.get("foo"), "bar");
    assert_eq!(s1[0].samples.len(), 1);
    assert_eq!((s1[0].samples[0].t, s1[0].samples[0].value.clone()), (1, SampleValue::Float(2.0)));

    let s2 = q.select(true, &[LabelMatcher::new(MatchType::RegexNotMatch, "foo", "^.?$").unwrap()]).unwrap();
    assert_eq!(s2.len(), 2);
    assert_eq!(s2[0].labels.get("foo"), "bar");
    assert_eq!(s2[1].labels.get("foo"), "baz");
}

// Upstream: github.com/prometheus/prometheus/tsdb/block_test.go::TestSearchLabelValuesIndexFormatV1
#[test]
fn test_search_label_values_index_format_v1() {
    let dir = tempfile::tempdir().unwrap();
    let bdir = write_v1_fixture_block(dir.path());
    let b = Arc::new(Block::open(&bdir).unwrap());
    assert_eq!(b.index().version(), INDEX_FORMAT_V1);

    let want_smallest = vec!["0", "1", "10", "11", "12"];
    let all = b.index().label_values("bar").unwrap();
    assert_eq!(all.len(), 100);
    let smallest = b.index().label_values_with_hints("bar", Some(5), true).unwrap();
    assert_eq!(smallest, want_smallest);

    let q = TsdbQuerier::new(None, vec![b], 0, 1000);
    let hints = SearchHints { limit: 5, ..SearchHints::default() };
    let got: Vec<String> = q.search_label_values("bar", &hints, &[]).unwrap().into_iter().map(|r| r.value).collect();
    assert_eq!(got, want_smallest);
}

// Upstream: github.com/prometheus/prometheus/tsdb/block_test.go::TestLabelNamesWithMatchers
#[test]
fn test_label_names_with_matchers() {
    let dir = tempfile::tempdir().unwrap();
    let mut series = Vec::new();
    for i in 0..100usize {
        let u = format!("value{i}");
        series.push(simple_series(&[("unique", &u)], &[(100, 0.0)]));
        if i % 10 == 0 {
            let t = format!("value{}", i / 10);
            series.push(simple_series(&[("tens", &t), ("unique", &u)], &[(100, 0.0)]));
        }
        if i % 20 == 0 {
            let t = format!("value{}", i / 10);
            let tw = format!("value{}", i / 20);
            series.push(simple_series(&[("tens", &t), ("twenties", &tw), ("unique", &u)], &[(100, 0.0)]));
        }
    }
    let bdir = create_block_series(dir.path(), 0, 200, series);
    let b = Arc::new(Block::open(&bdir).unwrap());
    let q = TsdbQuerier::new(None, vec![b], 0, 200);
    assert_eq!(q.label_names(&[LabelMatcher::new(MatchType::NotEqual, "unique", "").unwrap()]).unwrap(), vec!["tens", "twenties", "unique"]);
    assert_eq!(q.label_names(&[LabelMatcher::new(MatchType::RegexMatch, "unique", "value.*1").unwrap()]).unwrap(), vec!["unique"]);
    assert_eq!(q.label_names(&[LabelMatcher::new(MatchType::Equal, "unique", "value20").unwrap()]).unwrap(), vec!["tens", "twenties", "unique"]);
    assert_eq!(q.label_names(&[LabelMatcher::new(MatchType::Equal, "tens", "value1").unwrap()]).unwrap(), vec!["tens", "unique"]);
}

// Upstream: github.com/prometheus/prometheus/tsdb/block_test.go::TestBlockIndexReader_PostingsForLabelMatching
#[test]
fn test_block_index_reader_postings_for_label_matching() {
    let dir = tempfile::tempdir().unwrap();
    let series = vec![
        simple_series(&[("n", "1")], &[(100, 0.0)]),
        simple_series(&[("n", "1"), ("i", "a")], &[(100, 0.0)]),
        simple_series(&[("n", "1"), ("i", "b")], &[(100, 0.0)]),
        simple_series(&[("n", "2")], &[(100, 0.0)]),
        simple_series(&[("n", "2.5")], &[(100, 0.0)]),
    ];
    let bdir = create_block_series(dir.path(), 0, 200, series);
    let b = Block::open(&bdir).unwrap();
    let idx = b.index();
    let all_n1 = idx.postings_for_label_matching("n", &mut |v| v == "1").unwrap();
    assert_eq!(all_n1.len(), 3);
    let all_n2 = idx.postings_for_label_matching("n", &mut |v| v == "2").unwrap();
    assert_eq!(all_n2.len(), 1);
    assert_ne!(all_n1[2], all_n2[0]);
    let missing = idx.postings_for_label_matching("missing", &mut |_| true).unwrap();
    assert!(missing.is_empty());
}

fn meta_range(name: &str, mint: i64, maxt: i64, stats: Option<BlockStats>) -> DirMeta {
    DirMeta {
        dir: PathBuf::from(name),
        meta: BlockMeta {
            ulid: Ulid::new(),
            min_time: mint,
            max_time: maxt,
            stats: stats.unwrap_or_default(),
            compaction: BlockMetaCompaction::default(),
            version: META_VERSION_1,
            out_of_order: false,
            thanos: None,
        },
    }
}

fn stale_meta_range(name: &str, mint: i64, maxt: i64) -> DirMeta {
    let mut dm = meta_range(name, mint, maxt, None);
    dm.meta.compaction.set_stale_series();
    dm
}

fn plan_names(dms: Vec<DirMeta>, ranges: &[i64], overlap: bool) -> Option<Vec<String>> {
    plan_dir_metas(dms, ranges, overlap).unwrap().map(|v| {
        v.into_iter()
            .map(|p| p.to_string_lossy().into_owned())
            .collect()
    })
}

// Upstream: github.com/prometheus/prometheus/tsdb/compact_test.go::TestSplitByRange
#[test]
fn test_split_by_range() {
    type SplitCase = (i64, &'static [(i64, i64)], &'static [&'static [(i64, i64)]]);
    let cases: &[SplitCase] = &[
        (60, &[(0, 10)], &[&[(0, 10)]]),
        (60, &[(0, 60)], &[&[(0, 60)]]),
        (60, &[(0, 10), (9, 15), (30, 60)], &[&[(0, 10), (9, 15), (30, 60)]]),
        (60, &[(70, 90), (125, 130), (130, 180), (1000, 1001)], &[&[(70, 90)], &[(125, 130), (130, 180)], &[(1000, 1001)]]),
        (60, &[(50, 70), (70, 80)], &[&[(70, 80)]]),
        (72, &[(0, 144), (144, 216), (216, 288)], &[&[(144, 216)], &[(216, 288)]]),
        (60, &[(-10, -5)], &[&[(-10, -5)]]),
        (60, &[(-60, -50), (-10, -5)], &[&[(-60, -50), (-10, -5)]]),
        (60, &[(-60, -50), (-10, -5), (0, 15)], &[&[(-60, -50), (-10, -5)], &[(0, 15)]]),
    ];
    for &(tr, ranges, exp) in cases {
        let blocks: Vec<DirMeta> = ranges.iter().map(|&(a, b)| meta_range("", a, b, None)).collect();
        let got: Vec<Vec<(i64, i64)>> = split_by_range(&blocks, tr)
            .into_iter()
            .map(|g| g.into_iter().map(|d| (d.meta.min_time, d.meta.max_time)).collect())
            .collect();
        let exp_v: Vec<Vec<(i64, i64)>> = exp.iter().map(|g| g.to_vec()).collect();
        assert_eq!(got, exp_v);
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/compact_test.go::TestNoPanicFor0Tombstones
#[test]
fn test_no_panic_for_0_tombstones() {
    let metas = vec![meta_range("1", 0, 100, None), meta_range("2", 101, 200, None)];
    let _ = plan_names(metas, &[50], true);
}

// Upstream: github.com/prometheus/prometheus/tsdb/compact_test.go::TestPlanDoesNotMergeStaleAndNonStaleBlocks
#[test]
fn test_plan_does_not_merge_stale_and_non_stale_blocks() {
    let ranges = [20, 60, 180];
    let r1 = plan_names(
        vec![meta_range("1", 0, 20, None), stale_meta_range("stale", 0, 20), meta_range("2", 20, 40, None), meta_range("3", 40, 60, None), meta_range("fresh", 60, 80, None)],
        &ranges,
        true,
    );
    assert_eq!(r1, Some(vec!["1".into(), "2".into(), "3".into()]));

    let r2 = plan_names(
        vec![stale_meta_range("s1", 0, 20), stale_meta_range("s2", 20, 40), stale_meta_range("s3", 40, 60), stale_meta_range("s4", 60, 80)],
        &ranges,
        true,
    );
    assert_eq!(r2, Some(vec!["s1".into(), "s2".into(), "s3".into()]));

    let r3 = plan_names(
        vec![meta_range("1", 0, 20, None), stale_meta_range("s1", 0, 20), stale_meta_range("s2", 20, 40), stale_meta_range("s3", 40, 60), stale_meta_range("s4", 60, 80)],
        &ranges,
        true,
    );
    assert_eq!(r3, Some(vec!["s1".into(), "s2".into(), "s3".into()]));
}

fn plan_cases(enable_overlapping: bool) {
    let ranges = [20, 60, 180, 540, 1620];
    let tomb_stats = Some(BlockStats { num_series: 10, num_tombstones: 3, ..BlockStats::default() });
    let zero_stats = Some(BlockStats::default());
    let cases: Vec<(Vec<DirMeta>, Option<Vec<&str>>)> = vec![
        (vec![meta_range("1", 0, 20, None)], None),
        (vec![meta_range("1", 0, 20, None), meta_range("2", 20, 40, None)], None),
        (vec![meta_range("1", 0, 20, None), meta_range("2", 20, 40, None), meta_range("3", 40, 60, None)], None),
        (vec![meta_range("1", 0, 20, None), meta_range("2", 20, 40, None), meta_range("3", 40, 60, None), meta_range("4", 60, 80, None)], Some(vec!["1", "2", "3"])),
        (vec![meta_range("1", 0, 20, None), meta_range("2", 20, 40, None), meta_range("3", 60, 80, None)], None),
        (vec![meta_range("1", 0, 20, None), meta_range("2", 20, 40, None), meta_range("3", 60, 80, None), meta_range("4", 80, 100, None)], Some(vec!["1", "2"])),
        (vec![meta_range("1", 0, 20, None), meta_range("2", 20, 40, None), meta_range("3", 40, 60, None), meta_range("4", 60, 120, None), meta_range("5", 120, 180, None)], Some(vec!["1", "2", "3"])),
        (vec![meta_range("2", 20, 40, None), meta_range("4", 60, 120, None), meta_range("5", 960, 980, None), meta_range("6", 120, 180, None), meta_range("7", 720, 960, None)], Some(vec!["2", "4", "6"])),
        (vec![meta_range("1", 0, 540, tomb_stats.clone())], None),
        (vec![meta_range("1", 0, 540, tomb_stats.clone()), meta_range("2", 540, 560, None)], Some(vec!["1"])),
        (vec![meta_range("1", 0, 60, tomb_stats), meta_range("2", 60, 80, None)], None),
        (vec![meta_range("1", 0, 540, zero_stats), meta_range("2", 540, 560, None)], None),
        (vec![meta_range("5", 0, 360, None), meta_range("6", 540, 560, None), meta_range("7", 360, 420, None), meta_range("8", 420, 540, None)], Some(vec!["7", "8"])),
        (vec![meta_range("1", 0, 20, None), meta_range("2", 19, 40, None), meta_range("3", 40, 60, None)], if enable_overlapping { Some(vec!["1", "2"]) } else { None }),
        (vec![meta_range("1", 0, 20, None), meta_range("2", 20, 40, None), meta_range("3", 30, 50, None)], if enable_overlapping { Some(vec!["2", "3"]) } else { None }),
        (vec![meta_range("1", 0, 20, None), meta_range("2", 10, 40, None), meta_range("3", 30, 50, None)], if enable_overlapping { Some(vec!["1", "2", "3"]) } else { None }),
        (vec![meta_range("5", 0, 360, None), meta_range("6", 340, 560, None), meta_range("7", 360, 420, None), meta_range("8", 420, 540, None)], if enable_overlapping { Some(vec!["5", "6", "7", "8"]) } else { None }),
        (vec![meta_range("1", 0, 10, None), meta_range("2", 9, 20, None), meta_range("3", 30, 40, None), meta_range("4", 39, 50, None)], if enable_overlapping { Some(vec!["1", "2"]) } else { None }),
    ];
    for (i, (metas, exp)) in cases.into_iter().enumerate() {
        let got = plan_names(metas, &ranges, enable_overlapping);
        let exp_s = exp.map(|v| v.into_iter().map(str::to_string).collect());
        assert_eq!(got, exp_s, "case {i}");
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/compact_test.go::TestLeveledCompactor
#[test]
fn test_leveled_compactor() {
    plan_cases(true);

    // BlockExcludeFilter stops iteration.
    let dir = tempfile::tempdir().unwrap();
    let mut uids = Vec::new();
    for i in 0..4u128 {
        let uid = Ulid::from_parts(i as u64 + 1, 0);
        let bdir = dir.path().join(uid.to_string());
        fs::create_dir_all(&bdir).unwrap();
        let mut meta = BlockMeta {
            ulid: uid,
            min_time: (i as i64) * 10,
            max_time: (i as i64 + 1) * 10,
            stats: BlockStats::default(),
            compaction: BlockMetaCompaction { level: 1, ..BlockMetaCompaction::default() },
            version: META_VERSION_1,
            out_of_order: false,
            thanos: None,
        };
        write_meta_file(&bdir, &mut meta).unwrap();
        uids.push(uid);
    }
    let mut evaluated = Vec::new();
    let mut filter = |m: &BlockMeta| -> bool {
        evaluated.push(m.ulid);
        m.ulid == uids[1]
    };
    let _ = plan_block_compactions_with_filter(dir.path(), &[20], true, Some(&mut filter)).unwrap();
    assert_eq!(evaluated.len(), 2);
    assert!(evaluated.contains(&uids[0]));
    assert!(evaluated.contains(&uids[1]));
}

// Upstream: github.com/prometheus/prometheus/tsdb/compact_test.go::TestRangeWithFailedCompactionWontGetSelected
#[test]
fn test_range_with_failed_compaction_wont_get_selected() {
    let ranges = [20, 60, 240, 720, 2160];
    let inputs = [
        vec![meta_range("1", 0, 20, None), meta_range("2", 20, 40, None), meta_range("3", 40, 60, None), meta_range("4", 60, 80, None)],
        vec![meta_range("1", 0, 20, None), meta_range("2", 20, 40, None), meta_range("3", 60, 80, None), meta_range("4", 80, 100, None)],
        vec![meta_range("1", 0, 20, None), meta_range("2", 20, 40, None), meta_range("3", 40, 60, None), meta_range("4", 60, 120, None), meta_range("5", 120, 180, None), meta_range("6", 180, 200, None)],
    ];
    for mut metas in inputs {
        metas[1].meta.compaction.failed = true;
        assert_eq!(plan_names(metas, &ranges, true), None);
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/compact_test.go::TestCompactionFailWillCleanUpTempDir
#[test]
fn test_compaction_fail_will_clean_up_temp_dir() {
    let dir = tempfile::tempdir().unwrap();
    let bdir = create_gen_block(dir.path(), 2, 2, 0, 100);
    let b = Block::open(&bdir).unwrap();
    // Corrupt chunk segment so reading chunks fails during compaction.
    fs::write(bdir.join("chunks").join("000001"), [0u8; 4]).unwrap();
    assert!(compact_blocks(dir.path(), &[&b]).is_err());
    for e in fs::read_dir(dir.path()).unwrap() {
        let name = e.unwrap().file_name().to_string_lossy().into_owned();
        assert!(!name.contains(".tmp"), "leftover tmp dir: {name}");
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/compact_test.go::TestCompaction_CompactWithSplitting
#[test]
fn test_compaction_compact_with_splitting() {
    for series in [10usize, 1234] {
        let dir = tempfile::tempdir().unwrap();
        let mut blocks = Vec::new();
        for (a, b) in [(0, 5000), (3000, 8000), (6000, 11000), (9000, 14000)] {
            let bd = create_gen_block(dir.path(), series, 10, a, b);
            blocks.push(Block::open(&bd).unwrap());
        }
        let brefs: Vec<&Block> = blocks.iter().collect();
        for shard_count in [1u64, 13] {
            let ids = compact_blocks_with_splitting(dir.path(), &brefs, shard_count).unwrap();
            assert_eq!(ids.len(), shard_count as usize);
            let mut total_series = 0u64;
            let mut ts = None;
            for (shard_idx, uid_opt) in ids.into_iter().enumerate() {
                let Some(uid) = uid_opt else { continue };
                if let Some(prev_ts) = ts {
                    assert_eq!(uid.timestamp_ms(), prev_ts);
                } else {
                    ts = Some(uid.timestamp_ms());
                }
                let sb = Block::open(dir.path().join(uid.to_string())).unwrap();
                total_series += sb.meta().stats.num_series;
                let mut series_syms = HashSet::new();
                series_syms.insert(String::new());
                let all_ids = sb.index().postings("", &[""]).unwrap();
                for r in all_ids {
                    let (lbls, _) = sb.index().series(r).unwrap();
                    assert_eq!(lbls.stable_hash() % shard_count, shard_idx as u64);
                    for (k, v) in lbls.iter() {
                        series_syms.insert(k.to_string());
                        series_syms.insert(v.to_string());
                    }
                }
                let idx_syms = sb.index().symbols().unwrap();
                for s in idx_syms {
                    assert!(series_syms.remove(&s), "unused symbol {s:?}");
                }
                assert!(series_syms.is_empty());
            }
            assert_eq!(total_series, series as u64);
            for b in &blocks {
                assert!(!b.meta().compaction.deletable);
            }
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/compact_test.go::TestCompaction_CompactEmptyBlocks
#[test]
fn test_compaction_compact_empty_blocks() {
    let dir = tempfile::tempdir().unwrap();
    let mut blocks = Vec::new();
    for (a, b) in [(0, 5000), (3000, 8000), (6000, 11000), (9000, 14000)] {
        let uid = Ulid::new();
        let bdir = dir.path().join(uid.to_string());
        fs::create_dir_all(bdir.join("chunks")).unwrap();
        let mut m = BlockMeta {
            ulid: uid,
            min_time: a,
            max_time: b,
            stats: BlockStats::default(),
            compaction: BlockMetaCompaction { level: 1, sources: vec![uid], ..BlockMetaCompaction::default() },
            version: META_VERSION_1,
            out_of_order: false,
            thanos: None,
        };
        write_meta_file(&bdir, &mut m).unwrap();
        write_tombstones_file(&bdir, &MemTombstones::new()).unwrap();
        let mut iw = IndexWriter::new(bdir.join(INDEX_FILENAME)).unwrap();
        iw.add_symbol("hello").unwrap();
        iw.add_symbol("world").unwrap();
        iw.close().unwrap();
        blocks.push(Block::open(&bdir).unwrap());
    }
    let brefs: Vec<&Block> = blocks.iter().collect();
    let out_ids = compact_blocks_with_splitting(dir.path(), &brefs, 5).unwrap();
    assert!(out_ids.iter().all(Option::is_none));
    for b in &blocks {
        let m = read_meta_file(b.dir()).unwrap();
        assert!(m.compaction.deletable);
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/compact_test.go::TestCompaction_populateBlock
#[test]
fn test_compaction_populate_block() {
    // Empty input.
    let dir0 = tempfile::tempdir().unwrap();
    assert!(compact_blocks(dir0.path(), &[]).unwrap().is_none());

    // Two blocks with overlapping and non-overlapping series, negative timestamps, and tombstones.
    let dir = tempfile::tempdir().unwrap();
    let b1_dir = create_block_series(dir.path(), -20, 25, vec![
        simple_series(&[("a", "b")], &[(0, 1.0), (10, 2.0), (11, 3.0), (20, 4.0)]),
        simple_series(&[("a", "c")], &[(-11, 5.0), (-9, 6.0), (10, 7.0), (19, 8.0)]),
    ]);
    let b2_dir = create_block_series(dir.path(), 20, 50, vec![
        simple_series(&[("a", "b")], &[(21, 9.0), (30, 10.0)]),
        simple_series(&[("a", "c")], &[(40, 11.0), (45, 12.0)]),
    ]);
    let b1 = Block::open(&b1_dir).unwrap();
    let b2 = Block::open(&b2_dir).unwrap();
    // Delete [10, 15] in b1 only; b2 samples remain intact.
    b1.delete(10, 15, &[LabelMatcher::new(MatchType::Equal, "a", "b").unwrap()]).unwrap();
    let out_meta = compact_blocks(dir.path(), &[&b1, &b2]).unwrap().unwrap();
    let out_b = Arc::new(Block::open(dir.path().join(out_meta.ulid.to_string())).unwrap());
    let q = TsdbQuerier::new(None, vec![out_b], i64::MIN, i64::MAX);
    let got = q.select(true, &[LabelMatcher::new(MatchType::RegexMatch, "a", ".*").unwrap()]).unwrap();
    assert_eq!(got.len(), 2);
    let ts_b: Vec<i64> = got[0].samples.iter().map(|s| s.t).collect();
    let ts_c: Vec<i64> = got[1].samples.iter().map(|s| s.t).collect();
    assert_eq!(ts_b, vec![0, 20, 21, 30]);
    assert_eq!(ts_c, vec![-11, -9, 10, 19, 40, 45]);
}

// Upstream: github.com/prometheus/prometheus/tsdb/compact_test.go::TestCompactionFromHead
#[test]
fn test_compaction_from_head() {
    let dir = tempfile::tempdir().unwrap();
    let opts = HeadOptions { chunk_range_ms: 100, ..HeadOptions::default() };
    let head = Head::open(dir.path(), opts, i64::MIN).unwrap();
    let mut exp = Vec::new();
    {
        let mut app = head.appender();
        for i in (0..1000i64).step_by(5) {
            app.append_float(None, &[("__name__", "test")], i, i as f64).unwrap();
            exp.push((i, i as f64));
        }
        app.commit().unwrap();
    }
    head.mmap_head_chunks().unwrap();
    let bdir_root = tempfile::tempdir().unwrap();
    let mint = head.min_time();
    let maxt_incl = head.max_time();
    let candidates = head.collect_compaction_candidates(mint, maxt_incl).unwrap();
    let meta = mimir_tsdb::compact::write_head_block_streaming(
        bdir_root.path(),
        mint,
        maxt_incl + 1,
        candidates,
        BlockMetaCompaction::default(),
        false,
        0,
        |ref_id, chks, tombs| {
            head.load_series_in_order_chunks_into(ref_id, mint, maxt_incl, chks, tombs)
                .map_err(|e| CompactError::Io(std::io::Error::other(e.to_string())))
        },
    ).unwrap().unwrap();
    let b = Arc::new(Block::open(bdir_root.path().join(meta.ulid.to_string())).unwrap());
    let q = TsdbQuerier::new(None, vec![b], i64::MIN, i64::MAX);
    let got = q.select(false, &[LabelMatcher::new(MatchType::Equal, "__name__", "test").unwrap()]).unwrap();
    assert_eq!(got.len(), 1);
    let got_s: Vec<(i64, f64)> = got[0].samples.iter().map(|s| match s.value { SampleValue::Float(v) => (s.t, v), _ => panic!() }).collect();
    assert_eq!(got_s, exp);
    head.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/compact_test.go::TestDisableAutoCompactions
#[test]
fn test_disable_auto_compactions() {
    let dir = tempfile::tempdir().unwrap();
    let reg = Arc::new(prometheus::Registry::new());
    let opts = TsdbOptions {
        head: HeadOptions { registry: Some(Arc::clone(&reg)), chunk_range_ms: 1000, ..HeadOptions::default() },
        ..TsdbOptions::default()
    };
    let db = Tsdb::open_with_reload_interval(dir.path(), opts, Duration::from_millis(20)).unwrap();
    db.disable_compactions();
    {
        let mut app = db.appender();
        for i in 0..3i64 {
            app.append_float(None, &[("foo", "bar")], i * 1000, 1.0).unwrap();
            app.append_float(None, &[("foo", "bar")], i * 1000 + 500, 2.0).unwrap();
        }
        app.commit().unwrap();
    }
    db.trigger_auto_compaction().unwrap();
    assert!(counter_val(&reg, "prometheus_tsdb_compactions_skipped_total") > 0.0);
    assert!(db.blocks().is_empty());

    db.enable_compactions();
    db.trigger_auto_compaction().unwrap();
    assert!(!db.blocks().is_empty());
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/compact_test.go::TestCancelCompactions
#[test]
fn test_cancel_compactions() {
    let dir = tempfile::tempdir().unwrap();
    let reg = Arc::new(prometheus::Registry::new());
    let opts = TsdbOptions {
        head: HeadOptions { registry: Some(Arc::clone(&reg)), chunk_range_ms: 1000, ..HeadOptions::default() },
        ..TsdbOptions::default()
    };
    let db = Tsdb::open_with_reload_interval(dir.path(), opts, Duration::ZERO).unwrap();
    let bdir1 = create_gen_block(dir.path(), 1, 2, 0, 100);
    let bdir2 = create_gen_block(dir.path(), 1, 2, 100, 200);
    let b1 = Block::open(&bdir1).unwrap();
    let b2 = Block::open(&bdir2).unwrap();
    let cancel = AtomicBool::new(true);
    let res = compact_blocks_cancellable(dir.path(), &[&b1, &b2], Some(&cancel));
    assert!(matches!(res, Err(CompactError::Cancelled)));
    db.close().unwrap();
    assert_eq!(counter_val(&reg, "prometheus_tsdb_compactions_failed_total"), 0.0);
}

// Upstream: github.com/prometheus/prometheus/tsdb/compact_test.go::TestCanceledCompactionDoesNotMarkBlocksFailed
#[test]
fn test_canceled_compaction_does_not_mark_blocks_failed() {
    let dir = tempfile::tempdir().unwrap();
    let b1_dir = create_gen_block(dir.path(), 1, 2, 0, 100);
    let b2_dir = create_gen_block(dir.path(), 1, 2, 100, 200);
    let b1 = Block::open(&b1_dir).unwrap();
    let b2 = Block::open(&b2_dir).unwrap();
    let cancel = AtomicBool::new(true);
    let err = compact_blocks_cancellable(dir.path(), &[&b1, &b2], Some(&cancel)).unwrap_err();
    assert!(matches!(err, CompactError::Cancelled));
    for bd in [&b1_dir, &b2_dir] {
        let m = read_meta_file(bd).unwrap();
        assert!(!m.compaction.failed);
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/compact_test.go::TestDeleteCompactionBlockAfterFailedReload
#[test]
fn test_delete_compaction_block_after_failed_reload() {
    // Case 1: Head compaction + corrupt block triggers failed reload -> new block deleted.
    {
        let dir = tempfile::tempdir().unwrap();
        let reg = Arc::new(prometheus::Registry::new());
        let opts = TsdbOptions {
            head: HeadOptions { registry: Some(Arc::clone(&reg)), chunk_range_ms: 100, ..HeadOptions::default() },
            ..TsdbOptions::default()
        };
        let db = Tsdb::open_with_reload_interval(dir.path(), opts, Duration::ZERO).unwrap();
        db.set_block_ranges(vec![100]);
        {
            let mut app = db.appender();
            app.append_float(None, &[("foo", "bar")], 1, 0.0).unwrap();
            app.append_float(None, &[("foo", "bar")], 2, 0.0).unwrap();
            app.append_float(None, &[("foo", "bar")], 152, 0.0).unwrap();
            app.commit().unwrap();
        }
        let corrupt = create_gen_block(dir.path(), 1, 1, 200, 300);
        fs::remove_file(corrupt.join(INDEX_FILENAME)).unwrap();
        assert!(db.compact().is_err());
        assert_eq!(counter_val(&reg, "prometheus_tsdb_reloads_failures_total"), 1.0);
        assert_eq!(counter_val(&reg, "prometheus_tsdb_compactions_failed_total"), 1.0);
        let dirs = list_block_dirs(dir.path()).unwrap();
        assert_eq!(dirs.len(), 1); // only the corrupt block remains
        db.close().unwrap();
    }
    // Case 2: Block compaction + corrupt block triggers failed reload -> merged block deleted.
    {
        let dir = tempfile::tempdir().unwrap();
        let reg = Arc::new(prometheus::Registry::new());
        let opts = TsdbOptions {
            head: HeadOptions { registry: Some(Arc::clone(&reg)), chunk_range_ms: 1, ..HeadOptions::default() },
            ..TsdbOptions::default()
        };
        for (a, b) in [(0, 100), (100, 150), (150, 200)] {
            create_gen_block(dir.path(), 1, 1, a, b);
        }
        let db = Tsdb::open_with_reload_interval(dir.path(), opts, Duration::ZERO).unwrap();
        db.set_block_ranges(vec![1, 100]);
        assert_eq!(db.blocks().len(), 3);
        let corrupt = create_gen_block(dir.path(), 1, 1, 200, 300);
        fs::remove_file(corrupt.join(INDEX_FILENAME)).unwrap();
        assert!(db.compact().is_err());
        assert_eq!(counter_val(&reg, "prometheus_tsdb_reloads_failures_total"), 1.0);
        assert_eq!(counter_val(&reg, "prometheus_tsdb_compactions_failed_total"), 1.0);
        let dirs = list_block_dirs(dir.path()).unwrap();
        assert_eq!(dirs.len(), 4); // 3 original + 1 corrupt
        db.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/compact_test.go::TestOpenBlocksForCompaction
#[test]
fn test_open_blocks_for_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let dirs: Vec<_> = (0..5).map(|_| create_gen_block(dir.path(), 100, 10, 0, 5000)).collect();
    let (opened, to_close, res) = open_blocks_for_compaction(&dirs[..2], &[], 10);
    res.unwrap();
    assert_eq!(opened.len(), 2);
    assert_eq!(to_close.len(), 2);
    let (opened2, to_close2, res2) = open_blocks_for_compaction(&dirs, &opened, 10);
    res2.unwrap();
    assert_eq!(opened2.len(), 5);
    assert_eq!(to_close2.len(), 3);
}

// Upstream: github.com/prometheus/prometheus/tsdb/compact_test.go::TestOpenBlocksForCompactionErrorsNoMeta
#[test]
fn test_open_blocks_for_compaction_errors_no_meta() {
    let dir = tempfile::tempdir().unwrap();
    let mut dirs = Vec::new();
    for i in 0..5 {
        dirs.push(create_gen_block(dir.path(), 10, 2, 0, 5000));
        if i == 3 {
            dirs.push(dir.path().join("invalid-block"));
        }
    }
    let b0 = Arc::new(Block::open(&dirs[0]).unwrap());
    let (_, to_close, res) = open_blocks_for_compaction(&dirs, &[b0], 10);
    assert!(res.is_err());
    assert!(to_close.is_empty());
}

// Upstream: github.com/prometheus/prometheus/tsdb/compact_test.go::TestOpenBlocksForCompactionErrorsMissingIndex
#[test]
fn test_open_blocks_for_compaction_errors_missing_index() {
    let dir = tempfile::tempdir().unwrap();
    let mut dirs = Vec::new();
    for i in 0..5 {
        let bd = create_gen_block(dir.path(), 10, 2, 0, 5000);
        if i == 3 {
            fs::remove_file(bd.join(INDEX_FILENAME)).unwrap();
        }
        dirs.push(bd);
    }
    let b1 = Arc::new(Block::open(&dirs[1]).unwrap());
    let (opened, to_close, res) = open_blocks_for_compaction(&dirs, &[b1], 1);
    assert!(res.is_err());
    assert_eq!(opened.len(), 3);
    assert_eq!(to_close.len(), 2);
}

// Upstream: github.com/prometheus/prometheus/tsdb/compact_test.go::TestHeadCompactionWithHistograms
#[test]
fn test_head_compaction_with_histograms() {
    for float_test in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let head = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();
        let h = Histogram {
            counter_reset_hint: CounterResetHint::UnknownCounterReset,
            schema: 1,
            zero_threshold: 0.001,
            zero_count: 4,
            count: 15,
            sum: 35.5,
            positive_spans: vec![HistogramSpan { offset: 0, length: 2 }, HistogramSpan { offset: 2, length: 2 }],
            negative_spans: vec![HistogramSpan { offset: 0, length: 1 }, HistogramSpan { offset: 1, length: 2 }],
            positive_buckets: vec![1, 1, -1, 0],
            negative_buckets: vec![1, 2, -1],
            custom_values: vec![],
        };
        let fh = FloatHistogram {
            counter_reset_hint: CounterResetHint::UnknownCounterReset,
            schema: 1,
            zero_threshold: 0.001,
            zero_count: 4.0,
            count: 15.0,
            sum: 35.5,
            positive_spans: h.positive_spans.clone(),
            negative_spans: h.negative_spans.clone(),
            positive_buckets: vec![1.0, 2.0, 1.0, 1.0],
            negative_buckets: vec![1.0, 3.0, 2.0],
            custom_values: vec![],
        };
        let min_ms = |m: i64| m * 60_000;
        let append_h = |name: &str, from: i64, to: i64| {
            let mut app = head.appender();
            for m in from..=to {
                if float_test {
                    app.append_float_histogram(None, &[("foo", name)], 0, min_ms(m), fh.clone()).unwrap();
                } else {
                    app.append_histogram(None, &[("foo", name)], 0, min_ms(m), h.clone()).unwrap();
                }
            }
            app.commit().unwrap();
        };
        let append_f = |name: &str, from: i64, to: i64| {
            let mut app = head.appender();
            for m in from..=to {
                app.append_float(None, &[("foo", name)], min_ms(m), m as f64).unwrap();
            }
            app.commit().unwrap();
        };
        append_h("bar1", 100, 105);
        append_f("bar2", 100, 102); append_h("bar2", 103, 105); append_f("bar2", 106, 107); append_h("bar2", 108, 109);
        append_h("bar3", 101, 103); append_f("bar3", 104, 106); append_h("bar3", 107, 108); append_f("bar3", 109, 110);
        append_f("bar4", 100, 102);

        let bdir_root = tempfile::tempdir().unwrap();
        let mint = head.min_time();
        let maxt_incl = head.max_time();
        let candidates = head.collect_compaction_candidates(mint, maxt_incl).unwrap();
        let meta = mimir_tsdb::compact::write_head_block_streaming(
            bdir_root.path(), mint, maxt_incl + 1, candidates, BlockMetaCompaction::default(), false, 0,
            |ref_id, chks, tombs| head.load_series_in_order_chunks_into(ref_id, mint, maxt_incl, chks, tombs).map_err(|e| CompactError::Io(std::io::Error::other(e.to_string()))),
        ).unwrap().unwrap();
        let b = Arc::new(Block::open(bdir_root.path().join(meta.ulid.to_string())).unwrap());
        let q = TsdbQuerier::new(None, vec![b], i64::MIN, i64::MAX);
        let got = q.select(true, &[LabelMatcher::new(MatchType::RegexMatch, "foo", "bar.*").unwrap()]).unwrap();
        assert_eq!(got.len(), 4);
        assert_eq!(got[0].samples.len(), 6);
        assert_eq!(got[1].samples.len(), 10);
        assert_eq!(got[2].samples.len(), 10);
        assert_eq!(got[3].samples.len(), 3);
        head.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/compact_test.go::TestSparseHistogramSpaceSavings
#[test]
fn test_sparse_histogram_space_savings() {
    let sparse_dir = tempfile::tempdir().unwrap();
    let flat_dir = tempfile::tempdir().unwrap();
    let sparse_db = Tsdb::open(sparse_dir.path(), TsdbOptions::default()).unwrap();
    let flat_db = Tsdb::open(flat_dir.path(), TsdbOptions::default()).unwrap();
    let h = Histogram {
        counter_reset_hint: CounterResetHint::UnknownCounterReset,
        schema: 0,
        zero_threshold: 1e-128,
        zero_count: 1,
        count: 121,
        sum: 42.0,
        positive_spans: vec![HistogramSpan { offset: 0, length: 15 }],
        negative_spans: vec![],
        positive_buckets: vec![1; 15],
        negative_buckets: vec![],
        custom_values: vec![],
    };
    {
        let mut sapp = sparse_db.appender();
        let mut fapp = flat_db.appender();
        for t in 0..60i64 {
            let mut h_t = h.clone();
            h_t.count += t as u64;
            h_t.zero_count += t as u64;
            sapp.append_histogram(None, &[("__name__", "rpc_hist")], 0, t * 15_000, h_t).unwrap();
            for b in 0..15 {
                let le = format!("{b}");
                fapp.append_float(None, &[("__name__", "rpc_hist_bucket"), ("le", &le)], t * 15_000, (t + 1) as f64).unwrap();
            }
        }
        sapp.commit().unwrap();
        fapp.commit().unwrap();
    }
    let suid = sparse_db.compact_head_all().unwrap().unwrap();
    let fuid = flat_db.compact_head_all().unwrap().unwrap();
    let sb = Block::open(sparse_dir.path().join(suid.to_string())).unwrap();
    let fb = Block::open(flat_dir.path().join(fuid.to_string())).unwrap();
    assert!(sb.size() < fb.size(), "native histogram {} should be smaller than expanded float series {}", sb.size(), fb.size());
    sparse_db.close().unwrap();
    flat_db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/compact_test.go::TestCompactBlockMetas
#[test]
fn test_compact_block_metas() {
    let p1 = Ulid::from_parts(100, 0);
    let p2 = Ulid::from_parts(200, 0);
    let p3 = Ulid::from_parts(300, 0);
    let p4 = Ulid::from_parts(400, 0);
    let s1 = Ulid::from_parts(1, 0);
    let s5 = Ulid::from_parts(5, 0);
    let s6 = Ulid::from_parts(6, 0);
    let s10 = Ulid::from_parts(10, 0);
    let m1 = BlockMeta { ulid: p1, min_time: 1000, max_time: 2000, stats: BlockStats::default(), compaction: BlockMetaCompaction { level: 2, sources: vec![s1, s10], ..BlockMetaCompaction::default() }, version: META_VERSION_1, out_of_order: false, thanos: None };
    let m2 = BlockMeta { ulid: p2, min_time: 200, max_time: 500, stats: BlockStats::default(), compaction: BlockMetaCompaction { level: 1, ..BlockMetaCompaction::default() }, version: META_VERSION_1, out_of_order: false, thanos: None };
    let m3 = BlockMeta { ulid: p3, min_time: 500, max_time: 2500, stats: BlockStats::default(), compaction: BlockMetaCompaction { level: 3, sources: vec![s5, s6], ..BlockMetaCompaction::default() }, version: META_VERSION_1, out_of_order: false, thanos: None };
    let m4 = BlockMeta { ulid: p4, min_time: 100, max_time: 900, stats: BlockStats::default(), compaction: BlockMetaCompaction { level: 1, ..BlockMetaCompaction::default() }, version: META_VERSION_1, out_of_order: false, thanos: None };
    let out_uid = Ulid::from_parts(1000, 0);
    let got = compact_block_metas(out_uid, &[&m1, &m2, &m3, &m4]);
    assert_eq!(got.ulid, out_uid);
    assert_eq!((got.min_time, got.max_time), (100, 2500));
    assert_eq!(got.compaction.level, 4);
    assert_eq!(got.compaction.sources, vec![s1, s5, s6, s10]);
    assert_eq!(got.compaction.parents, vec![
        BlockDesc { ulid: p1, min_time: 1000, max_time: 2000 },
        BlockDesc { ulid: p2, min_time: 200, max_time: 500 },
        BlockDesc { ulid: p3, min_time: 500, max_time: 2500 },
        BlockDesc { ulid: p4, min_time: 100, max_time: 900 },
    ]);
}

// Upstream: github.com/prometheus/prometheus/tsdb/compact_test.go::TestCompactBlockMetasHints
#[test]
fn test_compact_block_metas_hints() {
    let p1 = Ulid::from_parts(100, 0);
    let p2 = Ulid::from_parts(200, 0);
    let out_uid = Ulid::from_parts(1000, 0);
    let mk = |u, stale, ooo| {
        let mut c = BlockMetaCompaction { level: 1, ..BlockMetaCompaction::default() };
        if stale { c.set_stale_series(); }
        if ooo { c.set_out_of_order(); }
        BlockMeta { ulid: u, min_time: 0, max_time: 1000, stats: BlockStats::default(), compaction: c, version: META_VERSION_1, out_of_order: false, thanos: None }
    };
    let stale_ooo = mk(p1, true, true);
    let stale = mk(p1, true, false);
    let ooo = mk(p2, false, true);
    let plain = mk(p2, false, false);

    let r1 = compact_block_metas(out_uid, &[&stale_ooo]);
    assert!(r1.compaction.from_stale_series() && r1.compaction.from_out_of_order());
    let r2 = compact_block_metas(out_uid, &[&stale_ooo, &ooo]);
    assert!(r2.compaction.from_stale_series() && r2.compaction.from_out_of_order());
    let r3 = compact_block_metas(out_uid, &[&stale_ooo, &plain]);
    assert!(r3.compaction.from_stale_series() && !r3.compaction.from_out_of_order());
    let r4 = compact_block_metas(out_uid, &[&stale, &mk(p2, true, false)]);
    assert!(r4.compaction.from_stale_series() && !r4.compaction.from_out_of_order());
    let r5 = compact_block_metas(out_uid, &[&mk(p1, false, true), &ooo]);
    assert!(!r5.compaction.from_stale_series() && r5.compaction.from_out_of_order());
    let r6 = compact_block_metas(out_uid, &[&mk(p1, false, true), &plain]);
    assert!(!r6.compaction.from_out_of_order());
    let r7 = compact_block_metas(out_uid, &[&stale, &ooo]);
    assert!(r7.compaction.from_stale_series() && !r7.compaction.from_out_of_order());
    let r8 = compact_block_metas(out_uid, &[&mk(p1, false, false), &plain]);
    assert!(!r8.compaction.from_stale_series() && !r8.compaction.from_out_of_order());
}

// Upstream: github.com/prometheus/prometheus/tsdb/compact_test.go::TestLeveledCompactor_plan_overlapping_disabled
#[test]
fn test_leveled_compactor_plan_overlapping_disabled() {
    plan_cases(false);
}

// Upstream: github.com/prometheus/prometheus/tsdb/compact_test.go::TestAsyncBlockWriterSuccess
#[test]
fn test_async_block_writer_success() {
    let cdir = tempfile::tempdir().unwrap();
    let idir = tempfile::tempdir().unwrap();
    let cw = BlockChunksWriter::open(cdir.path()).unwrap();
    let mut iw = IndexWriter::new(idir.path().join(INDEX_FILENAME)).unwrap();
    let n = 100u64;
    iw.add_symbol("__name__").unwrap();
    let mut names: Vec<String> = (0..n).map(|i| format!("s_{i:03}")).collect();
    names.sort();
    for s in &names {
        iw.add_symbol(s).unwrap();
    }
    let abw = AsyncBlockWriter::new(cw, iw);
    for s in &names {
        abw.add_series(
            Labels::from_pairs([("__name__", s.as_str())]),
            vec![ChunkMeta { ref_id: 0, min_time: 0, max_time: i64::MAX, chunk: Some(xor_chunk(&[(1, 1.0)])) }],
        ).unwrap();
    }
    abw.close_async();
    abw.close_async();
    let stats = abw.wait_finished().unwrap();
    assert_eq!(stats.num_series, n);
    assert_eq!(stats.num_chunks, n);
    for _ in 0..5 {
        let s2 = abw.wait_finished().unwrap();
        assert_eq!(s2, stats);
        abw.close_async();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/compact_test.go::TestAsyncBlockWriterFailure
#[test]
fn test_async_block_writer_failure() {
    let cdir = tempfile::tempdir().unwrap();
    let idir = tempfile::tempdir().unwrap();
    let cw = BlockChunksWriter::open(cdir.path()).unwrap();
    let iw = IndexWriter::new(idir.path().join(INDEX_FILENAME)).unwrap();
    let abw = AsyncBlockWriter::new(cw, iw);
    abw.add_series(
        Labels::from_pairs([("__name__", "test")]),
        vec![ChunkMeta { ref_id: 0, min_time: 0, max_time: i64::MAX, chunk: Some(xor_chunk(&[(1, 1.0)])) }],
    ).unwrap();
    abw.close_async();
    abw.close_async();
    let err = abw.wait_finished().unwrap_err().to_string();
    assert!(err.contains("__name__"), "{err}");
    for _ in 0..5 {
        let e2 = abw.wait_finished().unwrap_err().to_string();
        assert_eq!(e2, err);
        abw.close_async();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/compact_test.go::TestCompactEmptyResultBlockWithTombstone
#[test]
fn test_compact_empty_result_block_with_tombstone() {
    let dir = tempfile::tempdir().unwrap();
    let bdir = create_gen_block(dir.path(), 1, 1, 0, 10);
    let b = Block::open(&bdir).unwrap();
    b.delete(0, 10, &[LabelMatcher::new(MatchType::Equal, "m_0", "m_0").unwrap()]).unwrap();
    let m = compact_blocks(dir.path(), &[&b]).unwrap();
    // Check with empty matcher covering all series.
    let _ = m;
    let b2 = Block::open(&bdir).unwrap();
    b2.delete(0, 10, &[LabelMatcher::new(MatchType::Equal, "__name__", "m_0").unwrap()]).unwrap();
    assert!(compact_blocks(dir.path(), &[&b2]).unwrap().is_none());
}

// Upstream: github.com/prometheus/prometheus/tsdb/compact_test.go::TestDelayedCompaction
#[test]
fn test_delayed_compaction() {
    let delay = Duration::from_millis(80);
    let no_delay_bound = Duration::from_secs(5);
    // Delay not enabled.
    {
        let dir = tempfile::tempdir().unwrap();
        let opts = TsdbOptions { head: HeadOptions { chunk_range_ms: 10, ..HeadOptions::default() }, ..TsdbOptions::default() };
        let db = Tsdb::open_with_reload_interval(dir.path(), opts, Duration::ZERO).unwrap();
        db.set_block_ranges(vec![10]);
        let mut app = db.appender();
        for ts in [0, 11, 21] {
            app.append_float(None, &[("foo", "bar")], ts, 0.0).unwrap();
        }
        app.commit().unwrap();
        let t0 = Instant::now();
        db.compact().unwrap();
        assert!(!db.head().is_compactable());
        assert!(t0.elapsed() < no_delay_bound);
        db.close().unwrap();
    }
    // Delay enabled.
    {
        let dir = tempfile::tempdir().unwrap();
        let reg = Arc::new(prometheus::Registry::new());
        let opts = TsdbOptions {
            head: HeadOptions { registry: Some(Arc::clone(&reg)), chunk_range_ms: 10, ..HeadOptions::default() },
            compaction_delay: delay,
            ..TsdbOptions::default()
        };
        let db = Tsdb::open_with_reload_interval(dir.path(), opts, Duration::ZERO).unwrap();
        db.set_block_ranges(vec![10]);
        db.enable_compactions();
        {
            let mut app = db.appender();
            for ts in [0, 11, 21] {
                app.append_float(None, &[("foo", "bar")], ts, 0.0).unwrap();
            }
            app.commit().unwrap();
        }
        db.trigger_auto_compaction().unwrap();
        assert!(db.compaction_delay_started().is_some());
        assert_eq!(counter_val(&reg, "prometheus_tsdb_compactions_total"), 0.0);
        std::thread::sleep(delay + Duration::from_millis(20));
        db.trigger_auto_compaction().unwrap();
        assert!(counter_val(&reg, "prometheus_tsdb_compactions_total") >= 1.0);
        assert!(counter_val(&reg, "prometheus_tsdb_compactions_triggered_total") >= 2.0);

        // Manual compaction clears delay when head is no longer compactable.
        {
            let mut app = db.appender();
            app.append_float(None, &[("foo", "bar")], 51, 0.0).unwrap();
            app.commit().unwrap();
        }
        assert!(db.head().is_compactable());
        db.trigger_auto_compaction().unwrap();
        assert!(db.compaction_delay_started().is_some());
        db.compact_head_range(0, 50).unwrap();
        db.trigger_auto_compaction().unwrap();
        assert!(db.compaction_delay_started().is_none());
        db.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/compact_test.go::TestDelayedCompactionDoesNotBlockUnrelatedOps
#[test]
fn test_delayed_compaction_does_not_block_unrelated_ops() {
    for when_compactable in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        for (a, b) in [(0, 100), (100, 200), (200, 300)] {
            create_gen_block(dir.path(), 1, 1, a, b);
        }
        let opts = TsdbOptions {
            head: HeadOptions { chunk_range_ms: 10, ..HeadOptions::default() },
            compaction_delay: Duration::from_secs(3600),
            ..TsdbOptions::default()
        };
        let db = Tsdb::open_with_reload_interval(dir.path(), opts, Duration::ZERO).unwrap();
        db.set_block_ranges(vec![10, 200]);
        db.disable_compactions();
        assert_eq!(db.blocks().len(), 3);
        if when_compactable {
            let mut app = db.appender();
            app.append_float(None, &[("foo", "bar")], 301, 0.0).unwrap();
            app.append_float(None, &[("foo", "bar")], 317, 0.0).unwrap();
            app.commit().unwrap();
            assert!(db.head().is_compactable());
        }
        db.compact().unwrap();
        assert_eq!(db.blocks().len(), 2);
        db.close().unwrap();
    }
}

