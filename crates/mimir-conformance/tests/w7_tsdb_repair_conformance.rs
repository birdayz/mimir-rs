//! Tier-1 Go-vs-Rust differential conformance for the TSDB startup repair paths.
//!
//! Each scenario builds an identical on-disk TSDB (written by the Rust engine, then
//! corrupted the way a crash / OOMKill / bit-rot would) twice; Go `tsdb.Open`
//! (`target/go-oracle tsdb-open-report`, unmodified vendored Prometheus TSDB) opens one
//! copy and Rust `Tsdb::open` the other. Both must agree on: whether Open succeeds, the
//! `prometheus_tsdb_mmap_chunk_corruptions_total` / `prometheus_tsdb_wal_corruptions_total`
//! counters, the files left in `chunks_head`, `wal` and `wbl`, the number of intact WAL/WBL
//! records, and the queried data.
//!
//! Upstream behaviour ported: github.com/prometheus/prometheus/tsdb/head.go (`Head.Init`,
//! `loadMmappedChunks`, `removeCorruptedMmappedChunks`), tsdb/db.go (`DB.Open` repair),
//! tsdb/chunks/head_chunks.go (`DeleteCorrupted`, `IterateAllChunks`, `openMMapFiles`),
//! tsdb/wlog/wlog.go (`WL.Repair`).

use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;

use mimir_conformance::oracle::{invoke_oracle, LabelJson};
use mimir_tsdb::chunkenc::{EncodedChunk, XorChunk};
use mimir_tsdb::compact::SampleValue;
use mimir_tsdb::db::{Tsdb, TsdbOptions};
use mimir_tsdb::head_chunks::{HeadChunksWriter, HEAD_CHUNK_FILE_HEADER_SIZE};
use mimir_tsdb::labels::Labels;
use mimir_tsdb::matcher::{LabelMatcher, MatchType};
use mimir_tsdb::record::{RecordEncoder, RefSample, RefSeries};
use mimir_tsdb::wlog::{CompressionType, WlogReader, WlogWriter};
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
struct OpenReportRequest {
    dir: String,
    out_of_order_time_window_ms: i64,
    compact: bool,
}

#[derive(Deserialize, Debug, Clone, PartialEq)]
struct OpenReportSeries {
    labels: Vec<LabelJson>,
    samples: Vec<[f64; 2]>,
}

#[derive(Deserialize, Debug, Clone, PartialEq)]
struct OpenReport {
    open_error: String,
    metrics: BTreeMap<String, f64>,
    files: BTreeMap<String, Option<Vec<String>>>,
    records: BTreeMap<String, i64>,
    head_series: u64,
    min_ooo_time: i64,
    series: Option<Vec<OpenReportSeries>>,
}

const COMPARED_METRICS: &[&str] = &[
    "prometheus_tsdb_mmap_chunk_corruptions_total",
    "prometheus_tsdb_wal_corruptions_total",
];

fn skip() -> bool {
    std::env::var("MIMIR_CONFORMANCE_SKIP_GO_ORACLE").is_ok()
}

fn list_names(dir: &Path) -> Option<Vec<String>> {
    let rd = fs::read_dir(dir).ok()?;
    let mut v: Vec<String> = rd
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    v.sort();
    Some(v)
}

fn count_records(dir: &Path) -> i64 {
    if !dir.exists() {
        return -1;
    }
    let mut r = match WlogReader::open_dir(dir) {
        Ok(r) => r,
        Err(_) => return -1,
    };
    let mut n = 0;
    while let Ok(Some(_)) = r.next_record() {
        n += 1;
    }
    n
}

fn gather(reg: &prometheus::Registry) -> BTreeMap<String, f64> {
    let mut out = BTreeMap::new();
    for mf in reg.gather() {
        let v: f64 = mf
            .get_metric()
            .iter()
            .map(|m| match mf.get_field_type() {
                prometheus::proto::MetricType::COUNTER => m.get_counter().get_value(),
                prometheus::proto::MetricType::GAUGE => m.get_gauge().get_value(),
                prometheus::proto::MetricType::SUMMARY => m.get_summary().get_sample_count() as f64,
                prometheus::proto::MetricType::HISTOGRAM => {
                    m.get_histogram().get_sample_count() as f64
                }
                _ => 0.0,
            })
            .sum();
        out.insert(mf.get_name().to_string(), v);
    }
    out
}

fn rust_report(dir: &Path, ooo_window_ms: i64) -> OpenReport {
    let reg = Arc::new(prometheus::Registry::new());
    let mut opts = TsdbOptions::default();
    opts.head.registry = Some(Arc::clone(&reg));
    opts.head.ooo_time_window_ms = ooo_window_ms;
    let snapshot = |files: &mut BTreeMap<String, Option<Vec<String>>>,
                    records: &mut BTreeMap<String, i64>| {
        for sub in ["chunks_head", "wal", "wbl"] {
            files.insert(sub.to_string(), list_names(&dir.join(sub)));
            if sub != "chunks_head" {
                records.insert(sub.to_string(), count_records(&dir.join(sub)));
            }
        }
    };
    let mut files = BTreeMap::new();
    let mut records = BTreeMap::new();
    let db = match Tsdb::open(dir, opts) {
        Ok(db) => db,
        Err(e) => {
            snapshot(&mut files, &mut records);
            return OpenReport {
                open_error: e.to_string(),
                metrics: gather(&reg),
                files,
                records,
                head_series: 0,
                min_ooo_time: 0,
                series: None,
            };
        }
    };
    snapshot(&mut files, &mut records);
    let all = LabelMatcher::new(MatchType::RegexMatch, "__name__", ".*").unwrap();
    let res = db
        .querier(i64::MIN, i64::MAX)
        .select(true, std::slice::from_ref(&all))
        .unwrap();
    let series: Vec<OpenReportSeries> = res
        .into_iter()
        .map(|s| OpenReportSeries {
            labels: s
                .labels
                .iter()
                .map(|(n, v)| LabelJson {
                    name: n.to_string(),
                    value: v.to_string(),
                })
                .collect(),
            samples: s
                .samples
                .iter()
                .filter_map(|ds| match ds.value {
                    SampleValue::Float(v) => Some([ds.t as f64, v]),
                    _ => None,
                })
                .collect(),
        })
        .collect();
    let report = OpenReport {
        open_error: String::new(),
        metrics: gather(&reg),
        files,
        records,
        head_series: db.head().num_series(),
        min_ooo_time: db.head().min_ooo_time(),
        series: if series.is_empty() { None } else { Some(series) },
    };
    db.close().unwrap();
    report
}

fn go_report(dir: &Path, ooo_window_ms: i64) -> OpenReport {
    invoke_oracle(
        "tsdb-open-report",
        &OpenReportRequest {
            dir: dir.display().to_string(),
            out_of_order_time_window_ms: ooo_window_ms,
            compact: false,
        },
    )
    .expect("go-oracle tsdb-open-report")
}

/// Runs `build` into two fresh directories and diffs Go vs Rust `Open` behaviour.
fn differential(name: &str, ooo_window_ms: i64, build: impl Fn(&Path)) -> (OpenReport, OpenReport) {
    let root = tempfile::tempdir().unwrap();
    let go_dir = root.path().join("go");
    let rust_dir = root.path().join("rust");
    fs::create_dir_all(&go_dir).unwrap();
    fs::create_dir_all(&rust_dir).unwrap();
    build(&go_dir);
    build(&rust_dir);

    let pre_open = chunk_file_contents(&go_dir);
    assert_eq!(pre_open, chunk_file_contents(&rust_dir), "[{name}] fixture not deterministic");

    let mut go = go_report(&go_dir, ooo_window_ms);
    let mut rust = rust_report(&rust_dir, ooo_window_ms);

    // Go m-maps the head chunks re-created by WAL replay eagerly (`appendChunkAndMmap`), so
    // it may cut new `chunks_head` files (possibly re-using a deleted file's number) during
    // Open; Rust m-maps them later (tracked in PORT_STATUS.md). Compare which of the
    // pre-existing chunk files survived the repair byte-for-byte instead of raw listings.
    let go_survivors = surviving_chunk_files(&pre_open, &go_dir);
    let rust_survivors = surviving_chunk_files(&pre_open, &rust_dir);
    assert_eq!(go_survivors, rust_survivors, "[{name}] surviving chunks_head files differ");
    for r in [&mut go, &mut rust] {
        r.files.remove("chunks_head");
    }

    assert_eq!(
        go.open_error.is_empty(),
        rust.open_error.is_empty(),
        "[{name}] Open outcome differs: go={:?} rust={:?}",
        go.open_error,
        rust.open_error
    );
    if go.open_error.is_empty() {
        for m in COMPARED_METRICS {
            assert_eq!(
                go.metrics.get(*m),
                rust.metrics.get(*m),
                "[{name}] metric {m} differs"
            );
        }
    }
    assert_eq!(go.files, rust.files, "[{name}] files left after Open differ");
    assert_eq!(go.records, rust.records, "[{name}] intact WAL/WBL records differ");
    assert_eq!(go.head_series, rust.head_series, "[{name}] head series differ");
    assert_eq!(go.min_ooo_time, rust.min_ooo_time, "[{name}] head min OOO time differs");
    assert_eq!(go.series, rust.series, "[{name}] queried data differs");
    (go, rust)
}

// ---------------------------------------------------------------------------------------
// Fixture builders
// ---------------------------------------------------------------------------------------

/// A realistic head written by the Rust engine: 3 series x 600 samples (15s apart), the
/// completed head chunks m-mapped into several small `chunks_head` files.
fn build_rust_head(dir: &Path) {
    let mut opts = TsdbOptions::default();
    opts.head.max_head_chunk_file_size = 512;
    let db = Tsdb::open(dir, opts).unwrap();
    for i in 0..600i64 {
        let mut app = db.appender();
        for s in 0..3 {
            let name = format!("series_{s}");
            app.append_float(None, &[("__name__", name.as_str())], i * 15_000, (i * (s + 1)) as f64)
                .unwrap();
        }
        app.commit().unwrap();
    }
    db.head().mmap_head_chunks().unwrap();
    db.close().unwrap();
}

fn chunk_file_contents(dir: &Path) -> BTreeMap<String, Vec<u8>> {
    chunk_files(dir)
        .into_iter()
        .map(|f| {
            let b = fs::read(dir.join("chunks_head").join(&f)).unwrap();
            (f, b)
        })
        .collect()
}

fn surviving_chunk_files(pre_open: &BTreeMap<String, Vec<u8>>, dir: &Path) -> Vec<String> {
    let now = chunk_file_contents(dir);
    pre_open
        .iter()
        .filter(|(f, b)| now.get(*f) == Some(*b))
        .map(|(f, _)| f.clone())
        .collect()
}

fn chunk_files(dir: &Path) -> Vec<String> {
    list_names(&dir.join("chunks_head")).unwrap_or_default()
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

fn write_wal_records(dir: &Path, recs: &[Vec<u8>]) {
    let mut w = WlogWriter::open(dir.join("wal"), CompressionType::None).unwrap();
    for r in recs {
        w.log(&[r]).unwrap();
    }
    w.close().unwrap();
}

fn series_rec(ref_id: u64, name: &str) -> Vec<u8> {
    RecordEncoder::new(false).series(
        &[RefSeries {
            ref_id,
            labels: Labels::from_pairs([("__name__", name)]),
        }],
        Vec::new(),
    )
}

fn samples_rec(ref_id: u64, ts: &[i64]) -> Vec<u8> {
    let s: Vec<RefSample> = ts
        .iter()
        .map(|&t| RefSample {
            ref_id,
            st: 0,
            t,
            v: t as f64,
        })
        .collect();
    RecordEncoder::new(false).samples(&s, Vec::new())
}

// ---------------------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------------------

/// TSDB-1 audit repro: non-zero garbage appended to the last `chunks_head` file (torn
/// buffered write after an OOMKill).
#[test]
fn torn_tail_of_last_chunks_head_file() {
    if skip() {
        return;
    }
    let (go, _) = differential("torn_tail", 0, |dir| {
        build_rust_head(dir);
        let files = chunk_files(dir);
        assert!(files.len() > 2, "fixture must span several chunk files");
        let mut f = OpenOptions::new()
            .append(true)
            .open(dir.join("chunks_head").join(files.last().unwrap()))
            .unwrap();
        f.write_all(&[0x5a; 30]).unwrap();
    });
    assert!(go.open_error.is_empty());
    assert_eq!(go.metrics["prometheus_tsdb_mmap_chunk_corruptions_total"], 1.0);
}

/// Exactly the audit repro: a lone `000001` holding a header plus 30 random bytes.
#[test]
fn header_plus_random_bytes_only_file() {
    if skip() {
        return;
    }
    differential("header_plus_random", 0, |dir| {
        fs::create_dir_all(dir.join("wal")).unwrap();
        fs::create_dir_all(dir.join("chunks_head")).unwrap();
        let mut data = vec![0x01, 0x30, 0xBC, 0x91, 0x01, 0, 0, 0];
        data.extend((1..=30u8).map(|b| b.wrapping_mul(37) | 1));
        fs::write(dir.join("chunks_head").join("000001"), &data).unwrap();
    });
}

/// CRC mismatch in a middle file: that file and all later ones are deleted; the data is
/// recovered from the WAL.
#[test]
fn crc_mismatch_in_middle_chunks_head_file() {
    if skip() {
        return;
    }
    differential("crc_middle", 0, |dir| {
        build_rust_head(dir);
        let files = chunk_files(dir);
        let mid = &files[files.len() / 2];
        let f = OpenOptions::new()
            .write(true)
            .open(dir.join("chunks_head").join(mid))
            .unwrap();
        f.write_all_at(&[67, 88], (HEAD_CHUNK_FILE_HEADER_SIZE + 2) as u64)
            .unwrap();
    });
}

/// Out-of-sequence in-order chunk: not a `CorruptionErr`, so every chunk file is discarded.
#[test]
fn out_of_sequence_mmapped_chunk() {
    if skip() {
        return;
    }
    differential("out_of_sequence", 0, |dir| {
        write_wal_records(
            dir,
            &[series_rec(1, "oos"), samples_rec(1, &[4000, 5000, 6000])],
        );
        let mut w = HeadChunksWriter::with_max_file_size(dir.join("chunks_head"), 60).unwrap();
        w.write_chunk(1, 0, 999, &xor_chunk(&[0, 999]), false).unwrap();
        w.write_chunk(1, 2000, 2999, &xor_chunk(&[2000, 2999]), false).unwrap();
        w.write_chunk(1, 2500, 3500, &xor_chunk(&[2500, 3500]), false).unwrap();
        w.close().unwrap();
    });
}

/// Unknown chunk encoding with a valid CRC: skipped, no corruption.
#[test]
fn unsupported_chunk_encoding_is_skipped() {
    if skip() {
        return;
    }
    differential("unsupported_encoding", 0, |dir| {
        write_wal_records(dir, &[series_rec(1, "enc")]);
        {
            let mut w = HeadChunksWriter::open(dir.join("chunks_head")).unwrap();
            w.write_chunk(1, 0, 999, &xor_chunk(&[0, 999]), false).unwrap();
            w.write_chunk(1, 1000, 1999, &xor_chunk(&[1000, 1999]), false).unwrap();
            w.close().unwrap();
        }
        let p = dir.join("chunks_head").join("000001");
        let mut data = fs::read(&p).unwrap();
        let first_len = 25 + 1 + data[HEAD_CHUNK_FILE_HEADER_SIZE + 25] as usize + 4;
        let start = HEAD_CHUNK_FILE_HEADER_SIZE + first_len;
        data[start + 24] = 0x7f;
        let end = start + 26 + data[start + 25] as usize;
        let crc = crc32c::crc32c(&data[start..end]);
        data[end..end + 4].copy_from_slice(&crc.to_be_bytes());
        fs::write(&p, &data).unwrap();
    });
}

/// Invalid file magic is fatal in Go (`openMMapFiles` plain error) and must be in Rust.
#[test]
fn invalid_chunk_file_magic_fails_open() {
    if skip() {
        return;
    }
    let (go, rust) = differential("bad_magic", 0, |dir| {
        build_rust_head(dir);
        let files = chunk_files(dir);
        let f = OpenOptions::new()
            .write(true)
            .open(dir.join("chunks_head").join(&files[0]))
            .unwrap();
        f.write_all_at(&[0xde, 0xad, 0xbe, 0xef], 0).unwrap();
    });
    assert!(go.open_error.contains("invalid magic number deadbeef"), "{}", go.open_error);
    assert!(rust.open_error.contains("invalid magic number deadbeef"), "{}", rust.open_error);
}

/// A WAL record that cannot be decoded: repaired up to the record before it.
#[test]
fn wal_record_decode_error_is_repaired() {
    if skip() {
        return;
    }
    differential("wal_decode_error", 0, |dir| {
        let s = samples_rec(1, &[1000, 2000]);
        let recs = vec![
            series_rec(1, "a"),
            s.clone(),
            samples_rec(1, &[3000]),
            s[..3].to_vec(),
            samples_rec(1, &[4000]),
        ];
        write_wal_records(dir, &recs);
    });
}

/// A torn WAL tail (CRC mismatch inside the last page) on top of a realistic head.
#[test]
fn wal_torn_tail_is_repaired() {
    if skip() {
        return;
    }
    differential("wal_torn_tail", 0, |dir| {
        build_rust_head(dir);
        let wal = dir.join("wal");
        let segs = list_names(&wal).unwrap();
        let last = segs
            .iter()
            .rev()
            .find(|s| fs::metadata(wal.join(s)).unwrap().len() > 0)
            .unwrap();
        let p = wal.join(last);
        let len = fs::metadata(&p).unwrap().len();
        let f = OpenOptions::new().write(true).open(&p).unwrap();
        f.write_all_at(&[0xff, 0xff, 0xff], len / 2).unwrap();
    });
}

/// WAL corruption + a valid WBL: the WBL is not replayed in the boot that repairs the WAL.
#[test]
fn wal_corruption_skips_wbl_replay() {
    if skip() {
        return;
    }
    let (go, _) = differential("wal_corrupt_wbl", 3_600_000, |dir| {
        let s = samples_rec(1, &[10_000]);
        write_wal_records(dir, &[series_rec(1, "a"), s.clone(), s[..3].to_vec()]);
        let mut wbl = WlogWriter::open(dir.join("wbl"), CompressionType::None).unwrap();
        wbl.log(&[&samples_rec(1, &[5_000])]).unwrap();
        wbl.close().unwrap();
    });
    assert_eq!(go.min_ooo_time, i64::MAX);
}

/// A WBL record that cannot be decoded: the WBL is repaired; the WAL is untouched.
#[test]
fn wbl_record_decode_error_is_repaired() {
    if skip() {
        return;
    }
    differential("wbl_decode_error", 3_600_000, |dir| {
        write_wal_records(dir, &[series_rec(1, "a"), samples_rec(1, &[10_000])]);
        let mut wbl = WlogWriter::open(dir.join("wbl"), CompressionType::None).unwrap();
        let ooo = samples_rec(1, &[5_000]);
        wbl.log(&[&ooo]).unwrap();
        wbl.log(&[&samples_rec(1, &[6_000])]).unwrap();
        wbl.log(&[&ooo[..3]]).unwrap();
        wbl.log(&[&samples_rec(1, &[7_000])]).unwrap();
        wbl.close().unwrap();
    });
}

/// Go hands a corrupted checkpoint to `wal.Repair`, which repairs the *WAL* directory at
/// the checkpoint's segment index: it deletes every later WAL segment and then fails Open
/// (the segment to rewrite does not exist). Rust must do exactly the same.
#[test]
fn corrupted_checkpoint() {
    if skip() {
        return;
    }
    let (go, _) = differential("corrupt_checkpoint", 0, |dir| {
        let wal = dir.join("wal");
        let cp = wal.join("checkpoint.00000002");
        fs::create_dir_all(&cp).unwrap();
        {
            let mut w = WlogWriter::open(&cp, CompressionType::None).unwrap();
            let s = series_rec(1, "a");
            w.log(&[&s]).unwrap();
            w.log(&[&s[..3]]).unwrap();
            w.close().unwrap();
        }
        // WAL segments 00000003 and 00000004 after the checkpoint.
        for seg in 3..=4usize {
            let scratch = dir.join(format!("scratch-{seg}"));
            let mut w = WlogWriter::open(&scratch, CompressionType::None).unwrap();
            w.log(&[&samples_rec(1, &[seg as i64 * 1000])]).unwrap();
            w.close().unwrap();
            fs::rename(scratch.join("00000000"), wal.join(format!("{seg:08}"))).unwrap();
            fs::remove_dir_all(&scratch).unwrap();
        }
    });
    assert!(!go.open_error.is_empty(), "Go must fail Open on a corrupted checkpoint");
}

// ---------------------------------------------------------------------------------------
// TSDB-2: per-tenant prometheus_tsdb_* metrics
// ---------------------------------------------------------------------------------------

/// Every metric Mimir's `TSDBMetrics` aggregates from the per-tenant registry
/// (github.com/grafana/mimir/pkg/storage/tsdb/tsdb_metrics.go `Collect`).
const MIMIR_AGGREGATED_TSDB_METRICS: &[&str] = &[
    "prometheus_tsdb_compactions_total",
    "prometheus_tsdb_compaction_duration_seconds",
    "prometheus_tsdb_wal_fsync_duration_seconds",
    "prometheus_tsdb_wal_page_flushes_total",
    "prometheus_tsdb_wal_completed_pages_total",
    "prometheus_tsdb_wal_truncations_failed_total",
    "prometheus_tsdb_wal_truncations_total",
    "prometheus_tsdb_wal_truncate_duration_seconds",
    "prometheus_tsdb_wal_corruptions_total",
    "prometheus_tsdb_wal_writes_failed_total",
    "prometheus_tsdb_head_truncations_failed_total",
    "prometheus_tsdb_head_truncations_total",
    "prometheus_tsdb_head_gc_duration_seconds",
    "prometheus_tsdb_head_active_appenders",
    "prometheus_tsdb_head_series_not_found_total",
    "prometheus_tsdb_head_chunks",
    "prometheus_tsdb_head_chunks_created_total",
    "prometheus_tsdb_head_chunks_removed_total",
    "prometheus_tsdb_mmap_chunk_corruptions_total",
    "prometheus_tsdb_chunk_write_queue_operations_total",
    "prometheus_tsdb_mmap_chunks_total",
    "prometheus_tsdb_head_chunks_max_mmapped",
    "prometheus_tsdb_sample_ooo_delta",
    "prometheus_tsdb_blocks_loaded",
    "prometheus_tsdb_symbol_table_size_bytes",
    "prometheus_tsdb_reloads_total",
    "prometheus_tsdb_reloads_failures_total",
    "prometheus_tsdb_time_retentions_total",
    "prometheus_tsdb_storage_blocks_bytes",
    "prometheus_tsdb_checkpoint_deletions_failed_total",
    "prometheus_tsdb_checkpoint_deletions_total",
    "prometheus_tsdb_checkpoint_creations_failed_total",
    "prometheus_tsdb_checkpoint_creations_total",
    "prometheus_tsdb_exemplar_exemplars_appended_total",
    "prometheus_tsdb_exemplar_exemplars_in_storage",
    "prometheus_tsdb_exemplar_series_with_exemplars_in_storage",
    "prometheus_tsdb_exemplar_last_exemplars_timestamp_seconds",
    "prometheus_tsdb_exemplar_out_of_order_exemplars_total",
    "prometheus_tsdb_head_out_of_order_samples_appended_total",
    "prometheus_tsdb_head_series",
    "prometheus_tsdb_head_series_created_total",
    "prometheus_tsdb_head_series_removed_total",
    "prometheus_tsdb_wal_replay_unknown_refs_total",
    "prometheus_tsdb_wbl_replay_unknown_refs_total",
    "prometheus_tsdb_head_statistics_last_update_timestamp_seconds",
    "prometheus_tsdb_head_statistics_time_to_update_seconds",
];

/// Values that are fully determined by the on-disk state at Open and must match exactly.
const DETERMINISTIC_OPEN_METRICS: &[&str] = &[
    "prometheus_tsdb_blocks_loaded",
    "prometheus_tsdb_storage_blocks_bytes",
    "prometheus_tsdb_symbol_table_size_bytes",
    "prometheus_tsdb_reloads_total",
    "prometheus_tsdb_reloads_failures_total",
    "prometheus_tsdb_time_retentions_total",
    "prometheus_tsdb_wal_corruptions_total",
    "prometheus_tsdb_mmap_chunk_corruptions_total",
    "prometheus_tsdb_head_series",
    "prometheus_tsdb_head_series_created_total",
    "prometheus_tsdb_checkpoint_creations_total",
    "prometheus_tsdb_checkpoint_deletions_total",
    "prometheus_tsdb_wal_truncations_total",
    "prometheus_tsdb_compactions_total",
    "prometheus_tsdb_wal_replay_unknown_refs_total",
];

/// Two on-disk blocks + a WAL-backed head (with records referencing an unknown series)
/// written by the Rust engine; Go and Rust must register every per-tenant metric Mimir
/// aggregates and agree on the values determined by the on-disk state.
#[test]
fn per_tenant_tsdb_metrics_registered_and_equal_at_open() {
    if skip() {
        return;
    }
    let build = |dir: &Path| {
        let db = Tsdb::open(dir, TsdbOptions::default()).unwrap();
        for i in 0..400i64 {
            let mut app = db.appender();
            for s in 0..5 {
                let name = format!("metric_{s}");
                let job = format!("job_{}", s % 2);
                app.append_float(
                    None,
                    &[("__name__", name.as_str()), ("job", job.as_str())],
                    i * 60_000,
                    i as f64,
                )
                .unwrap();
            }
            app.commit().unwrap();
        }
        db.compact_head_range(0, 2 * 3_600_000).unwrap();
        db.compact_head_range(2 * 3_600_000, 4 * 3_600_000).unwrap();
        db.close().unwrap();
        drop(db);
        // Samples for a series ref that has no series record (unknown refs at replay).
        let mut w = WlogWriter::open(dir.join("wal"), CompressionType::None).unwrap();
        w.log(&[&samples_rec(999_999, &[5 * 3_600_000, 5 * 3_600_000 + 1])])
            .unwrap();
        w.close().unwrap();
    };
    let root = tempfile::tempdir().unwrap();
    let (go_dir, rust_dir) = (root.path().join("go"), root.path().join("rust"));
    fs::create_dir_all(&go_dir).unwrap();
    fs::create_dir_all(&rust_dir).unwrap();
    build(&go_dir);
    // Identical bytes on disk for both engines.
    copy_dir(&go_dir, &rust_dir);

    let go = go_report(&go_dir, 0);
    let rust = rust_report(&rust_dir, 0);
    assert!(go.open_error.is_empty() && rust.open_error.is_empty());
    assert_eq!(go.metrics["prometheus_tsdb_blocks_loaded"], 2.0);
    // The fixture exercises non-trivial values (the comparisons below are not vacuous).
    for m in [
        "prometheus_tsdb_storage_blocks_bytes",
        "prometheus_tsdb_symbol_table_size_bytes",
        "prometheus_tsdb_wal_replay_unknown_refs_total",
        "prometheus_tsdb_head_series",
    ] {
        assert!(go.metrics.get(m).copied().unwrap_or(0.0) > 0.0, "{m}: {:?}", go.metrics.get(m));
    }

    for m in MIMIR_AGGREGATED_TSDB_METRICS {
        if go.metrics.contains_key(*m) {
            assert!(
                rust.metrics.contains_key(*m),
                "metric {m} is registered by Go but not by Rust"
            );
        }
    }
    for m in DETERMINISTIC_OPEN_METRICS {
        assert_eq!(go.metrics.get(*m), rust.metrics.get(*m), "metric {m} differs");
    }
    assert_eq!(go.series, rust.series, "queried data differs");
}

fn copy_dir(src: &Path, dst: &Path) {
    fs::create_dir_all(dst).unwrap();
    for e in fs::read_dir(src).unwrap() {
        let e = e.unwrap();
        let to = dst.join(e.file_name());
        if e.file_type().unwrap().is_dir() {
            copy_dir(&e.path(), &to);
        } else {
            fs::copy(e.path(), to).unwrap();
        }
    }
}
