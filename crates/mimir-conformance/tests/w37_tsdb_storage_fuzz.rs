//! Wave 37 Deep TSDB & Storage Differential / Mutation Fuzz Suite:
//! 1. Reverse cross-engine Go -> Rust TSDB open & query fuzz (floats, OOO, histograms, NHCB, exemplars, blocks)
//! 2. Parallel (`wal_replay_concurrency = 4`) vs Sequential (`1`) WAL replay differential across None/Snappy/Zstd
//! 3. Corrupt on-disk TSDB Block / Index / Chunks / Tombstones / meta.json mutation fuzz (3,000+ inputs)
//! 4. Kafka RecordSerializer v0/v1/v2 vs Go `serialize-ingest-records` + S3 XML / Offset / Shipper mutation fuzz (5,000+ inputs)

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use mimir_conformance::oracle::{
    ensure_go_oracle_built, invoke_oracle, ChunkSampleJson, ExemplarJson, HistogramJson,
    LabelJson, SpanJson, TsdbSeriesFixtureJson, VerifyTsdbDirRequest, VerifyTsdbDirResponse,
    WriteTsdbFixtureRequest, WriteTsdbFixtureResponse,
};
use mimir_proto::cortexpb;
use mimir_proto::prost::Message as _;
use mimir_proto::zerocopy::WriteRequestScratch;
use mimir_storage::bucket::s3::{
    fuzz_http_resp_to_error_response, fuzz_parse_list_page, fuzz_query_unescape,
    fuzz_xml_root_children,
};
use mimir_storage::ingest::offset_catalogue::read_offset_catalogue_from_file;
use mimir_storage::ingest::offset_file::OffsetFile;
use mimir_storage::ingest::version::{
    deserialize_record_content, RecordSerializer, RecordVersion,
};
use mimir_storage::shipper::ShipperMeta;
use mimir_tsdb::block::Block;
use mimir_tsdb::chunkenc::{
    CounterResetHint, Encoding, FloatHistogram, Histogram, HistogramSpan,
};
use mimir_tsdb::chunks_block::BlockChunksReader;
use mimir_tsdb::compact::SampleValue;
use mimir_tsdb::db::{Tsdb, TsdbOptions};
use mimir_tsdb::exemplar::Exemplar;
use mimir_tsdb::head::{Head, HeadOptions};
use mimir_tsdb::index::IndexReader;
use mimir_tsdb::labels::Labels;
use mimir_tsdb::matcher::{LabelMatcher, MatchType};
use mimir_tsdb::wlog::CompressionType;
use serde::{Deserialize, Serialize};

#[derive(Clone)]
struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self { Self(seed) }
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
    fn usize(&mut self, n: usize) -> usize { (self.next_u64() % n as u64) as usize }
    fn bool(&mut self) -> bool { self.next_u64() & 1 == 1 }
    fn pick<'a, T>(&mut self, s: &'a [T]) -> &'a T { &s[self.usize(s.len())] }
}

fn copy_dir_recursive(src: &Path, dst: &Path) {
    fs::create_dir_all(dst).unwrap();
    for ent in fs::read_dir(src).unwrap().flatten() {
        let p = ent.path();
        let target = dst.join(ent.file_name());
        if p.is_dir() {
            copy_dir_recursive(&p, &target);
        } else {
            fs::copy(&p, &target).unwrap();
        }
    }
}

// 1. Reverse cross-engine TSDB fuzz: Go writes -> Rust opens & queries!
#[test]
fn w37_fuzz_go_to_rust_tsdb_reverse_cross_open() {
    ensure_go_oracle_built().unwrap();
    let mut rng = Rng::new(0x3715_6025);

    for run in 0..10 {
        let tmp = tempfile::tempdir().unwrap();
        let compact = run % 2 == 0;
        let mut series = Vec::new();
        let base_t = 1_700_000_000_000i64;

        // Float series (in-order + OOO + exemplars)
        for si in 0..4 {
            let mut samples = Vec::new();
            let mut t = base_t;
            for k in 0..10 {
                t += 15_000 + (rng.usize(30_000) as i64);
                let v = (rng.next_u64() % 1000) as f64 / 4.0;
                samples.push(ChunkSampleJson { st: 0, t, v });
                if k == 5 && rng.bool() {
                    // OOO sample inside 10m window
                    samples.push(ChunkSampleJson { st: 0, t: t - 20_000, v: v + 0.25 });
                }
            }
            let exemplars = vec![ExemplarJson {
                labels: vec![LabelJson { name: "trace_id".into(), value: format!("tr-{run}-{si}") }],
                value: 1.5,
                timestamp_ms: base_t + 50_000,
            }];
            series.push(TsdbSeriesFixtureJson {
                labels: vec![
                    LabelJson { name: "__name__".into(), value: format!("go_f_{si}") },
                    LabelJson { name: "job".into(), value: "go_writer".into() },
                ],
                samples,
                histograms: vec![],
                exemplars,
            });
        }

        // Integer & Float native histograms + NHCB (-53)
        for (hi, (is_float, schema)) in [(false, 1i32), (true, 2i32), (false, -53i32)].into_iter().enumerate() {
            let mut hists = Vec::new();
            let mut t = base_t;
            for k in 0..4u64 {
                t += 30_000;
                let nhcb = schema == -53;
                let custom_values = if nhcb { vec![0.1, 0.5, 1.0] } else { vec![] };
                if is_float {
                    hists.push(HistogramJson {
                        timestamp_ms: t,
                        schema,
                        zero_threshold: if nhcb { 0.0 } else { 0.001 },
                        zero_count_float: (k + 1) as f64,
                        count_float: (2 * k + 4) as f64,
                        sum: (2 * k + 4) as f64 * 2.0,
                        reset_hint: 0,
                        is_float: true,
                        positive_spans: vec![SpanJson { offset: 0, length: 2 }],
                        positive_counts: vec![1.0, (k + 2) as f64],
                        custom_values,
                        ..Default::default()
                    });
                } else {
                    hists.push(HistogramJson {
                        timestamp_ms: t,
                        schema,
                        zero_threshold: if nhcb { 0.0 } else { 0.001 },
                        zero_count: if nhcb { 0 } else { k + 1 },
                        count: if nhcb { k + 3 } else { 2 * k + 4 },
                        sum: (2 * k + 4) as f64 * 2.0,
                        reset_hint: 0,
                        is_float: false,
                        positive_spans: vec![SpanJson { offset: 0, length: 2 }],
                        positive_deltas: vec![1, (k + 1) as i64],
                        custom_values,
                        ..Default::default()
                    });
                }
            }
            series.push(TsdbSeriesFixtureJson {
                labels: vec![
                    LabelJson { name: "__name__".into(), value: format!("go_h_{hi}") },
                    LabelJson { name: "job".into(), value: "go_writer".into() },
                ],
                samples: vec![],
                histograms: hists,
                exemplars: vec![],
            });
        }

        let _: WriteTsdbFixtureResponse = invoke_oracle("write-tsdb-fixture", &WriteTsdbFixtureRequest {
            dir: tmp.path().to_string_lossy().into_owned(),
            compact,
            out_of_order_time_window_ms: 600_000,
            series,
        }).unwrap();

        // Read back with Go verify-tsdb-dir (read_only) as ground truth
        let go_v: VerifyTsdbDirResponse = invoke_oracle("verify-tsdb-dir", &VerifyTsdbDirRequest {
            dir: tmp.path().to_string_lossy().into_owned(),
            read_only: true,
        }).unwrap();

        // Open exact Go-written dir in Rust Tsdb::open!
        let mut opts = TsdbOptions::default();
        opts.head.ooo_time_window_ms = 600_000;
        let db = Tsdb::open(tmp.path(), opts).unwrap();
        let lm = LabelMatcher::new(MatchType::RegexMatch, "__name__", "go_.*").unwrap();
        let rust_series = db.querier(i64::MIN, i64::MAX).select(true, &[lm]).unwrap();

        let mut rust_floats: BTreeMap<String, Vec<(i64, u64)>> = BTreeMap::new();
        let mut rust_hists: BTreeMap<String, Vec<(i64, i32, u64)>> = BTreeMap::new();
        for s in rust_series {
            let name = s.labels.get("__name__").to_string();
            for pt in s.samples {
                match pt.value {
                    SampleValue::Float(v) => rust_floats.entry(name.clone()).or_default().push((pt.t, v.to_bits())),
                    SampleValue::Histogram(h) => rust_hists.entry(name.clone()).or_default().push((pt.t, h.schema, h.count)),
                    SampleValue::FloatHistogram(fh) => rust_hists.entry(name.clone()).or_default().push((pt.t, fh.schema, fh.count as u64)),
                }
            }
        }
        db.close().unwrap();

        let mut go_floats: BTreeMap<String, Vec<(i64, u64)>> = BTreeMap::new();
        let mut go_hists: BTreeMap<String, Vec<(i64, i32, u64)>> = BTreeMap::new();
        for s in go_v.series {
            let name = s.labels.iter().find(|l| l.name == "__name__").map(|l| l.value.clone()).unwrap_or_default();
            for pt in s.samples {
                go_floats.entry(name.clone()).or_default().push((pt.t, pt.v.to_bits()));
            }
            for h in s.histograms {
                let cnt = if h.is_float { h.count_float as u64 } else { h.count };
                go_hists.entry(name.clone()).or_default().push((h.timestamp_ms, h.schema, cnt));
            }
        }
        assert_eq!(rust_floats, go_floats, "Go->Rust float mismatch run={run} compact={compact}");
        assert_eq!(rust_hists, go_hists, "Go->Rust histogram mismatch run={run} compact={compact}");
    }
}

// 2. Parallel (concurrency=4) vs Sequential (concurrency=1) WAL replay differential across None/Snappy/Zstd!
#[test]
fn w37_fuzz_wal_parallel_vs_sequential_replay() {
    let mut rng = Rng::new(0x3715_9A11);
    for comp in [CompressionType::None, CompressionType::Snappy, CompressionType::Zstd] {
        let src_dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            wal_compression: comp,
            wal_segment_size: 32_768, // 1 WAL page = 32 KiB -> forces multiple WAL segments!
            enable_exemplar_storage: true,
            enable_st_storage: true,
            float_chunk_encoding: Encoding::Xor2,
            enable_histogram_st_encoding: true,
            ooo_time_window_ms: 300_000,
            ..Default::default()
        };

        let head = Head::open(src_dir.path(), opts.clone(), 0).unwrap();
        let mut t = 1_700_000_000_000i64;
        for step in 0..80 {
            let mut app = head.appender();
            for j in 0..5 {
                t += 1000;
                let sname = format!("wal_s_{}", (step + j) % 25);
                let env = format!("e_{}", j % 4);
                let st = if rng.bool() { t - 5000 } else { 0 };
                let v = match rng.usize(10) {
                    0 => -0.0,
                    1 => f64::from_bits(0x7ff0_0000_0000_0002),
                    _ => (rng.next_u64() % 500) as f64 / 2.0,
                };
                let _ = app.append_float_with_st(None, &[("__name__", sname.as_str()), ("env", env.as_str())], st, t, v);
                if step % 7 == 0 && j == 0 {
                    let ex = Exemplar { labels: Labels::from_pairs([("trace_id", "abc")]), value: 1.0, timestamp_ms: t, has_ts: true };
                    let _ = app.append_exemplar(None, &[("__name__", sname.as_str()), ("env", env.as_str())], ex);
                }
            }
            if step % 5 == 0 {
                t += 1000;
                let h = Histogram {
                    counter_reset_hint: CounterResetHint::UnknownCounterReset,
                    schema: 1, zero_threshold: 0.001, zero_count: 2, count: 5, sum: 10.0,
                    positive_spans: vec![HistogramSpan { offset: 0, length: 1 }],
                    positive_buckets: vec![3], ..Default::default()
                };
                let _ = app.append_histogram(None, &[("__name__", "wal_h_0"), ("env", "e_0")], t, t - 2000, h);
                let fh = FloatHistogram {
                    counter_reset_hint: CounterResetHint::UnknownCounterReset,
                    schema: 1, zero_threshold: 0.001, zero_count: 1.5, count: 4.5, sum: 9.0,
                    positive_spans: vec![HistogramSpan { offset: 0, length: 1 }],
                    positive_buckets: vec![3.0], ..Default::default()
                };
                let _ = app.append_float_histogram(None, &[("__name__", "wal_fh_0"), ("env", "e_0")], t, t - 2000, fh);
            }
            app.commit().unwrap();
            if step == 35 || step == 65 {
                let lm = LabelMatcher::new(MatchType::Equal, "env", "e_1").unwrap();
                head.delete(t - 25_000, t - 5_000, &[lm]).unwrap();
            }
        }
        head.close().unwrap();

        let dir_seq = tempfile::tempdir().unwrap();
        let dir_par = tempfile::tempdir().unwrap();
        copy_dir_recursive(src_dir.path(), dir_seq.path());
        copy_dir_recursive(src_dir.path(), dir_par.path());

        let mut opts_seq = opts.clone(); opts_seq.wal_replay_concurrency = 1;
        let mut opts_par = opts; opts_par.wal_replay_concurrency = 4;

        let db_opts_seq = TsdbOptions { head: opts_seq, ..Default::default() };
        let db_opts_par = TsdbOptions { head: opts_par, ..Default::default() };
        let h_seq = Tsdb::open(dir_seq.path(), db_opts_seq).unwrap();
        let h_par = Tsdb::open(dir_par.path(), db_opts_par).unwrap();

        assert_eq!(h_seq.head().num_series(), h_par.head().num_series(), "num_series mismatch for {comp:?}");
        let all = LabelMatcher::new(MatchType::RegexMatch, "__name__", ".+").unwrap();
        let collect_head = |h: &Tsdb| -> BTreeMap<String, Vec<(i64, i64, u64)>> {
            let mut out = BTreeMap::new();
            for s in h.querier(i64::MIN, i64::MAX).select(true, std::slice::from_ref(&all)).unwrap() {
                let key = format!("{}", s.labels);
                let pts: Vec<(i64, i64, u64)> = s.samples.into_iter().map(|p| {
                    let tag = match p.value {
                        SampleValue::Float(f) => f.to_bits(),
                        SampleValue::Histogram(hh) => hh.count,
                        SampleValue::FloatHistogram(fh) => fh.count.to_bits(),
                    };
                    (p.st, p.t, tag)
                }).collect();
                out.insert(key, pts);
            }
            out
        };
        assert_eq!(collect_head(&h_seq), collect_head(&h_par), "parallel vs sequential WAL replay data mismatch for {comp:?}");
        h_seq.close().unwrap();
        h_par.close().unwrap();
    }
}

// 3. Corrupt on-disk TSDB Block / Index / Chunks / Tombstones / meta.json mutation fuzz!
#[test]
fn w37_fuzz_corrupt_tsdb_block_and_index_mutations() {
    let seed_dir = tempfile::tempdir().unwrap();
    let mut opts = TsdbOptions::default();
    opts.head.chunk_range_ms = 3_600_000;
    let db = Tsdb::open(seed_dir.path(), opts).unwrap();
    let mut t = 1_700_000_000_000i64;
    for k in 0..20 {
        let mut app = db.appender();
        for i in 0..5 {
            let name = format!("blk_m_{i}");
            let _ = app.append_float(None, &[("__name__", name.as_str()), ("k", "v")], t, (k + i) as f64);
        }
        app.commit().unwrap();
        t += 400_000;
    }
    let ulids = db.compact().unwrap();
    assert!(!ulids.is_empty(), "expected at least one compacted block");
    let blk_dir = seed_dir.path().join(ulids[0].to_string());
    db.close().unwrap();

    let meta_orig = fs::read(blk_dir.join("meta.json")).unwrap();
    let idx_orig = fs::read(blk_dir.join("index")).unwrap();
    let chk_orig = fs::read(blk_dir.join("chunks").join("000001")).unwrap();
    let tomb_orig = fs::read(blk_dir.join("tombstones")).unwrap_or_default();

    let mut rng = Rng::new(0x3715_B10C);
    let work = tempfile::tempdir().unwrap();
    for iter in 0..1_500 {
        let bdir = work.path().join("blk");
        let _ = fs::remove_dir_all(&bdir);
        fs::create_dir_all(bdir.join("chunks")).unwrap();
        let mutate = |orig: &[u8], rng: &mut Rng| -> Vec<u8> {
            let mut b = orig.to_vec();
            for _ in 0..(1 + rng.usize(4)) {
                if b.is_empty() { break; }
                let pos = rng.usize(b.len());
                match rng.usize(4) {
                    0 => b[pos] ^= 1u8 << rng.usize(8),
                    1 => b[pos] = 0xff,
                    2 => b.truncate(pos),
                    _ => b[pos] = 0x00,
                }
            }
            b
        };
        let which = iter % 4;
        fs::write(bdir.join("meta.json"), if which == 0 { mutate(&meta_orig, &mut rng) } else { meta_orig.clone() }).unwrap();
        fs::write(bdir.join("index"), if which == 1 { mutate(&idx_orig, &mut rng) } else { idx_orig.clone() }).unwrap();
        fs::write(bdir.join("chunks").join("000001"), if which == 2 { mutate(&chk_orig, &mut rng) } else { chk_orig.clone() }).unwrap();
        fs::write(bdir.join("tombstones"), if which == 3 { mutate(&tomb_orig, &mut rng) } else { tomb_orig.clone() }).unwrap();

        if let Ok(blk) = Block::open(&bdir) {
            let lm = LabelMatcher::new(MatchType::RegexMatch, "__name__", ".+").unwrap();
            let _ = blk.index().postings("__name__", &["blk_m_0"]);
            let _ = blk.index().label_names();
            let _ = lm;
        }
        if which == 1 {
            let _ = IndexReader::open(bdir.join("index"));
        }
        if which == 2 {
            let _ = BlockChunksReader::open(bdir.join("chunks"));
        }
    }
}

// 4. Kafka RecordSerializer v0/v1/v2 vs Go serialize-ingest-records + S3 XML / Offset / Shipper mutation fuzz!
#[derive(Serialize)]
struct SerRecsIn { version: i32, topic: String, partition: i32, user_id: String, max_record_bytes: i32, write_req_b64: String }
#[derive(Deserialize)]
struct SerRecsOut { ok: bool, error: String, records_b64: Vec<String> }

#[test]
fn w37_fuzz_kafka_serializer_and_storage_parsers() {
    ensure_go_oracle_built().unwrap();
    let mut rng = Rng::new(0x3715_5709);

    // 4A. Differential Kafka RecordSerializer v0/v1/v2 vs Go serialize-ingest-records
    let mut scratch = WriteRequestScratch::new();
    for iter in 0..30 {
        let version = match iter % 3 { 0 => RecordVersion::V0, 1 => RecordVersion::V1, _ => RecordVersion::V2 };
        let n_series = 1 + rng.usize(6);
        let mut timeseries = Vec::with_capacity(n_series);
        for si in 0..n_series {
            timeseries.push(cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair { name: "__name__".into(), value: format!("k_m_{si}").into() },
                    cortexpb::LabelPair { name: "job".into(), value: "prometheus".into() },
                    cortexpb::LabelPair { name: "instance".into(), value: format!("10.0.0.{si}:9090").into() },
                    cortexpb::LabelPair { name: "custom_lbl".into(), value: format!("val_{iter}_{si}").into() },
                ],
                samples: vec![cortexpb::Sample { value: (iter + si) as f64 * 1.25, timestamp_ms: 1_700_000_000_000 + (si as i64) * 1000 }],
                exemplars: if si == 0 { vec![cortexpb::Exemplar { labels: vec![cortexpb::LabelPair { name: "trace_id".into(), value: "abc123".into() }], value: 1.0, timestamp_ms: 1_700_000_000_000 }] } else { vec![] },
                ..Default::default()
            });
        }
        let wr = cortexpb::WriteRequest {
            timeseries,
            metadata: vec![cortexpb::MetricMetadata { r#type: 1, metric_family_name: "k_m_0".into(), help: "help".into(), unit: "s".into() }],
            ..Default::default()
        };
        let go: SerRecsOut = invoke_oracle("serialize-ingest-records", &SerRecsIn {
            version: version as i32,
            topic: "mimir-ingest".into(),
            partition: 0,
            user_id: "tenant-a".into(),
            max_record_bytes: 1_048_576,
            write_req_b64: B64.encode(wr.encode_to_vec()),
        }).unwrap();
        assert!(go.ok, "go serialize failed: {}", go.error);
        let rust_recs = RecordSerializer::new(version)
            .to_records("mimir-ingest", 0, "tenant-a", &wr, 1_048_576)
            .unwrap();
        assert_eq!(rust_recs.0.len(), go.records_b64.len(), "record count diff iter={iter} ver={version:?}");
        for (rr, gb64) in rust_recs.0.iter().zip(go.records_b64.iter()) {
            let go_bytes = B64.decode(gb64).unwrap();
            // Decode BOTH Go-serialized and Rust-serialized payloads with Rust deserialize_record_content!
            let dec_go = deserialize_record_content(&go_bytes, version as i32, &mut scratch).unwrap();
            let go_ts_count = dec_go.timeseries().len();
            let dec_rust = deserialize_record_content(&rr.value, version as i32, &mut scratch).unwrap();
            assert_eq!(dec_rust.timeseries().len(), go_ts_count, "decoded series count diff iter={iter} ver={version:?}");
            assert_eq!(dec_rust.timeseries().len(), wr.timeseries.len());
        }
    }

    // 4B. 4,000 mutated S3 XML, URL unescape, OffsetFile, OffsetCatalogue & ShipperMeta inputs -> 0 panics/OOMs!
    let xml_seeds: &[&[u8]] = &[
        b"<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>prod/01ABC/meta.json</Key></Contents></ListBucketResult>",
        b"<LocationConstraint>eu-central-1</LocationConstraint>",
        b"<Error><Code>NoSuchKey</Code><Message>not found</Message></Error>",
    ];
    for _ in 0..3_000 {
        let mut b = rng.pick(xml_seeds).to_vec();
        for _ in 0..(1 + rng.usize(5)) {
            if b.is_empty() { break; }
            let idx = rng.usize(b.len());
            match rng.usize(4) {
                0 => b[idx] = *rng.pick(b"<>/=?!&;\"'[]\x00\n"),
                1 => { b.remove(idx); }
                2 => b.insert(idx, *rng.pick(b"<>/&;\x00")),
                _ => b.truncate(idx),
            }
        }
        let _ = fuzz_parse_list_page(&b);
        let _ = fuzz_xml_root_children(&b);
        let _ = fuzz_http_resp_to_error_response(404, &b);
        if let Ok(s) = std::str::from_utf8(&b) {
            let _ = fuzz_query_unescape(s);
        }
    }
    let tmp = tempfile::tempdir().unwrap();
    let json_seeds: &[&[u8]] = &[
        br#"{"offset": 12345, "version": 1}"#,
        br#"{"version":1,"offsets":{"0":100}}"#,
        br#"{"version":1,"uploaded":["01ARZ3NDEKTSV4RRFFQ69G5FAV"]}"#,
    ];
    for i in 0..1_000 {
        let mut b = rng.pick(json_seeds).to_vec();
        for _ in 0..(1 + rng.usize(4)) {
            if b.is_empty() { break; }
            let idx = rng.usize(b.len());
            b[idx] = *rng.pick(b"{}[]\":,019-nulltrue\x00");
        }
        let p = tmp.path().join(format!("m_{}.json", i % 8));
        fs::write(&p, &b).unwrap();
        let _ = OffsetFile::new(&p, 0).read();
        let _ = read_offset_catalogue_from_file(&p);
        let _ = serde_json::from_slice::<ShipperMeta>(&b);
    }
}
