use mimir_conformance::oracle::{
    ensure_go_oracle_built, invoke_oracle, ChunkSampleJson, ExemplarJson, HistogramJson,
    LabelJson, SpanJson, TsdbSeriesFixtureJson, VerifyTsdbDirRequest, VerifyTsdbDirResponse,
    WriteTsdbFixtureRequest, WriteTsdbFixtureResponse,
};
use mimir_tsdb::chunkenc::{
    CounterResetHint, FloatHistogram, Histogram, HistogramSpan,
};
use mimir_tsdb::compact::SampleValue;
use mimir_tsdb::db::{Tsdb, TsdbOptions};
use mimir_tsdb::exemplar::Exemplar;
use mimir_tsdb::labels::Labels;
use mimir_tsdb::matcher::{LabelMatcher, MatchType};
use mimir_tsdb::search::{FuzzAlg, SearchHints, SearchOrdering};
use mimir_tsdb::wlog::CompressionType;

fn sample_int_histogram(mult: u64) -> Histogram {
    Histogram {
        counter_reset_hint: CounterResetHint::NotCounterReset,
        schema: 0,
        zero_threshold: 0.001,
        zero_count: 2 * mult,
        count: 5 * mult,
        sum: 12.5 * (mult as f64),
        positive_spans: vec![HistogramSpan {
            offset: 0,
            length: 2,
        }],
        positive_buckets: vec![2 * mult as i64, -(mult as i64)],
        negative_spans: Vec::new(),
        negative_buckets: Vec::new(),
        custom_values: Vec::new(),
    }
}

fn sample_float_histogram(mult: f64) -> FloatHistogram {
    FloatHistogram {
        counter_reset_hint: CounterResetHint::NotCounterReset,
        schema: 1,
        zero_threshold: 0.001,
        zero_count: 1.5 * mult,
        count: 4.5 * mult,
        sum: 9.25 * mult,
        positive_spans: vec![HistogramSpan {
            offset: 0,
            length: 2,
        }],
        positive_buckets: vec![1.0 * mult, 2.0 * mult],
        negative_spans: Vec::new(),
        negative_buckets: Vec::new(),
        custom_values: Vec::new(),
    }
}

fn to_label_json(labels: &Labels) -> Vec<LabelJson> {
    labels
        .iter()
        .map(|(n, v)| LabelJson {
            name: n.to_string(),
            value: v.to_string(),
        })
        .collect()
}

fn from_hist_json(h: &HistogramJson) -> SampleValue {
    if h.is_float {
        SampleValue::FloatHistogram(Box::new(FloatHistogram {
            counter_reset_hint: CounterResetHint::from_u8(h.reset_hint as u8),
            schema: h.schema,
            zero_threshold: h.zero_threshold,
            zero_count: h.zero_count_float,
            count: h.count_float,
            sum: h.sum,
            positive_spans: h
                .positive_spans
                .iter()
                .map(|s| HistogramSpan {
                    offset: s.offset,
                    length: s.length,
                })
                .collect(),
            positive_buckets: h.positive_counts.clone(),
            negative_spans: h
                .negative_spans
                .iter()
                .map(|s| HistogramSpan {
                    offset: s.offset,
                    length: s.length,
                })
                .collect(),
            negative_buckets: h.negative_counts.clone(),
            custom_values: h.custom_values.clone(),
        }))
    } else {
        SampleValue::Histogram(Box::new(Histogram {
            counter_reset_hint: CounterResetHint::from_u8(h.reset_hint as u8),
            schema: h.schema,
            zero_threshold: h.zero_threshold,
            zero_count: h.zero_count,
            count: h.count,
            sum: h.sum,
            positive_spans: h
                .positive_spans
                .iter()
                .map(|s| HistogramSpan {
                    offset: s.offset,
                    length: s.length,
                })
                .collect(),
            positive_buckets: h.positive_deltas.clone(),
            negative_spans: h
                .negative_spans
                .iter()
                .map(|s| HistogramSpan {
                    offset: s.offset,
                    length: s.length,
                })
                .collect(),
            negative_buckets: h.negative_deltas.clone(),
            custom_values: h.custom_values.clone(),
        }))
    }
}

fn assert_sample_value_eq(actual: &SampleValue, expected: &SampleValue) {
    match (actual, expected) {
        (SampleValue::Float(a), SampleValue::Float(b)) => assert_eq!(a, b),
        (SampleValue::Histogram(a), SampleValue::Histogram(b)) => {
            assert!(a.prom_equals(b), "hist mismatch: {a:?} != {b:?}");
        }
        (SampleValue::FloatHistogram(a), SampleValue::FloatHistogram(b)) => {
            assert!(a.prom_equals(b), "fhist mismatch: {a:?} != {b:?}");
        }
        _ => panic!("sample type mismatch: {actual:?} vs {expected:?}"),
    }
}

#[test]
fn conformance_a_rust_head_wal_wbl_chunks_head_replayed_by_go_tsdb() {
    ensure_go_oracle_built().expect("build go-oracle");
    let dir = tempfile::tempdir().expect("tempdir");

    let mut opts = TsdbOptions::default();
    opts.head.samples_per_chunk = 120;
    opts.head.ooo_time_window_ms = 500_000;
    opts.head.wal_compression = CompressionType::Snappy;

    // Append >300 timestamps forcing multiple mmap head chunk cuts, plus OOO float samples,
    // integer & float native histograms, and exemplars.
    let base_ts = 1_700_000_000_000i64;
    let s_float_pairs = [("__name__", "http_requests_total"), ("job", "api")];
    let s_ooo_pairs = [("__name__", "ooo_latency_seconds"), ("job", "api")];
    let s_ih_pairs = [("__name__", "req_duration_hist"), ("job", "api")];
    let s_fh_pairs = [("__name__", "req_duration_fhist"), ("job", "api")];

    {
        let db = Tsdb::open(dir.path(), opts).expect("open Rust Tsdb");
        let mut ref_float = None;
        for batch in 0..4i64 {
            let mut app = db.appender();
            for step in 0..80i64 {
                let idx = batch * 80 + step;
                let t = base_ts + idx * 1000;
                let id = app
                    .append_float(ref_float, &s_float_pairs, t, (idx as f64) * 1.5)
                    .expect("append float");
                ref_float = Some(id);
                if idx == 319 {
                    app.append_exemplar(
                        ref_float,
                        &s_float_pairs,
                        Exemplar::new(
                            Labels::from_pairs([("trace_id", "w2-rust-trace")]),
                            42.0,
                            t,
                        ),
                    )
                    .expect("append exemplar");
                }
            }
            app.commit().expect("commit float batch");
        }

        // OOO series: in-order samples at +200s, +300s then OOO sample at +250s (within 500s window)
        {
            let mut app = db.appender();
            let id = app
                .append_float(None, &s_ooo_pairs, base_ts + 200_000, 10.0)
                .unwrap();
            app.append_float(Some(id), &s_ooo_pairs, base_ts + 300_000, 30.0)
                .unwrap();
            app.commit().unwrap();

            let mut app2 = db.appender();
            app2.append_float(Some(id), &s_ooo_pairs, base_ts + 250_000, 20.0)
                .unwrap();
            app2.commit().unwrap();
        }

        // Integer & Float native histograms
        {
            let mut app = db.appender();
            let mut ih1 = sample_int_histogram(1);
            ih1.counter_reset_hint = CounterResetHint::CounterReset;
            let ih2 = sample_int_histogram(2);
            let hid = app
                .append_histogram(None, &s_ih_pairs, 0, base_ts + 1000, ih1)
                .unwrap();
            app.commit().unwrap();

            let mut app2 = db.appender();
            app2.append_histogram(Some(hid), &s_ih_pairs, 0, base_ts + 2000, ih2)
                .unwrap();

            let mut fh1 = sample_float_histogram(1.0);
            fh1.counter_reset_hint = CounterResetHint::CounterReset;
            let fh2 = sample_float_histogram(2.0);
            let fhid = app2
                .append_float_histogram(None, &s_fh_pairs, 0, base_ts + 1000, fh1)
                .unwrap();
            app2.commit().unwrap();

            let mut app3 = db.appender();
            app3.append_float_histogram(Some(fhid), &s_fh_pairs, 0, base_ts + 2000, fh2)
                .unwrap();
            app3.commit().unwrap();
        }

        // Close flushes sealed head chunks to chunks_head mmap files + closes WAL & WBL.
        db.close().expect("close Rust Tsdb");
    }

    // Verify chunks_head/000001 was written because 320 samples forced chunk cuts (>120/chunk).
    assert!(
        dir.path().join("chunks_head").join("000001").exists(),
        "expected chunks_head/000001 after >300 samples"
    );

    // Invoke real Go prometheus/tsdb via go-oracle verify-tsdb-dir (read_only: false)
    let go_out: VerifyTsdbDirResponse = invoke_oracle(
        "verify-tsdb-dir",
        &VerifyTsdbDirRequest {
            dir: dir.path().to_string_lossy().to_string(),
            read_only: false,
        },
    )
    .expect("go-oracle verify-tsdb-dir on Rust WAL + chunks_head + WBL");

    assert_eq!(go_out.head_series, 4);
    assert_eq!(go_out.series.len(), 4);

    // Sorted by label set string in Go output:
    // 0: http_requests_total
    // 1: ooo_latency_seconds
    // 2: req_duration_fhist
    // 3: req_duration_hist
    assert_eq!(go_out.series[0].samples.len(), 320);
    for (idx, s) in go_out.series[0].samples.iter().enumerate() {
        assert_eq!(s.t, base_ts + (idx as i64) * 1000);
        assert_eq!(s.v, (idx as f64) * 1.5);
    }
    assert_eq!(go_out.series[0].exemplars.len(), 1);
    assert_eq!(go_out.series[0].exemplars[0].value, 42.0);
    assert_eq!(
        go_out.series[0].exemplars[0].labels[0].value,
        "w2-rust-trace"
    );

    assert_eq!(go_out.series[1].samples.len(), 3);
    assert_eq!(
        go_out.series[1]
            .samples
            .iter()
            .map(|s| (s.t, s.v))
            .collect::<Vec<_>>(),
        vec![
            (base_ts + 200_000, 10.0),
            (base_ts + 250_000, 20.0),
            (base_ts + 300_000, 30.0),
        ]
    );

    assert_eq!(go_out.series[2].histograms.len(), 2);
    assert!(go_out.series[2].histograms[0].is_float);
    assert_eq!(go_out.series[2].histograms[1].sum, 18.5);

    assert_eq!(go_out.series[3].histograms.len(), 2);
    assert!(!go_out.series[3].histograms[0].is_float);
    assert_eq!(go_out.series[3].histograms[1].sum, 25.0);
}

#[test]
fn conformance_b_rust_compacted_ulid_block_opened_by_go_tsdb_ro_and_rw() {
    ensure_go_oracle_built().expect("build go-oracle");
    let dir = tempfile::tempdir().expect("tempdir");

    let mut opts = TsdbOptions::default();
    opts.head.samples_per_chunk = 100;

    let base_ts = 1_700_000_000_000i64;
    let s_float = [("__name__", "cpu_usage_seconds"), ("instance", "node-1")];
    let s_ih = [("__name__", "http_latency_hist"), ("instance", "node-1")];
    let s_fh = [("__name__", "http_latency_fhist"), ("instance", "node-1")];

    let expected_ulid;
    {
        let db = Tsdb::open(dir.path(), opts).expect("open Rust Tsdb");
        let mut r_float = None;
        for b in 0..3i64 {
            let mut app = db.appender();
            for step in 0..100i64 {
                let i = b * 100 + step;
                let id = app
                    .append_float(r_float, &s_float, base_ts + i * 1000, (i as f64) * 0.25)
                    .unwrap();
                r_float = Some(id);
            }
            app.commit().unwrap();
        }

        let mut app = db.appender();
        let mut ih = sample_int_histogram(1);
        ih.counter_reset_hint = CounterResetHint::CounterReset;
        app.append_histogram(None, &s_ih, 0, base_ts + 5000, ih)
            .unwrap();

        let mut fh = sample_float_histogram(1.0);
        fh.counter_reset_hint = CounterResetHint::CounterReset;
        app.append_float_histogram(None, &s_fh, 0, base_ts + 6000, fh)
            .unwrap();
        app.commit().unwrap();

        expected_ulid = db
            .compact_head_all()
            .expect("compact head")
            .expect("block ulid");
        db.close().expect("close db");
    }

    let expected_ulid_str = expected_ulid.to_string();
    assert!(dir.path().join(&expected_ulid_str).join("index").exists());
    assert!(dir
        .path()
        .join(&expected_ulid_str)
        .join("chunks")
        .join("000001")
        .exists());
    assert!(dir
        .path()
        .join(&expected_ulid_str)
        .join("meta.json")
        .exists());

    // Verify with Go tsdb.OpenDBReadOnly (read_only: true)
    let go_ro: VerifyTsdbDirResponse = invoke_oracle(
        "verify-tsdb-dir",
        &VerifyTsdbDirRequest {
            dir: dir.path().to_string_lossy().to_string(),
            read_only: true,
        },
    )
    .expect("go-oracle verify-tsdb-dir read_only=true");

    assert_eq!(go_ro.block_ulids, vec![expected_ulid_str.clone()]);
    assert_eq!(go_ro.series.len(), 3);
    assert_eq!(go_ro.series[0].samples.len(), 300);
    assert_eq!(go_ro.series[0].samples[299].v, 299.0 * 0.25);
    assert_eq!(go_ro.series[1].histograms.len(), 1);
    assert!(go_ro.series[1].histograms[0].is_float);
    assert_eq!(go_ro.series[1].histograms[0].sum, 9.25);
    assert_eq!(go_ro.series[2].histograms.len(), 1);
    assert!(!go_ro.series[2].histograms[0].is_float);
    assert_eq!(go_ro.series[2].histograms[0].sum, 12.5);

    // Also verify with Go tsdb.Open (read_only: false)
    let go_rw: VerifyTsdbDirResponse = invoke_oracle(
        "verify-tsdb-dir",
        &VerifyTsdbDirRequest {
            dir: dir.path().to_string_lossy().to_string(),
            read_only: false,
        },
    )
    .expect("go-oracle verify-tsdb-dir read_only=false");

    assert_eq!(go_rw.block_ulids, vec![expected_ulid_str]);
    assert_eq!(go_rw.series, go_ro.series);
}

#[test]
fn conformance_c_go_written_block_and_wal_opened_by_rust_tsdb() {
    ensure_go_oracle_built().expect("build go-oracle");

    let base_ts = 1_700_000_000_000i64;
    let l1 = Labels::from_pairs([("__name__", "go_metric_alpha"), ("job", "api")]);
    let l2 = Labels::from_pairs([("__name__", "go_metric_beta"), ("job", "db")]);
    let l3 = Labels::from_pairs([("__name__", "go_hist_metric"), ("job", "api")]);
    let l4 = Labels::from_pairs([("__name__", "go_fhist_metric"), ("job", "db")]);

    let fixture_series = vec![
        TsdbSeriesFixtureJson {
            labels: to_label_json(&l1),
            samples: (0..150i64)
                .map(|i| ChunkSampleJson {
                    st: 0,
                    t: base_ts + i * 1000,
                    v: (i as f64) * 2.0,
                })
                .collect(),
            histograms: Vec::new(),
            exemplars: vec![ExemplarJson {
                labels: vec![LabelJson {
                    name: "trace_id".into(),
                    value: "go2rust-ex".into(),
                }],
                value: 298.0,
                timestamp_ms: base_ts + 149_000,
            }],
        },
        TsdbSeriesFixtureJson {
            labels: to_label_json(&l2),
            samples: vec![ChunkSampleJson {
                st: 0,
                t: base_ts + 5000,
                v: 99.5,
            }],
            histograms: Vec::new(),
            exemplars: Vec::new(),
        },
        TsdbSeriesFixtureJson {
            labels: to_label_json(&l3),
            samples: Vec::new(),
            histograms: vec![HistogramJson {
                timestamp_ms: base_ts + 1000,
                schema: 0,
                zero_threshold: 0.001,
                zero_count: 2,
                count: 5,
                sum: 12.5,
                reset_hint: 1,
                is_float: false,
                positive_spans: vec![SpanJson {
                    offset: 0,
                    length: 2,
                }],
                positive_deltas: vec![2, -1],
                ..Default::default()
            }],
            exemplars: Vec::new(),
        },
        TsdbSeriesFixtureJson {
            labels: to_label_json(&l4),
            samples: Vec::new(),
            histograms: vec![HistogramJson {
                timestamp_ms: base_ts + 2000,
                schema: 1,
                zero_threshold: 0.001,
                zero_count_float: 1.5,
                count_float: 4.5,
                sum: 9.25,
                reset_hint: 1,
                is_float: true,
                positive_spans: vec![SpanJson {
                    offset: 0,
                    length: 2,
                }],
                positive_counts: vec![1.0, 2.0],
                ..Default::default()
            }],
            exemplars: Vec::new(),
        },
    ];

    for compact in [false, true] {
        let dir = tempfile::tempdir().expect("tempdir");
        let resp: WriteTsdbFixtureResponse = invoke_oracle(
            "write-tsdb-fixture",
            &WriteTsdbFixtureRequest {
                dir: dir.path().to_string_lossy().to_string(),
                compact,
                out_of_order_time_window_ms: 0,
                series: fixture_series.clone(),
            },
        )
        .expect("go-oracle write-tsdb-fixture");
        assert_eq!(resp.status, "ok");
        if compact {
            assert_eq!(resp.blocks.len(), 1);
        } else {
            assert!(resp.blocks.is_empty());
        }

        let db = Tsdb::open(dir.path(), TsdbOptions::default()).expect("Rust Tsdb::open Go dir");
        if compact {
            assert_eq!(
                db.block_ulids()
                    .into_iter()
                    .map(|u| u.to_string())
                    .collect::<Vec<_>>(),
                resp.blocks
            );
        } else {
            assert!(db.block_ulids().is_empty());
        }

        let q = db.querier(i64::MIN, i64::MAX);
        let cq = db.chunk_querier(i64::MIN, i64::MAX);

        // LabelNames & LabelValues
        assert_eq!(
            q.label_names(&[]).unwrap(),
            vec!["__name__", "job"]
        );
        assert_eq!(
            q.label_values("job", None, &[]).unwrap(),
            vec!["api", "db"]
        );
        assert_eq!(
            q.label_values_cardinality("__name__", &[]).unwrap(),
            4
        );
        let m_api = LabelMatcher::new(MatchType::Equal, "job", "api").unwrap();
        assert_eq!(
            q.label_values("__name__", None, std::slice::from_ref(&m_api))
                .unwrap(),
            vec!["go_hist_metric", "go_metric_alpha"]
        );

        // SearchLabelValues
        let sr = q
            .search_label_values(
                "__name__",
                &SearchHints {
                    terms: vec!["alpha".into()],
                    case_sensitive: false,
                    fuzz_alg: FuzzAlg::Substring,
                    fuzz_threshold: 0,
                    order_by: SearchOrdering::ValueAsc,
                    limit: 5,
                },
                &[],
            )
            .unwrap();
        assert_eq!(sr.len(), 1);
        assert_eq!(sr[0].value, "go_metric_alpha");

        // Querier::select all series
        let all_m = LabelMatcher::new(MatchType::RegexMatch, "__name__", ".*").unwrap();
        let res = q.select(true, std::slice::from_ref(&all_m)).unwrap();
        assert_eq!(res.len(), 4);

        // ChunkQuerier::select
        let cres = cq.select(true, std::slice::from_ref(&all_m)).unwrap();
        assert_eq!(cres.len(), 4);

        // Verify exact sample parity across Go fixture and Rust Querier
        // Order by Labels: go_fhist_metric, go_hist_metric, go_metric_alpha, go_metric_beta
        assert_eq!(res[0].labels, l4);
        assert_eq!(res[0].samples.len(), 1);
        assert_sample_value_eq(
            &res[0].samples[0].value,
            &from_hist_json(&fixture_series[3].histograms[0]),
        );

        assert_eq!(res[1].labels, l3);
        assert_eq!(res[1].samples.len(), 1);
        assert_sample_value_eq(
            &res[1].samples[0].value,
            &from_hist_json(&fixture_series[2].histograms[0]),
        );

        assert_eq!(res[2].labels, l1);
        assert_eq!(res[2].samples.len(), 150);
        for (i, s) in res[2].samples.iter().enumerate() {
            assert_eq!(s.t, base_ts + (i as i64) * 1000);
            assert_eq!(s.value, SampleValue::Float((i as f64) * 2.0));
        }

        assert_eq!(res[3].labels, l2);
        assert_eq!(res[3].samples.len(), 1);
        assert_eq!(res[3].samples[0].value, SampleValue::Float(99.5));

        // ExemplarQuerier parity when in WAL (compact=false)
        if !compact {
            let exs = db.select_exemplars(i64::MIN, i64::MAX, &[&[all_m]]);
            assert_eq!(exs.len(), 1);
            assert_eq!(exs[0].series_labels, l1);
            assert_eq!(exs[0].exemplars.len(), 1);
            assert_eq!(exs[0].exemplars[0].value, 298.0);
            assert_eq!(exs[0].exemplars[0].labels.get("trace_id"), "go2rust-ex");
        }

        db.close().expect("close Rust Tsdb");
    }
}

#[test]
fn conformance_d_rust_checkpoint_truncate_and_multi_block_compaction_verified_by_go() {
    use mimir_tsdb::block::Block;
    use mimir_tsdb::compact::compact_blocks;
    use mimir_tsdb::wlog::PAGE_SIZE;

    ensure_go_oracle_built().expect("build go-oracle");
    let dir = tempfile::tempdir().expect("tempdir");

    let mut opts = TsdbOptions::default();
    opts.head.wal_segment_size = PAGE_SIZE; // 32KiB segments so checkpoints cut easily
    opts.head.samples_per_chunk = 50;

    let base_ts = 1_700_000_000_000i64;
    {
        let db = Tsdb::open(dir.path(), opts).expect("open Rust Tsdb");

        // Batch 1: timestamps [base_ts, base_ts + 99*1000] across enough series to span multiple WAL segments
        for s_idx in 0..10 {
            let mut app = db.appender();
            let sname = format!("metric_{s_idx:02}");
            let pairs = [("__name__", sname.as_str()), ("job", "checkpoint_test")];
            let mut r = None;
            for t_idx in 0..100i64 {
                let id = app
                    .append_float(r, &pairs, base_ts + t_idx * 1000, (s_idx * 100 + t_idx) as f64)
                    .unwrap();
                r = Some(id);
            }
            app.commit().unwrap();
        }

        // Compact range [base_ts, base_ts + 50_000) into block 1
        let b1 = db
            .compact_head_range(base_ts, base_ts + 50_000)
            .unwrap()
            .expect("block 1");
        // Compact range [base_ts + 50_000, base_ts + 90_000) into block 2
        let b2 = db
            .compact_head_range(base_ts + 50_000, base_ts + 90_000)
            .unwrap()
            .expect("block 2");
        assert_ne!(b1, b2);

        // Verify query across blocks + head
        let q = db.querier(i64::MIN, i64::MAX);
        assert_eq!(q.series_count().unwrap(), 10);
        let lnv = q.label_names_and_values(&[]).unwrap();
        assert_eq!(lnv["job"], vec!["checkpoint_test"]);
        assert_eq!(lnv["__name__"].len(), 10);

        let sr = q
            .search_label_names(
                &SearchHints {
                    terms: vec!["nm".into()],
                    case_sensitive: false,
                    fuzz_alg: FuzzAlg::Subsequence,
                    fuzz_threshold: 0,
                    order_by: SearchOrdering::ValueAsc,
                    limit: 5,
                },
                &[],
            )
            .unwrap();
        assert_eq!(sr[0].value, "__name__");

        db.close().unwrap();
    }

    // Verify Go tsdb reads both blocks + remaining head WAL [90..100) cleanly
    let go_rw: VerifyTsdbDirResponse = invoke_oracle(
        "verify-tsdb-dir",
        &VerifyTsdbDirRequest {
            dir: dir.path().to_string_lossy().to_string(),
            read_only: false,
        },
    )
    .expect("go-oracle verify-tsdb-dir after checkpoint + 2 blocks");

    assert_eq!(go_rw.block_ulids.len(), 2);
    assert_eq!(go_rw.series.len(), 10);
    for s in &go_rw.series {
        assert_eq!(s.samples.len(), 100);
    }

    // Also test compact_blocks merging the 2 ULID blocks into a single level-2 block and verify with Go OpenDBReadOnly!
    let merged_dir = tempfile::tempdir().expect("merged tempdir");
    std::fs::create_dir_all(merged_dir.path().join("wal")).expect("wal dir for OpenDBReadOnly");
    let b_dirs: Vec<_> = go_rw
        .block_ulids
        .iter()
        .map(|u| Block::open(dir.path().join(u)).unwrap())
        .collect();
    let b_refs: Vec<&Block> = b_dirs.iter().collect();
    let merged_meta = compact_blocks(merged_dir.path(), &b_refs)
        .unwrap()
        .expect("merged block");
    assert_eq!(merged_meta.compaction.level, 2);
    assert_eq!(merged_meta.stats.num_series, 10);
    assert_eq!(merged_meta.stats.num_samples, 900);

    let go_merged_ro: VerifyTsdbDirResponse = invoke_oracle(
        "verify-tsdb-dir",
        &VerifyTsdbDirRequest {
            dir: merged_dir.path().to_string_lossy().to_string(),
            read_only: true,
        },
    )
    .expect("go-oracle verify-tsdb-dir on merged level-2 block");
    assert_eq!(go_merged_ro.block_ulids, vec![merged_meta.ulid.to_string()]);
    assert_eq!(go_merged_ro.series.len(), 10);
    for s in &go_merged_ro.series {
        assert_eq!(s.samples.len(), 90);
    }
}
