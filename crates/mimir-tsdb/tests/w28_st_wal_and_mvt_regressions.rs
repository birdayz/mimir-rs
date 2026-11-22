//! Wave 28 Track 1 regression tests for C-1 and C-2:
//! - C-1: WAL & WBL replay preserves V2 start timestamps (st) verbatim even when enable_st_storage == false
//! - C-2: Mid-WAL MinValidTime (type 14) records are ignored during WAL replay (single-threaded & parallel)

use mimir_tsdb::chunkenc::{Encoding, FloatHistogram, Histogram, HistogramSpan};
use mimir_tsdb::compact::{decode_chunk_samples, SampleValue};
use mimir_tsdb::head::{Head, HeadOptions};
use mimir_tsdb::labels::Labels;
use mimir_tsdb::record::{
    RecordEncoder, RefFloatHistogramSample, RefHistogramSample, RefSample, RefSeries,
};
use mimir_tsdb::wlog::{CompressionType, WlogWriter, WBL_DIR_NAME};

fn make_int_hist(sum: f64, count: u64) -> Histogram {
    Histogram {
        schema: 0,
        zero_threshold: 0.001,
        zero_count: 1,
        count,
        sum,
        positive_spans: vec![HistogramSpan {
            offset: 0,
            length: 1,
        }],
        positive_buckets: vec![count as i64 - 1],
        ..Default::default()
    }
}

fn make_float_hist(sum: f64, count: f64) -> FloatHistogram {
    FloatHistogram {
        schema: 0,
        zero_threshold: 0.001,
        zero_count: 1.0,
        count,
        sum,
        positive_spans: vec![HistogramSpan {
            offset: 0,
            length: 1,
        }],
        positive_buckets: vec![count - 1.0],
        ..Default::default()
    }
}

#[test]
fn c1_wal_replay_preserves_st_when_enable_st_storage_false() {
    for concurrency in [1usize, 4usize] {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let f_lbls = Labels::from_pairs([("__name__", "st_float")]);
        let h_lbls = Labels::from_pairs([("__name__", "st_hist")]);
        let fh_lbls = Labels::from_pairs([("__name__", "st_fhist")]);

        // Write V2 WAL records carrying non-zero ST values across two segments so
        // both sequential (concurrency=1) and parallel (concurrency=4) paths are exercised.
        {
            let mut w = WlogWriter::open(&wal_dir, CompressionType::None).unwrap();
            let enc = RecordEncoder::new(true);
            let s_rec = enc.series(
                &[
                    RefSeries {
                        ref_id: 1,
                        labels: f_lbls.clone(),
                    },
                    RefSeries {
                        ref_id: 2,
                        labels: h_lbls.clone(),
                    },
                    RefSeries {
                        ref_id: 3,
                        labels: fh_lbls.clone(),
                    },
                ],
                Vec::new(),
            );
            let f_rec = enc.samples_v2(
                &[RefSample {
                    ref_id: 1,
                    st: 12_345,
                    t: 20_000,
                    v: 42.5,
                }],
                Vec::new(),
            );
            let h_rec = enc.histogram_samples_v2(
                &[RefHistogramSample {
                    ref_id: 2,
                    st: 23_456,
                    t: 30_000,
                    h: make_int_hist(10.0, 3),
                }],
                Vec::new(),
            );
            let fh_rec = enc.float_histogram_samples_v2(
                &[RefFloatHistogramSample {
                    ref_id: 3,
                    st: 34_567,
                    t: 40_000,
                    fh: make_float_hist(12.5, 4.0),
                }],
                Vec::new(),
            );
            w.log(&[&s_rec, &f_rec]).unwrap();
            w.next_segment().unwrap();
            w.log(&[&h_rec, &fh_rec]).unwrap();
            w.close().unwrap();
        }

        // Reopen with enable_st_storage == false, but ST-capable chunk encodings enabled.
        let opts = HeadOptions {
            enable_st_storage: false,
            float_chunk_encoding: Encoding::Xor2,
            enable_histogram_st_encoding: true,
            wal_replay_concurrency: concurrency,
            ..HeadOptions::default()
        };
        let head = Head::open(dir.path(), opts, i64::MIN).unwrap();

        let f_ref = head
            .get_entry_by_sorted_pairs(&[("__name__", "st_float")])
            .unwrap()
            .ref_id;
        let (_, f_chks, _) = head
            .series_chunks(f_ref, i64::MIN, i64::MAX)
            .unwrap()
            .unwrap();
        let f_dec = decode_chunk_samples(f_chks[0].chunk.as_ref().unwrap()).unwrap();
        assert_eq!(f_dec.len(), 1);
        assert_eq!(f_dec[0].st, 12_345);
        assert_eq!(f_dec[0].t, 20_000);
        assert_eq!(f_dec[0].value, SampleValue::Float(42.5));

        let h_ref = head
            .get_entry_by_sorted_pairs(&[("__name__", "st_hist")])
            .unwrap()
            .ref_id;
        let (_, h_chks, _) = head
            .series_chunks(h_ref, i64::MIN, i64::MAX)
            .unwrap()
            .unwrap();
        let h_dec = decode_chunk_samples(h_chks[0].chunk.as_ref().unwrap()).unwrap();
        assert_eq!(h_dec.len(), 1);
        assert_eq!(h_dec[0].st, 23_456);
        assert_eq!(h_dec[0].t, 30_000);

        let fh_ref = head
            .get_entry_by_sorted_pairs(&[("__name__", "st_fhist")])
            .unwrap()
            .ref_id;
        let (_, fh_chks, _) = head
            .series_chunks(fh_ref, i64::MIN, i64::MAX)
            .unwrap()
            .unwrap();
        let fh_dec = decode_chunk_samples(fh_chks[0].chunk.as_ref().unwrap()).unwrap();
        assert_eq!(fh_dec.len(), 1);
        assert_eq!(fh_dec[0].st, 34_567);
        assert_eq!(fh_dec[0].t, 40_000);

        head.close().unwrap();
    }
}

#[test]
fn c1_wbl_replay_preserves_st_when_enable_st_storage_false() {
    let dir = tempfile::tempdir().unwrap();
    let wal_dir = dir.path().join("wal");
    let wbl_dir = dir.path().join(WBL_DIR_NAME);

    let f_lbls = Labels::from_pairs([("__name__", "ooo_st_float")]);
    let h_lbls = Labels::from_pairs([("__name__", "ooo_st_hist")]);
    let fh_lbls = Labels::from_pairs([("__name__", "ooo_st_fhist")]);

    {
        let mut wal = WlogWriter::open(&wal_dir, CompressionType::None).unwrap();
        let enc = RecordEncoder::new(true);
        let s_rec = enc.series(
            &[
                RefSeries {
                    ref_id: 1,
                    labels: f_lbls,
                },
                RefSeries {
                    ref_id: 2,
                    labels: h_lbls,
                },
                RefSeries {
                    ref_id: 3,
                    labels: fh_lbls,
                },
            ],
            Vec::new(),
        );
        // Anchor in-order maxt at 100_000 so WBL samples at 50_000 are OOO.
        let anchor = enc.samples_v2(
            &[RefSample {
                ref_id: 1,
                st: 99_000,
                t: 100_000,
                v: 1.0,
            }],
            Vec::new(),
        );
        wal.log(&[&s_rec, &anchor]).unwrap();
        wal.close().unwrap();

        let mut wbl = WlogWriter::open(&wbl_dir, CompressionType::None).unwrap();
        let wf = enc.samples_v2(
            &[RefSample {
                ref_id: 1,
                st: 41_111,
                t: 50_000,
                v: 7.5,
            }],
            Vec::new(),
        );
        let wh = enc.histogram_samples_v2(
            &[RefHistogramSample {
                ref_id: 2,
                st: 42_222,
                t: 51_000,
                h: make_int_hist(8.0, 2),
            }],
            Vec::new(),
        );
        let wfh = enc.float_histogram_samples_v2(
            &[RefFloatHistogramSample {
                ref_id: 3,
                st: 43_333,
                t: 52_000,
                fh: make_float_hist(9.5, 3.0),
            }],
            Vec::new(),
        );
        wbl.log(&[&wf, &wh, &wfh]).unwrap();
        wbl.close().unwrap();
    }

    let opts = HeadOptions {
        enable_st_storage: false,
        float_chunk_encoding: Encoding::Xor2,
        enable_histogram_st_encoding: true,
        ooo_time_window_ms: 200_000,
        ..HeadOptions::default()
    };
    let head = Head::open(dir.path(), opts, i64::MIN).unwrap();

    for (metric, expected_st, expected_t) in [
        ("ooo_st_float", 41_111i64, 50_000i64),
        ("ooo_st_hist", 42_222i64, 51_000i64),
        ("ooo_st_fhist", 43_333i64, 52_000i64),
    ] {
        let ref_id = head
            .get_entry_by_sorted_pairs(&[("__name__", metric)])
            .unwrap()
            .ref_id;
        let mut chks = Vec::new();
        let mut tombs = Vec::new();
        head.load_series_ooo_chunks_into(ref_id, &mut chks, &mut tombs)
            .unwrap();
        assert_eq!(chks.len(), 1);
        let dec = decode_chunk_samples(chks[0].chunk.as_ref().unwrap()).unwrap();
        assert_eq!(dec.len(), 1);
        assert_eq!(dec[0].st, expected_st);
        assert_eq!(dec[0].t, expected_t);
    }

    head.close().unwrap();
}

#[test]
fn c2_mid_wal_min_valid_time_record_is_ignored_during_replay() {
    for concurrency in [1usize, 4usize] {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let lbls = Labels::from_pairs([("__name__", "mvt_test")]);

        {
            let mut w = WlogWriter::open(&wal_dir, CompressionType::None).unwrap();
            let enc = RecordEncoder::new(false);
            let s_rec = enc.series(
                &[RefSeries {
                    ref_id: 1,
                    labels: lbls,
                }],
                Vec::new(),
            );
            let samp1 = enc.samples(
                &[RefSample {
                    ref_id: 1,
                    st: 0,
                    t: 1_000,
                    v: 1.0,
                }],
                Vec::new(),
            );
            // Mid-WAL MinValidTime(5_000) record: Go ignores record.MinValidTime in loadWAL
            // (it is only read from the latest checkpoint's first record before replay).
            let mvt_rec = enc.min_valid_time(5_000, Vec::new());
            let samp2 = enc.samples(
                &[RefSample {
                    ref_id: 1,
                    st: 0,
                    t: 2_000,
                    v: 2.0,
                }],
                Vec::new(),
            );
            w.log(&[&s_rec, &samp1, &mvt_rec]).unwrap();
            w.next_segment().unwrap();
            w.log(&[&samp2]).unwrap();
            w.close().unwrap();
        }

        let opts = HeadOptions {
            wal_replay_concurrency: concurrency,
            ..HeadOptions::default()
        };
        let head = Head::open(dir.path(), opts, 0).unwrap();
        assert_eq!(head.min_valid_time(), 0);

        let ref_id = head
            .get_entry_by_sorted_pairs(&[("__name__", "mvt_test")])
            .unwrap()
            .ref_id;
        let (_, chks, _) = head
            .series_chunks(ref_id, i64::MIN, i64::MAX)
            .unwrap()
            .unwrap();
        let mut samples = Vec::new();
        for cm in chks {
            for s in decode_chunk_samples(&cm.chunk.unwrap()).unwrap() {
                if let SampleValue::Float(v) = s.value {
                    samples.push((s.t, v));
                }
            }
        }
        assert_eq!(samples, vec![(1_000, 1.0), (2_000, 2.0)]);
        head.close().unwrap();
    }
}
