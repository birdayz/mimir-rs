//! Ported from github.com/prometheus/prometheus/tsdb/chunkenc/
//! (float_histogram_st_test.go, float_histogram_test.go, st_helper_test.go).

use mimir_tsdb::chunkenc::float_histogram::AppendFloatHistogramOutcome;
use mimir_tsdb::chunkenc::float_histogram_st::AppendFloatHistogramStOutcome;
use mimir_tsdb::chunkenc::st::{HISTOGRAM_ST_SAMPLE_COUNT_MASK, MAX_FIRST_ST_CHANGE_ON};
use mimir_tsdb::chunkenc::{
    is_stale_nan, stale_nan, ChunkError, CounterResetHeader, CounterResetHint, FloatHistogram,
    FloatHistogramChunk, FloatHistogramStChunk, Histogram, HistogramSpan, CUSTOM_BUCKETS_SCHEMA,
    EXPONENTIAL_SCHEMA_MAX,
};

fn generate_test_float_histogram(i: i64) -> FloatHistogram {
    FloatHistogram {
        count: 12.0 + (i * 9) as f64,
        zero_count: 2.0 + i as f64,
        zero_threshold: 0.001,
        sum: 18.4 * (i + 1) as f64,
        schema: 1,
        positive_spans: vec![
            HistogramSpan { offset: 0, length: 2 },
            HistogramSpan { offset: 1, length: 2 },
        ],
        positive_buckets: vec![
            (i + 1) as f64,
            (i + 2) as f64,
            (i + 1) as f64,
            (i + 1) as f64,
        ],
        negative_spans: vec![
            HistogramSpan { offset: 0, length: 2 },
            HistogramSpan { offset: 1, length: 2 },
        ],
        negative_buckets: vec![
            (i + 1) as f64,
            (i + 2) as f64,
            (i + 1) as f64,
            (i + 1) as f64,
        ],
        ..Default::default()
    }
}

fn generate_test_float_histograms(n: usize) -> Vec<FloatHistogram> {
    (0..n)
        .map(|i| {
            let mut fh = generate_test_float_histogram(i as i64);
            if i > 0 {
                fh.counter_reset_hint = CounterResetHint::NotCounterReset;
            }
            fh
        })
        .collect()
}

fn generate_test_custom_buckets_float_histogram(i: i64) -> FloatHistogram {
    FloatHistogram {
        count: 5.0 + (i * 4) as f64,
        sum: 18.4 * (i + 1) as f64,
        schema: CUSTOM_BUCKETS_SCHEMA,
        positive_spans: vec![
            HistogramSpan { offset: 0, length: 2 },
            HistogramSpan { offset: 1, length: 2 },
        ],
        positive_buckets: vec![
            (i + 1) as f64,
            (i + 2) as f64,
            (i + 1) as f64,
            (i + 1) as f64,
        ],
        custom_values: vec![0.0, 1.0, 2.0, 3.0, 4.0],
        ..Default::default()
    }
}

fn assert_first_float_histogram_sample_hint(
    chunk: &FloatHistogramChunk,
    expected: CounterResetHint,
) {
    let mut it = chunk.iter();
    assert!(it.next_sample().unwrap().is_some());
    let (_, v) = it.at_float_histogram();
    assert_eq!(v.counter_reset_hint, expected);
}

fn assert_fhist_sample_count(chunk: &FloatHistogramChunk, exp: usize) {
    let mut count = 0usize;
    let mut it = chunk.iter();
    while it.next_sample().unwrap().is_some() {
        count += 1;
    }
    assert_eq!(count, exp);
}

// ============================================================================
// float_histogram_st_test.go
// ============================================================================

fn require_float_histogram_st_samples(samples: &[(i64, i64, FloatHistogram)]) {
    let mut c = FloatHistogramStChunk::new();
    {
        let mut app = c.appender().unwrap();
        for (st, t, fh) in samples {
            match app.append_float_histogram(None, *st, *t, fh, false).unwrap() {
                AppendFloatHistogramStOutcome::Appended { .. } => {}
                AppendFloatHistogramStOutcome::NewChunk(_) => panic!("unexpected new chunk"),
            }
        }
    }
    assert_eq!(c.num_samples(), samples.len());
    let mut it = c.iter();
    for (i, (st, t, _)) in samples.iter().enumerate() {
        let got_t = it.next_sample().unwrap().expect("expected sample");
        assert_eq!(got_t, *t, "sample {i}: timestamp");
        assert_eq!(it.at_st(), *st, "sample {i}: start timestamp");
    }
    assert!(it.next_sample().unwrap().is_none());
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/float_histogram_st_test.go::TestFloatHistogramSTChunkST
#[test]
fn test_float_histogram_st_chunk_st() {
    let run_case = |samples: &[(i64, i64, f64)]| {
        let mut chunk = FloatHistogramStChunk::new();
        let mut clone_bytes = Vec::new();
        {
            let mut app = chunk.appender().unwrap();
            for (i, &(st, t, v)) in samples.iter().enumerate() {
                if i + 1 == samples.len() {
                    drop(app);
                    clone_bytes = chunk.as_bytes().to_vec();
                    app = chunk.appender().unwrap();
                }
                let fh = FloatHistogram {
                    sum: v,
                    count: v * 10.0,
                    ..Default::default()
                };
                match app.append_float_histogram(None, st, t, &fh, false).unwrap() {
                    AppendFloatHistogramStOutcome::Appended { recoded } => assert!(!recoded),
                    AppendFloatHistogramStOutcome::NewChunk(_) => panic!("unexpected new chunk"),
                }
            }
        }
        let mut chunks_to_test = vec![chunk];
        if let Some(&(st, t, v)) = samples.last() {
            let mut clone_chunk = FloatHistogramStChunk::new();
            clone_chunk.reset(clone_bytes);
            let fh = FloatHistogram {
                sum: v,
                count: v * 10.0,
                ..Default::default()
            };
            let _ = clone_chunk
                .appender()
                .unwrap()
                .append_float_histogram(None, st, t, &fh, false)
                .unwrap();
            chunks_to_test.push(clone_chunk);
        }
        for chk in &chunks_to_test {
            assert_eq!(chk.num_samples(), samples.len());
            let mut it = chk.iter();
            for &(st, t, v) in samples {
                let got_t = it.next_sample().unwrap().unwrap();
                let (fh_t, fh) = it.at_float_histogram();
                assert_eq!(got_t, t);
                assert_eq!(fh_t, t);
                assert_eq!(it.at_st(), st);
                assert!((fh.sum - v).abs() <= 1e-9);
            }
            assert!(it.next_sample().unwrap().is_none());
        }
    };

    run_case(&[
        (0, 1000, 1.5),
        (0, 2000, 2.5),
        (0, 3000, 3.5),
        (0, 4000, 4.5),
    ]);

    let st_times = [0i64, 500, 1000, 1500, 2000, 2500, 3000, 3500, 4000];
    let ts_fn = |j: usize| -> i64 { 1000 * (j as i64 + 1) };
    for n in 0..5 {
        let mut samples = vec![(0i64, 0i64, 0.0f64); n];
        let mut idx = vec![0usize; n];
        loop {
            for j in 0..n {
                samples[j] = (st_times[idx[j]], ts_fn(j), j as f64 + 0.5);
            }
            run_case(&samples);
            let mut exhausted = true;
            for j in (0..n).rev() {
                if st_times[idx[j]] < ts_fn(j) {
                    idx[j] += 1;
                    exhausted = false;
                    break;
                }
                idx[j] = 0;
            }
            if exhausted {
                break;
            }
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/float_histogram_st_test.go::TestFloatHistogramSTBasic
#[test]
fn test_float_histogram_st_basic() {
    let fhs = generate_test_float_histograms(5);
    let samples: Vec<_> = fhs
        .into_iter()
        .enumerate()
        .map(|(i, fh)| (0, ((i + 1) * 1000) as i64, fh))
        .collect();
    require_float_histogram_st_samples(&samples);
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/float_histogram_st_test.go::TestFloatHistogramSTChunkAppendAndIterate
#[test]
fn test_float_histogram_st_chunk_append_and_iterate() {
    let fhs = generate_test_float_histograms(5);
    let sts = [100, 100, 200, 200, 300];
    let samples: Vec<_> = fhs
        .into_iter()
        .enumerate()
        .map(|(i, fh)| (sts[i], ((i + 1) * 1000) as i64, fh))
        .collect();
    require_float_histogram_st_samples(&samples);
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/float_histogram_st_test.go::TestFloatHistogramSTChunkCounterReset
#[test]
fn test_float_histogram_st_chunk_counter_reset() {
    let mut c = FloatHistogramStChunk::new();
    let fh1 = FloatHistogram {
        count: 10.0,
        zero_count: 2.0,
        sum: 18.4,
        zero_threshold: 1e-125,
        schema: 1,
        positive_spans: vec![HistogramSpan { offset: 0, length: 2 }],
        positive_buckets: vec![6.0, 3.0],
        ..Default::default()
    };
    {
        let mut app = c.appender().unwrap();
        let out = app
            .append_float_histogram(None, 100, 1000, &fh1, false)
            .unwrap();
        assert!(matches!(
            out,
            AppendFloatHistogramStOutcome::Appended { recoded: false }
        ));
    }

    let mut fh2 = fh1.clone();
    fh2.count = 3.0;
    fh2.sum = 5.0;
    fh2.positive_buckets = vec![2.0, 1.0];
    fh2.counter_reset_hint = CounterResetHint::CounterReset;

    let mut st_chunk = match c
        .appender()
        .unwrap()
        .append_float_histogram(None, 200, 2000, &fh2, false)
        .unwrap()
    {
        AppendFloatHistogramStOutcome::NewChunk(nc) => nc,
        AppendFloatHistogramStOutcome::Appended { .. } => panic!("expected new chunk"),
    };
    assert_eq!(st_chunk.counter_reset_header(), CounterResetHeader::CounterReset);
    assert_eq!(st_chunk.num_samples(), 1);

    {
        let mut it = st_chunk.iter();
        assert_eq!(it.next_sample().unwrap(), Some(2000));
        assert_eq!(it.at_st(), 200);
        assert!(it.next_sample().unwrap().is_none());
    }

    let mut fh3 = fh2.clone();
    fh3.counter_reset_hint = CounterResetHint::UnknownCounterReset;
    fh3.count = 8.0;
    fh3.sum = 10.0;
    fh3.positive_buckets = vec![4.0, 2.0];
    let _ = st_chunk
        .appender()
        .unwrap()
        .append_float_histogram(None, 300, 3000, &fh3, false)
        .unwrap();
    assert_eq!(st_chunk.num_samples(), 2);
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/float_histogram_st_test.go::TestFloatHistogramSTChunkRecode
#[test]
fn test_float_histogram_st_chunk_recode() {
    let mut c = FloatHistogramStChunk::new();
    let h1 = Histogram {
        count: 27,
        zero_count: 2,
        sum: 18.4,
        zero_threshold: 1e-125,
        schema: 1,
        positive_spans: vec![
            HistogramSpan { offset: 0, length: 2 },
            HistogramSpan { offset: 2, length: 1 },
            HistogramSpan { offset: 3, length: 2 },
            HistogramSpan { offset: 3, length: 1 },
            HistogramSpan { offset: 1, length: 1 },
        ],
        positive_buckets: vec![6, -3, 0, -1, 2, 1, -4],
        negative_spans: vec![HistogramSpan { offset: 1, length: 1 }],
        negative_buckets: vec![1],
        ..Default::default()
    };
    let fh1 = h1.to_float();
    let _ = c
        .appender()
        .unwrap()
        .append_float_histogram(None, 100, 1000, &fh1, false)
        .unwrap();

    let mut h2 = h1.clone();
    h2.positive_spans = vec![
        HistogramSpan { offset: 0, length: 3 },
        HistogramSpan { offset: 1, length: 1 },
        HistogramSpan { offset: 1, length: 4 },
        HistogramSpan { offset: 3, length: 3 },
    ];
    h2.negative_spans = vec![HistogramSpan { offset: 0, length: 2 }];
    h2.count = 35;
    h2.zero_count += 1;
    h2.sum = 30.0;
    h2.positive_buckets = vec![7, -2, -4, 2, -2, -1, 2, 3, 0, -5, 1];
    h2.negative_buckets = vec![2, -1];
    let fh2 = h2.to_float();

    match c
        .appender()
        .unwrap()
        .append_float_histogram(None, 200, 2000, &fh2, false)
        .unwrap()
    {
        AppendFloatHistogramStOutcome::Appended { recoded } => assert!(recoded),
        AppendFloatHistogramStOutcome::NewChunk(nc) => c = nc,
    }
    assert_eq!(c.num_samples(), 2);

    {
        let mut it = c.iter();
        assert_eq!(it.next_sample().unwrap(), Some(1000));
        assert_eq!(it.at_st(), 100);
        assert_eq!(it.next_sample().unwrap(), Some(2000));
        assert_eq!(it.at_st(), 200);
        assert!(it.next_sample().unwrap().is_none());
    }

    let mut fh3 = fh2.clone();
    fh3.count = 40.0;
    fh3.sum = 35.0;
    for b in &mut fh3.positive_buckets {
        *b += 1.0;
    }
    let _ = c
        .appender()
        .unwrap()
        .append_float_histogram(None, 300, 3000, &fh3, false)
        .unwrap();
    assert_eq!(c.num_samples(), 3);
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/float_histogram_st_test.go::TestFloatHistogramST_MoreThan127Samples
#[test]
fn test_float_histogram_st_more_than_127_samples() {
    let mut c = FloatHistogramStChunk::new();
    let fh = FloatHistogram {
        count: 5.0,
        zero_count: 2.0,
        sum: 18.4,
        zero_threshold: 1e-125,
        schema: 1,
        positive_spans: vec![HistogramSpan { offset: 0, length: 2 }],
        positive_buckets: vec![6.0, 3.0],
        ..Default::default()
    };

    let num_samples = usize::from(MAX_FIRST_ST_CHANGE_ON) + 3;
    {
        let mut app = c.appender().unwrap();
        for i in 0..num_samples {
            let mut fhi = fh.clone();
            fhi.count = (5 + i) as f64;
            fhi.sum = (18 + i) as f64;
            let _ = app
                .append_float_histogram(None, 500, 1000 + (i as i64) * 1000, &fhi, false)
                .unwrap();
        }
    }
    assert_eq!(c.num_samples(), num_samples);

    let mut it = c.iter();
    for i in 0..num_samples {
        assert_eq!(it.next_sample().unwrap(), Some(1000 + (i as i64) * 1000));
        assert_eq!(it.at_st(), 500);
    }
    assert!(it.next_sample().unwrap().is_none());

    let mut c2 = FloatHistogramStChunk::new();
    let first_batch = usize::from(MAX_FIRST_ST_CHANGE_ON) + 1;
    {
        let mut app2 = c2.appender().unwrap();
        for i in 0..first_batch {
            let mut fhi = fh.clone();
            fhi.count = (5 + i) as f64;
            fhi.sum = (18 + i) as f64;
            let _ = app2
                .append_float_histogram(None, 0, 1000 + (i as i64) * 1000, &fhi, false)
                .unwrap();
        }
        for i in 0..3 {
            let mut fhi = fh.clone();
            fhi.count = (200 + i) as f64;
            fhi.sum = (200 + i) as f64;
            let _ = app2
                .append_float_histogram(None, 100, 200_000 + (i as i64) * 1000, &fhi, false)
                .unwrap();
        }
    }
    let mut it2 = c2.iter();
    for i in 0..first_batch {
        assert_eq!(it2.next_sample().unwrap(), Some(1000 + (i as i64) * 1000));
        assert_eq!(it2.at_st(), 0);
    }
    for i in 0..3 {
        assert_eq!(it2.next_sample().unwrap(), Some(200_000 + (i as i64) * 1000));
        assert_eq!(it2.at_st(), 100);
    }
    assert!(it2.next_sample().unwrap().is_none());
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/float_histogram_st_test.go::TestFloatHistogramSTChunkMixedST
#[test]
fn test_float_histogram_st_chunk_mixed_st() {
    let fhs = generate_test_float_histograms(5);
    let sts = [0, 0, 100, 0, 200];
    let samples: Vec<_> = fhs
        .into_iter()
        .enumerate()
        .map(|(i, fh)| (sts[i], ((i + 1) * 1000) as i64, fh))
        .collect();
    require_float_histogram_st_samples(&samples);
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/float_histogram_st_test.go::TestFloatHistogramSTChunkSeek
#[test]
fn test_float_histogram_st_chunk_seek() {
    let fhs = generate_test_float_histograms(5);
    let sts = [0, 0, 100, 100, 200];
    let mut c = FloatHistogramStChunk::new();
    {
        let mut app = c.appender().unwrap();
        for (i, fh) in fhs.iter().enumerate() {
            let _ = app
                .append_float_histogram(None, sts[i], ((i + 1) * 1000) as i64, fh, false)
                .unwrap();
        }
    }

    let mut it = c.iter();
    assert_eq!(it.seek(2000).unwrap().unwrap().0, 2000);
    assert_eq!(it.at_st(), 0);

    assert_eq!(it.seek(3000).unwrap().unwrap().0, 3000);
    assert_eq!(it.at_st(), 100);

    assert_eq!(it.seek(3000).unwrap().unwrap().0, 3000);
    assert_eq!(it.at_st(), 100);

    assert_eq!(it.seek(4000).unwrap().unwrap().0, 4000);
    assert_eq!(it.at_st(), 100);

    assert_eq!(it.seek(5000).unwrap().unwrap().0, 5000);
    assert_eq!(it.at_st(), 200);

    assert!(it.seek(5001).unwrap().is_none());
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/float_histogram_st_test.go::TestFloatHistogramSTChunkStaleSamples
#[test]
fn test_float_histogram_st_chunk_stale_samples() {
    let mut c = FloatHistogramStChunk::new();
    let fh1 = generate_test_float_histogram(0);
    let stale1 = FloatHistogram {
        sum: stale_nan(),
        ..Default::default()
    };
    let stale2 = FloatHistogram {
        sum: stale_nan(),
        ..Default::default()
    };

    {
        let mut app = c.appender().unwrap();
        let _ = app
            .append_float_histogram(None, 100, 1000, &fh1, false)
            .unwrap();
        let _ = app
            .append_float_histogram(None, 200, 2000, &stale1, false)
            .unwrap();
        let _ = app
            .append_float_histogram(None, 0, 3000, &stale2, false)
            .unwrap();
    }

    let mut it = c.iter();
    assert_eq!(it.next_sample().unwrap(), Some(1000));
    assert_eq!(it.at_st(), 100);
    assert!(!is_stale_nan(it.at_float_histogram().1.sum));

    assert_eq!(it.next_sample().unwrap(), Some(2000));
    assert_eq!(it.at_st(), 200);
    assert!(is_stale_nan(it.at_float_histogram().1.sum));

    assert_eq!(it.next_sample().unwrap(), Some(3000));
    assert_eq!(it.at_st(), 0);
    assert!(is_stale_nan(it.at_float_histogram().1.sum));

    assert!(it.next_sample().unwrap().is_none());
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/float_histogram_st_test.go::TestFloatHistogramSTAppendOnlyErrorsPreserveChunk
#[test]
fn test_float_histogram_st_append_only_errors_preserve_chunk() {
    // schema change
    {
        let mut c = FloatHistogramStChunk::new();
        let h = generate_test_float_histogram(0);
        {
            let mut app = c.appender().unwrap();
            match app.append_float_histogram(None, 100, 1000, &h, true).unwrap() {
                AppendFloatHistogramStOutcome::Appended { recoded } => assert!(!recoded),
                _ => panic!("unexpected new chunk"),
            }
        }
        let before = c.as_bytes().to_vec();
        let before_samples = c.num_samples();

        let mut h2 = h.clone();
        h2.schema += 1;
        let err = c
            .appender()
            .unwrap()
            .append_float_histogram(None, 200, 2000, &h2, true)
            .unwrap_err();
        assert!(matches!(err, ChunkError::FloatHistogramSchemaChange));
        assert_eq!(c.num_samples(), before_samples);
        assert_eq!(c.as_bytes(), before.as_slice());
    }

    // counter reset
    {
        let mut c = FloatHistogramStChunk::new();
        let h = generate_test_float_histogram(0);
        {
            let mut app = c.appender().unwrap();
            match app.append_float_histogram(None, 100, 1000, &h, true).unwrap() {
                AppendFloatHistogramStOutcome::Appended { recoded } => assert!(!recoded),
                _ => panic!("unexpected new chunk"),
            }
        }
        let before = c.as_bytes().to_vec();
        let before_samples = c.num_samples();

        let mut h2 = h.clone();
        h2.counter_reset_hint = CounterResetHint::CounterReset;
        let err = c
            .appender()
            .unwrap()
            .append_float_histogram(None, 200, 2000, &h2, true)
            .unwrap_err();
        assert!(matches!(err, ChunkError::FloatHistogramCounterReset));
        assert_eq!(c.num_samples(), before_samples);
        assert_eq!(c.as_bytes(), before.as_slice());
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/float_histogram_st_test.go::TestFloatHistogramSTChunkGaugeRecodePreservesST
#[test]
fn test_float_histogram_st_chunk_gauge_recode_preserves_st() {
    let mut c = FloatHistogramStChunk::new();
    let fh1 = FloatHistogram {
        counter_reset_hint: CounterResetHint::GaugeType,
        count: 5.0,
        zero_count: 2.0,
        sum: 18.4,
        zero_threshold: 1e-125,
        schema: 1,
        positive_spans: vec![
            HistogramSpan { offset: 0, length: 2 },
            HistogramSpan { offset: 2, length: 1 },
            HistogramSpan { offset: 3, length: 2 },
            HistogramSpan { offset: 3, length: 1 },
            HistogramSpan { offset: 1, length: 1 },
        ],
        positive_buckets: vec![6.0, 3.0, 3.0, 2.0, 4.0, 5.0, 1.0],
        ..Default::default()
    };
    let _ = c
        .appender()
        .unwrap()
        .append_float_histogram(None, 100, 1000, &fh1, false)
        .unwrap();
    assert_eq!(c.counter_reset_header(), CounterResetHeader::GaugeType);

    let mut fh2 = fh1.clone();
    fh2.count += 9.0;
    fh2.zero_count += 1.0;
    fh2.sum = 30.0;
    fh2.positive_spans = vec![
        HistogramSpan { offset: 0, length: 3 },
        HistogramSpan { offset: 1, length: 1 },
        HistogramSpan { offset: 1, length: 4 },
        HistogramSpan { offset: 3, length: 3 },
    ];
    fh2.positive_buckets = vec![7.0, 5.0, 1.0, 3.0, 1.0, 0.0, 2.0, 5.0, 5.0, 0.0, 1.0];

    match c
        .appender()
        .unwrap()
        .append_float_histogram(None, 200, 2000, &fh2, false)
        .unwrap()
    {
        AppendFloatHistogramStOutcome::Appended { recoded } => assert!(recoded),
        AppendFloatHistogramStOutcome::NewChunk(nc) => c = nc,
    }
    assert_eq!(c.counter_reset_header(), CounterResetHeader::GaugeType);

    {
        let mut it = c.iter();
        assert_eq!(it.next_sample().unwrap(), Some(1000));
        assert_eq!(it.at_st(), 100);
        assert_eq!(it.next_sample().unwrap(), Some(2000));
        assert_eq!(it.at_st(), 200);
        assert!(it.next_sample().unwrap().is_none());
    }

    let mut fh3 = fh2.clone();
    fh3.sum = 35.0;
    for b in &mut fh3.positive_buckets {
        *b += 1.0;
    }
    let _ = c
        .appender()
        .unwrap()
        .append_float_histogram(None, 300, 3000, &fh3, false)
        .unwrap();
    assert_eq!(c.num_samples(), 3);
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/float_histogram_st_test.go::TestFloatHistogramSTChunk_CounterResetHeader
#[test]
fn test_float_histogram_st_chunk_counter_reset_header() {
    let mut c = FloatHistogramStChunk::new();
    assert_eq!(c.as_bytes().len(), 3);
    let fh = generate_test_float_histogram(0);
    let _ = c
        .appender()
        .unwrap()
        .append_float_histogram(None, 100, 1000, &fh, false)
        .unwrap();

    let byte2_before = c.as_bytes()[2];
    for cr in [
        CounterResetHeader::UnknownCounterReset,
        CounterResetHeader::CounterReset,
        CounterResetHeader::NotCounterReset,
        CounterResetHeader::GaugeType,
    ] {
        {
            let mut happ = c.appender().unwrap();
            happ.set_counter_reset_header(cr);
            assert_eq!(happ.num_samples(), 1);
        }
        assert_eq!(c.counter_reset_header(), cr);
        assert_eq!(c.num_samples(), 1);
        assert_eq!(c.as_bytes()[2], byte2_before);
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/float_histogram_st_test.go::TestFloatHistogramSTChunkOverFlowPanics
#[test]
fn test_float_histogram_st_chunk_overflow_panics() {
    let mut chunk = FloatHistogramStChunk::new();
    let mut app = chunk.appender().unwrap();
    let mut fh = FloatHistogram {
        zero_threshold: 1e-128,
        ..Default::default()
    };
    for i in 0..HISTOGRAM_ST_SAMPLE_COUNT_MASK {
        fh.count = f64::from(i);
        fh.zero_count = f64::from(i);
        let _ = app
            .append_float_histogram(None, 0, i64::from(i), &fh, true)
            .unwrap();
    }
    fh.count = f64::from(HISTOGRAM_ST_SAMPLE_COUNT_MASK);
    fh.zero_count = f64::from(HISTOGRAM_ST_SAMPLE_COUNT_MASK);
    let err = app
        .append_float_histogram(None, 0, i64::from(HISTOGRAM_ST_SAMPLE_COUNT_MASK), &fh, true)
        .unwrap_err();
    assert!(matches!(err, ChunkError::CapacityExceeded));
}

// ============================================================================
// float_histogram_test.go
// ============================================================================

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/float_histogram_test.go::TestFirstFloatHistogramExplicitCounterReset
#[test]
fn test_first_float_histogram_explicit_counter_reset() {
    let tests = [
        (
            CounterResetHint::CounterReset,
            CounterResetHeader::CounterReset,
            CounterResetHint::UnknownCounterReset,
        ),
        (
            CounterResetHint::NotCounterReset,
            CounterResetHeader::UnknownCounterReset,
            CounterResetHint::UnknownCounterReset,
        ),
        (
            CounterResetHint::UnknownCounterReset,
            CounterResetHeader::UnknownCounterReset,
            CounterResetHint::UnknownCounterReset,
        ),
        (
            CounterResetHint::GaugeType,
            CounterResetHeader::GaugeType,
            CounterResetHint::GaugeType,
        ),
    ];

    for (hint, exp_header, exp_hint) in tests {
        let fh = FloatHistogram {
            counter_reset_hint: hint,
            ..Default::default()
        };
        let mut chk = FloatHistogramChunk::new();
        let out = chk
            .appender()
            .unwrap()
            .append_float_histogram(None, 0, 0, &fh, false)
            .unwrap();
        assert!(matches!(
            out,
            AppendFloatHistogramOutcome::Appended { recoded: false }
        ));
        assert_eq!(chk.counter_reset_header(), exp_header);
        assert_first_float_histogram_sample_hint(&chk, exp_hint);
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/float_histogram_test.go::TestFloatHistogramChunkSameBuckets
#[test]
fn test_float_histogram_chunk_same_buckets() {
    let mut c = FloatHistogramChunk::new();
    let mut exp: Vec<(i64, FloatHistogram)> = Vec::new();
    assert_eq!(c.num_samples(), 0);

    let mut ts = 1_234_567_890i64;
    let mut h = Histogram {
        count: 15,
        zero_count: 2,
        sum: 18.4,
        zero_threshold: 1e-100,
        schema: 1,
        positive_spans: vec![
            HistogramSpan { offset: 0, length: 2 },
            HistogramSpan { offset: 1, length: 2 },
        ],
        positive_buckets: vec![1, 1, -1, 0],
        negative_spans: vec![
            HistogramSpan { offset: 1, length: 1 },
            HistogramSpan { offset: 2, length: 3 },
        ],
        negative_buckets: vec![2, 1, -1, -1],
        ..Default::default()
    };
    {
        let mut app = c.appender().unwrap();
        let fh1 = h.to_float();
        let out = app.append_float_histogram(None, 0, ts, &fh1, false).unwrap();
        assert!(matches!(out, AppendFloatHistogramOutcome::Appended { .. }));
        exp.push((ts, fh1));

        ts += 16;
        h.count = 32;
        h.zero_count += 1;
        h.sum = 24.4;
        h.positive_buckets = vec![5, -2, 1, -2];
        h.negative_buckets = vec![4, -1, 1, -1];
        let fh2 = h.to_float();
        let out = app.append_float_histogram(None, 0, ts, &fh2, false).unwrap();
        assert!(matches!(out, AppendFloatHistogramOutcome::Appended { .. }));
        let mut exp_h = fh2;
        exp_h.counter_reset_hint = CounterResetHint::NotCounterReset;
        exp.push((ts, exp_h));
    }
    assert_eq!(c.num_samples(), 2);

    {
        let mut app = c.appender().unwrap();
        ts += 14;
        h.count = 54;
        h.zero_count += 2;
        h.sum = 24.4;
        h.positive_buckets = vec![6, 1, -3, 6];
        h.negative_buckets = vec![5, 1, -2, 3];
        let fh3 = h.to_float();
        let out = app.append_float_histogram(None, 0, ts, &fh3, false).unwrap();
        assert!(matches!(out, AppendFloatHistogramOutcome::Appended { .. }));
        let mut exp_h = fh3;
        exp_h.counter_reset_hint = CounterResetHint::NotCounterReset;
        exp.push((ts, exp_h));
    }
    assert_eq!(c.num_samples(), 3);

    let mut it = c.iter();
    let mut act = Vec::new();
    while it.next_sample().unwrap().is_some() {
        act.push(it.at_float_histogram());
    }
    assert_eq!(act, exp);

    it.reset(c.as_bytes());
    let mut act2 = Vec::new();
    while it.next_sample().unwrap().is_some() {
        act2.push(it.at_float_histogram());
    }
    assert_eq!(act2, exp);

    let mut it_x = c.iter();
    while it_x.next_sample().unwrap().is_some() {}
    it_x.reset(c.as_bytes());
    let mut act3 = Vec::new();
    while it_x.next_sample().unwrap().is_some() {
        act3.push(it_x.at_float_histogram());
    }
    assert_eq!(act3, exp);

    let mid = exp.len() / 2;
    let mut it4 = c.iter();
    let mut act4 = Vec::new();
    assert!(it4.seek(exp[mid].0).unwrap().is_some());
    assert!(it4.seek(exp[mid].0).unwrap().is_some());
    assert!(it4.seek(exp[mid].0).unwrap().is_some());
    act4.push(it4.at_float_histogram());
    while it4.next_sample().unwrap().is_some() {
        act4.push(it4.at_float_histogram());
    }
    assert_eq!(act4, exp[mid..]);
    assert!(it4.seek(exp.last().unwrap().0 + 1).unwrap().is_none());
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/float_histogram_test.go::TestFloatHistogramChunkBucketChanges
#[test]
fn test_float_histogram_chunk_bucket_changes() {
    let mut c = FloatHistogramChunk::new();
    let ts1 = 1_234_567_890i64;
    let mut h1 = Histogram {
        count: 27,
        zero_count: 2,
        sum: 18.4,
        zero_threshold: 1e-125,
        schema: 1,
        positive_spans: vec![
            HistogramSpan { offset: 0, length: 2 },
            HistogramSpan { offset: 2, length: 1 },
            HistogramSpan { offset: 3, length: 2 },
            HistogramSpan { offset: 3, length: 1 },
            HistogramSpan { offset: 1, length: 1 },
        ],
        positive_buckets: vec![6, -3, 0, -1, 2, 1, -4],
        negative_spans: vec![HistogramSpan { offset: 1, length: 1 }],
        negative_buckets: vec![1],
        ..Default::default()
    };
    {
        let mut app = c.appender().unwrap();
        app.append_float_histogram(None, 0, ts1, &h1.to_float(), false)
            .unwrap();
    }

    let ts2 = ts1 + 16;
    let mut h2 = h1.clone();
    h2.positive_spans = vec![
        HistogramSpan { offset: 0, length: 3 },
        HistogramSpan { offset: 1, length: 1 },
        HistogramSpan { offset: 1, length: 4 },
        HistogramSpan { offset: 3, length: 3 },
    ];
    h2.negative_spans = vec![HistogramSpan { offset: 0, length: 2 }];
    h2.count = 35;
    h2.zero_count += 1;
    h2.sum = 30.0;
    h2.positive_buckets = vec![7, -2, -4, 2, -2, -1, 2, 3, 0, -5, 1];
    h2.negative_buckets = vec![2, -1];
    let fh2 = h2.to_float();

    {
        let mut h_app = c.appender().unwrap();
        let (p_fwd, n_fwd, p_bwd, n_bwd, ok, cr) = h_app.appendable(&fh2);
        assert!(!p_fwd.is_empty());
        assert!(!n_fwd.is_empty());
        assert!(p_bwd.is_empty());
        assert!(n_bwd.is_empty());
        assert!(ok);
        assert!(!cr);
        h_app
            .recode_chunk(&p_fwd, &n_fwd, &fh2.positive_spans, &fh2.negative_spans)
            .unwrap();
        h_app
            .append_float_histogram(None, 0, ts2, &fh2, false)
            .unwrap();
    }
    assert_eq!(c.num_samples(), 2);

    h1.positive_spans = h2.positive_spans.clone();
    h1.positive_buckets = vec![6, -3, -3, 3, -3, 0, 2, 2, 1, -5, 1];
    h1.negative_spans = h2.negative_spans.clone();
    h1.negative_buckets = vec![0, 1];
    let mut exp_h2 = fh2;
    exp_h2.counter_reset_hint = CounterResetHint::NotCounterReset;
    let exp = vec![(ts1, h1.to_float()), (ts2, exp_h2)];

    let mut it = c.iter();
    let mut act = Vec::new();
    while it.next_sample().unwrap().is_some() {
        act.push(it.at_float_histogram());
    }
    assert_eq!(act, exp);
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/float_histogram_test.go::TestFloatHistogramChunkAppendable
#[test]
fn test_float_histogram_chunk_appendable() {
    let eh = FloatHistogram {
        count: 5.0,
        zero_count: 2.0,
        sum: 18.4,
        zero_threshold: 1e-125,
        schema: 1,
        positive_spans: vec![
            HistogramSpan { offset: 0, length: 2 },
            HistogramSpan { offset: 2, length: 1 },
            HistogramSpan { offset: 3, length: 2 },
            HistogramSpan { offset: 3, length: 1 },
            HistogramSpan { offset: 1, length: 1 },
        ],
        positive_buckets: vec![6.0, 3.0, 3.0, 2.0, 4.0, 5.0, 1.0],
        ..Default::default()
    };

    let cbh = FloatHistogram {
        count: 24.0,
        sum: 18.4,
        schema: CUSTOM_BUCKETS_SCHEMA,
        positive_spans: vec![
            HistogramSpan { offset: 0, length: 2 },
            HistogramSpan { offset: 2, length: 1 },
            HistogramSpan { offset: 3, length: 2 },
            HistogramSpan { offset: 3, length: 1 },
            HistogramSpan { offset: 1, length: 1 },
        ],
        positive_buckets: vec![6.0, 3.0, 3.0, 2.0, 4.0, 5.0, 1.0],
        custom_values: (1..=20).map(|i| i as f64).collect(),
        ..Default::default()
    };

    let setup = |h: &FloatHistogram| -> (FloatHistogramChunk, i64, FloatHistogram) {
        let mut c = FloatHistogramChunk::new();
        let ts = 1_234_567_890i64;
        let out = c
            .appender()
            .unwrap()
            .append_float_histogram(None, 0, ts, h, false)
            .unwrap();
        assert!(matches!(out, AppendFloatHistogramOutcome::Appended { .. }));
        assert_eq!(c.num_samples(), 1);
        assert_eq!(c.counter_reset_header(), CounterResetHeader::UnknownCounterReset);
        (c, ts, h.clone())
    };

    let assert_new_chunk = |old: &mut FloatHistogramChunk,
                            ts: i64,
                            h: &FloatHistogram,
                            exp_header: CounterResetHeader,
                            exp_hint: CounterResetHint| {
        let old_bytes = old.as_bytes().to_vec();
        let out = old
            .appender()
            .unwrap()
            .append_float_histogram(None, 0, ts, h, false)
            .unwrap();
        assert_eq!(old.as_bytes(), old_bytes.as_slice());
        let AppendFloatHistogramOutcome::NewChunk(nc) = out else {
            panic!("expected new chunk");
        };
        assert_eq!(nc.counter_reset_header(), exp_header);
        assert_fhist_sample_count(&nc, 1);
        assert_first_float_histogram_sample_hint(&nc, exp_hint);
    };

    let assert_no_new_chunk = |curr: &mut FloatHistogramChunk,
                               ts: i64,
                               h: &FloatHistogram,
                               exp_header: CounterResetHeader| {
        let prev_len = curr.as_bytes().len();
        let out = curr
            .appender()
            .unwrap()
            .append_float_histogram(None, 0, ts, h, false)
            .unwrap();
        assert!(curr.as_bytes().len() > prev_len);
        assert!(matches!(
            out,
            AppendFloatHistogramOutcome::Appended { recoded: false }
        ));
        assert_eq!(curr.counter_reset_header(), exp_header);
        assert_fhist_sample_count(curr, 2);
    };

    let assert_recoded_chunk = |curr: &mut FloatHistogramChunk,
                                ts: i64,
                                h: &FloatHistogram,
                                exp_header: CounterResetHeader| {
        let out = curr
            .appender()
            .unwrap()
            .append_float_histogram(None, 0, ts, h, false)
            .unwrap();
        match out {
            AppendFloatHistogramOutcome::Appended { recoded } => {
                assert!(recoded);
                assert_eq!(curr.counter_reset_header(), exp_header);
                assert_fhist_sample_count(curr, 2);
            }
            AppendFloatHistogramOutcome::NewChunk(nc) => {
                assert_eq!(nc.counter_reset_header(), exp_header);
                assert_fhist_sample_count(&nc, 2);
            }
        }
    };

    // Schema change.
    {
        let (mut c, ts, h1) = setup(&eh);
        let mut h2 = h1.clone();
        h2.schema += 1;
        assert!(!c.appender().unwrap().appendable(&h2).4);
        assert_new_chunk(
            &mut c,
            ts + 1,
            &h2,
            CounterResetHeader::UnknownCounterReset,
            CounterResetHint::UnknownCounterReset,
        );
    }

    // Zero threshold change.
    {
        let (mut c, ts, h1) = setup(&eh);
        let mut h2 = h1.clone();
        h2.zero_threshold += 0.1;
        assert!(!c.appender().unwrap().appendable(&h2).4);
        assert_new_chunk(
            &mut c,
            ts + 1,
            &h2,
            CounterResetHeader::UnknownCounterReset,
            CounterResetHint::UnknownCounterReset,
        );
    }

    // More buckets.
    {
        let (mut c, ts, h1) = setup(&eh);
        let mut h2 = h1.clone();
        h2.positive_spans = vec![
            HistogramSpan { offset: 0, length: 3 },
            HistogramSpan { offset: 1, length: 1 },
            HistogramSpan { offset: 1, length: 4 },
            HistogramSpan { offset: 3, length: 3 },
        ];
        h2.count += 9.0;
        h2.zero_count += 1.0;
        h2.sum = 30.0;
        h2.positive_buckets = vec![7.0, 5.0, 1.0, 3.0, 1.0, 0.0, 2.0, 5.0, 5.0, 0.0, 1.0];
        let (p_i, n_i, p_bi, n_bi, ok, cr) = c.appender().unwrap().appendable(&h2);
        assert!(!p_i.is_empty() && n_i.is_empty() && p_bi.is_empty() && n_bi.is_empty() && ok && !cr);
        assert_recoded_chunk(&mut c, ts + 1, &h2, CounterResetHeader::UnknownCounterReset);
    }

    // Bucket missing.
    {
        let (mut c, ts, h1) = setup(&eh);
        let mut h2 = h1.clone();
        h2.positive_spans = vec![
            HistogramSpan { offset: 0, length: 2 },
            HistogramSpan { offset: 5, length: 2 },
            HistogramSpan { offset: 3, length: 1 },
            HistogramSpan { offset: 1, length: 1 },
        ];
        h2.sum = 21.0;
        h2.positive_buckets = vec![6.0, 3.0, 2.0, 4.0, 5.0, 1.0];
        let (p_i, n_i, p_bi, n_bi, ok, cr) = c.appender().unwrap().appendable(&h2);
        assert!(p_i.is_empty() && n_i.is_empty() && p_bi.is_empty() && n_bi.is_empty() && !ok && cr);
        assert_new_chunk(
            &mut c,
            ts + 1,
            &h2,
            CounterResetHeader::CounterReset,
            CounterResetHint::UnknownCounterReset,
        );
    }

    // Buckets missing but empty.
    {
        let mut empty_h = eh.clone();
        empty_h.positive_buckets = vec![6.0, 0.0, 3.0, 2.0, 4.0, 0.0, 1.0];
        let (mut c, ts, h1) = setup(&empty_h);
        let mut h2 = h1.clone();
        h2.positive_spans = vec![
            HistogramSpan { offset: 0, length: 1 },
            HistogramSpan { offset: 3, length: 1 },
            HistogramSpan { offset: 3, length: 2 },
            HistogramSpan { offset: 5, length: 1 },
        ];
        h2.positive_buckets = vec![7.0, 4.0, 3.0, 5.0, 2.0];
        let (p_i, n_i, p_bi, n_bi, ok, cr) = c.appender().unwrap().appendable(&h2);
        assert!(p_i.is_empty() && n_i.is_empty() && !p_bi.is_empty() && n_bi.is_empty() && ok && !cr);
        assert_no_new_chunk(&mut c, ts + 1, &h2, CounterResetHeader::UnknownCounterReset);
    }

    // New buckets AND empty buckets missing.
    {
        let mut empty_h = eh.clone();
        empty_h.positive_buckets = vec![6.0, 0.0, 3.0, 2.0, 4.0, 0.0, 1.0];
        let (mut c, ts, h1) = setup(&empty_h);
        let mut h2 = h1.clone();
        h2.positive_spans = vec![
            HistogramSpan { offset: 0, length: 1 },
            HistogramSpan { offset: 3, length: 1 },
            HistogramSpan { offset: 3, length: 2 },
            HistogramSpan { offset: 5, length: 2 },
        ];
        h2.positive_buckets = vec![7.0, 4.0, 3.0, 5.0, 2.0, 3.0];
        let (p_i, n_i, p_bi, n_bi, ok, cr) = c.appender().unwrap().appendable(&h2);
        assert!(!p_i.is_empty() && n_i.is_empty() && !p_bi.is_empty() && n_bi.is_empty() && ok && !cr);
        assert_recoded_chunk(&mut c, ts + 1, &h2, CounterResetHeader::UnknownCounterReset);
    }

    // Counter reset while buckets same / new buckets / before first / explicit.
    {
        let (mut c, ts, h1) = setup(&eh);
        let mut h2 = h1.clone();
        h2.sum = 23.0;
        h2.positive_buckets = vec![6.0, 2.0, 3.0, 2.0, 4.0, 5.0, 1.0];
        assert!(!c.appender().unwrap().appendable(&h2).4);
        assert_new_chunk(
            &mut c,
            ts + 1,
            &h2,
            CounterResetHeader::CounterReset,
            CounterResetHint::UnknownCounterReset,
        );
    }
    {
        let (mut c, ts, h1) = setup(&eh);
        let mut h2 = h1.clone();
        h2.positive_spans = vec![
            HistogramSpan { offset: 0, length: 3 },
            HistogramSpan { offset: 1, length: 1 },
            HistogramSpan { offset: 1, length: 4 },
            HistogramSpan { offset: 3, length: 3 },
        ];
        h2.sum = 29.0;
        h2.positive_buckets = vec![7.0, 5.0, 1.0, 3.0, 1.0, 0.0, 2.0, 5.0, 5.0, 0.0, 0.0];
        assert!(!c.appender().unwrap().appendable(&h2).4);
        assert_new_chunk(
            &mut c,
            ts + 1,
            &h2,
            CounterResetHeader::CounterReset,
            CounterResetHint::UnknownCounterReset,
        );
    }
    {
        let (mut c, ts, h1) = setup(&eh);
        let mut h2 = h1.clone();
        h2.positive_spans = vec![
            HistogramSpan { offset: -3, length: 2 },
            HistogramSpan { offset: 1, length: 2 },
            HistogramSpan { offset: 2, length: 1 },
            HistogramSpan { offset: 3, length: 2 },
            HistogramSpan { offset: 3, length: 1 },
            HistogramSpan { offset: 1, length: 1 },
        ];
        h2.sum = 26.0;
        h2.positive_buckets = vec![1.0, 2.0, 5.0, 3.0, 3.0, 2.0, 4.0, 5.0, 1.0];
        assert!(!c.appender().unwrap().appendable(&h2).4);
        assert_new_chunk(
            &mut c,
            ts + 1,
            &h2,
            CounterResetHeader::CounterReset,
            CounterResetHint::UnknownCounterReset,
        );
    }
    {
        let (mut c, ts, h1) = setup(&eh);
        let mut h2 = h1.clone();
        h2.counter_reset_hint = CounterResetHint::CounterReset;
        assert_new_chunk(
            &mut c,
            ts + 1,
            &h2,
            CounterResetHeader::CounterReset,
            CounterResetHint::UnknownCounterReset,
        );
    }

    // Start new chunk explicitly with prev state (appendable, not appendable, needs recode).
    {
        let (mut c, ts, h1) = setup(&eh);
        let h_app_state = c.appender().unwrap().into_state();
        let mut next_chunk = FloatHistogramChunk::new();
        let out = next_chunk
            .appender()
            .unwrap()
            .append_float_histogram(Some(&h_app_state), 0, ts + 1, &h1, false)
            .unwrap();
        assert!(matches!(
            out,
            AppendFloatHistogramOutcome::Appended { recoded: false }
        ));
        assert_fhist_sample_count(&next_chunk, 1);
        assert_eq!(next_chunk.counter_reset_header(), CounterResetHeader::NotCounterReset);
        assert_first_float_histogram_sample_hint(
            &next_chunk,
            CounterResetHint::UnknownCounterReset,
        );
    }
    {
        let (mut c, ts, h1) = setup(&eh);
        let h_app_state = c.appender().unwrap().into_state();
        let mut h2 = h1.clone();
        h2.count -= 1.0;
        let mut next_chunk = FloatHistogramChunk::new();
        let out = next_chunk
            .appender()
            .unwrap()
            .append_float_histogram(Some(&h_app_state), 0, ts + 1, &h2, false)
            .unwrap();
        assert!(matches!(
            out,
            AppendFloatHistogramOutcome::Appended { recoded: false }
        ));
        assert_fhist_sample_count(&next_chunk, 1);
        assert_eq!(next_chunk.counter_reset_header(), CounterResetHeader::CounterReset);
        assert_first_float_histogram_sample_hint(
            &next_chunk,
            CounterResetHint::UnknownCounterReset,
        );
    }
    {
        let (mut c, ts, h1) = setup(&eh);
        let h_app_state = c.appender().unwrap().into_state();
        let mut h2 = h1.clone();
        h2.positive_spans = vec![
            HistogramSpan { offset: 0, length: 3 },
            HistogramSpan { offset: 1, length: 1 },
            HistogramSpan { offset: 1, length: 4 },
            HistogramSpan { offset: 3, length: 3 },
        ];
        h2.count += 9.0;
        h2.zero_count += 1.0;
        h2.sum = 30.0;
        h2.positive_buckets = vec![7.0, 5.0, 1.0, 3.0, 1.0, 0.0, 2.0, 5.0, 5.0, 0.0, 1.0];
        let mut next_chunk = FloatHistogramChunk::new();
        let out = next_chunk
            .appender()
            .unwrap()
            .append_float_histogram(Some(&h_app_state), 0, ts + 1, &h2, false)
            .unwrap();
        assert!(matches!(
            out,
            AppendFloatHistogramOutcome::Appended { recoded: false }
        ));
        assert_fhist_sample_count(&next_chunk, 1);
        assert_eq!(next_chunk.counter_reset_header(), CounterResetHeader::NotCounterReset);
        assert_first_float_histogram_sample_hint(
            &next_chunk,
            CounterResetHint::UnknownCounterReset,
        );
    }

    // Empty bucket at start, next missing it.
    {
        let mut h1 = eh.clone();
        h1.positive_spans = Vec::with_capacity(eh.positive_spans.len() + 1);
        h1.positive_spans.push(HistogramSpan {
            offset: eh.positive_spans[0].offset - 10,
            length: 1,
        });
        h1.positive_spans.push(HistogramSpan {
            offset: eh.positive_spans[0].offset + 9,
            length: eh.positive_spans[0].length,
        });
        h1.positive_spans.extend_from_slice(&eh.positive_spans[1..]);
        h1.positive_buckets = vec![0.0];
        h1.positive_buckets.extend_from_slice(&eh.positive_buckets);
        let (mut c, ts, _) = setup(&h1);
        let h2 = eh.clone();
        let (p_i, n_i, p_bi, n_bi, ok, cr) = c.appender().unwrap().appendable(&h2);
        assert!(p_i.is_empty() && n_i.is_empty() && !p_bi.is_empty() && n_bi.is_empty() && ok && !cr);
        assert_no_new_chunk(&mut c, ts + 1, &h2, CounterResetHeader::UnknownCounterReset);
    }

    // Custom buckets.
    {
        let (mut c, ts, h1) = setup(&cbh);
        assert!(c.appender().unwrap().appendable(&h1).4);
        assert_no_new_chunk(&mut c, ts + 1, &h1, CounterResetHeader::UnknownCounterReset);
    }
    {
        let (mut c, ts, h1) = setup(&cbh);
        let mut h2 = h1.clone();
        h2.count += 1.0;
        h2.positive_buckets = vec![6.0, 3.0, 3.0, 2.0, 4.0, 5.0, 2.0];
        assert!(c.appender().unwrap().appendable(&h2).4);
        assert_no_new_chunk(&mut c, ts + 1, &h2, CounterResetHeader::UnknownCounterReset);
    }
    {
        let (mut c, ts, h1) = setup(&cbh);
        let mut h2 = h1.clone();
        h2.count -= 1.0;
        h2.positive_buckets = vec![6.0, 3.0, 3.0, 2.0, 4.0, 5.0, 0.0];
        assert!(!c.appender().unwrap().appendable(&h2).4);
        assert_new_chunk(
            &mut c,
            ts + 1,
            &h2,
            CounterResetHeader::CounterReset,
            CounterResetHint::UnknownCounterReset,
        );
    }
    {
        let (mut c, ts, h1) = setup(&cbh);
        let mut h2 = h1.clone();
        h2.custom_values = (1..=21).map(|i| i as f64).collect();
        assert!(!c.appender().unwrap().appendable(&h2).4);
        assert_new_chunk(
            &mut c,
            ts + 1,
            &h2,
            CounterResetHeader::CounterReset,
            CounterResetHint::UnknownCounterReset,
        );
    }
    {
        let (mut c, ts, h1) = setup(&cbh);
        let mut h2 = h1.clone();
        h2.positive_spans = vec![
            HistogramSpan { offset: 0, length: 3 },
            HistogramSpan { offset: 1, length: 1 },
            HistogramSpan { offset: 1, length: 4 },
            HistogramSpan { offset: 3, length: 3 },
        ];
        h2.count += 6.0;
        h2.sum = 30.0;
        h2.positive_buckets = vec![7.0, 5.0, 1.0, 3.0, 1.0, 0.0, 2.0, 5.0, 5.0, 0.0, 1.0];
        assert!(c.appender().unwrap().appendable(&h2).4);
        assert_recoded_chunk(&mut c, ts + 1, &h2, CounterResetHeader::UnknownCounterReset);
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/float_histogram_test.go::TestFloatHistogramChunkAppendableWithEmptySpan
#[test]
fn test_float_histogram_chunk_appendable_with_empty_span() {
    let base_neg = vec![
        HistogramSpan { offset: 1, length: 4 },
        HistogramSpan { offset: 2, length: 0 },
        HistogramSpan { offset: 2, length: 3 },
    ];
    let tests = [
        (
            FloatHistogram {
                schema: 0,
                count: 21.0,
                sum: 1234.5,
                zero_threshold: 0.001,
                zero_count: 4.0,
                positive_spans: vec![
                    HistogramSpan { offset: 0, length: 4 },
                    HistogramSpan { offset: 0, length: 0 },
                    HistogramSpan { offset: 0, length: 3 },
                ],
                positive_buckets: vec![1.0, 2.0, 1.0, 1.0, 1.0, 1.0, 1.0],
                negative_spans: base_neg.clone(),
                negative_buckets: vec![1.0, 2.0, 1.0, 2.0, 2.0, 2.0, 2.0],
                ..Default::default()
            },
            FloatHistogram {
                schema: 0,
                count: 37.0,
                sum: 2345.6,
                zero_threshold: 0.001,
                zero_count: 5.0,
                positive_spans: vec![
                    HistogramSpan { offset: 0, length: 4 },
                    HistogramSpan { offset: 0, length: 0 },
                    HistogramSpan { offset: 0, length: 3 },
                ],
                positive_buckets: vec![1.0, 3.0, 1.0, 2.0, 1.0, 1.0, 1.0],
                negative_spans: base_neg.clone(),
                negative_buckets: vec![1.0, 4.0, 2.0, 7.0, 5.0, 5.0, 2.0],
                ..Default::default()
            },
        ),
        (
            FloatHistogram {
                schema: 0,
                count: 21.0,
                sum: 1234.5,
                zero_threshold: 0.001,
                zero_count: 4.0,
                positive_spans: vec![
                    HistogramSpan { offset: 1, length: 0 },
                    HistogramSpan { offset: 2, length: 4 },
                    HistogramSpan { offset: 0, length: 3 },
                ],
                positive_buckets: vec![1.0, 2.0, 1.0, 1.0, 1.0, 1.0, 1.0],
                negative_spans: base_neg.clone(),
                negative_buckets: vec![1.0, 2.0, 1.0, 2.0, 2.0, 2.0, 2.0],
                ..Default::default()
            },
            FloatHistogram {
                schema: 0,
                count: 37.0,
                sum: 2345.6,
                zero_threshold: 0.001,
                zero_count: 5.0,
                positive_spans: vec![
                    HistogramSpan { offset: 3, length: 4 },
                    HistogramSpan { offset: 0, length: 3 },
                ],
                positive_buckets: vec![1.0, 3.0, 1.0, 2.0, 1.0, 1.0, 1.0],
                negative_spans: base_neg.clone(),
                negative_buckets: vec![1.0, 4.0, 2.0, 7.0, 5.0, 5.0, 2.0],
                ..Default::default()
            },
        ),
        (
            FloatHistogram {
                schema: 0,
                count: 21.0,
                sum: 1234.5,
                zero_threshold: 0.001,
                zero_count: 4.0,
                positive_spans: vec![
                    HistogramSpan { offset: 0, length: 4 },
                    HistogramSpan { offset: 3, length: 3 },
                ],
                positive_buckets: vec![1.0, 2.0, 1.0, 1.0, 1.0, 1.0, 1.0],
                negative_spans: base_neg.clone(),
                negative_buckets: vec![1.0, 2.0, 1.0, 2.0, 2.0, 2.0, 2.0],
                ..Default::default()
            },
            FloatHistogram {
                schema: 0,
                count: 37.0,
                sum: 2345.6,
                zero_threshold: 0.001,
                zero_count: 5.0,
                positive_spans: vec![
                    HistogramSpan { offset: 0, length: 4 },
                    HistogramSpan { offset: 1, length: 0 },
                    HistogramSpan { offset: 2, length: 3 },
                ],
                positive_buckets: vec![1.0, 3.0, 1.0, 2.0, 1.0, 1.0, 1.0],
                negative_spans: base_neg.clone(),
                negative_buckets: vec![1.0, 4.0, 2.0, 7.0, 5.0, 5.0, 2.0],
                ..Default::default()
            },
        ),
        (
            FloatHistogram {
                schema: 0,
                count: 21.0,
                sum: 1234.5,
                zero_threshold: 0.001,
                zero_count: 4.0,
                positive_spans: vec![
                    HistogramSpan { offset: 0, length: 4 },
                    HistogramSpan { offset: 1, length: 0 },
                    HistogramSpan { offset: 3, length: 0 },
                    HistogramSpan { offset: 4, length: 3 },
                ],
                positive_buckets: vec![1.0, 2.0, 1.0, 1.0, 1.0, 1.0, 1.0],
                negative_spans: base_neg.clone(),
                negative_buckets: vec![1.0, 2.0, 1.0, 2.0, 2.0, 2.0, 2.0],
                ..Default::default()
            },
            FloatHistogram {
                schema: 0,
                count: 37.0,
                sum: 2345.6,
                zero_threshold: 0.001,
                zero_count: 5.0,
                positive_spans: vec![
                    HistogramSpan { offset: 0, length: 4 },
                    HistogramSpan { offset: 3, length: 0 },
                    HistogramSpan { offset: 1, length: 0 },
                    HistogramSpan { offset: 4, length: 3 },
                ],
                positive_buckets: vec![1.0, 3.0, 1.0, 2.0, 1.0, 1.0, 1.0],
                negative_spans: base_neg,
                negative_buckets: vec![1.0, 4.0, 2.0, 7.0, 5.0, 5.0, 2.0],
                ..Default::default()
            },
        ),
        (
            FloatHistogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 7.0,
                sum: 1234.5,
                positive_spans: vec![
                    HistogramSpan { offset: 0, length: 4 },
                    HistogramSpan { offset: 0, length: 0 },
                    HistogramSpan { offset: 0, length: 3 },
                ],
                positive_buckets: vec![1.0, 2.0, 1.0, 1.0, 1.0, 1.0, 1.0],
                custom_values: (1..=10).map(|i| i as f64).collect(),
                ..Default::default()
            },
            FloatHistogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 10.0,
                sum: 2345.6,
                positive_spans: vec![
                    HistogramSpan { offset: 0, length: 4 },
                    HistogramSpan { offset: 0, length: 0 },
                    HistogramSpan { offset: 0, length: 3 },
                ],
                positive_buckets: vec![1.0, 3.0, 1.0, 2.0, 1.0, 1.0, 1.0],
                custom_values: (1..=10).map(|i| i as f64).collect(),
                ..Default::default()
            },
        ),
    ];

    for (h1, h2) in tests {
        let mut c = FloatHistogramChunk::new();
        {
            let mut app = c.appender().unwrap();
            app.append_float_histogram(None, 0, 1, &h1, true).unwrap();
            let (p_i, n_i, bp_i, bn_i, ok, cr) = app.appendable(&h2);
            assert!(p_i.is_empty() && n_i.is_empty() && bp_i.is_empty() && bn_i.is_empty());
            assert!(ok);
            assert!(!cr);
        }
        assert_eq!(c.num_samples(), 1);
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/float_histogram_test.go::TestFloatHistogramChunkAppendableGauge
#[test]
fn test_float_histogram_chunk_appendable_gauge() {
    let eh = FloatHistogram {
        counter_reset_hint: CounterResetHint::GaugeType,
        count: 5.0,
        zero_count: 2.0,
        sum: 18.4,
        zero_threshold: 1e-125,
        schema: 1,
        positive_spans: vec![
            HistogramSpan { offset: 0, length: 2 },
            HistogramSpan { offset: 2, length: 1 },
            HistogramSpan { offset: 3, length: 2 },
            HistogramSpan { offset: 3, length: 1 },
            HistogramSpan { offset: 1, length: 1 },
        ],
        positive_buckets: vec![6.0, 3.0, 3.0, 2.0, 4.0, 5.0, 1.0],
        ..Default::default()
    };

    let cbh = FloatHistogram {
        counter_reset_hint: CounterResetHint::GaugeType,
        count: 24.0,
        sum: 18.4,
        schema: CUSTOM_BUCKETS_SCHEMA,
        positive_spans: vec![
            HistogramSpan { offset: 0, length: 2 },
            HistogramSpan { offset: 2, length: 1 },
            HistogramSpan { offset: 3, length: 2 },
            HistogramSpan { offset: 3, length: 1 },
            HistogramSpan { offset: 1, length: 1 },
        ],
        positive_buckets: vec![6.0, 3.0, 3.0, 2.0, 4.0, 5.0, 1.0],
        custom_values: (1..=20).map(|i| i as f64).collect(),
        ..Default::default()
    };

    let setup = |h: &FloatHistogram| -> (FloatHistogramChunk, i64, FloatHistogram) {
        let mut c = FloatHistogramChunk::new();
        let ts = 1_234_567_890i64;
        let out = c
            .appender()
            .unwrap()
            .append_float_histogram(None, 0, ts, h, false)
            .unwrap();
        assert!(matches!(out, AppendFloatHistogramOutcome::Appended { .. }));
        assert_eq!(c.counter_reset_header(), CounterResetHeader::GaugeType);
        (c, ts, h.clone())
    };

    let assert_new_gauge = |old: &mut FloatHistogramChunk, ts: i64, h: &FloatHistogram| {
        let out = old
            .appender()
            .unwrap()
            .append_float_histogram(None, 0, ts, h, false)
            .unwrap();
        let AppendFloatHistogramOutcome::NewChunk(nc) = out else {
            panic!("expected new chunk");
        };
        assert_eq!(nc.counter_reset_header(), CounterResetHeader::GaugeType);
        assert_fhist_sample_count(&nc, 1);
        assert_first_float_histogram_sample_hint(&nc, CounterResetHint::GaugeType);
    };

    let assert_no_new_gauge = |curr: &mut FloatHistogramChunk, ts: i64, h: &FloatHistogram| {
        let out = curr
            .appender()
            .unwrap()
            .append_float_histogram(None, 0, ts, h, false)
            .unwrap();
        assert!(matches!(
            out,
            AppendFloatHistogramOutcome::Appended { recoded: false }
        ));
        assert_eq!(curr.counter_reset_header(), CounterResetHeader::GaugeType);
        assert_fhist_sample_count(curr, 2);
    };

    let assert_recoded_gauge = |curr: &mut FloatHistogramChunk, ts: i64, h: &FloatHistogram| {
        let out = curr
            .appender()
            .unwrap()
            .append_float_histogram(None, 0, ts, h, false)
            .unwrap();
        match out {
            AppendFloatHistogramOutcome::Appended { recoded } => {
                assert!(recoded);
                assert_eq!(curr.counter_reset_header(), CounterResetHeader::GaugeType);
                assert_fhist_sample_count(curr, 2);
            }
            AppendFloatHistogramOutcome::NewChunk(nc) => {
                assert_eq!(nc.counter_reset_header(), CounterResetHeader::GaugeType);
                assert_fhist_sample_count(&nc, 2);
            }
        }
    };

    // Schema / zero threshold.
    {
        let (mut c, ts, h1) = setup(&eh);
        let mut h2 = h1.clone();
        h2.schema += 1;
        assert!(!c.appender().unwrap().appendable_gauge(&h2).ok_to_append);
        assert_new_gauge(&mut c, ts + 1, &h2);
    }
    {
        let (mut c, ts, h1) = setup(&eh);
        let mut h2 = h1.clone();
        h2.zero_threshold += 0.1;
        assert!(!c.appender().unwrap().appendable_gauge(&h2).ok_to_append);
        assert_new_gauge(&mut c, ts + 1, &h2);
    }

    // More buckets, missing buckets, missing+new, counter reset same/new/before, custom buckets.
    {
        let (mut c, ts, h1) = setup(&eh);
        let mut h2 = h1.clone();
        h2.positive_spans = vec![
            HistogramSpan { offset: 0, length: 3 },
            HistogramSpan { offset: 1, length: 1 },
            HistogramSpan { offset: 1, length: 4 },
            HistogramSpan { offset: 3, length: 3 },
        ];
        h2.count += 9.0;
        h2.zero_count += 1.0;
        h2.sum = 30.0;
        h2.positive_buckets = vec![7.0, 5.0, 1.0, 3.0, 1.0, 0.0, 2.0, 5.0, 5.0, 0.0, 1.0];
        let g = c.appender().unwrap().appendable_gauge(&h2);
        assert!(!g.p_fwd.is_empty() && g.n_fwd.is_empty() && g.p_bwd.is_empty() && g.n_bwd.is_empty() && g.ok_to_append);
        assert_recoded_gauge(&mut c, ts + 1, &h2);
    }
    {
        let (mut c, ts, h1) = setup(&eh);
        let mut h2 = h1.clone();
        h2.positive_spans = vec![
            HistogramSpan { offset: 0, length: 2 },
            HistogramSpan { offset: 2, length: 1 },
            HistogramSpan { offset: 3, length: 1 },
            HistogramSpan { offset: 4, length: 1 },
            HistogramSpan { offset: 1, length: 1 },
        ];
        h2.count -= 4.0;
        h2.sum -= 1.0;
        h2.positive_buckets = vec![6.0, 3.0, 3.0, 2.0, 5.0, 1.0];
        let g = c.appender().unwrap().appendable_gauge(&h2);
        assert!(g.p_fwd.is_empty() && g.n_fwd.is_empty() && !g.p_bwd.is_empty() && g.n_bwd.is_empty() && g.ok_to_append);
        assert_no_new_gauge(&mut c, ts + 1, &h2);
    }
    {
        let (mut c, ts, h1) = setup(&eh);
        let mut h2 = h1.clone();
        h2.positive_spans = vec![
            HistogramSpan { offset: 0, length: 2 },
            HistogramSpan { offset: 5, length: 2 },
            HistogramSpan { offset: 3, length: 1 },
            HistogramSpan { offset: 1, length: 1 },
        ];
        h2.sum = 21.0;
        h2.positive_buckets = vec![6.0, 3.0, 2.0, 4.0, 5.0, 1.0];
        let g = c.appender().unwrap().appendable_gauge(&h2);
        assert!(!g.p_fwd.is_empty() && !g.p_bwd.is_empty() && g.n_fwd.is_empty() && g.n_bwd.is_empty() && g.ok_to_append);
        assert_recoded_gauge(&mut c, ts + 1, &h2);
    }
    {
        let (mut c, ts, h1) = setup(&eh);
        let mut h2 = h1.clone();
        h2.sum = 23.0;
        h2.positive_buckets = vec![6.0, 2.0, 3.0, 2.0, 4.0, 5.0, 1.0];
        assert!(c.appender().unwrap().appendable_gauge(&h2).ok_to_append);
        assert_no_new_gauge(&mut c, ts + 1, &h2);
    }
    {
        let (mut c, ts, h1) = setup(&eh);
        let mut h2 = h1.clone();
        h2.positive_spans = vec![
            HistogramSpan { offset: 0, length: 3 },
            HistogramSpan { offset: 1, length: 1 },
            HistogramSpan { offset: 1, length: 4 },
            HistogramSpan { offset: 3, length: 3 },
        ];
        h2.sum = 29.0;
        h2.positive_buckets = vec![7.0, 5.0, 1.0, 3.0, 1.0, 0.0, 2.0, 5.0, 5.0, 0.0, 0.0];
        assert!(c.appender().unwrap().appendable_gauge(&h2).ok_to_append);
        assert_recoded_gauge(&mut c, ts + 1, &h2);
    }
    {
        let (mut c, ts, h1) = setup(&eh);
        let mut h2 = h1.clone();
        h2.positive_spans = vec![
            HistogramSpan { offset: -3, length: 2 },
            HistogramSpan { offset: 1, length: 2 },
            HistogramSpan { offset: 2, length: 1 },
            HistogramSpan { offset: 3, length: 2 },
            HistogramSpan { offset: 3, length: 1 },
            HistogramSpan { offset: 1, length: 1 },
        ];
        h2.sum = 26.0;
        h2.positive_buckets = vec![1.0, 2.0, 5.0, 3.0, 3.0, 2.0, 4.0, 5.0, 1.0];
        assert!(c.appender().unwrap().appendable_gauge(&h2).ok_to_append);
        assert_recoded_gauge(&mut c, ts + 1, &h2);
    }
    {
        let (mut c, ts, h1) = setup(&cbh);
        assert!(c.appender().unwrap().appendable_gauge(&h1).ok_to_append);
        assert_no_new_gauge(&mut c, ts + 1, &h1);
    }
    {
        let (mut c, ts, h1) = setup(&cbh);
        let mut h2 = h1.clone();
        h2.count += 1.0;
        h2.positive_buckets = vec![6.0, 3.0, 3.0, 2.0, 4.0, 5.0, 2.0];
        assert!(c.appender().unwrap().appendable_gauge(&h2).ok_to_append);
        assert_no_new_gauge(&mut c, ts + 1, &h2);
    }
    {
        let (mut c, ts, h1) = setup(&cbh);
        let mut h2 = h1.clone();
        h2.count -= 1.0;
        h2.positive_buckets = vec![6.0, 3.0, 3.0, 2.0, 4.0, 5.0, 0.0];
        assert!(c.appender().unwrap().appendable_gauge(&h2).ok_to_append);
        assert_no_new_gauge(&mut c, ts + 1, &h2);
    }
    {
        let (mut c, ts, h1) = setup(&cbh);
        let mut h2 = h1.clone();
        h2.custom_values = (1..=21).map(|i| i as f64).collect();
        assert!(!c.appender().unwrap().appendable_gauge(&h2).ok_to_append);
        assert_new_gauge(&mut c, ts + 1, &h2);
    }
    {
        let (mut c, ts, h1) = setup(&cbh);
        let mut h2 = h1.clone();
        h2.positive_spans = vec![
            HistogramSpan { offset: 0, length: 3 },
            HistogramSpan { offset: 1, length: 1 },
            HistogramSpan { offset: 1, length: 4 },
            HistogramSpan { offset: 3, length: 3 },
        ];
        h2.count += 6.0;
        h2.sum = 30.0;
        h2.positive_buckets = vec![7.0, 5.0, 1.0, 3.0, 1.0, 0.0, 2.0, 5.0, 5.0, 0.0, 1.0];
        assert!(c.appender().unwrap().appendable_gauge(&h2).ok_to_append);
        assert_recoded_gauge(&mut c, ts + 1, &h2);
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/float_histogram_test.go::TestFloatHistogramAppendOnlyErrors
#[test]
fn test_float_histogram_append_only_errors() {
    {
        let mut c = FloatHistogramChunk::new();
        let h = generate_test_float_histogram(0);
        let mut app = c.appender().unwrap();
        app.append_float_histogram(None, 0, 1, &h, true).unwrap();
        let mut h2 = h.clone();
        h2.schema += 1;
        let err = app.append_float_histogram(None, 0, 2, &h2, true).unwrap_err();
        assert!(matches!(err, ChunkError::FloatHistogramSchemaChange));
    }
    {
        let mut c = FloatHistogramChunk::new();
        let h = generate_test_float_histogram(0);
        let mut app = c.appender().unwrap();
        app.append_float_histogram(None, 0, 1, &h, true).unwrap();
        let mut h2 = h.clone();
        h2.counter_reset_hint = CounterResetHint::CounterReset;
        let err = app.append_float_histogram(None, 0, 2, &h2, true).unwrap_err();
        assert!(matches!(err, ChunkError::FloatHistogramCounterReset));
    }
    {
        let mut c = FloatHistogramChunk::new();
        let h = generate_test_custom_buckets_float_histogram(0);
        let mut app = c.appender().unwrap();
        app.append_float_histogram(None, 0, 1, &h, true).unwrap();
        let mut h2 = h.clone();
        h2.custom_values = vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0];
        let err = app.append_float_histogram(None, 0, 2, &h2, true).unwrap_err();
        assert!(matches!(err, ChunkError::FloatHistogramCounterReset));
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/float_histogram_test.go::TestFloatHistogramUniqueSpansAfterNext
#[test]
fn test_float_histogram_unique_spans_after_next() {
    let h1 = FloatHistogram {
        schema: 1,
        zero_threshold: 1e-100,
        count: 10.0,
        zero_count: 2.0,
        sum: 15.0,
        positive_spans: vec![
            HistogramSpan { offset: 0, length: 2 },
            HistogramSpan { offset: 1, length: 2 },
        ],
        positive_buckets: vec![1.0, 2.0, 3.0, 4.0],
        negative_spans: vec![HistogramSpan { offset: 1, length: 1 }],
        negative_buckets: vec![2.0],
        ..Default::default()
    };
    let h2 = h1.clone();

    let mut c = FloatHistogramChunk::new();
    {
        let mut app = c.appender().unwrap();
        app.append_float_histogram(None, 0, 0, &h1, false).unwrap();
        app.append_float_histogram(None, 0, 1, &h2, false).unwrap();
    }

    let mut it = c.iter();
    assert!(it.next_sample().unwrap().is_some());
    let (_, rh1) = it.at_float_histogram();
    assert!(it.next_sample().unwrap().is_some());
    let (_, rh2) = it.at_float_histogram();

    assert_eq!(rh1.positive_spans, h1.positive_spans);
    assert_eq!(rh1.negative_spans, h1.negative_spans);
    assert_eq!(rh2.positive_spans, h1.positive_spans);
    assert_eq!(rh2.negative_spans, h1.negative_spans);
    assert_ne!(rh1.positive_spans.as_ptr(), rh2.positive_spans.as_ptr());
    assert_ne!(rh1.negative_spans.as_ptr(), rh2.negative_spans.as_ptr());
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/float_histogram_test.go::TestFloatHistogramUniqueCustomValuesAfterNext
#[test]
fn test_float_histogram_unique_custom_values_after_next() {
    let h1 = FloatHistogram {
        schema: CUSTOM_BUCKETS_SCHEMA,
        count: 10.0,
        sum: 15.0,
        positive_spans: vec![
            HistogramSpan { offset: 0, length: 2 },
            HistogramSpan { offset: 1, length: 2 },
        ],
        positive_buckets: vec![1.0, 2.0, 3.0, 4.0],
        custom_values: vec![10.0, 11.0, 12.0, 13.0],
        ..Default::default()
    };
    let h2 = h1.clone();

    let mut c = FloatHistogramChunk::new();
    {
        let mut app = c.appender().unwrap();
        app.append_float_histogram(None, 0, 0, &h1, false).unwrap();
        app.append_float_histogram(None, 0, 1, &h2, false).unwrap();
    }

    let mut it = c.iter();
    assert!(it.next_sample().unwrap().is_some());
    let (_, rh1) = it.at_float_histogram();
    assert!(it.next_sample().unwrap().is_some());
    let (_, rh2) = it.at_float_histogram();

    assert_eq!(rh1.positive_spans, h1.positive_spans);
    assert_eq!(rh1.custom_values, h1.custom_values);
    assert_eq!(rh2.positive_spans, h1.positive_spans);
    assert_eq!(rh2.custom_values, h1.custom_values);
    assert_ne!(rh1.positive_spans.as_ptr(), rh2.positive_spans.as_ptr());
    assert_ne!(rh1.custom_values.as_ptr(), rh2.custom_values.as_ptr());
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/float_histogram_test.go::TestFloatHistogramEmptyBucketsWithGaps
#[test]
fn test_float_histogram_empty_buckets_with_gaps() {
    let h1 = FloatHistogram {
        positive_spans: vec![
            HistogramSpan { offset: -19, length: 2 },
            HistogramSpan { offset: 1, length: 2 },
        ],
        positive_buckets: vec![0.0, 0.0, 0.0, 0.0],
        ..Default::default()
    };
    h1.validate().unwrap();

    let mut c = FloatHistogramChunk::new();
    {
        let mut app = c.appender().unwrap();
        app.append_float_histogram(None, 0, 1, &h1, false).unwrap();
        let h2 = FloatHistogram {
            positive_spans: vec![
                HistogramSpan { offset: -19, length: 1 },
                HistogramSpan { offset: 4, length: 1 },
                HistogramSpan { offset: 3, length: 1 },
            ],
            positive_buckets: vec![0.0, 0.0, 0.0],
            ..Default::default()
        };
        h2.validate().unwrap();

        match app.append_float_histogram(None, 0, 2, &h2, false).unwrap() {
            AppendFloatHistogramOutcome::Appended { recoded } => assert!(recoded),
            AppendFloatHistogramOutcome::NewChunk(nc) => c = nc,
        }
    }

    let mut it = c.iter();
    assert!(it.next_sample().unwrap().is_some());
    it.at_float_histogram().1.validate().unwrap();
    assert!(it.next_sample().unwrap().is_some());
    it.at_float_histogram().1.validate().unwrap();
    assert!(it.next_sample().unwrap().is_none());
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/float_histogram_test.go::TestFloatHistogramIteratorFailIfSchemaInValid
#[test]
fn test_float_histogram_iterator_fail_if_schema_invalid() {
    for schema in [-101i32, 101] {
        let h = FloatHistogram {
            schema,
            count: 10.0,
            sum: 15.0,
            zero_threshold: 1e-100,
            positive_spans: vec![
                HistogramSpan { offset: 0, length: 2 },
                HistogramSpan { offset: 1, length: 2 },
            ],
            positive_buckets: vec![1.0, 2.0, 3.0, 4.0],
            ..Default::default()
        };
        let mut c = FloatHistogramChunk::new();
        c.appender()
            .unwrap()
            .append_float_histogram(None, 0, 1, &h, false)
            .unwrap();
        let mut it = c.iter();
        let err = it.next_sample().unwrap_err();
        assert!(matches!(err, ChunkError::UnknownSchema(s) if s == schema));
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/float_histogram_test.go::TestFloatHistogramIteratorReduceSchema
#[test]
fn test_float_histogram_iterator_reduce_schema() {
    for schema in [9i32, 52] {
        let h = FloatHistogram {
            schema,
            count: 10.0,
            sum: 15.0,
            zero_threshold: 1e-100,
            positive_spans: vec![
                HistogramSpan { offset: 0, length: 2 },
                HistogramSpan { offset: 1, length: 2 },
            ],
            positive_buckets: vec![1.0, 2.0, 3.0, 4.0],
            ..Default::default()
        };
        let mut c = FloatHistogramChunk::new();
        c.appender()
            .unwrap()
            .append_float_histogram(None, 0, 1, &h, false)
            .unwrap();
        let mut it = c.iter();
        assert!(it.next_sample().unwrap().is_some());
        let (_, rh) = it.at_float_histogram();
        assert_eq!(rh.schema, EXPONENTIAL_SCHEMA_MAX);
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/float_histogram_test.go::TestFloatHistogramChunkOverFlowPanics
#[test]
fn test_float_histogram_chunk_overflow_panics() {
    let mut chunk = FloatHistogramChunk::new();
    let mut app = chunk.appender().unwrap();
    let mut fh = FloatHistogram {
        zero_threshold: 1e-128,
        ..Default::default()
    };
    for i in 0..(u16::MAX as u64) {
        fh.count = i as f64;
        fh.zero_count = i as f64;
        let _ = app
            .append_float_histogram(None, 0, i as i64, &fh, true)
            .unwrap();
    }
    fh.count = u16::MAX as f64;
    fh.zero_count = u16::MAX as f64;
    let err = app
        .append_float_histogram(None, 0, u16::MAX as i64, &fh, true)
        .unwrap_err();
    assert!(matches!(err, ChunkError::CapacityExceeded));
}
