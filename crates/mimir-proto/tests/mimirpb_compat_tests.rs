use std::path::PathBuf;
use std::sync::Mutex;

use mimir_proto::compat::{
    are_label_names_sorted_and_unique, compare_label_adapters, copy_labels,
    float_histogram_from_prometheus_model, float_histogram_to_prometheus_model,
    from_float_histogram_proto_to_float_histogram, from_float_histogram_to_histogram_proto,
    from_float_histogram_to_prom_histogram, from_float_samples_to_fpoints,
    from_fpoints_to_float_samples, from_histogram_proto_to_histogram,
    from_histogram_to_histogram_proto, from_histograms_to_hpoints, from_hpoints_to_histograms,
    from_label_adapters_to_labels, from_label_adapters_to_labels_with_copy,
    marshal_float_sample_json, marshal_sample_histogram_pair_json,
    metric_metadata_metric_type_to_metric_type, non_stable_hash, set_json_testing_mode,
    unmarshal_float_sample_json, unmarshal_sample_histogram_pair_json, CounterResetHint,
    LabelAdapter, ModelMetricType, PreallocatingMetric, PromFPoint, PromFloatHistogram,
    PromHPoint, PromHistogram, PromHistogramBucket, PromLabel, PromLabels, PromSampleHistogram,
    PromSpan, NON_STABLE_HASH_FAST_PATH_CAP,
};
use mimir_proto::cortexpb;
use mimir_proto::grpcutilpb::{
    google_rpc, status_with_cortex_error_details, status_with_http_response,
};
use mimir_proto::httpgrpcpb;
use mimir_proto::symbols::FastSymbolsTable;
use mimir_proto::timeseries::PreallocWriteRequest;
use prost::Message as _;
use xxhash_rust::xxh64::xxh64;

static JSON_TESTING_LOCK: Mutex<()> = Mutex::new(());

fn generate_test_histogram(i: i64) -> PromHistogram {
    PromHistogram {
        count: 12 + (i * 9) as u64,
        zero_count: 2 + i as u64,
        zero_threshold: 0.001,
        sum: 18.4 * ((i + 1) as f64),
        schema: 1,
        positive_spans: vec![
            PromSpan {
                offset: 0,
                length: 2,
            },
            PromSpan {
                offset: 1,
                length: 2,
            },
        ],
        positive_buckets: vec![i + 1, 1, -1, 0],
        negative_spans: vec![
            PromSpan {
                offset: 0,
                length: 2,
            },
            PromSpan {
                offset: 1,
                length: 2,
            },
        ],
        negative_buckets: vec![i + 1, 1, -1, 0],
        custom_values: Vec::new(),
        counter_reset_hint: 0,
    }
}

fn generate_test_gauge_histogram(i: i64) -> PromHistogram {
    let mut h = generate_test_histogram(i);
    h.counter_reset_hint = CounterResetHint::GaugeType as i32;
    h
}

fn generate_test_float_histogram(i: i64) -> PromFloatHistogram {
    PromFloatHistogram {
        count: 12.0 + ((i * 9) as f64),
        zero_count: 2.0 + (i as f64),
        zero_threshold: 0.001,
        sum: 18.4 * ((i + 1) as f64),
        schema: 1,
        positive_spans: vec![
            PromSpan {
                offset: 0,
                length: 2,
            },
            PromSpan {
                offset: 1,
                length: 2,
            },
        ],
        positive_buckets: vec![(i + 1) as f64, (i + 2) as f64, (i + 1) as f64, (i + 1) as f64],
        negative_spans: vec![
            PromSpan {
                offset: 0,
                length: 2,
            },
            PromSpan {
                offset: 1,
                length: 2,
            },
        ],
        negative_buckets: vec![(i + 1) as f64, (i + 2) as f64, (i + 1) as f64, (i + 1) as f64],
        custom_values: Vec::new(),
        counter_reset_hint: 0,
    }
}

fn generate_test_gauge_float_histogram(i: i64) -> PromFloatHistogram {
    let mut h = generate_test_float_histogram(i);
    h.counter_reset_hint = CounterResetHint::GaugeType as i32;
    h
}

// Upstream: pkg/mimirpb/compat_test.go::TestJsoniterMarshalForFloatSample
#[test]
fn test_jsoniter_marshal_for_float_sample() {
    let _g = JSON_TESTING_LOCK.lock().unwrap();
    set_json_testing_mode(true);
    let out = marshal_float_sample_json(cortexpb::FloatSample {
        value: 12345.0,
        timestamp_ms: 98765,
    })
    .unwrap();
    assert_eq!(out, r#"[98.765,"12345"]"#);

    let err = marshal_float_sample_json(cortexpb::FloatSample {
        value: f64::NAN,
        timestamp_ms: 0,
    })
    .unwrap_err();
    assert_eq!(err, "test sample");

    set_json_testing_mode(false);
    let out = marshal_float_sample_json(cortexpb::FloatSample {
        value: f64::NAN,
        timestamp_ms: 0,
    })
    .unwrap();
    assert_eq!(out, r#"[0,"NaN"]"#);
}

// Upstream: pkg/mimirpb/compat_test.go::TestStdlibJsonMarshalForFloatSample
#[test]
fn test_stdlib_json_marshal_for_float_sample() {
    let _g = JSON_TESTING_LOCK.lock().unwrap();
    set_json_testing_mode(true);
    let out = marshal_float_sample_json(cortexpb::FloatSample {
        value: 12345.0,
        timestamp_ms: 98765,
    })
    .unwrap();
    assert_eq!(out, r#"[98.765,"12345"]"#);

    let err = marshal_float_sample_json(cortexpb::FloatSample {
        value: f64::NAN,
        timestamp_ms: 0,
    })
    .unwrap_err();
    assert!(err.contains("test sample"));

    set_json_testing_mode(false);
    let out = marshal_float_sample_json(cortexpb::FloatSample {
        value: f64::NAN,
        timestamp_ms: 0,
    })
    .unwrap();
    assert_eq!(out, r#"[0,"NaN"]"#);
}

// Upstream: pkg/mimirpb/compat_test.go::TestJsoniterUnmarshalForFloatSample
#[test]
fn test_jsoniter_unmarshal_for_float_sample() {
    let _g = JSON_TESTING_LOCK.lock().unwrap();
    set_json_testing_mode(true);
    let sample = unmarshal_float_sample_json(br#"[98.765,"12345"]"#).unwrap();
    assert_eq!(
        sample,
        cortexpb::FloatSample {
            value: 12345.0,
            timestamp_ms: 98765,
        }
    );

    let err = unmarshal_float_sample_json(br#"[0.0,"NaN"]"#).unwrap_err();
    assert_eq!(err, "test sample");

    set_json_testing_mode(false);
    let sample = unmarshal_float_sample_json(br#"[0.0,"NaN"]"#).unwrap();
    assert_eq!(sample.timestamp_ms, 0);
    assert!(sample.value.is_nan());
}

// Upstream: pkg/mimirpb/compat_test.go::TestStdlibJsonUnmarshalForFloatSample
#[test]
fn test_stdlib_json_unmarshal_for_float_sample() {
    let _g = JSON_TESTING_LOCK.lock().unwrap();
    set_json_testing_mode(true);
    let sample = unmarshal_float_sample_json(br#"[98.765,"12345"]"#).unwrap();
    assert_eq!(
        sample,
        cortexpb::FloatSample {
            value: 12345.0,
            timestamp_ms: 98765,
        }
    );

    let err = unmarshal_float_sample_json(br#"[0.0,"NaN"]"#).unwrap_err();
    assert_eq!(err, "test sample");

    set_json_testing_mode(false);
    let sample = unmarshal_float_sample_json(br#"[0.0,"NaN"]"#).unwrap();
    assert_eq!(sample.timestamp_ms, 0);
    assert!(sample.value.is_nan());
}

// Upstream: pkg/mimirpb/compat_test.go::TestMarshalSampleHistogramPair
#[test]
fn test_marshal_sample_histogram_pair() {
    let shp = cortexpb::SampleHistogramPair {
        timestamp: 1234,
        histogram: Some(cortexpb::SampleHistogram {
            count: 10.0,
            sum: 18.4,
            buckets: vec![
                cortexpb::HistogramBucket {
                    boundaries: 1,
                    lower: -4.0,
                    upper: -2.82842712474619,
                    count: 1.0,
                },
                cortexpb::HistogramBucket {
                    boundaries: 1,
                    lower: -2.82842712474619,
                    upper: -2.0,
                    count: 1.0,
                },
            ],
        }),
    };
    let expected = r#"[1.234,{"count":"10","sum":"18.4","buckets":[[1,"-4","-2.82842712474619","1"],[1,"-2.82842712474619","-2","1"]]}]"#;
    let data = marshal_sample_histogram_pair_json(&shp).unwrap();
    assert_eq!(data, expected);
}

// Upstream: pkg/mimirpb/compat_test.go::TestUnmarshalSampleHistogramPair
#[test]
fn test_unmarshal_sample_histogram_pair() {
    let plain = br#"[1.234,{"count":"10","sum":"18.4","buckets":[[1,"-4","-2.82842712474619","1"],[1,"-2.82842712474619","-2","1"]]}]"#;
    let expected = cortexpb::SampleHistogramPair {
        timestamp: 1234,
        histogram: Some(cortexpb::SampleHistogram {
            count: 10.0,
            sum: 18.4,
            buckets: vec![
                cortexpb::HistogramBucket {
                    boundaries: 1,
                    lower: -4.0,
                    upper: -2.82842712474619,
                    count: 1.0,
                },
                cortexpb::HistogramBucket {
                    boundaries: 1,
                    lower: -2.82842712474619,
                    upper: -2.0,
                    count: 1.0,
                },
            ],
        }),
    };
    let shp = unmarshal_sample_histogram_pair_json(plain).unwrap();
    assert_eq!(shp, expected);
}

// Upstream: pkg/mimirpb/compat_test.go::TestMetricMetadataToMetricTypeToMetricType
#[test]
fn test_metric_metadata_to_metric_type_to_metric_type() {
    use cortexpb::metric_metadata::MetricType;
    let cases = [
        (MetricType::Counter as i32, ModelMetricType::Counter),
        (MetricType::Stateset as i32, ModelMetricType::Stateset),
        (100, ModelMetricType::Unknown),
    ];
    for (input, expected) in cases {
        assert_eq!(metric_metadata_metric_type_to_metric_type(input), expected);
    }
}

// Upstream: pkg/mimirpb/compat_test.go::TestFromLabelAdaptersToLabels
#[test]
fn test_from_label_adapters_to_labels() {
    let input = vec![LabelAdapter::new("hello", "world")];
    let expected = PromLabels::from_strings(&["hello", "world"]);
    let actual = from_label_adapters_to_labels(&input);
    assert_eq!(actual, expected);
}

// Upstream: pkg/mimirpb/compat_test.go::TestFromLabelAdaptersToLabelsWithCopy
#[test]
fn test_from_label_adapters_to_labels_with_copy() {
    let input = vec![LabelAdapter::new("hello", "world")];
    let expected = PromLabels::from_strings(&["hello", "world"]);
    let actual = from_label_adapters_to_labels_with_copy(&input);
    assert_eq!(actual, expected);
    let actual_val = actual.get("hello").unwrap();
    assert_ne!(input[0].value.as_ptr(), actual_val.as_ptr());
}

// Upstream: pkg/mimirpb/compat_test.go::TestFromFPointsToFloatSamples
#[test]
fn test_from_fpoints_to_float_samples() {
    let input = vec![PromFPoint { t: 1, f: 2.0 }, PromFPoint { t: 3, f: 4.0 }];
    let expected = vec![
        cortexpb::FloatSample {
            timestamp_ms: 1,
            value: 2.0,
        },
        cortexpb::FloatSample {
            timestamp_ms: 3,
            value: 4.0,
        },
    ];
    assert_eq!(from_fpoints_to_float_samples(&input), expected);
}

// Upstream: pkg/mimirpb/compat_test.go::TestFromFloatSamplesToFPoints
#[test]
fn test_from_float_samples_to_fpoints() {
    let input = vec![
        cortexpb::FloatSample {
            timestamp_ms: 1,
            value: 2.0,
        },
        cortexpb::FloatSample {
            timestamp_ms: 3,
            value: 4.0,
        },
    ];
    let expected = vec![PromFPoint { t: 1, f: 2.0 }, PromFPoint { t: 3, f: 4.0 }];
    assert_eq!(from_float_samples_to_fpoints(&input), expected);
}

// Upstream: pkg/mimirpb/compat_test.go::TestPrometheusFPointInSyncWithMimirPbFloatSample
#[test]
fn test_prometheus_fpoint_in_sync_with_mimirpb_float_sample() {
    let fp = PromFPoint { t: 123, f: 45.5 };
    let fs = from_fpoints_to_float_samples(&[fp])[0];
    assert_eq!(fs.timestamp_ms, fp.t);
    assert_eq!(fs.value, fp.f);
    assert_eq!(from_float_samples_to_fpoints(&[fs])[0], fp);
}

// Upstream: pkg/mimirpb/compat_test.go::TestFromHPointsToHistograms
#[test]
fn test_from_hpoints_to_histograms() {
    let input = vec![
        PromHPoint {
            t: 3,
            h: generate_test_float_histogram(0),
        },
        PromHPoint {
            t: 5,
            h: generate_test_float_histogram(1),
        },
    ];
    let expected = vec![
        cortexpb::FloatHistogramPair {
            timestamp_ms: 3,
            histogram: Some(float_histogram_from_prometheus_model(
                &generate_test_float_histogram(0),
            )),
        },
        cortexpb::FloatHistogramPair {
            timestamp_ms: 5,
            histogram: Some(float_histogram_from_prometheus_model(
                &generate_test_float_histogram(1),
            )),
        },
    ];
    assert_eq!(from_hpoints_to_histograms(&input), expected);
}

// Upstream: pkg/mimirpb/compat_test.go::TestFromHistogramsToHPoints
#[test]
fn test_from_histograms_to_hpoints() {
    let input = vec![
        cortexpb::FloatHistogramPair {
            timestamp_ms: 3,
            histogram: Some(float_histogram_from_prometheus_model(
                &generate_test_float_histogram(0),
            )),
        },
        cortexpb::FloatHistogramPair {
            timestamp_ms: 5,
            histogram: Some(float_histogram_from_prometheus_model(
                &generate_test_float_histogram(1),
            )),
        },
    ];
    let expected = vec![
        PromHPoint {
            t: 3,
            h: generate_test_float_histogram(0),
        },
        PromHPoint {
            t: 5,
            h: generate_test_float_histogram(1),
        },
    ];
    assert_eq!(from_histograms_to_hpoints(&input), expected);
}

// Upstream: pkg/mimirpb/compat_test.go::TestPrometheusHPointInSyncWithMimirPbFloatHistogramPair
#[test]
fn test_prometheus_hpoint_in_sync_with_mimirpb_float_histogram_pair() {
    let hp = PromHPoint {
        t: 99,
        h: generate_test_float_histogram(2),
    };
    let pair = from_hpoints_to_histograms(std::slice::from_ref(&hp))[0].clone();
    assert_eq!(pair.timestamp_ms, hp.t);
    assert_eq!(from_histograms_to_hpoints(&[pair])[0], hp);
}

// Upstream: pkg/mimirpb/compat_test.go::TestPreallocatingMetric
#[test]
fn test_preallocating_metric() {
    let metric = cortexpb::Metric {
        labels: vec![
            cortexpb::LabelPair::from_static("l1", "v1"),
            cortexpb::LabelPair::from_static("l2", "v2"),
            cortexpb::LabelPair::from_static("l3", "v3"),
            cortexpb::LabelPair::from_static("l4", "v4"),
        ],
    };
    let metric_bytes = metric.encode_to_vec();
    let mut prealloc = PreallocatingMetric::default();
    prealloc.unmarshal(&metric_bytes).unwrap();
    assert_eq!(prealloc.metric, metric);

    let mut bad = PreallocatingMetric::default();
    assert!(bad.unmarshal(&[1, 3, 5, 6, 238, 55, 135]).is_err());

    let metric5 = cortexpb::Metric {
        labels: vec![
            cortexpb::LabelPair::from_static("l1", "v1"),
            cortexpb::LabelPair::from_static("l2", "v2"),
            cortexpb::LabelPair::from_static("l3", "v3"),
            cortexpb::LabelPair::from_static("l4", "v4"),
            cortexpb::LabelPair::from_static("l5", "v5"),
        ],
    };
    let bytes5 = metric5.encode_to_vec();
    let mut prealloc5 = PreallocatingMetric::default();
    prealloc5.unmarshal(&bytes5).unwrap();
    assert_eq!(prealloc5.metric, metric5);
    assert!(prealloc5.metric.labels.capacity() >= metric5.labels.len());
    assert_eq!(PreallocatingMetric::labels_count(&bytes5), Some(5));

    let empty = cortexpb::Metric::default();
    let empty_bytes = empty.encode_to_vec();
    let mut prealloc_empty = PreallocatingMetric::default();
    prealloc_empty.unmarshal(&empty_bytes).unwrap();
    assert!(prealloc_empty.metric.labels.is_empty());

    assert_eq!(prealloc5.marshal(), bytes5);
}

// Upstream: pkg/mimirpb/compat_test.go::TestRemoteWriteContainsHistogram
#[test]
fn test_remote_write_contains_histogram() {
    let hist = from_histogram_to_histogram_proto(1337, &generate_test_histogram(0));
    let rw = cortexpb::WriteRequest {
        timeseries: vec![cortexpb::TimeSeries {
            histograms: vec![hist],
            ..Default::default()
        }],
        ..Default::default()
    };
    let data = rw.encode_to_vec();
    let decoded = cortexpb::WriteRequest::decode(data.as_slice()).unwrap();
    assert!(!decoded.timeseries.is_empty());
    assert!(!decoded.timeseries[0].histograms.is_empty());
}

// Upstream: pkg/mimirpb/compat_test.go::TestFromPromRemoteWriteHistogramToMimir
#[test]
fn test_from_prom_remote_write_histogram_to_mimir() {
    for (name, tsdb_hist, expect_gauge) in [
        ("counter", generate_test_histogram(0), false),
        ("gauge", generate_test_gauge_histogram(0), true),
    ] {
        let proto = from_histogram_to_histogram_proto(1337, &tsdb_hist);
        let data = proto.encode_to_vec();
        let received = cortexpb::Histogram::decode(data.as_slice()).unwrap();
        assert!(!received.is_float_histogram(), "{name}");
        assert_eq!(received.is_gauge(), expect_gauge, "{name}");
        let mimir_hist = from_histogram_proto_to_histogram(&received);
        assert_eq!(mimir_hist, tsdb_hist, "{name}");
    }
}

// Upstream: pkg/mimirpb/compat_test.go::TestFromPromRemoteWriteFloatHistogramToMimir
#[test]
fn test_from_prom_remote_write_float_histogram_to_mimir() {
    for (name, tsdb_hist, expect_gauge) in [
        ("counter", generate_test_float_histogram(0), false),
        ("gauge", generate_test_gauge_float_histogram(0), true),
    ] {
        let proto = from_float_histogram_to_histogram_proto(1337, &tsdb_hist);
        let data = proto.encode_to_vec();
        let received = cortexpb::Histogram::decode(data.as_slice()).unwrap();
        assert!(received.is_float_histogram(), "{name}");
        assert_eq!(received.is_gauge(), expect_gauge, "{name}");
        let mimir_hist = from_float_histogram_proto_to_float_histogram(&received);
        assert_eq!(mimir_hist, tsdb_hist, "{name}");
    }
}

// Upstream: pkg/mimirpb/compat_test.go::TestCounterResetHint
#[test]
fn test_counter_reset_hint() {
    use cortexpb::histogram::ResetHint;
    assert_eq!(ResetHint::Unknown as i32, 0);
    assert_eq!(ResetHint::Yes as i32, 1);
    assert_eq!(ResetHint::No as i32, 2);
    assert_eq!(ResetHint::Gauge as i32, 3);
    assert_eq!(ResetHint::from_str_name("UNKNOWN"), Some(ResetHint::Unknown));
    assert_eq!(ResetHint::from_str_name("YES"), Some(ResetHint::Yes));
    assert_eq!(ResetHint::from_str_name("NO"), Some(ResetHint::No));
    assert_eq!(ResetHint::from_str_name("GAUGE"), Some(ResetHint::Gauge));
}

// Upstream: pkg/mimirpb/compat_test.go::TestFromHistogramToHistogramProto
#[test]
fn test_from_histogram_to_histogram_proto() {
    let ts = 1i64;
    let mut h = generate_test_histogram(ts);
    h.counter_reset_hint = CounterResetHint::NotCounterReset as i32;

    let p = from_histogram_to_histogram_proto(ts, &h);
    let expected = cortexpb::Histogram {
        count: Some(cortexpb::histogram::Count::CountInt(21)),
        sum: 36.8,
        schema: 1,
        zero_threshold: 0.001,
        zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountInt(3)),
        negative_spans: vec![
            cortexpb::BucketSpan {
                offset: 0,
                length: 2,
            },
            cortexpb::BucketSpan {
                offset: 1,
                length: 2,
            },
        ],
        negative_deltas: vec![2, 1, -1, 0],
        positive_spans: vec![
            cortexpb::BucketSpan {
                offset: 0,
                length: 2,
            },
            cortexpb::BucketSpan {
                offset: 1,
                length: 2,
            },
        ],
        positive_deltas: vec![2, 1, -1, 0],
        reset_hint: cortexpb::histogram::ResetHint::No as i32,
        timestamp: ts,
        ..Default::default()
    };
    assert_eq!(p, expected);
    assert_eq!(from_histogram_proto_to_histogram(&p), h);

    let wire = p.encode_to_vec();
    assert_eq!(cortexpb::Histogram::decode(wire.as_slice()).unwrap(), expected);
}

// Upstream: pkg/mimirpb/compat_test.go::TestFromFloatHistogramToHistogramProto
#[test]
fn test_from_float_histogram_to_histogram_proto() {
    let ts = 1i64;
    let mut h = generate_test_float_histogram(ts);
    h.counter_reset_hint = CounterResetHint::NotCounterReset as i32;

    let p = from_float_histogram_to_histogram_proto(ts, &h);
    let expected = cortexpb::Histogram {
        count: Some(cortexpb::histogram::Count::CountFloat(21.0)),
        sum: 36.8,
        schema: 1,
        zero_threshold: 0.001,
        zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountFloat(3.0)),
        negative_spans: vec![
            cortexpb::BucketSpan {
                offset: 0,
                length: 2,
            },
            cortexpb::BucketSpan {
                offset: 1,
                length: 2,
            },
        ],
        negative_counts: vec![2.0, 3.0, 2.0, 2.0],
        positive_spans: vec![
            cortexpb::BucketSpan {
                offset: 0,
                length: 2,
            },
            cortexpb::BucketSpan {
                offset: 1,
                length: 2,
            },
        ],
        positive_counts: vec![2.0, 3.0, 2.0, 2.0],
        reset_hint: cortexpb::histogram::ResetHint::No as i32,
        timestamp: ts,
        ..Default::default()
    };
    assert_eq!(p, expected);
    assert_eq!(from_float_histogram_proto_to_float_histogram(&p), h);

    let wire = p.encode_to_vec();
    assert_eq!(cortexpb::Histogram::decode(wire.as_slice()).unwrap(), expected);
}

// Upstream: pkg/mimirpb/compat_test.go::TestFromFloatHistogramToPromHistogram
#[test]
fn test_from_float_histogram_to_prom_histogram() {
    let cases = [
        (
            PromFloatHistogram {
                count: 18.0,
                zero_count: 2.0,
                zero_threshold: 0.001,
                sum: 18.4,
                schema: 0,
                positive_spans: vec![
                    PromSpan {
                        offset: 0,
                        length: 2,
                    },
                    PromSpan {
                        offset: 1,
                        length: 1,
                    },
                ],
                positive_buckets: vec![1.0, 2.0, 1.0],
                negative_spans: vec![
                    PromSpan {
                        offset: 0,
                        length: 2,
                    },
                    PromSpan {
                        offset: 1,
                        length: 1,
                    },
                ],
                negative_buckets: vec![1.0, 2.0, 1.0],
                ..Default::default()
            },
            PromSampleHistogram {
                count: 18.0,
                sum: 18.4,
                buckets: vec![
                    PromHistogramBucket {
                        boundaries: 1,
                        lower: -8.0,
                        upper: -4.0,
                        count: 1.0,
                    },
                    PromHistogramBucket {
                        boundaries: 1,
                        lower: -2.0,
                        upper: -1.0,
                        count: 2.0,
                    },
                    PromHistogramBucket {
                        boundaries: 1,
                        lower: -1.0,
                        upper: -0.5,
                        count: 1.0,
                    },
                    PromHistogramBucket {
                        boundaries: 3,
                        lower: -0.001,
                        upper: 0.001,
                        count: 2.0,
                    },
                    PromHistogramBucket {
                        boundaries: 0,
                        lower: 0.5,
                        upper: 1.0,
                        count: 1.0,
                    },
                    PromHistogramBucket {
                        boundaries: 0,
                        lower: 1.0,
                        upper: 2.0,
                        count: 2.0,
                    },
                    PromHistogramBucket {
                        boundaries: 0,
                        lower: 4.0,
                        upper: 8.0,
                        count: 1.0,
                    },
                ],
            },
        ),
        (
            PromFloatHistogram {
                count: 18.0,
                zero_count: 0.0,
                zero_threshold: 0.001,
                sum: 18.4,
                schema: 0,
                positive_spans: vec![
                    PromSpan {
                        offset: 0,
                        length: 2,
                    },
                    PromSpan {
                        offset: 1,
                        length: 1,
                    },
                ],
                positive_buckets: vec![1.0, 0.0, 1.0],
                negative_spans: vec![
                    PromSpan {
                        offset: 0,
                        length: 2,
                    },
                    PromSpan {
                        offset: 1,
                        length: 1,
                    },
                ],
                negative_buckets: vec![1.0, 2.0, 0.0],
                ..Default::default()
            },
            PromSampleHistogram {
                count: 18.0,
                sum: 18.4,
                buckets: vec![
                    PromHistogramBucket {
                        boundaries: 1,
                        lower: -2.0,
                        upper: -1.0,
                        count: 2.0,
                    },
                    PromHistogramBucket {
                        boundaries: 1,
                        lower: -1.0,
                        upper: -0.5,
                        count: 1.0,
                    },
                    PromHistogramBucket {
                        boundaries: 0,
                        lower: 0.5,
                        upper: 1.0,
                        count: 1.0,
                    },
                    PromHistogramBucket {
                        boundaries: 0,
                        lower: 4.0,
                        upper: 8.0,
                        count: 1.0,
                    },
                ],
            },
        ),
    ];

    for (h, exp) in cases {
        assert_eq!(from_float_histogram_to_prom_histogram(&h), exp);
    }
}

// Upstream: pkg/mimirpb/compat_test.go::TestPrometheusSampleHistogramInSyncWithMimirPbSampleHistogram
#[test]
fn test_prometheus_sample_histogram_in_sync_with_mimirpb_sample_histogram() {
    let sh = cortexpb::SampleHistogram {
        count: 10.0,
        sum: 20.0,
        buckets: vec![cortexpb::HistogramBucket {
            boundaries: 1,
            lower: 0.0,
            upper: 1.0,
            count: 5.0,
        }],
    };
    let wire = sh.encode_to_vec();
    let dec = cortexpb::SampleHistogram::decode(wire.as_slice()).unwrap();
    assert_eq!(dec, sh);
}

// Upstream: pkg/mimirpb/compat_test.go::TestPrometheusLabelsInSyncWithMimirPbLabelAdapter
#[test]
fn test_prometheus_labels_in_sync_with_mimirpb_label_adapter() {
    let la = LabelAdapter::new("k", "v");
    let pl = PromLabel {
        name: la.name.clone(),
        value: la.value.clone(),
    };
    assert_eq!(pl.name, "k");
    assert_eq!(pl.value, "v");
}

// Upstream: pkg/mimirpb/compat_test.go::TestPrometheusHistogramSpanInSyncWithMimirPbBucketSpan
#[test]
fn test_prometheus_histogram_span_in_sync_with_mimirpb_bucket_span() {
    let bs = cortexpb::BucketSpan {
        offset: -3,
        length: 4,
    };
    let ps = PromSpan {
        offset: bs.offset,
        length: bs.length,
    };
    assert_eq!(ps.offset, bs.offset);
    assert_eq!(ps.length, bs.length);
}

// Upstream: pkg/mimirpb/compat_test.go::TestCompareLabelAdapters
#[test]
fn test_compare_label_adapters() {
    let labels = vec![
        LabelAdapter::new("aaa", "111"),
        LabelAdapter::new("bbb", "222"),
    ];
    let tests = [
        (vec![LabelAdapter::new("aaa", "110"), LabelAdapter::new("bbb", "222")], 1),
        (vec![LabelAdapter::new("aaa", "111"), LabelAdapter::new("bbb", "233")], -1),
        (vec![LabelAdapter::new("aaa", "111"), LabelAdapter::new("bar", "222")], 1),
        (vec![LabelAdapter::new("aaa", "111"), LabelAdapter::new("bbc", "222")], -1),
        (vec![LabelAdapter::new("aaa", "111"), LabelAdapter::new("bb", "222")], 1),
        (vec![LabelAdapter::new("aaa", "111"), LabelAdapter::new("bbbb", "222")], -1),
        (vec![LabelAdapter::new("aaa", "111")], 1),
        (
            vec![
                LabelAdapter::new("aaa", "111"),
                LabelAdapter::new("bbb", "222"),
                LabelAdapter::new("ccc", "333"),
                LabelAdapter::new("ddd", "444"),
            ],
            -2,
        ),
        (vec![LabelAdapter::new("aaa", "111"), LabelAdapter::new("bbb", "222")], 0),
        (vec![], 1),
    ];

    let sign = |a: i32| a.signum();
    for (i, (compared, expected)) in tests.iter().enumerate() {
        let got = compare_label_adapters(&labels, compared);
        assert_eq!(sign(got), sign(*expected), "case {i}");
        let got_rev = compare_label_adapters(compared, &labels);
        assert_eq!(sign(got_rev), -sign(*expected), "rev case {i}");
    }
}

// Upstream: pkg/mimirpb/compat_test.go::TestNonStableHash
#[test]
fn test_non_stable_hash() {
    let serialized_hash = |ls: &[LabelAdapter]| -> u64 {
        let mut b = Vec::new();
        for l in ls {
            b.extend_from_slice(l.name.as_bytes());
            b.push(0xff);
            b.extend_from_slice(l.value.as_bytes());
            b.push(0xff);
        }
        xxh64(&b, 0)
    };

    let small = vec![
        LabelAdapter::new("__name__", "metric"),
        LabelAdapter::new("lbl", "value"),
    ];

    let mut big = Vec::new();
    let mut big_len = 0usize;
    while big_len <= NON_STABLE_HASH_FAST_PATH_CAP * 2 {
        let l = LabelAdapter::new(format!("label_{:03}", big.len()), "v".repeat(40));
        big_len += l.name.len() + l.value.len() + 2;
        big.push(l);
    }
    assert!(big_len > NON_STABLE_HASH_FAST_PATH_CAP);

    assert_eq!(non_stable_hash(&small), non_stable_hash(&small));
    assert_eq!(non_stable_hash(&big), non_stable_hash(&big));
    assert_eq!(serialized_hash(&small), non_stable_hash(&small));
    assert_eq!(serialized_hash(&big), non_stable_hash(&big));
    assert_eq!(non_stable_hash(&[]), serialized_hash(&[]));

    let a = vec![LabelAdapter::new("__name__", "metric"), LabelAdapter::new("lbl", "a")];
    let b = vec![LabelAdapter::new("__name__", "metric"), LabelAdapter::new("lbl", "b")];
    assert_ne!(non_stable_hash(&a), non_stable_hash(&b));

    let o1 = vec![LabelAdapter::new("a", "1"), LabelAdapter::new("b", "2")];
    let o2 = vec![LabelAdapter::new("b", "2"), LabelAdapter::new("a", "1")];
    assert_ne!(non_stable_hash(&o1), non_stable_hash(&o2));

    let s1 = vec![LabelAdapter::new("ab", "c")];
    let s2 = vec![LabelAdapter::new("a", "bc")];
    assert_ne!(non_stable_hash(&s1), non_stable_hash(&s2));
}

// Upstream: pkg/mimirpb/compat_slice_test.go::TestCopyLabels
#[test]
fn test_copy_labels() {
    let expected = vec![
        PromLabel {
            name: "name1".to_string(),
            value: "value1".to_string(),
        },
        PromLabel {
            name: "name2".to_string(),
            value: "value2".to_string(),
        },
    ];
    let got = copy_labels(&expected);
    assert_eq!(got.to_vec(), expected);

    // Check that all strings share the same contiguous underlying buffer
    let base = got.backing_ptr() as usize;
    let mut offset = 0usize;
    for (name, value) in got.iter() {
        assert_eq!(name.as_ptr() as usize, base + offset);
        offset += name.len();
        assert_eq!(value.as_ptr() as usize, base + offset);
        offset += value.len();
    }
}

// Upstream: pkg/mimirpb/compat_rw2_test.go::TestRW2TypesCompatible
#[test]
fn test_rw2_types_compatible() {
    // Verify RW2 protobuf wire tags & types match io.prometheus.write.v2.Request (RW2.0-rc3 + CreatedTimestamp)
    let req = cortexpb::WriteRequestRw2 {
        symbols: vec!["".to_string(), "__name__".to_string(), "up".to_string()],
        timeseries: vec![cortexpb::TimeSeriesRw2 {
            labels_refs: vec![1, 2],
            samples: vec![cortexpb::Sample {
                value: 1.0,
                timestamp_ms: 1000,
            }],
            histograms: vec![cortexpb::Histogram {
                sum: 2.0,
                schema: 1,
                zero_threshold: 0.001,
                reset_hint: cortexpb::histogram::ResetHint::Yes as i32,
                timestamp: 1000,
                count: Some(cortexpb::histogram::Count::CountInt(3)),
                zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountInt(1)),
                ..Default::default()
            }],
            exemplars: vec![cortexpb::ExemplarRw2 {
                labels_refs: vec![1, 2],
                value: 1.0,
                timestamp: 1000,
            }],
            metadata: Some(cortexpb::MetadataRw2 {
                r#type: cortexpb::metadata_rw2::MetricType::Counter as i32,
                help_ref: 0,
                unit_ref: 0,
            }),
            created_timestamp: 500,
        }],
    };
    let wire = req.encode_to_vec();
    let decoded_full = cortexpb::WriteRequest::decode(wire.as_slice()).unwrap();
    assert_eq!(decoded_full.symbols_rw2, req.symbols);
    assert_eq!(decoded_full.timeseries_rw2, req.timeseries);
}

fn make_test_rw2_write_request(syms: &mut FastSymbolsTable<'_>) -> cortexpb::WriteRequest {
    let mut req = cortexpb::WriteRequest {
        timeseries_rw2: vec![cortexpb::TimeSeriesRw2 {
            labels_refs: vec![
                syms.symbolize("__name__"),
                syms.symbolize("test_metric_total"),
                syms.symbolize("job"),
                syms.symbolize("test_job"),
            ],
            samples: vec![cortexpb::Sample {
                value: 123.456,
                timestamp_ms: 1234567890,
            }],
            exemplars: vec![cortexpb::ExemplarRw2 {
                value: 123.456,
                timestamp: 1234567890,
                labels_refs: vec![
                    syms.symbolize("__name__"),
                    syms.symbolize("test_metric_total"),
                    syms.symbolize("traceID"),
                    syms.symbolize("1234567890abcdef"),
                ],
            }],
            metadata: Some(cortexpb::MetadataRw2 {
                r#type: cortexpb::metadata_rw2::MetricType::Counter as i32,
                help_ref: syms.symbolize("test_metric_help"),
                unit_ref: syms.symbolize("test_metric_unit"),
            }),
            ..Default::default()
        }],
        ..Default::default()
    };
    req.symbols_rw2 = syms.symbols();
    req
}

// Upstream: pkg/mimirpb/compat_rw2_test.go::TestRW2Unmarshal
#[test]
fn test_rw2_unmarshal() {
    use cortexpb::metadata_rw2::MetricType as Rw2Type;
    use cortexpb::metric_metadata::MetricType as Rw1Type;

    // 1. rw2 compatible produces expected WriteRequest
    {
        let mut syms = FastSymbolsTable::new(100);
        let write_request = make_test_rw2_write_request(&mut syms);
        let data = write_request.encode_to_vec();

        let mut received = PreallocWriteRequest {
            unmarshal_from_rw2: true,
            ..Default::default()
        };
        received.unmarshal(&data).unwrap();

        let expected = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_static("__name__", "test_metric_total"),
                    cortexpb::LabelPair::from_static("job", "test_job"),
                ],
                samples: vec![cortexpb::Sample {
                    value: 123.456,
                    timestamp_ms: 1234567890,
                }],
                exemplars: vec![cortexpb::Exemplar {
                    value: 123.456,
                    timestamp_ms: 1234567890,
                    labels: vec![
                        cortexpb::LabelPair::from_static("__name__", "test_metric_total"),
                        cortexpb::LabelPair::from_static("traceID", "1234567890abcdef"),
                    ],
                }],
                ..Default::default()
            }],
            metadata: vec![cortexpb::MetricMetadata {
                metric_family_name: "test_metric_total".to_string(),
                r#type: Rw1Type::Counter as i32,
                help: "test_metric_help".to_string(),
                unit: "test_metric_unit".to_string(),
            }],
            ..Default::default()
        };
        assert_eq!(received.write_request, expected);
    }

    // 2. zero timeseries does not panic
    {
        let mut syms = FastSymbolsTable::new(100);
        syms.symbolize("unused_symbol");
        let req = cortexpb::WriteRequest {
            symbols_rw2: syms.symbols(),
            ..Default::default()
        };
        let data = req.encode_to_vec();
        let mut received = PreallocWriteRequest {
            unmarshal_from_rw2: true,
            ..Default::default()
        };
        received.unmarshal(&data).unwrap();
        assert!(received.write_request.timeseries.is_empty());
        assert!(received.write_request.metadata.is_empty());
    }

    // 3. metadata for all metric types map to expected values
    for (rw2_type, rw1_type) in [
        (Rw2Type::Unspecified, Rw1Type::Unknown),
        (Rw2Type::Counter, Rw1Type::Counter),
        (Rw2Type::Gauge, Rw1Type::Gauge),
        (Rw2Type::Histogram, Rw1Type::Histogram),
        (Rw2Type::Gaugehistogram, Rw1Type::Gaugehistogram),
        (Rw2Type::Summary, Rw1Type::Summary),
        (Rw2Type::Info, Rw1Type::Info),
        (Rw2Type::Stateset, Rw1Type::Stateset),
    ] {
        let mut syms = FastSymbolsTable::new(100);
        let req = cortexpb::WriteRequest {
            timeseries_rw2: vec![cortexpb::TimeSeriesRw2 {
                labels_refs: vec![syms.symbolize("__name__"), syms.symbolize("test_metric_total")],
                metadata: Some(cortexpb::MetadataRw2 {
                    r#type: rw2_type as i32,
                    help_ref: syms.symbolize("test_metric_help"),
                    unit_ref: syms.symbolize("test_metric_unit"),
                }),
                ..Default::default()
            }],
            symbols_rw2: syms.symbols(),
            ..Default::default()
        };
        let data = req.encode_to_vec();
        let mut received = PreallocWriteRequest {
            unmarshal_from_rw2: true,
            ..Default::default()
        };
        received.unmarshal(&data).unwrap();
        assert_eq!(received.write_request.metadata.len(), 1);
        assert_eq!(received.write_request.metadata[0].r#type, rw1_type as i32);
        assert_eq!(received.write_request.metadata[0].metric_family_name, "test_metric_total");
    }

    // 4 & 5. metadata metric family name normalization & skip_normalize
    {
        let mut syms = FastSymbolsTable::new(100);
        let req = cortexpb::WriteRequest {
            timeseries_rw2: vec![
                ("test_metric_summary_count", Rw2Type::Summary),
                ("test_metric_summary_sum", Rw2Type::Summary),
                ("test_metric_histogram_bucket", Rw2Type::Histogram),
                ("test_metric_histogram_count", Rw2Type::Histogram),
                ("test_metric_histogram_sum", Rw2Type::Histogram),
                ("test_metric_gaugehistogram_bucket", Rw2Type::Gaugehistogram),
                ("test_metric_gaugehistogram_gcount", Rw2Type::Gaugehistogram),
                ("test_metric_gaugehistogram_gsum", Rw2Type::Gaugehistogram),
            ]
            .into_iter()
            .map(|(n, ty)| cortexpb::TimeSeriesRw2 {
                labels_refs: vec![syms.symbolize("__name__"), syms.symbolize(n)],
                metadata: Some(cortexpb::MetadataRw2 {
                    r#type: ty as i32,
                    help_ref: syms.symbolize("test_metric_help"),
                    unit_ref: syms.symbolize("test_metric_unit"),
                }),
                ..Default::default()
            })
            .collect(),
            symbols_rw2: syms.symbols(),
            ..Default::default()
        };
        let data = req.encode_to_vec();

        let mut received = PreallocWriteRequest {
            unmarshal_from_rw2: true,
            ..Default::default()
        };
        received.unmarshal(&data).unwrap();
        assert_eq!(received.write_request.timeseries.len(), 8);
        assert_eq!(received.write_request.metadata.len(), 3);
        assert_eq!(received.write_request.metadata[0].metric_family_name, "test_metric_summary");
        assert_eq!(received.write_request.metadata[1].metric_family_name, "test_metric_histogram");
        assert_eq!(received.write_request.metadata[2].metric_family_name, "test_metric_gaugehistogram");

        let mut received_no_norm = PreallocWriteRequest {
            unmarshal_from_rw2: true,
            skip_normalize_metadata_metric_name: true,
            ..Default::default()
        };
        received_no_norm.unmarshal(&data).unwrap();
        assert_eq!(received_no_norm.write_request.timeseries.len(), 8);
        assert_eq!(received_no_norm.write_request.metadata.len(), 8);
        assert_eq!(
            received_no_norm
                .write_request
                .metadata
                .iter()
                .map(|m| m.metric_family_name.as_str())
                .collect::<Vec<_>>(),
            vec![
                "test_metric_summary_count",
                "test_metric_summary_sum",
                "test_metric_histogram_bucket",
                "test_metric_histogram_count",
                "test_metric_histogram_sum",
                "test_metric_gaugehistogram_bucket",
                "test_metric_gaugehistogram_gcount",
                "test_metric_gaugehistogram_gsum",
            ]
        );

        // Also test skip_deduplicate + skip_normalize
        let mut received_no_dedup = PreallocWriteRequest {
            unmarshal_from_rw2: true,
            skip_normalize_metadata_metric_name: true,
            skip_deduplicate_metadata: true,
            ..Default::default()
        };
        received_no_dedup.unmarshal(&data).unwrap();
        assert_eq!(received_no_dedup.write_request.metadata.len(), 8);
    }

    // 6. rw2 with offset
    {
        let mut syms = FastSymbolsTable::new(100);
        syms.configure_common_symbols(256, None);
        let req = make_test_rw2_write_request(&mut syms);
        let data = req.encode_to_vec();

        let mut received = PreallocWriteRequest {
            unmarshal_from_rw2: true,
            rw2_symbol_offset: 256,
            ..Default::default()
        };
        received.unmarshal(&data).unwrap();
        assert_eq!(received.write_request.timeseries.len(), 1);
        assert_eq!(received.write_request.metadata.len(), 1);
    }

    // 7. wrong offset fails to unmarshal
    {
        let mut syms = FastSymbolsTable::new(100);
        syms.configure_common_symbols(256, None);
        let req = make_test_rw2_write_request(&mut syms);
        let data = req.encode_to_vec();

        let mut recv_hi = PreallocWriteRequest {
            unmarshal_from_rw2: true,
            rw2_symbol_offset: 258,
            ..Default::default()
        };
        let err = recv_hi.unmarshal(&data).unwrap_err().to_string();
        assert!(err.contains("invalid"), "unexpected err: {err}");

        let mut recv_lo = PreallocWriteRequest {
            unmarshal_from_rw2: true,
            rw2_symbol_offset: 255,
            ..Default::default()
        };
        let err = recv_lo.unmarshal(&data).unwrap_err().to_string();
        assert!(err.contains("invalid"), "unexpected err: {err}");
    }

    // 8. offset and shared symbols
    {
        let common_vec = vec!["".to_string(), "__name__".to_string(), "job".to_string()];
        let cs = mimir_proto::symbols::CommonSymbols::new(common_vec.clone());
        let mut syms = FastSymbolsTable::new(100);
        syms.configure_common_symbols(common_vec.len() as u32, Some(&cs));
        let req = make_test_rw2_write_request(&mut syms);
        let data = req.encode_to_vec();

        let mut received = PreallocWriteRequest {
            unmarshal_from_rw2: true,
            rw2_symbol_offset: common_vec.len() as u32,
            rw2_common_symbols: common_vec,
            ..Default::default()
        };
        received.unmarshal(&data).unwrap();
        assert_eq!(received.write_request.timeseries.len(), 1);
        assert_eq!(received.write_request.metadata.len(), 1);
    }

    // 9. common symbol received but none defined
    {
        let mut syms = FastSymbolsTable::new(100);
        syms.configure_common_symbols(256, None);
        let mut req = make_test_rw2_write_request(&mut syms);
        req.timeseries_rw2[0].labels_refs[0] = 128;
        let data = req.encode_to_vec();

        let mut received = PreallocWriteRequest {
            unmarshal_from_rw2: true,
            rw2_symbol_offset: 256,
            ..Default::default()
        };
        let err = received.unmarshal(&data).unwrap_err().to_string();
        assert!(err.contains("invalid"), "unexpected err: {err}");
    }

    // 10. zero refs translate to empty string despite offset
    {
        let mut syms = FastSymbolsTable::new(100);
        syms.configure_common_symbols(256, None);
        let req = cortexpb::WriteRequest {
            timeseries_rw2: vec![cortexpb::TimeSeriesRw2 {
                labels_refs: vec![
                    syms.symbolize("__name__"),
                    syms.symbolize("test_metric_total"),
                ],
                metadata: Some(cortexpb::MetadataRw2 {
                    r#type: Rw2Type::Counter as i32,
                    help_ref: syms.symbolize("test_metric_help"),
                    unit_ref: 0,
                }),
                ..Default::default()
            }],
            symbols_rw2: syms.symbols(),
            ..Default::default()
        };
        let data = req.encode_to_vec();
        let mut received = PreallocWriteRequest {
            unmarshal_from_rw2: true,
            rw2_symbol_offset: 256,
            ..Default::default()
        };
        received.unmarshal(&data).unwrap();
        assert_eq!(received.write_request.metadata[0].unit, "");
    }

    // 11. common symbol out of bounds
    {
        let common_vec = vec!["__name__".to_string()];
        let cs = mimir_proto::symbols::CommonSymbols::new(common_vec.clone());
        let mut syms = FastSymbolsTable::new(100);
        syms.configure_common_symbols(256, Some(&cs));
        let mut req = make_test_rw2_write_request(&mut syms);
        req.timeseries_rw2[0].labels_refs[0] = 1; // out of bounds for len 1
        let data = req.encode_to_vec();

        let mut received = PreallocWriteRequest {
            unmarshal_from_rw2: true,
            rw2_symbol_offset: 256,
            rw2_common_symbols: common_vec,
            ..Default::default()
        };
        let err = received.unmarshal(&data).unwrap_err().to_string();
        assert!(err.contains("invalid"), "unexpected err: {err}");
    }

    // 12. first symbol not empty string rejected
    {
        let req = cortexpb::WriteRequest {
            symbols_rw2: vec![
                "__name__".to_string(),
                "test_metric_total".to_string(),
                "job".to_string(),
                "my_job".to_string(),
            ],
            timeseries_rw2: vec![cortexpb::TimeSeriesRw2 {
                labels_refs: vec![0, 1, 2, 3],
                samples: vec![cortexpb::Sample {
                    value: 123.456,
                    timestamp_ms: 1234567890,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let data = req.encode_to_vec();
        let mut received = PreallocWriteRequest {
            unmarshal_from_rw2: true,
            ..Default::default()
        };
        let err = received.unmarshal(&data).unwrap_err().to_string();
        assert!(err.contains("symbols must start with empty string"), "unexpected err: {err}");
    }

    // 13. metadata order is deterministic
    for _ in 0..1000 {
        let mut syms = FastSymbolsTable::new(100);
        let mut req = make_test_rw2_write_request(&mut syms);
        req.timeseries_rw2.push(cortexpb::TimeSeriesRw2 {
            labels_refs: vec![syms.symbolize("__name__"), syms.symbolize("metric_2")],
            metadata: Some(cortexpb::MetadataRw2 {
                r#type: Rw2Type::Counter as i32,
                help_ref: syms.symbolize("metric_2 help text."),
                unit_ref: 0,
            }),
            ..Default::default()
        });
        req.timeseries_rw2.push(cortexpb::TimeSeriesRw2 {
            labels_refs: vec![syms.symbolize("__name__"), syms.symbolize("metric_3")],
            metadata: Some(cortexpb::MetadataRw2 {
                r#type: Rw2Type::Counter as i32,
                help_ref: syms.symbolize("metric_3 help text."),
                unit_ref: 0,
            }),
            ..Default::default()
        });
        req.timeseries_rw2.push(cortexpb::TimeSeriesRw2 {
            labels_refs: vec![syms.symbolize("__name__"), syms.symbolize("metric_2")],
            metadata: Some(cortexpb::MetadataRw2 {
                r#type: Rw2Type::Counter as i32,
                help_ref: syms.symbolize("duplicated metric_2 help text, but different."),
                unit_ref: 0,
            }),
            ..Default::default()
        });
        req.symbols_rw2 = syms.symbols();
        let data = req.encode_to_vec();

        let mut received = PreallocWriteRequest {
            unmarshal_from_rw2: true,
            ..Default::default()
        };
        received.unmarshal(&data).unwrap();
        assert_eq!(received.write_request.metadata.len(), 3);
        assert_eq!(received.write_request.metadata[0].metric_family_name, "test_metric_total");
        assert_eq!(received.write_request.metadata[1].metric_family_name, "metric_2");
        assert_eq!(received.write_request.metadata[2].metric_family_name, "metric_3");
        assert_eq!(received.write_request.metadata[1].help, "metric_2 help text.");
    }

    // 14. conflicting metadata: first wins by default, skipDeduplicateMetadata preserves both
    {
        let req = cortexpb::WriteRequest {
            symbols_rw2: vec![
                "".to_string(),
                "__name__".to_string(),
                "my_cool_series".to_string(),
                "It's a cool series, but old description.".to_string(),
                "It's a cool series, but new description.".to_string(),
                "megawatts".to_string(),
            ],
            timeseries_rw2: vec![
                cortexpb::TimeSeriesRw2 {
                    labels_refs: vec![1, 2],
                    metadata: Some(cortexpb::MetadataRw2 {
                        r#type: Rw2Type::Counter as i32,
                        help_ref: 3,
                        unit_ref: 5,
                    }),
                    ..Default::default()
                },
                cortexpb::TimeSeriesRw2 {
                    labels_refs: vec![1, 2],
                    metadata: Some(cortexpb::MetadataRw2 {
                        r#type: Rw2Type::Counter as i32,
                        help_ref: 4,
                        unit_ref: 5,
                    }),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let data = req.encode_to_vec();

        let mut received = PreallocWriteRequest {
            unmarshal_from_rw2: true,
            ..Default::default()
        };
        received.unmarshal(&data).unwrap();
        assert_eq!(received.write_request.metadata.len(), 1);
        assert_eq!(received.write_request.metadata[0].help, "It's a cool series, but old description.");

        let mut received_no_dedup = PreallocWriteRequest {
            unmarshal_from_rw2: true,
            skip_deduplicate_metadata: true,
            ..Default::default()
        };
        received_no_dedup.unmarshal(&data).unwrap();
        assert_eq!(received_no_dedup.write_request.metadata.len(), 2);
        assert_eq!(received_no_dedup.write_request.metadata[0].help, "It's a cool series, but old description.");
        assert_eq!(received_no_dedup.write_request.metadata[1].help, "It's a cool series, but new description.");
    }
}

// Upstream: pkg/mimirpb/errors_test.go::TestIsClientError
#[test]
fn test_is_client_error() {
    let status_with_raw_code = |code: i32| {
        let rpc_status = google_rpc::Status {
            code,
            message: "this is an error".to_string(),
            details: Vec::new(),
        };
        tonic::Status::with_details(tonic::Code::Unknown, "this is an error", rpc_status.encode_to_vec().into())
    };

    let cases = [
        (
            "httpgrpc 400",
            status_with_http_response(
                &httpgrpcpb::HttpResponse {
                    code: 400,
                    ..Default::default()
                },
                "this is an error",
            ),
            true,
        ),
        ("grpc 429", status_with_raw_code(429), true),
        (
            "httpgrpc 500",
            status_with_http_response(
                &httpgrpcpb::HttpResponse {
                    code: 500,
                    ..Default::default()
                },
                "this is an error",
            ),
            false,
        ),
        ("grpc 503", status_with_raw_code(503), false),
        ("grpc Internal", tonic::Status::internal("this is an error"), false),
        (
            "BAD_DATA details",
            status_with_cortex_error_details(
                tonic::Code::InvalidArgument,
                "",
                &cortexpb::ErrorDetails {
                    cause: cortexpb::ErrorCause::BadData as i32,
                    ..Default::default()
                },
            ),
            true,
        ),
        (
            "TENANT_LIMIT details",
            status_with_cortex_error_details(
                tonic::Code::FailedPrecondition,
                "",
                &cortexpb::ErrorDetails {
                    cause: cortexpb::ErrorCause::TenantLimit as i32,
                    ..Default::default()
                },
            ),
            true,
        ),
        (
            "TSDB_UNAVAILABLE details",
            status_with_cortex_error_details(
                tonic::Code::FailedPrecondition,
                "",
                &cortexpb::ErrorDetails {
                    cause: cortexpb::ErrorCause::TsdbUnavailable as i32,
                    ..Default::default()
                },
            ),
            false,
        ),
    ];

    for (name, status, expected) in cases {
        assert_eq!(cortexpb::is_client_error(&status), expected, "{name}");
    }

    let plain_err = std::io::Error::other("this is a random error");
    assert!(!cortexpb::is_client_error_err(&plain_err));
}

fn extract_prometheus_strings(pattern: &str) -> Vec<String> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let api_go = manifest_dir.join("../../.ref/mimir/vendor/github.com/prometheus/prometheus/web/api/v1/api.go");
    let contents = std::fs::read_to_string(&api_go).expect("read prometheus api.go");
    let re = regex::Regex::new(pattern).unwrap();
    let out: Vec<String> = re
        .captures_iter(&contents)
        .map(|c| c[1].to_string())
        .collect();
    assert!(!out.is_empty(), "expected matches for {pattern}");
    out
}

// Upstream: pkg/mimirpb/query_response_extra_test.go::TestAllPrometheusStatusValues
#[test]
fn test_all_prometheus_status_values() {
    let status_strings = extract_prometheus_strings(r#"(?m)^\s*\S+\s+status\s+=\s+"([^"]*)""#);
    for s in status_strings {
        let status = cortexpb::QueryStatus::from_prometheus_string(&s).unwrap();
        let actual = status.to_prometheus_string().unwrap();
        assert_eq!(actual, s);
    }
}

// Upstream: pkg/mimirpb/query_response_extra_test.go::TestAllPrometheusErrorTypeValues
#[test]
fn test_all_prometheus_error_type_values() {
    let error_type_strings = extract_prometheus_strings(r#"(?m)^\s*\S+\s+=\s+errorType\{[^,]+,\s+"([^"]*)"\}"#);
    for s in error_type_strings {
        let et = cortexpb::QueryErrorType::from_prometheus_string(&s).unwrap();
        let actual = et.to_prometheus_string().unwrap();
        assert_eq!(actual, s);
        cortexpb::QueryErrorType::from_api_error_type(&s).unwrap();
    }
}

// Upstream: pkg/mimirpb/query_response_extra_test.go::TestFloatHistogramProtobufTypeRemainsInSyncWithPrometheus
#[test]
fn test_float_histogram_protobuf_type_remains_in_sync_with_prometheus() {
    let fh = PromFloatHistogram {
        counter_reset_hint: 3,
        schema: 2,
        zero_threshold: 0.001,
        zero_count: 1.5,
        count: 5.5,
        sum: 12.25,
        positive_spans: vec![PromSpan { offset: 0, length: 1 }],
        negative_spans: vec![PromSpan { offset: 1, length: 1 }],
        positive_buckets: vec![2.5],
        negative_buckets: vec![1.5],
        custom_values: vec![0.1, 0.2],
    };
    let proto = float_histogram_from_prometheus_model(&fh);
    assert_eq!(float_histogram_to_prometheus_model(&proto), fh);
}

// Upstream: pkg/mimirpb/query_response_extra_test.go::TestFloatHistogramProtobufToPrometheus
#[test]
fn test_float_histogram_protobuf_to_prometheus() {
    let fh = cortexpb::FloatHistogram {
        counter_reset_hint: 3,
        schema: 3,
        zero_threshold: 2.938735877055719e-39,
        zero_count: 0.0,
        count: 6.844444444444443,
        sum: 0.00872226031111156,
        positive_spans: vec![
            cortexpb::BucketSpan { offset: -134, length: 10 },
            cortexpb::BucketSpan { offset: 1, length: 2 },
            cortexpb::BucketSpan { offset: 2, length: 4 },
            cortexpb::BucketSpan { offset: 2, length: 2 },
            cortexpb::BucketSpan { offset: 5, length: 1 },
            cortexpb::BucketSpan { offset: 43, length: 1 },
            cortexpb::BucketSpan { offset: 3, length: 2 },
            cortexpb::BucketSpan { offset: 3, length: 1 },
            cortexpb::BucketSpan { offset: 21, length: 1 },
            cortexpb::BucketSpan { offset: 1, length: 2 },
            cortexpb::BucketSpan { offset: 4, length: 1 },
        ],
        negative_spans: Vec::new(),
        positive_buckets: vec![
            0.02222222222222222, 0.9999999999999998, 1.2, 1.0666666666666667,
            0.9777777777777776, 1.1777777777777776, 0.42222222222222217, 0.2444444444444444,
            0.1333333333333333, 0.17777777777777776, 0.04444444444444444, 0.02222222222222222,
            0.04444444444444444, 0.02222222222222222, 0.02222222222222222, 0.02222222222222222,
            0.02222222222222222, 0.02222222222222222, 0.02222222222222222, 0.02222222222222222,
            0.02222222222222222, 0.02222222222222222, 0.02222222222222222, 0.02222222222222222,
            0.02222222222222222, 0.02222222222222222, 0.02222222222222222,
        ],
        negative_buckets: Vec::new(),
        custom_values: Vec::new(),
    };

    let expected = PromFloatHistogram {
        counter_reset_hint: CounterResetHint::GaugeType as i32,
        schema: 3,
        zero_threshold: 2.938735877055719e-39,
        zero_count: 0.0,
        count: 6.844444444444443,
        sum: 0.00872226031111156,
        positive_spans: vec![
            PromSpan { offset: -134, length: 10 },
            PromSpan { offset: 1, length: 2 },
            PromSpan { offset: 2, length: 4 },
            PromSpan { offset: 2, length: 2 },
            PromSpan { offset: 5, length: 1 },
            PromSpan { offset: 43, length: 1 },
            PromSpan { offset: 3, length: 2 },
            PromSpan { offset: 3, length: 1 },
            PromSpan { offset: 21, length: 1 },
            PromSpan { offset: 1, length: 2 },
            PromSpan { offset: 4, length: 1 },
        ],
        negative_spans: Vec::new(),
        positive_buckets: fh.positive_buckets.clone(),
        negative_buckets: Vec::new(),
        custom_values: Vec::new(),
    };

    assert_eq!(float_histogram_to_prometheus_model(&fh), expected);
}

// Upstream: pkg/mimirpb/validate_test.go::TestLabelsAreSorted
#[test]
fn test_labels_are_sorted() {
    assert!(are_label_names_sorted_and_unique(&[
        LabelAdapter::new("a", "z"),
        LabelAdapter::new("b", "y"),
        LabelAdapter::new("c", "x"),
    ]));
    assert!(!are_label_names_sorted_and_unique(&[
        LabelAdapter::new("a", "z"),
        LabelAdapter::new("c", "x"),
        LabelAdapter::new("b", "y"),
    ]));
    assert!(!are_label_names_sorted_and_unique(&[
        LabelAdapter::new("a", "z"),
        LabelAdapter::new("b", "y"),
        LabelAdapter::new("b", "y"),
        LabelAdapter::new("c", "x"),
    ]));
    assert!(are_label_names_sorted_and_unique(&[LabelAdapter::new("a", "z")]));
    assert!(are_label_names_sorted_and_unique(&[]));
}

#[test]
fn w18_wpe_03_go_quote_escapes_control_chars_and_empty_labels() {
    assert_eq!(mimir_proto::compat::from_label_adapters_to_string(&[]), "{}");
    assert_eq!(
        mimir_proto::compat::from_label_adapters_to_string(&[LabelAdapter::new("__name__", "")]),
        "{}"
    );
    let ls = vec![
        LabelAdapter::new("__name__", "m"),
        LabelAdapter::new(
            "v",
            "a\x07b\x08c\x0cd\ne\rf\tg\x0bh\x00i\x1fj\x7fk\u{0085}l\u{00ad}m",
        ),
    ];
    let formatted = mimir_proto::compat::from_label_adapters_to_string(&ls);
    assert_eq!(
        formatted,
        r#"m{v="a\ab\bc\fd\ne\rf\tg\vh\x00i\x1fj\x7fk\u0085l\u00adm"}"#
    );
}

/// `strconv.AppendQuote` escapes every rune for which `strconv.IsPrint` is false: format (Cf),
/// separator (Z*), private-use (Co) and unassigned (Cn) code points, not only controls.
/// Expected strings produced by Go `strconv.Quote` (go1.27.3).
#[test]
fn w18_wpe_03_go_quote_matches_strconv_is_print_for_non_latin1_runes() {
    let mut out = String::new();
    mimir_proto::compat::append_go_quote_str(
        &mut out,
        "a\u{200b}b\u{feff}\u{a0}\u{ad}\u{378}\u{e000}\u{1F600}\u{E0001}é\x7f\x01\t\"\\\u{2028}\u{85}",
    );
    assert_eq!(
        out,
        r#""a\u200bb\ufeff\u00a0\u00ad\u0378\ue000😀\U000e0001é\x7f\x01\t\"\\\u2028\u0085""#
    );

    let mut out = String::new();
    mimir_proto::compat::append_go_quote_bytes(&mut out, b"ok\xff\xe2\x80\x8b\xc3\xa9");
    assert_eq!(out, r#""ok\xff\u200bé""#);
}
