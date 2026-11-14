use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use mimir_conformance::oracle::{
    ensure_go_oracle_built, invoke_oracle, BytesB64Response, DecodeWriteRequest, ExemplarJson,
    HashSeriesRequest, HashSeriesResponse, HistogramJson, LabelJson, MetadataJson, SampleJson,
    SpanJson, TimeSeriesJson, WriteRequestJson,
};
use mimir_proto::bytes::Bytes;
use mimir_proto::cortexpb;
use mimir_proto::prost::Message as _;
use mimir_proto::sharding::{shard_by_all_labels, shard_by_metric_name, shard_by_user};
use mimir_proto::symbols::{v2_common_symbols, V2_RECORD_SYMBOL_OFFSET};
use mimir_proto::zerocopy::{
    decode_write_request, encode_write_request_rw2_bytes, DecodeOptions, WriteRequestScratch,
};

#[test]
fn w1_proto_hash_series_conformance() {
    ensure_go_oracle_built().expect("go-oracle must build");

    type HashCase = (&'static str, &'static str, Vec<(&'static str, &'static str)>);
    let mut cases: Vec<HashCase> = vec![
        ("", "", vec![]),
        ("tenant-1", "", vec![]),
        ("", "up", vec![("__name__", "up")]),
        ("anonymous", "http_requests_total", vec![("__name__", "http_requests_total")]),
        (
            "tenant-unicode-日本語",
            "метрика_total",
            vec![("__name__", "метрика_total"), ("env", "prod-🚀"), ("zone", "eu-west-1")],
        ),
        (
            "a".repeat(256).leak(),
            "very_long_metric_name_0123456789_abcdefghijklmnopqrstuvwxyz",
            vec![
                ("__name__", "very_long_metric_name_0123456789_abcdefghijklmnopqrstuvwxyz"),
                ("instance", "10.0.0.1:8080"),
                ("job", "mimir-ingester"),
            ],
        ),
        (
            "tenant/with/slashes:and:colons",
            "metric:with:colons",
            vec![
                ("__name__", "metric:with:colons"),
                ("a", ""),
                ("b", "val"),
            ],
        ),
        (
            "single-char",
            "x",
            vec![("__name__", "x"), ("y", "z")],
        ),
    ];

    // Add 55 deterministic varied cases to exceed 60+ total cases.
    for i in 0..55 {
        let user: &'static str = format!("tenant-{:03}-{}", i, if i % 3 == 0 { "αβγ" } else { "org" }).leak();
        let metric: &'static str = format!("cortex_metric_{i}_total").leak();
        let mut lbls: Vec<(&'static str, &'static str)> = vec![("__name__", metric)];
        let num_extra = (i % 6) + 1;
        for j in 0..num_extra {
            let k: &'static str = format!("label_{}", (j * 7 + i) % 13).leak();
            let v: &'static str = format!("val_{}_{}", i, j).leak();
            lbls.push((k, v));
        }
        cases.push((user, metric, lbls));
    }

    assert!(cases.len() >= 60);

    for (idx, (user_id, metric_name, raw_labels)) in cases.iter().enumerate() {
        let labels_json: Vec<LabelJson> = raw_labels
            .iter()
            .map(|(k, v)| LabelJson {
                name: (*k).to_string(),
                value: (*v).to_string(),
            })
            .collect();

        let resp: HashSeriesResponse = invoke_oracle(
            "hash-series",
            &HashSeriesRequest {
                user_id: (*user_id).to_string(),
                metric_name: (*metric_name).to_string(),
                labels: labels_json,
            },
        )
        .unwrap_or_else(|e| panic!("case {idx} go-oracle failed: {e}"));

        assert_eq!(
            shard_by_user(user_id),
            resp.shard_by_user,
            "case {idx} shard_by_user mismatch"
        );
        assert_eq!(
            shard_by_metric_name(user_id, metric_name),
            resp.shard_by_metric_name,
            "case {idx} shard_by_metric_name mismatch"
        );

        // Go's labels.NewBuilder sorts labels lexicographically by name, drops empty values,
        // and deduplicates by name (last wins) before ShardByAllLabels iterates them.
        let mut sorted = raw_labels.clone();
        sorted.sort_by(|a, b| a.0.cmp(b.0).then_with(|| a.1.cmp(b.1)));
        let mut deduped: Vec<(&str, &str)> = Vec::with_capacity(sorted.len());
        for (k, v) in sorted {
            if let Some(last) = deduped.last_mut() {
                if last.0 == k {
                    last.1 = v;
                    continue;
                }
            }
            deduped.push((k, v));
        }
        deduped.retain(|(_, v)| !v.is_empty());
        assert_eq!(
            shard_by_all_labels(user_id, deduped),
            resp.shard_by_all_labels,
            "case {idx} shard_by_all_labels mismatch"
        );
    }
}

fn build_rich_write_request_json(mode: &str) -> WriteRequestJson {
    WriteRequestJson {
        mode: mode.to_string(),
        source: "RULE".to_string(),
        skip_label_validation: true,
        skip_label_count_validation: true,
        timeseries: vec![
            TimeSeriesJson {
                labels: vec![
                    LabelJson {
                        name: "__name__".to_string(),
                        value: "cortex_request_duration_seconds_bucket".to_string(),
                    },
                    LabelJson {
                        name: "job".to_string(),
                        value: "kube-system/kubelet".to_string(),
                    },
                    LabelJson {
                        name: "le".to_string(),
                        value: "0.5".to_string(),
                    },
                ],
                samples: vec![
                    SampleJson {
                        timestamp_ms: 1_700_000_000_000,
                        value: 0.0,
                    },
                    SampleJson {
                        timestamp_ms: 1_700_000_015_000,
                        value: -42.125,
                    },
                    SampleJson {
                        timestamp_ms: 1_700_000_030_000,
                        value: 1e308,
                    },
                    SampleJson {
                        timestamp_ms: 1_700_000_045_000,
                        value: 5e-324,
                    },
                ],
                exemplars: vec![ExemplarJson {
                    labels: vec![
                        LabelJson {
                            name: "job".to_string(),
                            value: "default/kubernetes".to_string(),
                        },
                        LabelJson {
                            name: "trace_id".to_string(),
                            value: "0123456789abcdef".to_string(),
                        },
                    ],
                    value: 0.42,
                    timestamp_ms: 1_700_000_015_123,
                }],
                histograms: vec![],
                created_timestamp: 1_699_999_000_000,
            },
            TimeSeriesJson {
                labels: vec![
                    LabelJson {
                        name: "__name__".to_string(),
                        value: "storage_operation_duration_seconds_bucket".to_string(),
                    },
                    LabelJson {
                        name: "operation".to_string(),
                        value: "append".to_string(),
                    },
                ],
                samples: vec![],
                exemplars: vec![],
                histograms: vec![
                    // Integer histogram with positive + negative buckets and custom values
                    HistogramJson {
                        timestamp_ms: 1_700_000_000_000,
                        schema: -2,
                        zero_threshold: 0.001,
                        zero_count: 7,
                        count: 25,
                        sum: 123.75,
                        reset_hint: 1, // YES
                        is_float: false,
                        positive_spans: vec![
                            SpanJson {
                                offset: 0,
                                length: 2,
                            },
                            SpanJson {
                                offset: 3,
                                length: 1,
                            },
                        ],
                        positive_deltas: vec![5, -2, 4],
                        negative_spans: vec![SpanJson {
                            offset: -1,
                            length: 2,
                        }],
                        negative_deltas: vec![2, -1],
                        custom_values: vec![0.1, 0.5, 1.0],
                        ..Default::default()
                    },
                    // Float histogram with positive + negative buckets
                    HistogramJson {
                        timestamp_ms: 1_700_000_015_000,
                        schema: 3,
                        zero_threshold: 1e-12,
                        zero_count_float: 1.5,
                        count_float: 18.25,
                        sum: 99.5,
                        reset_hint: 3, // GAUGE
                        is_float: true,
                        positive_spans: vec![SpanJson {
                            offset: 1,
                            length: 2,
                        }],
                        positive_counts: vec![6.25, 8.5],
                        negative_spans: vec![SpanJson {
                            offset: -2,
                            length: 1,
                        }],
                        negative_counts: vec![2.0],
                        custom_values: vec![0.25, 2.5],
                        ..Default::default()
                    },
                ],
                created_timestamp: 0,
            },
        ],
        metadata: vec![
            MetadataJson {
                metric_type: "HISTOGRAM".to_string(),
                metric_family_name: "cortex_request_duration_seconds".to_string(),
                help: "Request duration".to_string(),
                unit: "seconds".to_string(),
            },
            MetadataJson {
                metric_type: "COUNTER".to_string(),
                metric_family_name: "http_requests_total".to_string(),
                help: "Total HTTP requests".to_string(),
                unit: "".to_string(),
            },
        ],
    }
}

fn json_to_rust_write_request(req: &WriteRequestJson) -> cortexpb::WriteRequest {
    let source = match req.source.as_str() {
        "RULE" => cortexpb::write_request::SourceEnum::Rule as i32,
        "OTLP" => cortexpb::write_request::SourceEnum::Otlp as i32,
        _ => cortexpb::write_request::SourceEnum::Api as i32,
    };
    let metadata = req
        .metadata
        .iter()
        .map(|m| {
            let mt = cortexpb::metric_metadata::MetricType::from_str_name(&m.metric_type)
                .unwrap_or(cortexpb::metric_metadata::MetricType::Unknown)
                as i32;
            cortexpb::MetricMetadata {
                r#type: mt,
                metric_family_name: m.metric_family_name.clone(),
                help: m.help.clone(),
                unit: m.unit.clone(),
            }
        })
        .collect();
    let timeseries = req
        .timeseries
        .iter()
        .map(|ts| cortexpb::TimeSeries {
            labels: ts
                .labels
                .iter()
                .map(|l| cortexpb::LabelPair::from_strs(&l.name, &l.value))
                .collect(),
            samples: ts
                .samples
                .iter()
                .map(|s| cortexpb::Sample {
                    value: s.value,
                    timestamp_ms: s.timestamp_ms,
                })
                .collect(),
            exemplars: ts
                .exemplars
                .iter()
                .map(|e| cortexpb::Exemplar {
                    labels: e
                        .labels
                        .iter()
                        .map(|l| cortexpb::LabelPair::from_strs(&l.name, &l.value))
                        .collect(),
                    value: e.value,
                    timestamp_ms: e.timestamp_ms,
                })
                .collect(),
            histograms: ts
                .histograms
                .iter()
                .map(|h| {
                    let (count, zero_count) = if h.is_float {
                        (
                            Some(cortexpb::histogram::Count::CountFloat(h.count_float)),
                            Some(cortexpb::histogram::ZeroCount::ZeroCountFloat(
                                h.zero_count_float,
                            )),
                        )
                    } else {
                        (
                            Some(cortexpb::histogram::Count::CountInt(h.count)),
                            Some(cortexpb::histogram::ZeroCount::ZeroCountInt(h.zero_count)),
                        )
                    };
                    cortexpb::Histogram {
                        sum: h.sum,
                        schema: h.schema,
                        zero_threshold: h.zero_threshold,
                        negative_spans: h
                            .negative_spans
                            .iter()
                            .map(|s| cortexpb::BucketSpan {
                                offset: s.offset,
                                length: s.length,
                            })
                            .collect(),
                        negative_deltas: h.negative_deltas.clone(),
                        negative_counts: h.negative_counts.clone(),
                        positive_spans: h
                            .positive_spans
                            .iter()
                            .map(|s| cortexpb::BucketSpan {
                                offset: s.offset,
                                length: s.length,
                            })
                            .collect(),
                        positive_deltas: h.positive_deltas.clone(),
                        positive_counts: h.positive_counts.clone(),
                        reset_hint: h.reset_hint as i32,
                        timestamp: h.timestamp_ms,
                        custom_values: h.custom_values.clone(),
                        count,
                        zero_count,
                    }
                })
                .collect(),
            created_timestamp: ts.created_timestamp,
        })
        .collect();

    cortexpb::WriteRequest {
        timeseries,
        source,
        metadata,
        symbols_rw2: vec![],
        timeseries_rw2: vec![],
        skip_label_validation: req.skip_label_validation,
        skip_label_count_validation: req.skip_label_count_validation,
    }
}

fn rust_write_request_to_json(req: &cortexpb::WriteRequest, mode: &str) -> WriteRequestJson {
    let source = cortexpb::write_request::SourceEnum::try_from(req.source)
        .unwrap_or(cortexpb::write_request::SourceEnum::Api)
        .as_str_name()
        .to_string();
    let metadata = req
        .metadata
        .iter()
        .map(|m| {
            let mt = cortexpb::metric_metadata::MetricType::try_from(m.r#type)
                .unwrap_or(cortexpb::metric_metadata::MetricType::Unknown)
                .as_str_name()
                .to_string();
            MetadataJson {
                metric_type: mt,
                metric_family_name: m.metric_family_name.clone(),
                help: m.help.clone(),
                unit: m.unit.clone(),
            }
        })
        .collect();
    let timeseries = req
        .timeseries
        .iter()
        .map(|ts| TimeSeriesJson {
            labels: ts
                .labels
                .iter()
                .map(|l| LabelJson {
                    name: String::from_utf8_lossy(&l.name).into_owned(),
                    value: String::from_utf8_lossy(&l.value).into_owned(),
                })
                .collect(),
            samples: ts
                .samples
                .iter()
                .map(|s| SampleJson {
                    timestamp_ms: s.timestamp_ms,
                    value: s.value,
                })
                .collect(),
            exemplars: ts
                .exemplars
                .iter()
                .map(|e| ExemplarJson {
                    labels: e
                        .labels
                        .iter()
                        .map(|l| LabelJson {
                            name: String::from_utf8_lossy(&l.name).into_owned(),
                            value: String::from_utf8_lossy(&l.value).into_owned(),
                        })
                        .collect(),
                    value: e.value,
                    timestamp_ms: e.timestamp_ms,
                })
                .collect(),
            histograms: ts
                .histograms
                .iter()
                .map(|h| {
                    let is_float = h.is_float_histogram();
                    HistogramJson {
                        timestamp_ms: h.timestamp,
                        schema: h.schema,
                        zero_threshold: h.zero_threshold,
                        zero_count: match h.zero_count {
                            Some(cortexpb::histogram::ZeroCount::ZeroCountInt(v)) => v,
                            _ => 0,
                        },
                        zero_count_float: match h.zero_count {
                            Some(cortexpb::histogram::ZeroCount::ZeroCountFloat(v)) => v,
                            _ => 0.0,
                        },
                        count: match h.count {
                            Some(cortexpb::histogram::Count::CountInt(v)) => v,
                            _ => 0,
                        },
                        count_float: match h.count {
                            Some(cortexpb::histogram::Count::CountFloat(v)) => v,
                            _ => 0.0,
                        },
                        sum: h.sum,
                        reset_hint: h.reset_hint as u32,
                        is_float,
                        positive_spans: h
                            .positive_spans
                            .iter()
                            .map(|s| SpanJson {
                                offset: s.offset,
                                length: s.length,
                            })
                            .collect(),
                        positive_deltas: h.positive_deltas.clone(),
                        positive_counts: h.positive_counts.clone(),
                        negative_spans: h
                            .negative_spans
                            .iter()
                            .map(|s| SpanJson {
                                offset: s.offset,
                                length: s.length,
                            })
                            .collect(),
                        negative_deltas: h.negative_deltas.clone(),
                        negative_counts: h.negative_counts.clone(),
                        custom_values: h.custom_values.clone(),
                    }
                })
                .collect(),
            created_timestamp: ts.created_timestamp,
        })
        .collect();
    WriteRequestJson {
        mode: mode.to_string(),
        source,
        skip_label_validation: req.skip_label_validation,
        skip_label_count_validation: req.skip_label_count_validation,
        timeseries,
        metadata,
    }
}

#[test]
fn w1_proto_write_request_go_rust_conformance() {
    ensure_go_oracle_built().expect("go-oracle must build");

    let mut scratch = WriteRequestScratch::new();

    for mode in ["classic", "rw2", "kafka-v2"] {
        let mut req_json = build_rich_write_request_json(mode);
        // For rw2/kafka-v2, test metadata attached to a series whose __name__ matches MetricFamilyName
        // AND unmatched metadata (which becomes a synthetic series).
        if mode == "rw2" {
            // In rw2, series[0] has __name__="cortex_request_duration_seconds_bucket" and metadata has
            // MetricFamilyName="cortex_request_duration_seconds" -> unmatched during FromWriteRequestToRW2Request
            // because FromWriteRequestToRW2Request matches exact __name__, and then Go/Rust RW2 decoder
            // normalizes synthetic series or keeps both!
            // Let's test both: exact-match metadata AND suffix-normalized metadata!
            req_json.metadata.push(MetadataJson {
                metric_type: "HISTOGRAM".to_string(),
                metric_family_name: "cortex_request_duration_seconds_bucket".to_string(),
                help: "Matched by exact series name".to_string(),
                unit: "seconds".to_string(),
            });
        } else if mode == "kafka-v2" {
            req_json.metadata.push(MetadataJson {
                metric_type: "HISTOGRAM".to_string(),
                metric_family_name: "storage_operation_duration_seconds_bucket".to_string(),
                help: "Storage latency".to_string(),
                unit: "seconds".to_string(),
            });
        }

        // 1. Go encodes -> Rust prost & Rust zero-copy both decode identically!
        let go_encoded: BytesB64Response =
            invoke_oracle("encode-write-request", &req_json).expect("go encode-write-request");
        let go_wire = B64.decode(&go_encoded.bytes_b64).expect("decode b64");

        // Go also decodes its own wire bytes so we have the canonical expected WriteRequestJson
        let go_decoded: WriteRequestJson = invoke_oracle(
            "decode-write-request",
            &DecodeWriteRequest {
                mode: mode.to_string(),
                bytes_b64: go_encoded.bytes_b64.clone(),
            },
        )
        .expect("go decode-write-request");

        let opts = match mode {
            "classic" => DecodeOptions::classic(),
            "rw2" => DecodeOptions::rw2(),
            "kafka-v2" => DecodeOptions::kafka_v2(),
            _ => unreachable!(),
        };

        // Rust zero-copy decode of Go wire bytes
        let zc_decoded = decode_write_request(&go_wire, opts, &mut scratch)
            .unwrap_or_else(|e| panic!("mode {mode} rust zerocopy decode failed: {e}"));
        let zc_owned = zc_decoded.to_owned_write_request();
        let zc_json = rust_write_request_to_json(&zc_owned, mode);
        assert_eq!(zc_json, go_decoded, "mode {mode} Go->Rust zerocopy mismatch");

        // Rust prost decode of Go wire bytes
        let prost_msg = cortexpb::WriteRequest::decode(go_wire.as_slice())
            .unwrap_or_else(|e| panic!("mode {mode} rust prost decode failed: {e}"));
        if mode == "classic" {
            assert_eq!(prost_msg, zc_owned, "classic prost vs zerocopy mismatch");
        } else {
            assert!(!prost_msg.symbols_rw2.is_empty());
            assert!(!prost_msg.timeseries_rw2.is_empty());
        }

        // 2. Rust encodes -> Go oracle decodes with zero error and 100% field equality!
        let rust_input = json_to_rust_write_request(&req_json);
        let rust_wire = match mode {
            "classic" => rust_input.encode_to_vec(),
            "rw2" => {
                let mut buf = Vec::new();
                encode_write_request_rw2_bytes(&rust_input, 0, None, &mut buf)
                    .expect("rust rw2 encode");
                buf
            }
            "kafka-v2" => {
                let mut buf = Vec::new();
                encode_write_request_rw2_bytes(
                    &rust_input,
                    V2_RECORD_SYMBOL_OFFSET,
                    Some(v2_common_symbols()),
                    &mut buf,
                )
                .expect("rust kafka-v2 encode");
                buf
            }
            _ => unreachable!(),
        };

        let go_decoded_from_rust: WriteRequestJson = invoke_oracle(
            "decode-write-request",
            &DecodeWriteRequest {
                mode: mode.to_string(),
                bytes_b64: B64.encode(&rust_wire),
            },
        )
        .unwrap_or_else(|e| panic!("mode {mode} go decode of rust wire failed: {e}"));

        assert_eq!(
            go_decoded_from_rust, go_decoded,
            "mode {mode} Rust->Go decoded output mismatch vs Go->Go"
        );

        // And Rust zero-copy decodes Rust wire identically!
        let zc_from_rust = decode_write_request(&rust_wire, opts, &mut scratch)
            .expect("rust zerocopy decode of rust wire")
            .to_owned_write_request();
        assert_eq!(
            rust_write_request_to_json(&zc_from_rust, mode),
            go_decoded,
            "mode {mode} Rust->Rust zerocopy mismatch"
        );
    }
}

#[test]
fn w1_proto_nan_inf_stale_nan_bit_exact() {
    let stale_nan = f64::from_bits(0x7ff0_0000_0000_0002);
    let qnan = f64::from_bits(0x7ff8_0000_0000_0001);
    let req = cortexpb::WriteRequest {
        timeseries: vec![cortexpb::TimeSeries {
            labels: vec![
                cortexpb::LabelPair::from_static("__name__", "special_floats"),
                cortexpb::LabelPair::from_static("job", "test"),
            ],
            samples: vec![
                cortexpb::Sample {
                    value: stale_nan,
                    timestamp_ms: 100,
                },
                cortexpb::Sample {
                    value: qnan,
                    timestamp_ms: 200,
                },
                cortexpb::Sample {
                    value: f64::INFINITY,
                    timestamp_ms: 300,
                },
                cortexpb::Sample {
                    value: f64::NEG_INFINITY,
                    timestamp_ms: 400,
                },
            ],
            exemplars: vec![cortexpb::Exemplar {
                labels: vec![cortexpb::LabelPair::from_static("trace_id", "nan")],
                value: qnan,
                timestamp_ms: 200,
            }],
            histograms: vec![cortexpb::Histogram {
                sum: f64::INFINITY,
                schema: 0,
                zero_threshold: 0.001,
                count: Some(cortexpb::histogram::Count::CountFloat(stale_nan)),
                zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountFloat(
                    f64::NEG_INFINITY,
                )),
                timestamp: 500,
                ..Default::default()
            }],
            created_timestamp: 50,
        }],
        source: cortexpb::write_request::SourceEnum::Otlp as i32,
        ..Default::default()
    };

    let mut scratch = WriteRequestScratch::new();

    for (mode, opts) in [
        ("classic", DecodeOptions::classic()),
        ("rw2", DecodeOptions::rw2()),
        ("kafka-v2", DecodeOptions::kafka_v2()),
    ] {
        let wire = match mode {
            "classic" => req.encode_to_vec(),
            "rw2" => {
                let mut b = Vec::new();
                encode_write_request_rw2_bytes(&req, 0, None, &mut b).unwrap();
                b
            }
            "kafka-v2" => {
                let mut b = Vec::new();
                encode_write_request_rw2_bytes(
                    &req,
                    V2_RECORD_SYMBOL_OFFSET,
                    Some(v2_common_symbols()),
                    &mut b,
                )
                .unwrap();
                b
            }
            _ => unreachable!(),
        };

        let decoded = decode_write_request(&wire, opts, &mut scratch).unwrap();
        let owned = decoded.to_owned_write_request();
        assert_eq!(owned.timeseries.len(), 1);
        let samples = &owned.timeseries[0].samples;
        assert_eq!(samples[0].value.to_bits(), stale_nan.to_bits());
        assert_eq!(samples[1].value.to_bits(), qnan.to_bits());
        assert_eq!(samples[2].value.to_bits(), f64::INFINITY.to_bits());
        assert_eq!(samples[3].value.to_bits(), f64::NEG_INFINITY.to_bits());
        assert_eq!(owned.timeseries[0].exemplars[0].value.to_bits(), qnan.to_bits());

        let h = &owned.timeseries[0].histograms[0];
        assert_eq!(h.sum.to_bits(), f64::INFINITY.to_bits());
        match h.count {
            Some(cortexpb::histogram::Count::CountFloat(v)) => {
                assert_eq!(v.to_bits(), stale_nan.to_bits());
            }
            other => panic!("unexpected count: {other:?}"),
        }
        match h.zero_count {
            Some(cortexpb::histogram::ZeroCount::ZeroCountFloat(v)) => {
                assert_eq!(v.to_bits(), f64::NEG_INFINITY.to_bits());
            }
            other => panic!("unexpected zero_count: {other:?}"),
        }
    }

    // Also verify prost::Message::decode directly on classic wire
    let classic_wire = req.encode_to_vec();
    let prost_dec = cortexpb::WriteRequest::decode(classic_wire.as_slice()).unwrap();
    assert_eq!(
        prost_dec.timeseries[0].samples[0].value.to_bits(),
        stale_nan.to_bits()
    );
    let _ = Bytes::from_static(b"ok");
}
