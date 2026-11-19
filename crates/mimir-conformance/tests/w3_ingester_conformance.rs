//! Wave 3 Task W3-A Conformance Tests:
//! Full live Tonic gRPC + Axum HTTP + Go TSDB oracle + Kafka kfake tests for `mimir-ingester`.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mimir_common::block_on_for_tests;
use mimir_common::tenant::LOWER_ORG_ID_HEADER_NAME;
use mimir_config::{
    BlocksStorageConfig, GoDuration, IngestStorageConfig, IngesterConfig, LimitsConfig,
    ModelDuration, Overrides,
};
use mimir_conformance::oracle::{
    invoke_oracle, ChunkSampleJson, KfakeBrokerRequest, KfakeRecordInput, LabelJson, SampleJson,
    SpawnedKfakeBroker, TimeSeriesJson, VerifyTsdbDirRequest, VerifyTsdbDirResponse,
    WriteRequestJson,
};
use mimir_ingester::{ingester_router, Ingester, IngesterService};
use mimir_proto::cortexpb;
use mimir_proto::grpcutilpb;
use mimir_proto::ingesterpb;
use mimir_proto::ingesterpb::ingester_client::IngesterClient;
use mimir_tsdb::chunkenc::{EncodedChunk, Encoding};
use mimir_tsdb::compact::decode_chunk_samples;
use prometheus::Registry;
use tokio::net::TcpListener;
use tokio_stream::StreamExt;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, Server};

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn wire_enc_to_tsdb(enc: i32) -> Encoding {
    match enc {
        4 => Encoding::Xor,
        5 => Encoding::Histogram,
        6 => Encoding::FloatHistogram,
        7 => Encoding::Xor2,
        8 => Encoding::HistogramSt,
        9 => Encoding::FloatHistogramSt,
        other => panic!("unexpected wire chunk encoding {other}"),
    }
}

fn make_blocks_cfg(tsdb_dir: &std::path::Path, bucket_dir: Option<&std::path::Path>) -> BlocksStorageConfig {
    let mut blocks_cfg = BlocksStorageConfig::default();
    blocks_cfg.tsdb.dir = tsdb_dir.to_string_lossy().into_owned();
    blocks_cfg.tsdb.head_compaction_interval = GoDuration::from_secs(60);
    blocks_cfg.tsdb.head_compaction_idle_timeout = GoDuration::from_hours(1);
    if let Some(bdir) = bucket_dir {
        blocks_cfg.bucket.backend_cfg.backend = "filesystem".to_string();
        blocks_cfg.bucket.backend_cfg.filesystem.dir = bdir.to_string_lossy().into_owned();
        blocks_cfg.tsdb.ship_interval = GoDuration::from_secs(60);
    } else {
        blocks_cfg.tsdb.ship_interval = GoDuration::ZERO;
    }
    blocks_cfg
}

async fn spawn_ingester_servers(
    ingester: Arc<Ingester>,
) -> (
    SocketAddr,
    SocketAddr,
    IngesterClient<Channel>,
    tokio::sync::oneshot::Sender<()>,
) {
    let grpc_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let grpc_addr = grpc_listener.local_addr().unwrap();
    let grpc_incoming = tokio_stream::wrappers::TcpListenerStream::new(grpc_listener);

    let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_addr = http_listener.local_addr().unwrap();

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let svc = IngesterService::new(Arc::clone(&ingester)).into_server();
    let router = ingester_router(Arc::clone(&ingester));

    tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(svc)
            .serve_with_incoming_shutdown(grpc_incoming, async {
                let _ = shutdown_rx.await;
            })
            .await;
    });

    tokio::spawn(async move {
        let _ = axum::serve(http_listener, router).await;
    });

    let endpoint = format!("http://{grpc_addr}");
    let client = IngesterClient::connect(endpoint).await.unwrap();
    (grpc_addr, http_addr, client, shutdown_tx)
}

fn with_tenant<T>(tenant: &str, msg: T) -> tonic::Request<T> {
    let mut req = tonic::Request::new(msg);
    req.metadata_mut().insert(
        LOWER_ORG_ID_HEADER_NAME,
        MetadataValue::try_from(tenant).unwrap(),
    );
    req
}

#[test]
fn test_ingester_all_14_rpcs_and_query_stream_two_phase_protocol() {
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().unwrap();
        let tsdb_dir = tmp.path().join("tsdb");
        let reg = Registry::new();

        let limits = LimitsConfig {
            out_of_order_time_window: ModelDuration::from_hours(1),
            max_global_exemplars_per_user: 1000,
            native_histograms_ingestion_enabled: true,
            ..Default::default()
        };
        let overrides = Overrides::new(limits);

        let mut cfg = IngesterConfig::default();
        cfg.active_series.active_series_metrics_enabled = true;
        cfg.ring.instance_id = "ingester-0".to_string();

        let blocks_cfg = make_blocks_cfg(&tsdb_dir, None);
        let ingester = Ingester::new(
            cfg,
            blocks_cfg,
            IngestStorageConfig::default(),
            overrides,
            None,
            None,
            None,
            None,
            None,
            Some(&reg),
        )
        .unwrap();
        ingester.start().await.unwrap();

        let (_grpc_addr, _http_addr, mut client, shutdown_tx) =
            spawn_ingester_servers(Arc::clone(&ingester)).await;

        let base_ts = now_ms() - 60_000;
        let tenant = "tenant-alpha";

        // 1. Push: float series (in-order + OOO), integer histogram, float histogram, exemplar, metadata
        let push_req = cortexpb::WriteRequest {
            source: cortexpb::write_request::SourceEnum::Api as i32,
            metadata: vec![
                cortexpb::MetricMetadata {
                    r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
                    metric_family_name: "http_requests_total".to_string(),
                    help: "Total HTTP requests".to_string(),
                    unit: "requests".to_string(),
                },
                cortexpb::MetricMetadata {
                    r#type: cortexpb::metric_metadata::MetricType::Histogram as i32,
                    metric_family_name: "request_duration_seconds".to_string(),
                    help: "Request latency".to_string(),
                    unit: "seconds".to_string(),
                },
            ],
            timeseries: vec![
                cortexpb::TimeSeries {
                    labels: vec![
                        cortexpb::LabelPair::from_static("__name__", "http_requests_total"),
                        cortexpb::LabelPair::from_static("job", "api"),
                        cortexpb::LabelPair::from_static("method", "GET"),
                    ],
                    samples: vec![
                        cortexpb::Sample {
                            value: 10.0,
                            timestamp_ms: base_ts + 1000,
                        },
                        cortexpb::Sample {
                            value: 25.0,
                            timestamp_ms: base_ts + 3000,
                        },
                        // Out-of-order sample within 1h window
                        cortexpb::Sample {
                            value: 15.0,
                            timestamp_ms: base_ts + 2000,
                        },
                    ],
                    exemplars: vec![cortexpb::Exemplar {
                        labels: vec![cortexpb::LabelPair::from_static("trace_id", "trace-123")],
                        value: 1.0,
                        timestamp_ms: base_ts + 3000,
                    }],
                    histograms: Vec::new(),
                    created_timestamp: 0,
                },
                cortexpb::TimeSeries {
                    labels: vec![
                        cortexpb::LabelPair::from_static("__name__", "http_requests_total"),
                        cortexpb::LabelPair::from_static("job", "worker"),
                        cortexpb::LabelPair::from_static("method", "POST"),
                    ],
                    samples: vec![cortexpb::Sample {
                        value: 5.0,
                        timestamp_ms: base_ts + 1000,
                    }],
                    exemplars: Vec::new(),
                    histograms: Vec::new(),
                    created_timestamp: 0,
                },
                cortexpb::TimeSeries {
                    labels: vec![
                        cortexpb::LabelPair::from_static("__name__", "request_duration_seconds"),
                        cortexpb::LabelPair::from_static("job", "api"),
                    ],
                    samples: Vec::new(),
                    exemplars: Vec::new(),
                    histograms: vec![cortexpb::Histogram {
                        count: Some(cortexpb::histogram::Count::CountInt(5)),
                        sum: 1.5,
                        schema: 0,
                        zero_threshold: 0.001,
                        zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountInt(1)),
                        positive_spans: vec![cortexpb::BucketSpan {
                            offset: 0,
                            length: 2,
                        }],
                        positive_deltas: vec![2, 0],
                        positive_counts: Vec::new(),
                        negative_spans: Vec::new(),
                        negative_deltas: Vec::new(),
                        negative_counts: Vec::new(),
                        reset_hint: cortexpb::histogram::ResetHint::No as i32,
                        timestamp: base_ts + 1000,
                        custom_values: Vec::new(),
                    }],
                    created_timestamp: 0,
                },
            ],
            ..Default::default()
        };

        client.push(with_tenant(tenant, push_req)).await.unwrap();

        // Push for a second tenant with RULE source for AllUserStats coverage
        let push_t2 = cortexpb::WriteRequest {
            source: cortexpb::write_request::SourceEnum::Rule as i32,
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_static("__name__", "up"),
                    cortexpb::LabelPair::from_static("job", "rule_eval"),
                ],
                samples: vec![cortexpb::Sample {
                    value: 1.0,
                    timestamp_ms: base_ts + 1000,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        client
            .push(with_tenant("tenant-beta", push_t2))
            .await
            .unwrap();

        // 2. QueryStream: verify exact two-phase series-then-chunks protocol and chunk decoding
        let q_req = ingesterpb::QueryRequest {
            start_timestamp_ms: base_ts,
            end_timestamp_ms: base_ts + 10_000,
            matchers: vec![ingesterpb::LabelMatcher {
                r#type: ingesterpb::MatchType::Equal as i32,
                name: "__name__".to_string(),
                value: "http_requests_total".to_string(),
            }],
            streaming_chunks_batch_size: 1,
        };
        let mut q_stream = client
            .query_stream(with_tenant(tenant, q_req))
            .await
            .unwrap()
            .into_inner();
        let mut series_meta = Vec::new();
        let mut saw_end_of_series = false;
        let mut decoded_by_series: BTreeMap<u64, Vec<(i64, f64)>> = BTreeMap::new();

        while let Some(resp) = q_stream.next().await {
            let msg = resp.unwrap();
            if !msg.streaming_series.is_empty() || msg.is_end_of_series_stream {
                assert!(
                    !saw_end_of_series,
                    "received series batch after is_end_of_series_stream=true"
                );
                assert!(msg.streaming_series_chunks.is_empty());
                series_meta.extend(msg.streaming_series);
                if msg.is_end_of_series_stream {
                    saw_end_of_series = true;
                }
            } else {
                assert!(
                    saw_end_of_series,
                    "received chunks before is_end_of_series_stream=true"
                );
                for sc in msg.streaming_series_chunks {
                    let entry = decoded_by_series.entry(sc.series_index).or_default();
                    for ch in sc.chunks {
                        let enc = wire_enc_to_tsdb(ch.encoding);
                        let chunk = EncodedChunk::from_bytes(enc, ch.data.to_vec()).unwrap();
                        for s in decode_chunk_samples(&chunk).unwrap() {
                            if let mimir_tsdb::compact::SampleValue::Float(v) = s.value {
                                entry.push((s.t, v));
                            }
                        }
                    }
                }
            }
        }
        assert!(saw_end_of_series);
        assert_eq!(series_meta.len(), 2);
        // Sorted series: job="api" first, then job="worker"
        let api_samples = &decoded_by_series[&0];
        assert_eq!(
            api_samples,
            &vec![
                (base_ts + 1000, 10.0),
                (base_ts + 2000, 15.0),
                (base_ts + 3000, 25.0)
            ]
        );
        assert_eq!(decoded_by_series[&1], vec![(base_ts + 1000, 5.0)]);

        // 3. QueryExemplars
        let ex_resp = client
            .query_exemplars(with_tenant(
                tenant,
                ingesterpb::ExemplarQueryRequest {
                    start_timestamp_ms: base_ts,
                    end_timestamp_ms: base_ts + 10_000,
                    matchers: vec![ingesterpb::LabelMatchers {
                        matchers: vec![ingesterpb::LabelMatcher {
                            r#type: ingesterpb::MatchType::Equal as i32,
                            name: "__name__".to_string(),
                            value: "http_requests_total".to_string(),
                        }],
                    }],
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(ex_resp.timeseries.len(), 1);
        assert_eq!(ex_resp.timeseries[0].exemplars.len(), 1);
        assert_eq!(ex_resp.timeseries[0].exemplars[0].value, 1.0);

        // 4. LabelNames
        let ln_resp = client
            .label_names(with_tenant(
                tenant,
                ingesterpb::LabelNamesRequest {
                    start_timestamp_ms: base_ts,
                    end_timestamp_ms: base_ts + 10_000,
                    matchers: None,
                    limit: 0,
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(ln_resp.label_names, vec!["__name__", "job", "method"]);

        // 5. LabelValues
        let lv_resp = client
            .label_values(with_tenant(
                tenant,
                ingesterpb::LabelValuesRequest {
                    label_name: "job".to_string(),
                    start_timestamp_ms: base_ts,
                    end_timestamp_ms: base_ts + 10_000,
                    matchers: None,
                    limit: 0,
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(lv_resp.label_values, vec!["api", "worker"]);

        // 6. UserStats (IN_MEMORY & ACTIVE)
        let us_mem = client
            .user_stats(with_tenant(
                tenant,
                ingesterpb::UserStatsRequest {
                    count_method: ingesterpb::CountMethod::InMemory as i32,
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(us_mem.num_series, 3);

        let us_act = client
            .user_stats(with_tenant(
                tenant,
                ingesterpb::UserStatsRequest {
                    count_method: ingesterpb::CountMethod::Active as i32,
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(us_act.num_series, 3);

        // 7. AllUserStats (not per tenant, but behind dskit's gRPC auth interceptor like every
        // cortex.Ingester RPC: Go's distributor injects a fake org ID).
        let all_stats = client
            .all_user_stats(with_tenant(
                "fake",
                ingesterpb::UserStatsRequest {
                    count_method: ingesterpb::CountMethod::InMemory as i32,
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(all_stats.stats.len(), 2);

        // 8. MetricsForLabelMatchers
        let mflm = client
            .metrics_for_label_matchers(with_tenant(
                tenant,
                ingesterpb::MetricsForLabelMatchersRequest {
                    start_timestamp_ms: base_ts,
                    end_timestamp_ms: base_ts + 10_000,
                    matchers_set: vec![ingesterpb::LabelMatchers {
                        matchers: vec![ingesterpb::LabelMatcher {
                            r#type: ingesterpb::MatchType::Equal as i32,
                            name: "job".to_string(),
                            value: "api".to_string(),
                        }],
                    }],
                    limit: 0,
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(mflm.metric.len(), 2);

        // 9. MetricsMetadata (metric_names + deprecated metric + limit)
        let mm1 = client
            .metrics_metadata(with_tenant(
                tenant,
                ingesterpb::MetricsMetadataRequest {
                    limit: -1,
                    limit_per_metric: -1,
                    metric: String::new(),
                    metric_names: vec!["http_requests_total".to_string()],
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(mm1.metadata.len(), 1);
        assert_eq!(mm1.metadata[0].help, "Total HTTP requests");

        let mm2 = client
            .metrics_metadata(with_tenant(
                tenant,
                ingesterpb::MetricsMetadataRequest {
                    limit: -1,
                    limit_per_metric: -1,
                    metric: "request_duration_seconds".to_string(),
                    metric_names: Vec::new(),
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(mm2.metadata.len(), 1);
        assert_eq!(mm2.metadata[0].unit, "seconds");

        // 10. LabelNamesAndValues (streaming)
        let mut lnv_stream = client
            .label_names_and_values(with_tenant(
                tenant,
                ingesterpb::LabelNamesAndValuesRequest {
                    matchers: Vec::new(),
                    count_method: ingesterpb::CountMethod::Active as i32,
                },
            ))
            .await
            .unwrap()
            .into_inner();
        let mut lnv_map: BTreeMap<String, Vec<String>> = BTreeMap::new();
        while let Some(item) = lnv_stream.next().await {
            for lv in item.unwrap().items {
                lnv_map.entry(lv.label_name).or_default().extend(lv.values);
            }
        }
        assert_eq!(lnv_map["job"], vec!["api", "worker"]);

        // 11. LabelValuesCardinality (streaming)
        let mut lvc_stream = client
            .label_values_cardinality(with_tenant(
                tenant,
                ingesterpb::LabelValuesCardinalityRequest {
                    label_names: vec!["job".to_string()],
                    matchers: Vec::new(),
                    count_method: ingesterpb::CountMethod::Active as i32,
                },
            ))
            .await
            .unwrap()
            .into_inner();
        let mut job_card = BTreeMap::new();
        while let Some(item) = lvc_stream.next().await {
            for it in item.unwrap().items {
                if it.label_name == "job" {
                    for (k, v) in it.label_value_series {
                        job_card.insert(k, v);
                    }
                }
            }
        }
        assert_eq!(job_card.get("api"), Some(&2));
        assert_eq!(job_card.get("worker"), Some(&1));

        // 12. ActiveSeries (SERIES & NATIVE_HISTOGRAM_SERIES)
        let all_name_matcher = vec![ingesterpb::LabelMatcher {
            r#type: ingesterpb::MatchType::RegexMatch as i32,
            name: "__name__".to_string(),
            value: ".+".to_string(),
        }];
        let mut as_stream = client
            .active_series(with_tenant(
                tenant,
                ingesterpb::ActiveSeriesRequest {
                    matchers: all_name_matcher.clone(),
                    r#type: ingesterpb::active_series_request::RequestType::Series as i32,
                },
            ))
            .await
            .unwrap()
            .into_inner();
        let mut active_count = 0usize;
        while let Some(item) = as_stream.next().await {
            active_count += item.unwrap().metric.len();
        }
        assert_eq!(active_count, 3);

        let mut nh_stream = client
            .active_series(with_tenant(
                tenant,
                ingesterpb::ActiveSeriesRequest {
                    matchers: all_name_matcher,
                    r#type:
                        ingesterpb::active_series_request::RequestType::NativeHistogramSeries as i32,
                },
            ))
            .await
            .unwrap()
            .into_inner();
        let mut nh_buckets = Vec::new();
        while let Some(item) = nh_stream.next().await {
            let msg = item.unwrap();
            assert_eq!(msg.metric.len(), msg.bucket_count.len());
            nh_buckets.extend(msg.bucket_count);
        }
        assert_eq!(nh_buckets, vec![2]);

        // 13. SearchLabelNames
        let mut sln_stream = client
            .search_label_names(with_tenant(
                tenant,
                ingesterpb::SearchLabelNamesRequest {
                    start_timestamp_ms: base_ts,
                    end_timestamp_ms: base_ts + 10_000,
                    matchers: Vec::new(),
                    filter: Some(ingesterpb::SearchFilter {
                        terms: vec!["meth".to_string()],
                        case_insensitive: true,
                        fuzz_alg: ingesterpb::search_filter::FuzzAlg::Substring as i32,
                        fuzz_threshold: 0,
                    }),
                    ordering: ingesterpb::SearchOrdering::OrderByValueAsc as i32,
                    limit: 10,
                },
            ))
            .await
            .unwrap()
            .into_inner();
        let mut found_names = Vec::new();
        while let Some(batch) = sln_stream.next().await {
            for r in batch.unwrap().results {
                found_names.push(r.value);
            }
        }
        assert_eq!(found_names, vec!["method"]);

        // 14. SearchLabelValues
        let mut slv_stream = client
            .search_label_values(with_tenant(
                tenant,
                ingesterpb::SearchLabelValuesRequest {
                    start_timestamp_ms: base_ts,
                    end_timestamp_ms: base_ts + 10_000,
                    name: "__name__".to_string(),
                    matchers: Vec::new(),
                    filter: Some(ingesterpb::SearchFilter {
                        terms: vec!["hrt".to_string()],
                        case_insensitive: false,
                        fuzz_alg: ingesterpb::search_filter::FuzzAlg::Subsequence as i32,
                        fuzz_threshold: 0,
                    }),
                    ordering: ingesterpb::SearchOrdering::OrderByScoreDesc as i32,
                    limit: 5,
                },
            ))
            .await
            .unwrap()
            .into_inner();
        let mut found_vals = Vec::new();
        while let Some(batch) = slv_stream.next().await {
            for r in batch.unwrap().results {
                found_vals.push(r.value);
            }
        }
        assert_eq!(found_vals, vec!["http_requests_total"]);

        // Verify Prometheus metrics registered and populated
        let mfs = reg.gather();
        assert!(mfs
            .iter()
            .any(|mf| mf.get_name() == "cortex_ingester_ingested_samples_total"));
        assert!(mfs
            .iter()
            .any(|mf| mf.get_name() == "cortex_ingester_queries_total"));

        let _ = shutdown_tx.send(());
        ingester.stop().await.unwrap();
    });
}

#[test]
fn test_ingester_validation_and_limit_errors_exact_details() {
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().unwrap();
        let tsdb_dir = tmp.path().join("tsdb");

        let limits = LimitsConfig {
            max_global_series_per_user: 1,
            out_of_order_time_window: ModelDuration::ZERO,
            ..Default::default()
        };
        let overrides = Overrides::new(limits);

        let mut cfg = IngesterConfig::default();
        cfg.ring.instance_id = "ingester-0".to_string();
        let blocks_cfg = make_blocks_cfg(&tsdb_dir, None);

        let ingester = Ingester::new(
            cfg,
            blocks_cfg,
            IngestStorageConfig::default(),
            overrides,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        ingester.start().await.unwrap();

        let (_grpc_addr, _http_addr, mut client, shutdown_tx) =
            spawn_ingester_servers(Arc::clone(&ingester)).await;

        let base_ts = now_ms() - 10_000;
        let tenant = "tenant-err";

        // 1. Unsorted labels -> InvalidArgument + BadData + soft + RejectedSamples
        let unsorted_req = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_static("z", "1"),
                    cortexpb::LabelPair::from_static("__name__", "m1"),
                ],
                samples: vec![cortexpb::Sample {
                    value: 1.0,
                    timestamp_ms: base_ts,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let status = client
            .push(with_tenant(tenant, unsorted_req))
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        let details = grpcutilpb::extract_cortex_error_details(&status).unwrap();
        assert_eq!(details.cause, cortexpb::ErrorCause::BadData as i32);
        assert!(details.soft);
        assert_eq!(details.rejected_samples, 1);

        // 2. Partial commit: Go `headAppenderV2` silently drops intra-batch conflicts on a freshly
        // created series at Commit(), so first commit `10.0 @ base_ts+2000`, then send a second
        // WriteRequest containing a valid newer sample + conflicting duplicate + OOO sample ->
        // commits the valid sample, returns BadData with RejectedSamples=2.
        let seed_req = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_static("__name__", "m1"),
                    cortexpb::LabelPair::from_static("a", "1"),
                ],
                samples: vec![cortexpb::Sample {
                    value: 10.0,
                    timestamp_ms: base_ts + 2000,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        client.push(with_tenant(tenant, seed_req)).await.unwrap();

        let partial_req = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_static("__name__", "m1"),
                    cortexpb::LabelPair::from_static("a", "1"),
                ],
                samples: vec![
                    cortexpb::Sample {
                        value: 99.0, // duplicate timestamp, different value
                        timestamp_ms: base_ts + 2000,
                    },
                    cortexpb::Sample {
                        value: 5.0, // OOO outside window (OOO disabled)
                        timestamp_ms: base_ts + 1000,
                    },
                    cortexpb::Sample {
                        value: 20.0, // valid newer sample
                        timestamp_ms: base_ts + 3000,
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        };
        let status = client
            .push(with_tenant(tenant, partial_req))
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        let details = grpcutilpb::extract_cortex_error_details(&status).unwrap();
        assert_eq!(details.cause, cortexpb::ErrorCause::BadData as i32);
        assert!(details.soft);
        assert_eq!(details.rejected_samples, 2);

        // Verify the valid samples (10.0 @ +2000 and 20.0 @ +3000) WERE committed!
        let mut qs = client
            .query_stream(with_tenant(
                tenant,
                ingesterpb::QueryRequest {
                    start_timestamp_ms: base_ts,
                    end_timestamp_ms: base_ts + 5000,
                    matchers: vec![ingesterpb::LabelMatcher {
                        r#type: ingesterpb::MatchType::Equal as i32,
                        name: "__name__".to_string(),
                        value: "m1".to_string(),
                    }],
                    streaming_chunks_batch_size: 10,
                },
            ))
            .await
            .unwrap()
            .into_inner();
        let mut series_count = 0usize;
        while let Some(msg) = qs.next().await {
            series_count += msg.unwrap().streaming_series.len();
        }
        assert_eq!(series_count, 1);

        // 3. Per-user series limit (max_global_series_per_user = 1) -> FailedPrecondition + TenantLimit + soft
        let limit_req = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_static("__name__", "m2"),
                    cortexpb::LabelPair::from_static("a", "2"),
                ],
                samples: vec![cortexpb::Sample {
                    value: 2.0,
                    timestamp_ms: base_ts + 3000,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let status = client
            .push(with_tenant(tenant, limit_req))
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        let details = grpcutilpb::extract_cortex_error_details(&status).unwrap();
        assert_eq!(details.cause, cortexpb::ErrorCause::TenantLimit as i32);
        assert!(details.soft);
        assert_eq!(details.rejected_samples, 1);

        // Also verify httpgrpc status code packing roundtrip for TenantLimit -> 400
        let http_err = mimir_ingester::IngesterError::per_user_series_limit_reached(1);
        assert_eq!(http_err.http_status_code(), 400);
        let http_status = http_err.to_httpgrpc_status();
        let http_resp = grpcutilpb::extract_http_response(&http_status).unwrap();
        assert_eq!(http_resp.code, 400);

        let _ = shutdown_tx.send(());
        ingester.stop().await.unwrap();

        // 4. push_grpc_method_enabled = false -> Unimplemented + MethodNotAllowed
        let tmp2 = tempfile::tempdir().unwrap();
        let mut cfg2 = IngesterConfig {
            push_grpc_method_enabled: false,
            ..Default::default()
        };
        cfg2.ring.instance_id = "ingester-0".to_string();
        let ing2 = Ingester::new(
            cfg2,
            make_blocks_cfg(&tmp2.path().join("tsdb"), None),
            IngestStorageConfig::default(),
            Overrides::new(LimitsConfig::default()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        ing2.start().await.unwrap();
        let (_ga2, _ha2, mut client2, stx2) = spawn_ingester_servers(Arc::clone(&ing2)).await;

        let disabled_status = client2
            .push(with_tenant(
                "tenant-x",
                cortexpb::WriteRequest::default(),
            ))
            .await
            .unwrap_err();
        assert_eq!(disabled_status.code(), tonic::Code::Unimplemented);
        let d2 = grpcutilpb::extract_cortex_error_details(&disabled_status).unwrap();
        assert_eq!(d2.cause, cortexpb::ErrorCause::MethodNotAllowed as i32);
        assert!(!d2.soft);

        let _ = stx2.send(());
        ing2.stop().await.unwrap();
    });
}

#[test]
fn test_ingester_http_flush_ships_block_verified_by_go_prometheus_tsdb() {
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().unwrap();
        let tsdb_dir = tmp.path().join("tsdb");
        let bucket_dir = tmp.path().join("bucket");
        std::fs::create_dir_all(&bucket_dir).unwrap();

        let mut cfg = IngesterConfig::default();
        cfg.ring.instance_id = "ingester-0".to_string();
        let blocks_cfg = make_blocks_cfg(&tsdb_dir, Some(&bucket_dir));

        let ingester = Ingester::new(
            cfg,
            blocks_cfg,
            IngestStorageConfig::default(),
            Overrides::new(LimitsConfig::default()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        ingester.start().await.unwrap();

        let (_grpc_addr, http_addr, mut client, shutdown_tx) =
            spawn_ingester_servers(Arc::clone(&ingester)).await;

        let base_ts = now_ms() - 30_000;
        let tenant = "tenant-ship";
        let mut samples = Vec::new();
        for i in 0..10 {
            samples.push(cortexpb::Sample {
                value: (i as f64) * 2.5,
                timestamp_ms: base_ts + i * 1000,
            });
        }
        client
            .push(with_tenant(
                tenant,
                cortexpb::WriteRequest {
                    timeseries: vec![cortexpb::TimeSeries {
                        labels: vec![
                            cortexpb::LabelPair::from_static("__name__", "shipped_metric"),
                            cortexpb::LabelPair::from_static("env", "prod"),
                        ],
                        samples,
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ))
            .await
            .unwrap();

        let http = reqwest::Client::new();

        // Check /ingester/tenants
        let tenants_resp = http
            .get(format!("http://{http_addr}/ingester/tenants"))
            .header("Accept", "application/json")
            .send()
            .await
            .unwrap();
        assert_eq!(tenants_resp.status(), 200);
        let tenants_json: serde_json::Value = tenants_resp.json().await.unwrap();
        assert_eq!(tenants_json["Tenants"][0]["Tenant"], tenant);
        assert_eq!(tenants_json["Tenants"][0]["Series"], 1);

        // Check /ingester/tsdb/:tenant
        let tsdb_resp = http
            .get(format!("http://{http_addr}/ingester/tsdb/{tenant}"))
            .header("Accept", "application/json")
            .send()
            .await
            .unwrap();
        assert_eq!(tsdb_resp.status(), 200);

        // Trigger synchronous flush + ship via /ingester/flush?wait=true
        let flush_resp = http
            .post(format!("http://{http_addr}/ingester/flush?wait=true"))
            .send()
            .await
            .unwrap();
        assert_eq!(flush_resp.status(), 204);

        // Verify block was shipped into bucket_dir/tenant-ship/<ULID>/meta.json
        let tenant_bucket_dir = bucket_dir.join(tenant);
        assert!(tenant_bucket_dir.exists(), "shipped tenant dir must exist");
        let shipped_blocks: Vec<_> = std::fs::read_dir(&tenant_bucket_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_dir())
            .collect();
        assert_eq!(shipped_blocks.len(), 1);
        assert!(shipped_blocks[0].path().join("meta.json").exists());
        assert!(shipped_blocks[0].path().join("index").exists());
        assert!(shipped_blocks[0].path().join("chunks").join("000001").exists());

        // Verify Thanos metadata injected on shipped block while local block remains unmutated
        let shipped_meta: serde_json::Value = serde_json::from_slice(
            &std::fs::read(shipped_blocks[0].path().join("meta.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(shipped_meta["thanos"]["source"], "receive");

        // Differential Go Prometheus TSDB check via target/go-oracle verify-tsdb-dir
        // (tsdb.OpenDBReadOnly expects a wal/ directory alongside blocks)
        std::fs::create_dir_all(tenant_bucket_dir.join("wal")).unwrap();
        let go_verified: VerifyTsdbDirResponse = invoke_oracle(
            "verify-tsdb-dir",
            &VerifyTsdbDirRequest {
                dir: tenant_bucket_dir.to_string_lossy().into_owned(),
                read_only: true,
            },
        )
        .expect("Go prometheus/tsdb must open shipped block cleanly");
        assert_eq!(go_verified.block_ulids.len(), 1);
        assert_eq!(go_verified.series.len(), 1);
        assert_eq!(go_verified.series[0].samples.len(), 10);
        assert_eq!(go_verified.series[0].samples[9].v, 22.5);

        // Also verify the local tenant TSDB dir directly with Go prometheus/tsdb
        let go_local: VerifyTsdbDirResponse = invoke_oracle(
            "verify-tsdb-dir",
            &VerifyTsdbDirRequest {
                dir: tsdb_dir.join(tenant).to_string_lossy().into_owned(),
                read_only: true,
            },
        )
        .expect("Go prometheus/tsdb must open local tenant TSDB cleanly");
        assert_eq!(go_local.block_ulids, go_verified.block_ulids);

        let _ = shutdown_tx.send(());
        ingester.stop().await.unwrap();
    });
}

#[test]
fn test_ingester_kafka_ingest_storage_v2_and_strong_read_consistency() {
    let base_ts = now_ms() - 15_000;
    let tenant = "tenant-kafka";

    let broker = SpawnedKfakeBroker::spawn(&KfakeBrokerRequest {
        topic: "ingest-w3".to_string(),
        partitions: 1,
        compression: Some("snappy".to_string()),
        records: vec![
            KfakeRecordInput {
                partition: 0,
                tenant: tenant.to_string(),
                version: 2,
                compression: None,
                write_request: WriteRequestJson {
                    mode: "rw2".to_string(),
                    source: "API".to_string(),
                    skip_label_validation: false,
                    skip_label_count_validation: false,
                    timeseries: vec![TimeSeriesJson {
                        labels: vec![
                            LabelJson {
                                name: "__name__".to_string(),
                                value: "kafka_ingested_metric".to_string(),
                            },
                            LabelJson {
                                name: "part".to_string(),
                                value: "0".to_string(),
                            },
                        ],
                        samples: vec![
                            SampleJson {
                                timestamp_ms: base_ts + 1000,
                                value: 11.0,
                            },
                            SampleJson {
                                timestamp_ms: base_ts + 2000,
                                value: 22.0,
                            },
                        ],
                        ..Default::default()
                    }],
                    metadata: Vec::new(),
                },
            },
            KfakeRecordInput {
                partition: 0,
                tenant: tenant.to_string(),
                version: 2,
                compression: None,
                write_request: WriteRequestJson {
                    mode: "rw2".to_string(),
                    source: "API".to_string(),
                    skip_label_validation: false,
                    skip_label_count_validation: false,
                    timeseries: vec![TimeSeriesJson {
                        labels: vec![
                            LabelJson {
                                name: "__name__".to_string(),
                                value: "kafka_ingested_metric".to_string(),
                            },
                            LabelJson {
                                name: "part".to_string(),
                                value: "0".to_string(),
                            },
                        ],
                        samples: vec![SampleJson {
                            timestamp_ms: base_ts + 3000,
                            value: 33.0,
                        }],
                        ..Default::default()
                    }],
                    metadata: Vec::new(),
                },
            },
        ],
    })
    .expect("spawn kfake broker");

    let last_offset = broker.offsets()[0].end_offset - 1;
    assert_eq!(last_offset, 1);

    block_on_for_tests(async {
        let tmp = tempfile::tempdir().unwrap();
        let tsdb_dir = tmp.path().join("tsdb");

        let mut cfg = IngesterConfig::default();
        cfg.ring.instance_id = "ingester-0".to_string();
        cfg.push_grpc_method_enabled = false;

        let mut ingest_cfg = IngestStorageConfig {
            enabled: true,
            ..Default::default()
        };
        ingest_cfg.kafka.address.0 = vec![broker.broker_addr().to_string()];
        ingest_cfg.kafka.topic = broker.topic().to_string();
        ingest_cfg.kafka.consume_from_position_at_startup = "start".to_string();
        ingest_cfg.kafka.consumer_group_offset_commit_interval = GoDuration::from_millis(50);
        ingest_cfg.kafka.last_produced_offset_poll_interval = GoDuration::from_millis(25);

        let blocks_cfg = make_blocks_cfg(&tsdb_dir, None);

        let ingester = Ingester::new(
            cfg,
            blocks_cfg,
            ingest_cfg,
            Overrides::new(LimitsConfig::default()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        ingester.start().await.unwrap();

        let (_grpc_addr, _http_addr, mut client, shutdown_tx) =
            spawn_ingester_servers(Arc::clone(&ingester)).await;

        // QueryStream with strong read consistency + explicit consistency offset v1=0:<last_offset>
        let mut req = with_tenant(
            tenant,
            ingesterpb::QueryRequest {
                start_timestamp_ms: base_ts,
                end_timestamp_ms: base_ts + 10_000,
                matchers: vec![ingesterpb::LabelMatcher {
                    r#type: ingesterpb::MatchType::Equal as i32,
                    name: "__name__".to_string(),
                    value: "kafka_ingested_metric".to_string(),
                }],
                streaming_chunks_batch_size: 16,
            },
        );
        req.metadata_mut().insert(
            "__consistency_level__",
            MetadataValue::from_static("strong"),
        );
        req.metadata_mut().insert(
            "__consistency_offsets__",
            MetadataValue::try_from(format!("v1=0:{last_offset}")).unwrap(),
        );

        let mut stream = client.query_stream(req).await.unwrap().into_inner();
        let mut decoded_samples: Vec<ChunkSampleJson> = Vec::new();
        while let Some(resp) = stream.next().await {
            let msg = resp.unwrap();
            for sc in msg.streaming_series_chunks {
                for ch in sc.chunks {
                    let enc = wire_enc_to_tsdb(ch.encoding);
                    let chunk = EncodedChunk::from_bytes(enc, ch.data.to_vec()).unwrap();
                    for s in decode_chunk_samples(&chunk).unwrap() {
                        if let mimir_tsdb::compact::SampleValue::Float(v) = s.value {
                            decoded_samples.push(ChunkSampleJson { st: 0, t: s.t, v });
                        }
                    }
                }
            }
        }

        assert_eq!(
            decoded_samples,
            vec![
                ChunkSampleJson {
                    st: 0,
                    t: base_ts + 1000,
                    v: 11.0
                },
                ChunkSampleJson {
                    st: 0,
                    t: base_ts + 2000,
                    v: 22.0
                },
                ChunkSampleJson {
                    st: 0,
                    t: base_ts + 3000,
                    v: 33.0
                },
            ]
        );

        let _ = shutdown_tx.send(());
        ingester.stop().await.unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(1), async {}).await;
    });
}
