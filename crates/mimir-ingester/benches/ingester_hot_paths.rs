use std::hint::black_box;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use mimir_common::services::State;
use mimir_config::{BlocksStorageConfig, IngestStorageConfig, IngesterConfig, LimitsConfig, Overrides};
use mimir_ingester::active_series::ActiveSeries;
use mimir_ingester::Ingester;
use mimir_proto::{cortexpb, ingesterpb};
use mimir_tsdb::head::{Head, HeadOptions, SeriesEntry};
use std::sync::Arc;
use tonic::metadata::MetadataMap;

fn bench_active_series_and_nanos_slab(c: &mut Criterion) {
    let mut group = c.benchmark_group("ingester_hot_paths/active_series");
    let series_count = 1_000usize;
    let dir = tempfile::tempdir().expect("tempdir");
    let head = Head::open(dir.path(), HeadOptions::default(), i64::MIN).expect("open head");

    let owned_pairs: Vec<[(&str, String); 4]> = (0..series_count)
        .map(|i| {
            [
                ("__name__", "http_requests_total".to_string()),
                ("cluster", "prod-us-east-1".to_string()),
                ("namespace", format!("ns-{}", i % 10)),
                ("pod", format!("pod-{i:05}")),
            ]
        })
        .collect();

    {
        let mut app = head.appender_with_capacity(series_count);
        for (i, p) in owned_pairs.iter().enumerate() {
            let pairs = [
                ("__name__", p[0].1.as_str()),
                ("cluster", p[1].1.as_str()),
                ("namespace", p[2].1.as_str()),
                ("pod", p[3].1.as_str()),
            ];
            app.append_float(None, &pairs, 1_700_000_000_000, i as f64)
                .expect("seed append");
        }
        app.commit().expect("seed commit");
    }

    let entries: Vec<Arc<SeriesEntry>> = owned_pairs
        .iter()
        .map(|p| {
            let pairs = [
                ("__name__", p[0].1.as_str()),
                ("cluster", p[1].1.as_str()),
                ("namespace", p[2].1.as_str()),
                ("pod", p[3].1.as_str()),
            ];
            head.get_entry_by_sorted_pairs(&pairs).expect("entry")
        })
        .collect();

    let tracker = ActiveSeries::new(Vec::new(), Duration::from_secs(600));
    let t0 = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    for (i, p) in owned_pairs.iter().enumerate() {
        let pairs = [
            ("__name__", p[0].1.as_str()),
            ("cluster", p[1].1.as_str()),
            ("namespace", p[2].1.as_str()),
            ("pod", p[3].1.as_str()),
        ];
        let e = &entries[i];
        tracker.update_series_with_entry(Some(e), &pairs, e.hash, e.ref_id, t0, -1, false);
    }

    group.throughput(Throughput::Elements(series_count as u64));

    let mut tick = 1u64;
    group.bench_function("update_series_with_entry_nanos_slab_1k", |b| {
        b.iter(|| {
            tick += 1;
            let now = t0 + Duration::from_secs(tick);
            for (i, p) in owned_pairs.iter().enumerate() {
                let pairs = [
                    ("__name__", p[0].1.as_str()),
                    ("cluster", p[1].1.as_str()),
                    ("namespace", p[2].1.as_str()),
                    ("pod", p[3].1.as_str()),
                ];
                let e = &entries[i];
                tracker.update_series_with_entry(
                    Some(e),
                    &pairs,
                    e.hash,
                    e.ref_id,
                    now,
                    -1,
                    false,
                );
            }
        });
    });

    let cutoff_ns = t0.duration_since(UNIX_EPOCH).unwrap().as_nanos() as i64;
    group.bench_function("nanos_slab_is_ref_active_fast_1k", |b| {
        b.iter(|| {
            let mut active = 0usize;
            for e in &entries {
                if tracker.is_ref_active_fast(black_box(e.ref_id), black_box(cutoff_ns)) {
                    active += 1;
                }
            }
            black_box(active);
        });
    });

    group.finish();
}

fn bench_single_pass_label_endpoints_50k(c: &mut Criterion) {
    let mut group = c.benchmark_group("ingester_hot_paths/label_endpoints_50k");
    let series_count = 50_000usize;
    let tmp = tempfile::tempdir().expect("tempdir");

    let mut cfg = IngesterConfig::default();
    cfg.ring.instance_id = "bench-0".to_string();
    let mut blocks_cfg = BlocksStorageConfig::default();
    blocks_cfg.tsdb.dir = tmp.path().to_string_lossy().into_owned();

    let ing = Ingester::new(
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
    .expect("Ingester::new");
    ing.set_state_for_test(State::Running);
    ing.compute_worker_pool.start().expect("start compute_worker_pool");

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let metrics = [
        "http_server_requests_total",
        "http_client_requests_total",
        "grpc_server_handled_total",
        "container_cpu_usage_seconds_total",
        "process_network_bytes_total",
    ];
    let namespaces = [
        "prod-api",
        "prod-checkout",
        "prod-auth",
        "prod-billing",
        "prod-search",
    ];
    let statuses = ["200", "201", "400", "404", "500"];

    for start in (0..series_count).step_by(2_000) {
        let end = (start + 2_000).min(series_count);
        let mut timeseries = Vec::with_capacity(end - start);
        for k in start..end {
            let ns = namespaces[k % namespaces.len()];
            let pod = format!("pod-{:05}", k / 2);
            let instance = format!("10.42.{}.{}:8080", (k >> 8) & 0xff, k & 0xff);
            timeseries.push(cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_static("__name__", metrics[k % metrics.len()]),
                    cortexpb::LabelPair::from_static("cluster", "prod-us-east-1"),
                    cortexpb::LabelPair {
                        name: Bytes::from_static(b"instance"),
                        value: Bytes::from(instance),
                    },
                    cortexpb::LabelPair::from_static("job", "api-service"),
                    cortexpb::LabelPair::from_static("namespace", ns),
                    cortexpb::LabelPair {
                        name: Bytes::from_static(b"pod"),
                        value: Bytes::from(pod),
                    },
                    cortexpb::LabelPair::from_static("status_code", statuses[k % statuses.len()]),
                ],
                samples: vec![cortexpb::Sample {
                    value: k as f64,
                    timestamp_ms: now_ms,
                }],
                ..Default::default()
            });
        }
        ing.push_to_storage(
            "bench-tenant",
            &cortexpb::WriteRequest {
                source: cortexpb::write_request::SourceEnum::Api as i32,
                timeseries,
                ..Default::default()
            },
        )
        .expect("push_to_storage");
    }

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let md = MetadataMap::new();

    group.throughput(Throughput::Elements(series_count as u64));

    group.bench_function("label_names_and_values_active_50k", |b| {
        let req = ingesterpb::LabelNamesAndValuesRequest {
            matchers: vec![ingesterpb::LabelMatcher {
                r#type: ingesterpb::MatchType::Equal as i32,
                name: "namespace".to_string(),
                value: "prod-api".to_string(),
            }],
            count_method: ingesterpb::CountMethod::Active as i32,
        };
        b.iter(|| {
            let res = rt
                .block_on(ing.handle_label_names_and_values(&md, "bench-tenant", req.clone()))
                .expect("label_names_and_values");
            black_box(res.len());
        });
    });

    group.bench_function("label_values_cardinality_active_50k", |b| {
        let req = ingesterpb::LabelValuesCardinalityRequest {
            label_names: vec![
                "namespace".to_string(),
                "cluster".to_string(),
                "status_code".to_string(),
            ],
            matchers: Vec::new(),
            count_method: ingesterpb::CountMethod::Active as i32,
        };
        b.iter(|| {
            let res = rt
                .block_on(ing.handle_label_values_cardinality(&md, "bench-tenant", req.clone()))
                .expect("label_values_cardinality");
            black_box(res.len());
        });
    });

    group.finish();
    ing.compute_worker_pool.stop();
}

criterion_group!(
    benches,
    bench_active_series_and_nanos_slab,
    bench_single_pass_label_endpoints_50k
);
criterion_main!(benches);
