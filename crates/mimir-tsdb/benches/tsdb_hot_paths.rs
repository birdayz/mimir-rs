use std::hint::black_box;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use mimir_tsdb::chunkenc::histogram::HistogramChunk;
use mimir_tsdb::chunkenc::histogram_model::{CounterResetHint, Histogram, HistogramSpan};
use mimir_tsdb::chunkenc::xor::XorChunk;
use mimir_tsdb::head::{hash_sorted_pairs, Head, HeadOptions, SeriesEntry};
use mimir_tsdb::labels::{append_stringlabel_pair, SymbolLabels};
use mimir_tsdb::matcher::{LabelMatcher, MatchType};
use mimir_tsdb::postings::MemPostings;
use mimir_tsdb::querier::{LazyStreamEvent, TsdbQuerier};
use mimir_tsdb::record::{RecordEncoder, RefSample};
use mimir_tsdb::symbols::SymbolTable;
use mimir_tsdb::wlog::{compress_wal_record, CompressionType, WlogWriter};

fn make_series_labels(count: usize) -> Vec<Vec<(String, String)>> {
    (0..count)
        .map(|i| {
            vec![
                ("__name__".to_string(), "http_requests_total".to_string()),
                ("cluster".to_string(), "prod-us-east-1".to_string()),
                (
                    "instance".to_string(),
                    format!("10.42.{}.{}:8080", (i >> 8) & 0xff, i & 0xff),
                ),
                ("job".to_string(), "api".to_string()),
                ("namespace".to_string(), format!("ns-{}", i % 10)),
                ("pod".to_string(), format!("pod-{i:05}")),
            ]
        })
        .collect()
}

fn make_sample_histogram(iter: u64) -> Histogram {
    Histogram {
        counter_reset_hint: CounterResetHint::NotCounterReset,
        schema: 0,
        zero_threshold: 0.001,
        zero_count: 5 + iter,
        count: 31 + iter * 5,
        sum: 12.5 + (iter as f64) * 1.5,
        positive_spans: vec![HistogramSpan {
            offset: 0,
            length: 4,
        }],
        negative_spans: Vec::new(),
        positive_buckets: vec![5 + iter as i64, 3, -2, 1],
        negative_buckets: Vec::new(),
        custom_values: Vec::new(),
    }
}

fn bench_head_appender(c: &mut Criterion) {
    let mut group = c.benchmark_group("tsdb_hot_paths/head_appender");
    let series_count = 1_000usize;
    let owned_labels = make_series_labels(series_count);

    group.throughput(Throughput::Elements(series_count as u64));

    group.bench_function("cold_create_1k_series", |b| {
        b.iter_batched(
            || {
                let dir = tempfile::tempdir().expect("tempdir");
                let head = Head::open(dir.path(), HeadOptions::default(), i64::MIN).expect("open");
                (dir, head)
            },
            |(_dir, head)| {
                let mut app = head.appender_with_capacity(series_count);
                for (i, lbls) in owned_labels.iter().enumerate() {
                    let pairs: Vec<(&str, &str)> =
                        lbls.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
                    let r = app
                        .append_float(None, &pairs, 1_700_000_000_000, i as f64)
                        .expect("append_float");
                    black_box(r);
                }
                app.commit().expect("commit");
            },
            BatchSize::PerIteration,
        );
    });

    // Warm in-order float append using pre-cached SeriesEntry + prehashed encoded labels
    {
        let dir = tempfile::tempdir().expect("tempdir");
        let head = Head::open(dir.path(), HeadOptions::default(), i64::MIN).expect("open");
        let mut cached_entries: Vec<(Option<Arc<SeriesEntry>>, u64, Vec<u8>)> =
            Vec::with_capacity(series_count);
        {
            let mut app = head.appender_with_capacity(series_count);
            for (i, lbls) in owned_labels.iter().enumerate() {
                let pairs: Vec<(&str, &str)> =
                    lbls.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
                let mut encoded = Vec::with_capacity(128);
                for &(k, v) in &pairs {
                    append_stringlabel_pair(&mut encoded, k.as_bytes(), v.as_bytes());
                }
                let h = hash_sorted_pairs(&pairs);
                app.append_float(None, &pairs, 1_700_000_000_000, i as f64)
                    .expect("seed append");
                cached_entries.push((None, h, encoded));
            }
            app.commit().expect("seed commit");
        }
        for (i, lbls) in owned_labels.iter().enumerate() {
            let pairs: Vec<(&str, &str)> =
                lbls.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
            cached_entries[i].0 = head.get_entry_by_sorted_pairs(&pairs);
        }

        let mut ts = 1_700_000_015_000i64;
        group.bench_function("warm_in_order_float_append_1k_series", |b| {
            b.iter(|| {
                ts += 15_000;
                let mut app = head.appender_with_capacity(series_count);
                for (i, (entry, hash, encoded)) in cached_entries.iter_mut().enumerate() {
                    let r = app
                        .append_float_with_entry_prehashed_encoded(
                            entry,
                            *hash,
                            encoded,
                            0,
                            ts,
                            (i as f64) * 1.1,
                            false,
                        )
                        .expect("warm append");
                    black_box(r);
                }
                app.commit().expect("commit");
            });
        });
    }

    // Native histogram append (warm 1k series)
    {
        let dir = tempfile::tempdir().expect("tempdir");
        let head = Head::open(dir.path(), HeadOptions::default(), i64::MIN).expect("open");
        let mut refs = Vec::with_capacity(series_count);
        {
            let mut app = head.appender_with_capacity(series_count);
            let h0 = make_sample_histogram(0);
            for lbls in &owned_labels {
                let pairs: Vec<(&str, &str)> =
                    lbls.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
                let r = app
                    .append_histogram(None, &pairs, 0, 1_700_000_000_000, h0.clone())
                    .expect("seed hist");
                refs.push(r);
            }
            app.commit().expect("seed hist commit");
        }
        let mut ts = 1_700_000_015_000i64;
        let mut step = 1u64;
        group.bench_function("native_histogram_append_1k_series", |b| {
            b.iter(|| {
                ts += 15_000;
                step += 1;
                let hist = make_sample_histogram(step);
                let mut app = head.appender_with_capacity(series_count);
                for (i, lbls) in owned_labels.iter().enumerate() {
                    let pairs: Vec<(&str, &str)> =
                        lbls.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
                    let r = app
                        .append_histogram(Some(refs[i]), &pairs, 0, ts, hist.clone())
                        .expect("append_histogram");
                    black_box(r);
                }
                app.commit().expect("commit");
            });
        });
    }

    group.finish();
}

fn bench_chunk_encoding(c: &mut Criterion) {
    let mut group = c.benchmark_group("tsdb_hot_paths/chunk_encode");
    let samples_per_chunk = 120usize;
    group.throughput(Throughput::Elements(samples_per_chunk as u64));

    group.bench_function("xor_chunk_encode_120_samples", |b| {
        b.iter(|| {
            let mut chunk = XorChunk::new();
            let mut app = chunk.appender().expect("xor appender");
            for i in 0..samples_per_chunk {
                app.append(0, 1_700_000_000_000 + (i as i64) * 15_000, (i as f64) * 1.25)
                    .expect("xor append");
            }
            black_box(chunk.num_samples());
        });
    });

    let hist_samples: Vec<Histogram> = (0..samples_per_chunk)
        .map(|i| make_sample_histogram(i as u64))
        .collect();
    group.bench_function("histogram_chunk_encode_120_samples", |b| {
        b.iter(|| {
            let mut chunk = HistogramChunk::new();
            let mut app = chunk.appender().expect("hist appender");
            for (i, h) in hist_samples.iter().enumerate() {
                app.append_histogram(
                    None,
                    0,
                    1_700_000_000_000 + (i as i64) * 15_000,
                    h,
                    false,
                )
                .expect("hist append");
            }
            black_box(chunk.num_samples());
        });
    });

    group.finish();
}

fn bench_parallel_wal_precompressed_log(c: &mut Criterion) {
    let mut group = c.benchmark_group("tsdb_hot_paths/wal");
    let enc = RecordEncoder::new(false);
    let records: Vec<Vec<u8>> = (0..32)
        .map(|batch_idx| {
            let samples: Vec<RefSample> = (0..250)
                .map(|i| RefSample {
                    ref_id: (batch_idx * 250 + i) as u64,
                    st: 0,
                    t: 1_700_000_000_000 + (i as i64) * 15_000,
                    v: (i as f64) * 1.5,
                })
                .collect();
            enc.samples(&samples, Vec::new())
        })
        .collect();
    let total_raw_bytes: usize = records.iter().map(|r| r.len()).sum();

    let dir = tempfile::tempdir().expect("tempdir");
    let mut writer = WlogWriter::with_segment_size(dir.path(), 128 * 1024 * 1024, CompressionType::Snappy)
        .expect("WlogWriter");

    group.throughput(Throughput::Bytes(total_raw_bytes as u64));
    group.bench_function("parallel_precompress_and_log_precompressed_32_recs", |b| {
        b.iter(|| {
            let pre: Vec<(Vec<u8>, bool)> = std::thread::scope(|s| {
                let handles: Vec<_> = records
                    .chunks(8)
                    .map(|chunk| {
                        s.spawn(move || {
                            let mut out = Vec::with_capacity(chunk.len());
                            for r in chunk {
                                let mut buf = Vec::with_capacity(r.len());
                                let compressed =
                                    compress_wal_record(r, CompressionType::Snappy, &mut buf)
                                        .expect("compress_wal_record");
                                if compressed {
                                    out.push((buf, true));
                                } else {
                                    out.push((r.clone(), false));
                                }
                            }
                            out
                        })
                    })
                    .collect();
                handles
                    .into_iter()
                    .flat_map(|h| h.join().expect("join"))
                    .collect()
            });
            let refs: Vec<(&[u8], bool)> = pre.iter().map(|(b, c)| (b.as_slice(), *c)).collect();
            writer.log_precompressed(black_box(&refs)).expect("log_precompressed");
        });
    });

    group.finish();
}

fn bench_sort_lazy_entries_28k(c: &mut Criterion) {
    let mut group = c.benchmark_group("tsdb_hot_paths/querier");
    let series_count = 28_000usize;
    let dir = tempfile::tempdir().expect("tempdir");
    let head = Head::open(dir.path(), HeadOptions::default(), i64::MIN).expect("open");

    {
        let mut app = head.appender_with_capacity(series_count);
        for k in 0..series_count {
            // Permute pod index so entries are not already sorted in insertion order
            let shuffled = (k.wrapping_mul(7_919)) % series_count;
            let pod = format!("pod-{shuffled:05}");
            let instance = format!("10.42.{}.{}:8080", (shuffled >> 8) & 0xff, shuffled & 0xff);
            let pairs = [
                ("__name__", "http_requests_total"),
                ("cluster", "prod-us-east-1"),
                ("instance", instance.as_str()),
                ("job", "api"),
                ("pod", pod.as_str()),
            ];
            app.append_float(None, &pairs, 1_700_000_000_000, k as f64)
                .expect("append");
        }
        app.commit().expect("commit");
    }

    let matchers = [LabelMatcher::new(MatchType::Equal, "job", "api").expect("matcher")];
    let q = TsdbQuerier::new(Some(&head), Vec::new(), 1_699_999_000_000, 1_700_001_000_000);
    let prepared = q
        .prepare_select_chunks(&matchers, false)
        .expect("prepare_select_chunks");

    group.throughput(Throughput::Elements(series_count as u64));
    group.bench_function("sort_lazy_entries_28k_series", |b| {
        b.iter(|| {
            let mut seen = 0usize;
            q.stream_prepared_chunks_lazy(
                prepared.clone(),
                &matchers,
                true,
                None,
                |ev| match ev {
                    LazyStreamEvent::Series(_, _) => {
                        seen += 1;
                        Ok(true)
                    }
                    LazyStreamEvent::SeriesEnd => Ok(false),
                    LazyStreamEvent::SeriesChunks(_, _) => Ok(true),
                },
            )
            .expect("stream_prepared_chunks_lazy");
            black_box(seen);
        });
    });

    group.finish();
}

fn bench_mem_postings_direct_u32(c: &mut Criterion) {
    let mut group = c.benchmark_group("tsdb_hot_paths/mem_postings");
    let series_count = 50_000usize;
    let mut syms = SymbolTable::with_capacity(series_count + 64, series_count * 16);
    let postings = MemPostings::new_unordered();

    let namespaces = ["prod-api", "prod-checkout", "prod-auth", "prod-billing", "prod-search"];
    let statuses = ["200", "201", "400", "404", "500"];
    for k in 0..series_count {
        let pod = format!("pod-{}", k / 10);
        let ns = namespaces[k % namespaces.len()];
        let status = statuses[(k / 5) % statuses.len()];
        let sl = SymbolLabels::from_pairs(
            &mut syms,
            [
                ("__name__", "http_requests_total"),
                ("cluster", "prod-us-east-1"),
                ("namespace", ns),
                ("pod", pod.as_str()),
                ("status_code", status),
            ],
        );
        postings.add((k + 1) as u64, &sl);
    }
    postings.ensure_order_parallel();

    let matchers_eq = [
        LabelMatcher::new(MatchType::Equal, "__name__", "http_requests_total").expect("m1"),
        LabelMatcher::new(MatchType::Equal, "namespace", "prod-api").expect("m2"),
        LabelMatcher::new(MatchType::Equal, "status_code", "200").expect("m3"),
    ];
    let matchers_neg = [
        LabelMatcher::new(MatchType::Equal, "__name__", "http_requests_total").expect("m1"),
        LabelMatcher::new(MatchType::Equal, "namespace", "prod-api").expect("m2"),
        LabelMatcher::new(MatchType::NotEqual, "status_code", "500").expect("m3"),
    ];

    group.throughput(Throughput::Elements(series_count as u64));

    group.bench_function("direct_u32_intersect_eq_50k", |b| {
        b.iter(|| {
            let refs = postings
                .postings_for_matchers(&syms, black_box(&matchers_eq))
                .expect("postings_for_matchers");
            black_box(refs.len());
        });
    });

    group.bench_function("direct_u32_intersect_and_subtract_50k", |b| {
        b.iter(|| {
            let refs = postings
                .postings_for_matchers(&syms, black_box(&matchers_neg))
                .expect("postings_for_matchers");
            black_box(refs.len());
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_head_appender,
    bench_chunk_encoding,
    bench_parallel_wal_precompressed_log,
    bench_sort_lazy_entries_28k,
    bench_mem_postings_direct_u32
);
criterion_main!(benches);
