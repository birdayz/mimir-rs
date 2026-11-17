use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use mimir_proto::compat::{non_stable_hash, LabelAdapter};
use mimir_proto::ringpb::{
    Desc, InstanceDesc, InstanceState, PartitionDesc, PartitionRingDesc, PartitionState,
    PartitionTokenScheme,
};
use mimir_proto::sharding::shard_by_all_labels;
use mimir_ring::partition_ring::{ActivePartitionBatchRing, PartitionRing, PartitionRingOptions};
use mimir_ring::ring::replication_strategy::{DefaultReplicationStrategy, WRITE};
use mimir_ring::ring::{Ring, RingSettings};
use mimir_ring::token_gen::{PartitionTokenTable, RandomTokenGenerator, TokenGenerator};
use xxhash_rust::xxh64::xxh64;

fn encode_stringlabels(pairs: &[(&str, &str)]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(128);
    for &(k, v) in pairs {
        for s in [k.as_bytes(), v.as_bytes()] {
            buf.push(s.len() as u8);
            buf.extend_from_slice(s);
        }
    }
    buf
}

fn bench_series_fingerprinting(c: &mut Criterion) {
    let mut group = c.benchmark_group("ring_lookup/series_fingerprinting");
    let series_count = 1_000usize;

    let label_sets_raw: Vec<Vec<(&str, String)>> = (0..series_count)
        .map(|i| {
            vec![
                ("__name__", "http_server_requests_total".to_string()),
                ("cluster", "prod-us-east-1".to_string()),
                ("container", "app".to_string()),
                (
                    "instance",
                    format!("10.42.{}.{}:8080", (i >> 8) & 0xff, i & 0xff),
                ),
                ("job", "prod-api/service".to_string()),
                ("namespace", "prod-api".to_string()),
                ("pod", format!("svc-{}", i / 4)),
                ("status_code", "200".to_string()),
            ]
        })
        .collect();

    let adapters: Vec<Vec<LabelAdapter>> = label_sets_raw
        .iter()
        .map(|set| {
            set.iter()
                .map(|(k, v)| LabelAdapter::new(*k, v.as_str()))
                .collect()
        })
        .collect();

    let stringlabels_bufs: Vec<Vec<u8>> = label_sets_raw
        .iter()
        .map(|set| {
            let pairs: Vec<(&str, &str)> = set.iter().map(|(k, v)| (*k, v.as_str())).collect();
            encode_stringlabels(&pairs)
        })
        .collect();

    group.throughput(Throughput::Elements(series_count as u64));

    group.bench_function("xxhash64_stringlabels_1k", |b| {
        b.iter(|| {
            let mut acc = 0u64;
            for buf in &stringlabels_bufs {
                acc ^= xxh64(black_box(buf), 0);
            }
            black_box(acc);
        });
    });

    group.bench_function("xxhash64_non_stable_hash_1k", |b| {
        b.iter(|| {
            let mut acc = 0u64;
            for ls in &adapters {
                acc ^= non_stable_hash(black_box(ls));
            }
            black_box(acc);
        });
    });

    group.bench_function("fnv1_shard_by_all_labels_1k", |b| {
        b.iter(|| {
            let mut acc = 0u32;
            for ls in &adapters {
                acc ^= shard_by_all_labels(
                    "tenant-0",
                    black_box(ls).iter().map(|l| (l.name.as_str(), l.value.as_str())),
                );
            }
            black_box(acc);
        });
    });

    group.finish();
}

fn make_zone_aware_ring(now: SystemTime) -> Arc<Ring> {
    let now_sec = now
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let gen = RandomTokenGenerator::with_seed(42);
    let zones = ["zone-a", "zone-b", "zone-c"];
    let mut ingesters = HashMap::new();
    let mut used = Vec::new();
    for idx in 0..15 {
        let zone = zones[idx % zones.len()].to_string();
        let tokens = gen.generate_tokens(128, &used);
        used.extend_from_slice(&tokens);
        let id = format!("ingester-{zone}-{idx}");
        ingesters.insert(
            id,
            InstanceDesc {
                addr: format!("10.0.{}.{}:9095", idx / 10, idx + 1),
                timestamp: now_sec,
                state: InstanceState::Active as i32,
                tokens,
                zone,
                registered_timestamp: now_sec - 3600,
                read_only: false,
                read_only_updated_timestamp: 0,
                ..Default::default()
            },
        );
    }
    let desc = Desc { ingesters };
    Ring::from_desc(
        RingSettings {
            heartbeat_timeout: Duration::from_secs(60),
            replication_factor: 3,
            zone_awareness_enabled: true,
            excluded_zones: Vec::new(),
            subring_cache_disabled: false,
        },
        desc,
        Arc::new(DefaultReplicationStrategy),
    )
    .expect("Ring::from_desc")
}

fn make_partition_ring() -> (Arc<PartitionRing>, ActivePartitionBatchRing) {
    let num_partitions = 64i32;
    let table = Arc::new(PartitionTokenTable::new(num_partitions, None).expect("token table"));
    let mut partitions = HashMap::new();
    for pid in 0..num_partitions {
        partitions.insert(
            pid,
            PartitionDesc {
                state: PartitionState::PartitionActive as i32,
                state_timestamp: 1_700_000_000,
                tokens: Vec::new(),
                token_scheme: PartitionTokenScheme::PartitionTokensSmt512 as i32,
                ..Default::default()
            },
        );
    }
    let desc = PartitionRingDesc {
        partitions,
        owners: HashMap::new(),
    };
    let ring = PartitionRing::with_options(
        desc,
        PartitionRingOptions {
            token_generator: Some(table),
            shuffle_shard_cache_size: 1024,
        },
    )
    .expect("PartitionRing");
    let batch_ring = ActivePartitionBatchRing::new(Arc::clone(&ring));
    (ring, batch_ring)
}

fn bench_ring_lookups(c: &mut Criterion) {
    let mut group = c.benchmark_group("ring_lookup/replication_and_partition");
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let ring = make_zone_aware_ring(now);
    let (part_ring, batch_ring) = make_partition_ring();

    let keys: Vec<u32> = (0..1_024u32)
        .map(|i| i.wrapping_mul(2_654_435_761))
        .collect();

    group.throughput(Throughput::Elements(keys.len() as u64));

    group.bench_function("classic_ring_rf3_zone_aware_1k", |b| {
        b.iter(|| {
            let mut total = 0usize;
            for &k in &keys {
                let rs = ring.get_at(black_box(k), WRITE, 3, now).expect("ring.get_at");
                total += rs.instances.len();
            }
            black_box(total);
        });
    });

    group.bench_function("partition_ring_active_partition_for_key_1k", |b| {
        b.iter(|| {
            let mut acc = 0i32;
            for &k in &keys {
                acc = acc.wrapping_add(
                    part_ring
                        .active_partition_for_key(black_box(k))
                        .expect("active_partition_for_key"),
                );
            }
            black_box(acc);
        });
    });

    group.bench_function("partition_ring_batch_get_1k", |b| {
        b.iter(|| {
            let mut acc = 0i32;
            for &k in &keys {
                let rs = batch_ring.get(black_box(k)).expect("batch_ring.get");
                acc = acc.wrapping_add(rs.partition_id);
            }
            black_box(acc);
        });
    });

    group.finish();
}

criterion_group!(benches, bench_series_fingerprinting, bench_ring_lookups);
criterion_main!(benches);
