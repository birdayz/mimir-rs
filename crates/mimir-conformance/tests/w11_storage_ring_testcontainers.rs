//! Wave 11 Storage & Ring correctness conformance tests (Docker / testcontainers).
//!
//! Scenarios:
//! - (a) 3-broker Redpanda cluster: `PartitionOffsetClient` shards `ListOffsets` across brokers
//!   leading different partitions (P0-2).
//! - (b) `MemberlistKv` `LEFT` / `PartitionDeleted` / `OwnerDeleted` merge when incoming gossip
//!   carries an expired tombstone (P0-1) + `watch_key` deletion notification (P1-7).
//! - (c) `EtcdKv` watch failover across reconnect + `compact_revision` compaction recovery +
//!   `DELETE` event (`KeyDeleted`) + auth token + empty-prefix `list("")`/`watch_prefix("")`
//!   (P0-3).
//! - (d) `ShipperMeta` / `BlockMeta` with Go `"labels": null, "shipped": null` (P1-4) and
//!   `upload_block_to_bucket` cleanup deleting `meta.json` first (P1-3).
//!
//! Requires Docker for (a) and (c). `MIMIR_CONFORMANCE_SKIP_DOCKER=1` skips Docker tests
//! explicitly (prints `SKIPPED`). Leftover containers carry `org.mimir-rs.conformance=w11-storage-ring`.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Write as _;
use std::fs;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, ensure, Context, Result};
use bytes::Bytes;
use prometheus::Registry;
use testcontainers::core::{ContainerPort, ExecCommand, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, CopyTargetOptions, GenericImage, ImageExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use ulid::Ulid;

use mimir_common::block_on_for_tests;
use mimir_common::services::ServiceContext;
use mimir_config::ingester::EtcdConfig;
use mimir_config::server::MemberlistConfig;
use mimir_config::{GoDuration, KafkaConfig, StringSliceCSV, StringSliceCSVMulti};
use mimir_conformance::free_test_port;
use mimir_proto::memberlistpb::KeyValuePair;
use mimir_proto::prost::Message as _;
use mimir_proto::ringpb::{
    Desc, InstanceDesc, InstanceState, OwnerDesc, OwnerState, PartitionDesc, PartitionRingDesc,
    PartitionState,
};
use mimir_ring::kv::etcd::EtcdKv;
use mimir_ring::kv::memberlist::{MemberlistKv, MemberlistKvClient};
use mimir_ring::kv::{cas_typed, get_typed, is_key_deleted, KvClient, StringCodec};
use mimir_ring::partition_ring::model::partition_ring_desc_codec;
use mimir_ring::ring::model::ring_desc_codec;
use mimir_storage::bucket::BucketClient;
use mimir_storage::ingest::{KafkaClient, KafkaCompression, PartitionOffsetClient, ProduceRecord};
use mimir_storage::shipper::{
    upload_block_to_bucket, BlockMeta, BlockMetaCompaction, BlockStats, Shipper, ShipperError,
    ShipperMeta, ShipperMetrics, SourceType, ThanosMeta, INDEX_FILENAME, META_FILENAME,
    SHIPPER_META_FILENAME, TSDB_VERSION_1,
};

const REDPANDA_IMAGE: &str = "redpandadata/redpanda";
const REDPANDA_TAG: &str =
    "v26.2.7@sha256:43545ee03966431f5451e0b3c1e651b39fbe3eb383360220311021f50133b649";
const ETCD_IMAGE: &str = "quay.io/coreos/etcd";
const ETCD_TAG: &str = "v3.5.17";
const CONTAINER_LABEL: (&str, &str) = ("org.mimir-rs.conformance", "w11-storage-ring");

fn skip_docker(test: &str) -> bool {
    if std::env::var("MIMIR_CONFORMANCE_SKIP_DOCKER").as_deref() == Ok("1") {
        eprintln!("SKIPPED {test}: MIMIR_CONFORMANCE_SKIP_DOCKER=1 (this test needs Docker)");
        return true;
    }
    false
}

fn redpanda_yaml(node: usize, names: &[String], ports: &[u16]) -> String {
    let mut y = format!(
        "redpanda:\n  data_directory: /var/lib/redpanda/data\n  node_id: {node}\n  empty_seed_starts_cluster: false\n  seed_servers:\n"
    );
    for n in names {
        let _ = writeln!(y, "    - host: {{address: {n}, port: 33145}}");
    }
    let _ = write!(
        y,
        "  rpc_server: {{address: 0.0.0.0, port: 33145}}\n  advertised_rpc_api: {{address: {name}, port: 33145}}\n  admin: [{{address: 0.0.0.0, port: 9644}}]\n  developer_mode: true\n  kafka_api:\n    - {{name: external, address: 0.0.0.0, port: {port}}}\n  advertised_kafka_api:\n    - {{name: external, address: 127.0.0.1, port: {port}}}\n",
        name = names[node],
        port = ports[node]
    );
    y
}

struct Redpanda3 {
    nodes: Vec<ContainerAsync<GenericImage>>,
    ports: Vec<u16>,
}

impl Redpanda3 {
    async fn start() -> Result<Self> {
        let suffix = format!(
            "{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("clock")?
                .subsec_nanos()
        );
        let network = format!("mimir-w11-rp-{suffix}");
        let names: Vec<String> = (0..3).map(|n| format!("w11-rp{n}-{suffix}")).collect();
        let ports = vec![free_test_port(), free_test_port(), free_test_port()];

        let mut starts = Vec::new();
        for node in 0..3 {
            let req = GenericImage::new(REDPANDA_IMAGE, REDPANDA_TAG)
                .with_entrypoint("/bin/sh")
                .with_cmd([
                    "-c",
                    "cp /config/redpanda.yaml /etc/redpanda/redpanda.yaml && exec rpk redpanda start --mode dev-container --smp 1 --memory 1G --reserve-memory 0M --default-log-level=warn",
                ])
                .with_label(CONTAINER_LABEL.0, CONTAINER_LABEL.1)
                .with_network(network.clone())
                .with_container_name(names[node].clone())
                .with_startup_timeout(Duration::from_secs(180))
                .with_copy_to(
                    CopyTargetOptions::new("/config/redpanda.yaml").with_mode(0o644),
                    redpanda_yaml(node, &names, &ports).into_bytes(),
                )
                .with_mapped_port(ports[node], ContainerPort::Tcp(ports[node]));
            starts.push(req.start());
        }
        let mut nodes = Vec::new();
        for r in futures::future::join_all(starts).await {
            nodes.push(r.context("starting 3-broker Redpanda via testcontainers")?);
        }
        let cluster = Self { nodes, ports };
        cluster.wait_healthy().await?;
        Ok(cluster)
    }

    async fn wait_healthy(&self) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(180);
        loop {
            let out = tokio::time::timeout(
                Duration::from_secs(30),
                self.rpk(0, &["cluster", "health"]),
            )
            .await;
            if let Ok(Ok(text)) = &out {
                let healthy = text.lines().any(|l| l.contains("Healthy:") && l.contains("true"));
                let all_up = text
                    .lines()
                    .any(|l| l.contains("All nodes:") && ["0", "1", "2"].iter().all(|n| l.contains(n)));
                if healthy && all_up {
                    return Ok(());
                }
            }
            if Instant::now() >= deadline {
                bail!("3-broker Redpanda cluster never became healthy: {out:?}");
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    async fn rpk(&self, node: usize, args: &[&str]) -> Result<String> {
        let cmd: Vec<String> = std::iter::once("rpk")
            .chain(args.iter().copied())
            .map(str::to_string)
            .collect();
        let mut res = self.nodes[node].exec(ExecCommand::new(cmd)).await?;
        let stdout = res.stdout_to_vec().await?;
        let stderr = res.stderr_to_vec().await?;
        let code = loop {
            if let Some(c) = res.exit_code().await? {
                break c;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        };
        ensure!(
            code == 0,
            "rpk {args:?} exited {code}: {}{}",
            String::from_utf8_lossy(&stdout),
            String::from_utf8_lossy(&stderr)
        );
        Ok(String::from_utf8_lossy(&stdout).into_owned())
    }
}

#[test]
fn test_w11_p0_2_three_broker_redpanda_sharded_list_offsets() -> Result<()> {
    if skip_docker("test_w11_p0_2_three_broker_redpanda_sharded_list_offsets") {
        return Ok(());
    }
    block_on_for_tests(async {
        let cluster = Redpanda3::start().await?;
        let topic = "w11-sharded-offsets";
        let num_partitions = 6usize;

        cluster
            .rpk(
                0,
                &[
                    "topic",
                    "create",
                    topic,
                    "-p",
                    &num_partitions.to_string(),
                    "-r",
                    "3",
                ],
            )
            .await?;

        // Intentionally give only broker 0 as seed address so metadata discovery + leader
        // sharding must route ListOffsets to all distinct leader brokers.
        let cfg = KafkaConfig {
            address: StringSliceCSV(vec![format!("127.0.0.1:{}", cluster.ports[0])]),
            topic: topic.to_string(),
            client_id: "w11-test".to_string(),
            auto_create_topic_enabled: false,
            last_produced_offset_retry_timeout: GoDuration::from_secs(10),
            ..Default::default()
        };
        let client = Arc::new(KafkaClient::from_config(&cfg)?);

        let mut leaders = HashSet::new();
        let dl = Instant::now() + Duration::from_secs(30);
        while Instant::now() < dl {
            client.refresh_metadata(&[topic]).await?;
            leaders.clear();
            for p in 0..num_partitions as i32 {
                let (leader, _) = client.cached_partition_leader(topic, p);
                if leader >= 0 {
                    leaders.insert(leader);
                }
            }
            if leaders.len() >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        ensure!(
            leaders.len() >= 2,
            "expected partitions led by multiple brokers, got leaders {leaders:?}"
        );

        for p in 1..num_partitions as i32 {
            for seq in 0..p {
                let payload = format!("p{p}-m{seq}");
                let prod_dl = Instant::now() + Duration::from_secs(15);
                let base_offset = loop {
                    match client
                        .produce(
                            topic,
                            p,
                            KafkaCompression::None,
                            vec![ProduceRecord {
                                key: Some(Bytes::from(format!("k{p}"))),
                                value: Some(Bytes::from(payload.clone())),
                                headers: Vec::new(),
                                timestamp_ms: None,
                            }],
                        )
                        .await
                    {
                        Ok(off) => break off,
                        Err(e) if Instant::now() < prod_dl => {
                            let _ = e;
                            tokio::time::sleep(Duration::from_millis(200)).await;
                        }
                        Err(e) => {
                            return Err(e).with_context(|| format!("producing partition {p} seq {seq}"));
                        }
                    }
                };
                ensure!(base_offset == seq as i64);
            }
        }

        let reg = Registry::new();
        let oc = PartitionOffsetClient::with_config(Arc::clone(&client), &cfg, Some(&reg));
        let req: HashMap<String, Vec<i32>> =
            HashMap::from([(topic.to_string(), (0..num_partitions as i32).collect())]);

        let got = oc
            .fetch_partitions_last_produced_offsets(&req)
            .await
            .context("fetch_partitions_last_produced_offsets")?;
        let pmap = got.get(topic).context("missing topic in offset result")?;
        ensure!(pmap.get(&0) == Some(&-1), "expected -1 for empty p0, got {pmap:?}");
        for p in 1..num_partitions as i32 {
            let expected = (p - 1) as i64;
            ensure!(
                pmap.get(&p) == Some(&expected),
                "expected offset {expected} for partition {p}, got {pmap:?}"
            );
        }
        Ok(())
    })
}

#[test]
fn test_w11_p0_1_memberlist_expired_tombstone_left_and_partition_deleted_merge() -> Result<()> {
    block_on_for_tests(async {
        let mut cfg_a = MemberlistConfig {
            bind_addr: vec!["127.0.0.1".to_string()],
            bind_port: free_test_port() as i64,
            node_name: "w11-ml-a".to_string(),
            randomize_node_name: false,
            left_ingesters_timeout: GoDuration::from_secs(30),
            gossip_interval: GoDuration::from_millis(50),
            ..Default::default()
        };
        cfg_a.advertise_port = cfg_a.bind_port;

        let mut cfg_b = MemberlistConfig {
            bind_addr: vec!["127.0.0.1".to_string()],
            bind_port: free_test_port() as i64,
            node_name: "w11-ml-b".to_string(),
            randomize_node_name: false,
            left_ingesters_timeout: GoDuration::from_secs(30),
            gossip_interval: GoDuration::from_millis(50),
            join_members: StringSliceCSVMulti(vec![format!("127.0.0.1:{}", cfg_a.bind_port)]),
            abort_if_cluster_fast_join_fails: true,
            ..Default::default()
        };
        cfg_b.advertise_port = cfg_b.bind_port;

        let codecs = vec![ring_desc_codec(), partition_ring_desc_codec()];
        let kv_a = MemberlistKv::start(cfg_a, codecs.clone(), "w11", None).await?;
        let kv_b = MemberlistKv::start(cfg_b, codecs, "w11", None).await?;

        let ring_client_a = MemberlistKvClient::new(Arc::clone(&kv_a), ring_desc_codec())?;
        let ring_client_b = Arc::new(MemberlistKvClient::new(
            Arc::clone(&kv_b),
            ring_desc_codec(),
        )?);
        let pr_client_a = MemberlistKvClient::new(Arc::clone(&kv_a), partition_ring_desc_codec())?;
        let pr_client_b = Arc::new(MemberlistKvClient::new(
            Arc::clone(&kv_b),
            partition_ring_desc_codec(),
        )?);

        let old_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("clock")?
            .as_secs() as i64
            - 120;

        cas_typed(&ring_client_a, "ring", move |cur: Option<&Desc>| {
            let mut d = cur.cloned().unwrap_or_default();
            d.ingesters.insert(
                "ing-left".to_string(),
                InstanceDesc {
                    id: "ing-left".to_string(),
                    addr: "127.0.0.1:9095".to_string(),
                    timestamp: old_ts,
                    state: InstanceState::Active as i32,
                    tokens: vec![11, 22],
                    ..Default::default()
                },
            );
            Ok(Some((d, true)))
        })
        .await?;

        cas_typed(
            &pr_client_a,
            "ingester-partitions",
            move |cur: Option<&PartitionRingDesc>| {
                let mut d = cur.cloned().unwrap_or_default();
                d.partitions.insert(
                    7,
                    PartitionDesc {
                        state: PartitionState::PartitionActive as i32,
                        state_timestamp: old_ts,
                        tokens: vec![100, 200],
                        ..Default::default()
                    },
                );
                d.owners.insert(
                    "owner-7".to_string(),
                    OwnerDesc {
                        state: OwnerState::OwnerActive as i32,
                        updated_timestamp: old_ts,
                        owned_partition: 7,
                    },
                );
                Ok(Some((d, true)))
            },
        )
        .await?;

        let dl = Instant::now() + Duration::from_secs(10);
        loop {
            let r_ok = get_typed::<Desc>(ring_client_b.as_ref(), "ring")
                .await?
                .is_some_and(|d| d.ingesters.contains_key("ing-left"));
            let p_ok = get_typed::<PartitionRingDesc>(pr_client_b.as_ref(), "ingester-partitions")
                .await?
                .is_some_and(|d| d.partitions.contains_key(&7) && d.owners.contains_key("owner-7"));
            if r_ok && p_ok {
                break;
            }
            ensure!(Instant::now() < dl, "timed out waiting for gossip sync to node B");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let (ring_tx, mut ring_rx) = mpsc::unbounded_channel::<Vec<String>>();
        let (pr_tx, mut pr_rx) = mpsc::unbounded_channel::<(Vec<i32>, Vec<String>)>();
        let wctx = ServiceContext::background();
        let rb_w = Arc::clone(&ring_client_b);
        let wctx1 = wctx.clone();
        let h1 = tokio::spawn(async move {
            rb_w.watch_key(
                "ring",
                wctx1,
                Box::new(move |any| {
                    if let Ok(d) = any.downcast::<Desc>() {
                        let _ = ring_tx.send(d.ingesters.keys().cloned().collect());
                    }
                    true
                }),
            )
            .await;
        });
        let pb_w = Arc::clone(&pr_client_b);
        let wctx2 = wctx.clone();
        let h2 = tokio::spawn(async move {
            pb_w.watch_key(
                "ingester-partitions",
                wctx2,
                Box::new(move |any| {
                    if let Ok(d) = any.downcast::<PartitionRingDesc>() {
                        let _ = pr_tx.send((
                            d.partitions.keys().copied().collect(),
                            d.owners.keys().cloned().collect(),
                        ));
                    }
                    true
                }),
            )
            .await;
        });

        ensure!(
            tokio::time::timeout(Duration::from_secs(2), ring_rx.recv())
                .await?
                .context("ring initial")?
                == vec!["ing-left".to_string()]
        );
        ensure!(
            tokio::time::timeout(Duration::from_secs(2), pr_rx.recv())
                .await?
                .context("pr initial")?
                == (vec![7], vec!["owner-7".to_string()])
        );

        let mut left_desc = Desc::default();
        left_desc.ingesters.insert(
            "ing-left".to_string(),
            InstanceDesc {
                id: "ing-left".to_string(),
                addr: "127.0.0.1:9095".to_string(),
                timestamp: old_ts + 1,
                state: InstanceState::Left as i32,
                ..Default::default()
            },
        );
        let ring_codec = ring_desc_codec();
        let left_bytes = ring_codec.encode(&left_desc)?;
        let ring_pkt = KeyValuePair {
            key: "ring".to_string(),
            value: Bytes::from(left_bytes),
            codec: ring_codec.codec_id().to_string(),
            deleted: false,
            update_time_millis: 0,
        };
        kv_b.notify_msg(&ring_pkt.encode_to_vec());

        let mut del_pr = PartitionRingDesc::default();
        del_pr.partitions.insert(
            7,
            PartitionDesc {
                state: PartitionState::PartitionDeleted as i32,
                state_timestamp: old_ts + 1,
                ..Default::default()
            },
        );
        del_pr.owners.insert(
            "owner-7".to_string(),
            OwnerDesc {
                state: OwnerState::OwnerDeleted as i32,
                updated_timestamp: old_ts + 1,
                owned_partition: 7,
            },
        );
        let pr_codec = partition_ring_desc_codec();
        let del_pr_bytes = pr_codec.encode(&del_pr)?;
        let pr_pkt = KeyValuePair {
            key: "ingester-partitions".to_string(),
            value: Bytes::from(del_pr_bytes),
            codec: pr_codec.codec_id().to_string(),
            deleted: false,
            update_time_millis: 0,
        };
        kv_b.notify_msg(&pr_pkt.encode_to_vec());

        let ring_after = tokio::time::timeout(Duration::from_secs(2), ring_rx.recv())
            .await?
            .context("ring update")?;
        ensure!(ring_after.is_empty(), "expected ing-left removed, got {ring_after:?}");

        let pr_after = tokio::time::timeout(Duration::from_secs(2), pr_rx.recv())
            .await?
            .context("pr update")?;
        ensure!(
            pr_after.0.is_empty() && pr_after.1.is_empty(),
            "expected partition 7 & owner-7 removed, got {pr_after:?}"
        );

        wctx.cancel();
        let _ = tokio::join!(h1, h2);
        kv_b.shutdown().await;
        kv_a.shutdown().await;
        Ok(())
    })
}

async fn etcdctl(container: &ContainerAsync<GenericImage>, args: &[&str]) -> Result<String> {
    let cmd: Vec<String> = std::iter::once("etcdctl")
        .chain(args.iter().copied())
        .map(str::to_string)
        .collect();
    let mut res = container.exec(ExecCommand::new(cmd)).await?;
    let stdout = res.stdout_to_vec().await?;
    let stderr = res.stderr_to_vec().await?;
    let code = loop {
        if let Some(c) = res.exit_code().await? {
            break c;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    ensure!(
        code == 0,
        "etcdctl {args:?} exited {code}: {}{}",
        String::from_utf8_lossy(&stdout),
        String::from_utf8_lossy(&stderr)
    );
    Ok(String::from_utf8_lossy(&stdout).into_owned())
}

struct DropProxy {
    port: u16,
    drop_now: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

impl DropProxy {
    async fn start(upstream_port: u16) -> Result<Self> {
        let port = free_test_port();
        let listener = TcpListener::bind(("127.0.0.1", port)).await?;
        let drop_now = Arc::new(AtomicBool::new(false));
        let drop_flag = Arc::clone(&drop_now);

        let task = tokio::spawn(async move {
            loop {
                let Ok((mut client, _)) = listener.accept().await else {
                    return;
                };
                if drop_flag.load(Ordering::SeqCst) {
                    drop(client);
                    continue;
                }
                let flag = Arc::clone(&drop_flag);
                tokio::spawn(async move {
                    let Ok(mut server) = TcpStream::connect(("127.0.0.1", upstream_port)).await
                    else {
                        return;
                    };
                    let (mut cr, mut cw) = client.split();
                    let (mut sr, mut sw) = server.split();
                    let c2s = async {
                        let mut buf = [0u8; 4096];
                        loop {
                            let n = cr.read(&mut buf).await.unwrap_or(0);
                            if n == 0 || sw.write_all(&buf[..n]).await.is_err() {
                                break;
                            }
                        }
                    };
                    let s2c = async {
                        let mut buf = [0u8; 4096];
                        loop {
                            if flag.load(Ordering::SeqCst) {
                                break;
                            }
                            let n = tokio::select! {
                                r = sr.read(&mut buf) => r.unwrap_or(0),
                                _ = async {
                                    while !flag.load(Ordering::SeqCst) {
                                        tokio::time::sleep(Duration::from_millis(20)).await;
                                    }
                                } => 0,
                            };
                            if n == 0 || cw.write_all(&buf[..n]).await.is_err() {
                                break;
                            }
                        }
                    };
                    tokio::select! {
                        _ = c2s => {}
                        _ = s2c => {}
                    }
                });
            }
        });
        Ok(Self {
            port,
            drop_now,
            task,
        })
    }

    fn pause_and_drop(&self) {
        self.drop_now.store(true, Ordering::SeqCst);
    }

    fn resume(&self) {
        self.drop_now.store(false, Ordering::SeqCst);
    }
}

impl Drop for DropProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[test]
fn test_w11_p0_3_etcd_watch_failover_auth_empty_prefix_delete_and_compaction() -> Result<()> {
    if skip_docker("test_w11_p0_3_etcd_watch_failover_auth_empty_prefix_delete_and_compaction") {
        return Ok(());
    }
    block_on_for_tests(async {
        let etcd_port = free_test_port();
        let container = GenericImage::new(ETCD_IMAGE, ETCD_TAG)
            .with_wait_for(WaitFor::message_on_either_std("ready to serve client requests"))
            .with_entrypoint("etcd")
            .with_cmd([
                "--name=w11-etcd",
                "--data-dir=/tmp/etcd",
                "--listen-client-urls=http://0.0.0.0:2379",
                "--advertise-client-urls=http://127.0.0.1:2379",
            ])
            .with_label(CONTAINER_LABEL.0, CONTAINER_LABEL.1)
            .with_mapped_port(etcd_port, ContainerPort::Tcp(2379))
            .with_startup_timeout(Duration::from_secs(120))
            .start()
            .await
            .context("starting etcd via testcontainers")?;

        etcdctl(&container, &["role", "add", "root"]).await?;
        etcdctl(&container, &["user", "add", "root:secretpass"]).await?;
        etcdctl(&container, &["user", "grant-role", "root", "root"]).await?;
        etcdctl(&container, &["auth", "enable"]).await?;

        let proxy = DropProxy::start(etcd_port).await?;
        let dead_port = free_test_port();

        let direct_cfg = EtcdConfig {
            endpoints: vec![format!("127.0.0.1:{etcd_port}")],
            dial_timeout: GoDuration::from_secs(2),
            max_retries: 5,
            username: "root".to_string(),
            password: "secretpass".to_string(),
            ..Default::default()
        };
        let direct = Arc::new(EtcdKv::new(direct_cfg, Arc::new(StringCodec))?);

        let watch_cfg = EtcdConfig {
            endpoints: vec![
                format!("127.0.0.1:{dead_port}"),
                format!("127.0.0.1:{}", proxy.port),
            ],
            dial_timeout: GoDuration::from_secs(2),
            max_retries: 5,
            username: "root".to_string(),
            password: "secretpass".to_string(),
            ..Default::default()
        };
        let client = Arc::new(EtcdKv::new(watch_cfg, Arc::new(StringCodec))?);

        cas_typed(direct.as_ref(), "alpha/1", |_: Option<&String>| {
            Ok(Some(("a1".to_string(), true)))
        })
        .await?;
        cas_typed(direct.as_ref(), "beta/2", |_: Option<&String>| {
            Ok(Some(("b2".to_string(), true)))
        })
        .await?;
        let mut keys = client.list("").await?;
        keys.sort();
        ensure!(
            keys == vec!["alpha/1".to_string(), "beta/2".to_string()],
            "list(\"\") failed: {keys:?}"
        );

        let (pref_tx, mut pref_rx) = mpsc::unbounded_channel::<(String, String)>();
        let ctx_pref = ServiceContext::background();
        let c_pref = Arc::clone(&client);
        let ctx_p_clone = ctx_pref.clone();
        let h_pref = tokio::spawn(async move {
            c_pref
                .watch_prefix(
                    "",
                    ctx_p_clone,
                    Box::new(move |k, v| {
                        if let Ok(s) = v.downcast::<String>() {
                            let _ = pref_tx.send((k.to_string(), *s));
                        }
                        true
                    }),
                )
                .await;
        });
        tokio::time::sleep(Duration::from_millis(250)).await;
        cas_typed(direct.as_ref(), "gamma/3", |_: Option<&String>| {
            Ok(Some(("g3".to_string(), true)))
        })
        .await?;
        let ev_pref = tokio::time::timeout(Duration::from_secs(5), pref_rx.recv())
            .await?
            .context("watch_prefix(\"\")")?;
        ensure!(ev_pref == ("gamma/3".to_string(), "g3".to_string()));
        ctx_pref.cancel();
        let _ = h_pref.await;

        let (wk_tx, mut wk_rx) = mpsc::unbounded_channel::<Result<String, ()>>();
        let ctx_wk = ServiceContext::background();
        let c_wk = Arc::clone(&client);
        let ctx_wk_clone = ctx_wk.clone();
        let h_wk = tokio::spawn(async move {
            c_wk.watch_key(
                "watch/k1",
                ctx_wk_clone,
                Box::new(move |any| {
                    if is_key_deleted(any.as_ref()) {
                        let _ = wk_tx.send(Err(()));
                    } else if let Ok(s) = any.downcast::<String>() {
                        let _ = wk_tx.send(Ok(*s));
                    }
                    true
                }),
            )
            .await;
        });
        tokio::time::sleep(Duration::from_millis(250)).await;

        cas_typed(direct.as_ref(), "watch/k1", |_: Option<&String>| {
            Ok(Some(("v1".to_string(), true)))
        })
        .await?;
        ensure!(
            tokio::time::timeout(Duration::from_secs(5), wk_rx.recv())
                .await?
                .context("v1")?
                == Ok("v1".to_string())
        );

        proxy.pause_and_drop();
        tokio::time::sleep(Duration::from_millis(150)).await;
        cas_typed(direct.as_ref(), "watch/k1", |_: Option<&String>| {
            Ok(Some(("v2".to_string(), true)))
        })
        .await?;
        proxy.resume();

        ensure!(
            tokio::time::timeout(Duration::from_secs(10), wk_rx.recv())
                .await?
                .context("v2 after reconnect")?
                == Ok("v2".to_string())
        );

        direct.delete("watch/k1").await?;
        ensure!(
            tokio::time::timeout(Duration::from_secs(5), wk_rx.recv())
                .await?
                .context("delete")?
                == Err(())
        );
        ctx_wk.cancel();
        let _ = h_wk.await;

        let (cmp_tx, mut cmp_rx) = mpsc::unbounded_channel::<String>();
        let ctx_cmp = ServiceContext::background();
        let c_cmp = Arc::clone(&client);
        let ctx_cmp_clone = ctx_cmp.clone();
        let h_cmp = tokio::spawn(async move {
            c_cmp
                .watch_key(
                    "compact/k",
                    ctx_cmp_clone,
                    Box::new(move |any| {
                        if let Ok(s) = any.downcast::<String>() {
                            let _ = cmp_tx.send(*s);
                        }
                        true
                    }),
                )
                .await;
        });
        tokio::time::sleep(Duration::from_millis(250)).await;

        cas_typed(direct.as_ref(), "compact/k", |_: Option<&String>| {
            Ok(Some(("c1".to_string(), true)))
        })
        .await?;
        ensure!(
            tokio::time::timeout(Duration::from_secs(5), cmp_rx.recv())
                .await?
                .context("c1")?
                == "c1"
        );

        proxy.pause_and_drop();
        tokio::time::sleep(Duration::from_millis(150)).await;
        for v in ["c2", "c3", "c4"] {
            let val = v.to_string();
            cas_typed(direct.as_ref(), "compact/k", move |_: Option<&String>| {
                Ok(Some((val.clone(), true)))
            })
            .await?;
        }
        let rev_out = etcdctl(
            &container,
            &[
                "--user=root:secretpass",
                "endpoint",
                "status",
                "--write-out=json",
            ],
        )
        .await?;
        let status_json: serde_json::Value = serde_json::from_str(&rev_out)?;
        let cur_rev = status_json[0]["Status"]["header"]["revision"]
            .as_i64()
            .context("parse etcd revision")?;
        etcdctl(
            &container,
            &[
                "--user=root:secretpass",
                "compact",
                "--physical",
                &cur_rev.to_string(),
            ],
        )
        .await?;
        proxy.resume();

        ensure!(
            tokio::time::timeout(Duration::from_secs(10), cmp_rx.recv())
                .await?
                .context("c4 after compaction recovery")?
                == "c4"
        );
        cas_typed(direct.as_ref(), "compact/k", |_: Option<&String>| {
            Ok(Some(("c5".to_string(), true)))
        })
        .await?;
        ensure!(
            tokio::time::timeout(Duration::from_secs(5), cmp_rx.recv())
                .await?
                .context("c5 live after compaction")?
                == "c5"
        );

        ctx_cmp.cancel();
        let _ = h_cmp.await;
        Ok(())
    })
}

#[test]
fn test_w11_p1_3_and_p1_4_shipper_go_nulls_and_delete_order() -> Result<()> {
    block_on_for_tests(async {
        let blocks_dir = tempfile::tempdir()?;
        let bkt_dir = tempfile::tempdir()?;
        let bkt = BucketClient::new_filesystem(bkt_dir.path())?;

        let id = Ulid::from_parts(1_700_000_000_000, 1);
        let bdir = blocks_dir.path().join(id.to_string());
        fs::create_dir_all(bdir.join("chunks"))?;
        fs::write(bdir.join("chunks").join("000001"), b"chunkdata")?;
        fs::write(bdir.join(INDEX_FILENAME), b"indexdata")?;
        let go_meta = format!(
            r#"{{
                "ulid": "{id}",
                "minTime": 1000,
                "maxTime": 2000,
                "stats": {{"numSamples": 10, "numSeries": 1, "numChunks": 1}},
                "compaction": {{
                    "level": 1,
                    "sources": null,
                    "parents": null,
                    "hints": null
                }},
                "version": 1,
                "thanos": {{
                    "version": 1,
                    "labels": null,
                    "source": "receive",
                    "segment_files": null,
                    "files": null
                }}
            }}"#
        );
        fs::write(bdir.join(META_FILENAME), go_meta)?;
        fs::write(
            blocks_dir.path().join(SHIPPER_META_FILENAME),
            r#"{"version":1,"shipped":null}"#,
        )?;

        let meta = BlockMeta::read_from_dir(&bdir)?;
        ensure!(meta.thanos.labels.is_empty());
        let sm: ShipperMeta =
            serde_json::from_slice(&fs::read(blocks_dir.path().join(SHIPPER_META_FILENAME))?)?;
        ensure!(sm.shipped.is_empty());

        let shipper = Shipper::new(
            "tenant-null",
            blocks_dir.path(),
            bkt.for_user("tenant-null"),
            SourceType::Receive,
            false,
            ShipperMetrics::new(None),
        );
        let shipped = shipper.sync().await?;
        ensure!(shipped == 1, "expected 1 block shipped, got {shipped}");
        ensure!(
            bkt.exists(&format!("tenant-null/{id}/{META_FILENAME}"))
                .await?,
            "shipped meta.json must exist in bucket"
        );

        let fail_id = Ulid::from_parts(1_700_000_005_000, 2);
        let fail_dir = blocks_dir.path().join(fail_id.to_string());
        fs::create_dir_all(fail_dir.join("chunks"))?;
        fs::write(fail_dir.join("chunks").join("000001"), b"chunk")?;
        // Make `index` a directory and include `meta.json` so `gather_file_stats` passes and `upload_file("index")` fails.
        fs::create_dir_all(fail_dir.join(INDEX_FILENAME))?;
        fs::write(fail_dir.join(META_FILENAME), b"{}")?;
        bkt.upload(
            &format!("tenant-null/{fail_id}/{META_FILENAME}"),
            Bytes::from_static(b"{}"),
        )
        .await?;
        let mut fail_meta = BlockMeta {
            ulid: fail_id,
            min_time: 5000,
            max_time: 6000,
            stats: BlockStats::default(),
            compaction: BlockMetaCompaction::default(),
            version: TSDB_VERSION_1,
            out_of_order: false,
            thanos: ThanosMeta {
                version: 1,
                labels: BTreeMap::new(),
                source: SourceType::Receive,
                ..Default::default()
            },
        };
        let err = upload_block_to_bucket(&bkt.for_user("tenant-null"), &fail_dir, &mut fail_meta)
            .await
            .err()
            .context("missing index must fail upload")?;
        ensure!(matches!(err, ShipperError::Upload { file_type: "index", .. }));
        ensure!(
            !bkt.exists(&format!("tenant-null/{fail_id}/{META_FILENAME}"))
                .await?,
            "partial upload cleanup must have deleted stale meta.json"
        );
        ensure!(
            !bkt.exists(&format!("tenant-null/{fail_id}/chunks/000001"))
                .await?,
            "partial upload cleanup must have deleted chunks"
        );
        Ok(())
    })
}
