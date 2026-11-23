//! Wave 37 Adversarial Bug-Hunting & Differential Fuzzing Suite
//!
//! 1. PartitionRing lookup differential fuzz vs go-oracle partition-ring-lookup (180 states)
//! 2. Adversarial Memberlist TCP/UDP wire + ProtoSnappyCodec mutation fuzz (25,000+ packets)
//! 3. Adversarial DNS wire packet mutation fuzz into mimir_ring::dns::godns (10,500+ packets)
//! 4. Adversarial CLI / env / Go durations / HumanBytes / CIDRs / HTTP query params /
//!    X-Scope-OrgID / uber-trace-id / httpgrpc mutation fuzz (24,000+ inputs)
//! 5. Multi-threaded mimir-ingester 6-task concurrency chaos fuzz
//! 6. Live twin-process Go (target/go-mimir) vs Rust (target/release/mimir) Ingester
//!    differential fuzz across all 13 read RPCs + Push + ErrorDetails parity.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Cursor;
use std::net::{IpAddr, Ipv4Addr};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, Request as AxumRequest};
use base64::Engine as _;
use bytes::Bytes;
use mimir_cmd::cli::{parse_cli_for_run, run_cli_test_mode};
use mimir_cmd::grpc::{
    httpgrpc_error_from_response, httpgrpc_response_from_error, httpgrpc_to_headers,
    parse_httpgrpc_url, HttpGrpcBridge,
};
use mimir_cmd::http::{build_http_router, AppHttpState};
use mimir_cmd::modules::ModuleStates;
use mimir_common::block_on_for_tests;
use mimir_common::tenant::{
    extract_org_id_from_grpc_metadata, extract_org_id_from_http_headers, parse_single_tenant_id_bytes,
    parse_tenant_ids_bytes, parse_with_metadata_bytes, valid_metadata_bytes, valid_tenant_id_bytes,
    Metadata as TenantMetadata, LOWER_ORG_ID_HEADER_NAME,
};
use mimir_common::tracing::{
    extract_from_headers, parse_traceparent, parse_uber_trace_id, PropagatorSet,
};
use mimir_config::{
    expand_environment_variables_with, format_go_duration, parse_config_file_parameter,
    parse_duration, parse_go_duration, parse_model_duration, BlocksStorageConfig, CIDRSliceCSV,
    GoDuration, HumanBytes, IngestStorageConfig, IngesterConfig, LimitsConfig, MimirConfig,
    ModelDuration, Overrides,
};
use mimir_conformance::oracle::{
    ensure_go_oracle_built, ensure_rust_mimir_release_built, go_mimir_bin, invoke_oracle,
    rust_mimir_release_bin, PartitionRingLookupRequest, PartitionRingLookupResponse,
};
use mimir_ingester::costattribution::Manager as CostAttributionManager;
use mimir_ingester::{Ingester, IngesterService};
use mimir_proto::cortexpb;
use mimir_proto::grpcutilpb;
use mimir_proto::httpgrpcpb::http_server::Http as HttpGrpcTrait;
use mimir_proto::httpgrpcpb::{Header as HttpGrpcHeader, HttpRequest, HttpResponse};
use mimir_proto::ingesterpb;
use mimir_proto::ingesterpb::ingester_client::IngesterClient;
use mimir_proto::memberlistpb::KeyValuePair;
use mimir_proto::prost::Message as ProstMessage;
use mimir_proto::ringpb::{
    Desc as RingDesc, InstanceDesc, InstanceState, OwnerState, PartitionDesc, PartitionRingDesc,
    PartitionState, PartitionTokenScheme,
};
use mimir_ring::dns::godns::Resolver as GoDnsResolver;
use mimir_ring::dns::IpLookupResolver;
use mimir_ring::hickory_resolver::config::{NameServerConfigGroup, ResolveHosts, ResolverConfig};
use mimir_ring::hickory_resolver::name_server::TokioConnectionProvider;
use mimir_ring::hickory_resolver::proto::op::Message as DnsMessage;
use mimir_ring::hickory_resolver::proto::serialize::binary::BinDecodable;
use mimir_ring::hickory_resolver::TokioResolver;
use mimir_ring::kv::memberlist::lzw::{lzw_compress, lzw_decompress};
use mimir_ring::kv::memberlist::security::{decrypt_payload, encrypt_payload, pkcs7_decode};
use mimir_ring::kv::memberlist::stream::{
    decrypt_remote_state_for_test, read_msgpack_map_bytes_for_test,
    read_push_pull_from_stream_for_test, read_user_msg_from_stream_for_test,
    remove_label_header_from_stream, skip_sync_val_for_test,
};
use mimir_ring::kv::memberlist::wire::{
    add_label_header, compress_payload, decode_ack_resp, decode_alive, decode_compound_message,
    decode_compressed_payload, decode_dead, decode_err_resp, decode_indirect_ping, decode_nack_resp,
    decode_ping, decode_push_pull, decode_suspect, decode_user_msg_stream,
    decompress_buffer_with_limit, decompress_payload, encode_ack_resp, encode_alive, encode_dead,
    encode_err_resp, encode_indirect_ping, encode_nack_resp, encode_ping, encode_push_pull,
    encode_suspect, encode_user_msg_stream, make_compound_message, remove_label_header,
    unwrap_crc32, wrap_crc32, AckResp, Alive, CompressionType as MlCompressionType, Dead, ErrResp,
    IndirectPingReq, MessageType as MlMsgType, NackResp, NodeStateType, Ping, PushNodeState,
    Suspect,
};
use mimir_ring::kv::ProtoSnappyCodec;
use mimir_ring::partition_ring::{
    partition_ring_desc_codec, ActivePartitionBatchRing, PartitionRing, PartitionRingDescExt,
    PartitionRingOptions,
};
use mimir_ring::ring::http::{go_parse_media_type, go_parse_query, go_query_unescape, parse_go_form};
use mimir_ring::ring::ring_desc_codec;
use mimir_ring::token_gen::PartitionTokenTable;
use mimir_tsdb::chunkenc::{EncodedChunk, Encoding};
use mimir_tsdb::compact::{decode_chunk_samples, SampleValue};
use prometheus::Registry;
use tokio::net::{TcpListener, UdpSocket};
use tokio_stream::StreamExt;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, Server};
use tower::ServiceExt;

// =================================================================================================
// Deterministic PRNG & Mutation Helpers
// =================================================================================================

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(if seed == 0 { 0x9e37_79b9_7f4a_7c15 } else { seed })
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn next_u32(&mut self) -> u32 {
        (self.next_u64() >> 32) as u32
    }

    fn gen_range(&mut self, lo: usize, hi: usize) -> usize {
        if hi <= lo {
            return lo;
        }
        lo + (self.next_u64() as usize) % (hi - lo)
    }

    fn gen_bool(&mut self) -> bool {
        (self.next_u64() & 1) == 1
    }

    fn gen_bytes(&mut self, len: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(len);
        while out.len() < len {
            let v = self.next_u64().to_le_bytes();
            let take = (len - out.len()).min(8);
            out.extend_from_slice(&v[..take]);
        }
        out
    }

    fn mutate_bytes(&mut self, seed: &[u8], max_len: usize) -> Vec<u8> {
        if seed.is_empty() || self.gen_range(0, 10) == 0 {
            let l = self.gen_range(0, max_len.clamp(1, 256));
            return self.gen_bytes(l);
        }
        let mut out = seed.to_vec();
        let ops = self.gen_range(1, 6);
        for _ in 0..ops {
            if out.is_empty() {
                break;
            }
            match self.gen_range(0, 6) {
                0 => {
                    let idx = self.gen_range(0, out.len());
                    let bit = 1u8 << self.gen_range(0, 8);
                    out[idx] ^= bit;
                }
                1 => {
                    let idx = self.gen_range(0, out.len());
                    out[idx] = self.next_u64() as u8;
                }
                2 => {
                    let new_len = self.gen_range(0, out.len());
                    out.truncate(new_len);
                }
                3 => {
                    if out.len() < max_len {
                        let idx = self.gen_range(0, out.len() + 1);
                        let b = self.next_u64() as u8;
                        out.insert(idx, b);
                    }
                }
                4 => {
                    if out.len() * 2 <= max_len {
                        let start = self.gen_range(0, out.len());
                        let end = self.gen_range(start, out.len());
                        let sub = out[start..end].to_vec();
                        out.extend_from_slice(&sub);
                    }
                }
                _ => {
                    let idx = self.gen_range(0, out.len());
                    let count = self.gen_range(1, 5).min(out.len() - idx);
                    for j in 0..count {
                        out[idx + j] = 0xff;
                    }
                }
            }
        }
        out
    }

    fn gen_ascii_str(&mut self, min_len: usize, max_len: usize) -> String {
        const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.:/${}%+&=?;| ";
        let len = self.gen_range(min_len, max_len + 1);
        let mut s = String::with_capacity(len);
        for _ in 0..len {
            let idx = self.gen_range(0, ALPHABET.len());
            s.push(ALPHABET[idx] as char);
        }
        s
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

// =================================================================================================
// 1. PartitionRing Lookup Differential Fuzz vs Go Oracle (180 randomized states)
// =================================================================================================

#[test]
fn fuzz_01_partition_ring_lookup_differential() {
    ensure_go_oracle_built().expect("go-oracle must build");
    let b64 = base64::engine::general_purpose::STANDARD;
    let mut rng = Rng::new(0x3701_fa22_1122_3344);
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);

    let max_derived_choices = [-1i32, 0, 1, 4, 8, 16];
    let states = [
        PartitionState::PartitionPending,
        PartitionState::PartitionActive,
        PartitionState::PartitionInactive,
        PartitionState::PartitionDeleted,
        PartitionState::PartitionUnknown,
    ];
    let ops = ["Write", "Read", ""];

    let mut total_states = 0usize;
    let mut both_ok = 0usize;
    let mut both_err = 0usize;

    for iter in 0..120 {
        total_states += 1;
        let max_derived = max_derived_choices[rng.gen_range(0, max_derived_choices.len())];
        let mut desc = PartitionRingDesc::default();

        let num_partitions = if iter % 15 == 0 {
            0
        } else {
            rng.gen_range(1, 9)
        };

        let mut used_pids = HashSet::new();
        for p_idx in 0..num_partitions {
            let pid = if iter % 23 == 0 && p_idx == 0 {
                -1
            } else if rng.gen_range(0, 4) == 0 {
                rng.gen_range(0, 20) as i32
            } else {
                p_idx as i32
            };
            if !used_pids.insert(pid) {
                continue;
            }

            let st = states[rng.gen_range(0, states.len())];
            let use_derived = match iter % 4 {
                0 => false,
                1 => true,
                _ => rng.gen_bool(),
            };

            if use_derived {
                let mut stray_tokens = Vec::new();
                if rng.gen_range(0, 5) == 0 {
                    stray_tokens.push(rng.next_u32());
                }
                desc.partitions.insert(
                    pid,
                    PartitionDesc {
                        id: pid,
                        tokens: stray_tokens,
                        state: st as i32,
                        state_timestamp: 1_700_000_000 + (p_idx as i64) * 10,
                        state_change_locked: rng.gen_bool(),
                        state_change_locked_timestamp: 1_700_000_000,
                        token_scheme: PartitionTokenScheme::PartitionTokensSmt512 as i32,
                    },
                );
            } else {
                let tok_count = if rng.gen_range(0, 6) == 0 {
                    0
                } else {
                    rng.gen_range(1, 9)
                };
                let mut toks = Vec::with_capacity(tok_count);
                for _ in 0..tok_count {
                    let t = match rng.gen_range(0, 8) {
                        0 => 0,
                        1 => 1,
                        2 => u32::MAX - 1,
                        3 => u32::MAX,
                        _ => rng.next_u32(),
                    };
                    if !toks.contains(&t) {
                        toks.push(t);
                    }
                }
                desc.partitions.insert(
                    pid,
                    PartitionDesc {
                        id: pid,
                        tokens: toks,
                        state: st as i32,
                        state_timestamp: 1_700_000_000 + (p_idx as i64) * 10,
                        state_change_locked: rng.gen_bool(),
                        state_change_locked_timestamp: 1_700_000_000,
                        token_scheme: PartitionTokenScheme::PartitionTokensStored as i32,
                    },
                );
            }

            let owner_count = rng.gen_range(0, 4);
            for o_idx in 0..owner_count {
                let ost = if rng.gen_bool() {
                    OwnerState::OwnerActive
                } else {
                    OwnerState::OwnerDeleted
                };
                let oid = format!("ing-{pid}-{o_idx}");
                desc.add_or_update_owner(&oid, ost, pid, now);
            }
        }

        if rng.gen_range(0, 5) == 0 {
            let ost = if rng.gen_bool() {
                OwnerState::OwnerActive
            } else {
                OwnerState::OwnerDeleted
            };
            desc.add_or_update_owner("orphan-ing", ost, 99, now);
        }

        let mut seen_tokens = HashSet::new();
        if max_derived >= 0 {
            use mimir_ring::token_gen::PartitionTokenGenerator as _;
            if let Ok(table) = PartitionTokenTable::new(max_derived, None) {
                for (&pid, p) in &desc.partitions {
                    if p.token_scheme == PartitionTokenScheme::PartitionTokensSmt512 as i32 {
                        if let Ok(dtoks) = table.tokens_for(pid) {
                            seen_tokens.extend(dtoks.iter().copied());
                        }
                    }
                }
            }
        }
        let mut sorted_pids: Vec<i32> = desc.partitions.keys().copied().collect();
        sorted_pids.sort_unstable();
        for pid in sorted_pids {
            if let Some(p) = desc.partitions.get_mut(&pid) {
                if p.token_scheme == PartitionTokenScheme::PartitionTokensStored as i32 {
                    p.tokens.retain(|t| seen_tokens.insert(*t));
                }
            }
        }

        let token = match iter % 7 {
            0 => 0,
            1 => 1,
            2 => u32::MAX - 1,
            3 => u32::MAX,
            _ => rng.next_u32(),
        };
        let op = ops[rng.gen_range(0, ops.len())];
        let desc_b64 = b64.encode(desc.encode_to_vec());

        let go_resp: PartitionRingLookupResponse = invoke_oracle(
            "partition-ring-lookup",
            &PartitionRingLookupRequest {
                desc_b64,
                max_derived_token_partitions: max_derived,
                token,
                op: op.to_string(),
            },
        )
        .unwrap_or_else(|e| panic!("iter={iter}: go-oracle partition-ring-lookup failed: {e}"));

        let rust_ring_res = (|| -> Result<(i32, Vec<String>, Arc<PartitionRing>), String> {
            let token_gen = if max_derived >= 0 {
                let table = PartitionTokenTable::new(max_derived, None)
                    .map_err(|e| e.to_string())?;
                Some(Arc::new(table) as Arc<dyn mimir_ring::token_gen::PartitionTokenGenerator>)
            } else {
                None
            };
            let pr = PartitionRing::with_options(
                desc.clone(),
                PartitionRingOptions {
                    token_generator: token_gen,
                    shuffle_shard_cache_size: 4,
                },
            )
            .map_err(|e| e.to_string())?;
            let pid = pr.active_partition_for_key(token).map_err(|e| e.to_string())?;
            let owners = pr.partition_owner_ids_copy(pid);
            Ok((pid, owners, pr))
        })();

        match (rust_ring_res, &go_resp.error) {
            (Ok((rust_pid, rust_owners, pr)), None) => {
                both_ok += 1;
                if Some(rust_pid) != go_resp.partition_id {
                    eprintln!("F1_DEBUG iter={iter} max_derived={max_derived} token={token} op={op} rust_pid={rust_pid} go_pid={:?}\n  desc={desc:?}", go_resp.partition_id);
                }
                assert_eq!(
                    Some(rust_pid),
                    go_resp.partition_id,
                    "iter={iter} max_derived={max_derived} token={token} op={op}: pid mismatch"
                );
                assert_eq!(
                    rust_owners, go_resp.owners,
                    "iter={iter} pid={rust_pid}: owners mismatch"
                );

                if !pr.ring_tokens().is_empty() {
                    let counts = pr.count_tokens();
                    let sum: i64 = counts.values().sum();
                    assert_eq!(
                        sum,
                        1i64 << 32,
                        "iter={iter}: token distance sum must cover full u32 space"
                    );
                }
                let batch_ring = ActivePartitionBatchRing::new(Arc::clone(&pr));
                let sample_keys = [0u32, token, token.wrapping_add(1), u32::MAX];
                if !pr.partitions().iter().any(|p| p.id < 0) {
                    let grouped = batch_ring.get_keys_by_partition(None, &sample_keys).unwrap();
                    let mut total_grouped = 0usize;
                    for g in grouped {
                        for idx in g.indexes {
                            total_grouped += 1;
                            assert_eq!(
                                pr.active_partition_for_key(sample_keys[idx]).unwrap(),
                                g.partition_id
                            );
                        }
                    }
                    assert_eq!(total_grouped, sample_keys.len());
                }
                let _ = pr.shuffle_shard("tenant-fuzz", 2);
                let _ = pr.shuffle_shard_with_lookback(
                    "tenant-fuzz",
                    2,
                    Duration::from_secs(60),
                    now + Duration::from_secs(120),
                );
                let _ = pr.get_token_ranges_for_partition(rust_pid);
            }
            (Err(_), Some(_)) => {
                both_err += 1;
            }
            (Ok((pid, owners, _)), Some(ge)) => {
                panic!(
                    "iter={iter} max_derived={max_derived} token={token} op={op}: Rust Ok(pid={pid}, owners={owners:?}) vs Go Err({ge})"
                );
            }
            (Err(re), None) => {
                panic!(
                    "iter={iter} max_derived={max_derived} token={token} op={op}: Rust Err({re}) vs Go Ok(pid={:?}, owners={:?})",
                    go_resp.partition_id, go_resp.owners
                );
            }
        }
    }

    assert_eq!(total_states, 120);
    assert!(both_ok > 0 && both_err > 0, "expected both Ok ({both_ok}) and Err ({both_err}) cases");
}

// =================================================================================================
// 2. Adversarial Memberlist Wire + ProtoSnappyCodec Mutation Fuzz (25,000+ packets)
// =================================================================================================

#[test]
fn fuzz_02_memberlist_wire_and_snappy_packets() {
    let mut rng = Rng::new(0x3702_deed_beef_0002);

    let ping_enc = encode_ping(&Ping {
        seq_no: 42,
        node: "n1".into(),
        source_addr: vec![127, 0, 0, 1],
        source_port: 7946,
        source_node: "n0".into(),
    });
    let iping_enc = encode_indirect_ping(&IndirectPingReq {
        seq_no: 7,
        target: vec![10, 0, 0, 1],
        port: 7946,
        node: "n2".into(),
        nack: true,
        source_addr: vec![10, 0, 0, 2],
        source_port: 7946,
        source_node: "n0".into(),
    });
    let ack_enc = encode_ack_resp(&AckResp {
        seq_no: 1,
        payload: Some(b"ok".to_vec()),
    });
    let nack_enc = encode_nack_resp(&NackResp { seq_no: 2 });
    let err_enc = encode_err_resp(&ErrResp {
        error: "err".into(),
    });
    let susp_enc = encode_suspect(&Suspect {
        incarnation: 5,
        node: "n3".into(),
        from: "n0".into(),
    });
    let alive_enc = encode_alive(&Alive {
        incarnation: 3,
        node: "n1".into(),
        addr: vec![127, 0, 0, 1],
        port: 7946,
        meta: Some(vec![1, 2, 3]),
        vsn: vec![1, 5, 2, 0, 0, 0],
    });
    let dead_enc = encode_dead(&Dead {
        incarnation: 4,
        node: "n1".into(),
        from: "n0".into(),
    });
    let pp_enc = encode_push_pull(
        &[PushNodeState {
            name: "n1".into(),
            addr: vec![127, 0, 0, 1],
            port: 7946,
            meta: Some(vec![1, 1, 0]),
            incarnation: 1,
            state: NodeStateType::Alive,
            vsn: vec![1, 5, 2, 0, 0, 0],
        }],
        b"user-kv-state",
        true,
    );
    let umsg_enc = encode_user_msg_stream(b"hello-msg");
    let comp_enc = make_compound_message(&[&ping_enc, &ack_enc, &alive_enc]);
    let lzw_enc = compress_payload(MlCompressionType::Lzw, &comp_enc).unwrap();
    let snappy_enc = compress_payload(MlCompressionType::Snappy, &comp_enc).unwrap();
    let crc_enc = wrap_crc32(&snappy_enc);
    let lbl_enc = add_label_header(&crc_enc, "cluster-a").unwrap();

    let aes_key = vec![0x42u8; 16];
    let mut enc_pkt = Vec::new();
    encrypt_payload(1, &aes_key, &ping_enc, b"cluster-a", &mut enc_pkt).unwrap();
    let lbl_enc_aes = add_label_header(&enc_pkt, "cluster-a").unwrap();

    let mut sample_ring = RingDesc::default();
    sample_ring.ingesters.insert(
        "ing-0".into(),
        InstanceDesc {
            addr: "127.0.0.1:9095".into(),
            timestamp: 1_700_000_000,
            state: InstanceState::Active as i32,
            tokens: vec![1, 1000, 2000],
            zone: "zone-a".into(),
            registered_timestamp: 1_700_000_000,
            ..Default::default()
        },
    );
    let ring_snappy = ProtoSnappyCodec::<RingDesc>::new("ringDesc");
    let pring_snappy = ProtoSnappyCodec::<PartitionRingDesc>::new("partitionRingDesc");
    let kv_snappy = ProtoSnappyCodec::<KeyValuePair>::new("kv");
    let encoded_ring = ring_snappy.encode_msg(&sample_ring).unwrap();

    let mut sample_pring = PartitionRingDesc::default();
    sample_pring.partitions.insert(
        0,
        PartitionDesc {
            state: PartitionState::PartitionActive as i32,
            state_timestamp: 1_700_000_000,
            tokens: vec![10, 20],
            token_scheme: PartitionTokenScheme::PartitionTokensStored as i32,
            ..Default::default()
        },
    );
    let encoded_pring = pring_snappy.encode_msg(&sample_pring).unwrap();
    let encoded_kv = KeyValuePair {
        key: "collectors/ring".into(),
        value: encoded_ring.clone().into(),
        codec: "ringDesc".into(),
        ..Default::default()
    }
    .encode_to_vec();
    let encoded_lzw_raw = lzw_compress(b"memberlist-lzw-fuzz-seed-payload-0123456789");

    let templates: [&[u8]; 19] = [
        &ping_enc,
        &iping_enc,
        &ack_enc,
        &nack_enc,
        &err_enc,
        &susp_enc,
        &alive_enc,
        &dead_enc,
        &pp_enc,
        &umsg_enc,
        &comp_enc,
        &lzw_enc,
        &snappy_enc,
        &crc_enc,
        &lbl_enc,
        &lbl_enc_aes,
        &encoded_ring,
        &encoded_pring,
        &encoded_kv,
    ];

    let ring_codec = ring_desc_codec();
    let pring_codec = partition_ring_desc_codec();
    let keys = vec![aes_key];

    block_on_for_tests(async {
        let mut iterations = 0usize;
        for iter in 0..10_000usize {
            iterations += 1;
            let pkt: Vec<u8> = match iter % 8 {
                0 => {
                    let tmpl = templates[rng.gen_range(0, templates.len())];
                    rng.mutate_bytes(tmpl, 1024)
                }
                1 => {
                    let tmpl = templates[rng.gen_range(0, 11)];
                    let slice = if tmpl.len() > 1 { &tmpl[1..] } else { tmpl };
                    rng.mutate_bytes(slice, 512)
                }
                2 => {
                    let tags = [0xc9u8, 0xdd, 0xdf, 0xdb, 0xc6, 0xda, 0xde, 0xdc];
                    let tag = tags[rng.gen_range(0, tags.len())];
                    let depth = rng.gen_range(1, 48);
                    let mut v = vec![0x91u8; depth];
                    v.push(tag);
                    v.extend_from_slice(&u32::MAX.to_be_bytes());
                    v.extend_from_slice(&rng.gen_bytes(16));
                    v
                }
                3 => rng.mutate_bytes(&encoded_lzw_raw, 512),
                4 => {
                    let mut bomb = vec![0xff, 0xff, 0xff, 0xff, 0x0f];
                    bomb.extend_from_slice(&rng.gen_bytes(24));
                    bomb
                }
                5 => {
                    let p1 = rng.mutate_bytes(&ping_enc, 128);
                    let p2 = rng.mutate_bytes(&comp_enc, 128);
                    let mut c = make_compound_message(&[&p1, &p2]);
                    if rng.gen_bool() {
                        c = wrap_crc32(&c);
                    }
                    if rng.gen_bool() {
                        if let Ok(l) = add_label_header(&c, "cluster-a") {
                            c = l;
                        }
                    }
                    c
                }
                6 => {
                    let mut c = rng.mutate_bytes(&lbl_enc_aes, 256);
                    if !c.is_empty() && rng.gen_bool() {
                        c[0] = MlMsgType::Encrypt as u8;
                    }
                    c
                }
                _ => {
                    let l = rng.gen_range(0, 256);
                    rng.gen_bytes(l)
                }
            };

            if let Ok((after_lbl, lbl)) = remove_label_header(&pkt) {
                let _ = decrypt_payload(&keys, after_lbl, lbl.as_bytes());
                if let Ok(after_crc) = unwrap_crc32(after_lbl) {
                    if let Some((&first, body)) = after_crc.split_first() {
                        match MlMsgType::try_from(first) {
                            Ok(MlMsgType::Compound) => {
                                if let Ok((_trunc, parts)) = decode_compound_message(body) {
                                    for p in parts.into_iter().take(64) {
                                        if let Some((&pt, pb)) = p.split_first() {
                                            let _ = (pt, decode_ping(pb), decode_alive(pb));
                                        }
                                    }
                                }
                            }
                            Ok(MlMsgType::Compress) => {
                                let (_, _) = decompress_payload(body);
                            }
                            _ => {}
                        }
                    }
                }
            }

            let body = if pkt.len() > 1 { &pkt[1..] } else { &pkt[..] };
            let _ = decode_ping(body);
            let _ = decode_indirect_ping(body);
            let _ = decode_ack_resp(body);
            let _ = decode_nack_resp(body);
            let _ = decode_err_resp(body);
            let _ = decode_suspect(body);
            let _ = decode_alive(body);
            let _ = decode_dead(body);
            let _ = decode_compressed_payload(body);
            let _ = decode_push_pull(body);
            let _ = decode_user_msg_stream(body);
            let _ = decode_compound_message(body);
            let _ = decompress_buffer_with_limit(MlCompressionType::Lzw, &pkt, 65_536);
            let _ = decompress_buffer_with_limit(MlCompressionType::Snappy, &pkt, 65_536);
            let _ = lzw_decompress(&pkt, 65_536);
            let _ = pkcs7_decode(&pkt, 16);

            let _ = ring_codec.decode(&pkt);
            let _ = pring_codec.decode(&pkt);
            let _ = kv_snappy.decode_msg(&pkt);
            let _ = KeyValuePair::decode(pkt.as_slice());

            if iter % 5 == 0 {
                let mut pos = 0usize;
                let _ = skip_sync_val_for_test(&pkt, &mut pos);
                let mut c1 = Cursor::new(pkt.as_slice());
                let _ = remove_label_header_from_stream(&mut c1).await;
                let mut c2 = Cursor::new(pkt.as_slice());
                let _ = read_msgpack_map_bytes_for_test(&mut c2).await;
                let mut c3 = Cursor::new(pkt.as_slice());
                let _ = read_push_pull_from_stream_for_test(&mut c3).await;
                let mut c4 = Cursor::new(pkt.as_slice());
                let _ = read_user_msg_from_stream_for_test(&mut c4).await;
                let mut c5 = Cursor::new(pkt.as_slice());
                let _ = decrypt_remote_state_for_test(&mut c5, "cluster-a").await;
            }
        }
        assert!(iterations >= 10_000);
    });
}

// =================================================================================================
// 3. Adversarial DNS Wire Response Packet Fuzz into mimir_ring::dns::godns (10,500+ packets)
// =================================================================================================

fn craft_mutated_dns_packet(rng: &mut Rng, req: &[u8], iter: usize) -> Vec<u8> {
    let txid = if req.len() >= 2 && !iter.is_multiple_of(25) {
        [req[0], req[1]]
    } else {
        [rng.next_u64() as u8, rng.next_u64() as u8]
    };

    let q_end = if req.len() >= 12 {
        let mut p = 12usize;
        while p < req.len() {
            let l = req[p] as usize;
            p += 1;
            if l == 0 {
                break;
            }
            p = p.saturating_add(l);
        }
        (p + 4).min(req.len())
    } else {
        req.len()
    };

    let mut resp = Vec::with_capacity(128);
    if q_end >= 12 {
        resp.extend_from_slice(&req[..q_end]);
        resp[0] = txid[0];
        resp[1] = txid[1];
        resp[2] = 0x81 | if rng.gen_bool() { 0x02 } else { 0x00 };
        resp[3] = 0x80 | ((rng.next_u64() as u8) & 0x0f);
    } else {
        resp.extend_from_slice(&[txid[0], txid[1], 0x81, 0x80, 0, 1, 0, 1, 0, 0, 0, 0]);
    }

    match iter % 9 {
        0 => {
            let ptr_off = resp.len() as u8;
            resp[6..8].copy_from_slice(&1u16.to_be_bytes());
            resp.extend_from_slice(&[0xc0, ptr_off]);
            resp.extend_from_slice(&1u16.to_be_bytes());
            resp.extend_from_slice(&1u16.to_be_bytes());
            resp.extend_from_slice(&30u32.to_be_bytes());
            resp.extend_from_slice(&4u16.to_be_bytes());
            resp.extend_from_slice(&[10, 0, 0, 1]);
        }
        1 => {
            resp[6..8].copy_from_slice(&1u16.to_be_bytes());
            let rdata_off = resp.len() + 12;
            let p1 = (rdata_off & 0x3f) as u8;
            let p2 = ((rdata_off + 2) & 0x3f) as u8;
            resp.extend_from_slice(&[0xc0, 0x0c]);
            resp.extend_from_slice(&5u16.to_be_bytes());
            resp.extend_from_slice(&1u16.to_be_bytes());
            resp.extend_from_slice(&30u32.to_be_bytes());
            resp.extend_from_slice(&4u16.to_be_bytes());
            resp.extend_from_slice(&[0xc0, p2, 0xc0, p1]);
        }
        2 => {
            resp[6..8].copy_from_slice(&1u16.to_be_bytes());
            resp.extend_from_slice(&[0xc0, 0xff]);
            resp.extend_from_slice(&28u16.to_be_bytes());
            resp.extend_from_slice(&1u16.to_be_bytes());
            resp.extend_from_slice(&30u32.to_be_bytes());
            resp.extend_from_slice(&16u16.to_be_bytes());
            resp.extend_from_slice(&rng.gen_bytes(16));
        }
        3 => {
            resp[6..8].copy_from_slice(&2u16.to_be_bytes());
            resp.extend_from_slice(&[0xc0, 0x0c]);
            let rtype = [1u16, 28, 33, 5][rng.gen_range(0, 4)];
            resp.extend_from_slice(&rtype.to_be_bytes());
            resp.extend_from_slice(&1u16.to_be_bytes());
            resp.extend_from_slice(&60u32.to_be_bytes());
            resp.extend_from_slice(&(rng.gen_range(4, 1000) as u16).to_be_bytes());
            let trunc = rng.gen_range(0, 6);
            resp.extend_from_slice(&rng.gen_bytes(trunc));
        }
        4 => {
            resp[6..8].copy_from_slice(&1u16.to_be_bytes());
            resp.extend_from_slice(&[0xc0, 0x0c]);
            resp.extend_from_slice(&33u16.to_be_bytes());
            resp.extend_from_slice(&1u16.to_be_bytes());
            resp.extend_from_slice(&30u32.to_be_bytes());
            let target_off = resp.len() + 8;
            let mut target = if rng.gen_bool() {
                vec![0xc0, (target_off as u8) & 0x3f]
            } else {
                let mut t = vec![4, b'n', b'o', b'd', b'e', 0];
                if rng.gen_bool() {
                    t.truncate(rng.gen_range(0, t.len()));
                }
                t
            };
            if target.len() > 20 {
                target.truncate(20);
            }
            let claimed_len = if rng.gen_bool() {
                (6 + target.len()) as u16
            } else {
                rng.gen_range(0, 12) as u16
            };
            resp.extend_from_slice(&claimed_len.to_be_bytes());
            resp.extend_from_slice(&(rng.next_u64() as u16).to_be_bytes());
            resp.extend_from_slice(&(rng.next_u64() as u16).to_be_bytes());
            resp.extend_from_slice(&(rng.next_u64() as u16).to_be_bytes());
            resp.extend_from_slice(&target);
        }
        5 => {
            resp[6..8].copy_from_slice(&1u16.to_be_bytes());
            resp.extend_from_slice(&[0xc0, 0x0c]);
            let is_v6 = rng.gen_bool();
            let rtype = if is_v6 { 28u16 } else { 1u16 };
            let rlen = if is_v6 {
                [0u16, 4, 15, 16, 17][rng.gen_range(0, 5)]
            } else {
                [0u16, 3, 4, 5][rng.gen_range(0, 4)]
            };
            resp.extend_from_slice(&rtype.to_be_bytes());
            resp.extend_from_slice(&1u16.to_be_bytes());
            resp.extend_from_slice(&30u32.to_be_bytes());
            resp.extend_from_slice(&rlen.to_be_bytes());
            resp.extend_from_slice(&rng.gen_bytes(rlen as usize));
        }
        6 => {
            resp[6..8].copy_from_slice(&u16::MAX.to_be_bytes());
            resp[8..10].copy_from_slice(&u16::MAX.to_be_bytes());
            resp[10..12].copy_from_slice(&u16::MAX.to_be_bytes());
            resp.extend_from_slice(&rng.gen_bytes(16));
        }
        7 => {
            resp[3] = 0x80;
            resp[6..8].copy_from_slice(&1u16.to_be_bytes());
            resp.extend_from_slice(&[0xc0, 0x0c]);
            let qtype = if req.len() >= q_end && q_end >= 4 {
                u16::from_be_bytes([req[q_end - 4], req[q_end - 3]])
            } else {
                1
            };
            match qtype {
                28 => {
                    resp.extend_from_slice(&28u16.to_be_bytes());
                    resp.extend_from_slice(&1u16.to_be_bytes());
                    resp.extend_from_slice(&30u32.to_be_bytes());
                    resp.extend_from_slice(&16u16.to_be_bytes());
                    resp.extend_from_slice(&[0xfd, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
                }
                33 => {
                    resp.extend_from_slice(&33u16.to_be_bytes());
                    resp.extend_from_slice(&1u16.to_be_bytes());
                    resp.extend_from_slice(&30u32.to_be_bytes());
                    let target = [4u8, b'h', b'o', b's', b't', 0];
                    resp.extend_from_slice(&((6 + target.len()) as u16).to_be_bytes());
                    resp.extend_from_slice(&(rng.gen_range(0, 100) as u16).to_be_bytes());
                    resp.extend_from_slice(&(rng.gen_range(0, 100) as u16).to_be_bytes());
                    resp.extend_from_slice(&7946u16.to_be_bytes());
                    resp.extend_from_slice(&target);
                }
                _ => {
                    resp.extend_from_slice(&1u16.to_be_bytes());
                    resp.extend_from_slice(&1u16.to_be_bytes());
                    resp.extend_from_slice(&30u32.to_be_bytes());
                    resp.extend_from_slice(&4u16.to_be_bytes());
                    resp.extend_from_slice(&[127, 0, 0, 2]);
                }
            }
        }
        _ => {
            let len = rng.gen_range(1, 96);
            let mut raw = rng.gen_bytes(len);
            if raw.len() >= 2 {
                raw[0] = txid[0];
                raw[1] = txid[1];
            }
            resp = raw;
        }
    }
    resp
}

#[test]
fn fuzz_03_dns_godns_wire_packets() {
    block_on_for_tests(async {
        let sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let port = sock.local_addr().unwrap().port();
        let stop = Arc::new(AtomicBool::new(false));
        let packets_served = Arc::new(AtomicUsize::new(0));

        let sock_srv = Arc::clone(&sock);
        let stop_srv = Arc::clone(&stop);
        let served_srv = Arc::clone(&packets_served);
        let srv_handle = tokio::spawn(async move {
            let mut rng = Rng::new(0x0370_3d15_c0de_0003);
            let mut buf = [0u8; 2048];
            while !stop_srv.load(Ordering::Relaxed) {
                let recv = tokio::time::timeout(
                    Duration::from_millis(50),
                    sock_srv.recv_from(&mut buf),
                )
                .await;
                if let Ok(Ok((n, peer))) = recv {
                    let iter = served_srv.fetch_add(1, Ordering::Relaxed);
                    let reply = craft_mutated_dns_packet(&mut rng, &buf[..n], iter);
                    let _ = sock_srv.send_to(&reply, peer).await;
                }
            }
        });

        let ns = NameServerConfigGroup::from_ips_clear(
            &[IpAddr::V4(Ipv4Addr::LOCALHOST)],
            port,
            true,
        );
        let mut builder = TokioResolver::builder_with_config(
            ResolverConfig::from_parts(None, vec![], ns),
            TokioConnectionProvider::default(),
        );
        builder.options_mut().use_hosts_file = ResolveHosts::Never;
        builder.options_mut().cache_size = 0;
        builder.options_mut().attempts = 1;
        builder.options_mut().timeout = Duration::from_millis(10);
        builder.options_mut().ndots = 1;
        let resolver = Arc::new(GoDnsResolver::with_resolver(builder.build()));

        let mut handles = Vec::new();
        for tid in 0..16usize {
            let r = Arc::clone(&resolver);
            handles.push(tokio::spawn(async move {
                let mut trng = Rng::new(0x3703_7a5e_0000_0000 + tid as u64);
                for j in 0..380usize {
                    if j % 3 == 0 {
                        let host = format!("svc-{tid}-{}.cluster.local.", j % 32);
                        let _ = r.lookup_srv("grpc", "tcp", &host).await;
                    } else if j % 20 == 1 {
                        let owned = trng.gen_ascii_str(0, 64);
                        let bad_hosts = [
                            "",
                            ".",
                            "..",
                            "127.0.0.1",
                            "::1",
                            "fe80::1%eth0",
                            "invalid..host.",
                            owned.as_str(),
                        ];
                        let h = bad_hosts[trng.gen_range(0, bad_hosts.len())];
                        let _ = r.lookup_ip_addr(h).await;
                    } else {
                        let host = format!("pod-{tid}-{}.svc.cluster.local.", j % 32);
                        let _ = r.lookup_ip_addr(&host).await;
                    }
                }
            }));
        }

        for h in handles {
            h.await.expect("godns worker task panicked");
        }
        stop.store(true, Ordering::Relaxed);
        let _ = srv_handle.await;

        let udp_count = packets_served.load(Ordering::Relaxed);

        let mut mrng = Rng::new(0x3703_b1a5_cafe_0003);
        let dummy_req = [
            0x12, 0x34, 0x01, 0x00, 0, 1, 0, 0, 0, 0, 0, 0, 3, b'f', b'o', b'o', 0, 0, 1, 0, 1,
        ];
        for i in 0..3_000usize {
            let pkt = craft_mutated_dns_packet(&mut mrng, &dummy_req, i);
            let _ = DnsMessage::from_bytes(&pkt);
            let mutated = mrng.mutate_bytes(&pkt, 256);
            let _ = DnsMessage::from_bytes(&mutated);
        }

        assert!(
            udp_count >= 7_500,
            "expected >= 7,500 UDP DNS packets served through godns, got {udp_count}"
        );
        assert!(udp_count + 3_000 >= 10_500);
    });
}

// =================================================================================================
// 4. CLI / Env / Durations / Bytes / CIDRs / HTTP Query / X-Scope-OrgID / uber-trace-id / httpgrpc
//    (24,000+ mutated inputs)
// =================================================================================================

#[test]
fn fuzz_04_cli_env_durations_bytes_cidr_http_tenant_trace_httpgrpc() {
    let mut rng = Rng::new(0x3704_c0de_f00d_0004);
    let tmp = tempfile::tempdir().unwrap();
    let empty_cfg = tmp.path().join("empty.yaml");
    std::fs::write(&empty_cfg, "target: ingester\n").unwrap();
    let empty_cfg_str = empty_cfg.to_string_lossy().into_owned();

    let reg = Registry::new();
    let mut cfg = MimirConfig::default();
    cfg.blocks_storage.tsdb.dir = tmp.path().join("tsdb").to_string_lossy().into_owned();
    let state = AppHttpState {
        config: Arc::new(cfg.clone()),
        default_config: Arc::new(MimirConfig::default()),
        registry: reg,
        shutdown_requested: Arc::new(AtomicBool::new(false)),
        ingester: None,
        runtime_config: None,
        overrides: Overrides::new(cfg.limits),
        memberlist_kv: None,
        modules: ModuleStates::new(),
        cost_attribution_registry: None,
    };
    let router = build_http_router(state);
    let bridge = HttpGrpcBridge::new(router.clone());

    let mut env_map: HashMap<String, String> = HashMap::new();
    env_map.insert("VAR".into(), "hello\nworld".into());
    env_map.insert("EMPTY".into(), "".into());
    env_map.insert("NEST".into(), "${VAR}".into());
    env_map.insert("PORT".into(), "9095".into());

    let mut total_iters = 0usize;

    block_on_for_tests(async {
        for iter in 0..10_000usize {
            total_iters += 1;

            let env_inputs: [&[u8]; 10] = [
                b"${VAR}",
                b"${VAR:-def}",
                b"${VAR:def}",
                b"${EMPTY:-def}",
                b"${EMPTY:def}",
                b"${UNSET:${PORT:8080}}",
                b"${NEST:${VAR:fallback}}",
                b"${}",
                b"$VAR $$ ${UNCLOSED",
                &[0xff, b'$', b'{', b'V', b'A', b'R', b':', 0xfe, b'}'],
            ];
            let base_env = env_inputs[iter % env_inputs.len()];
            let mut env_mut = rng.mutate_bytes(base_env, 128);
            if iter % 7 == 0 {
                env_mut.extend_from_slice(rng.gen_ascii_str(0, 32).as_bytes());
            }
            let _ = expand_environment_variables_with(&env_mut, |k| env_map.get(k).cloned());

            let dur_seeds = [
                "0", "-0", "+0", "5s", "1h30m45.125s", "-100ms", "1y2w3d", "999999h",
                "1.5µs", "1.5μs", "2562047h", "-2562047h", "invalid", "10", ".", "-.",
            ];
            let d_seed = dur_seeds[iter % dur_seeds.len()];
            let d_mut = if iter % 3 == 0 {
                String::from_utf8_lossy(&rng.mutate_bytes(d_seed.as_bytes(), 64)).into_owned()
            } else {
                rng.gen_ascii_str(0, 24)
            };
            let _ = parse_go_duration(&d_mut);
            let _ = parse_model_duration(&d_mut);
            let _ = parse_duration(&d_mut);
            let _ = format_go_duration(rng.next_u64() as i64);

            let byte_seeds = [
                "0", "0B", "64KiB", "1.5MiB", "10MB", "1024GiB", "-1B",
                "9223372036854775807B", "9223372036854775808B", "100EiB", "NaN", "bad",
            ];
            let b_seed = byte_seeds[iter % byte_seeds.len()];
            let b_mut = String::from_utf8_lossy(&rng.mutate_bytes(b_seed.as_bytes(), 48)).into_owned();
            let _ = HumanBytes::parse_str(&b_mut);
            let _ = HumanBytes::parse(&b_mut);
            let _ = HumanBytes(rng.next_u64()).to_string();

            let cidr_seeds = [
                "10.0.0.0/8", "192.168.1.5/24", "0.0.0.0/0", "::/0",
                "fe80::1/64", "10.0.0.1/33", "::1/129", "10.0.0.1/-1", "bad", "1.2.3.4/",
            ];
            let c_seed = cidr_seeds[iter % cidr_seeds.len()];
            let c_mut = String::from_utf8_lossy(&rng.mutate_bytes(c_seed.as_bytes(), 48)).into_owned();
            let _ = CIDRSliceCSV::parse_cidr(&c_mut);
            let mut csv = CIDRSliceCSV::default();
            let _ = csv.set(&format!("{c_mut},10.0.0.0/8"));
            let _ = csv.set_yaml(&c_mut);

            let q_seeds = [
                "mode=diff&tokens=true",
                "name[]=up&name[]=http_requests_total",
                "a=1;b=2&c=%ZZ&d=%4",
                "tenant=&t=t%281%29+x&flag",
                "a=b&c=d",
            ];
            let q_seed = q_seeds[iter % q_seeds.len()];
            let q_mut = String::from_utf8_lossy(&rng.mutate_bytes(q_seed.as_bytes(), 128)).into_owned();
            let mut qmap = HashMap::new();
            let _ = go_parse_query(&mut qmap, &q_mut);
            let _ = go_query_unescape(&q_mut);
            let _ = go_parse_media_type(&q_mut);

            let org_seeds: [&[u8]; 7] = [
                b"tenant-a",
                b"tenant-a|tenant-b",
                b"tenant-a:k1=v1:k2=v2|tenant-b:a=1",
                b"..",
                b".",
                b"bad/tenant",
                &[0xff, b'a', b'|', b'b', 0x00],
            ];
            let o_mut = rng.mutate_bytes(org_seeds[iter % org_seeds.len()], 200);
            let _ = valid_tenant_id_bytes(&o_mut);
            let _ = valid_metadata_bytes(&o_mut);
            let _ = parse_tenant_ids_bytes(&o_mut);
            let _ = parse_single_tenant_id_bytes(&o_mut);
            let _ = parse_with_metadata_bytes(&o_mut);
            let _ = TenantMetadata::parse_bytes(&o_mut);
            if let Ok(hv) = HeaderValue::from_bytes(&o_mut) {
                let mut hm = HeaderMap::new();
                hm.insert(HeaderName::from_static("x-scope-orgid"), hv);
                let _ = extract_org_id_from_http_headers(&hm);
            }
            if let Ok(s) = std::str::from_utf8(&o_mut) {
                if let Ok(mv) = MetadataValue::try_from(s) {
                    let mut md = tonic::metadata::MetadataMap::new();
                    md.insert(LOWER_ORG_ID_HEADER_NAME, mv);
                    let _ = extract_org_id_from_grpc_metadata(&md);
                }
            }

            let trace_seeds = [
                "48475050943e8e05:70e8b02d28e4337b:0:1",
                "0123456789abcdef0123456789abcdef:70e8b02d28e4337b:0:3",
                "0:70e8b02d28e4337b:0:1",
                "zzzz:1111:0:-1",
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            ];
            let t_seed = trace_seeds[iter % trace_seeds.len()];
            let t_mut = String::from_utf8_lossy(&rng.mutate_bytes(t_seed.as_bytes(), 96)).into_owned();
            let _ = parse_uber_trace_id(&t_mut);
            let _ = parse_traceparent(&t_mut);
            let hdrs = [("uber-trace-id", t_mut.as_str()), ("traceparent", t_mut.as_str())];
            let _ = extract_from_headers(hdrs.iter().copied(), &PropagatorSet::default());

            let hg_req_seed = HttpRequest {
                method: "GET".into(),
                url: format!("/metrics?{q_mut}"),
                headers: vec![
                    HttpGrpcHeader {
                        key: "X-Scope-OrgID".into(),
                        values: vec![String::from_utf8_lossy(&o_mut).into_owned()],
                    },
                    HttpGrpcHeader {
                        key: "Accept".into(),
                        values: vec![q_mut.clone()],
                    },
                ],
                body: rng.gen_bytes(16).into(),
            };
            let raw_hg = rng.mutate_bytes(&hg_req_seed.encode_to_vec(), 256);
            let _ = HttpRequest::decode(raw_hg.as_slice());
            let _ = HttpResponse::decode(raw_hg.as_slice());
            let _ = parse_httpgrpc_url(&q_mut);
            let mut out_hm = HeaderMap::new();
            let _ = httpgrpc_to_headers(&hg_req_seed.headers, &mut out_hm);
            let resp_err = httpgrpc_error_from_response(&HttpResponse {
                code: (rng.next_u32() % 600) as i32,
                headers: hg_req_seed.headers.clone(),
                body: raw_hg.into(),
            });
            let _ = httpgrpc_response_from_error(&resp_err);

            if iter % 20 == 0 {
                let flag_mut = rng.gen_ascii_str(0, 32);
                let cli_cases: Vec<Vec<String>> = vec![
                    vec![format!("-target={flag_mut}")],
                    vec![format!("--{flag_mut}=1")],
                    vec![
                        format!("-config.file={empty_cfg_str}"),
                        format!("-server.http-listen-port={flag_mut}"),
                    ],
                    vec!["-config.expand-env".into(), format!("-{flag_mut}")],
                ];
                let args = &cli_cases[(iter / 20) % cli_cases.len()];
                let _ = parse_config_file_parameter(args);
                let _ = parse_cli_for_run("mimir", args);
                if iter % 200 == 0 {
                    let _ = run_cli_test_mode("mimir", &["-target=ingester".into()]);
                }
            }

            if iter % 25 == 0 {
                let safe_q: String = q_mut
                    .chars()
                    .filter(|c| c.is_ascii_graphic() && *c != '#')
                    .take(64)
                    .collect();
                let uri = format!("/metrics?{safe_q}");
                if let Ok(req) = AxumRequest::builder()
                    .method(Method::GET)
                    .uri(&uri)
                    .body(Body::empty())
                {
                    let _ = router.clone().oneshot(req).await;
                    let _ = parse_go_form(
                        AxumRequest::builder()
                            .method(Method::GET)
                            .uri(&uri)
                            .body(Body::empty())
                            .unwrap(),
                    )
                    .await;
                }
                let _ = bridge
                    .handle(tonic::Request::new(HttpRequest {
                        method: "GET".into(),
                        url: format!("/config?{safe_q}"),
                        headers: vec![HttpGrpcHeader {
                            key: "Accept".into(),
                            values: vec![q_mut.clone()],
                        }],
                        body: Bytes::new(),
                    }))
                    .await;
            }
        }
    });

    assert!(total_iters >= 10_000);
}

// =================================================================================================
// 5. Multi-threaded mimir-ingester 6-Task Concurrency Chaos Fuzz
// =================================================================================================

#[test]
fn fuzz_05_ingester_concurrency_chaos() {
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().unwrap();
        let reg = Arc::new(Registry::new());
        let cost_reg = Arc::new(Registry::new());

        let mut limits = LimitsConfig {
            out_of_order_time_window: ModelDuration::from_hours(1),
            max_global_exemplars_per_user: 5_000,
            native_histograms_ingestion_enabled: true,
            max_cost_attribution_cardinality: 100,
            ..Default::default()
        };
        limits.cost_attribution_trackers.insert(
            "by-job".into(),
            mimir_config::CostAttributionTrackerConfig {
                labels: vec![mimir_config::CostAttributionLabel {
                    input: "job".into(),
                    output: "job".into(),
                }],
                internal: false,
            },
        );
        let overrides = Overrides::new(limits);

        let cost_mgr = CostAttributionManager::new(
            Duration::from_secs(60),
            overrides.clone(),
            Some(&reg),
            &cost_reg,
        )
        .unwrap();

        let mut cfg = IngesterConfig::default();
        cfg.active_series.active_series_metrics_enabled = true;
        cfg.ring.instance_id = "ingester-chaos-0".into();

        let mut blocks_cfg = BlocksStorageConfig::default();
        blocks_cfg.tsdb.dir = tmp.path().join("tsdb").to_string_lossy().into_owned();
        blocks_cfg.tsdb.head_compaction_interval = GoDuration::from_secs(60);
        blocks_cfg.tsdb.head_compaction_idle_timeout = GoDuration::from_hours(1);
        blocks_cfg.tsdb.ship_interval = GoDuration::ZERO;

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
        ingester.set_cost_attribution_manager(Some(Arc::clone(&cost_mgr)));
        ingester.start().await.unwrap();

        let grpc_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let grpc_addr = grpc_listener.local_addr().unwrap();
        let grpc_incoming = tokio_stream::wrappers::TcpListenerStream::new(grpc_listener);
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let svc = IngesterService::new(Arc::clone(&ingester)).into_server();
        tokio::spawn(async move {
            let _ = Server::builder()
                .add_service(svc)
                .serve_with_incoming_shutdown(grpc_incoming, async {
                    let _ = shutdown_rx.await;
                })
                .await;
        });

        let endpoint = format!("http://{grpc_addr}");
        let base_ts = now_ms() - 60_000;
        let tenants = ["chaos-t0", "chaos-t1", "chaos-t2", "chaos-t3"];

        let push_ok_count = Arc::new(AtomicUsize::new(0));
        let query_ok_count = Arc::new(AtomicUsize::new(0));

        let chaos_fut = async {
            let mut tasks = Vec::new();

            for writer_id in 0..2usize {
                let ep = endpoint.clone();
                let pok = Arc::clone(&push_ok_count);
                tasks.push(tokio::spawn(async move {
                    let mut client = IngesterClient::connect(ep).await.unwrap();
                    let mut rng = Rng::new(0x3705_a000 + writer_id as u64);
                    for step in 0..160usize {
                        let tenant = tenants[rng.gen_range(0, tenants.len())];
                        let job = format!("job-{}", rng.gen_range(0, 5));
                        let inst = format!("inst-{writer_id}-{}", step % 12);
                        let ts = base_ts + (step as i64) * 500;
                        let mut ts_list = Vec::new();

                        let mut samples = vec![cortexpb::Sample {
                            value: (step as f64) + 1.0,
                            timestamp_ms: ts,
                        }];
                        if step % 3 == 0 {
                            samples.push(cortexpb::Sample {
                                value: (step as f64) + 0.5,
                                timestamp_ms: ts - 2_000,
                            });
                        }
                        ts_list.push(cortexpb::TimeSeries {
                            labels: vec![
                                cortexpb::LabelPair::from_static("__name__", "chaos_requests_total"),
                                cortexpb::LabelPair::from_strs("instance", inst.clone()),
                                cortexpb::LabelPair::from_strs("job", job.clone()),
                            ],
                            samples,
                            exemplars: if step % 4 == 0 {
                                vec![cortexpb::Exemplar {
                                    labels: vec![cortexpb::LabelPair::from_strs(
                                        "trace_id",
                                        format!("tr-{writer_id}-{step}"),
                                    )],
                                    value: 1.0,
                                    timestamp_ms: ts,
                                }]
                            } else {
                                vec![]
                            },
                            ..Default::default()
                        });

                        match step % 3 {
                            0 => ts_list.push(cortexpb::TimeSeries {
                                labels: vec![
                                    cortexpb::LabelPair::from_static("__name__", "chaos_hist_int"),
                                    cortexpb::LabelPair::from_strs("job", job.clone()),
                                ],
                                histograms: vec![cortexpb::Histogram {
                                    count: Some(cortexpb::histogram::Count::CountInt(5)),
                                    sum: 2.5,
                                    schema: 0,
                                    zero_threshold: 0.001,
                                    zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountInt(1)),
                                    positive_spans: vec![cortexpb::BucketSpan {
                                        offset: 0,
                                        length: 2,
                                    }],
                                    positive_deltas: vec![2, 0],
                                    timestamp: ts,
                                    ..Default::default()
                                }],
                                ..Default::default()
                            }),
                            1 => ts_list.push(cortexpb::TimeSeries {
                                labels: vec![
                                    cortexpb::LabelPair::from_static("__name__", "chaos_hist_float"),
                                    cortexpb::LabelPair::from_strs("job", job.clone()),
                                ],
                                histograms: vec![cortexpb::Histogram {
                                    count: Some(cortexpb::histogram::Count::CountFloat(4.5)),
                                    sum: 1.5,
                                    schema: 1,
                                    zero_threshold: 0.001,
                                    zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountFloat(0.5)),
                                    positive_spans: vec![cortexpb::BucketSpan {
                                        offset: 0,
                                        length: 2,
                                    }],
                                    positive_counts: vec![2.0, 2.0],
                                    timestamp: ts,
                                    ..Default::default()
                                }],
                                ..Default::default()
                            }),
                            _ => ts_list.push(cortexpb::TimeSeries {
                                labels: vec![
                                    cortexpb::LabelPair::from_static("__name__", "chaos_hist_nhcb"),
                                    cortexpb::LabelPair::from_strs("job", job.clone()),
                                ],
                                histograms: vec![cortexpb::Histogram {
                                    count: Some(cortexpb::histogram::Count::CountInt(3)),
                                    sum: 1.2,
                                    schema: -53,
                                    zero_threshold: 0.0,
                                    zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountInt(0)),
                                    positive_spans: vec![cortexpb::BucketSpan {
                                        offset: 0,
                                        length: 2,
                                    }],
                                    positive_deltas: vec![2, -1],
                                    custom_values: vec![0.1, 0.5, 1.0],
                                    timestamp: ts,
                                    ..Default::default()
                                }],
                                ..Default::default()
                            }),
                        }

                        if step % 15 == 14 {
                            ts_list.push(cortexpb::TimeSeries {
                                labels: vec![
                                    cortexpb::LabelPair::from_static("z", "1"),
                                    cortexpb::LabelPair::from_static("__name__", "bad_unsorted"),
                                ],
                                samples: vec![cortexpb::Sample {
                                    value: 1.0,
                                    timestamp_ms: ts,
                                }],
                                ..Default::default()
                            });
                        }

                        let wr = cortexpb::WriteRequest {
                            source: cortexpb::write_request::SourceEnum::Api as i32,
                            metadata: if step % 10 == 0 {
                                vec![cortexpb::MetricMetadata {
                                    r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
                                    metric_family_name: "chaos_requests_total".into(),
                                    help: "chaos".into(),
                                    unit: "1".into(),
                                }]
                            } else {
                                vec![]
                            },
                            timeseries: ts_list,
                            ..Default::default()
                        };

                        let mut req = tonic::Request::new(wr);
                        req.metadata_mut().insert(
                            LOWER_ORG_ID_HEADER_NAME,
                            MetadataValue::try_from(tenant).unwrap(),
                        );
                        if client.push(req).await.is_ok() {
                            pok.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }));
            }

            {
                let ep = endpoint.clone();
                let qok = Arc::clone(&query_ok_count);
                tasks.push(tokio::spawn(async move {
                    let mut client = IngesterClient::connect(ep).await.unwrap();
                    let mut rng = Rng::new(0x3705_b002);
                    for step in 0..120usize {
                        let tenant = tenants[rng.gen_range(0, tenants.len())];
                        let mut req = tonic::Request::new(ingesterpb::QueryRequest {
                            start_timestamp_ms: base_ts - 10_000,
                            end_timestamp_ms: base_ts + 200_000,
                            matchers: vec![ingesterpb::LabelMatcher {
                                r#type: ingesterpb::MatchType::RegexMatch as i32,
                                name: "__name__".into(),
                                value: "chaos_.*".into(),
                            }],
                            streaming_chunks_batch_size: 4,
                        });
                        req.metadata_mut().insert(
                            LOWER_ORG_ID_HEADER_NAME,
                            MetadataValue::try_from(tenant).unwrap(),
                        );
                        if let Ok(resp) = client.query_stream(req).await {
                            let mut stream = resp.into_inner();
                            while let Some(item) = stream.next().await {
                                if item.is_err() {
                                    break;
                                }
                            }
                            qok.fetch_add(1, Ordering::Relaxed);
                        }
                        if step % 3 == 0 {
                            let mut ex_req = tonic::Request::new(ingesterpb::ExemplarQueryRequest {
                                start_timestamp_ms: base_ts - 10_000,
                                end_timestamp_ms: base_ts + 200_000,
                                matchers: vec![],
                            });
                            ex_req.metadata_mut().insert(
                                LOWER_ORG_ID_HEADER_NAME,
                                MetadataValue::try_from(tenant).unwrap(),
                            );
                            let _ = client.query_exemplars(ex_req).await;
                        }
                    }
                }));
            }

            {
                let ep = endpoint.clone();
                tasks.push(tokio::spawn(async move {
                    let mut client = IngesterClient::connect(ep).await.unwrap();
                    let mut rng = Rng::new(0x3705_c003);
                    for _ in 0..120usize {
                        let tenant = tenants[rng.gen_range(0, tenants.len())];
                        let mut ln_req = tonic::Request::new(ingesterpb::LabelNamesRequest {
                            start_timestamp_ms: base_ts - 10_000,
                            end_timestamp_ms: base_ts + 200_000,
                            matchers: None,
                            limit: 0,
                        });
                        ln_req.metadata_mut().insert(
                            LOWER_ORG_ID_HEADER_NAME,
                            MetadataValue::try_from(tenant).unwrap(),
                        );
                        let _ = client.label_names(ln_req).await;

                        let mut lv_req = tonic::Request::new(ingesterpb::LabelValuesRequest {
                            label_name: "job".into(),
                            start_timestamp_ms: base_ts - 10_000,
                            end_timestamp_ms: base_ts + 200_000,
                            matchers: None,
                            limit: 0,
                        });
                        lv_req.metadata_mut().insert(
                            LOWER_ORG_ID_HEADER_NAME,
                            MetadataValue::try_from(tenant).unwrap(),
                        );
                        let _ = client.label_values(lv_req).await;

                        let mut lvc_req = tonic::Request::new(ingesterpb::LabelValuesCardinalityRequest {
                            label_names: vec!["job".into()],
                            matchers: vec![],
                            count_method: ingesterpb::CountMethod::Active as i32,
                        });
                        lvc_req.metadata_mut().insert(
                            LOWER_ORG_ID_HEADER_NAME,
                            MetadataValue::try_from(tenant).unwrap(),
                        );
                        if let Ok(resp) = client.label_values_cardinality(lvc_req).await {
                            let mut s = resp.into_inner();
                            while let Some(Ok(_)) = s.next().await {}
                        }
                    }
                }));
            }

            {
                let ep = endpoint.clone();
                tasks.push(tokio::spawn(async move {
                    let mut client = IngesterClient::connect(ep).await.unwrap();
                    let mut rng = Rng::new(0x3705_d004);
                    for _ in 0..100usize {
                        let tenant = tenants[rng.gen_range(0, tenants.len())];
                        let mut as_req = tonic::Request::new(ingesterpb::ActiveSeriesRequest {
                            matchers: vec![],
                            r#type: ingesterpb::active_series_request::RequestType::Series as i32,
                        });
                        as_req.metadata_mut().insert(
                            LOWER_ORG_ID_HEADER_NAME,
                            MetadataValue::try_from(tenant).unwrap(),
                        );
                        if let Ok(resp) = client.active_series(as_req).await {
                            let mut s = resp.into_inner();
                            while let Some(Ok(_)) = s.next().await {}
                        }

                        let mut sln_req = tonic::Request::new(ingesterpb::SearchLabelNamesRequest {
                            start_timestamp_ms: base_ts - 10_000,
                            end_timestamp_ms: base_ts + 200_000,
                            matchers: vec![],
                            filter: Some(ingesterpb::SearchFilter {
                                terms: vec!["jo".into()],
                                case_insensitive: true,
                                fuzz_alg: ingesterpb::search_filter::FuzzAlg::Substring as i32,
                                fuzz_threshold: 0,
                            }),
                            ordering: ingesterpb::SearchOrdering::OrderByValueAsc as i32,
                            limit: 10,
                        });
                        sln_req.metadata_mut().insert(
                            LOWER_ORG_ID_HEADER_NAME,
                            MetadataValue::try_from(tenant).unwrap(),
                        );
                        if let Ok(resp) = client.search_label_names(sln_req).await {
                            let mut s = resp.into_inner();
                            while let Some(Ok(_)) = s.next().await {}
                        }

                        let mut slv_req = tonic::Request::new(ingesterpb::SearchLabelValuesRequest {
                            start_timestamp_ms: base_ts - 10_000,
                            end_timestamp_ms: base_ts + 200_000,
                            name: "__name__".into(),
                            matchers: vec![],
                            filter: Some(ingesterpb::SearchFilter {
                                terms: vec!["chaos".into()],
                                case_insensitive: true,
                                fuzz_alg: ingesterpb::search_filter::FuzzAlg::Substring as i32,
                                fuzz_threshold: 0,
                            }),
                            ordering: ingesterpb::SearchOrdering::OrderByValueAsc as i32,
                            limit: 10,
                        });
                        slv_req.metadata_mut().insert(
                            LOWER_ORG_ID_HEADER_NAME,
                            MetadataValue::try_from(tenant).unwrap(),
                        );
                        if let Ok(resp) = client.search_label_values(slv_req).await {
                            let mut s = resp.into_inner();
                            while let Some(Ok(_)) = s.next().await {}
                        }
                    }
                }));
            }

            {
                let ing = Arc::clone(&ingester);
                let cm = Arc::clone(&cost_mgr);
                let r = Arc::clone(&reg);
                let cr = Arc::clone(&cost_reg);
                tasks.push(tokio::spawn(async move {
                    for step in 0..25usize {
                        tokio::time::sleep(Duration::from_millis(15)).await;
                        ing.update_active_series_metrics(SystemTime::now());
                        for t in tenants {
                            let _ = cm.active_series_tracker(t);
                            let _ = cm.sample_tracker(t);
                        }
                        if step % 5 == 4 {
                            ing.flush().await;
                            cm.purge_inactive_attributions_until(
                                SystemTime::now() + Duration::from_secs(120),
                            );
                        }
                        let _ = r.gather();
                        let _ = cr.gather();
                    }
                }));
            }

            for (idx, t) in tasks.into_iter().enumerate() {
                t.await.unwrap_or_else(|e| panic!("chaos task {idx} failed/panicked: {e}"));
            }
        };

        tokio::time::timeout(Duration::from_secs(20), chaos_fut)
            .await
            .expect("ingester concurrency chaos test timed out (potential deadlock!)");

        let _ = shutdown_tx.send(());
        ingester.stop().await.unwrap();

        assert!(
            push_ok_count.load(Ordering::Relaxed) > 0,
            "expected successful Push calls during chaos fuzz"
        );
        assert!(
            query_ok_count.load(Ordering::Relaxed) > 0,
            "expected successful QueryStream calls during chaos fuzz"
        );

        let families = reg.gather();
        let mut saw_ingested_samples = false;
        let mut saw_queries_total = false;
        for mf in &families {
            if mf.get_name() == "cortex_ingester_ingested_samples_total" {
                let sum: f64 = mf.get_metric().iter().map(|m| m.get_counter().get_value()).sum();
                assert!(sum > 0.0, "expected ingested samples > 0, got {sum}");
                saw_ingested_samples = true;
            }
            if mf.get_name() == "cortex_ingester_queries_total" {
                let sum: f64 = mf.get_metric().iter().map(|m| m.get_counter().get_value()).sum();
                assert!(sum > 0.0, "expected queries_total > 0, got {sum}");
                saw_queries_total = true;
            }
        }
        assert!(saw_ingested_samples && saw_queries_total);
        let cost_families = cost_reg.gather();
        assert!(!cost_families.is_empty(), "expected cost attribution metrics");
    });
}

// =================================================================================================
// 6. Live Twin-Process Go vs Rust Ingester Differential Fuzz (all 13 read RPCs + ErrorDetails)
// =================================================================================================

struct TwinProc {
    name: String,
    child: Option<Child>,
}

impl TwinProc {
    fn spawn(name: &str, bin: &Path, cfg: &Path, dir: &Path) -> Self {
        let child = Command::new(bin)
            .args([
                "-target=ingester".to_string(),
                format!("-config.file={}", cfg.display()),
            ])
            .current_dir(dir)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap_or_else(|e| panic!("spawn {name} ({}): {e}", bin.display()));
        Self {
            name: name.to_string(),
            child: Some(child),
        }
    }
}

impl Drop for TwinProc {
    fn drop(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

async fn wait_ready_proc(http: &reqwest::Client, port: u16, proc: &mut TwinProc) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Some(c) = proc.child.as_mut() {
            if let Ok(Some(st)) = c.try_wait() {
                panic!("{} exited prematurely with {st}", proc.name);
            }
        }
        if let Ok(r) = http.get(format!("http://127.0.0.1:{port}/ready")).send().await {
            if r.status().as_u16() == 200 {
                return;
            }
        }
        assert!(Instant::now() < deadline, "{} not ready within 20s", proc.name);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn with_tenant<T>(tenant: &str, msg: T) -> tonic::Request<T> {
    let mut req = tonic::Request::new(msg);
    req.metadata_mut()
        .insert(LOWER_ORG_ID_HEADER_NAME, MetadataValue::try_from(tenant).unwrap());
    req
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

#[derive(Debug, Clone, PartialEq)]
enum DiffVal {
    Float(i64, f64),
    IntHist(i64, i32, u64, f64, Vec<f64>),
    FloatHist(i64, i32, f64, f64, Vec<f64>),
}

#[derive(Debug, Clone, PartialEq)]
struct DiffSeries {
    labels: Vec<(String, String)>,
    values: Vec<DiffVal>,
}

fn canon_labels(labels: Vec<cortexpb::LabelPair>) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = labels
        .into_iter()
        .map(|l| {
            (
                String::from_utf8_lossy(&l.name).into_owned(),
                String::from_utf8_lossy(&l.value).into_owned(),
            )
        })
        .collect();
    out.sort();
    out
}

type LabelList = Vec<(String, String)>;
type ExemplarItem = (i64, f64, LabelList);

#[derive(Debug, PartialEq)]
struct All13Snapshot {
    query_stream: Vec<DiffSeries>,
    exemplars: Vec<(LabelList, Vec<ExemplarItem>)>,
    label_values: Vec<String>,
    label_names: Vec<String>,
    user_stats_mem: u64,
    all_user_stats: Vec<(String, u64)>,
    metrics_for_label_matchers: Vec<Vec<(String, String)>>,
    metrics_metadata: Vec<(i32, String, String, String)>,
    label_names_and_values: BTreeMap<String, Vec<String>>,
    label_values_cardinality: BTreeMap<String, u64>,
    active_series: Vec<Vec<(String, String)>>,
    active_nh_series: Vec<(Vec<(String, String)>, u64)>,
    search_label_names: Vec<String>,
    search_label_values: Vec<String>,
}

async fn snapshot_all_13_rpcs(
    client: &mut IngesterClient<Channel>,
    tenant: &str,
    start_ms: i64,
    end_ms: i64,
) -> All13Snapshot {
    let all_matchers = vec![ingesterpb::LabelMatcher {
        r#type: ingesterpb::MatchType::RegexMatch as i32,
        name: "__name__".into(),
        value: ".+".into(),
    }];
    let mut qs_stream = client
        .query_stream(with_tenant(
            tenant,
            ingesterpb::QueryRequest {
                start_timestamp_ms: start_ms,
                end_timestamp_ms: end_ms,
                matchers: all_matchers.clone(),
                streaming_chunks_batch_size: 4,
            },
        ))
        .await
        .unwrap()
        .into_inner();
    let mut series_labels = Vec::new();
    let mut saw_end = false;
    let mut chunks_by_idx: BTreeMap<u64, Vec<DiffVal>> = BTreeMap::new();
    while let Some(item) = qs_stream.next().await {
        let msg = item.unwrap();
        if !msg.streaming_series.is_empty() || msg.is_end_of_series_stream {
            assert!(!saw_end);
            for s in msg.streaming_series {
                series_labels.push(canon_labels(s.labels));
            }
            if msg.is_end_of_series_stream {
                saw_end = true;
            }
        } else {
            assert!(saw_end);
            for sc in msg.streaming_series_chunks {
                let entry = chunks_by_idx.entry(sc.series_index).or_default();
                for ch in sc.chunks {
                    let chunk =
                        EncodedChunk::from_bytes(wire_enc_to_tsdb(ch.encoding), ch.data.to_vec())
                            .unwrap();
                    for s in decode_chunk_samples(&chunk).unwrap() {
                        match s.value {
                            SampleValue::Float(v) => entry.push(DiffVal::Float(s.t, v)),
                            SampleValue::Histogram(h) => entry.push(DiffVal::IntHist(
                                s.t,
                                h.schema,
                                h.count,
                                h.sum,
                                h.custom_values,
                            )),
                            SampleValue::FloatHistogram(fh) => entry.push(DiffVal::FloatHist(
                                s.t,
                                fh.schema,
                                fh.count,
                                fh.sum,
                                fh.custom_values,
                            )),
                        }
                    }
                }
            }
        }
    }
    let mut query_stream: Vec<DiffSeries> = series_labels
        .into_iter()
        .enumerate()
        .map(|(i, labels)| DiffSeries {
            labels,
            values: chunks_by_idx.remove(&(i as u64)).unwrap_or_default(),
        })
        .collect();
    query_stream.sort_by(|a, b| a.labels.cmp(&b.labels));

    let ex_resp = client
        .query_exemplars(with_tenant(
            tenant,
            ingesterpb::ExemplarQueryRequest {
                start_timestamp_ms: start_ms,
                end_timestamp_ms: end_ms,
                matchers: vec![ingesterpb::LabelMatchers {
                    matchers: vec![ingesterpb::LabelMatcher {
                        r#type: ingesterpb::MatchType::RegexMatch as i32,
                        name: "__name__".into(),
                        value: ".+".into(),
                    }],
                }],
            },
        ))
        .await
        .unwrap()
        .into_inner();
    let mut exemplars: Vec<_> = ex_resp
        .timeseries
        .into_iter()
        .map(|ts| {
            let mut exs: Vec<_> = ts
                .exemplars
                .into_iter()
                .map(|e| (e.timestamp_ms, e.value, canon_labels(e.labels)))
                .collect();
            exs.sort_by_key(|x| x.0);
            (canon_labels(ts.labels), exs)
        })
        .collect();
    exemplars.sort_by(|a, b| a.0.cmp(&b.0));

    let mut label_values = client
        .label_values(with_tenant(
            tenant,
            ingesterpb::LabelValuesRequest {
                label_name: "job".into(),
                start_timestamp_ms: start_ms,
                end_timestamp_ms: end_ms,
                matchers: None,
                limit: 0,
            },
        ))
        .await
        .unwrap()
        .into_inner()
        .label_values;
    label_values.sort();

    let mut label_names = client
        .label_names(with_tenant(
            tenant,
            ingesterpb::LabelNamesRequest {
                start_timestamp_ms: start_ms,
                end_timestamp_ms: end_ms,
                matchers: None,
                limit: 0,
            },
        ))
        .await
        .unwrap()
        .into_inner()
        .label_names;
    label_names.sort();

    let user_stats_mem = client
        .user_stats(with_tenant(
            tenant,
            ingesterpb::UserStatsRequest {
                count_method: ingesterpb::CountMethod::InMemory as i32,
            },
        ))
        .await
        .unwrap()
        .into_inner()
        .num_series;

    let all_us = client
        .all_user_stats(with_tenant(
            tenant,
            ingesterpb::UserStatsRequest {
                count_method: ingesterpb::CountMethod::InMemory as i32,
            },
        ))
        .await
        .unwrap()
        .into_inner();
    let mut all_user_stats: Vec<_> = all_us
        .stats
        .into_iter()
        .map(|s| (s.user_id, s.data.map_or(0, |d| d.num_series)))
        .collect();
    all_user_stats.sort();

    let mflm = client
        .metrics_for_label_matchers(with_tenant(
            tenant,
            ingesterpb::MetricsForLabelMatchersRequest {
                start_timestamp_ms: start_ms,
                end_timestamp_ms: end_ms,
                matchers_set: vec![ingesterpb::LabelMatchers {
                    matchers: vec![ingesterpb::LabelMatcher {
                        r#type: ingesterpb::MatchType::RegexMatch as i32,
                        name: "job".into(),
                        value: ".+".into(),
                    }],
                }],
                limit: 0,
            },
        ))
        .await
        .unwrap()
        .into_inner();
    let mut metrics_for_label_matchers: Vec<_> = mflm
        .metric
        .into_iter()
        .map(|m| canon_labels(m.labels))
        .collect();
    metrics_for_label_matchers.sort();

    let mm = client
        .metrics_metadata(with_tenant(
            tenant,
            ingesterpb::MetricsMetadataRequest {
                limit: -1,
                limit_per_metric: -1,
                metric: String::new(),
                metric_names: vec![],
            },
        ))
        .await
        .unwrap()
        .into_inner();
    let mut metrics_metadata: Vec<_> = mm
        .metadata
        .into_iter()
        .map(|m| (m.r#type, m.metric_family_name, m.help, m.unit))
        .collect();
    metrics_metadata.sort();

    let mut lnv_stream = client
        .label_names_and_values(with_tenant(
            tenant,
            ingesterpb::LabelNamesAndValuesRequest {
                matchers: vec![],
                count_method: ingesterpb::CountMethod::Active as i32,
            },
        ))
        .await
        .unwrap()
        .into_inner();
    let mut label_names_and_values: BTreeMap<String, Vec<String>> = BTreeMap::new();
    while let Some(item) = lnv_stream.next().await {
        for lv in item.unwrap().items {
            label_names_and_values
                .entry(lv.label_name)
                .or_default()
                .extend(lv.values);
        }
    }
    for vals in label_names_and_values.values_mut() {
        vals.sort();
    }

    let mut lvc_stream = client
        .label_values_cardinality(with_tenant(
            tenant,
            ingesterpb::LabelValuesCardinalityRequest {
                label_names: vec!["job".into()],
                matchers: vec![],
                count_method: ingesterpb::CountMethod::Active as i32,
            },
        ))
        .await
        .unwrap()
        .into_inner();
    let mut label_values_cardinality = BTreeMap::new();
    while let Some(item) = lvc_stream.next().await {
        for it in item.unwrap().items {
            if it.label_name == "job" {
                for (k, v) in it.label_value_series {
                    label_values_cardinality.insert(k, v);
                }
            }
        }
    }

    let mut as_stream = client
        .active_series(with_tenant(
            tenant,
            ingesterpb::ActiveSeriesRequest {
                matchers: all_matchers.clone(),
                r#type: ingesterpb::active_series_request::RequestType::Series as i32,
            },
        ))
        .await
        .unwrap()
        .into_inner();
    let mut active_series = Vec::new();
    while let Some(item) = as_stream.next().await {
        for m in item.unwrap().metric {
            active_series.push(canon_labels(m.labels));
        }
    }
    active_series.sort();

    let mut nh_stream = client
        .active_series(with_tenant(
            tenant,
            ingesterpb::ActiveSeriesRequest {
                matchers: all_matchers,
                r#type: ingesterpb::active_series_request::RequestType::NativeHistogramSeries
                    as i32,
            },
        ))
        .await
        .unwrap()
        .into_inner();
    let mut active_nh_series = Vec::new();
    while let Some(item) = nh_stream.next().await {
        let msg = item.unwrap();
        for (m, bc) in msg.metric.into_iter().zip(msg.bucket_count) {
            active_nh_series.push((canon_labels(m.labels), bc));
        }
    }
    active_nh_series.sort();

    let mut sln_stream = client
        .search_label_names(with_tenant(
            tenant,
            ingesterpb::SearchLabelNamesRequest {
                start_timestamp_ms: start_ms,
                end_timestamp_ms: end_ms,
                matchers: vec![],
                filter: Some(ingesterpb::SearchFilter {
                    terms: vec!["j".into()],
                    case_insensitive: true,
                    fuzz_alg: ingesterpb::search_filter::FuzzAlg::Substring as i32,
                    fuzz_threshold: 0,
                }),
                ordering: ingesterpb::SearchOrdering::OrderByValueAsc as i32,
                limit: 20,
            },
        ))
        .await
        .unwrap()
        .into_inner();
    let mut search_label_names = Vec::new();
    while let Some(b) = sln_stream.next().await {
        for r in b.unwrap().results {
            search_label_names.push(r.value);
        }
    }

    let mut slv_stream = client
        .search_label_values(with_tenant(
            tenant,
            ingesterpb::SearchLabelValuesRequest {
                start_timestamp_ms: start_ms,
                end_timestamp_ms: end_ms,
                name: "__name__".into(),
                matchers: vec![],
                filter: Some(ingesterpb::SearchFilter {
                    terms: vec!["fuzz".into()],
                    case_insensitive: true,
                    fuzz_alg: ingesterpb::search_filter::FuzzAlg::Substring as i32,
                    fuzz_threshold: 0,
                }),
                ordering: ingesterpb::SearchOrdering::OrderByValueAsc as i32,
                limit: 20,
            },
        ))
        .await
        .unwrap()
        .into_inner();
    let mut search_label_values = Vec::new();
    while let Some(b) = slv_stream.next().await {
        for r in b.unwrap().results {
            search_label_values.push(r.value);
        }
    }

    All13Snapshot {
        query_stream,
        exemplars,
        label_values,
        label_names,
        user_stats_mem,
        all_user_stats,
        metrics_for_label_matchers,
        metrics_metadata,
        label_names_and_values,
        label_values_cardinality,
        active_series,
        active_nh_series,
        search_label_names,
        search_label_values,
    }
}

fn write_twin_fuzz_yaml(
    path: &Path,
    http_port: u16,
    grpc_port: u16,
    tsdb_dir: &Path,
    bucket_dir: &Path,
) {
    let yaml = format!(
        r#"target: ingester
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http_port}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc_port}
  register_instrumentation: false
multitenancy_enabled: true
ingester:
  ring:
    kvstore:
      store: inmemory
    instance_id: ingester-0
    instance_addr: 127.0.0.1
    min_ready_duration: 0s
    replication_factor: 1
    unregister_on_shutdown: true
  active_series_metrics_enabled: true
limits:
  out_of_order_time_window: 5m
  past_grace_period: 5m
  creation_grace_period: 5m
  max_label_names_per_series: 10
  max_label_value_length: 128
  native_histograms_ingestion_enabled: true
  max_global_exemplars_per_user: 1000
  max_global_series_per_user: 100
blocks_storage:
  backend: filesystem
  filesystem:
    dir: {bucket}
  tsdb:
    dir: {tsdb}
    ship_interval: 10m
    head_compaction_interval: 10m
activity_tracker:
  filepath: ""
"#,
        bucket = bucket_dir.display(),
        tsdb = tsdb_dir.display()
    );
    std::fs::write(path, yaml).unwrap();
}

#[test]
fn fuzz_06_twin_process_ingester_differential_fuzz() {
    ensure_go_oracle_built().expect("go-mimir must exist");
    ensure_rust_mimir_release_built().expect("target/release/mimir must exist");

    let tmp = tempfile::tempdir().unwrap();
    let go_dir = tmp.path().join("go");
    let rust_dir = tmp.path().join("rust");
    std::fs::create_dir_all(go_dir.join("tsdb")).unwrap();
    std::fs::create_dir_all(go_dir.join("bucket")).unwrap();
    std::fs::create_dir_all(rust_dir.join("tsdb")).unwrap();
    std::fs::create_dir_all(rust_dir.join("bucket")).unwrap();

    let go_http = mimir_conformance::free_test_port();
    let go_grpc = mimir_conformance::free_test_port();
    let rust_http = mimir_conformance::free_test_port();
    let rust_grpc = mimir_conformance::free_test_port();

    let go_yaml = go_dir.join("mimir.yaml");
    let rust_yaml = rust_dir.join("mimir.yaml");
    write_twin_fuzz_yaml(&go_yaml, go_http, go_grpc, &go_dir.join("tsdb"), &go_dir.join("bucket"));
    write_twin_fuzz_yaml(
        &rust_yaml,
        rust_http,
        rust_grpc,
        &rust_dir.join("tsdb"),
        &rust_dir.join("bucket"),
    );

    let mut go_proc = TwinProc::spawn("go-mimir", &go_mimir_bin(), &go_yaml, &go_dir);
    let mut rust_proc = TwinProc::spawn("rust-mimir", &rust_mimir_release_bin(), &rust_yaml, &rust_dir);

    block_on_for_tests(async {
        let http = reqwest::Client::builder()
            .pool_max_idle_per_host(0)
            .build()
            .unwrap();
        wait_ready_proc(&http, go_http, &mut go_proc).await;
        wait_ready_proc(&http, rust_http, &mut rust_proc).await;

        let mut go_client = IngesterClient::connect(format!("http://127.0.0.1:{go_grpc}"))
            .await
            .unwrap();
        let mut rust_client = IngesterClient::connect(format!("http://127.0.0.1:{rust_grpc}"))
            .await
            .unwrap();

        let mut rng = Rng::new(0x3706_7a1a_d1ff_0006);
        let base_ts = now_ms() - 120_000;
        let tenants = ["fuzz-t0", "fuzz-t1", "fuzz-t2"];

        for (ti, tenant) in tenants.iter().enumerate() {
            let seed = cortexpb::WriteRequest {
                source: cortexpb::write_request::SourceEnum::Api as i32,
                metadata: vec![cortexpb::MetricMetadata {
                    r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
                    metric_family_name: "fuzz_counter_total".into(),
                    help: format!("help-{ti}"),
                    unit: "1".into(),
                }],
                timeseries: vec![cortexpb::TimeSeries {
                    labels: vec![
                        cortexpb::LabelPair::from_static("__name__", "fuzz_counter_total"),
                        cortexpb::LabelPair::from_strs("job", format!("seed-{ti}")),
                    ],
                    created_timestamp: base_ts - 500,
                    samples: vec![cortexpb::Sample {
                        value: 1.0,
                        timestamp_ms: base_ts,
                    }],
                    exemplars: vec![cortexpb::Exemplar {
                        labels: vec![cortexpb::LabelPair::from_static("trace_id", "seed-tr")],
                        value: 1.0,
                        timestamp_ms: base_ts,
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            };
            let g = go_client.push(with_tenant(tenant, seed.clone())).await;
            let r = rust_client.push(with_tenant(tenant, seed)).await;
            assert_eq!(r.is_ok(), g.is_ok(), "seed push mismatch for {tenant}");
        }

        for batch_idx in 0..30usize {
            let tenant = tenants[batch_idx % tenants.len()];
            let ts = base_ts + ((batch_idx as i64) + 1) * 2_000;
            let job = format!("job-{}", batch_idx % 4);
            let mut series = Vec::new();

            let mut samples = vec![cortexpb::Sample {
                value: ((batch_idx + 1) as f64) * 1.25,
                timestamp_ms: ts,
            }];
            if batch_idx % 2 == 0 {
                samples.push(cortexpb::Sample {
                    value: (batch_idx as f64) + 0.5,
                    timestamp_ms: ts - 1_000,
                });
            }
            series.push(cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_static("__name__", "fuzz_counter_total"),
                    cortexpb::LabelPair::from_strs("inst", format!("i-{}", batch_idx % 3)),
                    cortexpb::LabelPair::from_strs("job", job.clone()),
                ],
                created_timestamp: ts - 500,
                samples,
                exemplars: if batch_idx % 3 == 0 {
                    vec![cortexpb::Exemplar {
                        labels: vec![cortexpb::LabelPair::from_strs("tid", format!("t{batch_idx}"))],
                        value: 2.5,
                        timestamp_ms: ts,
                    }]
                } else {
                    vec![]
                },
                ..Default::default()
            });

            match batch_idx % 3 {
                0 => series.push(cortexpb::TimeSeries {
                    labels: vec![
                        cortexpb::LabelPair::from_static("__name__", "fuzz_int_hist"),
                        cortexpb::LabelPair::from_strs("job", job.clone()),
                    ],
                    histograms: vec![cortexpb::Histogram {
                        count: Some(cortexpb::histogram::Count::CountInt(6)),
                        sum: 3.0,
                        schema: 0,
                        zero_threshold: 0.001,
                        zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountInt(2)),
                        positive_spans: vec![cortexpb::BucketSpan {
                            offset: 0,
                            length: 2,
                        }],
                        positive_deltas: vec![2, 0],
                        timestamp: ts,
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                1 => series.push(cortexpb::TimeSeries {
                    labels: vec![
                        cortexpb::LabelPair::from_static("__name__", "fuzz_float_hist"),
                        cortexpb::LabelPair::from_strs("job", job.clone()),
                    ],
                    histograms: vec![cortexpb::Histogram {
                        count: Some(cortexpb::histogram::Count::CountFloat(5.5)),
                        sum: 2.25,
                        schema: 1,
                        zero_threshold: 0.001,
                        zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountFloat(1.5)),
                        positive_spans: vec![cortexpb::BucketSpan {
                            offset: 0,
                            length: 2,
                        }],
                        positive_counts: vec![2.5, 1.5],
                        timestamp: ts,
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                _ => series.push(cortexpb::TimeSeries {
                    labels: vec![
                        cortexpb::LabelPair::from_static("__name__", "fuzz_nhcb_hist"),
                        cortexpb::LabelPair::from_strs("job", job.clone()),
                    ],
                    histograms: vec![cortexpb::Histogram {
                        count: Some(cortexpb::histogram::Count::CountInt(4)),
                        sum: 1.75,
                        schema: -53,
                        zero_threshold: 0.0,
                        zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountInt(0)),
                        positive_spans: vec![cortexpb::BucketSpan {
                            offset: 0,
                            length: 2,
                        }],
                        positive_deltas: vec![3, -2],
                        custom_values: vec![0.1, 0.5, 1.0],
                        timestamp: ts,
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
            }

            if batch_idx == 7 {
                let mut many_labels = vec![
                    cortexpb::LabelPair::from_static("__name__", "fuzz_many_labels"),
                    cortexpb::LabelPair::from_strs("job", job.clone()),
                ];
                for li in 0..12 {
                    many_labels.push(cortexpb::LabelPair::from_strs(
                        format!("lbl_{li:02}"),
                        "x".repeat(140),
                    ));
                }
                series.push(cortexpb::TimeSeries {
                    labels: many_labels,
                    samples: vec![cortexpb::Sample {
                        value: 7.0,
                        timestamp_ms: ts,
                    }],
                    ..Default::default()
                });
            }

            let wr = cortexpb::WriteRequest {
                source: if batch_idx % 4 == 0 {
                    cortexpb::write_request::SourceEnum::Rule as i32
                } else {
                    cortexpb::write_request::SourceEnum::Api as i32
                },
                metadata: if batch_idx % 5 == 0 {
                    vec![cortexpb::MetricMetadata {
                        r#type: cortexpb::metric_metadata::MetricType::Histogram as i32,
                        metric_family_name: "fuzz_int_hist".into(),
                        help: "hist".into(),
                        unit: "s".into(),
                    }]
                } else {
                    vec![]
                },
                timeseries: series,
                ..Default::default()
            };

            let go_res = go_client.push(with_tenant(tenant, wr.clone())).await;
            let rust_res = rust_client.push(with_tenant(tenant, wr)).await;
            assert_eq!(
                rust_res.is_ok(),
                go_res.is_ok(),
                "batch {batch_idx} push outcome mismatch: rust={rust_res:?}, go={go_res:?}"
            );
        }

        for tenant in tenants {
            let go_snap =
                snapshot_all_13_rpcs(&mut go_client, tenant, base_ts - 60_000, base_ts + 240_000)
                    .await;
            let rust_snap =
                snapshot_all_13_rpcs(&mut rust_client, tenant, base_ts - 60_000, base_ts + 240_000)
                    .await;
            assert_eq!(rust_snap.query_stream, go_snap.query_stream, "{tenant}: QueryStream mismatch");
            assert_eq!(rust_snap.exemplars, go_snap.exemplars, "{tenant}: QueryExemplars mismatch");
            assert_eq!(rust_snap.label_values, go_snap.label_values, "{tenant}: LabelValues mismatch");
            assert_eq!(rust_snap.label_names, go_snap.label_names, "{tenant}: LabelNames mismatch");
            assert_eq!(rust_snap.user_stats_mem, go_snap.user_stats_mem, "{tenant}: UserStats mismatch");
            assert_eq!(rust_snap.all_user_stats, go_snap.all_user_stats, "{tenant}: AllUserStats mismatch");
            assert_eq!(
                rust_snap.metrics_for_label_matchers,
                go_snap.metrics_for_label_matchers,
                "{tenant}: MetricsForLabelMatchers mismatch"
            );
            assert_eq!(rust_snap.metrics_metadata, go_snap.metrics_metadata, "{tenant}: MetricsMetadata mismatch");
            assert_eq!(
                rust_snap.label_names_and_values,
                go_snap.label_names_and_values,
                "{tenant}: LabelNamesAndValues mismatch"
            );
            assert_eq!(
                rust_snap.label_values_cardinality,
                go_snap.label_values_cardinality,
                "{tenant}: LabelValuesCardinality mismatch"
            );
            assert_eq!(rust_snap.active_series, go_snap.active_series, "{tenant}: ActiveSeries mismatch");
            assert_eq!(rust_snap.active_nh_series, go_snap.active_nh_series, "{tenant}: ActiveSeries NH mismatch");
            assert_eq!(rust_snap.search_label_names, go_snap.search_label_names, "{tenant}: SearchLabelNames mismatch");
            assert_eq!(rust_snap.search_label_values, go_snap.search_label_values, "{tenant}: SearchLabelValues mismatch");
        }

        let err_tenant = "fuzz-err";
        let err_seed_ts = base_ts + 10_000;
        let seed_err_wr = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_static("__name__", "err_base"),
                    cortexpb::LabelPair::from_static("k", "v"),
                ],
                samples: vec![cortexpb::Sample {
                    value: 42.0,
                    timestamp_ms: err_seed_ts,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        go_client.push(with_tenant(err_tenant, seed_err_wr.clone())).await.unwrap();
        rust_client.push(with_tenant(err_tenant, seed_err_wr)).await.unwrap();

        for iter in 0..16usize {
            let count = rng.gen_range(1, 5);
            let wr = match iter % 5 {
                0 => cortexpb::WriteRequest {
                    timeseries: (0..count)
                        .map(|k| cortexpb::TimeSeries {
                            labels: vec![
                                cortexpb::LabelPair::from_static("z", "1"),
                                cortexpb::LabelPair::from_strs("__name__", format!("bad_u_{k}")),
                            ],
                            samples: vec![cortexpb::Sample {
                                value: 1.0,
                                timestamp_ms: err_seed_ts + 1_000,
                            }],
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                },
                1 => cortexpb::WriteRequest {
                    timeseries: vec![cortexpb::TimeSeries {
                        labels: vec![
                            cortexpb::LabelPair::from_static("__name__", "err_base"),
                            cortexpb::LabelPair::from_static("k", "v"),
                        ],
                        samples: vec![cortexpb::Sample {
                            value: 999.0 + iter as f64,
                            timestamp_ms: err_seed_ts,
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                2 => cortexpb::WriteRequest {
                    timeseries: vec![cortexpb::TimeSeries {
                        labels: vec![
                            cortexpb::LabelPair::from_static("__name__", "err_base"),
                            cortexpb::LabelPair::from_static("k", "v"),
                        ],
                        samples: (0..count)
                            .map(|k| cortexpb::Sample {
                                value: k as f64,
                                timestamp_ms: err_seed_ts - 25 * 60 * 1000 + (k as i64) * 100,
                            })
                            .collect(),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                3 => cortexpb::WriteRequest {
                    timeseries: vec![cortexpb::TimeSeries {
                        labels: vec![
                            cortexpb::LabelPair::from_static("__name__", "err_future"),
                            cortexpb::LabelPair::from_strs("k", format!("v{iter}")),
                        ],
                        samples: (0..count)
                            .map(|k| cortexpb::Sample {
                                value: k as f64,
                                timestamp_ms: now_ms() + 20 * 60 * 1000 + (k as i64) * 100,
                            })
                            .collect(),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                _ => cortexpb::WriteRequest {
                    timeseries: vec![cortexpb::TimeSeries {
                        labels: vec![
                            cortexpb::LabelPair::from_static("__name__", "err_dup_lbl"),
                            cortexpb::LabelPair::from_static("a", "1"),
                            cortexpb::LabelPair::from_static("a", "2"),
                        ],
                        samples: vec![cortexpb::Sample {
                            value: 1.0,
                            timestamp_ms: err_seed_ts + 2_000,
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            };

            let go_res = go_client.push(with_tenant(err_tenant, wr.clone())).await;
            let rust_res = rust_client.push(with_tenant(err_tenant, wr)).await;
            assert_eq!(
                rust_res.is_ok(),
                go_res.is_ok(),
                "iter={iter} case={}: ok/err mismatch go={go_res:?} rust={rust_res:?}",
                iter % 5
            );
            let (Err(go_err), Err(rust_err)) = (go_res, rust_res) else {
                continue;
            };

            assert_eq!(
                rust_err.code(),
                go_err.code(),
                "err iter={iter}: status code mismatch (rust={rust_err:?}, go={go_err:?})"
            );
            let go_det = grpcutilpb::extract_cortex_error_details(&go_err)
                .unwrap_or_else(|| panic!("err iter={iter}: Go missing ErrorDetails: {go_err:?}"));
            let rust_det = grpcutilpb::extract_cortex_error_details(&rust_err)
                .unwrap_or_else(|| panic!("err iter={iter}: Rust missing ErrorDetails: {rust_err:?}"));
            assert_eq!(rust_det.cause, go_det.cause, "err iter={iter}: cause mismatch");
            assert_eq!(rust_det.soft, go_det.soft, "err iter={iter}: soft mismatch");
            assert_eq!(
                rust_det.rejected_samples, go_det.rejected_samples,
                "err iter={iter}: rejected_samples mismatch"
            );
        }
    });
}
