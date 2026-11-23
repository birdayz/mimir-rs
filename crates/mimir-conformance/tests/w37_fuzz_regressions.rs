//! Wave 37 Differential Fuzzing Regressions (Tier-1 Go-Oracle + Stateful TSDB + Wire Mutation).
//!
//! Permanent fast, deterministic regressions for the 4 bugs discovered during the Wave 37
//! fuzzing campaign plus seeded differential/mutation coverage:
//! 1. `prom_regexp::FastRegexMatcher` grouped literal quantification (`(?:foo)?`, `(?i:foo)*`, etc.)
//! 2. `parse_traceparent` exact Go OTel `TraceContext.extract` parity (whitespace & trailing `-`)
//! 3. `runtime_config` YAML 1.1 underscore-separated numbers (`1_000`, `1_000.5`)
//! 4. `yaml_v3` double-quoted escape parity with Go `go.yaml.in/yaml/v3/scannerc.go` (`\/`)
//! 5. Seeded `proxyproto` v1/v2/TLV, `RingDesc`/`PartitionRingDesc` CRDT merge, and stateful TSDB.

use std::collections::BTreeMap;
use std::io::Cursor;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use mimir_cmd::server::proxyproto::{self, ProxyBufReader};
use mimir_common::tracing::parse_traceparent;
use mimir_config::runtime_config::parse_runtime_config_yaml;
use mimir_config::yaml_v3::parse_first_document;
use mimir_config::MimirConfig;
use mimir_conformance::oracle::{
    ensure_go_oracle_built, invoke_oracle, RingMergeRequest, RingMergeResponse,
    VerifyTsdbDirRequest, VerifyTsdbDirResponse,
};
use mimir_proto::prost::Message as _;
use mimir_proto::ringpb::{
    Desc, InstanceDesc, InstanceState, OwnerDesc, OwnerState, PartitionDesc, PartitionRingDesc,
    PartitionState,
};
use mimir_ring::partition_ring::model::PartitionRingDescExt;
use mimir_ring::ring::model::DescExt;
use mimir_tsdb::chunkenc::{CounterResetHint, Encoding, Histogram, HistogramSpan};
use mimir_tsdb::compact::SampleValue;
use mimir_tsdb::db::{Tsdb, TsdbExtraOptions, TsdbOptions};
use mimir_tsdb::labels::prom_regexp::FastRegexMatcher as PromFastRegexMatcher;
use mimir_tsdb::matcher::{FastRegexMatcher, LabelMatcher, MatchType};
use serde::{Deserialize, Serialize};

#[derive(Clone)]
struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self { Self(seed) }
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
    fn usize(&mut self, n: usize) -> usize { (self.next_u64() % n as u64) as usize }
    fn bool(&mut self) -> bool { self.next_u64() & 1 == 1 }
    fn pick<'a, T>(&mut self, s: &'a [T]) -> &'a T { &s[self.usize(s.len())] }
}

#[derive(Serialize)]
struct FastRegexCaseIn<'a> { pattern: &'a str, inputs: &'a [&'a str] }
#[derive(Serialize)]
struct FastRegexCheckIn<'a> { cases: Vec<FastRegexCaseIn<'a>> }
#[derive(Deserialize)]
struct FastRegexCaseOut { error: String, matches: Vec<bool> }
#[derive(Deserialize)]
struct FastRegexCheckOut { cases: Vec<FastRegexCaseOut> }

fn gen_regex_atom(rng: &mut Rng, depth: usize) -> String {
    let atoms: &[&str] = &[
        "", "a", "A", "b", "foo", "FOO", "0", "_", ".", ".*", ".+", "^", "$",
        "\\A", "\\z", "\\d", "\\w", "\\s", "[a-z]", "[^a\\n]", "[[:digit:]]",
        "(?i)", "(?-i)", "(?s)", "(?-s)", "(?m)", "(?-m)", "ß", "ſ", "K", "é", "中",
    ];
    if depth == 0 || rng.usize(10) < 6 {
        return (*rng.pick(atoms)).to_string();
    }
    match rng.usize(5) {
        0 => format!("({})", gen_regex_expr(rng, depth - 1)),
        1 => format!("(?:{})", gen_regex_expr(rng, depth - 1)),
        2 => format!("(?i:{})", gen_regex_expr(rng, depth - 1)),
        3 => format!("(?-s:{})", gen_regex_expr(rng, depth - 1)),
        _ => {
            let inner = gen_regex_atom(rng, depth - 1);
            let q = *rng.pick(&["*", "+", "?", "*?", "+?", "??", "{0,2}", "{1,2}"]);
            format!("(?:{inner}){q}")
        }
    }
}

fn gen_regex_expr(rng: &mut Rng, depth: usize) -> String {
    let branches = 1 + rng.usize(3);
    (0..branches).map(|_| {
        let terms = 1 + rng.usize(3);
        (0..terms).map(|_| gen_regex_atom(rng, depth.saturating_sub(1))).collect::<String>()
    }).collect::<Vec<_>>().join("|")
}

#[test]
fn w37_fast_regex_ast_and_seeded_differential() {
    ensure_go_oracle_built().unwrap();
    let probes: &[&str] = &[
        "", "\n", "\r", "\x00", "f", "fo", "foo", "fooo", "foofoo", "FOO", "bar", "foobar",
        "a", "A", "ab", "abc", "a\nb", "foo\nbar", "0", "_", "ß", "ſ", "s", "K", "k", "é", "中",
    ];
    let mut patterns: Vec<String> = vec![
        "(?:foo)?".into(), "(?:foo)??".into(), "(?:foo)*".into(), "(?:foo)*?".into(),
        "(?:foo)+".into(), "(?:foo)+?".into(), "(?:foo){0,2}".into(), "(?:foo){2}".into(),
        "(?:foo){0}".into(), "(?:fo)(?:o)?".into(), "((?:foo))?".into(), "(?:(?i)foo)?".into(),
        "(?i:foo)?".into(), "(?i:foo)*".into(), "(?i:foo)+".into(), "(?:foo)bar".into(),
        "a(?:foo)?b".into(), "(?:ab)+c".into(), "(?:ab)*c".into(), "^(?:foo)?$".into(),
        "\\A(?:foo)?\\z".into(), "(?:abc){0,1}".into(), "(?:foo)??(?m:(?is)|(?-m).+)\\z".into(),
        "(?-s:.)+".into(), "(?-s:foo.bar)".into(), "(?m:^foo|bar$)".into(), "(?i:k)".into(),
    ];
    let mut rng = Rng::new(0x3701_F022);
    for _ in 0..450 { patterns.push(gen_regex_expr(&mut rng, 2)); }
    patterns.sort();
    patterns.dedup();

    for chunk in patterns.chunks(250) {
        let req = FastRegexCheckIn {
            cases: chunk.iter().map(|p| FastRegexCaseIn { pattern: p, inputs: probes }).collect(),
        };
        let out: FastRegexCheckOut = invoke_oracle("fast-regex-check", &req).unwrap();
        for (pat, go) in chunk.iter().zip(out.cases.iter()) {
            if !go.error.is_empty() {
                assert!(FastRegexMatcher::new(pat).is_err(), "matcher accepted Go-rejected {pat:?}");
                assert!(PromFastRegexMatcher::new_without_cache(pat).is_err(), "prom_regexp accepted Go-rejected {pat:?}");
                continue;
            }
            let Ok(m1) = FastRegexMatcher::new(pat) else { continue };
            let m2 = PromFastRegexMatcher::new_without_cache(pat)
                .unwrap_or_else(|e| panic!("prom_regexp rejected Go-accepted {pat:?}: {e}"));
            for (j, &s) in probes.iter().enumerate() {
                let want = go.matches[j];
                assert_eq!(m1.is_match(s), want, "matcher mismatch pat={pat:?} input={s:?}");
                assert_eq!(m2.is_match(s), want, "prom_regexp mismatch pat={pat:?} input={s:?}");
            }
        }
    }
}

#[derive(Serialize)]
struct TraceparentCheckIn { headers: Vec<String> }
#[derive(Deserialize)]
struct TraceparentHeaderOut { valid: bool, trace_id: String, span_id: String, trace_flags: u8, sampled: bool }
#[derive(Deserialize)]
struct TraceparentCheckOut { headers: Vec<TraceparentHeaderOut> }

#[test]
fn w37_traceparent_seeded_differential() {
    ensure_go_oracle_built().unwrap();
    let mut rng = Rng::new(0x3702_7000);
    let mut headers: Vec<String> = vec![
        "".into(),
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".into(),
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-".into(),
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-x".into(),
        " 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01 ".into(),
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01 ".into(),
        "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-ff-extra".into(),
        "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".into(),
    ];
    let base = b"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    for _ in 0..600 {
        let mut b = base.to_vec();
        for _ in 0..(1 + rng.usize(4)) {
            let idx = rng.usize(b.len().max(1));
            match rng.usize(5) {
                0 => b[idx] = *rng.pick(b"0123456789abcdefABCDEFgZ- \t\n"),
                1 => { if b.len() > 1 { b.remove(idx); } }
                2 => b.insert(idx, *rng.pick(b"0-afF ")),
                3 => b.extend_from_slice(rng.pick(&[b"-".as_slice(), b"-x".as_slice(), b" ".as_slice(), b"-01".as_slice()])),
                _ => { b[0] = *rng.pick(b"0129afF"); b[1] = *rng.pick(b"0129afF"); }
            }
        }
        if let Ok(s) = String::from_utf8(b) { headers.push(s); }
    }
    let out: TraceparentCheckOut = invoke_oracle("traceparent-check", &TraceparentCheckIn { headers: headers.clone() }).unwrap();
    for (h, go) in headers.iter().zip(out.headers.iter()) {
        let r = parse_traceparent(h);
        match (&r, go.valid) {
            (None, false) => {}
            (Some(tc), true) => {
                assert_eq!(tc.trace_id_hex(), go.trace_id, "hdr={h:?}");
                assert_eq!(tc.span_id_hex(), go.span_id, "hdr={h:?}");
                assert_eq!(tc.trace_flags, go.trace_flags, "hdr={h:?}");
                assert_eq!(tc.is_sampled(), go.sampled, "hdr={h:?}");
            }
            _ => panic!("traceparent mismatch hdr={h:?} rust={r:?} go_valid={}", go.valid),
        }
    }
}

#[derive(Serialize)]
struct LoadRcIn { files: Vec<String>, loader: String }
#[derive(Deserialize)]
struct LoadRcOut { ok: bool, error: String }

#[test]
fn w37_runtime_config_underscore_numbers_and_edge_cases_differential() {
    ensure_go_oracle_built().unwrap();
    let defs = MimirConfig::default();
    let tmp = tempfile::tempdir().unwrap();
    let cases = [
        "overrides:\n  t1:\n    ingestion_rate: 1_000\n    max_global_series_per_user: 1_000_000\n",
        "overrides:\n  t1:\n    ingestion_rate: 1_000.5\n",
        "ingester_limits:\n  max_series: 500_000\n  max_ingestion_rate: 100_000\n",
        "overrides:\n  12345:\n    ingestion_rate: 5_000\n",
        "overrides:\n  t1:\n    unknown_field: 1\n",
        "overrides:\n  t1:\n    <<: 42\n",
        "multi_kv_config:\n  primary: etcd\n  mirror_enabled: true\n",
    ];
    for (i, yaml) in cases.iter().enumerate() {
        let p = tmp.path().join(format!("rc_{i}.yaml"));
        std::fs::write(&p, yaml).unwrap();
        let go: LoadRcOut = invoke_oracle("load-runtime-config", &LoadRcIn {
            files: vec![p.to_string_lossy().into_owned()],
            loader: "yaml".to_string(),
        }).unwrap();
        let rust_res = parse_runtime_config_yaml(yaml.as_bytes(), &defs.limits, &defs.ingester.instance_limits);
        assert_eq!(rust_res.is_ok(), go.ok, "runtime_config diff i={i} yaml={yaml:?} go_err={:?} rust_err={:?}", go.error, rust_res.err());
    }
    let parsed = parse_runtime_config_yaml(
        b"overrides:\n  t1:\n    ingestion_rate: 1_000.5\n    max_global_series_per_user: 1_000_000\n",
        &defs.limits,
        &defs.ingester.instance_limits,
    ).unwrap();
    let t1 = parsed.overrides.get("t1").and_then(|o| o.as_deref()).expect("t1 override");
    assert_eq!(t1.ingestion_rate, 1000.5);
    assert_eq!(t1.max_global_series_per_user, 1_000_000);
}

#[derive(Serialize)]
struct YamlV3CheckIn { docs: Vec<String> }
#[derive(Deserialize)]
struct YamlV3DocOut { ok: bool, error: String }
#[derive(Deserialize)]
struct YamlV3CheckOut { docs: Vec<YamlV3DocOut> }

#[test]
fn w37_yaml_v3_double_quoted_escapes_differential() {
    ensure_go_oracle_built().unwrap();
    // Note: Go yaml.v3 accepts `\'` inside double-quoted scalars via a Go-specific patch to libyaml;
    // all YAML 1.2 escapes + `\/` rejection (Go yaml.v3 omits `/`) must match Go 1:1.
    let esc_chars = ["0", "a", "b", "t", "n", "v", "f", "r", "e", " ", "\"", "\\", "/", "N", "_", "L", "P", "q", "z"];
    let docs: Vec<String> = esc_chars.iter().map(|c| format!("k: \"\\{c}\"\n")).collect();
    let go: YamlV3CheckOut = invoke_oracle("yaml-v3-check", &YamlV3CheckIn { docs: docs.clone() }).unwrap();
    for (doc, g) in docs.iter().zip(go.docs.iter()) {
        let r = parse_first_document(doc.as_bytes());
        assert_eq!(r.is_ok(), g.ok, "yaml_v3 escape diff doc={doc:?} go_err={:?} rust_err={:?}", g.error, r.err());
    }
    let mut cfg = MimirConfig::default();
    let err = cfg.load_config_from_bytes(b"target: ingester\nblocks_storage:\n  backend: filesystem\n  filesystem:\n    dir: \"a\\/b\"\n", false).unwrap_err();
    assert!(err.to_string().contains("found unknown escape character"), "{err}");
}

#[test]
fn w37_proxyproto_mutation_fuzz() {
    let mut rng = Rng::new(0x3702_9900);
    let seeds: &[&[u8]] = &[
        b"PROXY TCP4 192.168.0.1 192.168.0.11 56324 443\r\n",
        b"PROXY TCP6 2001:db8::1 2001:db8::2 12345 443\r\n",
        b"PROXY UNKNOWN\r\n",
        b"\x0D\x0A\x0D\x0A\x00\x0D\x0A\x51\x55\x49\x54\x0A\x21\x11\x00\x0C\x7F\x00\x00\x01\x7F\x00\x00\x01\x1F\x90\x01\xBB",
    ];
    for _ in 0..5_000 {
        let mut buf = rng.pick(seeds).to_vec();
        for _ in 0..(1 + rng.usize(5)) {
            if buf.is_empty() { break; }
            let idx = rng.usize(buf.len());
            match rng.usize(4) {
                0 => buf[idx] = (rng.next_u64() & 0xff) as u8,
                1 => { buf.remove(idx); }
                2 => buf.insert(idx, (rng.next_u64() & 0xff) as u8),
                _ => buf.truncate(idx),
            }
        }
        let mut reader = ProxyBufReader::new(Cursor::new(&buf));
        if let Ok(hdr) = proxyproto::read(&mut reader) {
            let _ = hdr.tlvs();
            let mut out = Vec::new();
            let _ = hdr.write_to(&mut out);
        }
        if let Ok(tlvs) = proxyproto::split_tlvs(&buf) {
            if let Ok(joined) = proxyproto::join_tlvs(&tlvs) {
                assert_eq!(tlvs, proxyproto::split_tlvs(&joined).unwrap());
            }
        }
    }
}

#[derive(Serialize)]
struct PrMergeIn { local_b64: String, remote_b64: String, local_cas: bool, now_unix: i64 }
#[derive(Deserialize)]
struct PrMergeOut { merged_b64: String, change_b64: Option<String> }

#[test]
fn w37_ring_and_partition_ring_merge_differential() {
    ensure_go_oracle_built().unwrap();
    let mut rng = Rng::new(0x3703_C0DE);
    let now_unix = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
    for iter in 0..30 {
        let mut local = PartitionRingDesc::default();
        let mut remote = PartitionRingDesc::default();
        for d in [&mut local, &mut remote] {
            for pid in 0..3i32 {
                if rng.usize(4) == 0 { continue; }
                let mut toks: Vec<u32> = (0..rng.usize(4)).map(|_| (rng.next_u64() % 500) as u32).collect();
                toks.sort_unstable(); toks.dedup();
                d.partitions.insert(pid, PartitionDesc {
                    id: pid, tokens: toks,
                    state: *rng.pick(&[PartitionState::PartitionPending as i32, PartitionState::PartitionActive as i32, PartitionState::PartitionInactive as i32]),
                    state_timestamp: now_unix - 100 + (rng.usize(5) as i64 - 2),
                    state_change_locked: rng.bool(),
                    state_change_locked_timestamp: now_unix - 100 + (rng.usize(5) as i64 - 2),
                    token_scheme: rng.usize(2) as i32,
                });
            }
            d.owners.insert("ing-0".into(), OwnerDesc {
                owned_partition: rng.usize(3) as i32,
                state: *rng.pick(&[OwnerState::OwnerActive as i32, OwnerState::OwnerDeleted as i32]),
                updated_timestamp: now_unix - 100 + (rng.usize(5) as i64 - 2),
            });
        }
        let cas = rng.bool();
        let go: PrMergeOut = invoke_oracle("ring-merge-check", &PrMergeIn {
            local_b64: B64.encode(local.encode_to_vec()),
            remote_b64: B64.encode(remote.encode_to_vec()),
            local_cas: cas,
            now_unix,
        }).unwrap();
        let go_m = PartitionRingDesc::decode(B64.decode(&go.merged_b64).unwrap().as_slice()).unwrap();
        let go_c = go.change_b64.as_deref().map(|s| PartitionRingDesc::decode(B64.decode(s).unwrap().as_slice()).unwrap());
        let mut rl = local.clone();
        let rc = rl.merge_with_time(&remote, cas, UNIX_EPOCH + Duration::from_secs(now_unix as u64));
        assert_eq!(rl, go_m, "pr merged diff iter={iter}");
        assert_eq!(rc, go_c, "pr change diff iter={iter}");

        let mut cl = Desc::default();
        let mut cr = Desc::default();
        for d in [&mut cl, &mut cr] {
            for id in ["ing-0", "ing-1", "ing-2"] {
                if rng.usize(3) == 0 { continue; }
                let mut toks: Vec<u32> = (0..rng.usize(4)).map(|_| (rng.next_u64() % 500) as u32).collect();
                toks.sort_unstable(); toks.dedup();
                d.ingesters.insert(id.into(), InstanceDesc {
                    addr: format!("10.0.0.{}:9095", rng.usize(3) + 1),
                    timestamp: now_unix - 100 + (rng.usize(5) as i64 - 2),
                    state: *rng.pick(&[InstanceState::Pending as i32, InstanceState::Joining as i32, InstanceState::Active as i32, InstanceState::Leaving as i32, InstanceState::Left as i32]),
                    tokens: toks, zone: (*rng.pick(&["zone-a", "zone-b", ""])).to_string(),
                    registered_timestamp: now_unix - 110 + (rng.usize(4) as i64),
                    read_only: rng.bool(), read_only_updated_timestamp: now_unix - 100 + (rng.usize(5) as i64 - 2),
                    ..Default::default()
                });
            }
        }
        let go2: RingMergeResponse = invoke_oracle("ring-merge", &RingMergeRequest {
            local_b64: B64.encode(cl.encode_to_vec()),
            remote_b64: B64.encode(cr.encode_to_vec()),
            cas,
        }).unwrap();
        let go2_m = Desc::decode(B64.decode(&go2.merged_b64).unwrap().as_slice()).unwrap();
        let go2_c = go2.change_b64.as_deref().map(|s| Desc::decode(B64.decode(s).unwrap().as_slice()).unwrap());
        let mut r2 = cl.clone();
        let mut r2_rem = cr.clone();
        let rc2 = r2.merge_with_time(&mut r2_rem, cas, SystemTime::now());
        assert_eq!(r2, go2_m, "ring merged diff iter={iter}");
        assert_eq!(rc2, go2_c, "ring change diff iter={iter}");
    }
}

#[test]
fn w37_tsdb_stateful_snapshot_tombstone_go_reopen() {
    ensure_go_oracle_built().unwrap();
    let mut rng = Rng::new(0x3710_75DB);
    for run in 0..6 {
        let tmp = tempfile::tempdir().unwrap();
        let use_xor2 = run % 2 == 0;
        let enable_snap = run % 3 == 0;
        let mut opts = TsdbOptions::default();
        opts.head.ooo_time_window_ms = 300_000;
        opts.head.chunk_range_ms = 3_600_000;
        opts.head.enable_st_storage = use_xor2;
        opts.head.float_chunk_encoding = if use_xor2 { Encoding::Xor2 } else { Encoding::Xor };
        let extra = TsdbExtraOptions { enable_memory_snapshot_on_shutdown: enable_snap, ..Default::default() };
        let db = Tsdb::open_with_extra(tmp.path(), opts.clone(), extra.clone()).unwrap();
        let mut t = 1_700_000_000_000i64;
        for _ in 0..60 {
            let sname = *rng.pick(&["s_0", "s_1", "s_2", "s_3"]);
            match rng.usize(10) {
                0..=5 => {
                    let mut app = db.appender();
                    let ts = if rng.usize(3) == 0 { t - (1 + rng.usize(100_000)) as i64 } else { t += 2000; t };
                    let st = if use_xor2 && rng.bool() { ts - 5000 } else { 0 };
                    let v = (rng.next_u64() % 1000) as f64 / 4.0;
                    let _ = app.append_float_with_st(None, &[("__name__", sname), ("job", "j")], st, ts, v);
                    if rng.usize(5) != 0 { let _ = app.commit(); } else { let _ = app.rollback(); }
                }
                6 => {
                    let mut app = db.appender();
                    t += 2000;
                    let h = Histogram { counter_reset_hint: CounterResetHint::UnknownCounterReset, schema: 2, zero_threshold: 0.001, zero_count: 1, count: 2, sum: 3.0, positive_spans: vec![HistogramSpan { offset: 0, length: 1 }], positive_buckets: vec![1], ..Default::default() };
                    let _ = app.append_histogram(None, &[("__name__", "h_0"), ("job", "j")], t, 0, h);
                    let _ = app.commit();
                }
                7 => { let lm = LabelMatcher::new(MatchType::Equal, "__name__", sname).unwrap(); let _ = db.delete(t - 40_000, t - 10_000, &[lm]); }
                8 => { let _ = db.compact(); }
                _ => { let _ = db.compact_ooo_head(); t += 1_900_000; }
            }
        }
        db.close().unwrap();

        let db2 = Tsdb::open_with_extra(tmp.path(), opts, extra).unwrap();
        let lm = LabelMatcher::new(MatchType::RegexMatch, "__name__", "s_.*").unwrap();
        let mut rust_map: BTreeMap<String, BTreeMap<i64, u64>> = BTreeMap::new();
        for s in db2.querier(i64::MIN, i64::MAX).select(true, &[lm]).unwrap() {
            let name = s.labels.get("__name__").to_string();
            for pt in s.samples {
                if let SampleValue::Float(f) = pt.value { rust_map.entry(name.clone()).or_default().insert(pt.t, f.to_bits()); }
            }
        }
        db2.close().unwrap();

        let gv: VerifyTsdbDirResponse = invoke_oracle("verify-tsdb-dir", &VerifyTsdbDirRequest {
            dir: tmp.path().to_string_lossy().into_owned(),
            read_only: false,
        }).unwrap();
        let mut go_map: BTreeMap<String, BTreeMap<i64, u64>> = BTreeMap::new();
        for s in gv.series {
            let name = s.labels.iter().find(|l| l.name == "__name__").map(|l| l.value.as_str()).unwrap_or("");
            if name.starts_with("s_") {
                for pt in s.samples { go_map.entry(name.to_string()).or_default().insert(pt.t, pt.v.to_bits()); }
            }
        }
        assert_eq!(rust_map, go_map, "Go vs Rust TSDB diff run={run}");
    }
}
