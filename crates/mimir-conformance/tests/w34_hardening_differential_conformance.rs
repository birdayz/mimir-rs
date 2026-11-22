//! Wave 34 Tier-1 Go-Oracle Differential Hardening Conformance Suite.
//!
//! Differential tests against real upstream Go packages via `target/go-oracle`:
//! - `fast-regex-check` (`prometheus/model/labels.NewFastRegexMatcher`)
//! - `yaml-v3-check` (`go.yaml.in/yaml/v3.Unmarshal`)
//! - `traceparent-check` (`go.opentelemetry.io/otel/propagation.TraceContext`)
//! - `ring-merge-check` (`dskit/ring.PartitionRingDesc.Merge`)
//! - `verify-tsdb-dir` (`prometheus/tsdb.Open` cross-engine memory-snapshot + `chunks_head` reopen)

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;
use std::time::{Duration, UNIX_EPOCH};

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use mimir_common::tracing::parse_traceparent;
use mimir_config::yaml_v3::{doc_to_serde_value, parse_first_document};
use mimir_conformance::oracle::{
    ensure_go_oracle_built, invoke_oracle, VerifyTsdbDirRequest, VerifyTsdbDirResponse,
};
use mimir_proto::prost::Message as _;
use mimir_proto::ringpb::{OwnerDesc, OwnerState, PartitionDesc, PartitionRingDesc, PartitionState};
use mimir_ring::partition_ring::model::PartitionRingDescExt;
use mimir_tsdb::compact::SampleValue;
use mimir_tsdb::db::{Tsdb, TsdbExtraOptions, TsdbOptions};
use mimir_tsdb::head::HeadOptions;
use mimir_tsdb::labels::prom_regexp::FastRegexMatcher as UpstreamFastRegexMatcher;
use mimir_tsdb::matcher::{FastRegexMatcher, LabelMatcher, MatchType};
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
struct FastRegexCaseIn<'a> {
    pattern: &'a str,
    inputs: &'a [&'a str],
}

#[derive(Serialize)]
struct FastRegexCheckIn<'a> {
    cases: Vec<FastRegexCaseIn<'a>>,
}

#[derive(Deserialize)]
struct FastRegexCaseOut {
    error: String,
    set_matches: Vec<String>,
    matches: Vec<bool>,
}

#[derive(Deserialize)]
struct FastRegexCheckOut {
    cases: Vec<FastRegexCaseOut>,
}

#[test]
fn w34_oracle_fast_regex_matcher_differential() {
    ensure_go_oracle_built().expect("build go-oracle");

    let probe_inputs: &[&str] = &[
        "",
        "0",
        "9",
        "a",
        "Z",
        "f",
        "F",
        "g",
        "_",
        "-",
        "!",
        " ",
        "\t",
        "\n",
        "\x07",
        "aa",
        "a{02}",
        "{01}",
        "{00}",
        "p",
        "L",
        "{",
        "}",
        "é",
        "α",
        "中",
        "٣",
        "foo",
        "foo\nbar",
        "\u{E000}",
    ];

    let patterns: &[&str] = &[
        "{01}",
        "{00}",
        "({01})",
        "a|{01}",
        "a{02}",
        "a{0,2}",
        "[[:digit:]]",
        "[[:^digit:]]",
        "[[:alpha:]]",
        "[[:^alpha:]]",
        "[[:alnum:]]",
        "[[:^alnum:]]",
        "[[:lower:]]",
        "[[:^lower:]]",
        "[[:upper:]]",
        "[[:^upper:]]",
        "[[:space:]]",
        "[[:^space:]]",
        "[[:ascii:]]",
        "[[:^ascii:]]",
        "[[:xdigit:]]",
        "[[:^xdigit:]]",
        "[[:word:]]",
        "[[:^word:]]",
        "[[:punct:]]",
        "[[:^punct:]]",
        "[[:blank:]]",
        "[[:^blank:]]",
        "[[:cntrl:]]",
        "[[:^cntrl:]]",
        "[[:graph:]]",
        "[[:^graph:]]",
        "[[:print:]]",
        "[[:^print:]]",
        r"[\p{L}]",
        r"[\P{Nd}]",
        r"[\pL]",
        r"\p{L}+",
        r"(?i)[\x{D800}-\x{F000}]",
        r"\Q{01}[[:^digit:]]\E",
        "(?i)FOO",
        "(?-s)foo.bar",
    ];

    let req = FastRegexCheckIn {
        cases: patterns
            .iter()
            .map(|&p| FastRegexCaseIn {
                pattern: p,
                inputs: probe_inputs,
            })
            .collect(),
    };
    let go_out: FastRegexCheckOut =
        invoke_oracle("fast-regex-check", &req).expect("fast-regex-check oracle");
    assert_eq!(go_out.cases.len(), patterns.len());

    let mut mismatches = Vec::new();
    for (idx, &pat) in patterns.iter().enumerate() {
        let go = &go_out.cases[idx];
        let go_ok = go.error.is_empty();

        // Check both labels::upstream::FastRegexMatcher AND matcher::FastRegexMatcher (query path).
        let up_res = UpstreamFastRegexMatcher::new_without_cache(pat);
        if go_ok != up_res.is_ok() {
            mismatches.push(format!(
                "[upstream] pattern {pat:?}: go_ok={go_ok} ({:?}) vs rust_ok={} ({:?})",
                go.error,
                up_res.is_ok(),
                up_res.as_ref().err()
            ));
        } else if let Ok(matcher) = up_res {
            for (j, &inp) in probe_inputs.iter().enumerate() {
                let r_match = matcher.is_match(inp);
                let g_match = go.matches[j];
                if r_match != g_match {
                    mismatches.push(format!(
                        "[upstream] pattern {pat:?} input {inp:?}: go={g_match} rust={r_match} (go_set={:?} rust_set={:?})",
                        go.set_matches,
                        matcher.set_matches()
                    ));
                }
            }
        }

        let m_res = FastRegexMatcher::new(pat);
        if go_ok != m_res.is_ok() {
            mismatches.push(format!(
                "[matcher] pattern {pat:?}: go_ok={go_ok} ({:?}) vs rust_ok={} ({:?})",
                go.error,
                m_res.is_ok(),
                m_res.as_ref().err()
            ));
        } else if let Ok(matcher) = m_res {
            for (j, &inp) in probe_inputs.iter().enumerate() {
                let r_match = matcher.is_match(inp);
                let g_match = go.matches[j];
                if r_match != g_match {
                    mismatches.push(format!(
                        "[matcher] pattern {pat:?} input {inp:?}: go={g_match} rust={r_match} (go_set={:?} rust_set={:?})",
                        go.set_matches,
                        matcher.set_matches()
                    ));
                }
            }
        }
    }
    assert!(
        mismatches.is_empty(),
        "FastRegexMatcher Go-vs-Rust mismatches ({}):\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}

#[derive(Serialize)]
struct YamlV3CheckIn {
    docs: Vec<String>,
}

#[derive(Deserialize)]
struct YamlV3DocOut {
    ok: bool,
    error: String,
}

#[derive(Deserialize)]
struct YamlV3CheckOut {
    docs: Vec<YamlV3DocOut>,
}

#[test]
fn w34_oracle_yaml_v3_adversarial_differential() {
    ensure_go_oracle_built().expect("build go-oracle");

    // Billion-laughs exponential alias DAG (CVE-2019-11254)
    let billion_laughs = [
        "a: &a [a, a, a, a, a, a, a, a, a, a]",
        "b: &b [*a, *a, *a, *a, *a, *a, *a, *a, *a, *a]",
        "c: &c [*b, *b, *b, *b, *b, *b, *b, *b, *b, *b]",
        "d: &d [*c, *c, *c, *c, *c, *c, *c, *c, *c, *c]",
        "e: &e [*d, *d, *d, *d, *d, *d, *d, *d, *d, *d]",
        "f: &f [*e, *e, *e, *e, *e, *e, *e, *e, *e, *e]",
        "g: &g [*f, *f, *f, *f, *f, *f, *f, *f, *f, *f]",
        "h: &h [*g, *g, *g, *g, *g, *g, *g, *g, *g, *g]",
        "i: &i [*h, *h, *h, *h, *h, *h, *h, *h, *h, *h]",
    ]
    .join("\n");

    let valid_alias = "base: &b\n  x: 1\n  y: 2\ncopy: *b\n".to_string();
    let direct_cycle = "a: &a [*a]\n".to_string();
    let deep_nesting = format!("{}1{}", "[".repeat(300), "]".repeat(300));

    let docs = vec![valid_alias, billion_laughs, direct_cycle];
    let go_out: YamlV3CheckOut = invoke_oracle(
        "yaml-v3-check",
        &YamlV3CheckIn { docs: docs.clone() },
    )
    .expect("yaml-v3-check oracle");
    assert_eq!(go_out.docs.len(), docs.len());

    for (i, doc) in docs.iter().enumerate() {
        let rust_res = parse_first_document(doc.as_bytes()).and_then(|opt| match opt {
            Some(d) => doc_to_serde_value(&d).map(|_| ()),
            None => Ok(()),
        });
        assert_eq!(
            rust_res.is_ok(),
            go_out.docs[i].ok,
            "yaml doc #{i}: go_ok={} ({:?}) vs rust_res={:?}",
            go_out.docs[i].ok,
            go_out.docs[i].error,
            rust_res
        );
    }

    // Deep nesting (>256) must be rejected cleanly by Rust without stack overflow.
    assert!(
        parse_first_document(deep_nesting.as_bytes()).is_err(),
        "300-deep nested YAML must error cleanly without stack overflow"
    );
}

#[derive(Serialize)]
struct TraceparentCheckIn {
    headers: Vec<String>,
}

#[derive(Deserialize)]
struct TraceparentHeaderOut {
    valid: bool,
    trace_id: String,
    span_id: String,
    trace_flags: u8,
    sampled: bool,
}

#[derive(Deserialize)]
struct TraceparentCheckOut {
    headers: Vec<TraceparentHeaderOut>,
}

#[test]
fn w34_oracle_w3c_traceparent_all_256_flags_differential() {
    ensure_go_oracle_built().expect("build go-oracle");

    let mut headers = Vec::with_capacity(264);
    for flags in 0u8..=255u8 {
        headers.push(format!(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-{flags:02x}"
        ));
        headers.push(format!(
            "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-{flags:02x}"
        ));
    }
    // Edge cases: future version, ff version (invalid), all-zero trace_id/span_id, uppercase hex.
    headers.push("01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra".to_string());
    headers.push("ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string());
    headers.push("00-00000000000000000000000000000000-00f067aa0ba902b7-01".to_string());
    headers.push("00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01".to_string());
    headers.push("00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01".to_string());
    headers.push("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra".to_string());

    let go_out: TraceparentCheckOut = invoke_oracle(
        "traceparent-check",
        &TraceparentCheckIn {
            headers: headers.clone(),
        },
    )
    .expect("traceparent-check oracle");
    assert_eq!(go_out.headers.len(), headers.len());

    for (h, g) in headers.iter().zip(&go_out.headers) {
        let r = parse_traceparent(h);
        assert_eq!(
            r.is_some(),
            g.valid,
            "traceparent {h:?}: go valid={} vs rust {:?}",
            g.valid,
            r
        );
        if let Some(ctx) = r {
            let tid: String = ctx.trace_id.iter().map(|b| format!("{b:02x}")).collect();
            let sid: String = ctx.span_id.iter().map(|b| format!("{b:02x}")).collect();
            assert_eq!(tid, g.trace_id, "trace_id mismatch for {h:?}");
            assert_eq!(sid, g.span_id, "span_id mismatch for {h:?}");
            assert_eq!(ctx.trace_flags, g.trace_flags, "trace_flags mismatch for {h:?}");
            assert_eq!(ctx.sampled, g.sampled, "sampled mismatch for {h:?}");
        }
    }
}

#[derive(Serialize)]
struct RingMergeCheckIn {
    local_b64: String,
    remote_b64: String,
    local_cas: bool,
    now_unix: i64,
}

#[derive(Deserialize)]
struct RingMergeCheckOut {
    changed: bool,
    merged_b64: String,
}

#[test]
fn w34_oracle_partition_ring_merge_differential() {
    ensure_go_oracle_built().expect("build go-oracle");

    let now_unix = 1_750_000_000i64;
    let now = UNIX_EPOCH + Duration::from_secs(now_unix as u64);

    let mut local = PartitionRingDesc::default();
    local.partitions.insert(
        1,
        PartitionDesc {
            id: 1,
            tokens: vec![100, 500],
            state: PartitionState::PartitionActive as i32,
            state_timestamp: now_unix - 10,
            state_change_locked: false,
            state_change_locked_timestamp: now_unix - 10,
            ..Default::default()
        },
    );
    local.owners.insert(
        "owner-1".to_string(),
        OwnerDesc {
            owned_partition: 1,
            state: OwnerState::OwnerActive as i32,
            updated_timestamp: now_unix - 5,
        },
    );

    let mut remote = PartitionRingDesc::default();
    // Existing partition with future lock timestamp (> now + 10m) -> both Go & Rust must ignore lock update.
    remote.partitions.insert(
        1,
        PartitionDesc {
            id: 1,
            tokens: vec![100, 500],
            state: PartitionState::PartitionInactive as i32,
            state_timestamp: now_unix - 2,
            state_change_locked: true,
            state_change_locked_timestamp: now_unix + 3600,
            ..Default::default()
        },
    );
    // New partition with valid state_timestamp and future state_change_locked_timestamp (Go keeps on insert).
    remote.partitions.insert(
        2,
        PartitionDesc {
            id: 2,
            tokens: vec![200, 600],
            state: PartitionState::PartitionPending as i32,
            state_timestamp: now_unix - 1,
            state_change_locked: true,
            state_change_locked_timestamp: now_unix + 3600,
            ..Default::default()
        },
    );
    // New partition with future state_timestamp -> both Go & Rust reject.
    remote.partitions.insert(
        3,
        PartitionDesc {
            id: 3,
            tokens: vec![300],
            state: PartitionState::PartitionActive as i32,
            state_timestamp: now_unix + 3600,
            ..Default::default()
        },
    );
    remote.owners.insert(
        "owner-2".to_string(),
        OwnerDesc {
            owned_partition: 2,
            state: OwnerState::OwnerActive as i32,
            updated_timestamp: now_unix - 1,
        },
    );

    for local_cas in [false, true] {
        let req = RingMergeCheckIn {
            local_b64: B64.encode(local.encode_to_vec()),
            remote_b64: B64.encode(remote.encode_to_vec()),
            local_cas,
            now_unix,
        };
        let go_out: RingMergeCheckOut =
            invoke_oracle("ring-merge-check", &req).expect("ring-merge-check oracle");
        let go_merged = PartitionRingDesc::decode(
            B64.decode(&go_out.merged_b64).expect("decode merged_b64").as_slice(),
        )
        .expect("decode go PartitionRingDesc");

        let mut rust_merged = local.clone();
        let rust_change = rust_merged.merge_with_time(&remote, local_cas, now);
        assert_eq!(rust_change.is_some(), go_out.changed, "local_cas={local_cas}");
        assert_eq!(
            rust_merged.partitions.keys().copied().collect::<BTreeSet<_>>(),
            go_merged.partitions.keys().copied().collect::<BTreeSet<_>>(),
            "partition keys mismatch (local_cas={local_cas})"
        );
        for (&pid, gp) in &go_merged.partitions {
            let rp = &rust_merged.partitions[&pid];
            assert_eq!(rp.state, gp.state, "pid={pid} state (local_cas={local_cas})");
            assert_eq!(
                rp.state_timestamp, gp.state_timestamp,
                "pid={pid} state_timestamp (local_cas={local_cas})"
            );
            assert_eq!(
                rp.state_change_locked, gp.state_change_locked,
                "pid={pid} state_change_locked (local_cas={local_cas})"
            );
            assert_eq!(
                rp.state_change_locked_timestamp, gp.state_change_locked_timestamp,
                "pid={pid} state_change_locked_timestamp (local_cas={local_cas})"
            );
            assert_eq!(rp.tokens, gp.tokens, "pid={pid} tokens (local_cas={local_cas})");
        }
        assert_eq!(
            rust_merged.owners, go_merged.owners,
            "owners mismatch (local_cas={local_cas})"
        );
    }
}

fn copy_dir_recursive(src: &Path, dst: &Path) {
    fs::create_dir_all(dst).unwrap();
    for entry in fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let ty = entry.file_type().unwrap();
        let to = dst.join(entry.file_name());
        if ty.is_dir() {
            copy_dir_recursive(&entry.path(), &to);
        } else {
            fs::copy(entry.path(), &to).unwrap();
        }
    }
}

#[test]
fn w34_oracle_chunk_snapshot_plus_mmapped_cross_engine_reopen() {
    ensure_go_oracle_built().expect("build go-oracle");

    let src_dir = tempfile::tempdir().unwrap();
    let mut expected_ino: BTreeMap<i64, f64> = BTreeMap::new();
    let mut expected_ooo: BTreeMap<i64, f64> = BTreeMap::new();

    {
        let opts = TsdbOptions {
            head: HeadOptions {
                ooo_time_window_ms: 600_000,
                ..Default::default()
            },
            ..Default::default()
        };
        let extra = TsdbExtraOptions {
            enable_memory_snapshot_on_shutdown: true,
            ..Default::default()
        };
        let db = Tsdb::open_with_extra(src_dir.path(), opts, extra).expect("open Rust TSDB");

        // Write 320 in-order samples (> 2 chunks of 120 samples) so close() mmaps >= 2 sealed
        // chunks into chunks_head/ AND writes chunk_snapshot.* for the active tail chunk.
        let base_t = 1_000_000i64;
        for i in 0..320i64 {
            let t = base_t + i * 1_000;
            let v = (i as f64) * 1.25;
            let mut app = db.appender();
            app.append_float(None, &[("__name__", "snap_ino_metric"), ("job", "w34")], t, v)
                .unwrap();
            app.commit().unwrap();
            expected_ino.insert(t, v);
        }

        // Write 1 in-order anchor + 45 OOO samples (> ooo_cap_max=32) so an OOO chunk is also
        // mmapped into chunks_head/ before shutdown snapshot.
        {
            let mut app = db.appender();
            let anchor_t = base_t + 319_000;
            app.append_float(
                None,
                &[("__name__", "snap_ooo_metric"), ("job", "w34")],
                anchor_t,
                999.0,
            )
            .unwrap();
            app.commit().unwrap();
            expected_ooo.insert(anchor_t, 999.0);
        }
        for i in 0..45i64 {
            let t = base_t + 10_000 + i * 1_000;
            let v = (i as f64) + 0.5;
            let mut app = db.appender();
            app.append_float(None, &[("__name__", "snap_ooo_metric"), ("job", "w34")], t, v)
                .unwrap();
            app.commit().unwrap();
            expected_ooo.insert(t, v);
        }

        db.close().expect("close Rust TSDB with snapshot");
    }

    assert!(
        src_dir.path().join("chunks_head").join("000001").exists(),
        "expected chunks_head/000001"
    );
    let has_snapshot = fs::read_dir(src_dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .any(|e| e.file_name().to_string_lossy().starts_with("chunk_snapshot."));
    assert!(has_snapshot, "expected chunk_snapshot.* on shutdown");

    // Copy directory so both Rust and Go reopen the exact same snapshot + chunks_head + WAL/WBL.
    let go_dir = tempfile::tempdir().unwrap();
    copy_dir_recursive(src_dir.path(), go_dir.path());

    // 1. Reopen in Rust and verify ALL 320 in-order samples + ALL 46 OOO+anchor samples survived!
    let rust_opts = TsdbOptions {
        head: HeadOptions {
            ooo_time_window_ms: 600_000,
            ..Default::default()
        },
        ..Default::default()
    };
    let rust_db = Tsdb::open(src_dir.path(), rust_opts).expect("reopen Rust TSDB from snapshot");
    let q = rust_db.querier(i64::MIN, i64::MAX);
    let rust_series = q
        .select(true, &[LabelMatcher::new(MatchType::Equal, "job", "w34").unwrap()])
        .expect("select after Rust snapshot reopen");
    let mut rust_map: BTreeMap<String, BTreeMap<i64, f64>> = BTreeMap::new();
    for s in rust_series {
        let name = s.labels.get("__name__").to_string();
        let entry = rust_map.entry(name).or_default();
        for sample in s.samples {
            if let SampleValue::Float(v) = sample.value {
                entry.insert(sample.t, v);
            }
        }
    }
    assert_eq!(
        rust_map.get("snap_ino_metric"),
        Some(&expected_ino),
        "Rust snapshot reopen lost mmapped in-order chunks"
    );
    assert_eq!(
        rust_map.get("snap_ooo_metric"),
        Some(&expected_ooo),
        "Rust snapshot reopen lost mmapped OOO chunks"
    );
    rust_db.close().unwrap();

    // 2. Reopen the exact same directory in real Go prometheus/tsdb and assert 100% parity!
    let go_out: VerifyTsdbDirResponse = invoke_oracle(
        "verify-tsdb-dir",
        &VerifyTsdbDirRequest {
            dir: go_dir.path().to_string_lossy().to_string(),
            read_only: false,
        },
    )
    .expect("go-oracle verify-tsdb-dir on Rust snapshot + chunks_head");

    let mut go_map: BTreeMap<String, BTreeMap<i64, f64>> = BTreeMap::new();
    for s in go_out.series {
        let name = s
            .labels
            .iter()
            .find(|l| l.name == "__name__")
            .map(|l| l.value.clone())
            .unwrap_or_default();
        let entry = go_map.entry(name).or_default();
        for sample in s.samples {
            entry.insert(sample.t, sample.v);
        }
    }
    assert_eq!(
        go_map.get("snap_ino_metric"),
        Some(&expected_ino),
        "Go reopen of Rust snapshot + chunks_head mismatch on in-order series"
    );
    assert_eq!(
        go_map.get("snap_ooo_metric"),
        Some(&expected_ooo),
        "Go reopen of Rust snapshot + chunks_head mismatch on OOO series"
    );
}
