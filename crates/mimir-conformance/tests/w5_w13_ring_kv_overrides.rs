//! Wave 13 conformance tests for ring, memberlist, MultiKV, and runtime-config overrides:
//! 1. Live Go-vs-Rust memberlist Join + PushPull with `-memberlist.cluster-label=prod-a`
//!    in BOTH directions (Go dialing Rust AND Rust dialing Go), plus label isolation and >255 rejection.
//! 2. MultiKV -> memberlist secondary zero-stall CAS (F-02) & mid-CAS mirror toggle (F-09).
//! 3. Runtime-config `cost_attribution_trackers` overlay & OTel override inheritance (F-04).

use std::fs;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use mimir_config::ingester::MultiKVConfig;
use mimir_config::server::{MemberlistConfig, RuntimeConfigConfig};
use mimir_config::{
    parse_runtime_config_yaml, CostAttributionLabel, CostAttributionTrackerConfig, GoDuration,
    InstanceLimits, LimitsConfig, NameValidationScheme, OTelTranslationStrategy,
    RuntimeConfigManager,
};
use mimir_conformance::free_test_port;
use mimir_conformance::oracle::{
    ensure_go_oracle_built, ensure_rust_mimir_release_built, go_mimir_bin, rust_mimir_release_bin,
};
use mimir_proto::ringpb::{Desc, InstanceDesc, InstanceState};
use mimir_ring::kv::inmemory::InMemoryKv;
use mimir_ring::kv::memberlist::{MemberlistKv, MemberlistKvClient};
use mimir_ring::kv::multi::{MultiKv, MultiRuntimeConfig, NamedKvClient};
use mimir_ring::kv::{CasOutcome, Codec, KvClient, StringCodec};
use mimir_ring::ring::model::ring_desc_codec;
use prometheus::Registry;

struct Proc {
    name: String,
    child: Option<Child>,
    log: PathBuf,
    http_port: u16,
}

impl Proc {
    fn kill9(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }

    fn log_all(&self) -> String {
        fs::read_to_string(&self.log).unwrap_or_default()
    }

    fn log_tail(&self) -> String {
        let s = self.log_all();
        let lines: Vec<&str> = s.lines().collect();
        lines[lines.len().saturating_sub(40)..].join("\n")
    }
}

impl Drop for Proc {
    fn drop(&mut self) {
        self.kill9();
    }
}

fn ensure_bins() {
    if !go_mimir_bin().exists() {
        ensure_go_oracle_built().expect("go-mimir must build");
    }
    ensure_rust_mimir_release_built().expect("target/release/mimir must build");
}

fn http_get(port: u16, path: &str) -> Option<(u16, String)> {
    let addr = format!("127.0.0.1:{port}").parse().ok()?;
    let mut s = TcpStream::connect_timeout(&addr, Duration::from_secs(2)).ok()?;
    s.set_read_timeout(Some(Duration::from_secs(5))).ok()?;
    write!(s, "GET {path} HTTP/1.0\r\nHost: 127.0.0.1\r\n\r\n").ok()?;
    let mut raw = String::new();
    s.read_to_string(&mut raw).ok()?;
    let (head, body) = raw.split_once("\r\n\r\n")?;
    let status = head
        .lines()
        .next()?
        .split_whitespace()
        .nth(1)?
        .parse::<u16>()
        .ok()?;
    Some((status, body.to_string()))
}

fn members_count(p: &Proc) -> Option<u64> {
    let (status, body) = http_get(p.http_port, "/metrics")?;
    if status != 200 {
        return None;
    }
    body.lines()
        .find(|l| l.starts_with("memberlist_client_cluster_members_count "))
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|v| v.parse::<f64>().ok())
        .map(|v| v as u64)
}

fn wait_until(timeout: Duration, mut f: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if f() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    f()
}

struct NodeSpec<'a> {
    go: bool,
    name: &'a str,
    ml_port: u16,
    join: Vec<String>,
    cluster_label: &'a str,
    extra_yaml: &'a str,
    extra_args: &'a [&'a str],
}

fn spawn_node(dir: &Path, spec: NodeSpec<'_>) -> Proc {
    let http_port = free_test_port();
    let grpc_port = free_test_port();
    let ndir = dir.join(spec.name);
    fs::create_dir_all(ndir.join("tsdb")).unwrap();
    fs::create_dir_all(ndir.join("bucket")).unwrap();
    let join = spec
        .join
        .iter()
        .map(|j| format!("    - {j}\n"))
        .collect::<String>();
    let yaml = format!(
        r#"target: ingester
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http_port}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc_port}
multitenancy_enabled: true
memberlist:
  node_name: {name}
  randomize_node_name: false
  bind_addr: [127.0.0.1]
  bind_port: {ml}
  advertise_addr: 127.0.0.1
  advertise_port: {ml}
  gossip_interval: 100ms
  rejoin_interval: 200ms
  abort_if_cluster_join_fails: false
  join_members:
{join}ingester:
  ring:
    kvstore:
      store: memberlist
    instance_id: {name}
    instance_addr: 127.0.0.1
    num_tokens: 16
    min_ready_duration: 0s
    replication_factor: 1
blocks_storage:
  backend: filesystem
  filesystem:
    dir: {bucket}
  tsdb:
    dir: {tsdb}
    ship_interval: 0s
activity_tracker:
  filepath: ""
{extra_yaml}"#,
        name = spec.name,
        ml = spec.ml_port,
        bucket = ndir.join("bucket").display(),
        tsdb = ndir.join("tsdb").display(),
        extra_yaml = spec.extra_yaml,
    );
    let cfg = ndir.join("mimir.yaml");
    fs::write(&cfg, yaml).unwrap();
    let log = ndir.join("stderr.log");
    let bin = if spec.go {
        go_mimir_bin()
    } else {
        rust_mimir_release_bin()
    };
    let mut cmd = Command::new(&bin);
    cmd.arg(format!("-config.file={}", cfg.display()));
    if !spec.cluster_label.is_empty() {
        cmd.arg(format!("-memberlist.cluster-label={}", spec.cluster_label));
    }
    for a in spec.extra_args {
        cmd.arg(a);
    }
    let child = cmd
        .current_dir(&ndir)
        .stdout(Stdio::null())
        .stderr(fs::File::create(&log).unwrap())
        .spawn()
        .unwrap_or_else(|e| panic!("spawn {}: {e}", bin.display()));
    Proc {
        name: spec.name.to_string(),
        child: Some(child),
        log,
        http_port,
    }
}

fn assert_members(procs: &[&Proc], want: u64, timeout: Duration, what: &str) {
    for p in procs {
        let ok = wait_until(timeout, || members_count(p) == Some(want));
        assert!(
            ok,
            "{what}: {} sees {:?} members, want {want}\n--- {} log ---\n{}",
            p.name,
            members_count(p),
            p.name,
            p.log_tail()
        );
    }
}

#[test]
fn go_vs_rust_memberlist_cluster_label_bidirectional_join_and_push_pull() {
    ensure_bins();
    let tmp = tempfile::tempdir().unwrap();

    // Direction A: Go dialing Rust with -memberlist.cluster-label=prod-a.
    // Prior to F-01 fix, Rust prepended HasLabel (244) on server replies, breaking 100% of Go -> Rust joins.
    let p_rust_seed = free_test_port();
    let p_go_joiner = free_test_port();
    let rust_seed = spawn_node(
        tmp.path(),
        NodeSpec {
            go: false,
            name: "rust-seed",
            ml_port: p_rust_seed,
            join: vec![],
            cluster_label: "prod-a",
            extra_yaml: "",
            extra_args: &[],
        },
    );
    assert_members(&[&rust_seed], 1, Duration::from_secs(30), "rust-seed ready");

    let go_joiner = spawn_node(
        tmp.path(),
        NodeSpec {
            go: true,
            name: "go-joiner",
            ml_port: p_go_joiner,
            join: vec![format!("127.0.0.1:{p_rust_seed}")],
            cluster_label: "prod-a",
            extra_yaml: "",
            extra_args: &[],
        },
    );
    assert_members(
        &[&rust_seed, &go_joiner],
        2,
        Duration::from_secs(30),
        "Go dialing Rust with cluster-label=prod-a",
    );
    for p in [&rust_seed, &go_joiner] {
        let ok = wait_until(Duration::from_secs(20), || {
            http_get(p.http_port, "/ingester/ring")
                .is_some_and(|(st, b)| st == 200 && b.contains("rust-seed") && b.contains("go-joiner"))
        });
        assert!(ok, "{} /ingester/ring missing peer:\n{}", p.name, p.log_tail());
    }
    assert!(
        !go_joiner.log_all().contains("received invalid msgType (244)"),
        "go-joiner hit HasLabel (244) on server reply:\n{}",
        go_joiner.log_tail()
    );
    drop(rust_seed);
    drop(go_joiner);

    // Direction B: Rust dialing Go with -memberlist.cluster-label=prod-a.
    let p_go_seed = free_test_port();
    let p_rust_joiner = free_test_port();
    let go_seed = spawn_node(
        tmp.path(),
        NodeSpec {
            go: true,
            name: "go-seed",
            ml_port: p_go_seed,
            join: vec![],
            cluster_label: "prod-a",
            extra_yaml: "",
            extra_args: &[],
        },
    );
    assert_members(&[&go_seed], 1, Duration::from_secs(30), "go-seed ready");

    let rust_joiner = spawn_node(
        tmp.path(),
        NodeSpec {
            go: false,
            name: "rust-joiner",
            ml_port: p_rust_joiner,
            join: vec![format!("127.0.0.1:{p_go_seed}")],
            cluster_label: "prod-a",
            extra_yaml: "",
            extra_args: &[],
        },
    );
    assert_members(
        &[&go_seed, &rust_joiner],
        2,
        Duration::from_secs(30),
        "Rust dialing Go with cluster-label=prod-a",
    );
    for p in [&go_seed, &rust_joiner] {
        let ok = wait_until(Duration::from_secs(20), || {
            http_get(p.http_port, "/ingester/ring")
                .is_some_and(|(st, b)| st == 200 && b.contains("go-seed") && b.contains("rust-joiner"))
        });
        assert!(ok, "{} /ingester/ring missing peer:\n{}", p.name, p.log_tail());
    }

    // Mismatched label (prod-b) must NOT join prod-a cluster.
    let p_mismatch = free_test_port();
    let rust_mismatch = spawn_node(
        tmp.path(),
        NodeSpec {
            go: false,
            name: "rust-mismatch",
            ml_port: p_mismatch,
            join: vec![format!("127.0.0.1:{p_go_seed}")],
            cluster_label: "prod-b",
            extra_yaml: "",
            extra_args: &[],
        },
    );
    std::thread::sleep(Duration::from_secs(3));
    assert_eq!(members_count(&rust_mismatch), Some(1));
    assert_eq!(members_count(&go_seed), Some(2));
    drop(rust_mismatch);
    drop(rust_joiner);
    drop(go_seed);

    // Startup validation parity: > 255 byte cluster_label must fail startup in both Go and Rust.
    let too_long = "x".repeat(256);
    for go in [true, false] {
        let name = if go { "go-longlabel" } else { "rust-longlabel" };
        let mut p = spawn_node(
            tmp.path(),
            NodeSpec {
                go,
                name,
                ml_port: free_test_port(),
                join: vec![],
                cluster_label: &too_long,
                extra_yaml: "",
                extra_args: &[],
            },
        );
        let mut child = p.child.take().unwrap();
        let deadline = Instant::now() + Duration::from_secs(20);
        let status = loop {
            if let Some(s) = child.try_wait().unwrap() {
                break Some(s);
            }
            if Instant::now() > deadline {
                let _ = child.kill();
                break None;
            }
            std::thread::sleep(Duration::from_millis(100));
        };
        let status = status.unwrap_or_else(|| panic!("{name} did not exit on >255 cluster_label"));
        assert!(!status.success(), "{name} must exit non-zero on >255 cluster_label");
        assert!(
            p.log_all().contains("memberlist-kv"),
            "{name} log missing 'memberlist-kv':\n{}",
            p.log_tail()
        );
        if !go {
            assert!(
                p.log_all().contains("too long"),
                "{name} log missing 'too long':\n{}",
                p.log_tail()
            );
        }
    }
}

#[tokio::test]
async fn multi_kv_memberlist_secondary_zero_stall_cas() {
    let codec: Arc<dyn Codec> = ring_desc_codec();
    let reg = Registry::new();

    let ml_cfg = MemberlistConfig {
        bind_addr: vec!["127.0.0.1".to_string()],
        bind_port: 0,
        node_name: "multi-sec-ml".to_string(),
        randomize_node_name: false,
        ..Default::default()
    };
    let ml = MemberlistKv::start(ml_cfg, vec![Arc::clone(&codec)], "test", Some(&reg))
        .await
        .unwrap();
    let secondary: Arc<dyn KvClient> =
        Arc::new(MemberlistKvClient::new(Arc::clone(&ml), Arc::clone(&codec)).unwrap());
    let primary: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(Arc::clone(&codec)));

    let multi = MultiKv::with_registerer_and_options(
        MultiKVConfig {
            primary: "inmemory".to_string(),
            secondary: "memberlist".to_string(),
            mirror_enabled: true,
            mirror_timeout: GoDuration::from_secs(3),
        },
        vec![
            NamedKvClient {
                name: "inmemory".to_string(),
                client: Arc::clone(&primary),
            },
            NamedKvClient {
                name: "memberlist".to_string(),
                client: Arc::clone(&secondary),
            },
        ],
        Some(Arc::clone(&codec)),
        None,
        None,
    );

    let make_desc = |ts: i64| {
        let mut d = Desc::default();
        d.ingesters.insert(
            "ing-1".to_string(),
            InstanceDesc {
                id: "ing-1".to_string(),
                addr: "127.0.0.1:9095".to_string(),
                timestamp: ts,
                state: InstanceState::Active as i32,
                tokens: vec![1, 2, 3],
                ..Default::default()
            },
        );
        d
    };

    // Pre-seed secondary as if gossip already delivered the identical state.
    let ts = 1_700_000_000;
    secondary
        .cas("ring", Box::new(move |_| Ok(CasOutcome::Update(Box::new(make_desc(ts))))))
        .await
        .unwrap();

    let attempts_before = reg
        .gather()
        .into_iter()
        .find(|mf| mf.get_name().ends_with("memberlist_client_cas_attempt_total"))
        .and_then(|mf| mf.get_metric().first().map(|m| m.get_counter().get_value() as u64))
        .unwrap_or(0);

    // Synchronous MultiKv::cas must NOT stall for mirror_timeout (3s) and must try secondary only once.
    let started = Instant::now();
    multi
        .cas("ring", Box::new(move |_| Ok(CasOutcome::Update(Box::new(make_desc(ts))))))
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_millis(400),
        "MultiKv::cas stalled {elapsed:?} waiting on memberlist secondary no-change"
    );

    let attempts_after = reg
        .gather()
        .into_iter()
        .find(|mf| mf.get_name().ends_with("memberlist_client_cas_attempt_total"))
        .and_then(|mf| mf.get_metric().first().map(|m| m.get_counter().get_value() as u64))
        .unwrap_or(0);
    assert_eq!(
        attempts_after - attempts_before,
        1,
        "secondary write must attempt CAS exactly once (retry=false)"
    );

    // Mid-CAS mirror_enabled false -> true toggle captures and mirrors the written value.
    let sec_toggle: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(Arc::new(StringCodec)));
    let multi_toggle = MultiKv::with_registerer_and_options(
        MultiKVConfig {
            primary: "inmemory".to_string(),
            secondary: "memberlist".to_string(),
            mirror_enabled: false,
            mirror_timeout: GoDuration::from_secs(2),
        },
        vec![
            NamedKvClient {
                name: "inmemory".to_string(),
                client: Arc::new(InMemoryKv::new(Arc::new(StringCodec))),
            },
            NamedKvClient {
                name: "memberlist".to_string(),
                client: Arc::clone(&sec_toggle),
            },
        ],
        Some(Arc::new(StringCodec)),
        None,
        None,
    );
    let mt_cb = Arc::clone(&multi_toggle);
    multi_toggle
        .cas(
            "k",
            Box::new(move |_| {
                mt_cb
                    .apply_runtime_config(MultiRuntimeConfig {
                        primary_store: String::new(),
                        mirroring: Some(true),
                    })
                    .unwrap();
                Ok(CasOutcome::Update(Box::new("mirrored-val".to_string())))
            }),
        )
        .await
        .unwrap();
    let mirrored = sec_toggle
        .get("k")
        .await
        .unwrap()
        .expect("mid-CAS mirror_enabled toggle must mirror value to secondary")
        .downcast::<String>()
        .unwrap();
    assert_eq!(*mirrored, "mirrored-val");

    ml.shutdown().await;
}

#[test]
fn runtime_config_cost_attribution_trackers_and_otel_override_inheritance() {
    ensure_bins();
    let tmp = tempfile::tempdir().unwrap();

    // 1. Unit/library-level verification of global otel_translation_strategy + overlay + Overrides getters.
    let mut defaults = LimitsConfig::default();
    defaults.cost_attribution_trackers.insert(
        "global_base".to_string(),
        CostAttributionTrackerConfig {
            labels: vec![CostAttributionLabel {
                input: "team".to_string(),
                output: "my_team".to_string(),
            }],
            internal: false,
        },
    );
    defaults.additional_cost_attribution_trackers.insert(
        "global_add".to_string(),
        CostAttributionTrackerConfig {
            labels: vec![CostAttributionLabel {
                input: "env".to_string(),
                output: "my_env".to_string(),
            }],
            internal: false,
        },
    );
    defaults.name_validation_scheme = NameValidationScheme::Utf8;
    defaults.otel_metric_suffixes_enabled = Some(true);
    defaults.otel_translation_strategy =
        OTelTranslationStrategy("NoUTF8EscapingWithSuffixes".to_string());

    let rt_yaml = br#"
overrides:
  tenant_a:
    max_global_series_per_user: 12345
    cost_attribution_trackers:
      tenant_base:
        labels:
          - input: service
            output: svc
    additional_cost_attribution_trackers:
      tenant_add:
        labels:
          - input: region
            output: reg
"#;
    let parsed =
        parse_runtime_config_yaml(rt_yaml, &defaults, &InstanceLimits::default()).unwrap();
    let ta = parsed.overrides["tenant_a"].as_ref().unwrap();
    assert!(ta.cost_attribution_trackers.contains_key("global_base"));
    assert!(ta.cost_attribution_trackers.contains_key("tenant_base"));
    assert!(ta.additional_cost_attribution_trackers.contains_key("global_add"));
    assert!(ta.additional_cost_attribution_trackers.contains_key("tenant_add"));

    let mgr = RuntimeConfigManager::new(
        RuntimeConfigConfig::default(),
        defaults,
        InstanceLimits::default(),
    )
    .unwrap();
    mgr.set_values_for_tests(parsed);
    let ov = mgr.overrides();
    assert!(ov.otel_metric_suffixes_enabled("tenant_a"));
    assert_eq!(ov.name_validation_scheme("tenant_a"), NameValidationScheme::Utf8);
    assert_eq!(
        ov.otel_translation_strategy("tenant_a").0,
        "NoUTF8EscapingWithSuffixes"
    );

    // 2. Twin-process Go vs Rust /runtime_config parity with cost_attribution_trackers overlay
    // and OTel defaults inherited by tenant override.
    let rt_file = tmp.path().join("runtime.yaml");
    fs::write(
        &rt_file,
        r#"overrides:
  tenant_a:
    max_global_series_per_user: 12345
    cost_attribution_trackers:
      tenant_base:
        labels:
          - input: service
            output: svc
    additional_cost_attribution_trackers:
      tenant_add:
        labels:
          - input: region
            output: reg
"#,
    )
    .unwrap();

    let extra_yaml = format!(
        r#"runtime_config:
  file: {rt}
limits:
  name_validation_scheme: utf8
  otel_metric_suffixes_enabled: true
  cost_attribution_trackers:
    global_base:
      labels:
        - input: team
          output: my_team
  additional_cost_attribution_trackers:
    global_add:
      labels:
        - input: env
          output: my_env
"#,
        rt = rt_file.display()
    );

    let go_proc = spawn_node(
        tmp.path(),
        NodeSpec {
            go: true,
            name: "go-rc",
            ml_port: free_test_port(),
            join: vec![],
            cluster_label: "",
            extra_yaml: &extra_yaml,
            extra_args: &[],
        },
    );
    let rust_proc = spawn_node(
        tmp.path(),
        NodeSpec {
            go: false,
            name: "rust-rc",
            ml_port: free_test_port(),
            join: vec![],
            cluster_label: "",
            extra_yaml: &extra_yaml,
            extra_args: &[],
        },
    );

    for p in [&go_proc, &rust_proc] {
        let ready = wait_until(Duration::from_secs(30), || {
            http_get(p.http_port, "/ready").is_some_and(|(s, _)| s == 200)
        });
        assert!(ready, "{} failed to become ready:\n{}", p.name, p.log_tail());
    }

    for q in ["", "?mode=diff", "?mode=diff&mode=defaults"] {
        let (gs, g_body) = http_get(go_proc.http_port, &format!("/runtime_config{q}")).unwrap();
        let (rs, r_body) = http_get(rust_proc.http_port, &format!("/runtime_config{q}")).unwrap();
        assert_eq!(gs, 200);
        assert_eq!(rs, 200);
        assert_eq!(
            r_body, g_body,
            "/runtime_config{q} mismatch between Go and Rust"
        );
    }
}
