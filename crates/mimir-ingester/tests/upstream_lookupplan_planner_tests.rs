//! Ported from:
//! - github.com/grafana/mimir/pkg/ingester/lookupplan/planner_factory_test.go
//! - github.com/grafana/mimir/pkg/ingester/lookupplan/planner_test.go
//! - github.com/grafana/mimir/pkg/ingester/lookupplan/plan_test.go
//! - github.com/grafana/mimir/pkg/ingester/lookupplan/predicate_test.go
//! - github.com/grafana/mimir/pkg/ingester/planner_provider_test.go
//!
//! Helpers ported from `lookupplan/csv_test_data_test.go` and `lookupplan/mock_statistics_test.go`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use mimir_ingester::lookupplan::{
    BasicLookupPlan, BlockIndexReader, CostBasedPlanner, CostConfig, LookupPlanner, Metrics,
    NoopPlanner, Plan, PlanPredicate, PlannerFactory, PlannerFactoryTrait, PlannerProvider,
    PlanningBlockMeta, PlanningContext, SelectHints, ShardSelector, Statistics,
    StatisticsGenerator,
};
use mimir_tsdb::matcher::{LabelMatcher, MatchType};
use ulid::Ulid;

// ---------------------------------------------------------------------------
// Helpers: `lookupplan/csv_test_data_test.go`
// ---------------------------------------------------------------------------

/// Go `defaultCostConfig` (`lookupplan/config.go`): note that the sketch cardinality
/// thresholds are left at their zero values upstream.
fn default_cost_config() -> CostConfig {
    CostConfig {
        retrieved_posting_cost: 0.01,
        retrieved_series_cost: 15.0,
        retrieved_posting_list_cost: 10.0,
        min_series_per_block_for_query_planning: 10_000,
        label_cardinality_for_larger_sketch: 0,
        label_cardinality_for_smaller_sketch: 0,
    }
}

/// Reads a `;`-separated CSV file from `tests/testdata`, asserting the header row
/// matches `column_names` and every record has the right number of columns.
fn parse_csv_test_cases(file: &str, column_names: &[&str]) -> Vec<Vec<String>> {
    let path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "tests", "testdata", file]
        .iter()
        .collect();
    let content = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path:?}: {e}"));
    let mut lines = content.lines().filter(|l| !l.is_empty());
    let header: Vec<String> = lines
        .next()
        .expect("csv has header")
        .split(';')
        .map(str::to_string)
        .collect();
    assert_eq!(column_names.iter().map(|s| s.to_string()).collect::<Vec<_>>(), header);
    let mut out = Vec::new();
    for line in lines {
        let record: Vec<String> = line.split(';').map(str::to_string).collect();
        assert_eq!(record.len(), column_names.len(), "bad record: {line}");
        out.push(record);
    }
    out
}

fn parse_matcher_type(op: &str) -> MatchType {
    match op {
        "=" => MatchType::Equal,
        "!=" => MatchType::NotEqual,
        "=~" => MatchType::RegexMatch,
        "!~" => MatchType::RegexNotMatch,
        other => panic!("Unexpected matcher type: {other}"),
    }
}

/// Parses `name<op>"value"` (alertmanager matcher syntax, as used by `parseMatcher`).
fn parse_matcher(m: &str) -> LabelMatcher {
    let (m, rest) = parse_one_matcher(m);
    assert!(rest.trim().is_empty(), "Failed to parse matcher: {m}");
    m
}

/// Parses one `name<op>"quoted value"` and returns the remaining input.
fn parse_one_matcher(s: &str) -> (LabelMatcher, &str) {
    let s = s.trim_start();
    let name_end = s
        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == ':'))
        .unwrap_or_else(|| panic!("Failed to parse matcher: {s}"));
    let name = &s[..name_end];
    assert!(!name.is_empty(), "Failed to parse matcher: {s}");
    let rest = &s[name_end..];
    let op_len = if rest.starts_with("=~") || rest.starts_with("!~") || rest.starts_with("!=") {
        2
    } else if rest.starts_with('=') {
        1
    } else {
        panic!("Failed to parse matcher: {s}");
    };
    let mt = parse_matcher_type(&rest[..op_len]);
    let rest = &rest[op_len..];
    assert!(rest.starts_with('"'), "Failed to parse matcher: {s}");
    let mut value = String::new();
    let mut chars = rest[1..].char_indices();
    let mut end = None;
    while let Some((i, c)) = chars.next() {
        match c {
            '\\' => {
                let (_, n) = chars.next().expect("dangling escape");
                match n {
                    'n' => value.push('\n'),
                    't' => value.push('\t'),
                    '"' | '\\' => value.push(n),
                    other => {
                        // Keep regex escapes verbatim.
                        value.push('\\');
                        value.push(other);
                    }
                }
            }
            '"' => {
                end = Some(i);
                break;
            }
            c => value.push(c),
        }
    }
    let end = end.expect("unterminated quoted value");
    let m = LabelMatcher::new(mt, name, value).expect("valid matcher");
    (m, &rest[1 + end + 1..])
}

/// Parses `{a="b",c=~"d"}` selectors (as used by `parseVectorSelector`).
fn parse_vector_selector(s: &str) -> Vec<LabelMatcher> {
    let s = s.trim();
    let inner = s
        .strip_prefix('{')
        .and_then(|x| x.strip_suffix('}'))
        .unwrap_or_else(|| panic!("unsupported selector: {s}"));
    let mut out = Vec::new();
    let mut rest = inner.trim();
    while !rest.is_empty() {
        let (m, r) = parse_one_matcher(rest);
        out.push(m);
        rest = r.trim_start();
        if let Some(r) = rest.strip_prefix(',') {
            rest = r.trim_start();
        } else {
            assert!(rest.is_empty(), "unexpected trailing input: {rest}");
        }
    }
    out
}

fn matchers_strings(ms: &[LabelMatcher]) -> Vec<String> {
    ms.iter().map(|m| m.to_string()).collect()
}

fn assert_elements_match(expected: Vec<String>, actual: Vec<String>, msg: &str) {
    let mut e = expected;
    let mut a = actual;
    e.sort();
    a.sort();
    assert_eq!(e, a, "{msg}");
}

/// Go `sharding.ParseShardIDLabelValue`.
fn parse_shard_id_label_value(val: &str) -> Result<(u64, u64), String> {
    let parts: Vec<&str> = val.split('_').collect();
    if parts.len() != 3 || parts[1] != "of" {
        return Err(format!("invalid shard ID: {val:?}"));
    }
    let index: u64 = parts[0].parse().map_err(|e| format!("invalid shard ID: {val:?}: {e}"))?;
    let count: u64 = parts[2].parse().map_err(|e| format!("invalid shard ID: {val:?}: {e}"))?;
    if index == 0 || count == 0 || index > count {
        return Err(format!("invalid shard ID: {val:?}"));
    }
    Ok((index - 1, count))
}

// ---------------------------------------------------------------------------
// Helpers: `lookupplan/mock_statistics_test.go`
// ---------------------------------------------------------------------------

struct MockStatistics {
    /// label name -> label value -> number of series
    series_per_value: HashMap<String, HashMap<String, u64>>,
    total_series: u64,
}

impl Statistics for MockStatistics {
    fn total_series(&self) -> u64 {
        self.total_series
    }

    fn label_values_count(&self, name: &str) -> u64 {
        self.series_per_value
            .get(name)
            .map_or(0, |v| v.values().filter(|&&c| c > 0).count() as u64)
    }

    fn label_values_cardinality(&self, name: &str, values: &[&str]) -> u64 {
        let Some(label_values) = self.series_per_value.get(name) else {
            return 0;
        };
        if values.is_empty() {
            return label_values.values().sum();
        }
        values.iter().filter_map(|v| label_values.get(*v)).sum()
    }
}

fn vmap(entries: &[(&str, u64)]) -> HashMap<String, u64> {
    entries.iter().map(|(k, v)| (k.to_string(), *v)).collect()
}

fn new_mock_statistics() -> MockStatistics {
    let mut m = HashMap::new();
    m.insert(
        "__name__".to_string(),
        vmap(&[
            ("http_requests_total", 1000),
            ("cpu_usage_percent", 500),
            ("memory_usage_bytes", 300),
            ("disk_io_operations", 200),
            ("network_bytes_sent", 150),
        ]),
    );
    m.insert(
        "job".to_string(),
        vmap(&[("prometheus", 800), ("grafana", 600), ("alertmanager", 400), ("node", 300)]),
    );
    m.insert(
        "instance".to_string(),
        vmap(&[
            ("localhost:9090", 200),
            ("localhost:3000", 150),
            ("localhost:9093", 100),
            ("localhost:9100", 80),
            ("prod-server-1", 300),
            ("prod-server-2", 250),
            ("prod-server-3", 200),
        ]),
    );
    m.insert("status".to_string(), vmap(&[("200", 800), ("404", 300), ("500", 100)]));
    m.insert(
        "method".to_string(),
        vmap(&[("GET", 600), ("POST", 300), ("PUT", 150), ("DELETE", 50)]),
    );
    MockStatistics {
        series_per_value: m,
        total_series: 2100,
    }
}

fn new_high_cardinality_mock_statistics() -> MockStatistics {
    let mut s: HashMap<String, HashMap<String, u64>> = HashMap::new();

    s.insert(
        "__name__".into(),
        vmap(&[
            ("http_requests_total", 800000),
            ("cpu_usage_percent", 650000),
            ("memory_usage_bytes", 500000),
            ("disk_io_operations", 400000),
            ("network_bytes_sent", 350000),
            ("network_bytes_received", 350000),
            ("process_resident_memory", 300000),
            ("process_virtual_memory", 300000),
            ("go_memstats_alloc_bytes", 250000),
            ("go_memstats_heap_objects", 250000),
            ("prometheus_config_reload", 200000),
            ("test_metric_small", 50),
            ("debug_counter", 25),
            ("health_check", 10),
            ("startup_duration", 5),
        ]),
    );

    s.insert(
        "job".into(),
        vmap(&[
            ("prometheus", 1200000),
            ("grafana", 800000),
            ("alertmanager", 600000),
            ("node-exporter", 400000),
            ("blackbox", 300000),
            ("pushgateway", 250000),
            ("cortex", 200000),
            ("loki", 180000),
            ("tempo", 150000),
            ("mimir", 120000),
            ("test-runner", 100),
            ("backup-service", 75),
            ("log-shipper", 50),
            ("health-checker", 25),
            ("config-reloader", 15),
        ]),
    );

    let mut instance_map = HashMap::new();
    let regions = ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"];
    let azs = ["a", "b", "c"];
    let host_types = ["web", "api", "worker", "db", "cache", "monitor"];
    let series_per_instance = 50u64;
    for region in regions {
        for az in azs {
            for host_type in host_types {
                for i in 1..=200 {
                    instance_map.insert(format!("{host_type}-{region}-{az}-{i:03}"), series_per_instance);
                }
            }
        }
    }
    s.insert("instance".into(), instance_map);

    s.insert(
        "status".into(),
        vmap(&[
            ("200", 2000000),
            ("404", 800000),
            ("500", 400000),
            ("502", 300000),
            ("503", 250000),
            ("401", 200000),
            ("403", 150000),
            ("400", 120000),
            ("429", 100000),
            ("301", 80000),
            ("302", 70000),
            ("204", 60000),
        ]),
    );

    s.insert(
        "method".into(),
        vmap(&[
            ("GET", 2500000),
            ("POST", 1200000),
            ("PUT", 600000),
            ("DELETE", 300000),
            ("PATCH", 200000),
            ("HEAD", 150000),
            ("OPTIONS", 100000),
        ]),
    );

    let mut pods = generate_pod_names(1000, 800);
    pods.insert("backup-controller-abcdefghij-12345".into(), 25);
    pods.insert("config-manager-klmnopqrst-67890".into(), 20);
    pods.insert("health-monitor-uvwxyzabcd-54321".into(), 15);
    pods.insert("log-aggregator-efghijklmn-98765".into(), 10);
    pods.insert("test-runner-opqrstuvwx-11111".into(), 5);
    s.insert("pod".into(), pods);

    s.insert(
        "container".into(),
        vmap(&[
            ("prometheus", 600000),
            ("grafana", 400000),
            ("alertmanager", 300000),
            ("node-exporter", 250000),
            ("nginx", 200000),
            ("redis", 180000),
            ("postgres", 150000),
            ("elasticsearch", 120000),
            ("kibana", 100000),
            ("fluentd", 80000),
            ("istio-proxy", 200),
            ("jaeger-agent", 150),
            ("config-reloader", 100),
            ("log-shipper", 75),
            ("health-check", 50),
            ("init-container", 25),
        ]),
    );

    s.insert(
        "namespace".into(),
        vmap(&[
            ("monitoring", 1_500_000),
            ("default", 1_200_000),
            ("kube-system", 800000),
            ("logging", 600000),
            ("ingress", 400000),
            ("cert-manager", 300000),
            ("prometheus", 250000),
            ("grafana", 200000),
            ("testing", 300),
            ("staging", 250),
            ("development", 200),
            ("backup", 100),
            ("security", 75),
            ("admin", 50),
        ]),
    );

    MockStatistics {
        series_per_value: s,
        total_series: 5_000_000,
    }
}

fn generate_pod_names(num_deployments: usize, series_per_pod: u64) -> HashMap<String, u64> {
    let mut pod_map = HashMap::new();
    let deployments = [
        "prometheus", "grafana", "alertmanager", "node-exporter", "blackbox-exporter",
        "pushgateway", "nginx-ingress", "cert-manager", "external-dns", "cluster-autoscaler",
        "fluentd", "elasticsearch", "kibana", "redis", "postgres", "mysql", "mongodb",
        "rabbitmq", "kafka", "zookeeper", "consul", "vault", "etcd", "coredns",
        "api-gateway", "user-service", "auth-service", "payment-service", "notification-service",
    ];
    for i in 0..num_deployments {
        let mut deployment = deployments[i % deployments.len()].to_string();
        if i >= deployments.len() {
            deployment = format!("{}-{}", deployment, i / deployments.len());
        }
        let replicas = (i % 5) + 1;
        for j in 0..replicas {
            let pod_name = format!(
                "{}-{}-{}",
                deployment,
                generate_random_string(i, 10),
                generate_random_string(j, 5)
            );
            pod_map.insert(pod_name, series_per_pod);
        }
    }
    pod_map
}

fn generate_random_string(mut seed: usize, length: usize) -> String {
    let chars = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let mut result = Vec::with_capacity(length);
    for _ in 0..length {
        result.push(chars[(seed * 7 + length * 3) % chars.len()]);
        seed += 1;
    }
    String::from_utf8(result).unwrap()
}

fn new_single_value_statistics() -> MockStatistics {
    let mut m = HashMap::new();
    m.insert("label".to_string(), vmap(&[("value", 10)]));
    m.insert(
        "__name__".to_string(),
        vmap(&[
            ("a", 1), ("b", 1), ("c", 1), ("d", 1), ("e", 1),
            ("f", 1), ("g", 1), ("h", 1), ("i", 1), ("j", 1),
        ]),
    );
    MockStatistics {
        series_per_value: m,
        total_series: 10,
    }
}

fn eq_matcher(name: &str, value: &str) -> LabelMatcher {
    LabelMatcher::new(MatchType::Equal, name, value).unwrap()
}

fn matcher(mt: MatchType, name: &str, value: &str) -> LabelMatcher {
    LabelMatcher::new(mt, name, value).unwrap()
}

// ---------------------------------------------------------------------------
// planner_factory_test.go
// ---------------------------------------------------------------------------

/// Mock `BlockIndexReader` with `num_series` series `{__name__="test_metric", instance="<i>"}`
/// (mirrors `createMockIndexReaderWithSeries`).
struct MockIndexReader {
    num_series: u64,
}

impl BlockIndexReader for MockIndexReader {
    fn all_postings_count(&self) -> Result<u64, String> {
        Ok(self.num_series)
    }
    fn label_names(&self) -> Result<Vec<String>, String> {
        if self.num_series == 0 {
            return Ok(vec![]);
        }
        Ok(vec!["__name__".into(), "instance".into()])
    }
    fn label_values(&self, name: &str) -> Result<Vec<String>, String> {
        if self.num_series == 0 {
            return Ok(vec![]);
        }
        match name {
            "__name__" => Ok(vec!["test_metric".into()]),
            "instance" => Ok((0..self.num_series).map(|i| i.to_string()).collect()),
            _ => Ok(vec![]),
        }
    }
    fn postings_for_all_values_count(&self, name: &str) -> Result<u64, String> {
        match name {
            "__name__" | "instance" => Ok(self.num_series),
            _ => Ok(0),
        }
    }
    fn postings_count(&self, name: &str, value: &str) -> Result<u64, String> {
        match (name, value) {
            ("__name__", "test_metric") => Ok(self.num_series),
            ("instance", v) => Ok(u64::from(v.parse::<u64>().is_ok_and(|i| i < self.num_series))),
            _ => Ok(0),
        }
    }
}

/// Mock `BlockIndexReader` that always returns errors (`errorIndexReader`).
struct ErrorIndexReader;

impl BlockIndexReader for ErrorIndexReader {
    fn all_postings_count(&self) -> Result<u64, String> {
        Err("mock error from Postings".into())
    }
    fn label_names(&self) -> Result<Vec<String>, String> {
        Err("mock error from LabelNames".into())
    }
    fn label_values(&self, _name: &str) -> Result<Vec<String>, String> {
        Err("mock error from LabelValues".into())
    }
    fn postings_for_all_values_count(&self, _name: &str) -> Result<u64, String> {
        Err("mock error from PostingsForAllLabelValues".into())
    }
    fn postings_count(&self, _name: &str, _value: &str) -> Result<u64, String> {
        Err("mock error from Postings".into())
    }
}

// Upstream: pkg/ingester/lookupplan/planner_factory_test.go::TestPlannerFactory_CreatePlanner
#[test]
fn test_planner_factory_create_planner() {
    let cfg = default_cost_config();
    let metrics = Metrics::new(None);
    let stats_generator = StatisticsGenerator::new();
    let factory = PlannerFactory::new(metrics, stats_generator, cfg);
    let min = cfg.min_series_per_block_for_query_planning;

    const NOOP: &str = "NoopPlanner";
    const COST: &str = "CostBasedPlanner";

    let cases: Vec<(&str, u64, Box<dyn BlockIndexReader>, &str)> = vec![
        ("small block returns NoopPlanner", 5000, Box::new(MockIndexReader { num_series: 5000 }), NOOP),
        ("very small block returns NoopPlanner", 1, Box::new(MockIndexReader { num_series: 1 }), NOOP),
        ("empty block returns NoopPlanner", 0, Box::new(MockIndexReader { num_series: 0 }), NOOP),
        (
            "boundary case just below threshold returns NoopPlanner",
            min - 1,
            Box::new(MockIndexReader { num_series: min - 1 }),
            NOOP,
        ),
        (
            "large block with working IndexReader returns CostBasedPlanner",
            15000,
            Box::new(MockIndexReader { num_series: 15000 }),
            COST,
        ),
        (
            "exactly at threshold with working IndexReader returns CostBasedPlanner",
            min,
            Box::new(MockIndexReader { num_series: min }),
            COST,
        ),
        (
            "very large block with working IndexReader returns CostBasedPlanner",
            100000,
            Box::new(MockIndexReader { num_series: 100000 }),
            COST,
        ),
        (
            "large block with error IndexReader fallbacks to NoopPlanner",
            50000,
            Box::new(ErrorIndexReader),
            NOOP,
        ),
        (
            "exactly at threshold with error IndexReader fallbacks to NoopPlanner",
            min,
            Box::new(ErrorIndexReader),
            NOOP,
        ),
        (
            "very large block with working IndexReader returns CostBasedPlanner",
            1000000,
            Box::new(MockIndexReader { num_series: 1000000 }),
            COST,
        ),
    ];

    for (name, num_series, reader, expected) in cases {
        let meta = PlanningBlockMeta {
            ulid: Ulid::default(),
            num_series,
        };
        let planner = factory.create_planner(&meta, reader.as_ref());
        assert_eq!(expected, planner.planner_kind(), "{name}");
    }
}

// ---------------------------------------------------------------------------
// planner_test.go
// ---------------------------------------------------------------------------

struct PlannerTestCase {
    name: String,
    input_matchers: Vec<LabelMatcher>,
    expected_index_matchers: Vec<LabelMatcher>,
    expected_scan_matchers: Vec<LabelMatcher>,
    query_shard: String,
}

fn hints_for_shard(query_shard: &str) -> Option<SelectHints> {
    if query_shard.is_empty() {
        return None;
    }
    let (shard_index, shard_count) = parse_shard_id_label_value(query_shard)
        .unwrap_or_else(|e| panic!("failed to parse queryShard: {query_shard:?}: {e}"));
    Some(SelectHints {
        shard_index,
        shard_count,
    })
}

fn new_planner(stats: MockStatistics) -> CostBasedPlanner {
    CostBasedPlanner::new(
        Metrics::new(None).for_user("test-user"),
        Arc::new(stats),
        default_cost_config(),
    )
}

// Upstream: pkg/ingester/lookupplan/planner_test.go::TestCostBasedPlannerPlanIndexLookup
#[test]
fn test_cost_based_planner_plan_index_lookup() {
    let records = parse_csv_test_cases(
        "planner_test_cases.csv",
        &["queryShard", "testName", "inputMatchers", "expectedIndexMatchers", "expectedScanMatchers"],
    );
    let test_cases: Vec<PlannerTestCase> = records
        .iter()
        .map(|r| PlannerTestCase {
            query_shard: r[0].clone(),
            name: r[1].clone(),
            input_matchers: parse_vector_selector(&r[2]),
            expected_index_matchers: parse_vector_selector(&r[3]),
            expected_scan_matchers: parse_vector_selector(&r[4]),
        })
        .collect();
    assert_eq!(67, test_cases.len());

    let planner = new_planner(new_high_cardinality_mock_statistics());

    for tc in &test_cases {
        let ctx = format!("{} (shard {:?})", tc.name, tc.query_shard);
        let input_plan = BasicLookupPlan::new(tc.input_matchers.clone(), vec![]);
        let hints = hints_for_shard(&tc.query_shard);

        let result = planner
            .plan_index_lookup(&input_plan, hints.as_ref())
            .unwrap_or_else(|e| panic!("{ctx}: {e}"));

        assert!(
            !tc.expected_index_matchers.is_empty(),
            "{ctx}: PostingsForMatchers doesn't support empty index matchers, your test case is wrong"
        );

        assert_elements_match(
            matchers_strings(&tc.expected_index_matchers),
            matchers_strings(result.index_matchers()),
            &format!("{ctx}: index matchers"),
        );
        assert_elements_match(
            matchers_strings(&tc.expected_scan_matchers),
            matchers_strings(result.scan_matchers()),
            &format!("{ctx}: scan matchers"),
        );

        let mut all_actual = matchers_strings(result.index_matchers());
        all_actual.extend(matchers_strings(result.scan_matchers()));
        assert_elements_match(
            matchers_strings(&tc.input_matchers),
            all_actual,
            &format!("{ctx}: all input matchers accounted for"),
        );
    }
}

// Upstream: pkg/ingester/lookupplan/planner_test.go::TestCostBasedPlannerWithManyMatchers
#[test]
fn test_cost_based_planner_with_many_matchers() {
    let planner = new_planner(new_high_cardinality_mock_statistics());

    let matchers: Vec<LabelMatcher> = (0..1000)
        .map(|i| eq_matcher(&format!("label_{i}"), &format!("value_{i}")))
        .collect();
    let input_plan = BasicLookupPlan::new(matchers.clone(), vec![]);

    let start = Instant::now();
    let result = planner.plan_index_lookup(&input_plan, None).unwrap();
    let elapsed = start.elapsed();

    assert!(
        elapsed.as_secs_f64() < 30.0,
        "Planning with 1000 matchers should complete in less than 30 seconds, took {elapsed:?}"
    );

    let mut all_result = matchers_strings(result.index_matchers());
    all_result.extend(matchers_strings(result.scan_matchers()));
    assert_elements_match(matchers_strings(&matchers), all_result, "all matchers are preserved");
    assert!(!result.index_matchers().is_empty(), "Result should have index matchers");
}

// Upstream: pkg/ingester/lookupplan/planner_test.go::TestCostBasedPlannerPreservesAllMatchers
#[test]
fn test_cost_based_planner_preserves_all_matchers() {
    let planner = new_planner(new_high_cardinality_mock_statistics());

    // mixed_index_and_scan_matchers
    {
        let index_matchers = vec![
            eq_matcher("__name__", "http_requests_total"),
            eq_matcher("method", "GET"),
        ];
        let scan_matchers = vec![
            eq_matcher("status", "200"),
            matcher(MatchType::RegexMatch, "instance", "web-.*"),
        ];
        let input_plan = BasicLookupPlan::new(index_matchers.clone(), scan_matchers.clone());

        let result = planner.plan_index_lookup(&input_plan, None).unwrap();

        let mut all_original = matchers_strings(&index_matchers);
        all_original.extend(matchers_strings(&scan_matchers));
        let mut all_result = matchers_strings(result.index_matchers());
        all_result.extend(matchers_strings(result.scan_matchers()));

        assert_elements_match(
            all_original,
            all_result,
            "Planner should preserve all matchers, just potentially repartition them",
        );
        assert!(!result.index_matchers().is_empty(), "Result should have index matchers");
    }

    // scan_only_input_gets_optimized
    {
        let scan_only = vec![
            eq_matcher("__name__", "cpu_usage_percent"),
            eq_matcher("job", "prometheus"),
            eq_matcher("method", "POST"),
        ];
        let input_plan = BasicLookupPlan::new(vec![], scan_only.clone());

        let result = planner.plan_index_lookup(&input_plan, None).unwrap();

        let mut all_result = matchers_strings(result.index_matchers());
        all_result.extend(matchers_strings(result.scan_matchers()));
        assert_elements_match(
            matchers_strings(&scan_only),
            all_result,
            "Planner should preserve all matchers from scan-only input",
        );
    }
}

// Upstream: pkg/ingester/lookupplan/planner_test.go::TestCostBasedPlannerPrefersIndexMatchersOverCheapestPlan
#[test]
fn test_cost_based_planner_prefers_index_matchers_over_cheapest_plan() {
    let planner = new_planner(new_single_value_statistics());

    let input_plan = BasicLookupPlan::new(vec![eq_matcher("label", "value")], vec![]);

    let result = planner.plan_index_lookup(&input_plan, None).unwrap();
    assert!(
        !result.index_matchers().is_empty(),
        "Result should have index matchers despite potentially higher cost"
    );
}

// Upstream: pkg/ingester/lookupplan/planner_test.go::TestCostBasedPlannerDoesntAllowNoMatcherLookups
#[test]
fn test_cost_based_planner_doesnt_allow_no_matcher_lookups() {
    let planner = new_planner(new_mock_statistics());

    let result = planner.plan_index_lookup(&BasicLookupPlan::default(), None);
    let err = result.expect_err("Result should be an error when no matchers are provided");
    assert!(
        err.contains("no plan with index matchers found"),
        "unexpected error: {err}"
    );
}

// Upstream: pkg/ingester/lookupplan/planner_test.go::TestCostBasedPlannerWithDisabledPlanning
#[test]
fn test_cost_based_planner_with_disabled_planning() {
    let planner = new_planner(new_high_cardinality_mock_statistics());
    let ctx = PlanningContext::with_disabled_planning();

    let input_plan = BasicLookupPlan::new(
        vec![
            eq_matcher("__name__", "cpu_usage_percent"),
            eq_matcher("job", "prometheus"),
            // this would normally be set as a scan matcher since it doesn't exist
            matcher(MatchType::NotEqual, "non_existent", ""),
        ],
        vec![],
    );

    // disabled_planning_returns_input_plan
    {
        let result = planner
            .plan_index_lookup_with_context(&input_plan, None, Some(&ctx))
            .unwrap();
        assert_eq!(input_plan, result);
        assert_eq!(
            matchers_strings(input_plan.index_matchers()),
            matchers_strings(result.index_matchers())
        );
        assert_eq!(
            matchers_strings(input_plan.scan_matchers()),
            matchers_strings(result.scan_matchers())
        );
    }

    // disabled_planning_works_with_mixed_matchers
    {
        let mixed_plan = BasicLookupPlan::new(
            vec![eq_matcher("__name__", "cpu_usage_percent")],
            vec![
                eq_matcher("job", "prometheus"),
                matcher(MatchType::RegexMatch, "instance", "localhost.*"),
            ],
        );
        let result = planner
            .plan_index_lookup_with_context(&mixed_plan, None, Some(&ctx))
            .unwrap();
        assert_eq!(mixed_plan, result);
        assert_eq!(
            matchers_strings(mixed_plan.index_matchers()),
            matchers_strings(result.index_matchers())
        );
        assert_eq!(
            matchers_strings(mixed_plan.scan_matchers()),
            matchers_strings(result.scan_matchers())
        );
    }

    // disabled_planning_works_with_empty_plan
    {
        let empty_plan = BasicLookupPlan::new(vec![], vec![]);
        let result = planner
            .plan_index_lookup_with_context(&empty_plan, None, Some(&ctx))
            .unwrap();
        assert_eq!(empty_plan, result);
    }
}

// ---------------------------------------------------------------------------
// plan_test.go
// ---------------------------------------------------------------------------

// Upstream: pkg/ingester/lookupplan/plan_test.go::TestPlanCosts
#[test]
fn test_plan_costs() {
    let stats = new_mock_statistics();
    let records = parse_csv_test_cases(
        "plan_cost_test_cases.csv",
        &[
            "testName",
            "indexMatchers",
            "scanMatchers",
            "cardinality",
            "indexCost",
            "intersectionCost",
            "seriesRetrievalCost",
            "filterCost",
            "totalCost",
        ],
    );
    assert_eq!(45, records.len());

    const MSG: &str = "Expected cost doesn't match actual cost. If you want to keep the new cost, update /testdata";
    const DELTA: f64 = 1e-5;
    let in_delta = |expected: f64, actual: f64, what: &str, name: &str| {
        assert!(
            (expected - actual).abs() <= DELTA,
            "{name}: {what}: expected {expected}, got {actual}. {MSG}"
        );
    };

    for r in &records {
        let name = &r[0];
        let index_matchers = parse_vector_selector(&r[1]);
        let scan_matchers = parse_vector_selector(&r[2]);
        let cardinality: u64 = r[3].parse().unwrap();
        let index_cost: f64 = r[4].parse().unwrap();
        let intersection_cost: f64 = r[5].parse().unwrap();
        let series_retrieval_cost: f64 = r[6].parse().unwrap();
        let filter_cost: f64 = r[7].parse().unwrap();
        let total_cost: f64 = r[8].parse().unwrap();

        let mut all_matchers = index_matchers.clone();
        all_matchers.extend(scan_matchers.clone());

        // Create a scan-only plan with all matchers (no sharding)
        let mut p = Plan::new_scan_only_plan(&stats, default_cost_config(), &all_matchers, None);
        // Use index for the first N predicates (corresponding to index matchers)
        for i in 0..index_matchers.len() {
            p = p.use_index_for(i);
        }

        assert_eq!(cardinality, p.final_cardinality(), "{name}: cardinality. {MSG}");
        in_delta(index_cost, p.index_lookup_cost(), "indexCost", name);
        in_delta(intersection_cost, p.intersection_cost(), "intersectionCost", name);
        in_delta(series_retrieval_cost, p.series_retrieval_cost(), "seriesRetrievalCost", name);
        in_delta(filter_cost, p.filter_cost(), "filterCost", name);
        in_delta(total_cost, p.total_cost(), "totalCost", name);

        assert!(index_cost >= 0.0, "can't have negative costs");
        assert!(intersection_cost >= 0.0, "can't have negative costs");
        assert!(series_retrieval_cost >= 0.0, "can't have negative costs");
        assert!(filter_cost >= 0.0, "can't have negative costs");
        assert!(total_cost >= 0.0, "can't have negative costs");
    }
}

// ---------------------------------------------------------------------------
// predicate_test.go
// ---------------------------------------------------------------------------

// Upstream: pkg/ingester/lookupplan/predicate_test.go::TestFilterCost
#[test]
fn test_filter_cost() {
    let stats = new_mock_statistics();
    let records = parse_csv_test_cases(
        "filter_cost_test_cases.csv",
        &["matcher", "seriesCount", "expectedCost"],
    );
    assert_eq!(22, records.len());

    for r in &records {
        let m = parse_matcher(&r[0]);
        let series_count: u64 = r[1].parse().unwrap();
        let expected_cost: f64 = r[2].parse().unwrap();
        let test_name = format!("{series_count}_series_{m}");

        let pred = PlanPredicate::new(m, &stats, default_cost_config());
        let cost = pred.filter_cost(series_count);
        assert!(cost >= 0.0, "{test_name}: can't have negative costs");
        assert_eq!(expected_cost, cost, "{test_name}");
    }
}

// Upstream: pkg/ingester/lookupplan/predicate_test.go::TestIndexLookupCost
#[test]
fn test_index_lookup_cost() {
    let stats = new_mock_statistics();
    let records = parse_csv_test_cases("index_lookup_cost_test_cases.csv", &["matcher", "expectedCost"]);
    assert_eq!(19, records.len());

    for r in &records {
        let m = parse_matcher(&r[0]);
        let expected_cost: f64 = r[1].parse().unwrap();
        let name = m.to_string();

        let pred = PlanPredicate::new(m, &stats, default_cost_config());
        let actual_cost = pred.index_lookup_cost();
        assert!(actual_cost >= 0.0, "{name}: can't have negative cost");
        assert_eq!(
            expected_cost, actual_cost,
            "{name}: Expected cost doesn't match actual cost"
        );
    }
}

// Upstream: pkg/ingester/lookupplan/predicate_test.go::TestCardinalityEstimation
#[test]
fn test_cardinality_estimation() {
    let stats = new_mock_statistics();
    let records = parse_csv_test_cases(
        "cardinality_estimation_test_cases.csv",
        &["matcher", "expectedCardinality", "deltaTolerance"],
    );
    assert_eq!(10, records.len());

    for r in &records {
        let m = parse_matcher(&r[0]);
        let expected: u64 = r[1].parse().unwrap();
        let tolerance: f64 = r[2].parse().unwrap();
        let name = m.to_string();

        let pred = PlanPredicate::new(m, &stats, default_cost_config());
        let actual = pred.cardinality;

        if tolerance > 0.0 {
            assert!(
                (expected as f64 - actual as f64).abs() <= tolerance,
                "{name}: expected {expected} within {tolerance}, got {actual}"
            );
        } else {
            assert_eq!(
                expected, actual,
                "{name}: Expected cardinality doesn't match actual cardinality"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// planner_provider_test.go
// ---------------------------------------------------------------------------

/// Hand-written mock implementation of the planner factory (`mockPlannerFactory`).
struct MockPlannerFactory {
    #[allow(clippy::type_complexity)]
    create_planner_func: Box<dyn Fn(&PlanningBlockMeta) -> Arc<dyn LookupPlanner> + Send + Sync>,
}

impl PlannerFactoryTrait for MockPlannerFactory {
    fn create_planner(
        &self,
        meta: &PlanningBlockMeta,
        _reader: &dyn BlockIndexReader,
    ) -> Arc<dyn LookupPlanner> {
        (self.create_planner_func)(meta)
    }
}

fn same_planner(a: &Arc<dyn LookupPlanner>, b: &Arc<dyn LookupPlanner>) -> bool {
    std::ptr::eq(Arc::as_ptr(a) as *const (), Arc::as_ptr(b) as *const ())
}

fn provider_fixture() -> (PlannerProvider, Arc<dyn LookupPlanner>, Arc<AtomicUsize>, PlanningBlockMeta) {
    let block_meta = PlanningBlockMeta {
        ulid: Ulid::from_parts(1, 0),
        num_series: 15000,
    };
    let expected: Arc<dyn LookupPlanner> = Arc::new(CostBasedPlanner::new(
        Metrics::new(None),
        Arc::new(new_mock_statistics()),
        default_cost_config(),
    ));
    let call_count = Arc::new(AtomicUsize::new(0));
    let factory = MockPlannerFactory {
        create_planner_func: Box::new({
            let expected = Arc::clone(&expected);
            let call_count = Arc::clone(&call_count);
            move |_meta| {
                call_count.fetch_add(1, Ordering::SeqCst);
                Arc::clone(&expected)
            }
        }),
    };
    (
        PlannerProvider::new(Arc::new(factory)),
        expected,
        call_count,
        block_meta,
    )
}

// Upstream: pkg/ingester/planner_provider_test.go::TestPlannerProvider_getPlanner_DoesNotCachePlanners
#[test]
fn test_planner_provider_get_planner_does_not_cache_planners() {
    let (provider, expected, call_count, block_meta) = provider_fixture();
    let reader = ErrorIndexReader;

    let result = provider.get_planner(&block_meta, &reader);
    assert!(same_planner(&expected, &result), "should return planner from factory");

    let result = provider.get_planner(&block_meta, &reader);
    assert!(same_planner(&expected, &result), "should return planner from factory");

    assert_eq!(
        2,
        call_count.load(Ordering::SeqCst),
        "CreatePlanner should be called twice (no caching)"
    );
}

// Upstream: pkg/ingester/planner_provider_test.go::TestPlannerProvider_generateAndStorePlanner_CachesPlanners
#[test]
fn test_planner_provider_generate_and_store_planner_caches_planners() {
    let (provider, expected, call_count, block_meta) = provider_fixture();
    let reader = ErrorIndexReader;

    provider.generate_and_store_planner(&block_meta, &reader);
    let result = provider.get_planner(&block_meta, &reader);

    assert!(same_planner(&expected, &result), "should return cached planner");
    assert_eq!(
        1,
        call_count.load(Ordering::SeqCst),
        "CreatePlanner should be called only once (planner is cached)"
    );
}

// Silence unused-import warnings for items only referenced to document parity with upstream.
#[allow(dead_code)]
fn _api_surface(_: NoopPlanner, _: ShardSelector) {}
