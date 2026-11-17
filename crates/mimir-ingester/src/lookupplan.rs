//! Cost-based index lookup planning, count-min sketch block statistics, query stats, and planner provider.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/ingester/lookupplan/config.go
//! - github.com/grafana/mimir/pkg/ingester/lookupplan/metrics.go
//! - github.com/grafana/mimir/pkg/ingester/lookupplan/statistics.go
//! - github.com/grafana/mimir/pkg/ingester/lookupplan/stats.go
//! - github.com/grafana/mimir/pkg/ingester/lookupplan/predicate.go
//! - github.com/grafana/mimir/pkg/ingester/lookupplan/plan.go
//! - github.com/grafana/mimir/pkg/ingester/lookupplan/branch_and_bound.go
//! - github.com/grafana/mimir/pkg/ingester/lookupplan/planner.go
//! - github.com/grafana/mimir/pkg/ingester/lookupplan/planner_factory.go
//! - github.com/grafana/mimir/pkg/ingester/lookupplan/query_stats.go
//! - github.com/grafana/mimir/pkg/ingester/planner_provider.go
//! - github.com/prometheus/prometheus/model/labels/cost.go
//! - github.com/tylertreat/BoomFilters/countmin.go

use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::Instant;

use mimir_tsdb::index::IndexReader;
use mimir_tsdb::matcher::{LabelMatcher, MatchType};
use parking_lot::RwLock;
use prometheus::{HistogramOpts, HistogramVec, Registry};
use ulid::Ulid;

// ---------------------------------------------------------------------------
// CostConfig (`lookupplan/config.go`)
// ---------------------------------------------------------------------------

pub const DEFAULT_RETRIEVED_POSTING_COST: f64 = 0.01;
pub const DEFAULT_RETRIEVED_SERIES_COST: f64 = 15.0;
pub const DEFAULT_RETRIEVED_POSTING_LIST_COST: f64 = 10.0;
pub const DEFAULT_MIN_SERIES_PER_BLOCK_FOR_QUERY_PLANNING: u64 = 10_000;
pub const DEFAULT_LABEL_CARDINALITY_FOR_LARGER_SKETCH: u64 = 1_000_000;
pub const DEFAULT_LABEL_CARDINALITY_FOR_SMALLER_SKETCH: u64 = 1_000;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CostConfig {
    pub retrieved_posting_cost: f64,
    pub retrieved_series_cost: f64,
    pub retrieved_posting_list_cost: f64,
    pub min_series_per_block_for_query_planning: u64,
    pub label_cardinality_for_larger_sketch: u64,
    pub label_cardinality_for_smaller_sketch: u64,
}

impl Default for CostConfig {
    fn default() -> Self {
        Self {
            retrieved_posting_cost: DEFAULT_RETRIEVED_POSTING_COST,
            retrieved_series_cost: DEFAULT_RETRIEVED_SERIES_COST,
            retrieved_posting_list_cost: DEFAULT_RETRIEVED_POSTING_LIST_COST,
            min_series_per_block_for_query_planning: DEFAULT_MIN_SERIES_PER_BLOCK_FOR_QUERY_PLANNING,
            label_cardinality_for_larger_sketch: DEFAULT_LABEL_CARDINALITY_FOR_LARGER_SKETCH,
            label_cardinality_for_smaller_sketch: DEFAULT_LABEL_CARDINALITY_FOR_SMALLER_SKETCH,
        }
    }
}

impl From<&mimir_config::IndexLookupPlanningConfig> for CostConfig {
    /// The inlined `lookupplan.CostConfig` of `-blocks-storage.tsdb.index-lookup-planning.*`.
    fn from(c: &mimir_config::IndexLookupPlanningConfig) -> Self {
        Self {
            retrieved_posting_cost: c.retrieved_posting_cost,
            retrieved_series_cost: c.retrieved_series_cost,
            retrieved_posting_list_cost: c.retrieved_posting_list_cost,
            min_series_per_block_for_query_planning: c.min_series_per_block_for_query_planning,
            label_cardinality_for_larger_sketch: c.label_cardinality_for_larger_sketch,
            label_cardinality_for_smaller_sketch: c.label_cardinality_for_smaller_sketch,
        }
    }
}

impl CostConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.label_cardinality_for_smaller_sketch > self.label_cardinality_for_larger_sketch {
            return Err(
                "cardinality limit for smaller sketches cannot be larger than cardinality limit for larger sketches".to_string(),
            );
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Metrics (`lookupplan/metrics.go`)
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct Metrics {
    pub planning_duration: HistogramVec,
    pub filtered_ratio: HistogramVec,
    pub intersection_size_ratio: HistogramVec,
    pub final_cardinality_ratio: HistogramVec,
    user_id: Option<String>,
}

impl Metrics {
    pub fn new(reg: Option<&Registry>) -> Self {
        let planning_duration = HistogramVec::new(
            HistogramOpts::new(
                "cortex_tsdb_index_lookup_planning_duration_seconds",
                "Time spent planning query requests.",
            ),
            &["outcome", "user"],
        )
        .expect("valid planning_duration histogram");

        let filtered_ratio = HistogramVec::new(
            HistogramOpts::new(
                "cortex_tsdb_index_lookup_planning_filtered_ratio",
                "Ratio of series retrieved from the index which were also matching the vector selectors from the query.",
            ),
            &["user"],
        )
        .expect("valid filtered_ratio histogram");

        let intersection_size_ratio = HistogramVec::new(
            HistogramOpts::new(
                "cortex_tsdb_index_lookup_planning_index_selection_accuracy_ratio",
                "Ratio between estimated number of series selected from the index and the actual number of series selected from the index.",
            ),
            &["user"],
        )
        .expect("valid intersection_size_ratio histogram");

        let final_cardinality_ratio = HistogramVec::new(
            HistogramOpts::new(
                "cortex_tsdb_index_lookup_planning_block_cardinality_accuracy_ratio",
                "Ratio between estimated final number of series after all filtering and the actual final number of series after all filtering.",
            ),
            &["user"],
        )
        .expect("valid final_cardinality_ratio histogram");

        if let Some(r) = reg {
            let _ = r.register(Box::new(planning_duration.clone()));
            let _ = r.register(Box::new(filtered_ratio.clone()));
            let _ = r.register(Box::new(intersection_size_ratio.clone()));
            let _ = r.register(Box::new(final_cardinality_ratio.clone()));
        }

        Self {
            planning_duration,
            filtered_ratio,
            intersection_size_ratio,
            final_cardinality_ratio,
            user_id: None,
        }
    }

    pub fn for_user(&self, user_id: &str) -> Self {
        Self {
            planning_duration: self.planning_duration.clone(),
            filtered_ratio: self.filtered_ratio.clone(),
            intersection_size_ratio: self.intersection_size_ratio.clone(),
            final_cardinality_ratio: self.final_cardinality_ratio.clone(),
            user_id: Some(user_id.to_string()),
        }
    }

    pub fn observe_planning_duration(&self, outcome: &str, secs: f64) {
        let user = self.user_id.as_deref().unwrap_or("");
        self.planning_duration
            .with_label_values(&[outcome, user])
            .observe(secs);
    }
}

// ---------------------------------------------------------------------------
// QueryStats & Thread/Context Helpers (`lookupplan/query_stats.go`)
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct QueryStats {
    estimated_final_cardinality: AtomicU64,
    estimated_selected_postings: AtomicU64,
    actual_selected_postings: AtomicU64,
    actual_final_cardinality: AtomicU64,
}

impl QueryStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_estimated_final_cardinality(q: Option<&Self>, cardinality: u64) {
        if let Some(stats) = q {
            stats
                .estimated_final_cardinality
                .store(cardinality, AtomicOrdering::Relaxed);
        }
    }

    pub fn load_estimated_final_cardinality(q: Option<&Self>) -> u64 {
        q.map_or(0, |s| {
            s.estimated_final_cardinality.load(AtomicOrdering::Relaxed)
        })
    }

    pub fn set_estimated_selected_postings(q: Option<&Self>, postings: u64) {
        if let Some(stats) = q {
            stats
                .estimated_selected_postings
                .store(postings, AtomicOrdering::Relaxed);
        }
    }

    pub fn load_estimated_selected_postings(q: Option<&Self>) -> u64 {
        q.map_or(0, |s| {
            s.estimated_selected_postings.load(AtomicOrdering::Relaxed)
        })
    }

    pub fn set_actual_selected_postings(q: Option<&Self>, postings: u64) {
        if let Some(stats) = q {
            stats
                .actual_selected_postings
                .store(postings, AtomicOrdering::Relaxed);
        }
    }

    pub fn load_actual_selected_postings(q: Option<&Self>) -> u64 {
        q.map_or(0, |s| {
            s.actual_selected_postings.load(AtomicOrdering::Relaxed)
        })
    }

    pub fn set_actual_final_cardinality(q: Option<&Self>, cardinality: u64) {
        if let Some(stats) = q {
            stats
                .actual_final_cardinality
                .store(cardinality, AtomicOrdering::Relaxed);
        }
    }

    pub fn load_actual_final_cardinality(q: Option<&Self>) -> u64 {
        q.map_or(0, |s| {
            s.actual_final_cardinality.load(AtomicOrdering::Relaxed)
        })
    }
}

#[derive(Clone, Default)]
pub struct PlanningContext {
    pub query_stats: Option<Arc<QueryStats>>,
    pub disabled_planning: bool,
}

impl PlanningContext {
    pub fn with_query_stats(stats: Arc<QueryStats>) -> Self {
        Self {
            query_stats: Some(stats),
            disabled_planning: false,
        }
    }

    pub fn with_disabled_planning() -> Self {
        Self {
            query_stats: None,
            disabled_planning: true,
        }
    }
}

thread_local! {
    static CURRENT_QUERY_STATS: RefCell<Option<Arc<QueryStats>>> = const { RefCell::new(None) };
    static DISABLED_PLANNING: RefCell<bool> = const { RefCell::new(false) };
}

pub fn with_query_stats_context<F, R>(stats: Option<Arc<QueryStats>>, f: F) -> R
where
    F: FnOnce() -> R,
{
    let prev = CURRENT_QUERY_STATS.with(|c| c.replace(stats));
    let res = f();
    CURRENT_QUERY_STATS.with(|c| c.replace(prev));
    res
}

pub fn query_stats_from_current_context() -> Option<Arc<QueryStats>> {
    CURRENT_QUERY_STATS.with(|c| c.borrow().clone())
}

pub fn with_disabled_planning_context<F, R>(disabled: bool, f: F) -> R
where
    F: FnOnce() -> R,
{
    let prev = DISABLED_PLANNING.with(|c| c.replace(disabled));
    let res = f();
    DISABLED_PLANNING.with(|c| c.replace(prev));
    res
}

pub fn is_planning_disabled_in_context() -> bool {
    DISABLED_PLANNING.with(|c| *c.borrow())
}

// ---------------------------------------------------------------------------
// Count-Min Sketch & Block Statistics (`BoomFilters/countmin.go`, `stats.go`, `statistics.go`)
// ---------------------------------------------------------------------------

pub trait Statistics: Send + Sync {
    fn total_series(&self) -> u64;
    fn label_values_count(&self, name: &str) -> u64;
    fn label_values_cardinality(&self, name: &str, values: &[&str]) -> u64;
}

/// FNV-1 64-bit hash matching Go `BoomFilters/fnv.go:hash64DefaultFnv`.
#[inline]
fn boom_fnv1_64(data: &[u8]) -> (u32, u32) {
    const PRIME_64: u64 = 1_099_511_628_211;
    let mut sum: u64 = 14_695_981_039_346_656_037;
    for &c in data {
        sum = sum.wrapping_mul(PRIME_64);
        sum ^= c as u64;
    }
    let lower = (sum & 0xffff_ffff) as u32;
    let upper = ((sum >> 32) & 0xffff_ffff) as u32;
    (lower, upper)
}

#[derive(Clone, Debug)]
pub struct CountMinSketch {
    matrix: Vec<u64>,
    width: usize,
    depth: usize,
    count: u64,
    epsilon: f64,
    delta: f64,
}

impl CountMinSketch {
    pub fn new(epsilon: f64, delta: f64) -> Self {
        let width = (std::f64::consts::E / epsilon).ceil() as usize;
        let depth = (1.0 / delta).ln().ceil() as usize;
        Self {
            matrix: vec![0; depth * width],
            width: width.max(1),
            depth: depth.max(1),
            count: 0,
            epsilon,
            delta,
        }
    }

    pub fn epsilon(&self) -> f64 {
        self.epsilon
    }

    pub fn delta(&self) -> f64 {
        self.delta
    }

    pub fn total_count(&self) -> u64 {
        self.count
    }

    pub fn add_n(&mut self, data: &[u8], n: u64) {
        let (lower, upper) = boom_fnv1_64(data);
        for i in 0..self.depth {
            let col = ((lower as usize).wrapping_add((upper as usize).wrapping_mul(i))) % self.width;
            self.matrix[i * self.width + col] += n;
        }
        self.count += n;
    }

    pub fn count(&self, data: &[u8]) -> u64 {
        let (lower, upper) = boom_fnv1_64(data);
        let mut min_val = u64::MAX;
        for i in 0..self.depth {
            let col = ((lower as usize).wrapping_add((upper as usize).wrapping_mul(i))) % self.width;
            let v = self.matrix[i * self.width + col];
            if v < min_val {
                min_val = v;
            }
        }
        min_val
    }
}

#[derive(Clone, Debug)]
pub struct LabelValuesSketch {
    pub sketch: CountMinSketch,
    pub distinct_values: u64,
}

#[derive(Clone, Debug)]
pub struct BlockStatistics {
    pub total_series: u64,
    pub label_names: HashMap<String, LabelValuesSketch>,
}

impl Statistics for BlockStatistics {
    fn total_series(&self) -> u64 {
        self.total_series
    }

    fn label_values_count(&self, name: &str) -> u64 {
        self.label_names
            .get(name)
            .map_or(0, |s| s.distinct_values)
    }

    fn label_values_cardinality(&self, name: &str, values: &[&str]) -> u64 {
        let Some(sketch) = self.label_names.get(name) else {
            return 0;
        };
        if values.is_empty() {
            return sketch.sketch.total_count();
        }
        let mut total = 0u64;
        for val in values {
            total += sketch.sketch.count(val.as_bytes());
        }
        total
    }
}

#[derive(Clone, Debug, Default)]
pub struct PlanningBlockMeta {
    pub ulid: Ulid,
    pub num_series: u64,
}

impl From<&mimir_tsdb::block::BlockMeta> for PlanningBlockMeta {
    fn from(m: &mimir_tsdb::block::BlockMeta) -> Self {
        Self {
            ulid: m.ulid,
            num_series: m.stats.num_series,
        }
    }
}

pub trait BlockIndexReader: Send + Sync {
    fn all_postings_count(&self) -> Result<u64, String>;
    fn label_names(&self) -> Result<Vec<String>, String>;
    fn label_values(&self, name: &str) -> Result<Vec<String>, String>;
    fn postings_for_all_values_count(&self, name: &str) -> Result<u64, String>;
    fn postings_count(&self, name: &str, value: &str) -> Result<u64, String>;
}

impl BlockIndexReader for IndexReader {
    fn all_postings_count(&self) -> Result<u64, String> {
        let p = self.postings("", &[""]).map_err(|e| e.to_string())?;
        Ok(p.len() as u64)
    }

    fn label_names(&self) -> Result<Vec<String>, String> {
        Ok(self.label_names())
    }

    fn label_values(&self, name: &str) -> Result<Vec<String>, String> {
        self.label_values(name).map_err(|e| e.to_string())
    }

    fn postings_for_all_values_count(&self, name: &str) -> Result<u64, String> {
        let p = self.postings_for_all_values(name).map_err(|e| e.to_string())?;
        Ok(p.len() as u64)
    }

    fn postings_count(&self, name: &str, value: &str) -> Result<u64, String> {
        let p = self.postings(name, &[value]).map_err(|e| e.to_string())?;
        Ok(p.len() as u64)
    }
}

/// Prometheus `Head.Meta().ULID`: the bytes `"______head______"`.
pub const HEAD_ULID: Ulid = Ulid::from_bytes(*b"______head______");

/// `Head.Meta()` as seen by the planner: the fixed head ULID and the current series count.
pub fn head_planning_meta(head: &mimir_tsdb::head::Head) -> PlanningBlockMeta {
    PlanningBlockMeta {
        ulid: HEAD_ULID,
        num_series: head.num_series(),
    }
}

/// `Head.Index()` for statistics generation.
pub struct HeadIndexReader<'a>(pub &'a mimir_tsdb::head::Head);

impl BlockIndexReader for HeadIndexReader<'_> {
    fn all_postings_count(&self) -> Result<u64, String> {
        Ok(self.0.num_series())
    }

    fn label_names(&self) -> Result<Vec<String>, String> {
        self.0.label_names(&[]).map_err(|e| e.to_string())
    }

    fn label_values(&self, name: &str) -> Result<Vec<String>, String> {
        self.0.label_values(name, &[]).map_err(|e| e.to_string())
    }

    fn postings_for_all_values_count(&self, name: &str) -> Result<u64, String> {
        // Every series has at most one value per label name: the union of the per-value
        // postings is their sum.
        let values = self.0.label_values(name, &[]).map_err(|e| e.to_string())?;
        Ok(values
            .iter()
            .map(|v| self.0.postings_length(name, v) as u64)
            .sum())
    }

    fn postings_count(&self, name: &str, value: &str) -> Result<u64, String> {
        Ok(self.0.postings_length(name, value) as u64)
    }
}

#[derive(Clone, Debug, Default)]
pub struct StatisticsGenerator;

impl StatisticsGenerator {
    pub fn new() -> Self {
        Self
    }

    pub fn stats(
        &self,
        _meta: &PlanningBlockMeta,
        reader: &dyn BlockIndexReader,
        small_label_cardinality_threshold: u64,
        large_label_cardinality_threshold: u64,
    ) -> Result<BlockStatistics, String> {
        let series_count = reader.all_postings_count()?;
        let label_names = reader.label_names()?;
        let mut label_sketches = HashMap::with_capacity(label_names.len());

        for label_name in label_names {
            let values = reader.label_values(&label_name)?;
            let label_cardinality = reader.postings_for_all_values_count(&label_name)?;
            let epsilon = select_count_min_epsilon(
                small_label_cardinality_threshold,
                large_label_cardinality_threshold,
                label_cardinality,
            );
            let mut sketch = CountMinSketch::new(epsilon, 0.01);
            for value in &values {
                let count_for_value = reader.postings_count(&label_name, value)?;
                sketch.add_n(value.as_bytes(), count_for_value);
            }
            label_sketches.insert(
                label_name,
                LabelValuesSketch {
                    sketch,
                    distinct_values: values.len() as u64,
                },
            );
        }

        Ok(BlockStatistics {
            total_series: series_count,
            label_names: label_sketches,
        })
    }
}

fn select_count_min_epsilon(
    small_threshold: u64,
    large_threshold: u64,
    num_series: u64,
) -> f64 {
    if num_series >= large_threshold {
        0.005
    } else if num_series < small_threshold {
        0.05
    } else {
        0.01
    }
}

// ---------------------------------------------------------------------------
// Regex SingleMatchCost & EstimateSelectivity (`prometheus/model/labels/cost.go`)
// ---------------------------------------------------------------------------

const ESTIMATED_STRING_EQUALITY_COST: f64 = 1.0;
const ESTIMATED_STRING_HAS_PREFIX_COST: f64 = 1.0;
const ESTIMATED_SLICE_CONTAINS_COST_PER_ELEMENT: f64 = 1.1;
const ESTIMATED_MAP_CONTAINS_COST: f64 = 4.0;

pub fn single_match_cost(m: &LabelMatcher) -> f64 {
    match m.match_type {
        MatchType::Equal | MatchType::NotEqual => ESTIMATED_STRING_EQUALITY_COST,
        MatchType::RegexMatch | MatchType::RegexNotMatch => {
            if let Some(sm) = m.set_matches() {
                if !sm.is_empty() {
                    return ESTIMATED_SLICE_CONTAINS_COST_PER_ELEMENT * (sm.len() as f64);
                }
            }
            // Large literal alternation >= 256 (equalMultiStringMapMatcher without setMatches)
            if is_large_alternating_literals(&m.value) {
                return ESTIMATED_MAP_CONTAINS_COST;
            }
            // True matcher cases: `.*` or `.*lit1.*lit2.*` (isSimpleConcatenationPattern)
            if m.value == ".*" {
                return 1.0;
            }
            if let Some(chars) = simple_concat_contains_chars(&m.value) {
                return (chars as f64).max(1.0);
            }
            if !m.prefix().is_empty() {
                return ESTIMATED_STRING_HAS_PREFIX_COST;
            }
            fast_regex_single_match_cost(&m.value)
        }
    }
}

pub fn estimate_selectivity(m: &LabelMatcher, total_label_values: u64) -> f64 {
    if total_label_values == 0 {
        return 1.0;
    }
    let mut selectivity = match m.match_type {
        MatchType::Equal | MatchType::NotEqual => {
            if m.value.is_empty() {
                0.0
            } else {
                1.0 / (total_label_values as f64)
            }
        }
        MatchType::RegexMatch | MatchType::RegexNotMatch => {
            if let Some(sm) = m.set_matches() {
                if !sm.is_empty() {
                    (sm.len() as f64) / (total_label_values as f64)
                } else if m.value.is_empty() {
                    0.0
                } else if !m.prefix().is_empty() {
                    0.1
                } else if m.value == ".+" || m.value == ".*" {
                    1.0
                } else {
                    0.1
                }
            } else if m.value.is_empty() {
                0.0
            } else if !m.prefix().is_empty() {
                0.1
            } else if m.value == ".+" || m.value == ".*" {
                1.0
            } else {
                0.1
            }
        }
    };
    selectivity = selectivity.clamp(0.0, 1.0);
    if matches!(m.match_type, MatchType::NotEqual | MatchType::RegexNotMatch) {
        selectivity = 1.0 - selectivity;
    }
    selectivity
}

fn is_go_meta_char(b: u8) -> bool {
    matches!(
        b,
        b'\\' | b'+'
            | b'.'
            | b'*'
            | b'?'
            | b'('
            | b')'
            | b'|'
            | b'['
            | b']'
            | b'{'
            | b'}'
            | b'^'
            | b'$'
    )
}

fn is_large_alternating_literals(pattern: &str) -> bool {
    let mut count = 0usize;
    for part in pattern.split('|') {
        if part.bytes().any(is_go_meta_char) {
            return false;
        }
        count += 1;
    }
    count >= 256
}

fn simple_concat_contains_chars(pattern: &str) -> Option<usize> {
    let ast = parse_re_ast(pattern);
    let ast = optimize_alternating_simple_contains(ast);
    if let ReAst::Concat(subs) = &ast {
        if subs.len() >= 3 && subs.len() % 2 == 1 {
            let mut chars = 0usize;
            let mut num_lits = 0usize;
            for (i, sub) in subs.iter().enumerate() {
                if i % 2 == 0 {
                    if !is_ast_match_any(sub) {
                        return None;
                    }
                } else if let ReAst::Literal(s) = sub {
                    chars += s.chars().count();
                    num_lits += 1;
                } else {
                    return None;
                }
            }
            if num_lits > 1 {
                return Some(chars);
            }
        }
    }
    None
}

#[derive(Clone, Debug)]
enum ReAst {
    Empty,
    Literal(String),
    AnyChar,
    CharClass(usize),
    Star(Box<ReAst>),
    Plus(Box<ReAst>),
    Quest(Box<ReAst>),
    Capture(Box<ReAst>),
    Concat(Vec<ReAst>),
    Alternate(Vec<ReAst>),
    Other,
}

fn fast_regex_single_match_cost(pattern: &str) -> f64 {
    let ast = parse_re_ast(pattern);
    let ast = optimize_alternating_simple_contains(ast);
    let ast = strip_outer_wildcards(ast);
    cost_estimate(&ast).max(ESTIMATED_STRING_EQUALITY_COST)
}

fn is_ast_match_any(ast: &ReAst) -> bool {
    matches!(ast, ReAst::Star(inner) if matches!(inner.as_ref(), ReAst::AnyChar))
}

fn optimize_alternating_simple_contains(ast: ReAst) -> ReAst {
    let ReAst::Alternate(subs) = ast else {
        return ast;
    };
    let mut lits = Vec::with_capacity(subs.len());
    for sub in &subs {
        let ReAst::Concat(parts) = sub else {
            return ReAst::Alternate(subs);
        };
        if parts.len() != 3
            || !is_ast_match_any(&parts[0])
            || !is_ast_match_any(&parts[2])
        {
            return ReAst::Alternate(subs);
        }
        let ReAst::Literal(s) = &parts[1] else {
            return ReAst::Alternate(subs);
        };
        lits.push(ReAst::Literal(s.clone()));
    }
    if lits.len() > 1 {
        ReAst::Concat(vec![
            ReAst::Star(Box::new(ReAst::AnyChar)),
            ReAst::Alternate(lits),
            ReAst::Star(Box::new(ReAst::AnyChar)),
        ])
    } else {
        ReAst::Alternate(subs)
    }
}

/// Simulates the in-place `re.Sub` mutations performed by Go's `stringMatcherFromRegexp`
/// (`stringMatcherFromRegexpInternal`) on `m.parsedRe` before `m.re.SingleMatchCost()` evaluates it.
/// Mutations performed before the Go code bails out with a `nil` matcher are kept, as upstream.
fn strip_outer_wildcards(ast: ReAst) -> ReAst {
    let mut ast = ast;
    string_matcher_from_regexp_internal(&mut ast);
    ast
}

/// Go `clearCapture`: unwraps nested captures in place.
fn clear_capture_ast(re: &mut ReAst) {
    while let ReAst::Capture(_) = re {
        let ReAst::Capture(inner) = std::mem::replace(re, ReAst::Empty) else {
            unreachable!()
        };
        *re = *inner;
    }
}

/// Number of set matches (Go `findSetMatchesInternal`), or `None` if there are none / too many.
/// Character classes are approximated as a single match as their ranges are not retained.
fn find_set_matches_count(re: &ReAst) -> Option<usize> {
    const MAX_SET_MATCHES: usize = 256;
    match re {
        ReAst::Literal(_) => Some(1),
        ReAst::CharClass(_) => Some(1),
        ReAst::Capture(inner) => find_set_matches_count(inner),
        ReAst::Alternate(subs) => {
            let mut total = 0usize;
            for sub in subs {
                total += find_set_matches_count(sub)?;
                if total > MAX_SET_MATCHES {
                    return None;
                }
            }
            Some(total)
        }
        ReAst::Concat(subs) => {
            if subs.is_empty() {
                return None;
            }
            let mut count = 1usize;
            for sub in subs {
                let c = find_set_matches_count(sub)?;
                count = count.checked_mul(c)?;
                if count > MAX_SET_MATCHES {
                    return None;
                }
            }
            Some(count)
        }
        _ => None,
    }
}

/// Returns whether Go's `stringMatcherFromRegexpInternal` would return a non-nil matcher,
/// applying its in-place mutations of `re`.
fn string_matcher_from_regexp_internal(re: &mut ReAst) -> bool {
    clear_capture_ast(re);
    match re {
        ReAst::Plus(sub) | ReAst::Star(sub) | ReAst::Quest(sub) => {
            matches!(sub.as_ref(), ReAst::AnyChar)
        }
        ReAst::Empty | ReAst::Literal(_) => true,
        ReAst::Alternate(subs) => {
            for sub in subs.iter_mut() {
                if !string_matcher_from_regexp_internal(sub) {
                    return false;
                }
            }
            true
        }
        ReAst::Concat(subs) => {
            for sub in subs.iter_mut() {
                clear_capture_ast(sub);
            }
            subs.retain(|s| !matches!(s, ReAst::Empty));
            if subs.is_empty() {
                return true;
            }
            if subs.len() == 1 {
                return string_matcher_from_regexp_internal(&mut subs[0]);
            }

            let mut left = false;
            let mut right = false;
            if matches!(subs[0], ReAst::Plus(_) | ReAst::Star(_) | ReAst::Quest(_)) {
                left = string_matcher_from_regexp_internal(&mut subs[0]);
                if !left {
                    return false;
                }
                subs.remove(0);
            }
            let last = subs.len() - 1;
            if matches!(subs[last], ReAst::Plus(_) | ReAst::Star(_) | ReAst::Quest(_)) {
                right = string_matcher_from_regexp_internal(&mut subs[last]);
                if !right {
                    return false;
                }
                subs.pop();
            }

            let mut has_matches = find_set_matches_count(&ReAst::Concat(subs.clone())).is_some();
            if !has_matches && subs.len() == 2 {
                if !right && matches!(subs[0], ReAst::Literal(_)) {
                    right = string_matcher_from_regexp_internal(&mut subs[1]);
                    has_matches = right;
                } else if !left && matches!(subs[1], ReAst::Literal(_)) {
                    left = string_matcher_from_regexp_internal(&mut subs[0]);
                    has_matches = left;
                }
            }
            has_matches
        }
        _ => false,
    }
}

fn cost_estimate(ast: &ReAst) -> f64 {
    match ast {
        ReAst::Literal(s) => s.chars().count() as f64,
        ReAst::Star(_) => 10.0,
        ReAst::Alternate(subs) => 1.0 + subs.iter().map(cost_estimate).sum::<f64>(),
        ReAst::Capture(sub) => cost_estimate(sub),
        ReAst::Concat(subs) => subs.iter().map(cost_estimate).sum::<f64>(),
        ReAst::CharClass(rune_len) => *rune_len as f64,
        _ => 1.0,
    }
}

fn parse_re_ast(pattern: &str) -> ReAst {
    let chars: Vec<char> = pattern.chars().collect();
    let mut pos = 0usize;
    parse_re_alternation(&chars, &mut pos)
}

fn parse_re_alternation(chars: &[char], pos: &mut usize) -> ReAst {
    let mut branches = vec![parse_re_concat(chars, pos)];
    while *pos < chars.len() && chars[*pos] == '|' {
        *pos += 1;
        branches.push(parse_re_concat(chars, pos));
    }
    if branches.len() == 1 {
        branches.into_iter().next().unwrap()
    } else {
        ReAst::Alternate(branches)
    }
}

fn push_literal_char(items: &mut Vec<ReAst>, ch: char) {
    if let Some(ReAst::Literal(ref mut s)) = items.last_mut() {
        s.push(ch);
    } else {
        items.push(ReAst::Literal(ch.to_string()));
    }
}

fn pop_last_atom_for_quantifier(items: &mut Vec<ReAst>) -> Option<ReAst> {
    let last = items.pop()?;
    if let ReAst::Literal(mut s) = last {
        if let Some(ch) = s.pop() {
            if !s.is_empty() {
                items.push(ReAst::Literal(s));
            }
            return Some(ReAst::Literal(ch.to_string()));
        }
        return Some(ReAst::Empty);
    }
    Some(last)
}

fn parse_re_concat(chars: &[char], pos: &mut usize) -> ReAst {
    let mut items: Vec<ReAst> = Vec::new();
    while *pos < chars.len() {
        let ch = chars[*pos];
        match ch {
            '|' | ')' => break,
            '(' => {
                *pos += 1;
                let mut is_capture = true;
                if *pos < chars.len() && chars[*pos] == '?' {
                    is_capture = false;
                    *pos += 1;
                    while *pos < chars.len() && chars[*pos] != ':' && chars[*pos] != ')' {
                        if chars[*pos] == '<' {
                            is_capture = true;
                            *pos += 1;
                            while *pos < chars.len() && chars[*pos] != '>' {
                                *pos += 1;
                            }
                            if *pos < chars.len() {
                                *pos += 1;
                            }
                            break;
                        }
                        *pos += 1;
                    }
                    if *pos < chars.len() && chars[*pos] == ':' {
                        *pos += 1;
                    }
                }
                let inner = parse_re_alternation(chars, pos);
                if *pos < chars.len() && chars[*pos] == ')' {
                    *pos += 1;
                }
                if is_capture {
                    items.push(ReAst::Capture(Box::new(inner)));
                } else {
                    items.push(inner);
                }
            }
            '[' => {
                *pos += 1;
                let mut ranges = 0usize;
                if *pos < chars.len() && chars[*pos] == '^' {
                    *pos += 1;
                    ranges += 1;
                }
                if *pos < chars.len() && chars[*pos] == ']' {
                    *pos += 1;
                    ranges += 1;
                }
                while *pos < chars.len() && chars[*pos] != ']' {
                    if chars[*pos] == '\\' && *pos + 1 < chars.len() {
                        *pos += 2;
                    } else {
                        *pos += 1;
                    }
                    if *pos + 1 < chars.len() && chars[*pos] == '-' && chars[*pos + 1] != ']' {
                        *pos += 2;
                    }
                    ranges += 1;
                }
                if *pos < chars.len() && chars[*pos] == ']' {
                    *pos += 1;
                }
                items.push(ReAst::CharClass((ranges * 2).max(2)));
            }
            '\\' => {
                *pos += 1;
                if *pos < chars.len() {
                    let esc = chars[*pos];
                    *pos += 1;
                    match esc {
                        'd' | 'D' | 'w' | 'W' | 's' | 'S' => items.push(ReAst::CharClass(2)),
                        'b' | 'B' | 'A' | 'z' => items.push(ReAst::Other),
                        'n' => push_literal_char(&mut items, '\n'),
                        't' => push_literal_char(&mut items, '\t'),
                        'r' => push_literal_char(&mut items, '\r'),
                        other => push_literal_char(&mut items, other),
                    }
                }
            }
            '.' => {
                *pos += 1;
                items.push(ReAst::AnyChar);
            }
            '^' | '$' => {
                *pos += 1;
                items.push(ReAst::Other);
            }
            '*' => {
                *pos += 1;
                if *pos < chars.len() && chars[*pos] == '?' {
                    *pos += 1;
                }
                if let Some(atom) = pop_last_atom_for_quantifier(&mut items) {
                    items.push(ReAst::Star(Box::new(atom)));
                }
            }
            '+' => {
                *pos += 1;
                if *pos < chars.len() && chars[*pos] == '?' {
                    *pos += 1;
                }
                if let Some(atom) = pop_last_atom_for_quantifier(&mut items) {
                    items.push(ReAst::Plus(Box::new(atom)));
                }
            }
            '?' => {
                *pos += 1;
                if *pos < chars.len() && chars[*pos] == '?' {
                    *pos += 1;
                }
                if let Some(atom) = pop_last_atom_for_quantifier(&mut items) {
                    items.push(ReAst::Quest(Box::new(atom)));
                }
            }
            '{' => {
                let save = *pos;
                *pos += 1;
                while *pos < chars.len() && (chars[*pos].is_ascii_digit() || chars[*pos] == ',') {
                    *pos += 1;
                }
                if *pos < chars.len() && chars[*pos] == '}' {
                    *pos += 1;
                    if *pos < chars.len() && chars[*pos] == '?' {
                        *pos += 1;
                    }
                    let _ = pop_last_atom_for_quantifier(&mut items);
                    items.push(ReAst::Other);
                } else {
                    *pos = save + 1;
                    push_literal_char(&mut items, '{');
                }
            }
            c => {
                *pos += 1;
                push_literal_char(&mut items, c);
            }
        }
    }

    if items.is_empty() {
        ReAst::Empty
    } else if items.len() == 1 {
        items.into_iter().next().unwrap()
    } else {
        ReAst::Concat(items)
    }
}

// ---------------------------------------------------------------------------
// planPredicate (`lookupplan/predicate.go`)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct PlanPredicate {
    pub config: CostConfig,
    pub matcher: LabelMatcher,
    pub selectivity: f64,
    pub cardinality: u64,
    pub label_name_unique_vals: u64,
    pub single_match_cost: f64,
    pub index_scan_cost: f64,
}

impl PlanPredicate {
    pub fn new(matcher: LabelMatcher, stats: &dyn Statistics, config: CostConfig) -> Self {
        let single_match_cost = single_match_cost(&matcher);
        let label_name_unique_vals = stats.label_values_count(&matcher.name);
        let selectivity = estimate_selectivity(&matcher, label_name_unique_vals);
        let cardinality = estimate_predicate_cardinality(&matcher, stats, selectivity);

        let mut pred = Self {
            config,
            matcher,
            selectivity,
            cardinality,
            label_name_unique_vals,
            single_match_cost,
            index_scan_cost: 0.0,
        };
        pred.index_scan_cost = estimate_predicate_index_scan_cost(&pred);
        pred
    }

    pub fn index_lookup_cost(&self) -> f64 {
        self.index_scan_cost
            + self.config.retrieved_posting_list_cost
                * (self.label_name_unique_vals as f64)
                * self.selectivity
    }

    pub fn filter_cost(&self, series: u64) -> f64 {
        (series as f64) * self.single_match_cost
    }
}

fn estimate_predicate_index_scan_cost(pred: &PlanPredicate) -> f64 {
    let m = &pred.matcher;
    match m.match_type {
        MatchType::Equal | MatchType::NotEqual => {
            if m.value.is_empty() {
                pred.single_match_cost * (pred.label_name_unique_vals as f64)
            } else {
                pred.single_match_cost
            }
        }
        MatchType::RegexMatch | MatchType::RegexNotMatch => {
            if let Some(sm) = m.set_matches() {
                if !sm.is_empty() {
                    return pred.single_match_cost * (sm.len() as f64);
                }
            }
            pred.single_match_cost * (pred.label_name_unique_vals as f64)
        }
    }
}

fn estimate_predicate_cardinality(
    m: &LabelMatcher,
    stats: &dyn Statistics,
    selectivity: f64,
) -> u64 {
    match m.match_type {
        MatchType::Equal => {
            if m.matches("") {
                num_series_without_label(&m.name, stats)
            } else {
                stats.label_values_cardinality(&m.name, &[&m.value])
            }
        }
        MatchType::NotEqual => {
            if m.value.is_empty() {
                stats.label_values_cardinality(&m.name, &[])
            } else {
                stats
                    .total_series()
                    .wrapping_sub(stats.label_values_cardinality(&m.name, &[&m.value]))
            }
        }
        MatchType::RegexMatch => {
            let mut matched_series = if let Some(sm) = m.set_matches() {
                if !sm.is_empty() {
                    let refs: Vec<&str> = sm.iter().map(String::as_str).collect();
                    stats.label_values_cardinality(&m.name, &refs)
                } else {
                    let card = stats.label_values_cardinality(&m.name, &[]);
                    ((card as f64) * selectivity) as u64
                }
            } else {
                let card = stats.label_values_cardinality(&m.name, &[]);
                ((card as f64) * selectivity) as u64
            };
            if m.matches("") {
                matched_series = matched_series.wrapping_add(num_series_without_label(&m.name, stats));
            }
            matched_series
        }
        MatchType::RegexNotMatch => {
            let matched = if let Some(sm) = m.set_matches() {
                if !sm.is_empty() {
                    let refs: Vec<&str> = sm.iter().map(String::as_str).collect();
                    stats.label_values_cardinality(&m.name, &refs)
                } else {
                    let card = stats.label_values_cardinality(&m.name, &[]);
                    ((card as f64) * selectivity) as u64
                }
            } else {
                let card = stats.label_values_cardinality(&m.name, &[]);
                ((card as f64) * selectivity) as u64
            };
            let mut result = stats.total_series().wrapping_sub(matched);
            if m.matches("") {
                result = result.wrapping_add(num_series_without_label(&m.name, stats));
            }
            result
        }
    }
}

fn num_series_without_label(label_name: &str, stats: &dyn Statistics) -> u64 {
    stats
        .total_series()
        .wrapping_sub(stats.label_values_cardinality(label_name, &[]))
}

// ---------------------------------------------------------------------------
// Plan (`lookupplan/plan.go`)
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ShardSelector {
    pub shard_index: u64,
    pub shard_count: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SelectHints {
    pub shard_index: u64,
    pub shard_count: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BasicLookupPlan {
    pub index_matchers: Vec<LabelMatcher>,
    pub scan_matchers: Vec<LabelMatcher>,
}

impl BasicLookupPlan {
    pub fn new(index_matchers: Vec<LabelMatcher>, scan_matchers: Vec<LabelMatcher>) -> Self {
        Self {
            index_matchers,
            scan_matchers,
        }
    }

    pub fn index_matchers(&self) -> &[LabelMatcher] {
        &self.index_matchers
    }

    pub fn scan_matchers(&self) -> &[LabelMatcher] {
        &self.scan_matchers
    }
}

#[derive(Clone, Debug)]
pub struct Plan {
    pub predicates: Vec<PlanPredicate>,
    pub index_predicate: Vec<bool>,
    pub total_series: u64,
    pub shard: Option<ShardSelector>,
    pub num_decided_predicates: usize,
    pub config: CostConfig,
}

impl Plan {
    pub fn new_scan_only_plan(
        stats: &dyn Statistics,
        config: CostConfig,
        matchers: &[LabelMatcher],
        shard: Option<ShardSelector>,
    ) -> Self {
        let mut predicates = Vec::with_capacity(matchers.len());
        let mut index_predicate = Vec::with_capacity(matchers.len());
        for m in matchers {
            predicates.push(PlanPredicate::new(m.clone(), stats, config));
            index_predicate.push(false);
        }
        let n = predicates.len();
        Self {
            predicates,
            index_predicate,
            total_series: stats.total_series(),
            shard,
            num_decided_predicates: n,
            config,
        }
    }

    pub fn new_index_only_plan(
        stats: &dyn Statistics,
        config: CostConfig,
        matchers: &[LabelMatcher],
        shard: Option<ShardSelector>,
    ) -> Self {
        let mut p = Self::new_scan_only_plan(stats, config, matchers, shard);
        p.index_predicate.fill(true);
        p
    }

    pub fn virtual_predicate(&self, idx: usize) -> (PlanPredicate, bool) {
        if idx < self.num_decided_predicates {
            return (self.predicates[idx].clone(), self.index_predicate[idx]);
        }
        let mut virtual_pred = self.predicates[idx].clone();
        virtual_pred.single_match_cost = 1.0;
        virtual_pred.cardinality = 1;
        virtual_pred.label_name_unique_vals = 1;
        virtual_pred.selectivity = 1.0;
        virtual_pred.index_scan_cost = 1.0;
        (virtual_pred, idx == self.num_decided_predicates)
    }

    pub fn has_any_index_predicate(&self) -> bool {
        self.index_predicate.iter().any(|&b| b)
    }

    pub fn index_matchers(&self) -> Vec<LabelMatcher> {
        self.predicates
            .iter()
            .enumerate()
            .filter_map(|(i, p)| {
                if self.index_predicate[i] {
                    Some(p.matcher.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn scan_matchers(&self) -> Vec<LabelMatcher> {
        self.predicates
            .iter()
            .enumerate()
            .filter_map(|(i, p)| {
                if !self.index_predicate[i] {
                    Some(p.matcher.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn to_basic_lookup_plan(&self) -> BasicLookupPlan {
        BasicLookupPlan {
            index_matchers: self.index_matchers(),
            scan_matchers: self.scan_matchers(),
        }
    }

    pub fn use_index_for(&self, predicate_idx: usize) -> Self {
        let mut copy = self.clone();
        copy.index_predicate[predicate_idx] = true;
        copy
    }

    pub fn total_cost(&self) -> f64 {
        self.index_lookup_cost()
            + self.intersection_cost()
            + self.series_retrieval_cost()
            + self.filter_cost()
    }

    pub fn index_lookup_cost(&self) -> f64 {
        let mut cost = 0.0;
        for i in 0..self.predicates.len() {
            let (pr, is_index) = self.virtual_predicate(i);
            if is_index {
                cost += pr.index_lookup_cost();
            }
        }
        cost
    }

    pub fn intersection_cost(&self) -> f64 {
        let mut iterated_postings = 0u64;
        for i in 0..self.predicates.len() {
            let (pred, is_index) = self.virtual_predicate(i);
            if is_index {
                iterated_postings = iterated_postings.wrapping_add(pred.cardinality);
            }
        }
        (iterated_postings as f64) * self.config.retrieved_posting_cost
    }

    pub fn series_retrieval_cost(&self) -> f64 {
        (self.num_selected_postings() as f64) * self.config.retrieved_series_cost
    }

    pub fn filter_cost(&self) -> f64 {
        let mut cost = 0.0;
        let series_to_filter = self.num_selected_postings_in_our_shard();
        for i in 0..self.predicates.len() {
            let (pred, is_index) = self.virtual_predicate(i);
            if !is_index {
                cost += pred.filter_cost(series_to_filter);
            }
        }
        cost
    }

    pub fn num_selected_postings_in_our_shard(&self) -> u64 {
        sharded_cardinality(self.num_selected_postings(), self.shard)
    }

    pub fn num_selected_postings(&self) -> u64 {
        let mut final_selectivity = 1.0;
        for i in 0..self.predicates.len() {
            let (pred, is_index) = self.virtual_predicate(i);
            if !is_index {
                continue;
            }
            if self.total_series == 0 {
                return 0;
            }
            final_selectivity *= (pred.cardinality as f64) / (self.total_series as f64);
        }
        (final_selectivity * (self.total_series as f64)) as u64
    }

    pub fn non_sharded_cardinality(&self) -> u64 {
        if self.total_series == 0 {
            return 0;
        }
        let mut final_selectivity = 1.0;
        for pred in &self.predicates {
            final_selectivity *= (pred.cardinality as f64) / (self.total_series as f64);
        }
        (final_selectivity * (self.total_series as f64)) as u64
    }

    pub fn final_cardinality(&self) -> u64 {
        sharded_cardinality(self.non_sharded_cardinality(), self.shard)
    }
}

fn sharded_cardinality(cardinality: u64, shard: Option<ShardSelector>) -> u64 {
    let Some(s) = shard else {
        return cardinality;
    };
    if s.shard_count == 0 {
        return cardinality;
    }
    let sharded = cardinality / s.shard_count;
    if cardinality > 0 && sharded == 0 {
        1
    } else {
        sharded
    }
}

// ---------------------------------------------------------------------------
// Branch and Bound (`lookupplan/branch_and_bound.go`)
// ---------------------------------------------------------------------------

const MAX_MATCHERS_FOR_PLANNING: usize = 10;
const MAX_PLANS_FOR_PLANNING: usize = 1 << MAX_MATCHERS_FOR_PLANNING;

#[derive(Clone)]
struct PlanWithCost {
    plan: Plan,
    cost: f64,
}

impl PlanWithCost {
    fn new(mut plan: Plan, num_decided_predicates: usize) -> Self {
        plan.num_decided_predicates = num_decided_predicates;
        let cost = plan.total_cost();
        Self { plan, cost }
    }
}

/// Exact port of Go's `container/heap` over `[]planWithCost` (`Less: pq[i].cost < pq[j].cost`).
struct PlansHeap {
    items: Vec<PlanWithCost>,
}

// Negated float comparisons are deliberate: they mirror Go's `!h.Less(j, i)` NaN semantics.
#[allow(clippy::neg_cmp_op_on_partial_ord)]
impl PlansHeap {
    fn with_capacity(cap: usize) -> Self {
        Self {
            items: Vec::with_capacity(cap),
        }
    }

    fn push(&mut self, item: PlanWithCost) {
        self.items.push(item);
        let idx = self.items.len() - 1;
        self.up(idx);
    }

    fn pop(&mut self) -> Option<PlanWithCost> {
        if self.items.is_empty() {
            return None;
        }
        let n = self.items.len() - 1;
        self.items.swap(0, n);
        self.down(0, n);
        self.items.pop()
    }

    fn up(&mut self, mut j: usize) {
        while j > 0 {
            let i = (j - 1) / 2;
            if i == j || !(self.items[j].cost < self.items[i].cost) {
                break;
            }
            self.items.swap(i, j);
            j = i;
        }
    }

    fn down(&mut self, i0: usize, n: usize) {
        let mut i = i0;
        loop {
            let j1 = 2 * i + 1;
            if j1 >= n {
                break;
            }
            let mut j = j1;
            let j2 = j1 + 1;
            if j2 < n && self.items[j2].cost < self.items[j1].cost {
                j = j2;
            }
            if !(self.items[j].cost < self.items[i].cost) {
                break;
            }
            self.items.swap(i, j);
            i = j;
        }
    }
}

// ---------------------------------------------------------------------------
// LookupPlanner, NoopPlanner, CostBasedPlanner (`lookupplan/planner.go`)
// ---------------------------------------------------------------------------

pub trait LookupPlanner: Send + Sync {
    fn plan_index_lookup(
        &self,
        plan: &BasicLookupPlan,
        hints: Option<&SelectHints>,
    ) -> Result<BasicLookupPlan, String>;

    fn planner_kind(&self) -> &'static str;
    fn as_any(&self) -> &dyn Any;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NoopPlanner;

impl LookupPlanner for NoopPlanner {
    fn plan_index_lookup(
        &self,
        plan: &BasicLookupPlan,
        _hints: Option<&SelectHints>,
    ) -> Result<BasicLookupPlan, String> {
        Ok(plan.clone())
    }

    fn planner_kind(&self) -> &'static str {
        "NoopPlanner"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub struct CostBasedPlanner {
    pub config: CostConfig,
    pub stats: Arc<dyn Statistics>,
    pub metrics: Metrics,
}

impl CostBasedPlanner {
    pub fn new(metrics: Metrics, stats: Arc<dyn Statistics>, config: CostConfig) -> Self {
        Self {
            config,
            stats,
            metrics,
        }
    }

    fn generate_plans_branch_and_bound(
        &self,
        matchers: &[LabelMatcher],
        shard: Option<ShardSelector>,
    ) -> PlansHeap {
        let mut prospect_plans = PlansHeap::with_capacity(MAX_PLANS_FOR_PLANNING);
        let scan_only_plan = Plan::new_scan_only_plan(self.stats.as_ref(), self.config, matchers, shard);
        let num_predicates = scan_only_plan.predicates.len();
        prospect_plans.push(PlanWithCost::new(scan_only_plan, 0));

        let index_only_plan =
            Plan::new_index_only_plan(self.stats.as_ref(), self.config, matchers, shard);
        prospect_plans.push(PlanWithCost::new(index_only_plan, num_predicates));

        let mut complete_plans = PlansHeap::with_capacity(MAX_PLANS_FOR_PLANNING);
        let mut best_complete_cost = f64::MAX;

        for _ in 0..MAX_PLANS_FOR_PLANNING {
            let Some(current) = prospect_plans.pop() else {
                break;
            };
            if current.cost >= best_complete_cost {
                continue;
            }
            if current.plan.num_decided_predicates == num_predicates {
                if !current.plan.has_any_index_predicate() {
                    continue;
                }
                if current.cost < best_complete_cost {
                    best_complete_cost = current.cost;
                }
                complete_plans.push(current);
                continue;
            }

            let decided = current.plan.num_decided_predicates;
            let index_child = current.plan.use_index_for(decided);
            prospect_plans.push(PlanWithCost::new(index_child, decided + 1));
            prospect_plans.push(PlanWithCost::new(current.plan, decided + 1));
        }

        while let Some(mut p) = prospect_plans.pop() {
            p.plan.num_decided_predicates = num_predicates;
            p.cost = p.plan.total_cost();
            complete_plans.push(p);
        }

        complete_plans
    }

    fn choose_best_plan(&self, all_plans: PlansHeap) -> Option<Plan> {
        for pwc in all_plans.items {
            if pwc.plan.has_any_index_predicate() {
                return Some(pwc.plan);
            }
        }
        None
    }

    pub fn plan_index_lookup_with_context(
        &self,
        in_plan: &BasicLookupPlan,
        hints: Option<&SelectHints>,
        ctx: Option<&PlanningContext>,
    ) -> Result<BasicLookupPlan, String> {
        let disabled = ctx.map_or_else(is_planning_disabled_in_context, |c| c.disabled_planning);
        if disabled {
            return Ok(in_plan.clone());
        }

        let start = Instant::now();
        let shard = hints.and_then(|h| {
            if h.shard_count > 0 {
                Some(ShardSelector {
                    shard_index: h.shard_index,
                    shard_count: h.shard_count,
                })
            } else {
                None
            }
        });

        let mut matchers =
            Vec::with_capacity(in_plan.index_matchers.len() + in_plan.scan_matchers.len());
        matchers.extend(in_plan.index_matchers.iter().cloned());
        matchers.extend(in_plan.scan_matchers.iter().cloned());

        let all_plans = self.generate_plans_branch_and_bound(&matchers, shard);
        let selected = self.choose_best_plan(all_plans);

        match selected {
            Some(plan) => {
                let stats_opt = ctx
                    .and_then(|c| c.query_stats.clone())
                    .or_else(query_stats_from_current_context);
                if let Some(ref qs) = stats_opt {
                    QueryStats::set_estimated_selected_postings(
                        Some(qs.as_ref()),
                        plan.num_selected_postings(),
                    );
                    QueryStats::set_estimated_final_cardinality(
                        Some(qs.as_ref()),
                        plan.final_cardinality(),
                    );
                }
                self.metrics
                    .observe_planning_duration("success", start.elapsed().as_secs_f64());
                Ok(plan.to_basic_lookup_plan())
            }
            None => {
                self.metrics
                    .observe_planning_duration("error", start.elapsed().as_secs_f64());
                Err("no plan with index matchers found".to_string())
            }
        }
    }
}

impl LookupPlanner for CostBasedPlanner {
    fn plan_index_lookup(
        &self,
        plan: &BasicLookupPlan,
        hints: Option<&SelectHints>,
    ) -> Result<BasicLookupPlan, String> {
        self.plan_index_lookup_with_context(plan, hints, None)
    }

    fn planner_kind(&self) -> &'static str {
        "CostBasedPlanner"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// ---------------------------------------------------------------------------
// PlannerFactory & PlannerProvider (`lookupplan/planner_factory.go`, `pkg/ingester/planner_provider.go`)
// ---------------------------------------------------------------------------

pub trait PlannerFactoryTrait: Send + Sync {
    fn create_planner(
        &self,
        meta: &PlanningBlockMeta,
        reader: &dyn BlockIndexReader,
    ) -> Arc<dyn LookupPlanner>;
}

pub struct PlannerFactory {
    pub config: CostConfig,
    pub metrics: Metrics,
    pub stats_generator: StatisticsGenerator,
}

impl PlannerFactory {
    pub fn new(metrics: Metrics, stats_generator: StatisticsGenerator, config: CostConfig) -> Self {
        Self {
            config,
            metrics,
            stats_generator,
        }
    }
}

impl PlannerFactoryTrait for PlannerFactory {
    fn create_planner(
        &self,
        meta: &PlanningBlockMeta,
        reader: &dyn BlockIndexReader,
    ) -> Arc<dyn LookupPlanner> {
        if meta.num_series < self.config.min_series_per_block_for_query_planning {
            return Arc::new(NoopPlanner);
        }
        match self.stats_generator.stats(
            meta,
            reader,
            self.config.label_cardinality_for_smaller_sketch,
            self.config.label_cardinality_for_larger_sketch,
        ) {
            Ok(stats) => Arc::new(CostBasedPlanner::new(
                self.metrics.clone(),
                Arc::new(stats),
                self.config,
            )),
            Err(_) => Arc::new(NoopPlanner),
        }
    }
}

/// Ported from `pkg/ingester/planner_provider.go:plannerProvider`.
pub struct PlannerProvider {
    planner_factory: Arc<dyn PlannerFactoryTrait>,
    planners: RwLock<HashMap<Ulid, Arc<dyn LookupPlanner>>>,
}

impl PlannerProvider {
    pub fn new(planner_factory: Arc<dyn PlannerFactoryTrait>) -> Self {
        Self {
            planner_factory,
            planners: RwLock::new(HashMap::new()),
        }
    }

    pub fn get_planner(
        &self,
        block_meta: &PlanningBlockMeta,
        index_reader: &dyn BlockIndexReader,
    ) -> Arc<dyn LookupPlanner> {
        {
            let g = self.planners.read();
            if let Some(p) = g.get(&block_meta.ulid) {
                return Arc::clone(p);
            }
        }
        self.planner_factory.create_planner(block_meta, index_reader)
    }

    pub fn store_planner(&self, block_ulid: Ulid, planner: Arc<dyn LookupPlanner>) {
        self.planners.write().insert(block_ulid, planner);
    }

    pub fn generate_and_store_planner(
        &self,
        block_meta: &PlanningBlockMeta,
        index_reader: &dyn BlockIndexReader,
    ) {
        let planner = self.planner_factory.create_planner(block_meta, index_reader);
        self.store_planner(block_meta.ulid, planner);
    }
}
