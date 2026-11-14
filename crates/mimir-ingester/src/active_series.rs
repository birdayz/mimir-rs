//! Striped time-windowed active series, custom trackers, postings filters, and native histogram bucket tracker.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/ingester/activeseries/active_series.go
//! - github.com/grafana/mimir/pkg/ingester/activeseries/active_labels.go
//! - github.com/grafana/mimir/pkg/ingester/activeseries/active_postings.go
//! - github.com/grafana/mimir/pkg/ingester/activeseries/active_native_histogram_postings.go
//! - github.com/grafana/mimir/pkg/ingester/activeseries/model/custom_trackers_config.go
//! - github.com/grafana/mimir/pkg/ingester/activeseries/model/matchers.go
//! - github.com/grafana/mimir/pkg/ingester/active_series.go
//! - github.com/grafana/mimir/pkg/costattribution/active_tracker.go

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mimir_tsdb::labels::{Labels, METRIC_NAME};
use mimir_tsdb::matcher::{LabelMatcher, MatchType};
use mimir_tsdb::postings::MemPostings;
use mimir_tsdb::symbols::SymbolTable;
use parking_lot::{Mutex, RwLock};
use prometheus::core::{Collector, Desc};
use prometheus::proto::MetricFamily;
use prometheus::{GaugeVec, Opts, Registry};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

const NUM_STRIPES: usize = 128;

pub const ENABLED_FLAG: &str = "ingester.active-series-metrics-enabled";
pub const IDLE_TIMEOUT_FLAG: &str = "ingester.active-series-metrics-idle-timeout";
pub const MAX_NUMBER_OF_TRACKERS: usize = u16::MAX as usize;
const PRE_ALLOCATED_SIZE: usize = 3;

const FLAG_IS_DELETED: u8 = 1 << 0;
const FLAG_IS_OTLP: u8 = 1 << 1;

// ---------------------------------------------------------------------------
// Alertmanager-compatible matcher parser (`alertmanager/pkg/labels/parse.go`)
// ---------------------------------------------------------------------------

/// Alertmanager `MatchType` enum (`github.com/prometheus/alertmanager/pkg/labels`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum AmMatchType {
    Equal = 0,
    NotEqual = 1,
    RegexMatch = 2,
    RegexNotMatch = 3,
}

impl AmMatchType {
    pub fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Equal,
            1 => Self::NotEqual,
            2 => Self::RegexMatch,
            3 => Self::RegexNotMatch,
            _ => panic!("unknown match type"),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Equal => "=",
            Self::NotEqual => "!=",
            Self::RegexMatch => "=~",
            Self::RegexNotMatch => "!~",
        }
    }

    pub fn to_prom(self) -> MatchType {
        match self {
            Self::Equal => MatchType::Equal,
            Self::NotEqual => MatchType::NotEqual,
            Self::RegexMatch => MatchType::RegexMatch,
            Self::RegexNotMatch => MatchType::RegexNotMatch,
        }
    }
}

impl fmt::Display for AmMatchType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AmMatcher {
    pub match_type: AmMatchType,
    pub name: String,
    pub value: String,
}

impl AmMatcher {
    pub fn new(
        match_type: AmMatchType,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Self, String> {
        let name = name.into();
        let value = value.into();
        if matches!(match_type, AmMatchType::RegexMatch | AmMatchType::RegexNotMatch) {
            // Validate regex compilation via LabelMatcher
            LabelMatcher::new(match_type.to_prom(), &name, &value).map_err(|e| e.to_string())?;
        }
        Ok(Self {
            match_type,
            name,
            value,
        })
    }
}

pub fn am_label_matcher_to_prom(m: &AmMatcher) -> Result<LabelMatcher, String> {
    LabelMatcher::new(m.match_type.to_prom(), &m.name, &m.value).map_err(|e| e.to_string())
}

fn is_am_label_char_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_' || b == b':'
}

fn is_am_label_char_cont(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b':'
}

/// Ported 1:1 from `alertmanager/pkg/labels.ParseMatcher`.
pub fn am_parse_matcher(s: &str) -> Result<AmMatcher, String> {
    let trimmed = s.trim_start();
    let bytes = trimmed.as_bytes();
    if bytes.is_empty() || !is_am_label_char_start(bytes[0]) {
        return Err(format!("bad matcher format: {s}"));
    }
    let mut i = 1;
    while i < bytes.len() && is_am_label_char_cont(bytes[i]) {
        i += 1;
    }
    let name = &trimmed[..i];
    let rest = trimmed[i..].trim_start();

    let (match_type, after_op) = if let Some(r) = rest.strip_prefix("=~") {
        (AmMatchType::RegexMatch, r)
    } else if let Some(r) = rest.strip_prefix("!~") {
        (AmMatchType::RegexNotMatch, r)
    } else if let Some(r) = rest.strip_prefix("!=") {
        (AmMatchType::NotEqual, r)
    } else if let Some(r) = rest.strip_prefix('=') {
        (AmMatchType::Equal, r)
    } else {
        return Err(format!("bad matcher format: {s}"));
    };

    let raw_trimmed = after_op.trim();
    let mut raw_value = raw_trimmed;
    let mut expect_trailing_quote = false;
    if let Some(after) = raw_value.strip_prefix('"') {
        raw_value = after;
        expect_trailing_quote = true;
    }

    let mut value = String::with_capacity(raw_value.len());
    let mut escaped = false;
    let mut chars = raw_value.char_indices().peekable();
    while let Some((idx, ch)) = chars.next() {
        let is_last = chars.peek().is_none();
        if escaped {
            escaped = false;
            match ch {
                'n' => value.push('\n'),
                '"' | '\\' => value.push(ch),
                _ => {
                    value.push('\\');
                    value.push(ch);
                }
            }
            continue;
        }
        match ch {
            '\\' => {
                if !is_last {
                    escaped = true;
                    continue;
                }
                value.push('\\');
            }
            '"' => {
                if !expect_trailing_quote || idx + ch.len_utf8() < raw_value.len() {
                    return Err(format!(
                        "matcher value contains unescaped double quote: {raw_trimmed}"
                    ));
                }
                expect_trailing_quote = false;
            }
            _ => value.push(ch),
        }
    }

    if expect_trailing_quote {
        return Err(format!(
            "matcher value contains unescaped double quote: {raw_trimmed}"
        ));
    }

    AmMatcher::new(match_type, name, value)
}

/// Ported 1:1 from `alertmanager/pkg/labels.ParseMatchers`.
pub fn am_parse_matchers(s: &str) -> Result<Vec<AmMatcher>, String> {
    let s = s.strip_prefix('{').unwrap_or(s);
    let s = s.strip_suffix('}').unwrap_or(s);

    let mut inside_quotes = false;
    let mut escaped = false;
    let mut token = String::new();
    let mut tokens = Vec::new();

    for ch in s.chars() {
        match ch {
            ',' if !inside_quotes => {
                tokens.push(std::mem::take(&mut token));
                continue;
            }
            '"' => {
                if !escaped {
                    inside_quotes = !inside_quotes;
                } else {
                    escaped = false;
                }
            }
            '\\' => {
                escaped = !escaped;
            }
            _ => {
                escaped = false;
            }
        }
        token.push(ch);
    }

    let rem = token.trim();
    if !rem.is_empty() {
        tokens.push(rem.to_string());
    }

    let mut matchers = Vec::with_capacity(tokens.len());
    for tok in tokens {
        matchers.push(am_parse_matcher(&tok)?);
    }
    Ok(matchers)
}

// ---------------------------------------------------------------------------
// CustomTrackersConfig (`activeseries/model/custom_trackers_config.go`)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CustomTrackersConfig {
    pub source: BTreeMap<String, String>,
    pub config: BTreeMap<String, Vec<LabelMatcher>>,
    pub canonical_string: String,
}

fn custom_trackers_config_string(cfg: &BTreeMap<String, String>) -> String {
    if cfg.is_empty() {
        return String::new();
    }
    let mut out = String::new();
    for (i, (name, val)) in cfg.iter().enumerate() {
        if i > 0 {
            out.push(';');
        }
        out.push_str(name);
        out.push(':');
        out.push_str(val);
    }
    out
}

fn custom_tracker_flag_value_to_map(s: &str) -> Result<BTreeMap<String, String>, String> {
    let mut source = BTreeMap::new();
    for (i, p) in s.split(';').enumerate() {
        let Some((left, right)) = p.split_once(':') else {
            return Err(format!(
                "value should be <name>:<matcher>[;<name>:<matcher>]*, but colon was not found in the value {i}: {p:?}"
            ));
        };
        let name = left.trim();
        let matcher = right.trim();
        if name.is_empty() || matcher.is_empty() {
            return Err(format!(
                "semicolon-separated values should be <name>:<matcher>, but one of the sides was empty in the value {i}: {p:?}"
            ));
        }
        if source.contains_key(name) {
            return Err(format!(
                "matcher {name:?} for active series custom trackers is provided twice"
            ));
        }
        source.insert(name.to_string(), matcher.to_string());
    }
    Ok(source)
}

impl CustomTrackersConfig {
    pub fn new(m: BTreeMap<String, String>) -> Result<Self, String> {
        if m.len() > MAX_NUMBER_OF_TRACKERS {
            return Err(format!(
                "the number of trackers set [{}] exceeds the maximum number of trackers [{MAX_NUMBER_OF_TRACKERS}]",
                m.len()
            ));
        }
        let mut config = BTreeMap::new();
        for (name, matcher) in &m {
            let sm = am_parse_matchers(matcher)
                .map_err(|e| format!("can't build active series matcher {name}: {e}"))?;
            let mut prom_matchers = Vec::with_capacity(sm.len());
            for am in &sm {
                prom_matchers.push(am_label_matcher_to_prom(am)?);
            }
            config.insert(name.clone(), prom_matchers);
        }
        let canonical_string = custom_trackers_config_string(&m);
        Ok(Self {
            source: m,
            config,
            canonical_string,
        })
    }

    pub fn empty(&self) -> bool {
        self.canonical_string.is_empty()
    }

    pub fn as_str(&self) -> &str {
        &self.canonical_string
    }

    pub fn equal(&self, other: &Self) -> bool {
        self == other
    }

    pub fn set(&mut self, s: &str) -> Result<(), String> {
        if s.trim().is_empty() {
            return Ok(());
        }
        let f = custom_tracker_flag_value_to_map(s)?;
        let nc = Self::new(f.clone())?;

        if self.config.is_empty() {
            *self = nc;
            return Ok(());
        }

        for (name, ms) in nc.config {
            if self.config.contains_key(&name) {
                return Err(format!(
                    "matcher {name:?} for active series custom trackers is provided more than once"
                ));
            }
            self.config.insert(name.clone(), ms);
            if let Some(src) = f.get(&name) {
                self.source.insert(name, src.clone());
            }
        }
        self.canonical_string = custom_trackers_config_string(&self.source);
        Ok(())
    }

    pub fn validate(&self, max_trackers: isize) -> Result<(), String> {
        if max_trackers < 0 {
            return Err(format!("invalid max custom trackers limit: {max_trackers}"));
        }
        if max_trackers == 0 {
            return Ok(());
        }
        if self.config.len() > max_trackers as usize {
            return Err(format!(
                "the number of custom trackers [{}] exceeds the configured limit [{max_trackers}]",
                self.config.len()
            ));
        }
        Ok(())
    }

    /// Parses a YAML `map[string]string` representation of custom trackers.
    pub fn from_yaml_str(yaml: &str) -> Result<Self, String> {
        let map = parse_simple_yaml_string_map(yaml)?;
        Self::new(map)
    }

    /// Serializes `source` as a YAML `map[string]string`.
    pub fn to_yaml_string(&self) -> String {
        if self.source.is_empty() {
            return "{}\n".to_string();
        }
        let mut out = String::new();
        for (k, v) in &self.source {
            let escaped = serde_json::to_string(v).unwrap_or_else(|_| format!("{v:?}"));
            out.push_str(k);
            out.push_str(": ");
            out.push_str(&escaped);
            out.push('\n');
        }
        out
    }
}

impl fmt::Display for CustomTrackersConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.canonical_string)
    }
}

impl Serialize for CustomTrackersConfig {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.source.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for CustomTrackersConfig {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let map = BTreeMap::<String, String>::deserialize(deserializer)?;
        Self::new(map).map_err(serde::de::Error::custom)
    }
}

fn parse_simple_yaml_string_map(yaml: &str) -> Result<BTreeMap<String, String>, String> {
    let trimmed = yaml.trim();
    if trimmed.is_empty() || trimmed == "{}" || trimmed == "null" || trimmed == "~" {
        return Ok(BTreeMap::new());
    }
    let mut map = BTreeMap::new();
    for raw_line in trimmed.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((raw_k, raw_v)) = line.split_once(':') else {
            return Err(format!("invalid yaml line: {line}"));
        };
        let key = unquote_yaml_scalar(raw_k.trim())?;
        let val = unquote_yaml_scalar(raw_v.trim())?;
        map.insert(key, val);
    }
    Ok(map)
}

fn unquote_yaml_scalar(s: &str) -> Result<String, String> {
    if let Some(inner) = s.strip_prefix('"').and_then(|x| x.strip_suffix('"')) {
        let wrapped = format!("\"{inner}\"");
        serde_json::from_str::<String>(&wrapped).map_err(|e| e.to_string())
    } else if let Some(inner) = s.strip_prefix('\'').and_then(|x| x.strip_suffix('\'')) {
        Ok(inner.replace("''", "'"))
    } else {
        Ok(s.to_string())
    }
}

pub fn merge_custom_trackers_config(
    first: &CustomTrackersConfig,
    second: &CustomTrackersConfig,
) -> CustomTrackersConfig {
    if first.config.is_empty() && second.config.is_empty() {
        return CustomTrackersConfig::default();
    }
    if second.config.is_empty() {
        return first.clone();
    }
    if first.config.is_empty() {
        return second.clone();
    }
    let mut source = first.source.clone();
    for (k, v) in &second.source {
        source.insert(k.clone(), v.clone());
    }
    let mut config = first.config.clone();
    for (k, v) in &second.config {
        config.insert(k.clone(), v.clone());
    }
    let canonical_string = custom_trackers_config_string(&source);
    CustomTrackersConfig {
        source,
        config,
        canonical_string,
    }
}

// ---------------------------------------------------------------------------
// Optimized Matchers (`activeseries/model/matchers.go`)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Default)]
pub struct PreAllocDynamicSlice {
    arr: [u16; PRE_ALLOCATED_SIZE],
    arr_len: usize,
    rest: Vec<u16>,
}

impl PreAllocDynamicSlice {
    pub fn append(&mut self, val: usize) {
        if self.arr_len < PRE_ALLOCATED_SIZE {
            self.arr[self.arr_len] = val as u16;
            self.arr_len += 1;
            return;
        }
        self.rest.push(val as u16);
    }

    pub fn get(&self, idx: usize) -> u16 {
        if idx < PRE_ALLOCATED_SIZE {
            self.arr[idx]
        } else {
            self.rest[idx - PRE_ALLOCATED_SIZE]
        }
    }

    pub fn len(&self) -> usize {
        self.arr_len + self.rest.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn iter(&self) -> impl Iterator<Item = usize> + '_ {
        (0..self.len()).map(move |i| self.get(i) as usize)
    }
}

#[derive(Clone, Debug)]
struct IndexedMatchers {
    matchers: Vec<LabelMatcher>,
    index: usize,
}

impl IndexedMatchers {
    fn matches_labels(&self, lset: &Labels) -> bool {
        self.matchers.iter().all(|m| m.matches(lset.get(&m.name)))
    }

    fn matches_pairs(&self, pairs: &[(&str, &str)]) -> bool {
        self.matchers.iter().all(|m| {
            let val = pairs
                .iter()
                .find(|(k, _)| *k == m.name)
                .map(|(_, v)| *v)
                .unwrap_or("");
            m.matches(val)
        })
    }
}

#[derive(Clone, Debug, Default)]
pub struct Matchers {
    cfg: CustomTrackersConfig,
    names: Vec<String>,
    unoptimized: Vec<IndexedMatchers>,
    value_matchers: HashMap<String, HashMap<String, Vec<IndexedMatchers>>>,
    single_label_matchers: HashMap<String, Vec<IndexedMatchers>>,
}

impl Matchers {
    pub fn new(matchers_config: CustomTrackersConfig) -> Self {
        let cap = matchers_config.config.len();
        let mut names = Vec::with_capacity(cap);
        let mut unoptimized = Vec::with_capacity(cap);
        let mut single_label_matchers: HashMap<String, Vec<IndexedMatchers>> = HashMap::new();
        let mut matchers_for_exact_value = Vec::with_capacity(cap);
        let mut values_matched_per_label_name: HashMap<String, BTreeSet<String>> = HashMap::new();

        for (name, ms) in &matchers_config.config {
            let index = names.len();
            names.push(name.clone());

            if ms.len() == 1 && !ms[0].matches("") {
                single_label_matchers
                    .entry(ms[0].name.clone())
                    .or_default()
                    .push(IndexedMatchers {
                        matchers: ms.clone(),
                        index,
                    });
                continue;
            }

            let mut optimized = false;
            for m in ms {
                match m.match_type {
                    MatchType::Equal => {
                        if !m.value.is_empty() {
                            matchers_for_exact_value.push(IndexedMatchers {
                                matchers: ms.clone(),
                                index,
                            });
                            values_matched_per_label_name
                                .entry(m.name.clone())
                                .or_default()
                                .insert(m.value.clone());
                            optimized = true;
                            break;
                        }
                    }
                    MatchType::RegexMatch => {
                        if let Some(sm) = m.set_matches() {
                            if !sm.is_empty() {
                                matchers_for_exact_value.push(IndexedMatchers {
                                    matchers: ms.clone(),
                                    index,
                                });
                                let entry = values_matched_per_label_name
                                    .entry(m.name.clone())
                                    .or_default();
                                for v in sm {
                                    entry.insert(v.clone());
                                }
                                optimized = true;
                                break;
                            }
                        }
                    }
                    _ => {}
                }
            }

            if !optimized {
                unoptimized.push(IndexedMatchers {
                    matchers: ms.clone(),
                    index,
                });
            }
        }

        let mut label_names: Vec<String> = values_matched_per_label_name.keys().cloned().collect();
        label_names.sort_by(|a, b| {
            let ca = values_matched_per_label_name.get(a).map_or(0, BTreeSet::len);
            let cb = values_matched_per_label_name.get(b).map_or(0, BTreeSet::len);
            cb.cmp(&ca).then_with(|| a.cmp(b))
        });

        let mut value_matchers: HashMap<String, HashMap<String, Vec<IndexedMatchers>>> =
            HashMap::with_capacity(label_names.len());
        for name in label_names {
            let mut mi = 0;
            while mi < matchers_for_exact_value.len() {
                let mut removed = false;
                let ms_len = matchers_for_exact_value[mi].matchers.len();
                for lmi in 0..ms_len {
                    let m = &matchers_for_exact_value[mi].matchers[lmi];
                    if m.name != name {
                        continue;
                    }
                    let sm: Vec<String> = if m.match_type == MatchType::Equal && !m.value.is_empty() {
                        vec![m.value.clone()]
                    } else if m.match_type == MatchType::RegexMatch {
                        match m.set_matches() {
                            Some(s) if !s.is_empty() => s.to_vec(),
                            _ => continue,
                        }
                    } else {
                        continue;
                    };

                    let mut remaining = matchers_for_exact_value[mi].matchers.clone();
                    if remaining.len() > 1 {
                        remaining.remove(lmi);
                    } else {
                        remaining.clear();
                    }
                    let idx = matchers_for_exact_value[mi].index;
                    let vm = value_matchers.entry(name.clone()).or_default();
                    for v in sm {
                        vm.entry(v).or_default().push(IndexedMatchers {
                            matchers: remaining.clone(),
                            index: idx,
                        });
                    }
                    matchers_for_exact_value.remove(mi);
                    removed = true;
                    break;
                }
                if !removed {
                    mi += 1;
                }
            }
        }
        assert!(
            matchers_for_exact_value.is_empty(),
            "all exact value matchers must be classified"
        );

        Self {
            cfg: matchers_config,
            names,
            unoptimized,
            value_matchers,
            single_label_matchers,
        }
    }

    pub fn from_custom_trackers(trackers: &[CustomTrackerMatcher]) -> Self {
        let mut names = Vec::with_capacity(trackers.len());
        let mut unoptimized = Vec::with_capacity(trackers.len());
        let mut source = BTreeMap::new();
        let mut config = BTreeMap::new();
        for (idx, t) in trackers.iter().enumerate() {
            names.push(t.name.clone());
            unoptimized.push(IndexedMatchers {
                matchers: t.matchers.clone(),
                index: idx,
            });
            let expr = format!(
                "{{{}}}",
                t.matchers
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            );
            source.insert(t.name.clone(), expr);
            config.insert(t.name.clone(), t.matchers.clone());
        }
        let canonical_string = custom_trackers_config_string(&source);
        Self {
            cfg: CustomTrackersConfig {
                source,
                config,
                canonical_string,
            },
            names,
            unoptimized,
            value_matchers: HashMap::new(),
            single_label_matchers: HashMap::new(),
        }
    }

    pub fn matcher_names(&self) -> &[String] {
        &self.names
    }

    pub fn config(&self) -> &CustomTrackersConfig {
        &self.cfg
    }

    pub fn matches(&self, series: &Labels) -> PreAllocDynamicSlice {
        let mut matches = PreAllocDynamicSlice::default();
        if !self.value_matchers.is_empty() || !self.single_label_matchers.is_empty() {
            for (k, v) in series.iter() {
                if let Some(vm) = self.value_matchers.get(k) {
                    if let Some(list) = vm.get(v) {
                        for ms in list {
                            if ms.matches_labels(series) {
                                matches.append(ms.index);
                            }
                        }
                    }
                }
                if let Some(slm) = self.single_label_matchers.get(k) {
                    for ms in slm {
                        if ms.matches_labels(series) {
                            matches.append(ms.index);
                        }
                    }
                }
            }
        }
        for ms in &self.unoptimized {
            if ms.matches_labels(series) {
                matches.append(ms.index);
            }
        }
        matches
    }

    pub fn matches_pairs(&self, pairs: &[(&str, &str)]) -> PreAllocDynamicSlice {
        let mut matches = PreAllocDynamicSlice::default();
        if !self.value_matchers.is_empty() || !self.single_label_matchers.is_empty() {
            for &(k, v) in pairs {
                if let Some(vm) = self.value_matchers.get(k) {
                    if let Some(list) = vm.get(v) {
                        for ms in list {
                            if ms.matches_pairs(pairs) {
                                matches.append(ms.index);
                            }
                        }
                    }
                }
                if let Some(slm) = self.single_label_matchers.get(k) {
                    for ms in slm {
                        if ms.matches_pairs(pairs) {
                            matches.append(ms.index);
                        }
                    }
                }
            }
        }
        for ms in &self.unoptimized {
            if ms.matches_pairs(pairs) {
                matches.append(ms.index);
            }
        }
        matches
    }
}

// ---------------------------------------------------------------------------
// Backwards-compatible CustomTrackerMatcher helper
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct CustomTrackerMatcher {
    pub name: String,
    pub matchers: Vec<LabelMatcher>,
}

/// Parses `LimitsConfig.active_series_custom_trackers` (`map[name]promql_matchers`) into `CustomTrackerMatcher`s.
pub fn parse_custom_tracker_matchers(
    raw: &BTreeMap<String, String>,
) -> Vec<CustomTrackerMatcher> {
    let mut out = Vec::new();
    for (name, expr) in raw {
        if let Ok(ams) = am_parse_matchers(expr) {
            let mut prom = Vec::with_capacity(ams.len());
            let mut ok = true;
            for am in &ams {
                match am_label_matcher_to_prom(am) {
                    Ok(m) => prom.push(m),
                    Err(_) => {
                        ok = false;
                        break;
                    }
                }
            }
            if ok {
                out.push(CustomTrackerMatcher {
                    name: name.clone(),
                    matchers: prom,
                });
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Cost attribution active tracker (`pkg/costattribution/active_tracker.go`)
// ---------------------------------------------------------------------------

const TRACKED_SERIES_FACTOR: usize = 2;
const MISSING_VALUE: &str = "";
const KEY_SEP: char = '\x1e';

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CostAttributionLabel {
    pub input: String,
    pub output: String,
}

impl CostAttributionLabel {
    pub fn output_label(&self) -> &str {
        if self.output.is_empty() {
            &self.input
        } else {
            &self.output
        }
    }
}

#[derive(Default)]
struct CatCounters {
    active_series: AtomicI64,
    native_histograms: AtomicI64,
    native_histogram_buckets: AtomicI64,
}

/// Ported from `pkg/costattribution.activeSeriesTracker`.
pub struct CostAttributionActiveTracker {
    user_id: String,
    tracker_name: String,
    labels: Vec<CostAttributionLabel>,
    max_cardinality: usize,
    _cooldown: Duration,
    observed: RwLock<HashMap<String, Arc<CatCounters>>>,
    overflow_since: Mutex<Option<SystemTime>>,
    overflow_counter: CatCounters,
    registry: Option<Registry>,
    collector_registered: AtomicBool,
}

impl CostAttributionActiveTracker {
    pub fn new(
        user_id: impl Into<String>,
        tracker_name: impl Into<String>,
        labels: Vec<CostAttributionLabel>,
        max_cardinality: usize,
        cooldown: Duration,
        registry: Option<Registry>,
    ) -> Arc<Self> {
        let tracker = Arc::new(Self {
            user_id: user_id.into(),
            tracker_name: tracker_name.into(),
            labels,
            max_cardinality,
            _cooldown: cooldown,
            observed: RwLock::new(HashMap::new()),
            overflow_since: Mutex::new(None),
            overflow_counter: CatCounters::default(),
            registry: registry.clone(),
            collector_registered: AtomicBool::new(false),
        });
        if let Some(reg) = registry {
            if reg.register(Box::new(CatCollector(Arc::clone(&tracker)))).is_ok() {
                tracker.collector_registered.store(true, Ordering::Relaxed);
            }
        }
        tracker
    }

    pub fn equals(&self, other: Option<&Self>) -> bool {
        let Some(other) = other else {
            return false;
        };
        self.user_id == other.user_id
            && self.tracker_name == other.tracker_name
            && self.labels == other.labels
            && self.max_cardinality == other.max_cardinality
    }

    fn key_from_labels(&self, lbls: &Labels) -> String {
        let mut key = String::new();
        for (idx, cal) in self.labels.iter().enumerate() {
            if idx > 0 {
                key.push(KEY_SEP);
            }
            let v = lbls.get(&cal.input);
            if !v.is_empty() {
                key.push_str(v);
            } else {
                key.push_str(MISSING_VALUE);
            }
        }
        key
    }

    pub fn increment(&self, lbls: &Labels, now: SystemTime, native_histogram_bucket_num: i64) {
        let key = self.key_from_labels(lbls);
        {
            let guard = self.observed.read();
            if let Some(c) = guard.get(&key) {
                c.active_series.fetch_add(1, Ordering::Relaxed);
                if native_histogram_bucket_num >= 0 {
                    c.native_histograms.fetch_add(1, Ordering::Relaxed);
                    c.native_histogram_buckets
                        .fetch_add(native_histogram_bucket_num, Ordering::Relaxed);
                }
                return;
            }
            if guard.len() >= TRACKED_SERIES_FACTOR * self.max_cardinality {
                self.overflow_counter
                    .active_series
                    .fetch_add(1, Ordering::Relaxed);
                if native_histogram_bucket_num >= 0 {
                    self.overflow_counter
                        .native_histograms
                        .fetch_add(1, Ordering::Relaxed);
                    self.overflow_counter
                        .native_histogram_buckets
                        .fetch_add(native_histogram_bucket_num, Ordering::Relaxed);
                }
                return;
            }
        }

        let mut guard = self.observed.write();
        if let Some(c) = guard.get(&key) {
            c.active_series.fetch_add(1, Ordering::Relaxed);
            if native_histogram_bucket_num >= 0 {
                c.native_histograms.fetch_add(1, Ordering::Relaxed);
                c.native_histogram_buckets
                    .fetch_add(native_histogram_bucket_num, Ordering::Relaxed);
            }
            return;
        }
        if guard.len() >= TRACKED_SERIES_FACTOR * self.max_cardinality {
            self.overflow_counter
                .active_series
                .fetch_add(1, Ordering::Relaxed);
            if native_histogram_bucket_num >= 0 {
                self.overflow_counter
                    .native_histograms
                    .fetch_add(1, Ordering::Relaxed);
                self.overflow_counter
                    .native_histogram_buckets
                    .fetch_add(native_histogram_bucket_num, Ordering::Relaxed);
            }
            return;
        }

        let counter = Arc::new(CatCounters::default());
        counter.active_series.fetch_add(1, Ordering::Relaxed);
        if native_histogram_bucket_num >= 0 {
            counter.native_histograms.fetch_add(1, Ordering::Relaxed);
            counter
                .native_histogram_buckets
                .fetch_add(native_histogram_bucket_num, Ordering::Relaxed);
        }
        guard.insert(key, counter);

        if guard.len() > self.max_cardinality {
            let mut ov = self.overflow_since.lock();
            if ov.is_none() {
                *ov = Some(now);
            }
        }
    }

    pub fn decrement(&self, lbls: &Labels, native_histogram_bucket_num: i64) {
        let key = self.key_from_labels(lbls);
        {
            let guard = self.observed.read();
            if let Some(c) = guard.get(&key) {
                let nv = c.active_series.fetch_sub(1, Ordering::Relaxed) - 1;
                if native_histogram_bucket_num >= 0 {
                    c.native_histograms.fetch_sub(1, Ordering::Relaxed);
                    c.native_histogram_buckets
                        .fetch_sub(native_histogram_bucket_num, Ordering::Relaxed);
                }
                if nv > 0 {
                    return;
                }
            } else {
                drop(guard);
                if self.overflow_since.lock().is_some() {
                    self.overflow_counter
                        .active_series
                        .fetch_sub(1, Ordering::Relaxed);
                    if native_histogram_bucket_num >= 0 {
                        self.overflow_counter
                            .native_histograms
                            .fetch_sub(1, Ordering::Relaxed);
                        self.overflow_counter
                            .native_histogram_buckets
                            .fetch_sub(native_histogram_bucket_num, Ordering::Relaxed);
                    }
                    return;
                }
                panic!("decrementing non-existent active series: {lbls}");
            }
        }

        let mut guard = self.observed.write();
        if let Some(c) = guard.get(&key) {
            if c.active_series.load(Ordering::Relaxed) == 0 {
                guard.remove(&key);
            }
        }
    }

    pub fn snapshot(&self) -> Vec<(Vec<String>, i64, i64, i64)> {
        let guard = self.observed.read();
        let mut out = Vec::new();
        if self.overflow_since.lock().is_some() {
            let mut a = self.overflow_counter.active_series.load(Ordering::Relaxed);
            let mut h = self
                .overflow_counter
                .native_histograms
                .load(Ordering::Relaxed);
            let mut b = self
                .overflow_counter
                .native_histogram_buckets
                .load(Ordering::Relaxed);
            for c in guard.values() {
                a += c.active_series.load(Ordering::Relaxed);
                h += c.native_histograms.load(Ordering::Relaxed);
                b += c.native_histogram_buckets.load(Ordering::Relaxed);
            }
            let mut lvals = vec!["overflow".to_string(); self.labels.len()];
            lvals.push(self.user_id.clone());
            out.push((lvals, a, h, b));
            return out;
        }

        for (key, c) in guard.iter() {
            let mut keys: Vec<String> = if self.labels.is_empty() {
                Vec::new()
            } else {
                key.split(KEY_SEP).map(ToString::to_string).collect()
            };
            keys.push(self.user_id.clone());
            out.push((
                keys,
                c.active_series.load(Ordering::Relaxed),
                c.native_histograms.load(Ordering::Relaxed),
                c.native_histogram_buckets.load(Ordering::Relaxed),
            ));
        }
        out
    }

    pub fn registry(&self) -> Option<&Registry> {
        self.registry.as_ref()
    }
}

struct CatCollector(Arc<CostAttributionActiveTracker>);

impl Collector for CatCollector {
    fn desc(&self) -> Vec<&Desc> {
        // Static descriptor list per tracker isn't strictly required by prometheus::Registry::gather;
        // returning empty slice makes Registry call collect() unconditionally, or we can return one Desc.
        Vec::new()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let snap = self.0.snapshot();
        if snap.is_empty() {
            return Vec::new();
        }
        let mut label_names: Vec<&str> = self.0.labels.iter().map(|l| l.output_label()).collect();
        label_names.push("tenant");

        let mk_gv = |name: &str, help: &str| {
            GaugeVec::new(
                Opts::new(name, help).const_label("tracker", self.0.tracker_name.clone()),
                &label_names,
            )
            .expect("valid cat gauge vec")
        };

        let gv_series = mk_gv(
            "cortex_ingester_attributed_active_series",
            "The total number of active series per user and attribution.",
        );
        let gv_hist = mk_gv(
            "cortex_ingester_attributed_active_native_histogram_series",
            "The total number of active native histogram series per user and attribution.",
        );
        let gv_buckets = mk_gv(
            "cortex_ingester_attributed_active_native_histogram_buckets",
            "The total number of active native histogram buckets per user and attribution.",
        );

        for (lvals, s, h, b) in snap {
            let lrefs: Vec<&str> = lvals.iter().map(String::as_str).collect();
            gv_series.with_label_values(&lrefs).set(s as f64);
            if h > 0 {
                gv_hist.with_label_values(&lrefs).set(h as f64);
            }
            if b > 0 {
                gv_buckets.with_label_values(&lrefs).set(b as f64);
            }
        }

        let mut fams = gv_series.collect();
        fams.extend(gv_hist.collect());
        fams.extend(gv_buckets.collect());
        fams
    }
}

// ---------------------------------------------------------------------------
// SeriesIndexReader abstraction
// ---------------------------------------------------------------------------

pub trait SeriesIndexReader: Send + Sync {
    fn series_labels(&self, ref_id: u64) -> Option<Labels>;
}

impl SeriesIndexReader for mimir_tsdb::head::Head {
    fn series_labels(&self, ref_id: u64) -> Option<Labels> {
        self.series_labels(ref_id)
    }
}

// ---------------------------------------------------------------------------
// ActiveSeries & Stripes (`activeseries/active_series.go`)
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct SeriesEntry {
    nanos: Arc<AtomicI64>,
    matches: PreAllocDynamicSlice,
    num_native_histogram_buckets: i64, // -1 if not a native histogram
    flags: u8,
}

impl SeriesEntry {
    fn is_deleted(&self) -> bool {
        (self.flags & FLAG_IS_DELETED) != 0
    }

    fn mark_deleted(&mut self) {
        self.flags |= FLAG_IS_DELETED;
    }

    fn is_otlp(&self) -> bool {
        (self.flags & FLAG_IS_OTLP) != 0
    }
}

#[derive(Default)]
struct DeletedSeriesInner {
    keys: HashMap<u64, Vec<u8>>,
    refs: HashMap<Vec<u8>, u64>,
}

#[derive(Default)]
struct DeletedSeries {
    inner: Mutex<DeletedSeriesInner>,
}

impl DeletedSeries {
    fn add(&self, ref_id: u64, labels: &Labels) {
        let bytes = labels.as_bytes().to_vec();
        let mut g = self.inner.lock();
        g.keys.insert(ref_id, bytes.clone());
        g.refs.insert(bytes, ref_id);
    }

    fn find(&self, labels_bytes: &[u8]) -> Option<u64> {
        let g = self.inner.lock();
        if g.refs.is_empty() {
            return None;
        }
        g.refs.get(labels_bytes).copied()
    }

    fn purge(&self, ref_id: u64) {
        let mut g = self.inner.lock();
        if let Some(key) = g.keys.remove(&ref_id) {
            g.refs.remove(&key);
        }
    }

    fn clear(&self) {
        let mut g = self.inner.lock();
        g.keys.clear();
        g.refs.clear();
    }

    pub fn counts(&self) -> (usize, usize) {
        let g = self.inner.lock();
        (g.refs.len(), g.keys.len())
    }
}

struct SeriesStripeInner {
    refs: HashMap<u64, SeriesEntry>,
    active: u32,
    active_otlp: u32,
    active_matching: Vec<u32>,
    active_native_histograms: u32,
    active_matching_native_histograms: Vec<u32>,
    active_native_histogram_buckets: u32,
    active_matching_native_histogram_buckets: Vec<u32>,
    matchers: Matchers,
    cat: Option<Arc<CostAttributionActiveTracker>>,
    attribution_failures: f64,
}

struct SeriesStripe {
    oldest_entry_ns: AtomicI64,
    inner: RwLock<SeriesStripeInner>,
}

impl SeriesStripe {
    fn new(matchers: Matchers, cat: Option<Arc<CostAttributionActiveTracker>>) -> Self {
        let num_matchers = matchers.matcher_names().len();
        Self {
            oldest_entry_ns: AtomicI64::new(0),
            inner: RwLock::new(SeriesStripeInner {
                refs: HashMap::new(),
                active: 0,
                active_otlp: 0,
                active_matching: vec![0; num_matchers],
                active_native_histograms: 0,
                active_matching_native_histograms: vec![0; num_matchers],
                active_native_histogram_buckets: 0,
                active_matching_native_histogram_buckets: vec![0; num_matchers],
                matchers,
                cat,
                attribution_failures: 0.0,
            }),
        }
    }

    fn update_series_timestamp(
        &self,
        now_ns: i64,
        series_labels: Option<&Labels>,
        pairs: Option<&[(&str, &str)]>,
        ref_id: u64,
        num_native_histogram_buckets: i64,
        is_otlp: bool,
    ) -> bool {
        let mut need_write = false;
        let mut created = false;
        let mut entry_time_set = false;

        {
            let guard = self.inner.read();
            if let Some(entry) = guard.refs.get(&ref_id) {
                let prev = entry.nanos.load(Ordering::Relaxed);
                if now_ns > prev {
                    if entry
                        .nanos
                        .compare_exchange(prev, now_ns, Ordering::Relaxed, Ordering::Relaxed)
                        .is_ok()
                    {
                        entry_time_set = true;
                    } else if now_ns > entry.nanos.load(Ordering::Relaxed) {
                        entry.nanos.store(now_ns, Ordering::Relaxed);
                        entry_time_set = true;
                    }
                }
                if entry.num_native_histogram_buckets != num_native_histogram_buckets
                    || (is_otlp && !entry.is_otlp())
                {
                    need_write = true;
                }
            } else {
                need_write = true;
            }
        }

        if need_write {
            let mut guard = self.inner.write();
            if guard.refs.contains_key(&ref_id) {
                let (old_buckets, was_otlp, matches, nanos_arc) = {
                    let entry = guard.refs.get_mut(&ref_id).expect("checked contains_key");
                    let prev = entry.nanos.load(Ordering::Relaxed);
                    if now_ns > prev {
                        entry.nanos.store(now_ns, Ordering::Relaxed);
                        entry_time_set = true;
                    }
                    let was_otlp = entry.is_otlp();
                    if is_otlp && !was_otlp {
                        entry.flags |= FLAG_IS_OTLP;
                    }
                    let old_b = entry.num_native_histogram_buckets;
                    if old_b != num_native_histogram_buckets {
                        entry.num_native_histogram_buckets = num_native_histogram_buckets;
                    }
                    (old_b, was_otlp, entry.matches.clone(), Arc::clone(&entry.nanos))
                };
                let _ = nanos_arc;
                if is_otlp && !was_otlp {
                    guard.active_otlp += 1;
                }
                if old_buckets != num_native_histogram_buckets {
                    if old_buckets >= 0 && num_native_histogram_buckets < 0 {
                        guard.active_native_histograms =
                            guard.active_native_histograms.saturating_sub(1);
                        guard.active_native_histogram_buckets = guard
                            .active_native_histogram_buckets
                            .saturating_sub(old_buckets as u32);
                        for idx in matches.iter() {
                            if let Some(v) = guard.active_matching_native_histograms.get_mut(idx) {
                                *v = v.saturating_sub(1);
                            }
                            if let Some(v) =
                                guard.active_matching_native_histogram_buckets.get_mut(idx)
                            {
                                *v = v.saturating_sub(old_buckets as u32);
                            }
                        }
                    } else if old_buckets < 0 && num_native_histogram_buckets >= 0 {
                        guard.active_native_histograms += 1;
                        guard.active_native_histogram_buckets += num_native_histogram_buckets as u32;
                        for idx in matches.iter() {
                            if let Some(v) = guard.active_matching_native_histograms.get_mut(idx) {
                                *v += 1;
                            }
                            if let Some(v) =
                                guard.active_matching_native_histogram_buckets.get_mut(idx)
                            {
                                *v += num_native_histogram_buckets as u32;
                            }
                        }
                    } else if old_buckets >= 0 && num_native_histogram_buckets >= 0 {
                        let delta = num_native_histogram_buckets - old_buckets;
                        if delta >= 0 {
                            guard.active_native_histogram_buckets += delta as u32;
                            for idx in matches.iter() {
                                if let Some(v) =
                                    guard.active_matching_native_histogram_buckets.get_mut(idx)
                                {
                                    *v += delta as u32;
                                }
                            }
                        } else {
                            let sub = (-delta) as u32;
                            guard.active_native_histogram_buckets =
                                guard.active_native_histogram_buckets.saturating_sub(sub);
                            for idx in matches.iter() {
                                if let Some(v) =
                                    guard.active_matching_native_histogram_buckets.get_mut(idx)
                                {
                                    *v = v.saturating_sub(sub);
                                }
                            }
                        }
                    }

                    if let Some(ref cat) = guard.cat {
                        let lbls_owned;
                        let lbls = match series_labels {
                            Some(l) => l,
                            None => {
                                lbls_owned = Labels::from_pairs(pairs.unwrap_or(&[]).iter().copied());
                                &lbls_owned
                            }
                        };
                        cat.decrement(lbls, old_buckets);
                        cat.increment(
                            lbls,
                            UNIX_EPOCH + Duration::from_nanos(now_ns.max(0) as u64),
                            num_native_histogram_buckets,
                        );
                    }
                }
            } else {
                let matches = if let Some(l) = series_labels {
                    guard.matchers.matches(l)
                } else {
                    guard.matchers.matches_pairs(pairs.unwrap_or(&[]))
                };

                let mut flags = 0u8;
                if is_otlp {
                    flags |= FLAG_IS_OTLP;
                    guard.active_otlp += 1;
                }
                guard.active += 1;
                for idx in matches.iter() {
                    if let Some(v) = guard.active_matching.get_mut(idx) {
                        *v += 1;
                    }
                }
                if num_native_histogram_buckets >= 0 {
                    guard.active_native_histograms += 1;
                    guard.active_native_histogram_buckets += num_native_histogram_buckets as u32;
                    for idx in matches.iter() {
                        if let Some(v) = guard.active_matching_native_histograms.get_mut(idx) {
                            *v += 1;
                        }
                        if let Some(v) = guard.active_matching_native_histogram_buckets.get_mut(idx)
                        {
                            *v += num_native_histogram_buckets as u32;
                        }
                    }
                }

                if let Some(ref cat) = guard.cat {
                    let lbls_owned;
                    let lbls = match series_labels {
                        Some(l) => l,
                        None => {
                            lbls_owned = Labels::from_pairs(pairs.unwrap_or(&[]).iter().copied());
                            &lbls_owned
                        }
                    };
                    cat.increment(
                        lbls,
                        UNIX_EPOCH + Duration::from_nanos(now_ns.max(0) as u64),
                        num_native_histogram_buckets,
                    );
                }

                guard.refs.insert(
                    ref_id,
                    SeriesEntry {
                        nanos: Arc::new(AtomicI64::new(now_ns)),
                        matches,
                        num_native_histogram_buckets,
                        flags,
                    },
                );
                created = true;
                entry_time_set = true;
            }
        }

        if entry_time_set {
            let mut prev_oldest = self.oldest_entry_ns.load(Ordering::Relaxed);
            while prev_oldest > 0 && now_ns < prev_oldest {
                match self.oldest_entry_ns.compare_exchange_weak(
                    prev_oldest,
                    0,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(actual) => prev_oldest = actual,
                }
            }
        }

        created
    }

    fn contains_ref(&self, ref_id: u64) -> bool {
        self.inner.read().refs.contains_key(&ref_id)
    }

    fn native_histogram_buckets(&self, ref_id: u64) -> Option<u64> {
        let guard = self.inner.read();
        let e = guard.refs.get(&ref_id)?;
        if e.num_native_histogram_buckets >= 0 {
            Some(e.num_native_histogram_buckets as u64)
        } else {
            None
        }
    }

    fn mark_deleted(&self, ref_id: u64, labels: &Labels, deleted: &DeletedSeries) {
        let mut guard = self.inner.write();
        if let Some(entry) = guard.refs.get_mut(&ref_id) {
            entry.mark_deleted();
            deleted.add(ref_id, labels);
        }
    }

    fn remove(
        &self,
        ref_id: u64,
        idx: Option<&dyn SeriesIndexReader>,
        deleted: &DeletedSeries,
    ) {
        let mut guard = self.inner.write();
        let Some(entry) = guard.refs.remove(&ref_id) else {
            return;
        };
        guard.active = guard.active.saturating_sub(1);
        if entry.is_otlp() {
            guard.active_otlp = guard.active_otlp.saturating_sub(1);
        }
        if let Some(ref cat) = guard.cat {
            match idx.and_then(|r| r.series_labels(ref_id)) {
                Some(lbls) => cat.decrement(&lbls, entry.num_native_histogram_buckets),
                None => guard.attribution_failures += 1.0,
            }
        }
        for match_idx in entry.matches.iter() {
            if let Some(v) = guard.active_matching.get_mut(match_idx) {
                *v = v.saturating_sub(1);
            }
        }
        if entry.num_native_histogram_buckets >= 0 {
            let b = entry.num_native_histogram_buckets as u32;
            guard.active_native_histograms = guard.active_native_histograms.saturating_sub(1);
            guard.active_native_histogram_buckets =
                guard.active_native_histogram_buckets.saturating_sub(b);
            for match_idx in entry.matches.iter() {
                if let Some(v) = guard.active_matching_native_histograms.get_mut(match_idx) {
                    *v = v.saturating_sub(1);
                }
                if let Some(v) = guard
                    .active_matching_native_histogram_buckets
                    .get_mut(match_idx)
                {
                    *v = v.saturating_sub(b);
                }
            }
        }
        deleted.purge(ref_id);
    }

    fn reload_config(
        &self,
        new_matchers: &Matchers,
        new_cat: &Option<Arc<CostAttributionActiveTracker>>,
        matchers_changed: bool,
        cat_changed: bool,
        idx: Option<&dyn SeriesIndexReader>,
    ) {
        // Phase 1: pre-fetch refs under lock.
        let refs: Vec<u64> = {
            let g = self.inner.read();
            g.refs.keys().copied().collect()
        };

        // Phase 2: lookup labels and evaluate matchers outside lock.
        let mut resolved: HashMap<u64, (Labels, PreAllocDynamicSlice)> =
            HashMap::with_capacity(refs.len());
        if let Some(reader) = idx {
            for ref_id in refs {
                if let Some(lbls) = reader.series_labels(ref_id) {
                    let matches = if matchers_changed {
                        new_matchers.matches(&lbls)
                    } else {
                        PreAllocDynamicSlice::default()
                    };
                    resolved.insert(ref_id, (lbls, matches));
                }
            }
        }

        // Phase 3: update under write lock.
        let mut guard = self.inner.write();
        if matchers_changed {
            let num_m = new_matchers.matcher_names().len();
            guard.matchers = new_matchers.clone();
            guard.active_matching = vec![0; num_m];
            guard.active_matching_native_histograms = vec![0; num_m];
            guard.active_matching_native_histogram_buckets = vec![0; num_m];
        }

        if matchers_changed || cat_changed {
            let mut updates: Vec<(u64, PreAllocDynamicSlice)> =
                Vec::with_capacity(guard.refs.len());
            let mut failures = 0.0;
            for (&ref_id, entry) in &guard.refs {
                if let Some((lbls, matches)) = resolved.get(&ref_id) {
                    if cat_changed {
                        if let Some(ref cat) = new_cat {
                            let ts = entry.nanos.load(Ordering::Relaxed);
                            cat.increment(
                                lbls,
                                UNIX_EPOCH + Duration::from_nanos(ts.max(0) as u64),
                                entry.num_native_histogram_buckets,
                            );
                        }
                    }
                    if matchers_changed {
                        updates.push((ref_id, matches.clone()));
                    }
                    continue;
                }

                if let Some(lbls) = idx.and_then(|r| r.series_labels(ref_id)) {
                    if cat_changed {
                        if let Some(ref cat) = new_cat {
                            let ts = entry.nanos.load(Ordering::Relaxed);
                            cat.increment(
                                &lbls,
                                UNIX_EPOCH + Duration::from_nanos(ts.max(0) as u64),
                                entry.num_native_histogram_buckets,
                            );
                        }
                    }
                    if matchers_changed {
                        updates.push((ref_id, new_matchers.matches(&lbls)));
                    }
                } else {
                    if cat_changed && new_cat.is_some() {
                        failures += 1.0;
                    }
                    if matchers_changed {
                        updates.push((ref_id, new_matchers.matches(&Labels::empty())));
                    }
                }
            }
            guard.attribution_failures += failures;

            if matchers_changed {
                for (ref_id, matches) in updates {
                    let buckets = if let Some(e) = guard.refs.get_mut(&ref_id) {
                        e.matches = matches.clone();
                        e.num_native_histogram_buckets
                    } else {
                        continue;
                    };
                    for m_idx in matches.iter() {
                        if let Some(v) = guard.active_matching.get_mut(m_idx) {
                            *v += 1;
                        }
                        if buckets >= 0 {
                            if let Some(v) = guard.active_matching_native_histograms.get_mut(m_idx) {
                                *v += 1;
                            }
                            if let Some(v) =
                                guard.active_matching_native_histogram_buckets.get_mut(m_idx)
                            {
                                *v += buckets as u32;
                            }
                        }
                    }
                }
            }
        }

        if cat_changed {
            guard.cat = new_cat.clone();
        }
    }

    fn purge(
        &self,
        keep_until_ns: i64,
        idx: Option<&dyn SeriesIndexReader>,
        deleted: &DeletedSeries,
    ) {
        let oldest = self.oldest_entry_ns.load(Ordering::Relaxed);
        if oldest > 0 && keep_until_ns <= oldest {
            return;
        }
        let mut guard = self.inner.write();
        guard.active = 0;
        guard.active_otlp = 0;
        guard.active_native_histograms = 0;
        guard.active_native_histogram_buckets = 0;
        guard.active_matching.fill(0);
        guard.active_matching_native_histograms.fill(0);
        guard.active_matching_native_histogram_buckets.fill(0);

        let mut new_oldest = i64::MAX;
        let mut to_remove = Vec::new();
        for (&ref_id, entry) in &guard.refs {
            let ts = entry.nanos.load(Ordering::Relaxed);
            if ts < keep_until_ns {
                to_remove.push(ref_id);
            } else if ts < new_oldest {
                new_oldest = ts;
            }
        }

        for ref_id in to_remove {
            if let Some(entry) = guard.refs.remove(&ref_id) {
                if let Some(ref cat) = guard.cat {
                    match idx.and_then(|r| r.series_labels(ref_id)) {
                        Some(lbls) => cat.decrement(&lbls, entry.num_native_histogram_buckets),
                        None => guard.attribution_failures += 1.0,
                    }
                }
                if entry.is_deleted() {
                    deleted.purge(ref_id);
                }
            }
        }

        let entries: Vec<(PreAllocDynamicSlice, i64, bool)> = guard
            .refs
            .values()
            .map(|e| (e.matches.clone(), e.num_native_histogram_buckets, e.is_otlp()))
            .collect();
        for (matches, buckets, is_otlp) in entries {
            guard.active += 1;
            if is_otlp {
                guard.active_otlp += 1;
            }
            if buckets >= 0 {
                guard.active_native_histograms += 1;
                guard.active_native_histogram_buckets += buckets as u32;
            }
            for match_idx in matches.iter() {
                if let Some(v) = guard.active_matching.get_mut(match_idx) {
                    *v += 1;
                }
                if buckets >= 0 {
                    if let Some(v) = guard.active_matching_native_histograms.get_mut(match_idx) {
                        *v += 1;
                    }
                    if let Some(v) = guard
                        .active_matching_native_histogram_buckets
                        .get_mut(match_idx)
                    {
                        *v += buckets as u32;
                    }
                }
            }
        }

        if new_oldest == i64::MAX {
            self.oldest_entry_ns.store(0, Ordering::Relaxed);
        } else {
            self.oldest_entry_ns.store(new_oldest, Ordering::Relaxed);
        }
    }

    fn clear(&self) {
        let mut guard = self.inner.write();
        guard.refs.clear();
        guard.active = 0;
        guard.active_otlp = 0;
        guard.active_matching.fill(0);
        guard.active_native_histograms = 0;
        guard.active_matching_native_histograms.fill(0);
        guard.active_native_histogram_buckets = 0;
        guard.active_matching_native_histogram_buckets.fill(0);
        self.oldest_entry_ns.store(0, Ordering::Relaxed);
    }
}

/// 128-stripe active series & native histogram bucket tracker for a single tenant.
pub struct ActiveSeries {
    stripes: Vec<SeriesStripe>,
    deleted: DeletedSeries,
    config_mtx: RwLock<(Matchers, Option<Arc<CostAttributionActiveTracker>>)>,
    timeout: Duration,
}

impl ActiveSeries {
    pub fn new(matchers: Vec<CustomTrackerMatcher>, timeout: Duration) -> Self {
        let m = Matchers::from_custom_trackers(&matchers);
        Self::with_matchers(m, timeout, None)
    }

    pub fn with_matchers(
        matchers: Matchers,
        timeout: Duration,
        cat: Option<Arc<CostAttributionActiveTracker>>,
    ) -> Self {
        let mut stripes = Vec::with_capacity(NUM_STRIPES);
        for _ in 0..NUM_STRIPES {
            stripes.push(SeriesStripe::new(matchers.clone(), cat.clone()));
        }
        Self {
            stripes,
            deleted: DeletedSeries::default(),
            config_mtx: RwLock::new((matchers, cat)),
            timeout,
        }
    }

    pub fn current_matcher_names(&self) -> Vec<String> {
        self.config_mtx.read().0.matcher_names().to_vec()
    }

    pub fn matchers_differ(&self, ct_cfg: &CustomTrackersConfig) -> bool {
        ct_cfg.as_str() != self.config_mtx.read().0.config().as_str()
    }

    pub fn cost_attribution_differs(&self, cat: Option<&CostAttributionActiveTracker>) -> bool {
        let g = self.config_mtx.read();
        match (cat, g.1.as_deref()) {
            (None, None) => false,
            (Some(a), Some(b)) => !a.equals(Some(b)),
            _ => true,
        }
    }

    pub fn cost_attribution_tracker(&self) -> Option<Arc<CostAttributionActiveTracker>> {
        self.config_mtx.read().1.clone()
    }

    pub fn reload_matchers(
        &self,
        new_matchers: Vec<CustomTrackerMatcher>,
        head: &mimir_tsdb::head::Head,
    ) {
        let m = Matchers::from_custom_trackers(&new_matchers);
        let cat = self.cost_attribution_tracker();
        self.reload_series_config(m, cat, true, false, Some(head));
    }

    pub fn reload_series_config(
        &self,
        matchers: Matchers,
        cat: Option<Arc<CostAttributionActiveTracker>>,
        matchers_changed: bool,
        cat_changed: bool,
        idx: Option<&dyn SeriesIndexReader>,
    ) {
        let mut guard = self.config_mtx.write();
        for stripe in &self.stripes {
            stripe.reload_config(&matchers, &cat, matchers_changed, cat_changed, idx);
        }
        if matchers_changed {
            guard.0 = matchers;
        }
        if cat_changed {
            guard.1 = cat;
        }
    }

    pub fn update_series(
        &self,
        pairs: &[(&str, &str)],
        _hash: u64,
        ref_id: u64,
        now: SystemTime,
        num_native_histogram_buckets: i64,
        is_otlp: bool,
    ) {
        let now_ns = system_time_to_unix_nanos(now);
        let stripe_id = (ref_id as usize) % NUM_STRIPES;
        let created = self.stripes[stripe_id].update_series_timestamp(
            now_ns,
            None,
            Some(pairs),
            ref_id,
            num_native_histogram_buckets,
            is_otlp,
        );
        if created {
            let lbls = Labels::from_pairs(pairs.iter().copied());
            if let Some(deleted_ref) = self.deleted.find(lbls.as_bytes()) {
                let d_stripe = (deleted_ref as usize) % NUM_STRIPES;
                self.stripes[d_stripe].remove(deleted_ref, None, &self.deleted);
            }
        }
    }

    pub fn update_series_labels(
        &self,
        series: &Labels,
        ref_id: u64,
        now: SystemTime,
        num_native_histogram_buckets: i64,
        is_otlp: bool,
        idx: Option<&dyn SeriesIndexReader>,
    ) {
        let now_ns = system_time_to_unix_nanos(now);
        let stripe_id = (ref_id as usize) % NUM_STRIPES;
        let created = self.stripes[stripe_id].update_series_timestamp(
            now_ns,
            Some(series),
            None,
            ref_id,
            num_native_histogram_buckets,
            is_otlp,
        );
        if created {
            if let Some(deleted_ref) = self.deleted.find(series.as_bytes()) {
                let d_stripe = (deleted_ref as usize) % NUM_STRIPES;
                self.stripes[d_stripe].remove(deleted_ref, idx, &self.deleted);
            }
        }
    }

    pub fn post_deletion(&self, deleted: &[(u64, Labels)]) {
        for (ref_id, lbls) in deleted {
            let stripe_id = (*ref_id as usize) % NUM_STRIPES;
            if self.stripes[stripe_id].contains_ref(*ref_id) {
                self.stripes[stripe_id].mark_deleted(*ref_id, lbls, &self.deleted);
            }
        }
    }

    pub fn purge(&self, now: SystemTime) {
        self.purge_with_index(now, None);
    }

    pub fn purge_with_index(&self, now: SystemTime, idx: Option<&dyn SeriesIndexReader>) {
        let _cfg_guard = self.config_mtx.write();
        let keep_until = now.checked_sub(self.timeout).unwrap_or(UNIX_EPOCH);
        self.purge_at(keep_until, idx);
    }

    pub fn purge_at(&self, keep_until: SystemTime, idx: Option<&dyn SeriesIndexReader>) {
        let keep_until_ns = system_time_to_unix_nanos(keep_until);
        for stripe in &self.stripes {
            stripe.purge(keep_until_ns, idx, &self.deleted);
        }
    }

    pub fn contains_ref(&self, ref_id: u64) -> bool {
        self.stripes[(ref_id as usize) % NUM_STRIPES].contains_ref(ref_id)
    }

    pub fn native_histogram_buckets(&self, ref_id: u64) -> Option<u64> {
        self.stripes[(ref_id as usize) % NUM_STRIPES].native_histogram_buckets(ref_id)
    }

    pub fn active(&self) -> (usize, usize, usize, usize) {
        let mut total = 0usize;
        let mut otlp = 0usize;
        let mut histograms = 0usize;
        let mut buckets = 0usize;
        for stripe in &self.stripes {
            let g = stripe.inner.read();
            total += g.active as usize;
            otlp += g.active_otlp as usize;
            histograms += g.active_native_histograms as usize;
            buckets += g.active_native_histogram_buckets as usize;
        }
        (total, otlp, histograms, buckets)
    }

    pub fn active_with_matchers(
        &self,
    ) -> (
        usize,
        Vec<usize>,
        usize,
        usize,
        Vec<usize>,
        usize,
        Vec<usize>,
    ) {
        let n = self.config_mtx.read().0.matcher_names().len();
        let mut total = 0usize;
        let mut matching = vec![0usize; n];
        let mut otlp = 0usize;
        let mut histograms = 0usize;
        let mut matching_histograms = vec![0usize; n];
        let mut buckets = 0usize;
        let mut matching_buckets = vec![0usize; n];
        for stripe in &self.stripes {
            let g = stripe.inner.read();
            total += g.active as usize;
            otlp += g.active_otlp as usize;
            histograms += g.active_native_histograms as usize;
            buckets += g.active_native_histogram_buckets as usize;
            for (i, v) in g.active_matching.iter().enumerate() {
                if i < n {
                    matching[i] += *v as usize;
                }
            }
            for (i, v) in g.active_matching_native_histograms.iter().enumerate() {
                if i < n {
                    matching_histograms[i] += *v as usize;
                }
            }
            for (i, v) in g.active_matching_native_histogram_buckets.iter().enumerate() {
                if i < n {
                    matching_buckets[i] += *v as usize;
                }
            }
        }
        (
            total,
            matching,
            otlp,
            histograms,
            matching_histograms,
            buckets,
            matching_buckets,
        )
    }

    pub fn active_series_attribution_failure_count(&self) -> f64 {
        let mut total = 0.0;
        for stripe in &self.stripes {
            let mut g = stripe.inner.write();
            total += g.attribution_failures;
            g.attribution_failures = 0.0;
        }
        total
    }

    pub fn delete(&self, ref_id: u64) {
        self.delete_with_index(ref_id, None);
    }

    pub fn delete_with_index(&self, ref_id: u64, idx: Option<&dyn SeriesIndexReader>) {
        self.stripes[(ref_id as usize) % NUM_STRIPES].remove(ref_id, idx, &self.deleted);
    }

    pub fn clear(&self) {
        for stripe in &self.stripes {
            stripe.clear();
        }
        self.deleted.clear();
    }

    pub fn deleted_counts(&self) -> (usize, usize) {
        self.deleted.counts()
    }
}

fn system_time_to_unix_nanos(t: SystemTime) -> i64 {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_nanos().min(i64::MAX as u128) as i64,
        Err(e) => -(e.duration().as_nanos().min(i64::MAX as u128) as i64),
    }
}

// ---------------------------------------------------------------------------
// Postings interfaces (`active_postings.go`, `active_native_histogram_postings.go`, `active_labels.go`)
// ---------------------------------------------------------------------------

pub trait PostingsIter {
    fn next(&mut self) -> bool;
    fn seek(&mut self, v: u64) -> bool;
    fn at(&self) -> u64;
    fn err(&self) -> Option<&str>;
}

pub trait BucketCountPostings: PostingsIter {
    fn at_bucket_count(&self) -> (u64, usize);
}

#[derive(Clone, Debug)]
pub struct ListPostings {
    list: Vec<u64>,
    idx: Option<usize>,
}

impl ListPostings {
    pub fn new(list: Vec<u64>) -> Self {
        Self { list, idx: None }
    }
}

impl PostingsIter for ListPostings {
    fn next(&mut self) -> bool {
        let next_idx = match self.idx {
            None => 0,
            Some(i) => i + 1,
        };
        if next_idx < self.list.len() {
            self.idx = Some(next_idx);
            true
        } else {
            self.idx = Some(self.list.len());
            false
        }
    }

    fn seek(&mut self, v: u64) -> bool {
        let start = match self.idx {
            None => 0,
            Some(i) if i >= self.list.len() => return false,
            Some(i) if self.list[i] >= v => return true,
            Some(i) => i + 1,
        };
        let pos = self.list[start..].partition_point(|&x| x < v) + start;
        if pos < self.list.len() {
            self.idx = Some(pos);
            true
        } else {
            self.idx = Some(self.list.len());
            false
        }
    }

    fn at(&self) -> u64 {
        match self.idx {
            Some(i) if i < self.list.len() => self.list[i],
            _ => 0,
        }
    }

    fn err(&self) -> Option<&str> {
        None
    }
}

pub fn expand_postings(p: &mut dyn PostingsIter) -> Result<Vec<u64>, String> {
    let mut out = Vec::new();
    while p.next() {
        out.push(p.at());
    }
    if let Some(e) = p.err() {
        return Err(e.to_string());
    }
    Ok(out)
}

/// Ported from `activeseries/active_postings.go`.
pub struct ActivePostings<'a, P: PostingsIter> {
    active_series: &'a ActiveSeries,
    postings: P,
}

impl<'a, P: PostingsIter> ActivePostings<'a, P> {
    pub fn new(active_series: &'a ActiveSeries, postings: P) -> Self {
        Self {
            active_series,
            postings,
        }
    }
}

impl<P: PostingsIter> PostingsIter for ActivePostings<'_, P> {
    fn next(&mut self) -> bool {
        while self.postings.next() {
            if self.active_series.contains_ref(self.postings.at()) {
                return true;
            }
        }
        false
    }

    fn seek(&mut self, v: u64) -> bool {
        if !self.postings.seek(v) {
            return false;
        }
        if self.active_series.contains_ref(self.postings.at()) {
            return true;
        }
        self.next()
    }

    fn at(&self) -> u64 {
        self.postings.at()
    }

    fn err(&self) -> Option<&str> {
        self.postings.err()
    }
}

/// Ported from `pkg/ingester/active_series.go:ZeroBucketCountPostings`.
pub struct ZeroBucketCountPostings<'a, P: PostingsIter>(pub ActivePostings<'a, P>);

impl<P: PostingsIter> PostingsIter for ZeroBucketCountPostings<'_, P> {
    fn next(&mut self) -> bool {
        self.0.next()
    }
    fn seek(&mut self, v: u64) -> bool {
        self.0.seek(v)
    }
    fn at(&self) -> u64 {
        self.0.at()
    }
    fn err(&self) -> Option<&str> {
        self.0.err()
    }
}

impl<P: PostingsIter> BucketCountPostings for ZeroBucketCountPostings<'_, P> {
    fn at_bucket_count(&self) -> (u64, usize) {
        (self.at(), 0)
    }
}

/// Ported from `activeseries/active_native_histogram_postings.go`.
pub struct NativeHistogramPostings<'a, P: PostingsIter> {
    active_series: &'a ActiveSeries,
    postings: P,
    current_bucket_count: usize,
}

impl<'a, P: PostingsIter> NativeHistogramPostings<'a, P> {
    pub fn new(active_series: &'a ActiveSeries, postings: P) -> Self {
        Self {
            active_series,
            postings,
            current_bucket_count: 0,
        }
    }
}

impl<P: PostingsIter> PostingsIter for NativeHistogramPostings<'_, P> {
    fn next(&mut self) -> bool {
        while self.postings.next() {
            if let Some(count) = self
                .active_series
                .native_histogram_buckets(self.postings.at())
            {
                self.current_bucket_count = count as usize;
                return true;
            }
        }
        false
    }

    fn seek(&mut self, v: u64) -> bool {
        if !self.postings.seek(v) {
            return false;
        }
        if let Some(count) = self
            .active_series
            .native_histogram_buckets(self.postings.at())
        {
            self.current_bucket_count = count as usize;
            return true;
        }
        self.next()
    }

    fn at(&self) -> u64 {
        self.postings.at()
    }

    fn err(&self) -> Option<&str> {
        self.postings.err()
    }
}

impl<P: PostingsIter> BucketCountPostings for NativeHistogramPostings<'_, P> {
    fn at_bucket_count(&self) -> (u64, usize) {
        (self.postings.at(), self.current_bucket_count)
    }
}

/// Ported from `activeseries/active_labels.go:PostingsReader`.
pub trait ActivePostingsReader {
    fn postings(&self, name: &str, values: &[&str]) -> Result<ListPostings, String>;
}

pub struct MemPostingsReader<'a> {
    pub postings: &'a MemPostings,
    pub symbols: &'a SymbolTable,
}

impl ActivePostingsReader for MemPostingsReader<'_> {
    fn postings(&self, name: &str, values: &[&str]) -> Result<ListPostings, String> {
        let Some(name_id) = self.symbols.get(name) else {
            return Ok(ListPostings::new(Vec::new()));
        };
        let value_ids: Vec<_> = values.iter().filter_map(|v| self.symbols.get(v)).collect();
        Ok(ListPostings::new(
            self.postings.postings_for_ids(name_id, &value_ids),
        ))
    }
}

/// Ported from `activeseries/active_labels.go:IsLabelValueActive`.
pub fn is_label_value_active(
    reader: &dyn ActivePostingsReader,
    active_series: &ActiveSeries,
    name: &str,
    value: &str,
) -> Result<bool, String> {
    let value_postings = reader.postings(name, &[value])?;
    let mut active_postings = ActivePostings::new(active_series, value_postings);
    Ok(active_postings.next())
}

/// Ported from `pkg/ingester/active_series.go:matchAllSeries`.
pub fn match_all_series(matchers: &[LabelMatcher]) -> bool {
    if matchers.len() != 1 {
        return false;
    }
    if matchers[0].match_type == MatchType::RegexMatch && matchers[0].value == ".*" {
        return true;
    }
    if matchers[0].name == METRIC_NAME
        && matchers[0].match_type == MatchType::RegexMatch
        && matchers[0].value == ".+"
    {
        return true;
    }
    if matchers[0].name == METRIC_NAME
        && matchers[0].match_type == MatchType::NotEqual
        && matchers[0].value.is_empty()
    {
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn active_series_tracks_otlp_histograms_and_purges() {
        let mut map = BTreeMap::new();
        map.insert("api_only".to_string(), "{job=\"api\"}".to_string());
        let matchers = parse_custom_tracker_matchers(&map);
        let tracker = ActiveSeries::new(matchers, Duration::from_secs(60));

        let t0 = UNIX_EPOCH + Duration::from_secs(1000);
        tracker.update_series(&[("__name__", "m1"), ("job", "api")], 101, 1, t0, -1, false);
        tracker.update_series(&[("__name__", "m2"), ("job", "api")], 102, 2, t0, 12, true);
        tracker.update_series(&[("__name__", "m3"), ("job", "db")], 103, 3, t0, -1, true);

        let (total, matching, otlp, hists, m_hists, buckets, m_buckets) =
            tracker.active_with_matchers();
        assert_eq!(total, 3);
        assert_eq!(matching, vec![2]);
        assert_eq!(otlp, 2);
        assert_eq!(hists, 1);
        assert_eq!(m_hists, vec![1]);
        assert_eq!(buckets, 12);
        assert_eq!(m_buckets, vec![12]);
        assert_eq!(tracker.native_histogram_buckets(2), Some(12));
        assert_eq!(tracker.native_histogram_buckets(1), None);

        tracker.purge(t0 + Duration::from_secs(120));
        assert_eq!(tracker.active(), (0, 0, 0, 0));
    }
}
