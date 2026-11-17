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
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicPtr, AtomicU16, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
#[cfg(all(
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64"),
    not(miri)
))]
use std::sync::OnceLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;

use mimir_tsdb::labels::{Labels, METRIC_NAME};
use mimir_tsdb::matcher::{LabelMatcher, MatchType};
use mimir_tsdb::postings::MemPostings;
use mimir_tsdb::symbols::SymbolTable;
use parking_lot::{Mutex, RwLock};
pub use crate::costattribution::ActiveSeriesTracker as CostAttributionTracker;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

const NUM_STRIPES: usize = if cfg!(miri) { 16 } else { 4096 };
const STRIPE_MASK: usize = NUM_STRIPES - 1;

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
    arr_len: u8,
    #[allow(clippy::box_collection)]
    rest: Option<Box<Vec<u16>>>,
}

impl PreAllocDynamicSlice {
    pub fn append(&mut self, val: usize) {
        let len = self.arr_len as usize;
        if len < PRE_ALLOCATED_SIZE {
            self.arr[len] = val as u16;
            self.arr_len += 1;
            return;
        }
        self.rest.get_or_insert_with(Default::default).push(val as u16);
    }

    pub fn get(&self, idx: usize) -> u16 {
        if idx < PRE_ALLOCATED_SIZE {
            self.arr[idx]
        } else {
            self.rest.as_ref().expect("index in bounds")[idx - PRE_ALLOCATED_SIZE]
        }
    }

    pub fn len(&self) -> usize {
        (self.arr_len as usize) + self.rest.as_ref().map_or(0, |v| v.len())
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

/// Validates a custom trackers map like Go's `NewCustomTrackersConfig` (used as the
/// `mimir_config::limits` custom trackers validator).
pub fn validate_custom_trackers(m: &BTreeMap<String, String>) -> Result<(), String> {
    CustomTrackersConfig::new(m.clone()).map(|_| ())
}

/// Returns the active series custom trackers to use for a tenant: the base trackers
/// (`active_series_custom_trackers`) merged with the additional trackers
/// (`active_series_additional_custom_trackers`), where additional trackers win on a name clash.
///
/// Ported from `pkg/util/validation/limits.go` `Overrides.ActiveSeriesCustomTrackersConfig` +
/// `pkg/ingester/activeseries/model/custom_trackers_config.go` `MergeCustomTrackersConfig`.
pub fn merged_custom_trackers(
    base: &BTreeMap<String, String>,
    additional: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    if additional.is_empty() {
        return base.clone();
    }
    let mut merged = base.clone();
    for (k, v) in additional {
        merged.insert(k.clone(), v.clone());
    }
    merged
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
// Lock-free 2-level paged NanosSlab for zero-lock ActiveSeries timestamp updates
// ---------------------------------------------------------------------------

const SLAB_PAGE_SHIFT: usize = 12;
const SLAB_PAGE_SIZE: usize = 1 << SLAB_PAGE_SHIFT; // 4,096 slots = 32 KiB/page
const SLAB_PAGE_MASK: usize = SLAB_PAGE_SIZE - 1;
const SLAB_DIR_SHIFT: usize = 8;
const SLAB_DIR_SIZE: usize = 1 << SLAB_DIR_SHIFT; // 256 pages/dir
const SLAB_DIR_MASK: usize = SLAB_DIR_SIZE - 1;
const SLAB_TOP_SIZE: usize = 256; // 256 * 256 * 4096 = 268,435,456 slots (matches Head::PagedById)
const SLAB_MAX_SLOTS: u32 = (SLAB_TOP_SIZE * SLAB_DIR_SIZE * SLAB_PAGE_SIZE) as u32;
const SLOT_INACTIVE: i64 = i64::MIN;
const SLOT_INACTIVE_RAW: u64 = 0;
const NANOS_SIGN_BIT: u64 = i64::MIN as u64;

const PAGE_STATE_RECLAIMED: u32 = 1 << 30;
const PAGE_STATE_RECLAIMING: u32 = 1 << 31;

const CACHE_FLAG_PLAIN_FLOAT: u8 = 1 << 0;
const CACHE_FLAG_IS_OTLP: u8 = 1 << 1;

#[inline(always)]
const fn pack_active_cache(
    epoch: u16,
    is_plain_float: bool,
    is_otlp: bool,
    slot: u32,
) -> u64 {
    let flags = (if is_plain_float {
        CACHE_FLAG_PLAIN_FLOAT
    } else {
        0
    }) | (if is_otlp { CACHE_FLAG_IS_OTLP } else { 0 });
    ((epoch as u64) << 48)
        | ((flags as u64) << 32)
        | (slot as u64)
}

#[inline(always)]
const fn unpack_active_cache(packed: u64) -> (u16, u8, u32) {
    let epoch = (packed >> 48) as u16;
    let flags = (packed >> 32) as u8;
    let slot = packed as u32;
    (epoch, flags, slot)
}

#[inline(always)]
const fn sanitize_nanos(now_ns: i64) -> i64 {
    if now_ns == SLOT_INACTIVE {
        SLOT_INACTIVE + 1
    } else {
        now_ns
    }
}

/// Order-preserving bijection `i64 -> u64` where `SLOT_INACTIVE` (`i64::MIN`) maps to `0`.
/// Because `0` represents `SLOT_INACTIVE`, `alloc_zeroed` and Linux `madvise(MADV_DONTNEED)`
/// zero-fill pages directly to `SLOT_INACTIVE` while preserving all negative & positive timestamps.
#[inline(always)]
const fn encode_nanos(ts: i64) -> u64 {
    (ts as u64) ^ NANOS_SIGN_BIT
}

#[inline(always)]
const fn decode_nanos(raw: u64) -> i64 {
    (raw ^ NANOS_SIGN_BIT) as i64
}

#[repr(C, align(4096))]
struct SlabPage {
    slots: [AtomicU64; SLAB_PAGE_SIZE],
}

impl SlabPage {
    fn new_boxed() -> Box<Self> {
        let layout = std::alloc::Layout::new::<Self>();
        // SAFETY: `Self` has non-zero size (32 KiB) and 4 KiB alignment; all-zero bytes
        // initialize every `AtomicU64` to `SLOT_INACTIVE_RAW` (`0`), decoding to `SLOT_INACTIVE`.
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) as *mut Self };
        if ptr.is_null() {
            std::alloc::handle_alloc_error(layout);
        }
        // SAFETY: `ptr` was allocated with `layout` and zero-initialized.
        unsafe { Box::from_raw(ptr) }
    }
}

struct SlabPageDir {
    pages: [AtomicPtr<SlabPage>; SLAB_DIR_SIZE],
    states: [AtomicU32; SLAB_DIR_SIZE],
}

impl SlabPageDir {
    fn new_boxed() -> Box<Self> {
        Box::new(Self {
            pages: [const { AtomicPtr::new(std::ptr::null_mut()) }; SLAB_DIR_SIZE],
            states: [const { AtomicU32::new(0) }; SLAB_DIR_SIZE],
        })
    }
}

#[cfg(all(
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64"),
    not(miri)
))]
fn madvise_dontneed_page(page_ptr: *mut SlabPage) -> bool {
    const SC_PAGESIZE: std::ffi::c_int = 30;
    const MADV_DONTNEED: std::ffi::c_int = 4;
    extern "C" {
        fn sysconf(name: std::ffi::c_int) -> std::ffi::c_long;
        fn madvise(
            addr: *mut std::ffi::c_void,
            len: usize,
            advice: std::ffi::c_int,
        ) -> std::ffi::c_int;
    }
    static OS_PAGE_SIZE: OnceLock<usize> = OnceLock::new();
    let os_page_size = *OS_PAGE_SIZE.get_or_init(|| {
        // SAFETY: standard POSIX `sysconf(_SC_PAGESIZE)` query.
        let ps = unsafe { sysconf(SC_PAGESIZE) };
        if ps > 0 { ps as usize } else { 0 }
    });
    let size = std::mem::size_of::<SlabPage>();
    if os_page_size > 0
        && (page_ptr as usize).is_multiple_of(os_page_size)
        && size.is_multiple_of(os_page_size)
    {
        // SAFETY: `page_ptr` points to a `size`-byte OS-page-aligned allocation whose slots
        // are all `SLOT_INACTIVE_RAW` (`0`), and `PAGE_STATE_RECLAIMING` blocks concurrent stores.
        unsafe { madvise(page_ptr.cast::<std::ffi::c_void>(), size, MADV_DONTNEED) == 0 }
    } else {
        false
    }
}

#[cfg(not(all(
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64"),
    not(miri)
)))]
fn madvise_dontneed_page(_page_ptr: *mut SlabPage) -> bool {
    false
}

enum PurgeSlotOutcome {
    Inactive,
    Purged,
    Kept(i64),
}

struct NanosSlab {
    dirs: Box<[AtomicPtr<SlabPageDir>; SLAB_TOP_SIZE]>,
    alloc_mtx: Mutex<()>,
}

// SAFETY: Directory and page pointers are published with `Release`/`Acquire` and virtual
// memory is only freed in `NanosSlab::drop` (with `&mut self`), so lock-free readers never UAF.
unsafe impl Send for NanosSlab {}
unsafe impl Sync for NanosSlab {}

impl NanosSlab {
    fn new() -> Self {
        let dirs: Box<[AtomicPtr<SlabPageDir>]> = (0..SLAB_TOP_SIZE)
            .map(|_| AtomicPtr::new(std::ptr::null_mut()))
            .collect();
        let raw = Box::into_raw(dirs) as *mut [AtomicPtr<SlabPageDir>; SLAB_TOP_SIZE];
        Self {
            // SAFETY: `dirs` has exact length `SLAB_TOP_SIZE`.
            dirs: unsafe { Box::from_raw(raw) },
            alloc_mtx: Mutex::new(()),
        }
    }

    #[inline(always)]
    const fn split_id(ref_id: u32) -> (usize, usize, usize) {
        let id = ref_id as usize;
        let top = id >> (SLAB_PAGE_SHIFT + SLAB_DIR_SHIFT);
        let dir = (id >> SLAB_PAGE_SHIFT) & SLAB_DIR_MASK;
        let offset = id & SLAB_PAGE_MASK;
        (top, dir, offset)
    }

    #[inline(always)]
    fn slot_ref(&self, ref_id: u32) -> Option<&AtomicU64> {
        self.slot_and_state(ref_id).map(|(slot, _)| slot)
    }

    #[inline(always)]
    fn slot_and_state(&self, ref_id: u32) -> Option<(&AtomicU64, &AtomicU32)> {
        if ref_id >= SLAB_MAX_SLOTS {
            return None;
        }
        let (top, dir, offset) = Self::split_id(ref_id);
        let dir_ptr = self.dirs[top].load(Ordering::Acquire);
        if dir_ptr.is_null() {
            return None;
        }
        // SAFETY: `dir_ptr` points to a `SlabPageDir` freed only in `NanosSlab::drop`.
        let dir_ref = unsafe { &*dir_ptr };
        let page_ptr = dir_ref.pages[dir].load(Ordering::Acquire);
        if page_ptr.is_null() {
            return None;
        }
        // SAFETY: `page_ptr` points to a `SlabPage` freed only in `NanosSlab::drop`,
        // and `offset < SLAB_PAGE_SIZE`.
        unsafe { Some((&(*page_ptr).slots[offset], &dir_ref.states[dir])) }
    }

    fn ensure_and_store(&self, ref_id: u32, initial_nanos: i64) {
        if ref_id >= SLAB_MAX_SLOTS {
            return;
        }
        let (top, dir, offset) = Self::split_id(ref_id);
        let mut dir_ptr = self.dirs[top].load(Ordering::Acquire);
        if dir_ptr.is_null() {
            let _g = self.alloc_mtx.lock();
            dir_ptr = self.dirs[top].load(Ordering::Acquire);
            if dir_ptr.is_null() {
                dir_ptr = Box::into_raw(SlabPageDir::new_boxed());
                self.dirs[top].store(dir_ptr, Ordering::Release);
            }
        }
        // SAFETY: `dir_ptr` was just initialized/loaded and is valid until `NanosSlab::drop`.
        let dir_ref = unsafe { &*dir_ptr };
        let mut page_ptr = dir_ref.pages[dir].load(Ordering::Acquire);
        if page_ptr.is_null() {
            let _g = self.alloc_mtx.lock();
            page_ptr = dir_ref.pages[dir].load(Ordering::Acquire);
            if page_ptr.is_null() {
                page_ptr = Box::into_raw(SlabPage::new_boxed());
                dir_ref.pages[dir].store(page_ptr, Ordering::Release);
            }
        }

        let state_atomic = &dir_ref.states[dir];
        loop {
            let cur = state_atomic.load(Ordering::Acquire);
            if (cur & PAGE_STATE_RECLAIMING) != 0 {
                // Wait for concurrent reclaim_empty_pages() holding alloc_mtx to finish madvise.
                let _g = self.alloc_mtx.lock();
                std::hint::spin_loop();
                continue;
            }
            let next = (cur & !PAGE_STATE_RECLAIMED) + 1;
            if state_atomic
                .compare_exchange_weak(cur, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break;
            }
        }

        let encoded = encode_nanos(sanitize_nanos(initial_nanos));
        // SAFETY: `page_ptr` was just initialized/loaded and `offset < SLAB_PAGE_SIZE`.
        let slot_atomic = unsafe { &(*page_ptr).slots[offset] };
        let prev = slot_atomic.swap(encoded, Ordering::AcqRel);
        if prev != SLOT_INACTIVE_RAW {
            state_atomic.fetch_sub(1, Ordering::Release);
        }
    }

    #[inline(always)]
    fn write_nanos_if_active(&self, ref_id: u32, now_ns: i64) -> Option<bool> {
        let encoded_now = encode_nanos(sanitize_nanos(now_ns));
        let slot_atomic = self.slot_ref(ref_id)?;
        let mut cur = slot_atomic.load(Ordering::Relaxed);
        while cur != SLOT_INACTIVE_RAW && encoded_now > cur {
            match slot_atomic.compare_exchange_weak(
                cur,
                encoded_now,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Some(true),
                Err(actual) => cur = actual,
            }
        }
        if cur == SLOT_INACTIVE_RAW {
            None
        } else {
            Some(false)
        }
    }

    #[inline(always)]
    fn load(&self, ref_id: u32) -> i64 {
        self.slot_ref(ref_id).map_or(SLOT_INACTIVE, |a| {
            decode_nanos(a.load(Ordering::Acquire))
        })
    }

    #[inline(always)]
    fn mark_inactive(&self, ref_id: u32) {
        if let Some((slot_atomic, state_atomic)) = self.slot_and_state(ref_id) {
            let prev = slot_atomic.swap(SLOT_INACTIVE_RAW, Ordering::AcqRel);
            if prev != SLOT_INACTIVE_RAW {
                state_atomic.fetch_sub(1, Ordering::Release);
            }
        }
    }

    #[inline(always)]
    fn try_purge_slot(&self, ref_id: u32, keep_until_ns: i64) -> PurgeSlotOutcome {
        let Some((slot_atomic, state_atomic)) = self.slot_and_state(ref_id) else {
            return PurgeSlotOutcome::Inactive;
        };
        let mut cur = slot_atomic.load(Ordering::Acquire);
        loop {
            if cur == SLOT_INACTIVE_RAW {
                return PurgeSlotOutcome::Inactive;
            }
            let ts = decode_nanos(cur);
            if ts >= keep_until_ns {
                return PurgeSlotOutcome::Kept(ts);
            }
            match slot_atomic.compare_exchange_weak(
                cur,
                SLOT_INACTIVE_RAW,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    state_atomic.fetch_sub(1, Ordering::Release);
                    return PurgeSlotOutcome::Purged;
                }
                Err(actual) => cur = actual,
            }
        }
    }

    fn reclaim_empty_pages(&self) -> usize {
        let mut reclaimed = 0usize;
        for dir_atomic in self.dirs.iter() {
            let dir_ptr = dir_atomic.load(Ordering::Acquire);
            if dir_ptr.is_null() {
                continue;
            }
            // SAFETY: `dir_ptr` is valid until `NanosSlab::drop`.
            let dir_ref = unsafe { &*dir_ptr };
            for i in 0..SLAB_DIR_SIZE {
                let page_ptr = dir_ref.pages[i].load(Ordering::Acquire);
                if page_ptr.is_null() {
                    continue;
                }
                let state_atomic = &dir_ref.states[i];
                if state_atomic.load(Ordering::Relaxed) != 0 {
                    continue;
                }
                let _g = self.alloc_mtx.lock();
                if state_atomic
                    .compare_exchange(
                        0,
                        PAGE_STATE_RECLAIMING,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
                {
                    let next_state = if madvise_dontneed_page(page_ptr) {
                        reclaimed += 1;
                        PAGE_STATE_RECLAIMED
                    } else {
                        0
                    };
                    state_atomic.store(next_state, Ordering::Release);
                }
            }
        }
        reclaimed
    }

    fn stats(&self) -> (usize, usize, usize) {
        let mut allocated = 0usize;
        let mut resident = 0usize;
        let mut reclaimed = 0usize;
        for dir_atomic in self.dirs.iter() {
            let dir_ptr = dir_atomic.load(Ordering::Acquire);
            if dir_ptr.is_null() {
                continue;
            }
            // SAFETY: `dir_ptr` is valid until `NanosSlab::drop`.
            let dir_ref = unsafe { &*dir_ptr };
            for i in 0..SLAB_DIR_SIZE {
                let page_ptr = dir_ref.pages[i].load(Ordering::Acquire);
                if page_ptr.is_null() {
                    continue;
                }
                allocated += 1;
                let st = dir_ref.states[i].load(Ordering::Acquire);
                if (st & PAGE_STATE_RECLAIMED) != 0 {
                    reclaimed += 1;
                } else {
                    resident += 1;
                }
            }
        }
        (allocated, resident, reclaimed)
    }
}

impl Drop for NanosSlab {
    fn drop(&mut self) {
        for dir_atomic in self.dirs.iter_mut() {
            let dir_ptr = *dir_atomic.get_mut();
            if !dir_ptr.is_null() {
                // SAFETY: `&mut self` grants exclusive ownership during drop.
                let mut dir = unsafe { Box::from_raw(dir_ptr) };
                for page_atomic in dir.pages.iter_mut() {
                    let page_ptr = *page_atomic.get_mut();
                    if !page_ptr.is_null() {
                        // SAFETY: every non-null entry came from `Box::into_raw(SlabPage::new_boxed())`.
                        unsafe { drop(Box::from_raw(page_ptr)) };
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// ActiveSeries & Stripes (`activeseries/active_series.go`)
// ---------------------------------------------------------------------------

struct SeriesEntry {
    num_native_histogram_buckets: i32, // -1 if not a native histogram
    matches_handle: u32,               // 0 = no custom tracker matches; >0 = 1-based slot
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
    keys: hashbrown::HashMap<u64, Bytes>,
    by_hash: hashbrown::HashTable<u64>,
}

#[derive(Default)]
struct DeletedSeries {
    has_entries: AtomicBool,
    inner: Mutex<DeletedSeriesInner>,
}

impl DeletedSeries {
    #[inline]
    fn is_empty(&self) -> bool {
        !self.has_entries.load(Ordering::Acquire)
    }

    fn add(&self, ref_id: u64, labels: &Labels) {
        let bytes = labels.clone_bytes();
        let hash = xxhash_rust::xxh64::xxh64(&bytes, 0);
        let mut g = self.inner.lock();
        let DeletedSeriesInner { keys, by_hash } = &mut *g;
        if let Some(old_bytes) = keys.insert(ref_id, bytes) {
            let old_hash = xxhash_rust::xxh64::xxh64(&old_bytes, 0);
            if let Ok(found) = by_hash.find_entry(old_hash, |&id| id == ref_id) {
                found.remove();
            }
        }
        by_hash.insert_unique(hash, ref_id, |&id| {
            keys.get(&id).map_or(0, |b| xxhash_rust::xxh64::xxh64(b, 0))
        });
        self.has_entries.store(true, Ordering::Release);
    }

    fn find(&self, labels_bytes: &[u8]) -> Option<u64> {
        if self.is_empty() {
            return None;
        }
        let hash = xxhash_rust::xxh64::xxh64(labels_bytes, 0);
        let g = self.inner.lock();
        g.by_hash
            .find(hash, |&id| {
                g.keys.get(&id).is_some_and(|b| b.as_ref() == labels_bytes)
            })
            .copied()
    }

    fn purge(&self, ref_id: u64) {
        if self.is_empty() {
            return;
        }
        let mut g = self.inner.lock();
        if let Some(key) = g.keys.remove(&ref_id) {
            let hash = xxhash_rust::xxh64::xxh64(&key, 0);
            if let Ok(found) = g.by_hash.find_entry(hash, |&id| id == ref_id) {
                found.remove();
            }
            if g.keys.is_empty() {
                g.keys.shrink_to_fit();
                g.by_hash.shrink_to(0, |_| 0);
                self.has_entries.store(false, Ordering::Release);
            }
        }
    }

    fn clear(&self) {
        let mut g = self.inner.lock();
        g.keys = hashbrown::HashMap::new();
        g.by_hash = hashbrown::HashTable::new();
        self.has_entries.store(false, Ordering::Release);
    }

    pub fn counts(&self) -> (usize, usize) {
        let g = self.inner.lock();
        (g.by_hash.len(), g.keys.len())
    }
}

#[derive(Clone, Copy, Default)]
struct FastIntHasher(u64);

impl std::hash::Hasher for FastIntHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = self
                .0
                .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                .wrapping_add(b as u64);
        }
    }

    #[inline]
    fn write_u32(&mut self, i: u32) {
        self.write_u64(i as u64);
    }

    #[inline]
    fn write_u64(&mut self, i: u64) {
        let mut x = i.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        x ^= x >> 27;
        self.0 = x;
    }
}

#[derive(Clone, Copy, Default)]
struct FastIntBuildHasher;

impl std::hash::BuildHasher for FastIntBuildHasher {
    type Hasher = FastIntHasher;

    #[inline]
    fn build_hasher(&self) -> Self::Hasher {
        FastIntHasher(0)
    }
}

#[derive(Default)]
struct CustomMatchesTable {
    slots: Vec<Option<PreAllocDynamicSlice>>,
    free: Vec<u32>,
}

impl CustomMatchesTable {
    fn insert(&mut self, matches: PreAllocDynamicSlice) -> u32 {
        if let Some(idx) = self.free.pop() {
            self.slots[idx as usize] = Some(matches);
            return idx + 1;
        }
        let idx = self.slots.len() as u32;
        self.slots.push(Some(matches));
        idx + 1
    }

    #[inline]
    fn get(&self, handle: u32) -> Option<&PreAllocDynamicSlice> {
        if handle == 0 {
            return None;
        }
        self.slots
            .get((handle - 1) as usize)
            .and_then(|o| o.as_ref())
    }

    fn remove(&mut self, handle: u32) -> bool {
        if handle == 0 {
            return false;
        }
        let idx = (handle - 1) as usize;
        if idx < self.slots.len() && self.slots[idx].take().is_some() {
            self.free.push(handle - 1);
            if self.free.len() == self.slots.len() {
                self.slots.clear();
                self.slots.shrink_to_fit();
                self.free.clear();
                self.free.shrink_to_fit();
                return true;
            }
        }
        false
    }
}

#[derive(Default)]
struct OverflowStripeData {
    nanos: hashbrown::HashMap<u64, Box<AtomicI64>, FastIntBuildHasher>,
    refs: hashbrown::HashMap<u64, SeriesEntry, FastIntBuildHasher>,
}

impl OverflowStripeData {
    fn is_empty(&self) -> bool {
        self.nanos.is_empty() && self.refs.is_empty()
    }
}

struct SeriesStripeInner {
    refs: hashbrown::HashMap<u32, SeriesEntry, FastIntBuildHasher>,
    overflow: Option<Box<OverflowStripeData>>,
    custom_matches: Option<Box<CustomMatchesTable>>,
    active: u32,
    active_otlp: u32,
    active_matching: Vec<u32>,
    active_native_histograms: u32,
    active_matching_native_histograms: Vec<u32>,
    active_native_histogram_buckets: u32,
    active_matching_native_histogram_buckets: Vec<u32>,
    matchers: Arc<Matchers>,
    cat: Option<Arc<CostAttributionTracker>>,
    attribution_failures: f64,
}

impl SeriesStripeInner {
    #[inline]
    fn get_entry(&self, ref_id: u64) -> Option<&SeriesEntry> {
        if let Ok(r32) = u32::try_from(ref_id) {
            self.refs.get(&r32)
        } else {
            self.overflow.as_ref().and_then(|o| o.refs.get(&ref_id))
        }
    }

    #[inline]
    fn get_entry_mut(&mut self, ref_id: u64) -> Option<&mut SeriesEntry> {
        if let Ok(r32) = u32::try_from(ref_id) {
            self.refs.get_mut(&r32)
        } else {
            self.overflow
                .as_mut()
                .and_then(|o| o.refs.get_mut(&ref_id))
        }
    }

    fn insert_entry(&mut self, ref_id: u64, entry: SeriesEntry) {
        if let Ok(r32) = u32::try_from(ref_id) {
            self.refs.insert(r32, entry);
        } else {
            self.overflow
                .get_or_insert_with(Default::default)
                .refs
                .insert(ref_id, entry);
        }
    }

    fn remove_entry(&mut self, ref_id: u64) -> Option<SeriesEntry> {
        if let Ok(r32) = u32::try_from(ref_id) {
            self.refs.remove(&r32)
        } else {
            self.overflow
                .as_mut()
                .and_then(|o| o.refs.remove(&ref_id))
        }
    }

    #[inline]
    fn read_nanos(&self, ref_id: u64, slab: &NanosSlab) -> i64 {
        if ref_id < SLAB_MAX_SLOTS as u64 {
            slab.load(ref_id as u32)
        } else {
            self.overflow
                .as_ref()
                .and_then(|o| o.nanos.get(&ref_id))
                .map_or(SLOT_INACTIVE, |a| a.load(Ordering::Acquire))
        }
    }

    #[inline]
    fn write_nanos_existing(&self, ref_id: u64, slab: &NanosSlab, now_ns: i64) -> Option<bool> {
        let now_ns = sanitize_nanos(now_ns);
        if ref_id < SLAB_MAX_SLOTS as u64 {
            slab.write_nanos_if_active(ref_id as u32, now_ns)
        } else if let Some(slot_atomic) = self.overflow.as_ref().and_then(|o| o.nanos.get(&ref_id))
        {
            let mut cur = slot_atomic.load(Ordering::Relaxed);
            while cur != SLOT_INACTIVE && now_ns > cur {
                match slot_atomic.compare_exchange_weak(
                    cur,
                    now_ns,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => return Some(true),
                    Err(actual) => cur = actual,
                }
            }
            if cur == SLOT_INACTIVE {
                None
            } else {
                Some(false)
            }
        } else {
            None
        }
    }

    fn store_nanos_new_or_update(&mut self, ref_id: u64, slab: &NanosSlab, now_ns: i64) -> bool {
        let now_ns = sanitize_nanos(now_ns);
        if ref_id < SLAB_MAX_SLOTS as u64 {
            match slab.write_nanos_if_active(ref_id as u32, now_ns) {
                Some(updated) => updated,
                None => {
                    slab.ensure_and_store(ref_id as u32, now_ns);
                    true
                }
            }
        } else {
            let ov = self.overflow.get_or_insert_with(Default::default);
            if let Some(a) = ov.nanos.get(&ref_id) {
                let mut cur = a.load(Ordering::Relaxed);
                while now_ns > cur {
                    match a.compare_exchange_weak(cur, now_ns, Ordering::Relaxed, Ordering::Relaxed)
                    {
                        Ok(_) => return true,
                        Err(actual) => cur = actual,
                    }
                }
                false
            } else {
                ov.nanos.insert(ref_id, Box::new(AtomicI64::new(now_ns)));
                true
            }
        }
    }

    fn alloc_matches_handle(&mut self, matches: PreAllocDynamicSlice) -> u32 {
        if matches.is_empty() {
            return 0;
        }
        self.custom_matches
            .get_or_insert_with(Default::default)
            .insert(matches)
    }

    fn replace_matches_handle(&mut self, old_handle: u32, new_matches: PreAllocDynamicSlice) -> u32 {
        if new_matches.is_empty() {
            self.free_matches_handle(old_handle);
            return 0;
        }
        if old_handle != 0 {
            if let Some(ref mut cm) = self.custom_matches {
                let idx = (old_handle - 1) as usize;
                if idx < cm.slots.len() && cm.slots[idx].is_some() {
                    cm.slots[idx] = Some(new_matches);
                    return old_handle;
                }
            }
        }
        self.alloc_matches_handle(new_matches)
    }

    fn free_matches_handle(&mut self, handle: u32) {
        if handle == 0 {
            return;
        }
        let all_empty = self
            .custom_matches
            .as_mut()
            .is_some_and(|cm| cm.remove(handle));
        if all_empty {
            self.custom_matches = None;
        }
    }

    fn cleanup_overflow_if_empty(&mut self) {
        if self.overflow.as_ref().is_some_and(|o| o.is_empty()) {
            self.overflow = None;
        }
    }
}

struct SeriesStripe {
    oldest_entry_ns: AtomicI64,
    inner: RwLock<SeriesStripeInner>,
}

impl SeriesStripe {
    fn new(matchers: Arc<Matchers>, cat: Option<Arc<CostAttributionTracker>>) -> Self {
        let num_matchers = matchers.matcher_names().len();
        Self {
            oldest_entry_ns: AtomicI64::new(0),
            inner: RwLock::new(SeriesStripeInner {
                refs: hashbrown::HashMap::with_hasher(FastIntBuildHasher),
                overflow: None,
                custom_matches: None,
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

    #[allow(clippy::too_many_arguments)]
    fn update_series_timestamp(
        &self,
        now_ns: i64,
        series_labels: Option<&Labels>,
        pairs: Option<&[(&str, &str)]>,
        ref_id: u64,
        num_native_histogram_buckets: i64,
        is_otlp: bool,
        slab: &NanosSlab,
        epoch: u16,
        head_entry: Option<&mimir_tsdb::head::SeriesEntry>,
    ) -> bool {
        let now_ns = sanitize_nanos(now_ns);
        let buckets_i32 = num_native_histogram_buckets.clamp(i32::MIN as i64, i32::MAX as i64) as i32;
        let mut need_write = false;
        let mut created = false;
        let mut entry_time_set = false;

        {
            let guard = self.inner.read();
            if let Some(entry) = guard.get_entry(ref_id) {
                match guard.write_nanos_existing(ref_id, slab, now_ns) {
                    Some(true) => entry_time_set = true,
                    Some(false) => {}
                    None => need_write = true,
                }
                if entry.num_native_histogram_buckets != buckets_i32 {
                    need_write = true;
                } else if let Some(he) = head_entry {
                    if buckets_i32 < 0 && ref_id < SLAB_MAX_SLOTS as u64 {
                        he.set_active_cache(pack_active_cache(
                            epoch,
                            true,
                            entry.is_otlp(),
                            ref_id as u32,
                        ));
                    } else if buckets_i32 >= 0 {
                        he.set_active_cache(0);
                    }
                }
            } else {
                need_write = true;
            }
        }

        if need_write {
            let mut guard = self.inner.write();
            if guard.get_entry(ref_id).is_some() {
                if guard.store_nanos_new_or_update(ref_id, slab, now_ns) {
                    entry_time_set = true;
                }
                let (old_buckets, matches_handle, now_is_otlp) = {
                    let entry = guard.get_entry_mut(ref_id).expect("checked is_some");
                    let old_b = entry.num_native_histogram_buckets;
                    if old_b != buckets_i32 {
                        entry.num_native_histogram_buckets = buckets_i32;
                    }
                    (old_b, entry.matches_handle, entry.is_otlp())
                };
                if let Some(he) = head_entry {
                    if buckets_i32 < 0 && ref_id < SLAB_MAX_SLOTS as u64 {
                        he.set_active_cache(pack_active_cache(
                            epoch,
                            true,
                            now_is_otlp,
                            ref_id as u32,
                        ));
                    } else if buckets_i32 >= 0 {
                        he.set_active_cache(0);
                    }
                }
                if old_buckets != buckets_i32 {
                    let guard_ref = &mut *guard;
                    if old_buckets >= 0 && buckets_i32 < 0 {
                        guard_ref.active_native_histograms =
                            guard_ref.active_native_histograms.saturating_sub(1);
                        guard_ref.active_native_histogram_buckets = guard_ref
                            .active_native_histogram_buckets
                            .saturating_sub(old_buckets as u32);
                        if let Some(cm) = guard_ref.custom_matches.as_ref() {
                            if let Some(matches) = cm.get(matches_handle) {
                                for idx in matches.iter() {
                                    if let Some(v) =
                                        guard_ref.active_matching_native_histograms.get_mut(idx)
                                    {
                                        *v = v.saturating_sub(1);
                                    }
                                    if let Some(v) = guard_ref
                                        .active_matching_native_histogram_buckets
                                        .get_mut(idx)
                                    {
                                        *v = v.saturating_sub(old_buckets as u32);
                                    }
                                }
                            }
                        }
                    } else if old_buckets < 0 && buckets_i32 >= 0 {
                        guard_ref.active_native_histograms += 1;
                        guard_ref.active_native_histogram_buckets += buckets_i32 as u32;
                        if let Some(cm) = guard_ref.custom_matches.as_ref() {
                            if let Some(matches) = cm.get(matches_handle) {
                                for idx in matches.iter() {
                                    if let Some(v) =
                                        guard_ref.active_matching_native_histograms.get_mut(idx)
                                    {
                                        *v += 1;
                                    }
                                    if let Some(v) = guard_ref
                                        .active_matching_native_histogram_buckets
                                        .get_mut(idx)
                                    {
                                        *v += buckets_i32 as u32;
                                    }
                                }
                            }
                        }
                    } else if old_buckets >= 0 && buckets_i32 >= 0 {
                        let delta = buckets_i32 - old_buckets;
                        if delta >= 0 {
                            guard_ref.active_native_histogram_buckets += delta as u32;
                            if let Some(cm) = guard_ref.custom_matches.as_ref() {
                                if let Some(matches) = cm.get(matches_handle) {
                                    for idx in matches.iter() {
                                        if let Some(v) = guard_ref
                                            .active_matching_native_histogram_buckets
                                            .get_mut(idx)
                                        {
                                            *v += delta as u32;
                                        }
                                    }
                                }
                            }
                        } else {
                            let sub = (-delta) as u32;
                            guard_ref.active_native_histogram_buckets =
                                guard_ref.active_native_histogram_buckets.saturating_sub(sub);
                            if let Some(cm) = guard_ref.custom_matches.as_ref() {
                                if let Some(matches) = cm.get(matches_handle) {
                                    for idx in matches.iter() {
                                        if let Some(v) = guard_ref
                                            .active_matching_native_histogram_buckets
                                            .get_mut(idx)
                                        {
                                            *v = v.saturating_sub(sub);
                                        }
                                    }
                                }
                            }
                        }
                    }

                    if let Some(ref cat) = guard_ref.cat {
                        let lbls_owned;
                        let lbls = match series_labels {
                            Some(l) => l,
                            None => {
                                lbls_owned = Labels::from_pairs(pairs.unwrap_or(&[]).iter().copied());
                                &lbls_owned
                            }
                        };
                        cat.decrement(lbls, old_buckets as i64);
                        cat.increment(
                            lbls,
                            UNIX_EPOCH + Duration::from_nanos(now_ns.max(0) as u64),
                            buckets_i32 as i64,
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
                if buckets_i32 >= 0 {
                    guard.active_native_histograms += 1;
                    guard.active_native_histogram_buckets += buckets_i32 as u32;
                    for idx in matches.iter() {
                        if let Some(v) = guard.active_matching_native_histograms.get_mut(idx) {
                            *v += 1;
                        }
                        if let Some(v) = guard.active_matching_native_histogram_buckets.get_mut(idx)
                        {
                            *v += buckets_i32 as u32;
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
                        buckets_i32 as i64,
                    );
                }

                let matches_handle = guard.alloc_matches_handle(matches);
                guard.insert_entry(
                    ref_id,
                    SeriesEntry {
                        num_native_histogram_buckets: buckets_i32,
                        matches_handle,
                        flags,
                    },
                );
                guard.store_nanos_new_or_update(ref_id, slab, now_ns);
                if let Some(he) = head_entry {
                    if buckets_i32 < 0 && ref_id < SLAB_MAX_SLOTS as u64 {
                        he.set_active_cache(pack_active_cache(
                            epoch,
                            true,
                            is_otlp,
                            ref_id as u32,
                        ));
                    } else {
                        he.set_active_cache(0);
                    }
                }
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

    fn contains_ref_at(&self, ref_id: u64, cutoff_ns: i64, slab: &NanosSlab) -> bool {
        let guard = self.inner.read();
        if guard.get_entry(ref_id).is_none() {
            return false;
        }
        let ts = guard.read_nanos(ref_id, slab);
        ts != SLOT_INACTIVE && ts >= cutoff_ns
    }

    fn native_histogram_buckets(&self, ref_id: u64) -> Option<u64> {
        let guard = self.inner.read();
        let e = guard.get_entry(ref_id)?;
        if e.num_native_histogram_buckets >= 0 {
            Some(e.num_native_histogram_buckets as u64)
        } else {
            None
        }
    }

    fn mark_deleted(&self, ref_id: u64, labels: &Labels, deleted: &DeletedSeries) {
        let mut guard = self.inner.write();
        if let Some(entry) = guard.get_entry_mut(ref_id) {
            entry.mark_deleted();
            deleted.add(ref_id, labels);
        }
    }

    fn remove(
        &self,
        ref_id: u64,
        idx: Option<&dyn SeriesIndexReader>,
        deleted: &DeletedSeries,
        slab: &NanosSlab,
    ) {
        let mut guard = self.inner.write();
        let Some(entry) = guard.remove_entry(ref_id) else {
            return;
        };
        if ref_id < SLAB_MAX_SLOTS as u64 {
            slab.mark_inactive(ref_id as u32);
        } else if let Some(ref mut ov) = guard.overflow {
            ov.nanos.remove(&ref_id);
        }
        guard.active = guard.active.saturating_sub(1);
        if entry.is_otlp() {
            guard.active_otlp = guard.active_otlp.saturating_sub(1);
        }
        if let Some(ref cat) = guard.cat {
            match idx.and_then(|r| r.series_labels(ref_id)) {
                Some(lbls) => cat.decrement(&lbls, entry.num_native_histogram_buckets as i64),
                None => guard.attribution_failures += 1.0,
            }
        }
        let guard_ref = &mut *guard;
        if let Some(cm) = guard_ref.custom_matches.as_ref() {
            if let Some(matches) = cm.get(entry.matches_handle) {
                for match_idx in matches.iter() {
                    if let Some(v) = guard_ref.active_matching.get_mut(match_idx) {
                        *v = v.saturating_sub(1);
                    }
                }
            }
        }
        if entry.num_native_histogram_buckets >= 0 {
            let b = entry.num_native_histogram_buckets as u32;
            guard_ref.active_native_histograms =
                guard_ref.active_native_histograms.saturating_sub(1);
            guard_ref.active_native_histogram_buckets =
                guard_ref.active_native_histogram_buckets.saturating_sub(b);
            if let Some(cm) = guard_ref.custom_matches.as_ref() {
                if let Some(matches) = cm.get(entry.matches_handle) {
                    for match_idx in matches.iter() {
                        if let Some(v) =
                            guard_ref.active_matching_native_histograms.get_mut(match_idx)
                        {
                            *v = v.saturating_sub(1);
                        }
                        if let Some(v) = guard_ref
                            .active_matching_native_histogram_buckets
                            .get_mut(match_idx)
                        {
                            *v = v.saturating_sub(b);
                        }
                    }
                }
            }
        }
        guard_ref.free_matches_handle(entry.matches_handle);
        guard_ref.cleanup_overflow_if_empty();
        deleted.purge(ref_id);
    }

    fn reload_config(
        &self,
        new_matchers: &Arc<Matchers>,
        new_cat: &Option<Arc<CostAttributionTracker>>,
        matchers_changed: bool,
        cat_changed: bool,
        idx: Option<&dyn SeriesIndexReader>,
        slab: &NanosSlab,
    ) {
        // Phase 1: pre-fetch refs under lock.
        let refs: Vec<u64> = {
            let g = self.inner.read();
            let ov_len = g.overflow.as_ref().map_or(0, |o| o.refs.len());
            let mut out = Vec::with_capacity(g.refs.len() + ov_len);
            out.extend(g.refs.keys().map(|&r| r as u64));
            if let Some(ref ov) = g.overflow {
                out.extend(ov.refs.keys().copied());
            }
            out
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
            guard.matchers = Arc::clone(new_matchers);
            guard.active_matching = vec![0; num_m];
            guard.active_matching_native_histograms = vec![0; num_m];
            guard.active_matching_native_histogram_buckets = vec![0; num_m];
        }

        if matchers_changed || cat_changed {
            let ov_len = guard.overflow.as_ref().map_or(0, |o| o.refs.len());
            let mut updates: Vec<(u64, PreAllocDynamicSlice)> =
                Vec::with_capacity(guard.refs.len() + ov_len);
            let mut failures = 0.0;

            let mut process_entry = |ref_id: u64, buckets: i32| {
                if let Some((lbls, matches)) = resolved.get(&ref_id) {
                    if cat_changed {
                        if let Some(ref cat) = new_cat {
                            let ts = guard.read_nanos(ref_id, slab);
                            cat.increment(
                                lbls,
                                UNIX_EPOCH + Duration::from_nanos(ts.max(0) as u64),
                                buckets as i64,
                            );
                        }
                    }
                    if matchers_changed {
                        updates.push((ref_id, matches.clone()));
                    }
                    return;
                }

                if let Some(lbls) = idx.and_then(|r| r.series_labels(ref_id)) {
                    if cat_changed {
                        if let Some(ref cat) = new_cat {
                            let ts = guard.read_nanos(ref_id, slab);
                            cat.increment(
                                &lbls,
                                UNIX_EPOCH + Duration::from_nanos(ts.max(0) as u64),
                                buckets as i64,
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
            };

            let small_items: Vec<(u64, i32)> = guard
                .refs
                .iter()
                .map(|(&r, e)| (r as u64, e.num_native_histogram_buckets))
                .collect();
            for (ref_id, buckets) in small_items {
                process_entry(ref_id, buckets);
            }
            if let Some(ref ov) = guard.overflow {
                let large_items: Vec<(u64, i32)> = ov
                    .refs
                    .iter()
                    .map(|(&r, e)| (r, e.num_native_histogram_buckets))
                    .collect();
                for (ref_id, buckets) in large_items {
                    process_entry(ref_id, buckets);
                }
            }
            guard.attribution_failures += failures;

            if matchers_changed {
                for (ref_id, matches) in updates {
                    let Some((old_handle, buckets)) = guard
                        .get_entry(ref_id)
                        .map(|e| (e.matches_handle, e.num_native_histogram_buckets))
                    else {
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
                    let new_handle = guard.replace_matches_handle(old_handle, matches);
                    if let Some(e) = guard.get_entry_mut(ref_id) {
                        e.matches_handle = new_handle;
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
        slab: &NanosSlab,
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

        let check_ref = |ref_id: u64,
                         ov: Option<&OverflowStripeData>,
                         to_remove: &mut Vec<u64>,
                         new_oldest: &mut i64| {
            if ref_id < SLAB_MAX_SLOTS as u64 {
                match slab.try_purge_slot(ref_id as u32, keep_until_ns) {
                    PurgeSlotOutcome::Inactive | PurgeSlotOutcome::Purged => {
                        to_remove.push(ref_id);
                    }
                    PurgeSlotOutcome::Kept(ts) => {
                        if ts < *new_oldest {
                            *new_oldest = ts;
                        }
                    }
                }
                return;
            }
            let ts = ov
                .and_then(|o| o.nanos.get(&ref_id))
                .map_or(SLOT_INACTIVE, |a| a.load(Ordering::Acquire));
            if ts < keep_until_ns {
                to_remove.push(ref_id);
            } else if ts < *new_oldest {
                *new_oldest = ts;
            }
        };

        let ov_ref = guard.overflow.as_deref();
        for &r32 in guard.refs.keys() {
            check_ref(r32 as u64, ov_ref, &mut to_remove, &mut new_oldest);
        }
        if let Some(ov) = ov_ref {
            for &r64 in ov.refs.keys() {
                check_ref(r64, Some(ov), &mut to_remove, &mut new_oldest);
            }
        }

        for ref_id in to_remove {
            if let Some(entry) = guard.remove_entry(ref_id) {
                if ref_id >= SLAB_MAX_SLOTS as u64 {
                    if let Some(ref mut ov) = guard.overflow {
                        ov.nanos.remove(&ref_id);
                    }
                }
                guard.free_matches_handle(entry.matches_handle);
                if let Some(ref cat) = guard.cat {
                    match idx.and_then(|r| r.series_labels(ref_id)) {
                        Some(lbls) => cat.decrement(&lbls, entry.num_native_histogram_buckets as i64),
                        None => guard.attribution_failures += 1.0,
                    }
                }
                if entry.is_deleted() {
                    deleted.purge(ref_id);
                }
            }
        }

        let SeriesStripeInner {
            refs,
            overflow,
            custom_matches,
            active,
            active_otlp,
            active_matching,
            active_native_histograms,
            active_matching_native_histograms,
            active_native_histogram_buckets,
            active_matching_native_histogram_buckets,
            ..
        } = &mut *guard;

        let tally_entry = |entry: &SeriesEntry,
                           active: &mut u32,
                           active_otlp: &mut u32,
                           active_matching: &mut Vec<u32>,
                           active_native_histograms: &mut u32,
                           active_matching_native_histograms: &mut Vec<u32>,
                           active_native_histogram_buckets: &mut u32,
                           active_matching_native_histogram_buckets: &mut Vec<u32>| {
            *active += 1;
            if entry.is_otlp() {
                *active_otlp += 1;
            }
            let buckets = entry.num_native_histogram_buckets;
            if buckets >= 0 {
                *active_native_histograms += 1;
                *active_native_histogram_buckets += buckets as u32;
            }
            if let Some(cm) = custom_matches.as_deref() {
                if let Some(matches) = cm.get(entry.matches_handle) {
                    for match_idx in matches.iter() {
                        if let Some(v) = active_matching.get_mut(match_idx) {
                            *v += 1;
                        }
                        if buckets >= 0 {
                            if let Some(v) = active_matching_native_histograms.get_mut(match_idx) {
                                *v += 1;
                            }
                            if let Some(v) =
                                active_matching_native_histogram_buckets.get_mut(match_idx)
                            {
                                *v += buckets as u32;
                            }
                        }
                    }
                }
            }
        };

        for entry in refs.values() {
            tally_entry(
                entry,
                active,
                active_otlp,
                active_matching,
                active_native_histograms,
                active_matching_native_histograms,
                active_native_histogram_buckets,
                active_matching_native_histogram_buckets,
            );
        }
        if let Some(ov) = overflow.as_deref() {
            for entry in ov.refs.values() {
                tally_entry(
                    entry,
                    active,
                    active_otlp,
                    active_matching,
                    active_native_histograms,
                    active_matching_native_histograms,
                    active_native_histogram_buckets,
                    active_matching_native_histogram_buckets,
                );
            }
        }

        let refs_len = guard.refs.len();
        if guard.refs.is_empty() {
            if guard.refs.capacity() > 0 {
                guard.refs.shrink_to_fit();
            }
        } else if guard.refs.capacity() > (refs_len * 4).max(64) {
            guard.refs.shrink_to(refs_len * 2);
        }
        guard.cleanup_overflow_if_empty();

        if new_oldest == i64::MAX {
            self.oldest_entry_ns.store(0, Ordering::Relaxed);
        } else {
            self.oldest_entry_ns.store(new_oldest, Ordering::Relaxed);
        }
    }

    fn clear(&self, slab: &NanosSlab) {
        let mut guard = self.inner.write();
        for &r32 in guard.refs.keys() {
            if (r32 as u64) < SLAB_MAX_SLOTS as u64 {
                slab.mark_inactive(r32);
            }
        }
        guard.refs.clear();
        guard.overflow = None;
        guard.custom_matches = None;
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

/// 4096-stripe active series & native histogram bucket tracker for a single tenant.
pub struct ActiveSeries {
    stripes: Vec<SeriesStripe>,
    slab: NanosSlab,
    epoch: AtomicU16,
    deleted: DeletedSeries,
    config_mtx: RwLock<(Arc<Matchers>, Option<Arc<CostAttributionTracker>>)>,
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
        cat: Option<Arc<CostAttributionTracker>>,
    ) -> Self {
        let matchers = Arc::new(matchers);
        let mut stripes = Vec::with_capacity(NUM_STRIPES);
        for _ in 0..NUM_STRIPES {
            stripes.push(SeriesStripe::new(Arc::clone(&matchers), cat.clone()));
        }
        Self {
            stripes,
            slab: NanosSlab::new(),
            epoch: AtomicU16::new(1),
            deleted: DeletedSeries::default(),
            config_mtx: RwLock::new((matchers, cat)),
            timeout,
        }
    }

    pub fn current_matcher_names(&self) -> Vec<String> {
        self.config_mtx.read().0.matcher_names().to_vec()
    }

    pub fn current_canonical_string(&self) -> String {
        self.config_mtx.read().0.config().canonical_string.clone()
    }

    pub fn matchers_differ(&self, ct_cfg: &CustomTrackersConfig) -> bool {
        ct_cfg.as_str() != self.config_mtx.read().0.config().as_str()
    }

    /// Returns true if `cat` is not the same tracker instance currently in use
    /// (Go: `ActiveSeries.CostAttributionDiffers`).
    pub fn cost_attribution_differs(&self, cat: Option<&Arc<CostAttributionTracker>>) -> bool {
        let g = self.config_mtx.read();
        match (cat, g.1.as_ref()) {
            (None, None) => false,
            (Some(a), Some(b)) => !Arc::ptr_eq(a, b),
            _ => true,
        }
    }

    pub fn cost_attribution_tracker(&self) -> Option<Arc<CostAttributionTracker>> {
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
        cat: Option<Arc<CostAttributionTracker>>,
        matchers_changed: bool,
        cat_changed: bool,
        idx: Option<&dyn SeriesIndexReader>,
    ) {
        let matchers = Arc::new(matchers);
        let mut guard = self.config_mtx.write();
        for stripe in &self.stripes {
            stripe.reload_config(
                &matchers,
                &cat,
                matchers_changed,
                cat_changed,
                idx,
                &self.slab,
            );
        }
        if matchers_changed {
            guard.0 = matchers;
        }
        if cat_changed {
            guard.1 = cat;
        }
    }

    #[inline]
    fn try_fast_active_cache_update(
        &self,
        head_entry: &mimir_tsdb::head::SeriesEntry,
        ref_id: u64,
        now_ns: i64,
        _is_otlp: bool,
    ) -> bool {
        let now_ns = sanitize_nanos(now_ns);
        let packed = head_entry.active_cache();
        if packed != 0 {
            let (c_epoch, c_flags, slot) = unpack_active_cache(packed);
            let cur_epoch = self.epoch.load(Ordering::Acquire);
            if c_epoch == cur_epoch && (c_flags & CACHE_FLAG_PLAIN_FLOAT) != 0 {
                match self.slab.write_nanos_if_active(slot, now_ns) {
                    Some(true) => {
                        let stripe = &self.stripes[(ref_id as usize) & STRIPE_MASK];
                        let mut prev_oldest = stripe.oldest_entry_ns.load(Ordering::Relaxed);
                        while prev_oldest > 0 && now_ns < prev_oldest {
                            match stripe.oldest_entry_ns.compare_exchange_weak(
                                prev_oldest,
                                0,
                                Ordering::Relaxed,
                                Ordering::Relaxed,
                            ) {
                                Ok(_) => break,
                                Err(actual) => prev_oldest = actual,
                            }
                        }
                        return true;
                    }
                    Some(false) => return true,
                    None => {}
                }
            }
        }
        false
    }

    #[inline]
    #[allow(clippy::too_many_arguments)]
    pub fn update_series_with_entry(
        &self,
        entry: Option<&mimir_tsdb::head::SeriesEntry>,
        pairs: &[(&str, &str)],
        _hash: u64,
        ref_id: u64,
        now: SystemTime,
        num_native_histogram_buckets: i64,
        is_otlp: bool,
    ) {
        let now_ns = system_time_to_unix_nanos(now);
        if num_native_histogram_buckets < 0 && now_ns != SLOT_INACTIVE {
            if let Some(head_entry) = entry {
                if self.try_fast_active_cache_update(head_entry, ref_id, now_ns, is_otlp) {
                    return;
                }
            }
        }

        let epoch = self.epoch.load(Ordering::Acquire);
        let stripe_id = (ref_id as usize) & STRIPE_MASK;
        let created = self.stripes[stripe_id].update_series_timestamp(
            now_ns,
            None,
            Some(pairs),
            ref_id,
            num_native_histogram_buckets,
            is_otlp,
            &self.slab,
            epoch,
            entry,
        );
        if created && !self.deleted.is_empty() {
            let lbls = Labels::from_pairs(pairs.iter().copied());
            if let Some(deleted_ref) = self.deleted.find(lbls.as_bytes()) {
                let d_stripe = (deleted_ref as usize) & STRIPE_MASK;
                self.stripes[d_stripe].remove(deleted_ref, None, &self.deleted, &self.slab);
            }
        }
    }

    #[inline]
    #[allow(clippy::too_many_arguments)]
    pub fn update_series_with_entry_encoded(
        &self,
        entry: Option<&mimir_tsdb::head::SeriesEntry>,
        encoded_labels: &[u8],
        _hash: u64,
        ref_id: u64,
        now: SystemTime,
        num_native_histogram_buckets: i64,
        is_otlp: bool,
    ) {
        let now_ns = system_time_to_unix_nanos(now);
        if num_native_histogram_buckets < 0 && now_ns != SLOT_INACTIVE {
            if let Some(head_entry) = entry {
                if self.try_fast_active_cache_update(head_entry, ref_id, now_ns, is_otlp) {
                    return;
                }
            }
        }

        let epoch = self.epoch.load(Ordering::Acquire);
        let stripe_id = (ref_id as usize) & STRIPE_MASK;
        let owned_labels;
        let series_labels = match entry {
            Some(e) => &e.labels,
            None => {
                owned_labels =
                    Labels::from_raw_stringlabels_bytes(bytes::Bytes::copy_from_slice(encoded_labels));
                &owned_labels
            }
        };
        let created = self.stripes[stripe_id].update_series_timestamp(
            now_ns,
            Some(series_labels),
            None,
            ref_id,
            num_native_histogram_buckets,
            is_otlp,
            &self.slab,
            epoch,
            entry,
        );
        if created && !self.deleted.is_empty() {
            if let Some(deleted_ref) = self.deleted.find(encoded_labels) {
                let d_stripe = (deleted_ref as usize) & STRIPE_MASK;
                self.stripes[d_stripe].remove(deleted_ref, None, &self.deleted, &self.slab);
            }
        }
    }

    pub fn update_series(
        &self,
        pairs: &[(&str, &str)],
        hash: u64,
        ref_id: u64,
        now: SystemTime,
        num_native_histogram_buckets: i64,
        is_otlp: bool,
    ) {
        self.update_series_with_entry(
            None,
            pairs,
            hash,
            ref_id,
            now,
            num_native_histogram_buckets,
            is_otlp,
        );
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
        let epoch = self.epoch.load(Ordering::Acquire);
        let stripe_id = (ref_id as usize) & STRIPE_MASK;
        let created = self.stripes[stripe_id].update_series_timestamp(
            now_ns,
            Some(series),
            None,
            ref_id,
            num_native_histogram_buckets,
            is_otlp,
            &self.slab,
            epoch,
            None,
        );
        if created && !self.deleted.is_empty() {
            if let Some(deleted_ref) = self.deleted.find(series.as_bytes()) {
                let d_stripe = (deleted_ref as usize) & STRIPE_MASK;
                self.stripes[d_stripe].remove(deleted_ref, idx, &self.deleted, &self.slab);
            }
        }
    }

    pub fn post_deletion(&self, deleted: &[(u64, Labels)]) {
        for (ref_id, lbls) in deleted {
            if self.contains_ref(*ref_id) {
                let stripe_id = (*ref_id as usize) & STRIPE_MASK;
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
            stripe.purge(keep_until_ns, idx, &self.deleted, &self.slab);
        }
        self.slab.reclaim_empty_pages();
    }

    /// Lock-free check against `NanosSlab` whether `ref_id` is active at `cutoff_ns`
    /// (`ts != SLOT_INACTIVE && ts >= cutoff_ns`, including `cutoff_ns == i64::MIN`).
    /// Falls back to the stripe read lock only when `ref_id >= SLAB_MAX_SLOTS`.
    #[inline]
    pub fn is_ref_active_fast(&self, ref_id: u64, cutoff_ns: i64) -> bool {
        if ref_id < SLAB_MAX_SLOTS as u64 {
            let ts = self.slab.load(ref_id as u32);
            return ts != SLOT_INACTIVE && ts >= cutoff_ns;
        }
        self.stripes[(ref_id as usize) & STRIPE_MASK].contains_ref_at(
            ref_id,
            cutoff_ns,
            &self.slab,
        )
    }

    #[inline]
    pub fn contains_ref_at(&self, ref_id: u64, cutoff_ns: i64) -> bool {
        self.is_ref_active_fast(ref_id, cutoff_ns)
    }

    #[inline]
    pub fn contains_ref(&self, ref_id: u64) -> bool {
        self.is_ref_active_fast(ref_id, i64::MIN)
    }

    /// Buckets `(small, large)` series refs by `ref_id & STRIPE_MASK` for fast lock-free-of-postings counting.
    pub fn bucket_refs_by_stripe(small: &[u32], large: &[u64]) -> Vec<Vec<u64>> {
        let total_len = small.len() + large.len();
        let per_stripe = (total_len / NUM_STRIPES).max(4);
        let mut buckets: Vec<Vec<u64>> = (0..NUM_STRIPES)
            .map(|_| Vec::with_capacity(per_stripe))
            .collect();
        for &r in small {
            let r64 = r as u64;
            buckets[(r64 as usize) & STRIPE_MASK].push(r64);
        }
        for &r in large {
            buckets[(r as usize) & STRIPE_MASK].push(r);
        }
        buckets
    }

    /// Counts active series refs from pre-bucketed stripe lists.
    pub fn count_active_in_buckets(&self, buckets: &[Vec<u64>]) -> u64 {
        let mut count = 0u64;
        for (s_idx, refs) in buckets.iter().enumerate() {
            if refs.is_empty() {
                continue;
            }
            let guard = self.stripes[s_idx].inner.read();
            for &r in refs {
                if guard.get_entry(r).is_some() {
                    count += 1;
                }
            }
        }
        count
    }

    /// Counts how many series refs in `(small, large)` exist in `ActiveSeries`
    /// using the lock-free `NanosSlab` fast path for `ref_id < SLAB_MAX_SLOTS`.
    pub fn count_active_in_slices(&self, small: &[u32], large: &[u64]) -> u64 {
        let mut count = 0u64;
        for &r in small {
            if self.is_ref_active_fast(r as u64, i64::MIN) {
                count += 1;
            }
        }
        for &r in large {
            if self.is_ref_active_fast(r, i64::MIN) {
                count += 1;
            }
        }
        count
    }

    pub fn native_histogram_buckets(&self, ref_id: u64) -> Option<u64> {
        self.stripes[(ref_id as usize) & STRIPE_MASK].native_histogram_buckets(ref_id)
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
        let cfg_guard = self.config_mtx.read();
        let n = cfg_guard.0.matcher_names().len();
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
        self.stripes[(ref_id as usize) & STRIPE_MASK].remove(
            ref_id,
            idx,
            &self.deleted,
            &self.slab,
        );
    }

    pub fn clear(&self) {
        let mut cur = self.epoch.load(Ordering::Acquire);
        loop {
            let next = if cur == u16::MAX { 1 } else { cur + 1 };
            match self
                .epoch
                .compare_exchange_weak(cur, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => break,
                Err(actual) => cur = actual,
            }
        }
        for stripe in &self.stripes {
            stripe.clear(&self.slab);
        }
        self.deleted.clear();
        self.slab.reclaim_empty_pages();
    }

    /// Returns `(allocated_pages, resident_pages, reclaimed_pages)` in `NanosSlab` for diagnostics/tests.
    #[doc(hidden)]
    pub fn debug_slab_stats(&self) -> (usize, usize, usize) {
        self.slab.stats()
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

    #[test]
    fn series_entry_layout_is_compact_and_deleted_shares_bytes() {
        assert!(
            std::mem::size_of::<SeriesEntry>() <= 16,
            "SeriesEntry grew unexpectedly: {}",
            std::mem::size_of::<SeriesEntry>()
        );
        let deleted = DeletedSeries::default();
        let lbls = Labels::from_pairs([("__name__", "up"), ("job", "api")]);
        deleted.add(42, &lbls);
        assert!(!deleted.is_empty());
        assert_eq!(deleted.find(lbls.as_bytes()), Some(42));
        deleted.purge(42);
        assert!(deleted.is_empty());
    }

    #[test]
    fn nanos_slab_fast_path_purge_clear_and_flip_parity() {
        let dir = tempfile::tempdir().unwrap();
        let mut opts = mimir_tsdb::head::HeadOptions::default();
        if cfg!(miri) {
            opts.stripe_size = 16;
            opts.enable_exemplar_storage = false;
            opts.wal_replay_concurrency = 1;
            opts.wal_segment_size = 64 * 1024;
            opts.head_chunks_write_buffer_size = 4096;
        }
        let head = mimir_tsdb::head::Head::open(dir.path(), opts, i64::MIN).unwrap();
        let pairs = [("__name__", "up"), ("job", "api")];
        {
            let mut app = head.appender();
            app.append_float(None, &pairs, 1_000, 1.0).unwrap();
            app.commit().unwrap();
        }
        let head_entry = head.get_entry_by_sorted_pairs(&pairs).unwrap();
        assert_eq!(head_entry.active_cache(), 0);

        let tracker = ActiveSeries::new(Vec::new(), Duration::from_secs(60));
        let t0 = UNIX_EPOCH + Duration::from_secs(1000);
        // 1. Slow path populates NanosSlab & head_entry.active_cache
        tracker.update_series_with_entry(
            Some(&head_entry),
            &pairs,
            head_entry.hash,
            head_entry.ref_id,
            t0,
            -1,
            false,
        );
        assert_ne!(head_entry.active_cache(), 0);
        assert_eq!(tracker.active(), (1, 0, 0, 0));

        // 2. Fast path updates slab timestamp without touching Stripe lock
        let t1 = t0 + Duration::from_secs(30);
        tracker.update_series_with_entry(
            Some(&head_entry),
            &pairs,
            head_entry.hash,
            head_entry.ref_id,
            t1,
            -1,
            false,
        );
        assert_eq!(tracker.active(), (1, 0, 0, 0));

        // 3. Purge at t0 + 10s (keep_until = t0 + 10s -> kept because ts == t1 == t0 + 30s!)
        tracker.purge(t0 + Duration::from_secs(70));
        assert_eq!(tracker.active(), (1, 0, 0, 0));

        // 4. Purge past t1 -> evicted & slab slot marked SLOT_INACTIVE
        tracker.purge(t1 + Duration::from_secs(61));
        assert_eq!(tracker.active(), (0, 0, 0, 0));

        // 5. Fast path sees SLOT_INACTIVE and falls back to slow path -> re-activates!
        let t2 = t1 + Duration::from_secs(70);
        tracker.update_series_with_entry(
            Some(&head_entry),
            &pairs,
            head_entry.hash,
            head_entry.ref_id,
            t2,
            -1,
            false,
        );
        assert_eq!(tracker.active(), (1, 0, 0, 0));

        // 6. clear() bumps epoch; stale active_cache misses and re-inserts with new slot
        let old_packed = head_entry.active_cache();
        tracker.clear();
        assert_eq!(tracker.active(), (0, 0, 0, 0));
        let t3 = t2 + Duration::from_secs(10);
        tracker.update_series_with_entry(
            Some(&head_entry),
            &pairs,
            head_entry.hash,
            head_entry.ref_id,
            t3,
            -1,
            false,
        );
        assert_eq!(tracker.active(), (1, 0, 0, 0));
        assert_ne!(head_entry.active_cache(), old_packed);

        // 7. Switching to native histogram clears active_cache
        tracker.update_series_with_entry(
            Some(&head_entry),
            &pairs,
            head_entry.hash,
            head_entry.ref_id,
            t3 + Duration::from_secs(1),
            8,
            false,
        );
        assert_eq!(head_entry.active_cache(), 0);
        assert_eq!(tracker.active(), (1, 0, 1, 8));

        head.close().unwrap();
    }

    #[test]
    fn nanos_slab_ref_id_slots_and_is_ref_active_fast_parity() {
        let dir = tempfile::tempdir().unwrap();
        let mut opts = mimir_tsdb::head::HeadOptions::default();
        if cfg!(miri) {
            opts.stripe_size = 16;
            opts.enable_exemplar_storage = false;
            opts.wal_replay_concurrency = 1;
            opts.wal_segment_size = 64 * 1024;
            opts.head_chunks_write_buffer_size = 4096;
        }
        let head = mimir_tsdb::head::Head::open(dir.path(), opts, i64::MIN).unwrap();
        let p1 = [("__name__", "m1"), ("job", "api")];
        let p2 = [("__name__", "m2"), ("job", "api")];
        {
            let mut app = head.appender();
            app.append_float(None, &p1, 1_000, 1.0).unwrap();
            app.append_float(None, &p2, 1_000, 2.0).unwrap();
            app.commit().unwrap();
        }
        let e1 = head.get_entry_by_sorted_pairs(&p1).unwrap();
        let e2 = head.get_entry_by_sorted_pairs(&p2).unwrap();

        let tracker = ActiveSeries::new(Vec::new(), Duration::from_secs(60));
        let t0 = UNIX_EPOCH + Duration::from_secs(1000);
        let t0_ns = system_time_to_unix_nanos(t0);
        assert!(!tracker.is_ref_active_fast(e1.ref_id, i64::MIN));
        assert!(!tracker.is_ref_active_fast(e1.ref_id, t0_ns));

        tracker.update_series_with_entry(Some(&e1), &p1, e1.hash, e1.ref_id, t0, -1, false);
        let packed1 = e1.active_cache();
        let (_, _, slot1) = unpack_active_cache(packed1);
        assert_eq!(slot1, e1.ref_id as u32);
        assert!(tracker.is_ref_active_fast(e1.ref_id, i64::MIN));
        assert!(tracker.is_ref_active_fast(e1.ref_id, t0_ns));
        assert!(!tracker.is_ref_active_fast(e1.ref_id, t0_ns + 1));

        // Purge e1 -> slot marked inactive, is_ref_active_fast returns false for both i64::MIN and cutoff
        tracker.purge(t0 + Duration::from_secs(61));
        assert_eq!(tracker.active(), (0, 0, 0, 0));
        assert!(!tracker.is_ref_active_fast(e1.ref_id, i64::MIN));
        assert!(!tracker.is_ref_active_fast(e1.ref_id, t0_ns));

        // Insert e2 -> gets its own ref_id slot
        let t1 = t0 + Duration::from_secs(70);
        tracker.update_series_with_entry(Some(&e2), &p2, e2.hash, e2.ref_id, t1, -1, false);
        let packed2 = e2.active_cache();
        let (_, _, slot2) = unpack_active_cache(packed2);
        assert_eq!(slot2, e2.ref_id as u32);
        assert!(tracker.is_ref_active_fast(e2.ref_id, i64::MIN));

        // Stale active_cache on e1 must NOT update e2, and must re-insert e1!
        tracker.update_series_with_entry(
            Some(&e1),
            &p1,
            e1.hash,
            e1.ref_id,
            t1 + Duration::from_secs(1),
            -1,
            false,
        );
        assert_eq!(tracker.active(), (2, 0, 0, 0));
        assert!(tracker.is_ref_active_fast(e1.ref_id, i64::MIN));
        assert!(tracker.is_ref_active_fast(e2.ref_id, i64::MIN));

        // Overflow refs (>= SLAB_MAX_SLOTS and > u32::MAX) also preserve full semantics
        let high_slab_ref = (SLAB_MAX_SLOTS as u64) + 7;
        let high_u64_ref = (u32::MAX as u64) + 99;
        tracker.update_series(&p1, 1, high_slab_ref, t1, -1, false);
        tracker.update_series(&p2, 2, high_u64_ref, t1, 5, true);
        assert!(tracker.is_ref_active_fast(high_slab_ref, i64::MIN));
        assert!(tracker.is_ref_active_fast(high_u64_ref, i64::MIN));
        assert!(!tracker.is_ref_active_fast(high_u64_ref, system_time_to_unix_nanos(t1) + 1));
        assert_eq!(tracker.native_histogram_buckets(high_u64_ref), Some(5));
        assert_eq!(tracker.active(), (4, 1, 1, 5));

        tracker.delete(high_slab_ref);
        assert!(!tracker.is_ref_active_fast(high_slab_ref, i64::MIN));
        tracker.purge(t1 + Duration::from_secs(62));
        assert!(!tracker.is_ref_active_fast(high_u64_ref, i64::MIN));
        assert_eq!(tracker.active(), (0, 0, 0, 0));

        head.close().unwrap();
    }

    #[test]
    fn nanos_slab_preserves_negative_and_epoch_timestamps_and_reclaims_pages() {
        let tracker = ActiveSeries::new(Vec::new(), Duration::from_secs(60));
        let p = [("__name__", "neg_ts")];

        // Negative timestamp (before UNIX_EPOCH) & exact UNIX_EPOCH (0 ns)
        let neg_t = UNIX_EPOCH - Duration::from_secs(100);
        let zero_t = UNIX_EPOCH;
        let ref_neg = 50_000_001u64;
        let ref_zero = 268_435_455u64; // last paged slot before overflow
        let ref_ov = 268_435_456u64;   // first overflow slot

        tracker.update_series(&p, 1, ref_neg, neg_t, -1, false);
        tracker.update_series(&p, 2, ref_zero, zero_t, -1, false);
        tracker.update_series(&p, 3, ref_ov, zero_t, -1, false);

        assert!(tracker.is_ref_active_fast(ref_neg, system_time_to_unix_nanos(neg_t)));
        assert!(!tracker.is_ref_active_fast(ref_neg, system_time_to_unix_nanos(neg_t) + 1));
        assert!(tracker.is_ref_active_fast(ref_zero, 0));
        assert!(!tracker.is_ref_active_fast(ref_zero, 1));
        assert!(tracker.is_ref_active_fast(ref_ov, 0));
        assert!(!tracker.is_ref_active_fast(ref_ov, 1));
        assert_eq!(tracker.debug_slab_stats(), (2, 2, 0));

        // Purge all -> both slab pages become empty and are reclaimed (when madvise applies).
        tracker.purge(UNIX_EPOCH + Duration::from_secs(120));
        assert_eq!(tracker.active(), (0, 0, 0, 0));
        #[cfg(all(
            target_os = "linux",
            any(target_arch = "x86_64", target_arch = "aarch64"),
            not(miri)
        ))]
        {
            let stats = tracker.debug_slab_stats();
            assert!(stats == (2, 0, 2) || stats == (2, 2, 0), "unexpected slab stats: {stats:?}");
        }
        #[cfg(not(all(
            target_os = "linux",
            any(target_arch = "x86_64", target_arch = "aarch64"),
            not(miri)
        )))]
        assert_eq!(tracker.debug_slab_stats(), (2, 2, 0));
        assert!(!tracker.is_ref_active_fast(ref_neg, i64::MIN));
        assert!(!tracker.is_ref_active_fast(ref_zero, i64::MIN));
    }
}
