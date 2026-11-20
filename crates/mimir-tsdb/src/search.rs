//! Streaming label name & value fuzzy search (`Subsequence`, `JaroWinkler`, `SubstringLeft`, `Substring`).
//!
//! Ported from github.com/grafana/mimir/pkg/streaminglabelvalues/params.go,
//! github.com/grafana/mimir/pkg/streaminglabelvalues/filters.go,
//! github.com/prometheus/prometheus/util/strutil/subsequence.go,
//! github.com/prometheus/prometheus/util/strutil/jarowinkler.go,
//! and github.com/prometheus/prometheus/storage/generic.go.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use thiserror::Error;

const SUBSEQUENCE_NON_EXACT_SCORE_SCALE: f64 = 0.999;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SearchError {
    #[error("fuzz threshold {0} out of [0,100]")]
    InvalidThreshold(i32),
    #[error("search term {0} is empty")]
    EmptyTerm(usize),
}

/// Fuzzy search algorithm (`streaminglabelvalues.FuzzAlg`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum FuzzAlg {
    #[default]
    Subsequence = 0,
    JaroWinkler = 1,
    SubstringLeft = 2,
    Substring = 3,
}

/// Result ordering (`storage.Ordering`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SearchOrdering {
    #[default]
    ValueAsc,
    ValueDesc,
    ScoreDesc,
}

/// Search parameters matching `streaminglabelvalues.Params` + `storage.SearchHints`.
#[derive(Clone, Debug, Default)]
pub struct SearchHints {
    pub terms: Vec<String>,
    pub case_sensitive: bool,
    pub fuzz_alg: FuzzAlg,
    /// Minimum score threshold in `0..=100` (divided by 100.0 internally).
    pub fuzz_threshold: i32,
    pub order_by: SearchOrdering,
    /// `0` means no limit.
    pub limit: usize,
}

/// A single matched label name or value with its similarity score.
#[derive(Clone, Debug, PartialEq)]
pub struct SearchResult {
    pub value: String,
    pub score: f64,
}

impl SearchHints {
    pub fn validate(&self) -> Result<(), SearchError> {
        if !(0..=100).contains(&self.fuzz_threshold) {
            return Err(SearchError::InvalidThreshold(self.fuzz_threshold));
        }
        for (i, t) in self.terms.iter().enumerate() {
            if t.is_empty() {
                return Err(SearchError::EmptyTerm(i));
            }
        }
        Ok(())
    }
}

/// Greedy subsequence scorer matching `strutil.SubsequenceMatcher`.
pub fn subsequence_score(pattern: &str, text: &str) -> f64 {
    if pattern.is_empty() {
        return 1.0;
    }
    if text.is_empty() {
        return 0.0;
    }
    if pattern == text {
        return 1.0;
    }
    if pattern.len() > text.len() {
        return 0.0;
    }

    if pattern.is_ascii() && text.is_ascii() {
        return match_subsequence_bytes(pattern.as_bytes(), text.as_bytes());
    }
    if !pattern.is_ascii() && text.is_ascii() {
        return 0.0;
    }

    let p_chars: Vec<char> = pattern.chars().collect();
    let t_chars: Vec<char> = text.chars().collect();
    match_subsequence_chars(&p_chars, &t_chars)
}

fn match_subsequence_bytes(pattern: &[u8], text: &[u8]) -> f64 {
    let pattern_len = pattern.len();
    let text_len = text.len();
    let inv_text_len = 1.0 / (text_len as f64);
    let max_start = text_len - pattern_len;

    let score_from = |start_pos: usize| -> Option<f64> {
        let mut i = start_pos;
        let mut from = i;
        let mut to = i;
        let mut p_idx = 1usize;
        i += 1;
        while p_idx < pattern_len && i < text_len && text[i] == pattern[p_idx] {
            to = i;
            p_idx += 1;
            i += 1;
        }
        let mut score = 0.0f64;
        if from > 0 {
            score -= (from as f64) * inv_text_len;
        }
        let size = (to - from + 1) as f64;
        score += size * size;
        let mut prev_to = to;

        while p_idx < pattern_len {
            let rel = text[i..].iter().position(|&b| b == pattern[p_idx])?;
            i += rel;
            from = i;
            to = i;
            p_idx += 1;
            i += 1;
            while p_idx < pattern_len && i < text_len && text[i] == pattern[p_idx] {
                to = i;
                p_idx += 1;
                i += 1;
            }
            let gap = from - prev_to - 1;
            if gap > 0 {
                score -= (gap as f64) * inv_text_len;
            }
            let size = (to - from + 1) as f64;
            score += size * size;
            prev_to = to;
        }

        let trailing = text_len - 1 - prev_to;
        if trailing > 0 {
            score -= (trailing as f64) * inv_text_len * 0.5;
        }
        Some(score)
    };

    let mut best_score = -1.0f64;
    let mut i = 0usize;
    while i <= max_start {
        let Some(rel) = text[i..=max_start].iter().position(|&b| b == pattern[0]) else {
            break;
        };
        i += rel;
        let Some(s) = score_from(i) else {
            break;
        };
        if s > best_score {
            best_score = s;
        }
        i += 1;
    }

    if best_score < 0.0 {
        0.0
    } else {
        (best_score / ((pattern_len * pattern_len) as f64)) * SUBSEQUENCE_NON_EXACT_SCORE_SCALE
    }
}

fn match_subsequence_chars(pattern: &[char], text: &[char]) -> f64 {
    let pattern_len = pattern.len();
    let text_len = text.len();
    if pattern_len > text_len {
        return 0.0;
    }
    let inv_text_len = 1.0 / (text_len as f64);

    let match_from = |start_pos: usize| -> Option<f64> {
        let mut p_idx = 0usize;
        let mut i = start_pos;
        let mut score = 0.0f64;
        let mut prev_to: Option<usize> = None;

        while i < text_len && p_idx < pattern_len {
            if text[i] == pattern[p_idx] {
                let from = i;
                let mut to = i;
                p_idx += 1;
                i += 1;
                while i < text_len && p_idx < pattern_len && text[i] == pattern[p_idx] {
                    to = i;
                    p_idx += 1;
                    i += 1;
                }
                let gap = match prev_to {
                    None => from,
                    Some(pt) => from - pt - 1,
                };
                if gap > 0 {
                    score -= (gap as f64) * inv_text_len;
                }
                let size = (to - from + 1) as f64;
                score += size * size;
                prev_to = Some(to);
            } else {
                i += 1;
            }
        }
        if p_idx < pattern_len {
            return None;
        }
        let trailing = text_len - 1 - prev_to.unwrap();
        if trailing > 0 {
            score -= (trailing as f64) * inv_text_len * 0.5;
        }
        Some(score)
    };

    let mut best_score = -1.0f64;
    let max_start = text_len - pattern_len;
    for (i, &ch) in text.iter().enumerate().take(max_start + 1) {
        if ch != pattern[0] {
            continue;
        }
        let Some(s) = match_from(i) else {
            break;
        };
        if s > best_score {
            best_score = s;
        }
    }
    if best_score < 0.0 {
        0.0
    } else {
        (best_score / ((pattern_len * pattern_len) as f64)) * SUBSEQUENCE_NON_EXACT_SCORE_SCALE
    }
}

/// Jaro-Winkler similarity scorer matching `strutil.JaroWinklerMatcher`.
pub fn jaro_winkler_score(term: &str, s: &str) -> f64 {
    if term == s {
        return 1.0;
    }
    if term.is_empty() || s.is_empty() {
        return 0.0;
    }
    if term.is_ascii() && s.is_ascii() {
        return jaro_winkler_slice(term.as_bytes(), s.as_bytes());
    }
    let r1: Vec<char> = term.chars().collect();
    let r2: Vec<char> = s.chars().collect();
    jaro_winkler_slice(&r1, &r2)
}

fn jaro_winkler_slice<'a, T: Eq>(mut s1: &'a [T], mut s2: &'a [T]) -> f64 {
    if s1.len() > s2.len() {
        std::mem::swap(&mut s1, &mut s2);
    }
    let l1 = s1.len();
    let l2 = s2.len();
    let match_dist = (l2 / 2).saturating_sub(1);

    let mut s1_matches = vec![false; l1];
    let mut s2_matches = vec![false; l2];
    let mut matches = 0usize;

    for i in 0..l1 {
        let start = i.saturating_sub(match_dist);
        let end = (i + match_dist + 1).min(l2);
        for j in start..end {
            if !s2_matches[j] && s1[i] == s2[j] {
                s1_matches[i] = true;
                s2_matches[j] = true;
                matches += 1;
                break;
            }
        }
    }

    if matches == 0 {
        return 0.0;
    }

    let mut transpositions = 0usize;
    let mut k = 0usize;
    for i in 0..l1 {
        if !s1_matches[i] {
            continue;
        }
        while !s2_matches[k] {
            k += 1;
        }
        if s1[i] != s2[k] {
            transpositions += 1;
        }
        k += 1;
    }

    let m = matches as f64;
    let inv_l1 = 1.0 / (l1 as f64);
    let inv_l2 = 1.0 / (l2 as f64);
    let jaro = (m * inv_l1 + m * inv_l2 + (m - (transpositions as f64) * 0.5) / m) / 3.0;

    let max_prefix = 4.min(l1).min(l2);
    let mut prefix_len = 0usize;
    while prefix_len < max_prefix && s1[prefix_len] == s2[prefix_len] {
        prefix_len += 1;
    }

    const P: f64 = 0.1;
    jaro + (prefix_len as f64) * P * (1.0 - jaro)
}

enum CompiledTermFilter {
    Subsequence { term: String, threshold: f64 },
    JaroWinkler { term: String, threshold: f64, enable_fuzzy: bool },
    SubstringLeft { term: String },
    Substring { term: String },
}

impl CompiledTermFilter {
    fn accept(&self, val: &str) -> Option<f64> {
        match self {
            Self::Subsequence { term, threshold } => {
                if val.starts_with(term.as_str()) {
                    return Some(1.0);
                }
                let s = subsequence_score(term, val);
                if s > 0.0 && s >= *threshold {
                    Some(s)
                } else {
                    None
                }
            }
            Self::JaroWinkler {
                term,
                threshold,
                enable_fuzzy,
            } => {
                if let Some(idx) = val.find(term.as_str()) {
                    if idx == 0 {
                        return Some(1.0);
                    }
                    let max_idx = val.len() - term.len();
                    return Some(1.0 - 0.9 * (idx as f64) / (max_idx as f64));
                }
                if !enable_fuzzy {
                    return None;
                }
                let s = jaro_winkler_score(term, val);
                if s >= *threshold {
                    Some(s)
                } else {
                    None
                }
            }
            Self::SubstringLeft { term } => {
                let idx = val.find(term.as_str())?;
                if idx == 0 {
                    return Some(1.0);
                }
                let max_idx = val.len() - term.len();
                Some(1.0 - 0.9 * (idx as f64) / (max_idx as f64))
            }
            Self::Substring { term } => {
                if val.contains(term.as_str()) {
                    Some(1.0)
                } else {
                    None
                }
            }
        }
    }
}

/// Rune-wise lowercase matching Go `strings.ToLower` (`unicode.ToLower(r)`).
/// Differs from Rust `str::to_lowercase()` on `U+0130` (`'İ'` -> `'i'` vs `"i\u{0307}"`)
/// and contextual final sigma `'Σ'` (`'σ'` vs `'ς'`).
fn go_to_lower(s: &str) -> String {
    if s.is_ascii() {
        return s.to_ascii_lowercase();
    }
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if c == '\u{0130}' {
            out.push('i');
        } else {
            for lc in c.to_lowercase() {
                out.push(lc);
            }
        }
    }
    out
}

struct CompiledFilter {
    case_sensitive: bool,
    terms: Vec<CompiledTermFilter>,
}

impl CompiledFilter {
    fn from_hints(hints: &SearchHints) -> Result<Option<Self>, SearchError> {
        hints.validate()?;
        if hints.terms.is_empty() {
            return Ok(None);
        }
        let threshold = (hints.fuzz_threshold as f64) / 100.0;
        let mut terms = Vec::with_capacity(hints.terms.len());
        for raw_term in &hints.terms {
            let term = if hints.case_sensitive {
                raw_term.clone()
            } else {
                go_to_lower(raw_term)
            };
            let leaf = match hints.fuzz_alg {
                FuzzAlg::Subsequence => CompiledTermFilter::Subsequence { term, threshold },
                FuzzAlg::JaroWinkler => CompiledTermFilter::JaroWinkler {
                    term,
                    threshold,
                    enable_fuzzy: hints.fuzz_threshold > 0,
                },
                FuzzAlg::SubstringLeft => CompiledTermFilter::SubstringLeft { term },
                FuzzAlg::Substring => CompiledTermFilter::Substring { term },
            };
            terms.push(leaf);
        }
        Ok(Some(Self {
            case_sensitive: hints.case_sensitive,
            terms,
        }))
    }

    fn accept(&self, val: &str) -> Option<f64> {
        let folded;
        let candidate = if self.case_sensitive {
            val
        } else {
            folded = go_to_lower(val);
            folded.as_str()
        };

        let mut any = false;
        let mut best = 0.0f64;
        for f in &self.terms {
            if let Some(score) = f.accept(candidate) {
                any = true;
                if score > best {
                    best = score;
                }
                if best >= 1.0 {
                    return Some(1.0);
                }
            }
        }
        if any {
            Some(best)
        } else {
            None
        }
    }
}

#[derive(Clone, Debug)]
struct WorstFirstResult(SearchResult);

impl PartialEq for WorstFirstResult {
    fn eq(&self, other: &Self) -> bool {
        self.0.score == other.0.score && self.0.value == other.0.value
    }
}

impl Eq for WorstFirstResult {}

impl Ord for WorstFirstResult {
    fn cmp(&self, other: &Self) -> Ordering {
        // Max-heap in BinaryHeap pops largest first; we want WORST at the top so we can peek/pop it.
        // Worst = lower score, or on tie, lexicographically larger value.
        other
            .0
            .score
            .total_cmp(&self.0.score)
            .then_with(|| self.0.value.cmp(&other.0.value))
    }
}

impl PartialOrd for WorstFirstResult {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Applies `hints` to an ascending-sorted slice of `values`, matching Prometheus `storage.ApplySearchHints`.
pub fn apply_search_hints(
    values: &[String],
    hints: &SearchHints,
) -> Result<Vec<SearchResult>, SearchError> {
    let filter = CompiledFilter::from_hints(hints)?;

    let Some(filter) = filter else {
        let n = if hints.limit > 0 {
            hints.limit.min(values.len())
        } else {
            values.len()
        };
        let mut out = Vec::with_capacity(n);
        if hints.order_by == SearchOrdering::ValueDesc {
            for v in values.iter().rev().take(n) {
                out.push(SearchResult {
                    value: v.clone(),
                    score: 1.0,
                });
            }
        } else {
            for v in values.iter().take(n) {
                out.push(SearchResult {
                    value: v.clone(),
                    score: 1.0,
                });
            }
        }
        return Ok(out);
    };

    if hints.limit > 0 && hints.order_by == SearchOrdering::ScoreDesc {
        let mut heap: BinaryHeap<WorstFirstResult> =
            BinaryHeap::with_capacity(hints.limit.min(values.len()));
        for v in values {
            let Some(score) = filter.accept(v) else {
                continue;
            };
            if heap.len() < hints.limit {
                heap.push(WorstFirstResult(SearchResult {
                    value: v.clone(),
                    score,
                }));
            } else if let Some(worst) = heap.peek() {
                let better = score > worst.0.score
                    || (score == worst.0.score && v.as_str() < worst.0.value.as_str());
                if better {
                    heap.pop();
                    heap.push(WorstFirstResult(SearchResult {
                        value: v.clone(),
                        score,
                    }));
                }
            }
        }
        let mut out: Vec<SearchResult> = heap.into_iter().map(|w| w.0).collect();
        out.sort_by(|a, b| {
            b.score
                .total_cmp(&a.score)
                .then_with(|| a.value.cmp(&b.value))
        });
        return Ok(out);
    }

    if hints.limit > 0 && hints.order_by == SearchOrdering::ValueDesc {
        let mut out = Vec::with_capacity(hints.limit.min(values.len()));
        for v in values.iter().rev() {
            if let Some(score) = filter.accept(v) {
                out.push(SearchResult {
                    value: v.clone(),
                    score,
                });
                if out.len() >= hints.limit {
                    break;
                }
            }
        }
        return Ok(out);
    }

    let mut out = Vec::new();
    let early_exit = hints.order_by == SearchOrdering::ValueAsc && hints.limit > 0;
    for v in values {
        if let Some(score) = filter.accept(v) {
            out.push(SearchResult {
                value: v.clone(),
                score,
            });
            if early_exit && out.len() >= hints.limit {
                break;
            }
        }
    }

    match hints.order_by {
        SearchOrdering::ValueAsc => {}
        SearchOrdering::ValueDesc => out.reverse(),
        SearchOrdering::ScoreDesc => {
            out.sort_by(|a, b| {
                b.score
                    .total_cmp(&a.score)
                    .then_with(|| a.value.cmp(&b.value))
            });
        }
    }

    if hints.limit > 0 && out.len() > hints.limit {
        out.truncate(hints.limit);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn search_algorithms_and_orderings() {
        let values = vec![
            "alpha".to_string(),
            "ap_http_requests_total".to_string(),
            "http_requests_total".to_string(),
            "HTTP_ERRORS".to_string(),
            "z_http".to_string(),
        ];
        let mut sorted = values.clone();
        sorted.sort();

        // Subsequence case-insensitive, score desc
        let hints = SearchHints {
            terms: vec!["hrt".to_string()],
            case_sensitive: false,
            fuzz_alg: FuzzAlg::Subsequence,
            fuzz_threshold: 0,
            order_by: SearchOrdering::ScoreDesc,
            limit: 10,
        };
        let res = apply_search_hints(&sorted, &hints).unwrap();
        assert!(res.iter().any(|r| r.value == "http_requests_total"));
        assert!(!res.iter().any(|r| r.value == "alpha"));

        // SubstringLeft: prefix gets 1.0, later substring gets < 1.0
        let hints_left = SearchHints {
            terms: vec!["http".to_string()],
            case_sensitive: false,
            fuzz_alg: FuzzAlg::SubstringLeft,
            fuzz_threshold: 0,
            order_by: SearchOrdering::ScoreDesc,
            limit: 2,
        };
        let res_left = apply_search_hints(&sorted, &hints_left).unwrap();
        assert_eq!(res_left.len(), 2);
        assert_eq!(res_left[0].score, 1.0);

        // Substring: all matches score 1.0
        let hints_sub = SearchHints {
            terms: vec!["http".to_string()],
            case_sensitive: false,
            fuzz_alg: FuzzAlg::Substring,
            fuzz_threshold: 0,
            order_by: SearchOrdering::ValueDesc,
            limit: 0,
        };
        let res_sub = apply_search_hints(&sorted, &hints_sub).unwrap();
        assert_eq!(res_sub.len(), 4);
        assert!(res_sub.iter().all(|r| r.score == 1.0));
        assert_eq!(res_sub[0].value, "z_http");

        // JaroWinkler with threshold
        let hints_jw = SearchHints {
            terms: vec!["alphaa".to_string()],
            case_sensitive: true,
            fuzz_alg: FuzzAlg::JaroWinkler,
            fuzz_threshold: 80,
            order_by: SearchOrdering::ScoreDesc,
            limit: 5,
        };
        let res_jw = apply_search_hints(&sorted, &hints_jw).unwrap();
        assert_eq!(res_jw.len(), 1);
        assert_eq!(res_jw[0].value, "alpha");
    }
}
