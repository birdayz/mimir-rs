//! Prometheus label matchers and `FastRegexMatcher`.
//!
//! Ported from github.com/prometheus/prometheus/model/labels/matcher.go
//! and github.com/prometheus/prometheus/model/labels/regexp.go.

use std::fmt;

use hashbrown::HashSet;
use regex::Regex;
use regex_syntax::hir::{Class, Hir, HirKind, Look};
use regex_syntax::ParserBuilder;
use thiserror::Error;

const MAX_SET_MATCHES: usize = 256;
const MIN_EQUAL_MULTI_STRING_MATCHER_MAP_THRESHOLD: usize = 16;

#[derive(Debug, Error)]
pub enum MatcherError {
    /// Go `regexp/syntax` parse error (`error parsing regexp: <code>: `<expr>``).
    #[error("{0}")]
    GoSyntax(#[from] crate::go_regexp_syntax::GoSyntaxError),
    /// A pattern Go accepts but the Rust `regex` crate cannot compile.
    #[error("invalid regex pattern {pattern:?}: {reason}")]
    InvalidRegex {
        pattern: String,
        reason: String,
    },
}

/// Label matching operator.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MatchType {
    /// `=`
    Equal,
    /// `!=`
    NotEqual,
    /// `=~`
    RegexMatch,
    /// `!~`
    RegexNotMatch,
}

impl MatchType {
    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Equal => "=",
            Self::NotEqual => "!=",
            Self::RegexMatch => "=~",
            Self::RegexNotMatch => "!~",
        }
    }
}

impl fmt::Display for MatchType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A compiled Prometheus label matcher.
///
/// Missing labels in a series are represented by `""` when calling [`LabelMatcher::matches`].
#[derive(Clone, Debug)]
pub struct LabelMatcher {
    pub match_type: MatchType,
    pub name: String,
    pub value: String,
    re: Option<FastRegexMatcher>,
}

impl LabelMatcher {
    /// Creates a new `LabelMatcher`, compiling and optimizing the regex if needed.
    pub fn new(
        match_type: MatchType,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Self, MatcherError> {
        let name = name.into();
        let value = value.into();
        let re = match match_type {
            MatchType::RegexMatch | MatchType::RegexNotMatch => {
                Some(FastRegexMatcher::new(&value)?)
            }
            MatchType::Equal | MatchType::NotEqual => None,
        };
        Ok(Self {
            match_type,
            name,
            value,
            re,
        })
    }

    /// Evaluates whether this matcher matches `s` (pass `""` for missing labels).
    #[inline]
    pub fn matches(&self, s: &str) -> bool {
        match self.match_type {
            MatchType::Equal => s == self.value,
            MatchType::NotEqual => s != self.value,
            MatchType::RegexMatch => self
                .re
                .as_ref()
                .expect("regex matcher compiled")
                .is_match(s),
            MatchType::RegexNotMatch => !self
                .re
                .as_ref()
                .expect("regex matcher compiled")
                .is_match(s),
        }
    }

    /// Returns `true` if this matcher matches a missing/empty label value `""`.
    #[inline]
    pub fn matches_empty(&self) -> bool {
        self.matches("")
    }

    /// Returns the inverse matcher (`=` <-> `!=`, `=~` <-> `!~`).
    pub fn inverse(&self) -> Result<Self, MatcherError> {
        let inv = match self.match_type {
            MatchType::Equal => MatchType::NotEqual,
            MatchType::NotEqual => MatchType::Equal,
            MatchType::RegexMatch => MatchType::RegexNotMatch,
            MatchType::RegexNotMatch => MatchType::RegexMatch,
        };
        Self::new(inv, self.name.clone(), self.value.clone())
    }

    /// Returns the regex pattern string if this is a regex matcher, or `""`.
    pub fn regex_string(&self) -> &str {
        self.re.as_ref().map_or("", |r| r.regex_string())
    }

    /// Returns the finite set of exact equality values equivalent to this regex matcher,
    /// if the regex can be replaced by a small set of literals (e.g. `a(b|f)` -> `["ab", "af"]`).
    pub fn set_matches(&self) -> Option<&[String]> {
        self.re.as_ref().and_then(|r| r.set_matches())
    }

    /// Returns the required literal prefix of any matching value, if known (and empty for `=`/`!=`).
    pub fn prefix(&self) -> &str {
        self.re.as_ref().map_or("", |r| r.prefix())
    }

    /// Returns `true` if a fast-path optimization was applied to the regex matcher.
    pub fn is_regex_optimized(&self) -> bool {
        self.re.as_ref().is_some_and(|r| r.is_optimized())
    }
}

impl PartialEq for LabelMatcher {
    fn eq(&self, other: &Self) -> bool {
        self.match_type == other.match_type
            && self.name == other.name
            && self.value == other.value
    }
}

impl Eq for LabelMatcher {}

impl fmt::Display for LabelMatcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if should_quote_name(&self.name) {
            write!(f, "{:?}{}{:?}", self.name, self.match_type, self.value)
        } else {
            write!(f, "{}{}{:?}", self.name, self.match_type, self.value)
        }
    }
}

fn should_quote_name(name: &str) -> bool {
    if name.is_empty() {
        return true;
    }
    for (i, c) in name.chars().enumerate() {
        let valid =
            c == '_' || c.is_ascii_lowercase() || c.is_ascii_uppercase() || (i > 0 && c.is_ascii_digit());
        if !valid {
            return true;
        }
    }
    false
}

/// Fast-path optimized regex matcher matching Prometheus `\A(?s:...)\z` semantics.
#[derive(Clone, Debug)]
pub struct FastRegexMatcher {
    pattern: String,
    strategy: Strategy,
    set_matches: Option<Vec<String>>,
    prefix: String,
    suffix: String,
    contains: Vec<String>,
}

#[derive(Clone, Debug)]
enum Strategy {
    EmptyString,
    True,
    AnyNonEmpty,
    AnyWithoutNewline,
    AnyNonEmptyWithoutNewline,
    Equal(String),
    MultiSlice {
        values: Vec<String>,
        lengths_mask: u64,
    },
    MultiSet {
        values: HashSet<String>,
        lengths_mask: u64,
    },
    PrefixExact {
        prefix: String,
        rest: Box<Strategy>,
    },
    SuffixExact {
        rest: Box<Strategy>,
        suffix: String,
    },
    Contains {
        substrs: Vec<String>,
    },
    Regex(Regex),
}

impl Strategy {
    fn is_match(&self, s: &str) -> bool {
        match self {
            Self::EmptyString => s.is_empty(),
            Self::True => true,
            Self::AnyNonEmpty => !s.is_empty(),
            Self::AnyWithoutNewline => !s.as_bytes().contains(&b'\n'),
            Self::AnyNonEmptyWithoutNewline => !s.is_empty() && !s.as_bytes().contains(&b'\n'),
            Self::Equal(v) => s == v,
            Self::MultiSlice {
                values,
                lengths_mask,
            } => (lengths_mask & length_mask(s)) != 0 && values.iter().any(|v| v == s),
            Self::MultiSet {
                values,
                lengths_mask,
            } => (lengths_mask & length_mask(s)) != 0 && values.contains(s),
            Self::PrefixExact { prefix, rest } => {
                s.strip_prefix(prefix.as_str())
                    .is_some_and(|tail| rest.is_match(tail))
            }
            Self::SuffixExact { rest, suffix } => {
                s.strip_suffix(suffix.as_str())
                    .is_some_and(|head| rest.is_match(head))
            }
            Self::Contains { substrs } => substrs.iter().any(|sub| s.contains(sub.as_str())),
            Self::Regex(re) => re.is_match(s),
        }
    }
}

#[inline]
fn length_mask(s: &str) -> u64 {
    1u64 << s.len().min(63)
}

#[inline]
fn is_go_literal(s: &str) -> bool {
    !s.bytes().any(|b| {
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
    })
}

impl FastRegexMatcher {
    /// Compiles and optimizes `pattern` with Prometheus anchored `\A(?s:<pattern>)\z` semantics.
    pub fn new(pattern: &str) -> Result<Self, MatcherError> {
        if pattern.is_empty() {
            return Ok(Self {
                pattern: String::new(),
                strategy: Strategy::EmptyString,
                set_matches: None,
                prefix: String::new(),
                suffix: String::new(),
                contains: Vec::new(),
            });
        }

        // Fast path: alternating literals `a|b|c` or single literal `foo`
        if let Some((strategy, set_matches)) = optimize_alternating_literals(pattern) {
            return Ok(Self {
                pattern: pattern.to_string(),
                strategy,
                set_matches,
                prefix: String::new(),
                suffix: String::new(),
                contains: Vec::new(),
            });
        }

        // Go `syntax.Parse(v, syntax.Perl|syntax.DotNL)`: validate with Go's exact error text
        // and translate Go RE2 dialect constructs into equivalent Rust `regex` syntax.
        let translated = crate::go_regexp_syntax::translate_to_rust(pattern)?;

        // Parse translated pattern with DotNL (matching Go `syntax.Perl | syntax.DotNL`).
        let hir = ParserBuilder::new()
            .dot_matches_new_line(true)
            .build()
            .parse(&translated.pattern)
            .map_err(|e| MatcherError::InvalidRegex {
                pattern: pattern.to_string(),
                reason: e.to_string(),
            })?;

        let anchored = format!(r"\A(?s:{})\z", translated.pattern);
        let re = Regex::new(&anchored).map_err(|e| MatcherError::InvalidRegex {
            pattern: pattern.to_string(),
            reason: e.to_string(),
        })?;

        // Go `regexp.go:126` only populates `set_matches` when case-sensitive.
        let set_matches = if translated.has_case_insensitive {
            None
        } else {
            find_set_matches(&hir)
        };
        let (prefix, suffix, contains) = if translated.has_case_insensitive {
            (String::new(), String::new(), Vec::new())
        } else {
            extract_concat_hints(&hir)
        };

        let strategy = if let Some(ref matches) = set_matches {
            if matches.len() == 1 {
                Strategy::Equal(matches[0].clone())
            } else {
                build_multi_strategy(matches.clone())
            }
        } else if !translated.has_case_insensitive {
            strategy_from_hir(&hir).unwrap_or(Strategy::Regex(re.clone()))
        } else {
            Strategy::Regex(re.clone())
        };

        // If `strategy` fell back to `Regex`, we can still use `prefix`/`suffix`/`contains`
        // as pre-filters in `is_match`.
        Ok(Self {
            pattern: pattern.to_string(),
            strategy,
            set_matches,
            prefix,
            suffix,
            contains,
        })
    }

    #[inline]
    pub fn is_match(&self, s: &str) -> bool {
        if matches!(self.strategy, Strategy::Regex(_)) {
            if !self.prefix.is_empty() && !s.starts_with(&self.prefix) {
                return false;
            }
            if !self.suffix.is_empty() && !s.ends_with(&self.suffix) {
                return false;
            }
            if !self.contains.is_empty() && !contains_in_order(s, &self.contains) {
                return false;
            }
        }
        self.strategy.is_match(s)
    }

    #[inline]
    pub fn regex_string(&self) -> &str {
        &self.pattern
    }

    #[inline]
    pub fn set_matches(&self) -> Option<&[String]> {
        self.set_matches.as_deref()
    }

    #[inline]
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    #[inline]
    pub fn is_optimized(&self) -> bool {
        !matches!(self.strategy, Strategy::Regex(_))
            || !self.prefix.is_empty()
            || !self.suffix.is_empty()
            || !self.contains.is_empty()
            || self.set_matches.is_some()
    }
}

fn contains_in_order(s: &str, needles: &[String]) -> bool {
    let mut offset = 0;
    for needle in needles {
        match s[offset..].find(needle.as_str()) {
            Some(pos) => offset += pos + needle.len(),
            None => return false,
        }
    }
    true
}

fn optimize_alternating_literals(pattern: &str) -> Option<(Strategy, Option<Vec<String>>)> {
    if !pattern.contains('|') {
        if is_go_literal(pattern) {
            let owned = pattern.to_string();
            return Some((Strategy::Equal(owned.clone()), Some(vec![owned])));
        }
        return None;
    }

    let mut seen = HashSet::new();
    let mut values = Vec::new();
    for part in pattern.split('|') {
        if !is_go_literal(part) {
            return None;
        }
        if seen.insert(part) {
            values.push(part.to_string());
        }
    }
    let set = if values.len() < MAX_SET_MATCHES {
        Some(values.clone())
    } else {
        None
    };
    if values.len() == 1 {
        return Some((Strategy::Equal(values[0].clone()), set));
    }
    Some((build_multi_strategy(values), set))
}

fn build_multi_strategy(values: Vec<String>) -> Strategy {
    let mut lengths_mask = 0u64;
    for v in &values {
        lengths_mask |= length_mask(v);
    }
    if values.len() < MIN_EQUAL_MULTI_STRING_MATCHER_MAP_THRESHOLD {
        Strategy::MultiSlice {
            values,
            lengths_mask,
        }
    } else {
        let set: HashSet<String> = values.into_iter().collect();
        Strategy::MultiSet {
            values: set,
            lengths_mask,
        }
    }
}

fn strip_capture(mut hir: &Hir) -> &Hir {
    while let HirKind::Capture(cap) = hir.kind() {
        hir = &cap.sub;
    }
    hir
}

fn strip_outer_anchors(subs: &[Hir]) -> &[Hir] {
    let mut s = subs;
    if let Some(first) = s.first() {
        if matches!(strip_capture(first).kind(), HirKind::Look(Look::Start)) {
            s = &s[1..];
        }
    }
    if let Some(last) = s.last() {
        if matches!(strip_capture(last).kind(), HirKind::Look(Look::End)) {
            s = &s[..s.len() - 1];
        }
    }
    s
}

fn hir_literal_str(hir: &Hir) -> Option<String> {
    match strip_capture(hir).kind() {
        HirKind::Literal(lit) => std::str::from_utf8(&lit.0).ok().map(str::to_string),
        _ => None,
    }
}

fn extract_concat_hints(hir: &Hir) -> (String, String, Vec<String>) {
    let hir = strip_capture(hir);
    let HirKind::Concat(subs) = hir.kind() else {
        return (String::new(), String::new(), Vec::new());
    };
    let subs = strip_outer_anchors(subs);
    if subs.is_empty() {
        return (String::new(), String::new(), Vec::new());
    }
    let prefix = hir_literal_str(&subs[0]).unwrap_or_default();
    let suffix = if subs.len() > 1 {
        hir_literal_str(&subs[subs.len() - 1]).unwrap_or_default()
    } else {
        String::new()
    };
    let mut contains = Vec::new();
    if subs.len() > 2 {
        for mid in &subs[1..subs.len() - 1] {
            if let Some(lit) = hir_literal_str(mid) {
                contains.push(lit);
            }
        }
    }
    (prefix, suffix, contains)
}

fn find_set_matches(hir: &Hir) -> Option<Vec<String>> {
    let hir = strip_capture(hir);
    if matches!(hir.kind(), HirKind::Empty) {
        return None;
    }
    match hir.kind() {
        HirKind::Concat(subs) => {
            let subs = strip_outer_anchors(subs);
            if subs.is_empty() {
                return None;
            }
            let mut acc = vec![String::new()];
            for sub in subs {
                let parts = find_set_matches_inner(sub)?;
                if acc.len().saturating_mul(parts.len()) > MAX_SET_MATCHES {
                    return None;
                }
                let mut next = Vec::with_capacity(acc.len() * parts.len());
                for base in &acc {
                    for part in &parts {
                        let mut s = String::with_capacity(base.len() + part.len());
                        s.push_str(base);
                        s.push_str(part);
                        next.push(s);
                    }
                }
                acc = next;
            }
            Some(acc)
        }
        HirKind::Alternation(_) => find_set_matches_inner(hir),
        _ => find_set_matches_inner(hir),
    }
}

fn find_set_matches_inner(hir: &Hir) -> Option<Vec<String>> {
    let hir = strip_capture(hir);
    match hir.kind() {
        HirKind::Empty => Some(vec![String::new()]),
        HirKind::Literal(lit) => {
            let s = std::str::from_utf8(&lit.0).ok()?.to_string();
            Some(vec![s])
        }
        HirKind::Class(Class::Unicode(cls)) => {
            let mut total = 0usize;
            for r in cls.ranges() {
                total += (r.end() as u32 - r.start() as u32 + 1) as usize;
                if total > MAX_SET_MATCHES {
                    return None;
                }
            }
            let mut out = Vec::with_capacity(total);
            for r in cls.ranges() {
                for ch in r.start()..=r.end() {
                    out.push(ch.to_string());
                }
            }
            Some(out)
        }
        HirKind::Alternation(subs) => {
            let mut out = Vec::new();
            for sub in subs {
                let parts = find_set_matches_inner(sub)?;
                if out.len() + parts.len() > MAX_SET_MATCHES {
                    return None;
                }
                out.extend(parts);
            }
            Some(out)
        }
        HirKind::Concat(subs) => {
            let mut acc = vec![String::new()];
            for sub in subs {
                let parts = find_set_matches_inner(sub)?;
                if acc.len().saturating_mul(parts.len()) > MAX_SET_MATCHES {
                    return None;
                }
                let mut next = Vec::with_capacity(acc.len() * parts.len());
                for base in &acc {
                    for part in &parts {
                        let mut s = String::with_capacity(base.len() + part.len());
                        s.push_str(base);
                        s.push_str(part);
                        next.push(s);
                    }
                }
                acc = next;
            }
            Some(acc)
        }
        _ => None,
    }
}

fn is_any_char_with_nl(hir: &Hir) -> Option<bool> {
    let HirKind::Class(Class::Unicode(cls)) = strip_capture(hir).kind() else {
        return None;
    };
    let ranges = cls.ranges();
    if ranges.len() == 1 && ranges[0].start() == '\0' && ranges[0].end() == char::MAX {
        return Some(true);
    }
    if ranges.len() == 2
        && ranges[0].start() == '\0'
        && ranges[0].end() == '\u{9}'
        && ranges[1].start() == '\u{b}'
        && ranges[1].end() == char::MAX
    {
        return Some(false);
    }
    None
}

fn strip_outer_anchors_directional(mut subs: &[Hir], at_start: bool, at_end: bool) -> &[Hir] {
    if at_start {
        if let Some(first) = subs.first() {
            if matches!(strip_capture(first).kind(), HirKind::Look(Look::Start)) {
                subs = &subs[1..];
            }
        }
    }
    if at_end {
        if let Some(last) = subs.last() {
            if matches!(strip_capture(last).kind(), HirKind::Look(Look::End)) {
                subs = &subs[..subs.len() - 1];
            }
        }
    }
    subs
}

fn strategy_from_hir(hir: &Hir) -> Option<Strategy> {
    strategy_from_hir_inner(hir, true, true)
}

fn strategy_from_hir_inner(hir: &Hir, at_start: bool, at_end: bool) -> Option<Strategy> {
    let hir = strip_capture(hir);
    match hir.kind() {
        HirKind::Empty => Some(Strategy::EmptyString),
        HirKind::Literal(lit) => {
            let s = std::str::from_utf8(&lit.0).ok()?.to_string();
            Some(Strategy::Equal(s))
        }
        HirKind::Repetition(rep) => {
            let match_nl = is_any_char_with_nl(&rep.sub)?;
            match (rep.min, rep.max) {
                (0, None) => Some(if match_nl {
                    Strategy::True
                } else {
                    Strategy::AnyWithoutNewline
                }),
                (1, None) => Some(if match_nl {
                    Strategy::AnyNonEmpty
                } else {
                    Strategy::AnyNonEmptyWithoutNewline
                }),
                _ => None,
            }
        }
        HirKind::Concat(subs) => {
            let subs = strip_outer_anchors_directional(subs, at_start, at_end);
            if subs.is_empty() {
                return Some(Strategy::EmptyString);
            }
            if subs.len() == 1 {
                return strategy_from_hir_inner(&subs[0], at_start, at_end);
            }
            if subs.len() == 2 {
                if let Some(prefix) = hir_literal_str(&subs[0]) {
                    if let Some(rest) = strategy_from_hir_inner(&subs[1], false, at_end) {
                        return Some(Strategy::PrefixExact {
                            prefix,
                            rest: Box::new(rest),
                        });
                    }
                }
                if let Some(suffix) = hir_literal_str(&subs[1]) {
                    if let Some(rest) = strategy_from_hir_inner(&subs[0], at_start, false) {
                        return Some(Strategy::SuffixExact {
                            rest: Box::new(rest),
                            suffix,
                        });
                    }
                }
            }
            if subs.len() == 3
                && matches!(strategy_from_hir_inner(&subs[0], at_start, false), Some(Strategy::True))
                && matches!(strategy_from_hir_inner(&subs[2], false, at_end), Some(Strategy::True))
            {
                if let Some(mid) = hir_literal_str(&subs[1]) {
                    return Some(Strategy::Contains { substrs: vec![mid] });
                }
                if let HirKind::Alternation(alts) = strip_capture(&subs[1]).kind() {
                    let mut substrs = Vec::with_capacity(alts.len());
                    for a in alts {
                        substrs.push(hir_literal_str(a)?);
                    }
                    return Some(Strategy::Contains { substrs });
                }
            }
            None
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::labels::Labels;

    #[test]
    fn empty_value_prometheus_semantics() {
        let without_foo = Labels::from_pairs([("__name__", "up"), ("job", "api")]);
        let with_foo = Labels::from_pairs([("__name__", "up"), ("foo", "bar")]);

        // foo="" matches series WITHOUT label foo
        let eq_empty = LabelMatcher::new(MatchType::Equal, "foo", "").unwrap();
        assert!(without_foo.matches(std::slice::from_ref(&eq_empty)));
        assert!(!with_foo.matches(&[eq_empty]));

        // foo!="bar" matches series WITHOUT foo
        let neq_bar = LabelMatcher::new(MatchType::NotEqual, "foo", "bar").unwrap();
        assert!(without_foo.matches(std::slice::from_ref(&neq_bar)));
        assert!(!with_foo.matches(&[neq_bar]));

        // foo=~".*" matches series WITHOUT foo
        let re_star = LabelMatcher::new(MatchType::RegexMatch, "foo", ".*").unwrap();
        assert!(without_foo.matches(std::slice::from_ref(&re_star)));
        assert!(with_foo.matches(&[re_star]));

        // foo=~".+" does NOT match series WITHOUT foo
        let re_plus = LabelMatcher::new(MatchType::RegexMatch, "foo", ".+").unwrap();
        assert!(!without_foo.matches(std::slice::from_ref(&re_plus)));
        assert!(with_foo.matches(&[re_plus]));

        // foo!~".+" matches series WITHOUT foo
        let nre_plus = LabelMatcher::new(MatchType::RegexNotMatch, "foo", ".+").unwrap();
        assert!(without_foo.matches(std::slice::from_ref(&nre_plus)));
        assert!(!with_foo.matches(&[nre_plus]));
    }

    #[test]
    fn anchored_and_multiline_and_fast_paths() {
        // Anchored: "bar" should NOT match "foobar"
        let m = LabelMatcher::new(MatchType::RegexMatch, "x", "bar").unwrap();
        assert!(m.matches("bar"));
        assert!(!m.matches("foobar"));
        assert!(!m.matches("barfoo"));
        assert_eq!(m.set_matches(), Some(&["bar".to_string()][..]));

        // DotNL: .* matches newlines
        let m_nl = LabelMatcher::new(MatchType::RegexMatch, "x", "foo.*bar").unwrap();
        assert!(m_nl.matches("foo\nbar"));
        assert!(m_nl.is_regex_optimized());
        assert_eq!(m_nl.prefix(), "foo");

        // Alternation set_matches
        let m_alt = LabelMatcher::new(MatchType::RegexMatch, "x", "a(b|f)").unwrap();
        assert!(m_alt.matches("ab"));
        assert!(m_alt.matches("af"));
        assert!(!m_alt.matches("ac"));
        assert_eq!(
            m_alt.set_matches(),
            Some(&["ab".to_string(), "af".to_string()][..])
        );

        // (?m) inside pattern must not unanchor outer \A...\z
        let m_multi = LabelMatcher::new(MatchType::RegexMatch, "x", "(?m)^bar$").unwrap();
        assert!(m_multi.matches("bar"));
        assert!(!m_multi.matches("foo\nbar"));

        // Invalid syntax must fail even with wrapper
        assert!(LabelMatcher::new(MatchType::RegexMatch, "x", "foo)(bar").is_err());
    }

    #[test]
    fn go_re2_parity_word_boundary_fold_anchors_empty_qe_and_unicode() {
        // F-05: ASCII word boundaries (?-u:\b) and (?-u:\B)
        let wb = FastRegexMatcher::new(r"caf\bé").unwrap();
        assert!(wb.is_match("café"));
        let wb_non_ascii = FastRegexMatcher::new(r".+\b").unwrap();
        assert!(!wb_non_ascii.is_match("é"));
        let nwb = FastRegexMatcher::new(r"caf\B.").unwrap();
        assert!(!nwb.is_match("café"));
        assert!(nwb.is_match("cafe"));

        // F-06: (?i)[\W] and (?i)[^\W] must not match/reject ASCII s/S/k/K via ſ/K folding
        let ci_nw = FastRegexMatcher::new(r"(?i)[\W]").unwrap();
        for w in ["s", "S", "k", "K", "\u{017F}", "\u{212A}"] {
            assert!(!ci_nw.is_match(w), "(?i)[\\W] must not match {w:?}");
        }
        assert!(ci_nw.is_match("-"));
        let ci_neg_nw = FastRegexMatcher::new(r"(?i)[^\W]").unwrap();
        for w in ["s", "S", "k", "K", "\u{017F}", "\u{212A}"] {
            assert!(ci_neg_nw.is_match(w), "(?i)[^\\W] must match {w:?}");
        }
        assert!(!ci_neg_nw.is_match("-"));
        let cs_nw = FastRegexMatcher::new(r"[\W]").unwrap();
        assert!(cs_nw.is_match("\u{017F}"));
        assert!(cs_nw.is_match("\u{212A}"));

        // F-07: inner anchors must never be stripped by strip_outer_anchors
        let inner_start = FastRegexMatcher::new("foo(^bar)").unwrap();
        assert!(!inner_start.is_match("foobar"));
        let inner_end = FastRegexMatcher::new("(foo$)bar").unwrap();
        assert!(!inner_end.is_match("foobar"));
        let outer_grouped = FastRegexMatcher::new("^(foo)$").unwrap();
        assert!(outer_grouped.is_match("foo"));
        assert!(!outer_grouped.is_match("foobar"));

        // F-12: empty \Q\E quantifier target, empty/dup flags, digit/dup capture names, Unicode 17 & surrogates
        let qe_plus = FastRegexMatcher::new(r"(ab)\Q\E+").unwrap();
        assert!(qe_plus.is_match("abab"));
        assert!(qe_plus.is_match("ab"));
        assert!(!qe_plus.is_match(""));
        let qe_zero = FastRegexMatcher::new(r"a\Q\E{0}").unwrap();
        assert!(qe_zero.is_match(""));
        assert!(!qe_zero.is_match("a"));

        for pat in [
            "(?)a",
            "(?ii)a",
            "(?i-i)a",
            "(?ss:a)",
            "(?P<0>a)",
            "(?<1a>a)",
            "(?P<x>a)|(?P<x>b)",
        ] {
            let m = FastRegexMatcher::new(pat).unwrap();
            assert!(m.is_match("a"), "{pat:?} should match 'a'");
        }
        assert!(!FastRegexMatcher::new("(?i-i)a").unwrap().is_match("A"));

        for pat in [
            r"\p{Beria_Erfe}",
            r"\p{Sidetic}",
            r"\p{Tai_Yo}",
            r"\p{Tolong_Siki}",
            r"\p{Cs}",
            r"\P{Cs}",
            r"[\p{Cs}]",
            r"[^\p{Cs}]",
            r"\x{D800}",
            r"[\x{D800}]",
            r"[\x{D7FF}-\x{E000}]",
        ] {
            assert!(FastRegexMatcher::new(pat).is_ok(), "pattern {pat:?} must compile");
        }
        assert!(!FastRegexMatcher::new(r"\p{Cs}").unwrap().is_match("a"));
        assert!(FastRegexMatcher::new(r"\P{Cs}").unwrap().is_match("a"));
        assert!(!FastRegexMatcher::new(r"\x{D800}").unwrap().is_match("a"));
    }
}
