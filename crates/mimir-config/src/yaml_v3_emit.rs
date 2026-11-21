//! go.yaml.in/yaml/v3 `yaml.Marshal` of generically decoded YAML values.
//!
//! Ported from:
//! - go.yaml.in/yaml/v3/yaml.go (`Marshal`)
//! - go.yaml.in/yaml/v3/encode.go (`encoder`: `marshalDoc`, `marshal`, `mapv`, `slicev`,
//!   `stringv`, `boolv`, `intv`, `uintv`, `floatv`, `nilv`, `isBase60Float`, `isOldBool`)
//! - go.yaml.in/yaml/v3/sorter.go (`keyList.Less`, `keyFloat`, `numLess`)
//! - go.yaml.in/yaml/v3/resolve.go (`resolve`, `parseTimestamp`)
//! - go.yaml.in/yaml/v3/emitterc.go (block/flow sequence and mapping state machine,
//!   `increase_indent_compact`, `check_simple_key`, `select_scalar_style`, `analyze_scalar`,
//!   `write_indent`, `write_indicator`, plain/single-quoted/double-quoted/literal scalar writers,
//!   `write_block_scalar_hints`)
//! - go.yaml.in/yaml/v3/yamlprivateh.go (character class helpers)
//! - Go standard library pieces those depend on: `strconv.ParseInt`/`ParseUint`/`ParseFloat`
//!   (acceptance only), `strconv.FormatFloat(f, 'g', -1, 64)`, `time.Parse` for the four
//!   yaml.v3 timestamp layouts, and the `unicode.IsLetter`/`unicode.IsDigit` tables.
//!
//! dskit's runtimeconfig manager hashes `yaml.Marshal(mergedConfig)` (sha256) for the
//! `cortex_runtime_config_hash` metric, where `mergedConfig` is the result of yaml.v3 decoding
//! every runtime config source into `map[string]interface{}`. [`marshal_generic`] reproduces
//! those bytes for the equivalent [`serde_yaml::Value`] so that Go and Rust pods report the same
//! hash. Only the event shapes the generic encoder can produce are ported: no anchors, tags,
//! comments, flow style requests or folded scalars (yaml.v3 never emits them for plain Go maps,
//! slices and scalars).
//!
//! Known, inherent divergences (the input `Value` no longer carries the scalar's source text or
//! quoting style, so yaml.v3's typing of *plain* scalars cannot be recovered):
//! - serde_yaml keeps some plain scalars as strings that yaml.v3 resolves to numbers or
//!   timestamps: `1_000` (Go: int 1000), `0777` (Go: octal int 511), `09` (Go: float 9),
//!   `2001-12-14` and other timestamps (Go: `time.Time`, re-emitted as RFC3339Nano). They are
//!   emitted here exactly as Go would emit the corresponding *quoted* string.
//! - Values carrying an explicit non-core tag (`Value::Tagged`) are emitted as their inner
//!   value; Go would keep the raw scalar text as a string.
//! - Merge keys (`<<`) are not applied here; call `Value::apply_merge` first if needed.
//! - yaml.v3 rejects mappings where two keys have the same text but different types
//!   (`1` and `"1"`); serde_yaml accepts them.

use std::borrow::Cow;

use serde_yaml::Value;

/// Returns the exact bytes go.yaml.in/yaml/v3 `yaml.Marshal(v)` produces, where `v` is the Go
/// value yaml.v3 decodes the same document into (`map[string]interface{}` /
/// `map[interface{}]interface{}` / `[]interface{}` / `int` / `uint64` / `float64` / `bool` /
/// `nil` / `string`).
pub fn marshal_generic(value: &Value) -> Result<String, String> {
    let mut events = vec![Event::StreamStart, Event::DocumentStart];
    match value {
        Value::Mapping(root) => marshal_root_mapping(root, &mut events)?,
        other => marshal(other, KeyOrder::Sorted, &mut events)?,
    }
    emit(events)
}

/// `yaml.Marshal(v)` for a Go *struct* `v` serialized into `value` field by field: mappings
/// keep their insertion order (yaml.v3 emits struct fields in declaration order), everything
/// else (scalar typing and quoting, indentation, sequences) is identical to
/// [`marshal_generic`]. Go maps nested in the struct are emitted sorted by yaml.v3; reorder such
/// mappings with [`sort_mapping_like_go`] first.
pub fn marshal_struct(value: &Value) -> Result<String, String> {
    let mut events = vec![Event::StreamStart, Event::DocumentStart];
    marshal(value, KeyOrder::Insertion, &mut events)?;
    emit(events)
}

/// Reorders `mapping` by yaml.v3's map key order (sorter.go `keyList.Less`), as `yaml.Marshal`
/// emits a Go map.
pub fn sort_mapping_like_go(mapping: &mut serde_yaml::Mapping) -> Result<(), String> {
    let mut entries = Vec::with_capacity(mapping.len());
    for (k, v) in std::mem::take(mapping) {
        entries.push((SortKey::new(&k)?, k, v));
    }
    for (_, k, v) in sort_by_less(entries, &|a, b| key_less(&a.0, &b.0)) {
        mapping.insert(k, v);
    }
    Ok(())
}

fn emit(mut events: Vec<Event<'_>>) -> Result<String, String> {
    events.push(Event::DocumentEnd);
    events.push(Event::StreamEnd);

    let mut emitter = Emitter::new();
    emitter.emit_all(&events)?;
    String::from_utf8(emitter.out).map_err(|err| format!("yaml: emitted invalid UTF-8: {err}"))
}

/// Mapping key order of [`marshal`]: Go maps are sorted, Go structs keep their field order.
#[derive(Clone, Copy, PartialEq, Eq)]
enum KeyOrder {
    Sorted,
    Insertion,
}

// ---------------------------------------------------------------------------------------------
// Events (yamlh.go `yaml_event_t`, reduced to what encode.go emits for generic values).
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScalarStyle {
    Plain,
    SingleQuoted,
    DoubleQuoted,
    Literal,
}

/// Every event the generic encoder produces is implicit (no tag, no anchor, implicit document
/// start/end), so those flags are not stored.
#[derive(Debug)]
enum Event<'a> {
    StreamStart,
    StreamEnd,
    DocumentStart,
    DocumentEnd,
    SequenceStart { flow: bool },
    SequenceEnd,
    MappingStart,
    MappingEnd,
    Scalar {
        value: Cow<'a, str>,
        style: ScalarStyle,
    },
}

// ---------------------------------------------------------------------------------------------
// Encoder (encode.go).
// ---------------------------------------------------------------------------------------------

fn scalar<'a>(value: impl Into<Cow<'a, str>>, style: ScalarStyle) -> Event<'a> {
    Event::Scalar {
        value: value.into(),
        style,
    }
}

fn flow_field_pairs() -> &'static [(String, String)] {
    use serde::Deserialize;
    use std::sync::OnceLock;

    #[derive(Deserialize)]
    struct Field {
        key: String,
        #[serde(rename = "type")]
        ty: usize,
        #[serde(default)]
        flow: bool,
    }
    #[derive(Deserialize)]
    struct Type {
        #[serde(default)]
        kind: String,
        #[serde(default)]
        elem: Option<usize>,
        #[serde(default)]
        fields: Vec<Field>,
    }
    #[derive(Deserialize)]
    struct Db {
        types: Vec<Type>,
    }

    static FLOW_PAIRS: OnceLock<Vec<(String, String)>> = OnceLock::new();
    FLOW_PAIRS.get_or_init(|| {
        let db: Db = serde_json::from_str(include_str!("yaml_types.json"))
            .expect("valid embedded yaml_types.json");
        let mut pairs = Vec::new();
        for t in &db.types {
            if t.kind != "struct" {
                continue;
            }
            for parent in &t.fields {
                let mut cur = parent.ty;
                while let Some(info) = db.types.get(cur) {
                    match (info.kind.as_str(), info.elem) {
                        ("slice" | "ptr", Some(next)) => cur = next,
                        ("struct", _) => {
                            for child in &info.fields {
                                if child.flow {
                                    pairs.push((parent.key.clone(), child.key.clone()));
                                }
                            }
                            break;
                        }
                        _ => break,
                    }
                }
            }
        }
        pairs
    })
}

fn is_flow_sequence_field(parent_key: Option<&str>, field_key: Option<&str>) -> bool {
    let (Some(p), Some(f)) = (parent_key, field_key) else {
        return false;
    };
    flow_field_pairs()
        .iter()
        .any(|(parent, field)| parent == p && field == f)
}

/// encode.go `marshal`.
fn marshal<'a>(value: &'a Value, order: KeyOrder, events: &mut Vec<Event<'a>>) -> Result<(), String> {
    marshal_with_parent(value, order, None, false, events)
}

fn marshal_with_parent<'a>(
    value: &'a Value,
    order: KeyOrder,
    parent_key: Option<&'a str>,
    flow_seq: bool,
    events: &mut Vec<Event<'a>>,
) -> Result<(), String> {
    match value {
        // nilv
        Value::Null => events.push(scalar("null", ScalarStyle::Plain)),
        // boolv
        Value::Bool(b) => events.push(scalar(
            if *b { "true" } else { "false" },
            ScalarStyle::Plain,
        )),
        Value::Number(n) => {
            let text = if let Some(i) = n.as_i64() {
                // intv: strconv.FormatInt(v, 10)
                i.to_string()
            } else if let Some(u) = n.as_u64() {
                // uintv: strconv.FormatUint(v, 10)
                u.to_string()
            } else if let Some(f) = n.as_f64() {
                // floatv
                format_float(f)?
            } else {
                return Err(format!("yaml: cannot marshal number {n}"));
            };
            events.push(scalar(text, ScalarStyle::Plain));
        }
        Value::String(s) => events.push(stringv(Cow::Borrowed(s))),
        Value::Sequence(seq) => {
            // slicev
            events.push(Event::SequenceStart { flow: flow_seq });
            for item in seq {
                marshal_with_parent(item, order, parent_key, false, events)?;
            }
            events.push(Event::SequenceEnd);
        }
        Value::Mapping(mapping) if order == KeyOrder::Insertion => {
            // structv: fields in declaration order.
            events.push(Event::MappingStart);
            for (k, v) in mapping {
                marshal_with_parent(k, order, None, false, events)?;
                let key_str = k.as_str();
                let child_flow = is_flow_sequence_field(parent_key, key_str);
                marshal_with_parent(v, order, key_str, child_flow, events)?;
            }
            events.push(Event::MappingEnd);
        }
        Value::Mapping(mapping) => {
            // mapv: keys sorted with keyList.Less.
            let mut entries = Vec::with_capacity(mapping.len());
            for (k, v) in mapping {
                entries.push((SortKey::new(k)?, k, v));
            }
            let entries = sort_by_less(entries, &|a, b| key_less(&a.0, &b.0));
            events.push(Event::MappingStart);
            for (_, k, v) in entries {
                marshal(k, order, events)?;
                marshal(v, order, events)?;
            }
            events.push(Event::MappingEnd);
        }
        Value::Tagged(tagged) => marshal(&tagged.value, order, events)?,
    }
    Ok(())
}

/// The root of dskit's merged runtime config is a `map[string]interface{}`: yaml.v3 decodes
/// every root key into a Go string (the scalar's source text) and silently drops null keys,
/// because `d.null` cannot set a string. The source text of non-string keys is not available
/// from a `Value`, so booleans and numbers use their canonical text (Go keeps e.g. `0x10` or
/// `1.50` verbatim). Such keys are rejected by the strict runtime config loader anyway.
fn marshal_root_mapping<'a>(
    root: &'a serde_yaml::Mapping,
    events: &mut Vec<Event<'a>>,
) -> Result<(), String> {
    let mut entries = Vec::with_capacity(root.len());
    for (k, v) in root {
        let mut key = k;
        while let Value::Tagged(tagged) = key {
            key = &tagged.value;
        }
        let text: Cow<'a, str> = match key {
            Value::Null => continue,
            Value::String(s) => Cow::Borrowed(s.as_str()),
            Value::Bool(b) => Cow::Borrowed(if *b { "true" } else { "false" }),
            Value::Number(n) => Cow::Owned(n.to_string()),
            Value::Sequence(_) | Value::Mapping(_) | Value::Tagged(_) => {
                return Err("yaml: invalid map key: sequence or mapping".to_string())
            }
        };
        entries.push((SortKey::Str(text.chars().collect()), text, v));
    }
    let entries = sort_by_less(entries, &|a, b| key_less(&a.0, &b.0));
    events.push(Event::MappingStart);
    for (_, k, v) in entries {
        events.push(stringv(k));
        marshal(v, KeyOrder::Sorted, events)?;
    }
    events.push(Event::MappingEnd);
    Ok(())
}

/// encode.go `stringv` (with an empty tag; Rust strings are always valid UTF-8, so the
/// `!!binary` fallback for invalid UTF-8 cannot trigger).
fn stringv(s: Cow<'_, str>) -> Event<'_> {
    // Check to see if it would resolve to a specific tag when encoded unquoted. If it
    // doesn't, there's no need to quote it.
    let can_use_plain = resolve_tag(&s) == Tag::Str && !(is_base60_float(&s) || is_old_bool(&s));
    let style = if s.contains('\n') {
        ScalarStyle::Literal
    } else if can_use_plain {
        ScalarStyle::Plain
    } else {
        ScalarStyle::DoubleQuoted
    };
    scalar(s, style)
}

/// encode.go `isBase60Float`: `^[-+]?[0-9][0-9_]*(?::[0-5]?[0-9])+(?:\.[0-9_]*)?$`.
fn is_base60_float(s: &str) -> bool {
    let b = s.as_bytes();
    let Some(&c) = b.first() else {
        return false;
    };
    if !(c == b'+' || c == b'-' || c.is_ascii_digit()) || !b.contains(&b':') {
        return false;
    }
    let mut i = 0;
    if b[i] == b'+' || b[i] == b'-' {
        i += 1;
    }
    if !b.get(i).is_some_and(u8::is_ascii_digit) {
        return false;
    }
    i += 1;
    while b.get(i).is_some_and(|c| c.is_ascii_digit() || *c == b'_') {
        i += 1;
    }
    let mut groups = 0;
    while b.get(i) == Some(&b':') {
        i += 1;
        // `[0-5]?[0-9]`: taking two digits whenever possible is equivalent to backtracking,
        // since a leftover digit could never be followed by ':' / '.' / end of input.
        match (b.get(i), b.get(i + 1)) {
            (Some(d1), Some(d2)) if (b'0'..=b'5').contains(d1) && d2.is_ascii_digit() => i += 2,
            (Some(d1), _) if d1.is_ascii_digit() => i += 1,
            _ => return false,
        }
        groups += 1;
    }
    if groups == 0 {
        return false;
    }
    if b.get(i) == Some(&b'.') {
        i += 1;
        while b.get(i).is_some_and(|c| c.is_ascii_digit() || *c == b'_') {
            i += 1;
        }
    }
    i == b.len()
}

/// encode.go `isOldBool`.
fn is_old_bool(s: &str) -> bool {
    matches!(
        s,
        "y" | "Y"
            | "yes"
            | "Yes"
            | "YES"
            | "on"
            | "On"
            | "ON"
            | "n"
            | "N"
            | "no"
            | "No"
            | "NO"
            | "off"
            | "Off"
            | "OFF"
    )
}

/// encode.go `floatv`: `strconv.FormatFloat(f, 'g', -1, 64)` with `+Inf`/`-Inf`/`NaN` mapped to
/// `.inf`/`-.inf`/`.nan`.
fn format_float(f: f64) -> Result<String, String> {
    if f.is_nan() {
        return Ok(".nan".to_string());
    }
    if f.is_infinite() {
        return Ok(if f > 0.0 { ".inf" } else { "-.inf" }.to_string());
    }
    let mut out = String::new();
    if f.is_sign_negative() {
        out.push('-');
    }
    if f == 0.0 {
        out.push('0');
        return Ok(out);
    }
    // Shortest round-trip decimal digits (Rust and Go both pick the shortest digit string that
    // round-trips, closest to the exact value).
    let sci = format!("{:e}", f.abs());
    let (mantissa, exp) = sci
        .split_once('e')
        .ok_or_else(|| format!("yaml: cannot format float {f}"))?;
    let digits: Vec<u8> = mantissa.bytes().filter(|b| *b != b'.').collect();
    let exp: i64 = exp
        .parse()
        .map_err(|_| format!("yaml: cannot format float {f}"))?;
    let nd = digits.len() as i64;
    let dp = exp + 1;
    // %g with shortest precision: eprec = 6.
    let x = dp - 1;
    if !(-4..6).contains(&x) {
        // %e with prec = nd - 1.
        out.push(char::from(digits[0]));
        if nd > 1 {
            out.push('.');
            out.extend(digits[1..].iter().map(|d| char::from(*d)));
        }
        out.push('e');
        out.push(if x < 0 { '-' } else { '+' });
        let ax = x.unsigned_abs();
        if ax < 10 {
            out.push('0');
        }
        out.push_str(&ax.to_string());
    } else {
        // %f with prec = max(nd - dp, 0).
        if dp > 0 {
            let m = nd.min(dp);
            out.extend(digits[..m as usize].iter().map(|d| char::from(*d)));
            for _ in m..dp {
                out.push('0');
            }
        } else {
            out.push('0');
        }
        let prec = (nd - dp).max(0);
        if prec > 0 {
            out.push('.');
            for i in 1..=prec {
                let j = dp + i - 1;
                out.push(if (0..nd).contains(&j) {
                    char::from(digits[j as usize])
                } else {
                    '0'
                });
            }
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------------------------
// Key ordering (sorter.go).
// ---------------------------------------------------------------------------------------------

/// A map key as the Go value yaml.v3 decodes it into (`interface{}` holding the given kind).
enum SortKey {
    Bool(bool),
    Int(i64),
    Uint(u64),
    Float(f64),
    Nil,
    Str(Vec<char>),
}

impl SortKey {
    fn new(key: &Value) -> Result<Self, String> {
        Ok(match key {
            Value::Null => SortKey::Nil,
            Value::Bool(b) => SortKey::Bool(*b),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    SortKey::Int(i)
                } else if let Some(u) = n.as_u64() {
                    SortKey::Uint(u)
                } else {
                    SortKey::Float(n.as_f64().unwrap_or(f64::NAN))
                }
            }
            Value::String(s) => SortKey::Str(s.chars().collect()),
            Value::Tagged(tagged) => SortKey::new(&tagged.value)?,
            // decode.go `mapping`: map and slice keys are not hashable in Go.
            Value::Sequence(_) | Value::Mapping(_) => {
                return Err("yaml: invalid map key: sequence or mapping".to_string())
            }
        })
    }

    /// reflect.Kind of the dynamic value.
    fn kind(&self) -> u8 {
        match self {
            SortKey::Bool(_) => 1,   // reflect.Bool
            SortKey::Int(_) => 2,    // reflect.Int
            SortKey::Uint(_) => 11,  // reflect.Uint64
            SortKey::Float(_) => 14, // reflect.Float64
            SortKey::Nil => 20,      // reflect.Interface (nil interface)
            SortKey::Str(_) => 24,   // reflect.String
        }
    }

    /// sorter.go `keyFloat`.
    fn key_float(&self) -> Option<f64> {
        match self {
            SortKey::Int(i) => Some(*i as f64),
            SortKey::Float(f) => Some(*f),
            SortKey::Uint(u) => Some(*u as f64),
            SortKey::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
            SortKey::Nil | SortKey::Str(_) => None,
        }
    }
}

/// sorter.go `numLess` (a and b have the same kind).
fn num_less(a: &SortKey, b: &SortKey) -> bool {
    match (a, b) {
        (SortKey::Int(a), SortKey::Int(b)) => a < b,
        (SortKey::Float(a), SortKey::Float(b)) => a < b,
        (SortKey::Uint(a), SortKey::Uint(b)) => a < b,
        (SortKey::Bool(a), SortKey::Bool(b)) => !a && *b,
        _ => false,
    }
}

/// sorter.go `keyList.Less`.
fn key_less(a: &SortKey, b: &SortKey) -> bool {
    let (ak, bk) = (a.kind(), b.kind());
    if let (Some(af), Some(bf)) = (a.key_float(), b.key_float()) {
        if af != bf {
            return af < bf;
        }
        if ak != bk {
            return ak < bk;
        }
        return num_less(a, b);
    }
    let (SortKey::Str(ar), SortKey::Str(br)) = (a, b) else {
        return ak < bk;
    };
    let mut digits = false;
    let n = ar.len().min(br.len());
    for i in 0..n {
        if ar[i] == br[i] {
            digits = is_go_digit(ar[i]);
            continue;
        }
        let al = is_go_letter(ar[i]);
        let bl = is_go_letter(br[i]);
        if al && bl {
            return ar[i] < br[i];
        }
        if al || bl {
            return if digits { al } else { bl };
        }
        let (mut an, mut bn) = (0i64, 0i64);
        if ar[i] == '0' || br[i] == '0' {
            for j in (0..i).rev() {
                if !is_go_digit(ar[j]) {
                    break;
                }
                if ar[j] != '0' {
                    an = 1;
                    bn = 1;
                    break;
                }
            }
        }
        let mut ai = i;
        while ai < ar.len() && is_go_digit(ar[ai]) {
            an = an
                .wrapping_mul(10)
                .wrapping_add(i64::from(u32::from(ar[ai])) - i64::from(b'0'));
            ai += 1;
        }
        let mut bi = i;
        while bi < br.len() && is_go_digit(br[bi]) {
            bn = bn
                .wrapping_mul(10)
                .wrapping_add(i64::from(u32::from(br[bi])) - i64::from(b'0'));
            bi += 1;
        }
        if an != bn {
            return an < bn;
        }
        if ai != bi {
            return ai < bi;
        }
        return ar[i] < br[i];
    }
    ar.len() < br.len()
}

/// Stable merge sort driven by a `less` predicate. Unlike `slice::sort_by` it never panics when
/// `less` is not a strict weak order (sorter.go's natural ordering is not transitive for every
/// input; Go's `sort.Sort` simply produces some order in that case).
fn sort_by_less<T>(mut items: Vec<T>, less: &impl Fn(&T, &T) -> bool) -> Vec<T> {
    if items.len() <= 1 {
        return items;
    }
    let right = items.split_off(items.len() / 2);
    let left = sort_by_less(items, less);
    let right = sort_by_less(right, less);
    let mut out = Vec::with_capacity(left.len() + right.len());
    let mut l = left.into_iter().peekable();
    let mut r = right.into_iter().peekable();
    loop {
        match (l.peek(), r.peek()) {
            (Some(a), Some(b)) => {
                if less(b, a) {
                    out.extend(r.next());
                } else {
                    out.extend(l.next());
                }
            }
            (Some(_), None) => {
                out.extend(l);
                break;
            }
            (None, _) => {
                out.extend(r);
                break;
            }
        }
    }
    out
}

fn in_table(table: &[(u32, u32, u32)], c: char) -> bool {
    let r = u32::from(c);
    let idx = table.partition_point(|&(_, hi, _)| hi < r);
    match table.get(idx) {
        Some(&(lo, _, stride)) if lo <= r => stride != 0 && (r - lo) % stride == 0,
        _ => false,
    }
}

/// Go `unicode.IsLetter`.
fn is_go_letter(c: char) -> bool {
    if c.is_ascii() {
        return c.is_ascii_alphabetic();
    }
    in_table(UNICODE_L, c)
}

/// Go `unicode.IsDigit`.
fn is_go_digit(c: char) -> bool {
    if u32::from(c) <= 0xFF {
        return c.is_ascii_digit();
    }
    in_table(UNICODE_ND, c)
}

// ---------------------------------------------------------------------------------------------
// resolve.go `resolve("", in)` (tag only).
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Tag {
    Null,
    Bool,
    Str,
    Int,
    Float,
    Timestamp,
    Merge,
}

fn resolve_hint(c: u8) -> u8 {
    match c {
        b'+' | b'-' => b'S',
        b'0'..=b'9' => b'D',
        b'y' | b'Y' | b'n' | b'N' | b't' | b'T' | b'f' | b'F' | b'o' | b'O' | b'~' => b'M',
        b'.' => b'.',
        _ => 0,
    }
}

fn resolve_map(s: &str) -> Option<Tag> {
    Some(match s {
        "true" | "True" | "TRUE" | "false" | "False" | "FALSE" => Tag::Bool,
        "" | "~" | "null" | "Null" | "NULL" => Tag::Null,
        ".nan" | ".NaN" | ".NAN" | ".inf" | ".Inf" | ".INF" | "+.inf" | "+.Inf" | "+.INF"
        | "-.inf" | "-.Inf" | "-.INF" => Tag::Float,
        "<<" => Tag::Merge,
        _ => return None,
    })
}

/// resolve.go `resolve` for an untagged plain scalar; returns the resolved tag.
fn resolve_tag(s: &str) -> Tag {
    let hint = s.as_bytes().first().map_or(b'N', |c| resolve_hint(*c));
    if hint == 0 {
        return Tag::Str;
    }
    if let Some(tag) = resolve_map(s) {
        return tag;
    }
    match hint {
        b'.' => {
            if go_parse_float(s) {
                return Tag::Float;
            }
        }
        b'D' | b'S' => {
            if parse_timestamp(s) {
                return Tag::Timestamp;
            }
            let plain = s.replace('_', "");
            if go_parse_int(&plain, 0).is_some() || go_parse_uint(&plain, 0).is_some() {
                return Tag::Int;
            }
            if yaml_style_float(&plain) && go_parse_float(&plain) {
                return Tag::Float;
            }
            if let Some(rest) = plain.strip_prefix("0b") {
                if go_parse_int(rest, 2).is_some() || go_parse_uint(rest, 2).is_some() {
                    return Tag::Int;
                }
            } else if let Some(rest) = plain.strip_prefix("-0b") {
                if go_parse_int(&format!("-{rest}"), 2).is_some() {
                    return Tag::Int;
                }
            }
            if let Some(rest) = plain.strip_prefix("0o") {
                if go_parse_int(rest, 8).is_some() || go_parse_uint(rest, 8).is_some() {
                    return Tag::Int;
                }
            } else if let Some(rest) = plain.strip_prefix("-0o") {
                if go_parse_int(&format!("-{rest}"), 8).is_some() {
                    return Tag::Int;
                }
            }
        }
        _ => {}
    }
    Tag::Str
}

/// resolve.go `yamlStyleFloat`: `^[-+]?(\.[0-9]+|[0-9]+(\.[0-9]*)?)([eE][-+]?[0-9]+)?$`.
fn yaml_style_float(s: &str) -> bool {
    let b = s.as_bytes();
    let mut i = 0;
    let digits = |i: &mut usize| {
        let start = *i;
        while b.get(*i).is_some_and(u8::is_ascii_digit) {
            *i += 1;
        }
        *i - start
    };
    if matches!(b.first(), Some(b'-' | b'+')) {
        i += 1;
    }
    if b.get(i) == Some(&b'.') {
        i += 1;
        if digits(&mut i) == 0 {
            return false;
        }
    } else {
        if digits(&mut i) == 0 {
            return false;
        }
        if b.get(i) == Some(&b'.') {
            i += 1;
            digits(&mut i);
        }
    }
    if matches!(b.get(i), Some(b'e' | b'E')) {
        i += 1;
        if matches!(b.get(i), Some(b'-' | b'+')) {
            i += 1;
        }
        if digits(&mut i) == 0 {
            return false;
        }
    }
    i == b.len()
}

fn lower(c: u8) -> u8 {
    c | (b'x' - b'X')
}

/// Go `strconv.underscoreOK`.
fn underscore_ok(s: &[u8]) -> bool {
    let mut saw = b'^';
    let mut i = 0;
    let s = match s.first() {
        Some(b'-' | b'+') => &s[1..],
        _ => s,
    };
    let mut hex = false;
    if s.len() >= 2 && s[0] == b'0' && matches!(lower(s[1]), b'b' | b'o' | b'x') {
        i = 2;
        saw = b'0';
        hex = lower(s[1]) == b'x';
    }
    while i < s.len() {
        let c = s[i];
        if c.is_ascii_digit() || hex && (b'a'..=b'f').contains(&lower(c)) {
            saw = b'0';
        } else if c == b'_' {
            if saw != b'0' {
                return false;
            }
            saw = b'_';
        } else {
            if saw == b'_' {
                return false;
            }
            saw = b'!';
        }
        i += 1;
    }
    saw != b'_'
}

/// Go `strconv.ParseUint(s, base, 64)`; `None` on any error.
fn go_parse_uint(s: &str, base: u32) -> Option<u64> {
    let s0 = s.as_bytes();
    let mut b = s0;
    if b.is_empty() {
        return None;
    }
    let base0 = base == 0;
    let mut base = u64::from(base);
    if base0 {
        base = 10;
        if b[0] == b'0' {
            if b.len() >= 3 && lower(b[1]) == b'b' {
                base = 2;
                b = &b[2..];
            } else if b.len() >= 3 && lower(b[1]) == b'o' {
                base = 8;
                b = &b[2..];
            } else if b.len() >= 3 && lower(b[1]) == b'x' {
                base = 16;
                b = &b[2..];
            } else {
                base = 8;
                b = &b[1..];
            }
        }
    } else if !(2..=36).contains(&base) {
        return None;
    }
    let cutoff = u64::MAX / base + 1;
    let mut underscores = false;
    let mut n: u64 = 0;
    for &c in b {
        let d = if c == b'_' && base0 {
            underscores = true;
            continue;
        } else if c.is_ascii_digit() {
            c - b'0'
        } else if lower(c).is_ascii_lowercase() {
            lower(c) - b'a' + 10
        } else {
            return None;
        };
        if u64::from(d) >= base {
            return None;
        }
        if n >= cutoff {
            return None;
        }
        n *= base;
        n = n.checked_add(u64::from(d))?;
    }
    if underscores && !underscore_ok(s0) {
        return None;
    }
    Some(n)
}

/// Go `strconv.ParseInt(s, base, 64)`; `None` on any error.
fn go_parse_int(s: &str, base: u32) -> Option<i64> {
    let (neg, rest) = match s.as_bytes().first() {
        None => return None,
        Some(b'+') => (false, &s[1..]),
        Some(b'-') => (true, &s[1..]),
        Some(_) => (false, s),
    };
    let un = go_parse_uint(rest, base)?;
    const CUTOFF: u64 = 1 << 63;
    if !neg && un >= CUTOFF || neg && un > CUTOFF {
        return None;
    }
    Some(if neg {
        (un as i64).wrapping_neg()
    } else {
        un as i64
    })
}

/// Go `strconv.ParseFloat(s, 64)` succeeding without error (syntax or range).
fn go_parse_float(s: &str) -> bool {
    let b = s.as_bytes();
    // special(): infinities and NaN.
    if let Some(n) = go_float_special(b) {
        return n == b.len();
    }
    let Some(read) = go_read_float(b) else {
        return false;
    };
    if read.end != b.len() {
        return false;
    }
    let text: String = s[..read.end].chars().filter(|c| *c != '_').collect();
    let value = if read.hex {
        // Unreachable from resolve(): hex floats never pass its prefix/regex checks.
        go_hex_float_value(&text)
    } else {
        text.parse::<f64>().ok()
    };
    // ErrRange is reported when the value overflows to ±Inf (underflow to 0 is not an error).
    value.is_some_and(f64::is_finite)
}

fn common_prefix_len_ignore_case(s: &[u8], prefix: &[u8]) -> usize {
    s.iter()
        .zip(prefix)
        .take_while(|(a, b)| lower(**a) == **b)
        .count()
}

/// Go `strconv.special`: returns the consumed length when `s` starts with inf/infinity/nan.
fn go_float_special(s: &[u8]) -> Option<usize> {
    let first = *s.first()?;
    match first {
        b'+' | b'-' | b'i' | b'I' => {
            let (nsign, rest) = if first == b'+' || first == b'-' {
                (1, &s[1..])
            } else {
                (0, s)
            };
            let mut n = common_prefix_len_ignore_case(rest, b"infinity");
            if 3 < n && n < 8 {
                n = 3;
            }
            if n == 3 || n == 8 {
                return Some(nsign + n);
            }
            None
        }
        b'n' | b'N' => (common_prefix_len_ignore_case(s, b"nan") == 3).then_some(3),
        _ => None,
    }
}

struct ReadFloat {
    end: usize,
    hex: bool,
}

/// Go `strconv.readFloat` (syntax acceptance and consumed length).
fn go_read_float(s: &[u8]) -> Option<ReadFloat> {
    let mut i = 0;
    let mut underscores = false;
    if i >= s.len() {
        return None;
    }
    if s[i] == b'+' || s[i] == b'-' {
        i += 1;
    }
    let mut hex = false;
    let mut exp_char = b'e';
    if i + 2 < s.len() && s[i] == b'0' && lower(s[i + 1]) == b'x' {
        i += 2;
        exp_char = b'p';
        hex = true;
    }
    let mut saw_dot = false;
    let mut saw_digits = false;
    while i < s.len() {
        let c = s[i];
        if c == b'_' {
            underscores = true;
        } else if c == b'.' {
            if saw_dot {
                break;
            }
            saw_dot = true;
        } else if c.is_ascii_digit() || hex && (b'a'..=b'f').contains(&lower(c)) {
            saw_digits = true;
        } else {
            break;
        }
        i += 1;
    }
    if !saw_digits {
        return None;
    }
    if i < s.len() && lower(s[i]) == exp_char {
        i += 1;
        if i >= s.len() {
            return None;
        }
        if s[i] == b'+' || s[i] == b'-' {
            i += 1;
        }
        if i >= s.len() || !s[i].is_ascii_digit() {
            return None;
        }
        while i < s.len() && (s[i].is_ascii_digit() || s[i] == b'_') {
            if s[i] == b'_' {
                underscores = true;
            }
            i += 1;
        }
    } else if hex {
        return None;
    }
    if underscores && !underscore_ok(&s[..i]) {
        return None;
    }
    Some(ReadFloat { end: i, hex })
}

/// Approximate value of a validated Go hex float literal (`0x1.8p3`), used only for the
/// overflow check.
fn go_hex_float_value(text: &str) -> Option<f64> {
    let (neg, rest) = match text.as_bytes().first() {
        Some(b'-') => (true, &text[1..]),
        Some(b'+') => (false, &text[1..]),
        _ => (false, text),
    };
    let rest = rest.get(2..)?;
    let (mantissa, exp) = rest.split_once(['p', 'P'])?;
    let exp: i64 = exp.parse().unwrap_or(if exp.starts_with('-') {
        i64::MIN / 2
    } else {
        i64::MAX / 2
    });
    let (int_part, frac_part) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let mut value = 0f64;
    for c in int_part.chars().chain(frac_part.chars()) {
        value = value * 16.0 + f64::from(c.to_digit(16)?);
    }
    let shift = exp
        .saturating_sub(4 * frac_part.len() as i64)
        .clamp(-2000, 2000) as i32;
    let value = value * 2f64.powi(shift);
    Some(if neg { -value } else { value })
}

/// resolve.go `parseTimestamp`.
fn parse_timestamp(s: &str) -> bool {
    let b = s.as_bytes();
    let i = b.iter().take_while(|c| c.is_ascii_digit()).count();
    if i != 4 || i == b.len() || b[i] != b'-' {
        return false;
    }
    use TimeTok::*;
    const RFC3339_NANO_SHORT: &[TimeTok] = &[
        LongYear,
        Lit("-"),
        NumMonth,
        Lit("-"),
        Day,
        Lit("T"),
        Hour,
        Lit(":"),
        Minute,
        Lit(":"),
        Second,
        FracSecond9,
        Iso8601ColonTz,
    ];
    const RFC3339_NANO_SHORT_LOWER_T: &[TimeTok] = &[
        LongYear,
        Lit("-"),
        NumMonth,
        Lit("-"),
        Day,
        Lit("t"),
        Hour,
        Lit(":"),
        Minute,
        Lit(":"),
        Second,
        FracSecond9,
        Iso8601ColonTz,
    ];
    const SPACE_SEPARATED: &[TimeTok] = &[
        LongYear,
        Lit("-"),
        NumMonth,
        Lit("-"),
        Day,
        Lit(" "),
        Hour,
        Lit(":"),
        Minute,
        Lit(":"),
        Second,
        FracSecond9,
    ];
    const DATE_ONLY: &[TimeTok] = &[LongYear, Lit("-"), NumMonth, Lit("-"), Day];
    [
        RFC3339_NANO_SHORT,
        RFC3339_NANO_SHORT_LOWER_T,
        SPACE_SEPARATED,
        DATE_ONLY,
    ]
    .iter()
    .any(|layout| go_time_parse(layout, b))
}

/// The `time.Parse` layout elements used by yaml.v3's timestamp formats
/// ("2006-1-2T15:4:5.999999999Z07:00", "2006-1-2t15:4:5.999999999Z07:00",
/// "2006-1-2 15:4:5.999999999", "2006-1-2").
#[derive(Clone, Copy)]
enum TimeTok {
    Lit(&'static str),
    LongYear,
    NumMonth,
    Day,
    Hour,
    Minute,
    Second,
    FracSecond9,
    Iso8601ColonTz,
}

/// Go time `getnum`.
fn getnum(s: &[u8], fixed: bool) -> Option<(i32, &[u8])> {
    let d0 = *s.first().filter(|c| c.is_ascii_digit())?;
    match s.get(1).filter(|c| c.is_ascii_digit()) {
        None if fixed => None,
        None => Some((i32::from(d0 - b'0'), &s[1..])),
        Some(d1) => Some((i32::from(d0 - b'0') * 10 + i32::from(d1 - b'0'), &s[2..])),
    }
}

/// Go time `skip`.
fn time_skip<'s>(mut value: &'s [u8], prefix: &str) -> Option<&'s [u8]> {
    let mut prefix = prefix.as_bytes();
    while let Some(&p) = prefix.first() {
        if p == b' ' {
            if value.first().is_some_and(|c| *c != b' ') {
                return None;
            }
            while prefix.first() == Some(&b' ') {
                prefix = &prefix[1..];
            }
            while value.first() == Some(&b' ') {
                value = &value[1..];
            }
            continue;
        }
        if value.first() != Some(&p) {
            return None;
        }
        prefix = &prefix[1..];
        value = &value[1..];
    }
    Some(value)
}

fn days_in(month: i32, year: i32) -> i32 {
    match month {
        2 if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// Go `time.Parse(layout, value)` succeeding, for the layout elements in [`TimeTok`].
fn go_time_parse(layout: &[TimeTok], mut value: &[u8]) -> bool {
    let (mut year, mut month, mut day) = (0i32, -1i32, -1i32);
    for tok in layout {
        let next = match *tok {
            TimeTok::Lit(prefix) => time_skip(value, prefix),
            TimeTok::LongYear => {
                if value.len() < 4 || !value[0].is_ascii_digit() {
                    None
                } else {
                    // parseTimestamp already checked for four leading digits.
                    year = value[..4]
                        .iter()
                        .fold(0, |acc, c| acc * 10 + i32::from(c.wrapping_sub(b'0')));
                    Some(&value[4..])
                }
            }
            TimeTok::NumMonth => getnum(value, false).and_then(|(m, rest)| {
                month = m;
                (1..=12).contains(&m).then_some(rest)
            }),
            TimeTok::Day => getnum(value, false).map(|(d, rest)| {
                day = d;
                rest
            }),
            TimeTok::Hour => {
                getnum(value, false).and_then(|(h, rest)| (0..24).contains(&h).then_some(rest))
            }
            TimeTok::Minute | TimeTok::Second => {
                // Every layout here follows seconds with ".999999999", so stdSecond's
                // fractional-second special case defers to FracSecond9.
                getnum(value, false).and_then(|(m, rest)| (0..60).contains(&m).then_some(rest))
            }
            TimeTok::FracSecond9 => {
                if value.len() < 2 || !matches!(value[0], b'.' | b',') || !value[1].is_ascii_digit()
                {
                    // Fractional second omitted.
                    Some(value)
                } else {
                    let n = 1 + value[1..].iter().take_while(|c| c.is_ascii_digit()).count();
                    Some(&value[n..])
                }
            }
            TimeTok::Iso8601ColonTz => {
                if value.first() == Some(&b'Z') {
                    Some(&value[1..])
                } else if value.len() < 6 || value[3] != b':' {
                    None
                } else {
                    let hr = getnum(&value[1..3], true);
                    let mm = getnum(&value[4..6], true);
                    match (hr, mm) {
                        (Some((hr, _)), Some((mm, _)))
                            if hr <= 24 && mm <= 60 && matches!(value[0], b'+' | b'-') =>
                        {
                            Some(&value[6..])
                        }
                        _ => None,
                    }
                }
            }
        };
        match next {
            Some(rest) => value = rest,
            None => return false,
        }
    }
    if !value.is_empty() {
        // extra text
        return false;
    }
    day >= 1 && day <= days_in(month, year)
}

// ---------------------------------------------------------------------------------------------
// Emitter (emitterc.go).
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    StreamStart,
    FirstDocumentStart,
    DocumentStart,
    DocumentContent,
    DocumentEnd,
    FlowSequenceFirstItem,
    FlowSequenceItem,
    FlowMappingFirstKey,
    FlowMappingKey,
    FlowMappingSimpleValue,
    FlowMappingValue,
    BlockSequenceFirstItem,
    BlockSequenceItem,
    BlockMappingFirstKey,
    BlockMappingKey,
    BlockMappingSimpleValue,
    BlockMappingValue,
    End,
}

#[derive(Default)]
struct ScalarData {
    multiline: bool,
    flow_plain_allowed: bool,
    block_plain_allowed: bool,
    single_quoted_allowed: bool,
    block_allowed: bool,
    style: Option<ScalarStyle>,
}

/// emitterc.go `yaml_emitter_t` (the `line`, `space_above` and `sequence_context` fields are
/// write-only for the events the encoder produces and are omitted).
struct Emitter {
    out: Vec<u8>,
    best_indent: i64,
    best_width: i64,
    unicode: bool,
    compact_sequence_indent: bool,

    state: State,
    states: Vec<State>,
    indent: i64,
    indents: Vec<i64>,
    flow_level: i64,

    root_context: bool,
    mapping_context: bool,
    simple_key_context: bool,

    column: i64,
    whitespace: bool,
    indention: bool,
    open_ended: bool,
    foot_indent: i64,

    scalar_data: ScalarData,
}

// yamlprivateh.go character classes. Out-of-range reads see NUL, like libyaml's terminator.
fn at(b: &[u8], i: usize) -> u8 {
    b.get(i).copied().unwrap_or(0)
}

fn is_ascii(b: &[u8], i: usize) -> bool {
    at(b, i) <= 0x7F
}

fn is_printable(b: &[u8], i: usize) -> bool {
    let (c0, c1, c2) = (at(b, i), at(b, i + 1), at(b, i + 2));
    c0 == 0x0A
        || (0x20..=0x7E).contains(&c0)
        || (c0 == 0xC2 && c1 >= 0xA0)
        || (c0 > 0xC2 && c0 < 0xED)
        || (c0 == 0xED && c1 < 0xA0)
        || c0 == 0xEE
        || (c0 == 0xEF
            && !(c1 == 0xBB && c2 == 0xBF)
            && !(c1 == 0xBF && (c2 == 0xBE || c2 == 0xBF)))
}

/// Note: like upstream, this inspects the *start* of the buffer regardless of `i`.
fn is_bom(b: &[u8], _i: usize) -> bool {
    at(b, 0) == 0xEF && at(b, 1) == 0xBB && at(b, 2) == 0xBF
}

fn is_space(b: &[u8], i: usize) -> bool {
    at(b, i) == b' '
}

fn is_blank(b: &[u8], i: usize) -> bool {
    matches!(at(b, i), b' ' | b'\t')
}

fn is_break(b: &[u8], i: usize) -> bool {
    let (c0, c1, c2) = (at(b, i), at(b, i + 1), at(b, i + 2));
    c0 == b'\r'
        || c0 == b'\n'
        || (c0 == 0xC2 && c1 == 0x85)
        || (c0 == 0xE2 && c1 == 0x80 && c2 == 0xA8)
        || (c0 == 0xE2 && c1 == 0x80 && c2 == 0xA9)
}

fn is_blankz(b: &[u8], i: usize) -> bool {
    is_blank(b, i) || is_break(b, i) || at(b, i) == 0
}

fn width(c: u8) -> usize {
    if c & 0x80 == 0x00 {
        1
    } else if c & 0xE0 == 0xC0 {
        2
    } else if c & 0xF0 == 0xE0 {
        3
    } else if c & 0xF8 == 0xF0 {
        4
    } else {
        0
    }
}

const INTERNAL_ERROR: &str = "yaml: internal emitter error";

impl Emitter {
    /// apic.go `yaml_emitter_initialize` + encode.go `newEncoder`/`init` (unicode, indent 4).
    fn new() -> Self {
        Emitter {
            out: Vec::new(),
            best_indent: 4,
            best_width: -1,
            unicode: true,
            compact_sequence_indent: false,
            state: State::StreamStart,
            states: Vec::new(),
            indent: 0,
            indents: Vec::new(),
            flow_level: 0,
            root_context: false,
            mapping_context: false,
            simple_key_context: false,
            column: 0,
            whitespace: false,
            indention: false,
            open_ended: false,
            foot_indent: 0,
            scalar_data: ScalarData::default(),
        }
    }

    fn emit_all(&mut self, events: &[Event<'_>]) -> Result<(), String> {
        for head in 0..events.len() {
            if matches!(events[head], Event::StreamEnd) {
                // encode.go `finish`.
                self.open_ended = false;
            }
            if let Event::Scalar { value, .. } = &events[head] {
                self.analyze_scalar(value.as_bytes());
            }
            self.state_machine(events, head)?;
        }
        Ok(())
    }

    fn pop_state(&mut self) -> Result<(), String> {
        self.state = self.states.pop().ok_or(INTERNAL_ERROR)?;
        Ok(())
    }

    fn pop_indent(&mut self) -> Result<(), String> {
        self.indent = self.indents.pop().ok_or(INTERNAL_ERROR)?;
        Ok(())
    }

    // --- output primitives ---

    fn put(&mut self, value: u8) {
        self.out.push(value);
        self.column += 1;
    }

    fn put_break(&mut self) {
        self.out.push(b'\n');
        self.column = 0;
        self.indention = true;
    }

    fn write(&mut self, s: &[u8], i: &mut usize) -> Result<(), String> {
        let w = width(at(s, *i));
        let bytes = s.get(*i..*i + w).filter(|_| w > 0).ok_or(INTERNAL_ERROR)?;
        self.out.extend_from_slice(bytes);
        self.column += 1;
        *i += w;
        Ok(())
    }

    fn write_all(&mut self, s: &[u8]) -> Result<(), String> {
        let mut i = 0;
        while i < s.len() {
            self.write(s, &mut i)?;
        }
        Ok(())
    }

    fn write_break(&mut self, s: &[u8], i: &mut usize) -> Result<(), String> {
        if at(s, *i) == b'\n' {
            self.put_break();
            *i += 1;
        } else {
            self.write(s, i)?;
            self.column = 0;
            self.indention = true;
        }
        Ok(())
    }

    // --- state machine ---

    fn state_machine(&mut self, events: &[Event<'_>], head: usize) -> Result<(), String> {
        match self.state {
            State::StreamStart => self.emit_stream_start(&events[head]),
            State::FirstDocumentStart => self.emit_document_start(&events[head], true),
            State::DocumentStart => self.emit_document_start(&events[head], false),
            State::DocumentContent => self.emit_document_content(events, head),
            State::DocumentEnd => self.emit_document_end(&events[head]),
            State::FlowSequenceFirstItem => self.emit_flow_sequence_item(events, head, true),
            State::FlowSequenceItem => self.emit_flow_sequence_item(events, head, false),
            State::FlowMappingFirstKey => self.emit_flow_mapping_key(events, head, true),
            State::FlowMappingKey => self.emit_flow_mapping_key(events, head, false),
            State::FlowMappingSimpleValue => self.emit_flow_mapping_value(events, head, true),
            State::FlowMappingValue => self.emit_flow_mapping_value(events, head, false),
            State::BlockSequenceFirstItem => self.emit_block_sequence_item(events, head, true),
            State::BlockSequenceItem => self.emit_block_sequence_item(events, head, false),
            State::BlockMappingFirstKey => self.emit_block_mapping_key(events, head, true),
            State::BlockMappingKey => self.emit_block_mapping_key(events, head, false),
            State::BlockMappingSimpleValue => self.emit_block_mapping_value(events, head, true),
            State::BlockMappingValue => self.emit_block_mapping_value(events, head, false),
            State::End => Err("yaml: expected nothing after STREAM-END".to_string()),
        }
    }

    fn emit_stream_start(&mut self, event: &Event<'_>) -> Result<(), String> {
        if !matches!(event, Event::StreamStart) {
            return Err("yaml: expected STREAM-START".to_string());
        }
        if self.best_indent < 2 || self.best_indent > 9 {
            self.best_indent = 2;
        }
        if self.best_width >= 0 && self.best_width <= self.best_indent * 2 {
            self.best_width = 80;
        }
        if self.best_width < 0 {
            self.best_width = (1 << 31) - 1;
        }
        self.indent = -1;
        self.column = 0;
        self.whitespace = true;
        self.indention = true;
        self.foot_indent = -1;
        self.state = State::FirstDocumentStart;
        Ok(())
    }

    fn emit_document_start(&mut self, event: &Event<'_>, first: bool) -> Result<(), String> {
        match event {
            Event::DocumentStart => {
                // The encoder's document start is implicit; only the first one stays implicit.
                if !first {
                    self.write_indent();
                    self.write_indicator(b"---", true, false, false)?;
                    self.write_indent();
                }
                self.state = State::DocumentContent;
                Ok(())
            }
            Event::StreamEnd => {
                if self.open_ended {
                    self.write_indicator(b"...", true, false, false)?;
                    self.write_indent();
                }
                self.state = State::End;
                Ok(())
            }
            _ => Err("yaml: expected DOCUMENT-START or STREAM-END".to_string()),
        }
    }

    fn emit_document_content(&mut self, events: &[Event<'_>], head: usize) -> Result<(), String> {
        self.states.push(State::DocumentEnd);
        self.emit_node(events, head, true, false, false, false)
    }

    fn emit_document_end(&mut self, event: &Event<'_>) -> Result<(), String> {
        if !matches!(event, Event::DocumentEnd) {
            return Err("yaml: expected DOCUMENT-END".to_string());
        }
        // [Go] Force document foot separation (foot_indent = 0, then -1 after the absent foot
        // comment).
        self.foot_indent = -1;
        self.write_indent();
        // The encoder's document end is implicit: no "...".
        self.state = State::DocumentStart;
        Ok(())
    }

    fn emit_flow_sequence_item(
        &mut self,
        events: &[Event<'_>],
        head: usize,
        first: bool,
    ) -> Result<(), String> {
        if first {
            self.write_indicator(b"[", true, true, false)?;
            self.increase_indent(true, false);
            self.flow_level += 1;
        }
        if matches!(events[head], Event::SequenceEnd) {
            self.flow_level -= 1;
            self.pop_indent()?;
            if self.column == 0 {
                self.write_indent();
            }
            self.write_indicator(b"]", false, false, false)?;
            return self.pop_state();
        }
        if !first {
            self.write_indicator(b",", false, false, false)?;
        }
        if self.column == 0 {
            self.write_indent();
        }
        if self.column > self.best_width {
            self.write_indent();
        }
        self.states.push(State::FlowSequenceItem);
        self.emit_node(events, head, false, true, false, false)
    }

    fn emit_flow_mapping_key(
        &mut self,
        events: &[Event<'_>],
        head: usize,
        first: bool,
    ) -> Result<(), String> {
        if first {
            self.write_indicator(b"{", true, true, false)?;
            self.increase_indent(true, false);
            self.flow_level += 1;
        }
        if matches!(events[head], Event::MappingEnd) {
            self.flow_level -= 1;
            self.pop_indent()?;
            self.write_indicator(b"}", false, false, false)?;
            return self.pop_state();
        }
        if !first {
            self.write_indicator(b",", false, false, false)?;
        }
        if self.column == 0 {
            self.write_indent();
        }
        if self.column > self.best_width {
            self.write_indent();
        }
        if self.check_simple_key(events, head) {
            self.states.push(State::FlowMappingSimpleValue);
            return self.emit_node(events, head, false, false, true, true);
        }
        self.write_indicator(b"?", true, false, false)?;
        self.states.push(State::FlowMappingValue);
        self.emit_node(events, head, false, false, true, false)
    }

    fn emit_flow_mapping_value(
        &mut self,
        events: &[Event<'_>],
        head: usize,
        simple: bool,
    ) -> Result<(), String> {
        if simple {
            self.write_indicator(b":", false, false, false)?;
        } else {
            if self.column > self.best_width {
                self.write_indent();
            }
            self.write_indicator(b":", true, false, false)?;
        }
        self.states.push(State::FlowMappingKey);
        self.emit_node(events, head, false, false, true, false)
    }

    fn emit_block_sequence_item(
        &mut self,
        events: &[Event<'_>],
        head: usize,
        first: bool,
    ) -> Result<(), String> {
        if first {
            let seq = self.mapping_context
                && (self.column == 0 || !self.indention)
                && self.compact_sequence_indent;
            self.increase_indent_compact(false, false, seq);
        }
        if matches!(events[head], Event::SequenceEnd) {
            self.pop_indent()?;
            return self.pop_state();
        }
        self.write_indent();
        self.write_indicator(b"-", true, false, true)?;
        self.states.push(State::BlockSequenceItem);
        self.emit_node(events, head, false, true, false, false)
    }

    fn emit_block_mapping_key(
        &mut self,
        events: &[Event<'_>],
        head: usize,
        first: bool,
    ) -> Result<(), String> {
        if first {
            self.increase_indent(false, false);
        }
        if matches!(events[head], Event::MappingEnd) {
            self.pop_indent()?;
            return self.pop_state();
        }
        self.write_indent();
        if self.check_simple_key(events, head) {
            self.states.push(State::BlockMappingSimpleValue);
            return self.emit_node(events, head, false, false, true, true);
        }
        self.write_indicator(b"?", true, false, true)?;
        self.states.push(State::BlockMappingValue);
        self.emit_node(events, head, false, false, true, false)
    }

    fn emit_block_mapping_value(
        &mut self,
        events: &[Event<'_>],
        head: usize,
        simple: bool,
    ) -> Result<(), String> {
        if simple {
            self.write_indicator(b":", false, false, false)?;
        } else {
            self.write_indent();
            self.write_indicator(b":", true, false, true)?;
        }
        self.states.push(State::BlockMappingKey);
        self.emit_node(events, head, false, false, true, false)
    }

    fn emit_node(
        &mut self,
        events: &[Event<'_>],
        head: usize,
        root: bool,
        _sequence: bool,
        mapping: bool,
        simple_key: bool,
    ) -> Result<(), String> {
        self.root_context = root;
        self.mapping_context = mapping;
        self.simple_key_context = simple_key;
        match &events[head] {
            Event::Scalar { value, style } => self.emit_scalar(value.as_bytes(), *style),
            Event::SequenceStart { flow } => {
                self.state = if *flow || self.flow_level > 0 || self.check_empty_sequence(events, head) {
                    State::FlowSequenceFirstItem
                } else {
                    State::BlockSequenceFirstItem
                };
                Ok(())
            }
            Event::MappingStart => {
                self.state = if self.flow_level > 0 || self.check_empty_mapping(events, head) {
                    State::FlowMappingFirstKey
                } else {
                    State::BlockMappingFirstKey
                };
                Ok(())
            }
            other => Err(format!(
                "yaml: expected SCALAR, SEQUENCE-START, MAPPING-START, or ALIAS, but got {other:?}"
            )),
        }
    }

    fn emit_scalar(&mut self, value: &[u8], style: ScalarStyle) -> Result<(), String> {
        self.select_scalar_style(value, style);
        self.increase_indent(true, false);
        self.process_scalar(value)?;
        self.pop_indent()?;
        self.pop_state()
    }

    fn check_empty_sequence(&self, events: &[Event<'_>], head: usize) -> bool {
        matches!(events.get(head), Some(Event::SequenceStart { .. }))
            && matches!(events.get(head + 1), Some(Event::SequenceEnd))
    }

    fn check_empty_mapping(&self, events: &[Event<'_>], head: usize) -> bool {
        matches!(events.get(head), Some(Event::MappingStart))
            && matches!(events.get(head + 1), Some(Event::MappingEnd))
    }

    fn check_simple_key(&self, events: &[Event<'_>], head: usize) -> bool {
        let length = match &events[head] {
            Event::Scalar { value, .. } => {
                if self.scalar_data.multiline {
                    return false;
                }
                value.len()
            }
            Event::SequenceStart { .. } => {
                if !self.check_empty_sequence(events, head) {
                    return false;
                }
                0
            }
            Event::MappingStart => {
                if !self.check_empty_mapping(events, head) {
                    return false;
                }
                0
            }
            _ => return false,
        };
        length <= 128
    }

    /// emitterc.go `yaml_emitter_increase_indent_compact`.
    fn increase_indent_compact(&mut self, flow: bool, indentless: bool, compact_seq: bool) {
        self.indents.push(self.indent);
        if self.indent < 0 {
            self.indent = if flow { self.best_indent } else { 0 };
        } else if !indentless {
            // [Go] This was changed so that indentations are more regular.
            if self.states.last() == Some(&State::BlockSequenceItem) {
                // The first indent inside a sequence will just skip the "- " indicator.
                self.indent += 2;
            } else {
                // Everything else aligns to the chosen indentation.
                self.indent =
                    self.best_indent * ((self.indent + self.best_indent) / self.best_indent);
                if compact_seq {
                    self.indent -= 2;
                }
            }
        }
    }

    fn increase_indent(&mut self, flow: bool, indentless: bool) {
        self.increase_indent_compact(flow, indentless, false);
    }

    /// emitterc.go `yaml_emitter_select_scalar_style` (implicit, untagged scalars).
    fn select_scalar_style(&mut self, value: &[u8], requested: ScalarStyle) {
        let mut style = requested;
        if self.simple_key_context && self.scalar_data.multiline {
            style = ScalarStyle::DoubleQuoted;
        }
        if style == ScalarStyle::Plain {
            if self.flow_level > 0 && !self.scalar_data.flow_plain_allowed
                || self.flow_level == 0 && !self.scalar_data.block_plain_allowed
            {
                style = ScalarStyle::SingleQuoted;
            }
            if value.is_empty() && (self.flow_level > 0 || self.simple_key_context) {
                style = ScalarStyle::SingleQuoted;
            }
        }
        if style == ScalarStyle::SingleQuoted && !self.scalar_data.single_quoted_allowed {
            style = ScalarStyle::DoubleQuoted;
        }
        if style == ScalarStyle::Literal
            && (!self.scalar_data.block_allowed || self.flow_level > 0 || self.simple_key_context)
        {
            style = ScalarStyle::DoubleQuoted;
        }
        self.scalar_data.style = Some(style);
    }

    fn process_scalar(&mut self, value: &[u8]) -> Result<(), String> {
        match self.scalar_data.style {
            Some(ScalarStyle::Plain) => self.write_plain_scalar(value, !self.simple_key_context),
            Some(ScalarStyle::SingleQuoted) => {
                self.write_single_quoted_scalar(value, !self.simple_key_context)
            }
            Some(ScalarStyle::DoubleQuoted) => {
                self.write_double_quoted_scalar(value, !self.simple_key_context)
            }
            Some(ScalarStyle::Literal) => self.write_literal_scalar(value),
            None => Err(INTERNAL_ERROR.to_string()),
        }
    }

    /// emitterc.go `yaml_emitter_analyze_scalar`.
    fn analyze_scalar(&mut self, value: &[u8]) {
        let mut block_indicators = false;
        let mut flow_indicators = false;
        let mut line_breaks = false;
        let mut special_characters = false;
        let mut tab_characters = false;

        let mut leading_space = false;
        let mut leading_break = false;
        let mut trailing_space = false;
        let mut trailing_break = false;
        let mut break_space = false;
        let mut space_break = false;

        let mut previous_space = false;
        let mut previous_break = false;

        let sd = &mut self.scalar_data;
        if value.is_empty() {
            sd.multiline = false;
            sd.flow_plain_allowed = false;
            sd.block_plain_allowed = true;
            sd.single_quoted_allowed = true;
            sd.block_allowed = false;
            return;
        }

        if value.len() >= 3 && (value.starts_with(b"---") || value.starts_with(b"...")) {
            block_indicators = true;
            flow_indicators = true;
        }

        let mut preceded_by_whitespace = true;
        let mut i = 0;
        while i < value.len() {
            let w = width(value[i]).max(1);
            let followed_by_whitespace = i + w >= value.len() || is_blank(value, i + w);

            if i == 0 {
                match value[i] {
                    b'#' | b',' | b'[' | b']' | b'{' | b'}' | b'&' | b'*' | b'!' | b'|' | b'>'
                    | b'\'' | b'"' | b'%' | b'@' | b'`' => {
                        flow_indicators = true;
                        block_indicators = true;
                    }
                    b'?' | b':' => {
                        flow_indicators = true;
                        if followed_by_whitespace {
                            block_indicators = true;
                        }
                    }
                    b'-' if followed_by_whitespace => {
                        flow_indicators = true;
                        block_indicators = true;
                    }
                    _ => {}
                }
            } else {
                match value[i] {
                    b',' | b'?' | b'[' | b']' | b'{' | b'}' => flow_indicators = true,
                    b':' => {
                        flow_indicators = true;
                        if followed_by_whitespace {
                            block_indicators = true;
                        }
                    }
                    b'#' if preceded_by_whitespace => {
                        flow_indicators = true;
                        block_indicators = true;
                    }
                    _ => {}
                }
            }

            if value[i] == b'\t' {
                tab_characters = true;
            } else if !is_printable(value, i) || !is_ascii(value, i) && !self.unicode {
                special_characters = true;
            }
            if is_space(value, i) {
                if i == 0 {
                    leading_space = true;
                }
                if i + w == value.len() {
                    trailing_space = true;
                }
                if previous_break {
                    break_space = true;
                }
                previous_space = true;
                previous_break = false;
            } else if is_break(value, i) {
                line_breaks = true;
                if i == 0 {
                    leading_break = true;
                }
                if i + w == value.len() {
                    trailing_break = true;
                }
                if previous_space {
                    space_break = true;
                }
                previous_space = false;
                previous_break = true;
            } else {
                previous_space = false;
                previous_break = false;
            }

            // [Go]: Why 'z'? Couldn't be the end of the string as that's the loop condition.
            preceded_by_whitespace = is_blankz(value, i);
            i += w;
        }

        sd.multiline = line_breaks;
        sd.flow_plain_allowed = true;
        sd.block_plain_allowed = true;
        sd.single_quoted_allowed = true;
        sd.block_allowed = true;

        if leading_space || leading_break || trailing_space || trailing_break {
            sd.flow_plain_allowed = false;
            sd.block_plain_allowed = false;
        }
        if trailing_space {
            sd.block_allowed = false;
        }
        if break_space {
            sd.flow_plain_allowed = false;
            sd.block_plain_allowed = false;
            sd.single_quoted_allowed = false;
        }
        if space_break || tab_characters || special_characters {
            sd.flow_plain_allowed = false;
            sd.block_plain_allowed = false;
            sd.single_quoted_allowed = false;
        }
        if space_break || special_characters {
            sd.block_allowed = false;
        }
        if line_breaks {
            sd.flow_plain_allowed = false;
            sd.block_plain_allowed = false;
        }
        if flow_indicators {
            sd.flow_plain_allowed = false;
        }
        if block_indicators {
            sd.block_plain_allowed = false;
        }
    }

    fn write_indent(&mut self) {
        let indent = self.indent.max(0);
        if !self.indention || self.column > indent || (self.column == indent && !self.whitespace) {
            self.put_break();
        }
        if self.foot_indent == indent {
            self.put_break();
        }
        while self.column < indent {
            self.put(b' ');
        }
        self.whitespace = true;
        self.foot_indent = -1;
    }

    fn write_indicator(
        &mut self,
        indicator: &[u8],
        need_whitespace: bool,
        is_whitespace: bool,
        is_indention: bool,
    ) -> Result<(), String> {
        if need_whitespace && !self.whitespace {
            self.put(b' ');
        }
        self.write_all(indicator)?;
        self.whitespace = is_whitespace;
        self.indention = self.indention && is_indention;
        self.open_ended = false;
        Ok(())
    }

    fn write_plain_scalar(&mut self, value: &[u8], allow_breaks: bool) -> Result<(), String> {
        if !value.is_empty() && !self.whitespace {
            self.put(b' ');
        }
        let mut spaces = false;
        let mut breaks = false;
        let mut i = 0;
        while i < value.len() {
            if is_space(value, i) {
                if allow_breaks
                    && !spaces
                    && self.column > self.best_width
                    && !is_space(value, i + 1)
                {
                    self.write_indent();
                    i += width(value[i]);
                } else {
                    self.write(value, &mut i)?;
                }
                spaces = true;
            } else if is_break(value, i) {
                if !breaks && value[i] == b'\n' {
                    self.put_break();
                }
                self.write_break(value, &mut i)?;
                breaks = true;
            } else {
                if breaks {
                    self.write_indent();
                }
                self.write(value, &mut i)?;
                self.indention = false;
                spaces = false;
                breaks = false;
            }
        }
        if !value.is_empty() {
            self.whitespace = false;
        }
        self.indention = false;
        if self.root_context {
            self.open_ended = true;
        }
        Ok(())
    }

    fn write_single_quoted_scalar(
        &mut self,
        value: &[u8],
        allow_breaks: bool,
    ) -> Result<(), String> {
        self.write_indicator(b"'", true, false, false)?;
        let mut spaces = false;
        let mut breaks = false;
        let mut i = 0;
        while i < value.len() {
            if is_space(value, i) {
                if allow_breaks
                    && !spaces
                    && self.column > self.best_width
                    && i > 0
                    && i < value.len() - 1
                    && !is_space(value, i + 1)
                {
                    self.write_indent();
                    i += width(value[i]);
                } else {
                    self.write(value, &mut i)?;
                }
                spaces = true;
            } else if is_break(value, i) {
                if !breaks && value[i] == b'\n' {
                    self.put_break();
                }
                self.write_break(value, &mut i)?;
                breaks = true;
            } else {
                if breaks {
                    self.write_indent();
                }
                if value[i] == b'\'' {
                    self.put(b'\'');
                }
                self.write(value, &mut i)?;
                self.indention = false;
                spaces = false;
                breaks = false;
            }
        }
        self.write_indicator(b"'", false, false, false)?;
        self.whitespace = false;
        self.indention = false;
        Ok(())
    }

    fn write_double_quoted_scalar(
        &mut self,
        value: &[u8],
        allow_breaks: bool,
    ) -> Result<(), String> {
        let mut spaces = false;
        self.write_indicator(b"\"", true, false, false)?;
        let mut i = 0;
        while i < value.len() {
            if !is_printable(value, i)
                || (!self.unicode && !is_ascii(value, i))
                || is_bom(value, i)
                || is_break(value, i)
                || value[i] == b'"'
                || value[i] == b'\\'
            {
                let octet = value[i];
                let (w, mut v): (usize, u32) = if octet & 0x80 == 0x00 {
                    (1, u32::from(octet & 0x7F))
                } else if octet & 0xE0 == 0xC0 {
                    (2, u32::from(octet & 0x1F))
                } else if octet & 0xF0 == 0xE0 {
                    (3, u32::from(octet & 0x0F))
                } else if octet & 0xF8 == 0xF0 {
                    (4, u32::from(octet & 0x07))
                } else {
                    return Err(INTERNAL_ERROR.to_string());
                };
                for k in 1..w {
                    v = (v << 6) + (u32::from(at(value, i + k)) & 0x3F);
                }
                i += w;

                self.put(b'\\');
                match v {
                    0x00 => self.put(b'0'),
                    0x07 => self.put(b'a'),
                    0x08 => self.put(b'b'),
                    0x09 => self.put(b't'),
                    0x0A => self.put(b'n'),
                    0x0B => self.put(b'v'),
                    0x0C => self.put(b'f'),
                    0x0D => self.put(b'r'),
                    0x1B => self.put(b'e'),
                    0x22 => self.put(b'"'),
                    0x5C => self.put(b'\\'),
                    0x85 => self.put(b'N'),
                    0xA0 => self.put(b'_'),
                    0x2028 => self.put(b'L'),
                    0x2029 => self.put(b'P'),
                    _ => {
                        let digits = if v <= 0xFF {
                            self.put(b'x');
                            2
                        } else if v <= 0xFFFF {
                            self.put(b'u');
                            4
                        } else {
                            self.put(b'U');
                            8
                        };
                        for k in (0..digits).rev() {
                            let digit = ((v >> (4 * k)) & 0x0F) as u8;
                            self.put(if digit < 10 {
                                digit + b'0'
                            } else {
                                digit + b'A' - 10
                            });
                        }
                    }
                }
                spaces = false;
            } else if is_space(value, i) {
                if allow_breaks
                    && !spaces
                    && self.column > self.best_width
                    && i > 0
                    && i < value.len() - 1
                {
                    self.write_indent();
                    if is_space(value, i + 1) {
                        self.put(b'\\');
                    }
                    i += width(value[i]);
                } else {
                    self.write(value, &mut i)?;
                }
                spaces = true;
            } else {
                self.write(value, &mut i)?;
                spaces = false;
            }
        }
        self.write_indicator(b"\"", false, false, false)?;
        self.whitespace = false;
        self.indention = false;
        Ok(())
    }

    fn write_block_scalar_hints(&mut self, value: &[u8]) -> Result<(), String> {
        if is_space(value, 0) || is_break(value, 0) {
            let indent_hint = [b'0' + self.best_indent as u8];
            self.write_indicator(&indent_hint, false, false, false)?;
        }
        self.open_ended = false;

        let mut chomp_hint = None;
        if value.is_empty() {
            chomp_hint = Some(b'-');
        } else {
            let mut i = value.len() - 1;
            while i > 0 && value[i] & 0xC0 == 0x80 {
                i -= 1;
            }
            if !is_break(value, i) {
                chomp_hint = Some(b'-');
            } else if i == 0 {
                chomp_hint = Some(b'+');
                self.open_ended = true;
            } else {
                i -= 1;
                while i > 0 && value[i] & 0xC0 == 0x80 {
                    i -= 1;
                }
                if is_break(value, i) {
                    chomp_hint = Some(b'+');
                    self.open_ended = true;
                }
            }
        }
        if let Some(hint) = chomp_hint {
            self.write_indicator(&[hint], false, false, false)?;
        }
        Ok(())
    }

    fn write_literal_scalar(&mut self, value: &[u8]) -> Result<(), String> {
        self.write_indicator(b"|", true, false, false)?;
        self.write_block_scalar_hints(value)?;
        // yaml_emitter_process_line_comment_linebreak(emitter, true) without a comment.
        self.put_break();
        self.whitespace = true;
        let mut breaks = true;
        let mut i = 0;
        while i < value.len() {
            if is_break(value, i) {
                self.write_break(value, &mut i)?;
                breaks = true;
            } else {
                if breaks {
                    self.write_indent();
                }
                self.write(value, &mut i)?;
                self.indention = false;
                breaks = false;
            }
        }
        Ok(())
    }
}

// Go unicode.L / unicode.Nd range tables (Unicode 17.0.0), generated from the Go toolchain
// used to build .ref/mimir.
#[rustfmt::skip]
const UNICODE_L: &[(u32, u32, u32)] = &[
    (0x41, 0x5a, 1), (0x61, 0x7a, 1), (0xaa, 0xb5, 11), (0xba, 0xc0, 6), (0xc1, 0xd6, 1),
    (0xd8, 0xf6, 1), (0xf8, 0x2c1, 1), (0x2c6, 0x2d1, 1), (0x2e0, 0x2e4, 1), (0x2ec, 0x2ee, 2),
    (0x370, 0x374, 1), (0x376, 0x377, 1), (0x37a, 0x37d, 1), (0x37f, 0x386, 7), (0x388, 0x38a, 1),
    (0x38c, 0x38e, 2), (0x38f, 0x3a1, 1), (0x3a3, 0x3f5, 1), (0x3f7, 0x481, 1), (0x48a, 0x52f, 1),
    (0x531, 0x556, 1), (0x559, 0x560, 7), (0x561, 0x588, 1), (0x5d0, 0x5ea, 1), (0x5ef, 0x5f2, 1),
    (0x620, 0x64a, 1), (0x66e, 0x66f, 1), (0x671, 0x6d3, 1), (0x6d5, 0x6e5, 16), (0x6e6, 0x6ee, 8),
    (0x6ef, 0x6fa, 11), (0x6fb, 0x6fc, 1), (0x6ff, 0x710, 17), (0x712, 0x72f, 1), (0x74d, 0x7a5, 1),
    (0x7b1, 0x7ca, 25), (0x7cb, 0x7ea, 1), (0x7f4, 0x7f5, 1), (0x7fa, 0x800, 6), (0x801, 0x815, 1),
    (0x81a, 0x824, 10), (0x828, 0x840, 24), (0x841, 0x858, 1), (0x860, 0x86a, 1), (0x870, 0x887, 1),
    (0x889, 0x88f, 1), (0x8a0, 0x8c9, 1), (0x904, 0x939, 1), (0x93d, 0x950, 19), (0x958, 0x961, 1),
    (0x971, 0x980, 1), (0x985, 0x98c, 1), (0x98f, 0x990, 1), (0x993, 0x9a8, 1), (0x9aa, 0x9b0, 1),
    (0x9b2, 0x9b6, 4), (0x9b7, 0x9b9, 1), (0x9bd, 0x9ce, 17), (0x9dc, 0x9dd, 1), (0x9df, 0x9e1, 1),
    (0x9f0, 0x9f1, 1), (0x9fc, 0xa05, 9), (0xa06, 0xa0a, 1), (0xa0f, 0xa10, 1), (0xa13, 0xa28, 1),
    (0xa2a, 0xa30, 1), (0xa32, 0xa33, 1), (0xa35, 0xa36, 1), (0xa38, 0xa39, 1), (0xa59, 0xa5c, 1),
    (0xa5e, 0xa72, 20), (0xa73, 0xa74, 1), (0xa85, 0xa8d, 1), (0xa8f, 0xa91, 1), (0xa93, 0xaa8, 1),
    (0xaaa, 0xab0, 1), (0xab2, 0xab3, 1), (0xab5, 0xab9, 1), (0xabd, 0xad0, 19), (0xae0, 0xae1, 1),
    (0xaf9, 0xb05, 12), (0xb06, 0xb0c, 1), (0xb0f, 0xb10, 1), (0xb13, 0xb28, 1), (0xb2a, 0xb30, 1),
    (0xb32, 0xb33, 1), (0xb35, 0xb39, 1), (0xb3d, 0xb5c, 31), (0xb5d, 0xb5f, 2), (0xb60, 0xb61, 1),
    (0xb71, 0xb83, 18), (0xb85, 0xb8a, 1), (0xb8e, 0xb90, 1), (0xb92, 0xb95, 1), (0xb99, 0xb9a, 1),
    (0xb9c, 0xb9e, 2), (0xb9f, 0xba3, 4), (0xba4, 0xba8, 4), (0xba9, 0xbaa, 1), (0xbae, 0xbb9, 1),
    (0xbd0, 0xc05, 53), (0xc06, 0xc0c, 1), (0xc0e, 0xc10, 1), (0xc12, 0xc28, 1), (0xc2a, 0xc39, 1),
    (0xc3d, 0xc58, 27), (0xc59, 0xc5a, 1), (0xc5c, 0xc5d, 1), (0xc60, 0xc61, 1), (0xc80, 0xc85, 5),
    (0xc86, 0xc8c, 1), (0xc8e, 0xc90, 1), (0xc92, 0xca8, 1), (0xcaa, 0xcb3, 1), (0xcb5, 0xcb9, 1),
    (0xcbd, 0xcdc, 31), (0xcdd, 0xcde, 1), (0xce0, 0xce1, 1), (0xcf1, 0xcf2, 1), (0xd04, 0xd0c, 1),
    (0xd0e, 0xd10, 1), (0xd12, 0xd3a, 1), (0xd3d, 0xd4e, 17), (0xd54, 0xd56, 1), (0xd5f, 0xd61, 1),
    (0xd7a, 0xd7f, 1), (0xd85, 0xd96, 1), (0xd9a, 0xdb1, 1), (0xdb3, 0xdbb, 1), (0xdbd, 0xdc0, 3),
    (0xdc1, 0xdc6, 1), (0xe01, 0xe30, 1), (0xe32, 0xe33, 1), (0xe40, 0xe46, 1), (0xe81, 0xe82, 1),
    (0xe84, 0xe86, 2), (0xe87, 0xe8a, 1), (0xe8c, 0xea3, 1), (0xea5, 0xea7, 2), (0xea8, 0xeb0, 1),
    (0xeb2, 0xeb3, 1), (0xebd, 0xec0, 3), (0xec1, 0xec4, 1), (0xec6, 0xedc, 22), (0xedd, 0xedf, 1),
    (0xf00, 0xf40, 64), (0xf41, 0xf47, 1), (0xf49, 0xf6c, 1), (0xf88, 0xf8c, 1), (0x1000, 0x102a, 1),
    (0x103f, 0x1050, 17), (0x1051, 0x1055, 1), (0x105a, 0x105d, 1), (0x1061, 0x1065, 4), (0x1066, 0x106e, 8),
    (0x106f, 0x1070, 1), (0x1075, 0x1081, 1), (0x108e, 0x10a0, 18), (0x10a1, 0x10c5, 1), (0x10c7, 0x10cd, 6),
    (0x10d0, 0x10fa, 1), (0x10fc, 0x1248, 1), (0x124a, 0x124d, 1), (0x1250, 0x1256, 1), (0x1258, 0x125a, 2),
    (0x125b, 0x125d, 1), (0x1260, 0x1288, 1), (0x128a, 0x128d, 1), (0x1290, 0x12b0, 1), (0x12b2, 0x12b5, 1),
    (0x12b8, 0x12be, 1), (0x12c0, 0x12c2, 2), (0x12c3, 0x12c5, 1), (0x12c8, 0x12d6, 1), (0x12d8, 0x1310, 1),
    (0x1312, 0x1315, 1), (0x1318, 0x135a, 1), (0x1380, 0x138f, 1), (0x13a0, 0x13f5, 1), (0x13f8, 0x13fd, 1),
    (0x1401, 0x166c, 1), (0x166f, 0x167f, 1), (0x1681, 0x169a, 1), (0x16a0, 0x16ea, 1), (0x16f1, 0x16f8, 1),
    (0x1700, 0x1711, 1), (0x171f, 0x1731, 1), (0x1740, 0x1751, 1), (0x1760, 0x176c, 1), (0x176e, 0x1770, 1),
    (0x1780, 0x17b3, 1), (0x17d7, 0x17dc, 5), (0x1820, 0x1878, 1), (0x1880, 0x1884, 1), (0x1887, 0x18a8, 1),
    (0x18aa, 0x18b0, 6), (0x18b1, 0x18f5, 1), (0x1900, 0x191e, 1), (0x1950, 0x196d, 1), (0x1970, 0x1974, 1),
    (0x1980, 0x19ab, 1), (0x19b0, 0x19c9, 1), (0x1a00, 0x1a16, 1), (0x1a20, 0x1a54, 1), (0x1aa7, 0x1b05, 94),
    (0x1b06, 0x1b33, 1), (0x1b45, 0x1b4c, 1), (0x1b83, 0x1ba0, 1), (0x1bae, 0x1baf, 1), (0x1bba, 0x1be5, 1),
    (0x1c00, 0x1c23, 1), (0x1c4d, 0x1c4f, 1), (0x1c5a, 0x1c7d, 1), (0x1c80, 0x1c8a, 1), (0x1c90, 0x1cba, 1),
    (0x1cbd, 0x1cbf, 1), (0x1ce9, 0x1cec, 1), (0x1cee, 0x1cf3, 1), (0x1cf5, 0x1cf6, 1), (0x1cfa, 0x1d00, 6),
    (0x1d01, 0x1dbf, 1), (0x1e00, 0x1f15, 1), (0x1f18, 0x1f1d, 1), (0x1f20, 0x1f45, 1), (0x1f48, 0x1f4d, 1),
    (0x1f50, 0x1f57, 1), (0x1f59, 0x1f5f, 2), (0x1f60, 0x1f7d, 1), (0x1f80, 0x1fb4, 1), (0x1fb6, 0x1fbc, 1),
    (0x1fbe, 0x1fc2, 4), (0x1fc3, 0x1fc4, 1), (0x1fc6, 0x1fcc, 1), (0x1fd0, 0x1fd3, 1), (0x1fd6, 0x1fdb, 1),
    (0x1fe0, 0x1fec, 1), (0x1ff2, 0x1ff4, 1), (0x1ff6, 0x1ffc, 1), (0x2071, 0x207f, 14), (0x2090, 0x209c, 1),
    (0x2102, 0x2107, 5), (0x210a, 0x2113, 1), (0x2115, 0x2119, 4), (0x211a, 0x211d, 1), (0x2124, 0x212a, 2),
    (0x212b, 0x212d, 1), (0x212f, 0x2139, 1), (0x213c, 0x213f, 1), (0x2145, 0x2149, 1), (0x214e, 0x2183, 53),
    (0x2184, 0x2c00, 2684), (0x2c01, 0x2ce4, 1), (0x2ceb, 0x2cee, 1), (0x2cf2, 0x2cf3, 1), (0x2d00, 0x2d25, 1),
    (0x2d27, 0x2d2d, 6), (0x2d30, 0x2d67, 1), (0x2d6f, 0x2d80, 17), (0x2d81, 0x2d96, 1), (0x2da0, 0x2da6, 1),
    (0x2da8, 0x2dae, 1), (0x2db0, 0x2db6, 1), (0x2db8, 0x2dbe, 1), (0x2dc0, 0x2dc6, 1), (0x2dc8, 0x2dce, 1),
    (0x2dd0, 0x2dd6, 1), (0x2dd8, 0x2dde, 1), (0x2e2f, 0x3005, 470), (0x3006, 0x3031, 43), (0x3032, 0x3035, 1),
    (0x303b, 0x303c, 1), (0x3041, 0x3096, 1), (0x309d, 0x309f, 1), (0x30a1, 0x30fa, 1), (0x30fc, 0x30ff, 1),
    (0x3105, 0x312f, 1), (0x3131, 0x318e, 1), (0x31a0, 0x31bf, 1), (0x31f0, 0x31ff, 1), (0x3400, 0x4dbf, 1),
    (0x4e00, 0xa48c, 1), (0xa4d0, 0xa4fd, 1), (0xa500, 0xa60c, 1), (0xa610, 0xa61f, 1), (0xa62a, 0xa62b, 1),
    (0xa640, 0xa66e, 1), (0xa67f, 0xa69d, 1), (0xa6a0, 0xa6e5, 1), (0xa717, 0xa71f, 1), (0xa722, 0xa788, 1),
    (0xa78b, 0xa7dc, 1), (0xa7f1, 0xa801, 1), (0xa803, 0xa805, 1), (0xa807, 0xa80a, 1), (0xa80c, 0xa822, 1),
    (0xa840, 0xa873, 1), (0xa882, 0xa8b3, 1), (0xa8f2, 0xa8f7, 1), (0xa8fb, 0xa8fd, 2), (0xa8fe, 0xa90a, 12),
    (0xa90b, 0xa925, 1), (0xa930, 0xa946, 1), (0xa960, 0xa97c, 1), (0xa984, 0xa9b2, 1), (0xa9cf, 0xa9e0, 17),
    (0xa9e1, 0xa9e4, 1), (0xa9e6, 0xa9ef, 1), (0xa9fa, 0xa9fe, 1), (0xaa00, 0xaa28, 1), (0xaa40, 0xaa42, 1),
    (0xaa44, 0xaa4b, 1), (0xaa60, 0xaa76, 1), (0xaa7a, 0xaa7e, 4), (0xaa7f, 0xaaaf, 1), (0xaab1, 0xaab5, 4),
    (0xaab6, 0xaab9, 3), (0xaaba, 0xaabd, 1), (0xaac0, 0xaac2, 2), (0xaadb, 0xaadd, 1), (0xaae0, 0xaaea, 1),
    (0xaaf2, 0xaaf4, 1), (0xab01, 0xab06, 1), (0xab09, 0xab0e, 1), (0xab11, 0xab16, 1), (0xab20, 0xab26, 1),
    (0xab28, 0xab2e, 1), (0xab30, 0xab5a, 1), (0xab5c, 0xab69, 1), (0xab70, 0xabe2, 1), (0xac00, 0xd7a3, 1),
    (0xd7b0, 0xd7c6, 1), (0xd7cb, 0xd7fb, 1), (0xf900, 0xfa6d, 1), (0xfa70, 0xfad9, 1), (0xfb00, 0xfb06, 1),
    (0xfb13, 0xfb17, 1), (0xfb1d, 0xfb1f, 2), (0xfb20, 0xfb28, 1), (0xfb2a, 0xfb36, 1), (0xfb38, 0xfb3c, 1),
    (0xfb3e, 0xfb40, 2), (0xfb41, 0xfb43, 2), (0xfb44, 0xfb46, 2), (0xfb47, 0xfbb1, 1), (0xfbd3, 0xfd3d, 1),
    (0xfd50, 0xfd8f, 1), (0xfd92, 0xfdc7, 1), (0xfdf0, 0xfdfb, 1), (0xfe70, 0xfe74, 1), (0xfe76, 0xfefc, 1),
    (0xff21, 0xff3a, 1), (0xff41, 0xff5a, 1), (0xff66, 0xffbe, 1), (0xffc2, 0xffc7, 1), (0xffca, 0xffcf, 1),
    (0xffd2, 0xffd7, 1), (0xffda, 0xffdc, 1), (0x10000, 0x1000b, 1), (0x1000d, 0x10026, 1), (0x10028, 0x1003a, 1),
    (0x1003c, 0x1003d, 1), (0x1003f, 0x1004d, 1), (0x10050, 0x1005d, 1), (0x10080, 0x100fa, 1), (0x10280, 0x1029c, 1),
    (0x102a0, 0x102d0, 1), (0x10300, 0x1031f, 1), (0x1032d, 0x10340, 1), (0x10342, 0x10349, 1), (0x10350, 0x10375, 1),
    (0x10380, 0x1039d, 1), (0x103a0, 0x103c3, 1), (0x103c8, 0x103cf, 1), (0x10400, 0x1049d, 1), (0x104b0, 0x104d3, 1),
    (0x104d8, 0x104fb, 1), (0x10500, 0x10527, 1), (0x10530, 0x10563, 1), (0x10570, 0x1057a, 1), (0x1057c, 0x1058a, 1),
    (0x1058c, 0x10592, 1), (0x10594, 0x10595, 1), (0x10597, 0x105a1, 1), (0x105a3, 0x105b1, 1), (0x105b3, 0x105b9, 1),
    (0x105bb, 0x105bc, 1), (0x105c0, 0x105f3, 1), (0x10600, 0x10736, 1), (0x10740, 0x10755, 1), (0x10760, 0x10767, 1),
    (0x10780, 0x10785, 1), (0x10787, 0x107b0, 1), (0x107b2, 0x107ba, 1), (0x10800, 0x10805, 1), (0x10808, 0x1080a, 2),
    (0x1080b, 0x10835, 1), (0x10837, 0x10838, 1), (0x1083c, 0x1083f, 3), (0x10840, 0x10855, 1), (0x10860, 0x10876, 1),
    (0x10880, 0x1089e, 1), (0x108e0, 0x108f2, 1), (0x108f4, 0x108f5, 1), (0x10900, 0x10915, 1), (0x10920, 0x10939, 1),
    (0x10940, 0x10959, 1), (0x10980, 0x109b7, 1), (0x109be, 0x109bf, 1), (0x10a00, 0x10a10, 16), (0x10a11, 0x10a13, 1),
    (0x10a15, 0x10a17, 1), (0x10a19, 0x10a35, 1), (0x10a60, 0x10a7c, 1), (0x10a80, 0x10a9c, 1), (0x10ac0, 0x10ac7, 1),
    (0x10ac9, 0x10ae4, 1), (0x10b00, 0x10b35, 1), (0x10b40, 0x10b55, 1), (0x10b60, 0x10b72, 1), (0x10b80, 0x10b91, 1),
    (0x10c00, 0x10c48, 1), (0x10c80, 0x10cb2, 1), (0x10cc0, 0x10cf2, 1), (0x10d00, 0x10d23, 1), (0x10d4a, 0x10d65, 1),
    (0x10d6f, 0x10d85, 1), (0x10e80, 0x10ea9, 1), (0x10eb0, 0x10eb1, 1), (0x10ec2, 0x10ec7, 1), (0x10f00, 0x10f1c, 1),
    (0x10f27, 0x10f30, 9), (0x10f31, 0x10f45, 1), (0x10f70, 0x10f81, 1), (0x10fb0, 0x10fc4, 1), (0x10fe0, 0x10ff6, 1),
    (0x11003, 0x11037, 1), (0x11071, 0x11072, 1), (0x11075, 0x11083, 14), (0x11084, 0x110af, 1), (0x110d0, 0x110e8, 1),
    (0x11103, 0x11126, 1), (0x11144, 0x11147, 3), (0x11150, 0x11172, 1), (0x11176, 0x11183, 13), (0x11184, 0x111b2, 1),
    (0x111c1, 0x111c4, 1), (0x111da, 0x111dc, 2), (0x11200, 0x11211, 1), (0x11213, 0x1122b, 1), (0x1123f, 0x11240, 1),
    (0x11280, 0x11286, 1), (0x11288, 0x1128a, 2), (0x1128b, 0x1128d, 1), (0x1128f, 0x1129d, 1), (0x1129f, 0x112a8, 1),
    (0x112b0, 0x112de, 1), (0x11305, 0x1130c, 1), (0x1130f, 0x11310, 1), (0x11313, 0x11328, 1), (0x1132a, 0x11330, 1),
    (0x11332, 0x11333, 1), (0x11335, 0x11339, 1), (0x1133d, 0x11350, 19), (0x1135d, 0x11361, 1), (0x11380, 0x11389, 1),
    (0x1138b, 0x1138e, 3), (0x11390, 0x113b5, 1), (0x113b7, 0x113d1, 26), (0x113d3, 0x11400, 45), (0x11401, 0x11434, 1),
    (0x11447, 0x1144a, 1), (0x1145f, 0x11461, 1), (0x11480, 0x114af, 1), (0x114c4, 0x114c5, 1), (0x114c7, 0x11580, 185),
    (0x11581, 0x115ae, 1), (0x115d8, 0x115db, 1), (0x11600, 0x1162f, 1), (0x11644, 0x11680, 60), (0x11681, 0x116aa, 1),
    (0x116b8, 0x11700, 72), (0x11701, 0x1171a, 1), (0x11740, 0x11746, 1), (0x11800, 0x1182b, 1), (0x118a0, 0x118df, 1),
    (0x118ff, 0x11906, 1), (0x11909, 0x1190c, 3), (0x1190d, 0x11913, 1), (0x11915, 0x11916, 1), (0x11918, 0x1192f, 1),
    (0x1193f, 0x11941, 2), (0x119a0, 0x119a7, 1), (0x119aa, 0x119d0, 1), (0x119e1, 0x119e3, 2), (0x11a00, 0x11a0b, 11),
    (0x11a0c, 0x11a32, 1), (0x11a3a, 0x11a50, 22), (0x11a5c, 0x11a89, 1), (0x11a9d, 0x11ab0, 19), (0x11ab1, 0x11af8, 1),
    (0x11bc0, 0x11be0, 1), (0x11c00, 0x11c08, 1), (0x11c0a, 0x11c2e, 1), (0x11c40, 0x11c72, 50), (0x11c73, 0x11c8f, 1),
    (0x11d00, 0x11d06, 1), (0x11d08, 0x11d09, 1), (0x11d0b, 0x11d30, 1), (0x11d46, 0x11d60, 26), (0x11d61, 0x11d65, 1),
    (0x11d67, 0x11d68, 1), (0x11d6a, 0x11d89, 1), (0x11d98, 0x11db0, 24), (0x11db1, 0x11ddb, 1), (0x11ee0, 0x11ef2, 1),
    (0x11f02, 0x11f04, 2), (0x11f05, 0x11f10, 1), (0x11f12, 0x11f33, 1), (0x11fb0, 0x12000, 80), (0x12001, 0x12399, 1),
    (0x12480, 0x12543, 1), (0x12f90, 0x12ff0, 1), (0x13000, 0x1342f, 1), (0x13441, 0x13446, 1), (0x13460, 0x143fa, 1),
    (0x14400, 0x14646, 1), (0x16100, 0x1611d, 1), (0x16800, 0x16a38, 1), (0x16a40, 0x16a5e, 1), (0x16a70, 0x16abe, 1),
    (0x16ad0, 0x16aed, 1), (0x16b00, 0x16b2f, 1), (0x16b40, 0x16b43, 1), (0x16b63, 0x16b77, 1), (0x16b7d, 0x16b8f, 1),
    (0x16d40, 0x16d6c, 1), (0x16e40, 0x16e7f, 1), (0x16ea0, 0x16eb8, 1), (0x16ebb, 0x16ed3, 1), (0x16f00, 0x16f4a, 1),
    (0x16f50, 0x16f93, 67), (0x16f94, 0x16f9f, 1), (0x16fe0, 0x16fe1, 1), (0x16fe3, 0x16ff2, 15), (0x16ff3, 0x17000, 13),
    (0x17001, 0x18cd5, 1), (0x18cff, 0x18d1e, 1), (0x18d80, 0x18df2, 1), (0x1aff0, 0x1aff3, 1), (0x1aff5, 0x1affb, 1),
    (0x1affd, 0x1affe, 1), (0x1b000, 0x1b122, 1), (0x1b132, 0x1b150, 30), (0x1b151, 0x1b152, 1), (0x1b155, 0x1b164, 15),
    (0x1b165, 0x1b167, 1), (0x1b170, 0x1b2fb, 1), (0x1bc00, 0x1bc6a, 1), (0x1bc70, 0x1bc7c, 1), (0x1bc80, 0x1bc88, 1),
    (0x1bc90, 0x1bc99, 1), (0x1d400, 0x1d454, 1), (0x1d456, 0x1d49c, 1), (0x1d49e, 0x1d49f, 1), (0x1d4a2, 0x1d4a5, 3),
    (0x1d4a6, 0x1d4a9, 3), (0x1d4aa, 0x1d4ac, 1), (0x1d4ae, 0x1d4b9, 1), (0x1d4bb, 0x1d4bd, 2), (0x1d4be, 0x1d4c3, 1),
    (0x1d4c5, 0x1d505, 1), (0x1d507, 0x1d50a, 1), (0x1d50d, 0x1d514, 1), (0x1d516, 0x1d51c, 1), (0x1d51e, 0x1d539, 1),
    (0x1d53b, 0x1d53e, 1), (0x1d540, 0x1d544, 1), (0x1d546, 0x1d54a, 4), (0x1d54b, 0x1d550, 1), (0x1d552, 0x1d6a5, 1),
    (0x1d6a8, 0x1d6c0, 1), (0x1d6c2, 0x1d6da, 1), (0x1d6dc, 0x1d6fa, 1), (0x1d6fc, 0x1d714, 1), (0x1d716, 0x1d734, 1),
    (0x1d736, 0x1d74e, 1), (0x1d750, 0x1d76e, 1), (0x1d770, 0x1d788, 1), (0x1d78a, 0x1d7a8, 1), (0x1d7aa, 0x1d7c2, 1),
    (0x1d7c4, 0x1d7cb, 1), (0x1df00, 0x1df1e, 1), (0x1df25, 0x1df2a, 1), (0x1e030, 0x1e06d, 1), (0x1e100, 0x1e12c, 1),
    (0x1e137, 0x1e13d, 1), (0x1e14e, 0x1e290, 322), (0x1e291, 0x1e2ad, 1), (0x1e2c0, 0x1e2eb, 1), (0x1e4d0, 0x1e4eb, 1),
    (0x1e5d0, 0x1e5ed, 1), (0x1e5f0, 0x1e6c0, 208), (0x1e6c1, 0x1e6de, 1), (0x1e6e0, 0x1e6e2, 1), (0x1e6e4, 0x1e6e5, 1),
    (0x1e6e7, 0x1e6ed, 1), (0x1e6f0, 0x1e6f4, 1), (0x1e6fe, 0x1e6ff, 1), (0x1e7e0, 0x1e7e6, 1), (0x1e7e8, 0x1e7eb, 1),
    (0x1e7ed, 0x1e7ee, 1), (0x1e7f0, 0x1e7fe, 1), (0x1e800, 0x1e8c4, 1), (0x1e900, 0x1e943, 1), (0x1e94b, 0x1ee00, 1205),
    (0x1ee01, 0x1ee03, 1), (0x1ee05, 0x1ee1f, 1), (0x1ee21, 0x1ee22, 1), (0x1ee24, 0x1ee27, 3), (0x1ee29, 0x1ee32, 1),
    (0x1ee34, 0x1ee37, 1), (0x1ee39, 0x1ee3b, 2), (0x1ee42, 0x1ee47, 5), (0x1ee49, 0x1ee4d, 2), (0x1ee4e, 0x1ee4f, 1),
    (0x1ee51, 0x1ee52, 1), (0x1ee54, 0x1ee57, 3), (0x1ee59, 0x1ee61, 2), (0x1ee62, 0x1ee64, 2), (0x1ee67, 0x1ee6a, 1),
    (0x1ee6c, 0x1ee72, 1), (0x1ee74, 0x1ee77, 1), (0x1ee79, 0x1ee7c, 1), (0x1ee7e, 0x1ee80, 2), (0x1ee81, 0x1ee89, 1),
    (0x1ee8b, 0x1ee9b, 1), (0x1eea1, 0x1eea3, 1), (0x1eea5, 0x1eea9, 1), (0x1eeab, 0x1eebb, 1), (0x20000, 0x2a6df, 1),
    (0x2a700, 0x2b81d, 1), (0x2b820, 0x2cead, 1), (0x2ceb0, 0x2ebe0, 1), (0x2ebf0, 0x2ee5d, 1), (0x2f800, 0x2fa1d, 1),
    (0x30000, 0x3134a, 1), (0x31350, 0x33479, 1),
];
#[rustfmt::skip]
const UNICODE_ND: &[(u32, u32, u32)] = &[
    (0x30, 0x39, 1), (0x660, 0x669, 1), (0x6f0, 0x6f9, 1), (0x7c0, 0x7c9, 1), (0x966, 0x96f, 1),
    (0x9e6, 0x9ef, 1), (0xa66, 0xa6f, 1), (0xae6, 0xaef, 1), (0xb66, 0xb6f, 1), (0xbe6, 0xbef, 1),
    (0xc66, 0xc6f, 1), (0xce6, 0xcef, 1), (0xd66, 0xd6f, 1), (0xde6, 0xdef, 1), (0xe50, 0xe59, 1),
    (0xed0, 0xed9, 1), (0xf20, 0xf29, 1), (0x1040, 0x1049, 1), (0x1090, 0x1099, 1), (0x17e0, 0x17e9, 1),
    (0x1810, 0x1819, 1), (0x1946, 0x194f, 1), (0x19d0, 0x19d9, 1), (0x1a80, 0x1a89, 1), (0x1a90, 0x1a99, 1),
    (0x1b50, 0x1b59, 1), (0x1bb0, 0x1bb9, 1), (0x1c40, 0x1c49, 1), (0x1c50, 0x1c59, 1), (0xa620, 0xa629, 1),
    (0xa8d0, 0xa8d9, 1), (0xa900, 0xa909, 1), (0xa9d0, 0xa9d9, 1), (0xa9f0, 0xa9f9, 1), (0xaa50, 0xaa59, 1),
    (0xabf0, 0xabf9, 1), (0xff10, 0xff19, 1), (0x104a0, 0x104a9, 1), (0x10d30, 0x10d39, 1), (0x10d40, 0x10d49, 1),
    (0x11066, 0x1106f, 1), (0x110f0, 0x110f9, 1), (0x11136, 0x1113f, 1), (0x111d0, 0x111d9, 1), (0x112f0, 0x112f9, 1),
    (0x11450, 0x11459, 1), (0x114d0, 0x114d9, 1), (0x11650, 0x11659, 1), (0x116c0, 0x116c9, 1), (0x116d0, 0x116e3, 1),
    (0x11730, 0x11739, 1), (0x118e0, 0x118e9, 1), (0x11950, 0x11959, 1), (0x11bf0, 0x11bf9, 1), (0x11c50, 0x11c59, 1),
    (0x11d50, 0x11d59, 1), (0x11da0, 0x11da9, 1), (0x11de0, 0x11de9, 1), (0x11f50, 0x11f59, 1), (0x16130, 0x16139, 1),
    (0x16a60, 0x16a69, 1), (0x16ac0, 0x16ac9, 1), (0x16b50, 0x16b59, 1), (0x16d70, 0x16d79, 1), (0x1ccf0, 0x1ccf9, 1),
    (0x1d7ce, 0x1d7ff, 1), (0x1e140, 0x1e149, 1), (0x1e2f0, 0x1e2f9, 1), (0x1e4f0, 0x1e4f9, 1), (0x1e5f1, 0x1e5fa, 1),
    (0x1e950, 0x1e959, 1), (0x1fbf0, 0x1fbf9, 1),
];

#[cfg(test)]
mod tests {
    use super::*;

    fn marshal_str(input: &str) -> String {
        let value: Value = serde_yaml::from_str(input).expect("test input parses");
        marshal_generic(&value).expect("marshal succeeds")
    }

    /// `(input document, go.yaml.in/yaml/v3 output)` pairs. The expected bytes were produced by a
    /// throwaway Go program built against `.ref/mimir/vendor` that decodes each input with
    /// `yaml.Unmarshal(in, &map[string]interface{}{})` and prints `yaml.Marshal` of the result.
    #[rustfmt::skip]
    const GO_CASES: &[(&str, &str)] = &[
    (
r"overrides:
  tenant-a:
    ingestion_rate: 10000
    ingestion_burst_size: 200000
    max_global_series_per_user: 1500000
  tenant-b:
    ingestion_rate: 350000.5
",
r"overrides:
    tenant-a:
        ingestion_burst_size: 200000
        ingestion_rate: 10000
        max_global_series_per_user: 1500000
    tenant-b:
        ingestion_rate: 350000.5
",
    ),
    (
r#"overrides:
  a10: {max_global_series_per_user: 1}
  a9: {max_global_series_per_user: 2}
  a1: {max_global_series_per_user: 3}
  "1": {max_global_series_per_user: 4}
  "01": {max_global_series_per_user: 5}
  "001": {max_global_series_per_user: 6}
  "10": {max_global_series_per_user: 7}
  "2": {max_global_series_per_user: 8}
  b: {}
  B: {}
  _x: {}
  "-x": {}
  a: {}
  aa: {}
  a_1: {}
  a-1: {}
  a.1: {}
  a01: {}
  a001: {}
  a0b: {}
  a00b: {}
  ab1c: {}
  ab10c: {}
  ab9c: {}
"#,
r#"overrides:
    -x: {}
    _x: {}
    "1":
        max_global_series_per_user: 4
    "01":
        max_global_series_per_user: 5
    "001":
        max_global_series_per_user: 6
    "2":
        max_global_series_per_user: 8
    "10":
        max_global_series_per_user: 7
    B: {}
    a: {}
    a-1: {}
    a.1: {}
    a_1: {}
    a0b: {}
    a00b: {}
    a1:
        max_global_series_per_user: 3
    a01: {}
    a001: {}
    a9:
        max_global_series_per_user: 2
    a10:
        max_global_series_per_user: 1
    aa: {}
    ab1c: {}
    ab9c: {}
    ab10c: {}
    b: {}
"#,
    ),
    (
r#"overrides:
  user1: {}
  user10: {}
  user2: {}
  user02: {}
  user002: {}
  user20a: {}
  user20: {}
  user2a: {}
  User1: {}
  "user 1": {}
  "user.1": {}
"#,
r"overrides:
    User1: {}
    user 1: {}
    user.1: {}
    user1: {}
    user2: {}
    user2a: {}
    user02: {}
    user002: {}
    user10: {}
    user20: {}
    user20a: {}
",
    ),
    (
r"overrides:
  t:
    ingestion_rate: 1e3
    a_hex: 0x10
    a_oct: 0o17
    a_bin: 0b101
    a_neg: -42
    a_pos: +7
    a_float: 1.5
    a_float2: 0.000012
    a_float3: 1234567.0
    a_float4: 123456.0
    a_float5: 1e21
    a_float6: 1.0e-7
    a_float7: 3.0
    a_float8: -0.0
    a_float9: 0.1
    a_inf: .inf
    a_ninf: -.inf
    a_nan: .nan
    a_big: 9223372036854775807
    a_bigger: 9223372036854775808
    a_max_u: 18446744073709551615
    a_min: -9223372036854775808
    a_dot5: .5
    a_5dot: 5.
",
r"overrides:
    t:
        a_5dot: 5
        a_big: 9223372036854775807
        a_bigger: 9223372036854775808
        a_bin: 5
        a_dot5: 0.5
        a_float: 1.5
        a_float2: 1.2e-05
        a_float3: 1.234567e+06
        a_float4: 123456
        a_float5: 1e+21
        a_float6: 1e-07
        a_float7: 3
        a_float8: -0
        a_float9: 0.1
        a_hex: 16
        a_inf: .inf
        a_max_u: 18446744073709551615
        a_min: -9223372036854775808
        a_nan: .nan
        a_neg: -42
        a_ninf: -.inf
        a_oct: 15
        a_pos: 7
        ingestion_rate: 1000
",
    ),
    (
r"overrides:
  t:
    dur1: 1h
    dur2: 30s
    dur3: 1h30m
    dur4: 0s
    dur5: 2d
    b1: true
    b2: false
    b3: True
    b4: FALSE
    n1: null
    n2: ~
    n3:
    n4: Null
",
r"overrides:
    t:
        b1: true
        b2: false
        b3: true
        b4: false
        dur1: 1h
        dur2: 30s
        dur3: 1h30m
        dur4: 0s
        dur5: 2d
        n1: null
        n2: null
        n3: null
        n4: null
",
    ),
    (
r"overrides:
  t:
    s_empty: ''
    s_true: 'true'
    s_false: 'False'
    s_yes: 'yes'
    s_no: 'no'
    s_on: 'on'
    s_off: 'OFF'
    s_y: 'y'
    s_n: 'N'
    s_null: 'null'
    s_tilde: '~'
    s_float: '1.5'
    s_int: '42'
    s_oct: '0o17'
    s_hex: '0x1F'
    s_bin: '0b11'
    s_exp: '1e3'
    s_inf: '.inf'
    s_nan: '.NaN'
    s_merge: '<<'
    s_neg: '-1'
    s_plus: '+1'
    s_lead0: '0777'
    s_us: '1_000'
    s_base60: '1:20'
    s_base60b: '190:20:30.15'
    s_time: '12:30'
    s_date: '2001-12-14'
    s_ts: '2001-12-14t21:59:43.10-05:00'
",
r#"overrides:
    t:
        s_base60: "1:20"
        s_base60b: "190:20:30.15"
        s_bin: "0b11"
        s_date: "2001-12-14"
        s_empty: ""
        s_exp: "1e3"
        s_false: "False"
        s_float: "1.5"
        s_hex: "0x1F"
        s_inf: ".inf"
        s_int: "42"
        s_lead0: "0777"
        s_merge: <<
        s_n: "N"
        s_nan: ".NaN"
        s_neg: "-1"
        s_no: "no"
        s_null: "null"
        s_oct: "0o17"
        s_off: "OFF"
        s_on: "on"
        s_plus: "+1"
        s_tilde: "~"
        s_time: "12:30"
        s_true: "true"
        s_ts: "2001-12-14t21:59:43.10-05:00"
        s_us: "1_000"
        s_y: "y"
        s_yes: "yes"
"#,
    ),
    (
r#"overrides:
  t:
    s1: ' leading space'
    s2: 'trailing '
    s3: 'a: b'
    s4: '# x'
    s5: '- x'
    s6: '-x'
    s7: '{__name__=~"foo.*"}'
    s8: "it's"
    s9: 'a #b'
    s10: 'a#b'
    s11: ':x'
    s12: 'x:'
    s13: '? x'
    s14: '?x'
    s15: '[a]'
    s16: 'a,b'
    s17: '&a'
    s18: '*a'
    s19: '!a'
    s20: '|a'
    s21: '>a'
    s22: '%a'
    s23: '@a'
    s24: '`a'
    s25: '"quoted"'
    s26: "'single'"
    s27: '---'
    s28: '...'
    s29: '--- x'
    s30: 'a\b'
    s31: '  '
    s32: ' '
    s33: '-'
    s34: ':'
    s35: '?'
    s36: 'a:b'
    s37: 'a , b'
    s38: 'a{b}'
"#,
r#"overrides:
    t:
        s1: ' leading space'
        s2: 'trailing '
        s3: 'a: b'
        s4: '# x'
        s5: '- x'
        s6: -x
        s7: '{__name__=~"foo.*"}'
        s8: it's
        s9: 'a #b'
        s10: a#b
        s11: :x
        s12: 'x:'
        s13: '? x'
        s14: ?x
        s15: '[a]'
        s16: a,b
        s17: '&a'
        s18: '*a'
        s19: '!a'
        s20: '|a'
        s21: '>a'
        s22: '%a'
        s23: '@a'
        s24: '`a'
        s25: '"quoted"'
        s26: '''single'''
        s27: '---'
        s28: '...'
        s29: '--- x'
        s30: a\b
        s31: '  '
        s32: ' '
        s33: '-'
        s34: ':'
        s35: '?'
        s36: a:b
        s37: a , b
        s38: a{b}
"#,
    ),
    (
"overrides:\n  t:\n    m1: \"line1\\nline2\\n\"\n    m2: \"line1\\nline2\"\n    m3: \"line1\\nline2\\n\\n\"\n    m4: \"\\nleading newline\"\n    m5: \" leading space\\nsecond\"\n    m6: \"trailing space \\nsecond\"\n    m7: \"a\\n b\"\n    m8: \"\\n\"\n    m9: \"\\n\\n\"\n    m10: \"x\\n\"\n    m11: \"tab\\there\"\n    m12: \"tab\\tand\\nnewline\"\n    m13: \"cr\\rhere\"\n    m14: \"crlf\\r\\nhere\"\n    m15: \"bell\\a\"\n    m16: \"nul\\0x\"\n    m17: \"esc\\e\"\n    m18: \"nel\\Nx\"\n    m19: \"nbsp\\_x\"\n    m20: \"ls\\Lx\"\n    m21: \"ps\\Px\"\n    m22: \"del\\x7f\"\n    m23: \"\\ufeffbom\"\n    m24: \"mid\\ufeffbom\"\n    m25: \"\\uFFFE\"\n    m26: \"emoji \u{1f600} ok\"\n    m27: \"back\\\\slash\"\n    m28: \"dq \\\" x\"\n",
"overrides:\n    t:\n        m1: |\n            line1\n            line2\n        m2: |-\n            line1\n            line2\n        m3: |+\n            line1\n            line2\n\n        m4: |4-\n\n            leading newline\n        m5: |4-\n             leading space\n            second\n        m6: \"trailing space \\nsecond\"\n        m7: |-\n            a\n             b\n        m8: |4+\n\n        m9: |4+\n\n\n        m10: |\n            x\n        m11: \"tab\\there\"\n        m12: |-\n            tab\tand\n            newline\n        m13: \"cr\\rhere\"\n        m14: \"crlf\\r\\nhere\"\n        m15: \"bell\\a\"\n        m16: \"nul\\0x\"\n        m17: \"esc\\e\"\n        m18: \"nel\\Nx\"\n        m19: nbsp\u{a0}x\n        m20: 'ls\u{2028}            x'\n        m21: 'ps\u{2029}            x'\n        m22: \"del\\x7F\"\n        m23: \"\\uFEFF\\x62\\x6F\\x6D\"\n        m24: \"mid\\uFEFFbom\"\n        m25: \"\\uFFFE\"\n        m26: \"emoji \\U0001F600 ok\"\n        m27: back\\slash\n        m28: dq \" x\n",
    ),
    (
r#"overrides:
  t:
    u1: héllo
    u2: 日本語
    u3: "ünïcödé: x"
    u4: Ωmega
"#,
r"overrides:
    t:
        u1: héllo
        u2: 日本語
        u3: 'ünïcödé: x'
        u4: Ωmega
",
    ),
    (
r#"overrides:
  t:
    long1: this is a very long string with many spaces that should exceed eighty columns when emitted by the yaml encoder
    long2: 'this is a very long string with many spaces that should exceed eighty columns when emitted: by the yaml encoder'
    long3: "this is a very long string with many spaces that should exceed eighty columns\twhen emitted by the yaml encoder ok"
    long4: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
    long5: "this is a very long string with many spaces that should exceed eighty columns when emitted\nby the yaml encoder and it also has newlines in it that are rather long as well, more than eighty"
    long6: 'this is a very long string with double  spaces  that should exceed eighty columns  when  emitted  by the yaml encoder'
    long7: ' this is a very long string with a leading space that should exceed eighty columns when emitted by the yaml encoder'
    long8: "this is a very long string with a trailing space that should exceed eighty columns when emitted by the yaml encoder "
"#,
r#"overrides:
    t:
        long1: this is a very long string with many spaces that should exceed eighty columns when emitted by the yaml encoder
        long2: 'this is a very long string with many spaces that should exceed eighty columns when emitted: by the yaml encoder'
        long3: "this is a very long string with many spaces that should exceed eighty columns\twhen emitted by the yaml encoder ok"
        long4: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
        long5: |-
            this is a very long string with many spaces that should exceed eighty columns when emitted
            by the yaml encoder and it also has newlines in it that are rather long as well, more than eighty
        long6: this is a very long string with double  spaces  that should exceed eighty columns  when  emitted  by the yaml encoder
        long7: ' this is a very long string with a leading space that should exceed eighty columns when emitted by the yaml encoder'
        long8: 'this is a very long string with a trailing space that should exceed eighty columns when emitted by the yaml encoder '
"#,
    ),
    (
r"overrides:
  t:
    seq_of_maps:
      - name: a
        value: 1
      - name: b
        value: [1, 2]
      - {}
      - []
      - - nested
        - seq
      - - - deep
    map_of_seqs:
      x: [a, b, c]
      y: []
      z: {}
    empty_map: {}
    empty_seq: []
",
r#"overrides:
    t:
        empty_map: {}
        empty_seq: []
        map_of_seqs:
            x:
                - a
                - b
                - c
            "y": []
            z: {}
        seq_of_maps:
            - name: a
              value: 1
            - name: b
              value:
                - 1
                - 2
            - {}
            - []
            - - nested
              - seq
            - - - deep
"#,
    ),
    (
r"overrides: {}
",
r"overrides: {}
",
    ),
    (
r"{}
",
r"{}
",
    ),
    (
r"overrides:
",
r"overrides: null
",
    ),
    (
r#"overrides:
  "123": {ingestion_rate: 1}
  "456": {ingestion_rate: 2}
  "12": {ingestion_rate: 3}
  "0": {ingestion_rate: 4}
"#,
r#"overrides:
    "0":
        ingestion_rate: 4
    "12":
        ingestion_rate: 3
    "123":
        ingestion_rate: 1
    "456":
        ingestion_rate: 2
"#,
    ),
    (
r"overrides:
  t:
    nested_nonstring_keys:
      1: one
      2: two
      10: ten
      -1: minus
      1.5: onefive
      true: t
      false: f
      ~: nil
      a: a
      b: b
",
r"overrides:
    t:
        nested_nonstring_keys:
            -1: minus
            false: f
            true: t
            1: one
            1.5: onefive
            2: two
            10: ten
            null: nil
            a: a
            b: b
",
    ),
    (
r"overrides:
  t:
    float_keys:
      1.5: a
      0.5: b
      2: c
      1: d
",
r"overrides:
    t:
        float_keys:
            0.5: b
            1: d
            1.5: a
            2: c
",
    ),
    (
r"multi_kv_config:
  primary: consul
  mirror_enabled: false
ingester_limits:
  max_ingestion_rate: 20000
  max_series: 1500000
  max_tenants: 1000
  max_inflight_push_requests: 30000
distributor_limits:
  max_ingestion_rate: 0
  max_inflight_push_requests: 0
",
r"distributor_limits:
    max_inflight_push_requests: 0
    max_ingestion_rate: 0
ingester_limits:
    max_inflight_push_requests: 30000
    max_ingestion_rate: 20000
    max_series: 1500000
    max_tenants: 1000
multi_kv_config:
    mirror_enabled: false
    primary: consul
",
    ),
    (
r#"overrides:
  tenant:
    metric_relabel_configs:
      - source_labels: [__name__]
        regex: "up|down"
        action: drop
      - source_labels: [job, instance]
        separator: ;
        target_label: x
        replacement: $1
    drop_labels: [a, b]
    forwarding_rules:
      metric_a: {ingest: true}
"#,
r"overrides:
    tenant:
        drop_labels:
            - a
            - b
        forwarding_rules:
            metric_a:
                ingest: true
        metric_relabel_configs:
            - action: drop
              regex: up|down
              source_labels:
                - __name__
            - replacement: $1
              separator: ;
              source_labels:
                - job
                - instance
              target_label: x
",
    ),
    (
r#"overrides:
  tenant:
    cost_attribution_labels: team,service
    active_series_custom_trackers:
      prod: '{namespace="prod"}'
      dev: '{namespace=~"dev|test"}'
      longone: '{namespace=~"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa|bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb|ccccccccccccccccccccccccccc"}'
"#,
r#"overrides:
    tenant:
        active_series_custom_trackers:
            dev: '{namespace=~"dev|test"}'
            longone: '{namespace=~"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa|bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb|ccccccccccccccccccccccccccc"}'
            prod: '{namespace="prod"}'
        cost_attribution_labels: team,service
"#,
    ),
    (
r#"overrides:
  tenant:
    blocked_queries:
      - pattern: 'rate(metric_counter[5m])'
        regex: false
      - pattern: '.*metric_name.*'
        regex: true
    blocked_requests:
      - path: /api/v1/series
        method: GET
        query_params:
          match[]:
            value: "{__name__=\"x\"}"
"#,
r#"overrides:
    tenant:
        blocked_queries:
            - pattern: rate(metric_counter[5m])
              regex: false
            - pattern: .*metric_name.*
              regex: true
        blocked_requests:
            - method: GET
              path: /api/v1/series
              query_params:
                match[]:
                    value: '{__name__="x"}'
"#,
    ),
    (
r##"key with spaces: 1
"key: colon": 2
"#hash": 3
"- dash": 4
'': 5
"multi\nline": 6
"yes": 7
"true": 8
"123": 9
"##,
r#""": 5
'#hash': 3
'- dash': 4
"123": 9
key with spaces: 1
'key: colon': 2
? |-
    multi
    line
: 6
"true": 8
"yes": 7
"#,
    ),
    (
r#"? "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
: long key value
short: x
"#,
r"? aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
: long key value
short: x
",
    ),
    (
r#"overrides:
  t:
    ? "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    : exactly128
    ? "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    : exactly129
    ? "multi\nline key"
    : mv
    ? "trailing\n"
    : [a]
"#,
r"overrides:
    t:
        aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa: exactly128
        aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa: exactly129
        ? |-
            multi
            line key
        : mv
        ? |
            trailing
        :   - a
",
    ),
    (
r#"seq:
  - "multi\nline"
  - "two\nlines\n"
  - ""
  - null
  - ~
  - "~"
  - [ ]
  - { }
  - {a: 1}
  - {a: [1, {b: 2}]}
"#,
r#"seq:
    - |-
      multi
      line
    - |
      two
      lines
    - ""
    - null
    - null
    - "~"
    - []
    - {}
    - a: 1
    - a:
        - 1
        - b: 2
"#,
    ),
    (
r"a:
  b:
    c:
      d:
        e:
          f: deep
          g: [1, 2, 3]
          h:
            - i: 1
              j: 2
            - k: 3
",
r"a:
    b:
        c:
            d:
                e:
                    f: deep
                    g:
                        - 1
                        - 2
                        - 3
                    h:
                        - i: 1
                          j: 2
                        - k: 3
",
    ),
    (
r"list_of_lists:
  - [1, 2]
  - [[3, 4], [5]]
  - [[[]]]
  - [{}]
",
r"list_of_lists:
    - - 1
      - 2
    - - - 3
        - 4
      - - 5
    - - - []
    - - {}
",
    ),
    (
r"anchors:
  base: &b {x: 1, y: [a, b]}
  ref: *b
",
r#"anchors:
    base:
        x: 1
        "y":
            - a
            - b
    ref:
        x: 1
        "y":
            - a
            - b
"#,
    ),
    (
r"overrides:
  t:
    many_floats:
      - 100000.0
      - 1000000.0
      - 0.0001
      - 0.00001
      - 123456789012345680000.0
      - 1.7976931348623157e308
      - 5e-324
      - 2.2250738585072014e-308
      - 0.30000000000000004
      - 1.0e+100
      - 9007199254740993.0
      - 12345678.9
      - 1.25e-5
      - -1.5e-10
",
r"overrides:
    t:
        many_floats:
            - 100000
            - 1e+06
            - 0.0001
            - 1e-05
            - 1.2345678901234568e+20
            - 1.7976931348623157e+308
            - 5e-324
            - 2.2250738585072014e-308
            - 0.30000000000000004
            - 1e+100
            - 9.007199254740992e+15
            - 1.23456789e+07
            - 1.25e-05
            - -1.5e-10
",
    ),
    (
r"overrides:
  tenant-1:
    ingestion_rate: 25000
    ingestion_burst_size: 500000
    max_global_series_per_user: 0
    max_global_series_per_metric: 0
    ruler_max_rules_per_rule_group: 20
    ruler_max_rule_groups_per_tenant: 70
    compactor_blocks_retention_period: 1y
    out_of_order_time_window: 10m
    native_histograms_ingestion_enabled: true
    max_label_names_per_series: 30
    max_fetched_chunk_bytes_per_query: 0
    ingestion_tenant_shard_size: 3
  tenant-2:
    ingestion_rate: 1e6
    ingestion_partitions_tenant_shard_size: 0
",
r"overrides:
    tenant-1:
        compactor_blocks_retention_period: 1y
        ingestion_burst_size: 500000
        ingestion_rate: 25000
        ingestion_tenant_shard_size: 3
        max_fetched_chunk_bytes_per_query: 0
        max_global_series_per_metric: 0
        max_global_series_per_user: 0
        max_label_names_per_series: 30
        native_histograms_ingestion_enabled: true
        out_of_order_time_window: 10m
        ruler_max_rule_groups_per_tenant: 70
        ruler_max_rules_per_rule_group: 20
    tenant-2:
        ingestion_partitions_tenant_shard_size: 0
        ingestion_rate: 1e+06
",
    ),
    (
r#"overrides:
  "*": {}
  "!x": {}
  "(a)": {}
  "a'b": {}
  ".": {}
  "..": {}
  "a.b": {}
"#,
r"overrides:
    '!x': {}
    (a): {}
    '*': {}
    .: {}
    ..: {}
    a'b: {}
    a.b: {}
",
    ),
    (
r#"string_seq:
  - a
  - 'b c'
  - "  "
  - "x\ty"
  - 'true'
  - 1
  - 1.0
  - -1
"#,
r#"string_seq:
    - a
    - b c
    - '  '
    - "x\ty"
    - "true"
    - 1
    - 1
    - -1
"#,
    ),
    (
r#"x: "a very long double quoted string that contains a \"quote\" and needs to be folded somewhere past column eighty for sure yes"
y: "very long string ending with newline and spaces in between words so that it is longer than eighty chars\n"
z: "very long string with a backslash \\ in the middle of it so that we exceed the line width of eighty characters"
"#,
r#"x: a very long double quoted string that contains a "quote" and needs to be folded somewhere past column eighty for sure yes
"y": |
    very long string ending with newline and spaces in between words so that it is longer than eighty chars
z: very long string with a backslash \ in the middle of it so that we exceed the line width of eighty characters
"#,
    ),
    (
r#"indented_literal:
  - "  two leading spaces\nsecond line"
  - "\tleading tab\nsecond"
  - "trailing newlines\n\n\n"
"#,
"indented_literal:\n    - |4-\n        two leading spaces\n      second line\n    - |-\n      \tleading tab\n      second\n    - |+\n      trailing newlines\n\n\n",
    ),
    (
r"deep_seq:
  - - - - x
        - y
      - z
    - w
  - v
",
r#"deep_seq:
    - - - - x
          - "y"
        - z
      - w
    - v
"#,
    ),
    (
r#""Z": 1
"a": 2
"A": 3
"z": 4
"é": 5
"É": 6
"0": 7
"_": 8
"~": 9
"ä1": 10
"ä10": 11
"ä2": 12
"#,
r#"_: 8
"~": 9
"0": 7
A: 3
Z: 1
a: 2
z: 4
É: 6
ä1: 10
ä2: 12
ä10: 11
é: 5
"#,
    ),
    (
r"nat:
  x1y: a
  x01y: b
  x001y: c
  x10y: d
  x1: e
  x: f
  x0: g
  x00: h
  x000: i
  x0a: j
  xa: k
  1x: l
  01x: m
  10x: n
  0: o
",
r#"nat:
    0: o
    1x: l
    01x: m
    10x: "n"
    x: f
    x0: g
    x0a: j
    x00: h
    x000: i
    x1: e
    x1y: a
    x01y: b
    x001y: c
    x10y: d
    xa: k
"#,
    ),
    (
r#"mix:
  a1: 1
  a: 2
  a-: 3
  aa: 4
  a1a: 5
  a1-: 6
  a10a: 7
  "a1 ": 8
  a٣: 9
  a3: 10
  ab٣: 11
"#,
r"mix:
    a: 2
    a-: 3
    a1: 1
    a1a: 5
    'a1 ': 8
    a1-: 6
    a3: 10
    a10a: 7
    a٣: 9
    aa: 4
    ab٣: 11
",
    ),
    (
r#"doc: "  "
doc2: "a\u0085b"
doc3: "\u2028"
doc4: "x\u00a0y"
doc5: "\u00a0"
doc6: "a\u200bb"
doc7: "\U0001F600"
doc8: "\x01\x02"
doc9: "\uE000"
doc10: "\uFFFD"
"#,
"doc: '  '\ndoc2: \"a\\Nb\"\ndoc3: '\u{2028}'\ndoc4: x\u{a0}y\ndoc5: \u{a0}\ndoc6: a\u{200b}b\ndoc7: \"\\U0001F600\"\ndoc8: \"\\x01\\x02\"\ndoc9: \u{e000}\ndoc10: \u{fffd}\n",
    ),
    (
r#"fold:
  - word word word word word word word word word word word word word word word word word word word word
  - wordwordwordwordwordwordwordwordwordwordwordwordwordwordwordwordwordwordword wordwordwordwordword
  - "x: word word word word word word word word word word word word word word word word word word word word"
  - "ends with spaces word word word word word word word word word word word word word word word word word  "
  - "word word word word word word word word word word word word word word word word word word word wo  rd"
  - "'quote' word word word word word word word word word word word word word word word word word word word word"
"#,
r"fold:
    - word word word word word word word word word word word word word word word word word word word word
    - wordwordwordwordwordwordwordwordwordwordwordwordwordwordwordwordwordwordword wordwordwordwordword
    - 'x: word word word word word word word word word word word word word word word word word word word word'
    - 'ends with spaces word word word word word word word word word word word word word word word word word  '
    - word word word word word word word word word word word word word word word word word word word wo  rd
    - '''quote'' word word word word word word word word word word word word word word word word word word word word'
",
    ),
    (
r#"nested:
  key1:
    sub:
      - a: 1
        b:
          - c
          - d: e
      - "x"
"#,
r"nested:
    key1:
        sub:
            - a: 1
              b:
                - c
                - d: e
            - x
",
    ),
    (
r#"quoted_odd_numbers:
  a: "0x"
  d: "0b+1"
  e: "1e400"
  f: ".5_0"
  g: "+.5"
  i: "1.5e"
  j: "0x1p-2"
  k: "infinity"
  l: "nan"
  m: ".Inf"
  n: "+.inf"
  o: "1_2_3"
  p: "0x_1F"
  q: "1__0"
  s: "0o+7"
  t: "1e-400"
  u: "1E3"
  w: "0."
  y: "+12_3.5"
  z: "1.e3"
  aa: "09"
  ab: "0777"
  ac: "1_000"
  ad: "2001-12-14"
  ae: "2001-1-5"
  af: "2001-02-30"
  ag: "2001-12-14 21:59:43.10"
  ah: "2001-12-14t21:59:43.10-05:00"
  ai: "2002-12-14T00:00:00Z"
  aj: "2001-12-14T25:00:00Z"
  ak: "2001-12-14T21:59:43+24:00"
  al: "2001-12-14T21:59:43+25:00"
  am: "2001-12-14T21:59:43,5Z"
  an: "2001-12-14T1:2:3Z"
  ao: "2001-12-14  21:59:43"
  ap: "2000-02-29"
  aq: "1900-02-29"
  ar: "2001-12-14T21:59Z"
  as: "2001-12-14T21:59:43.1234567891234Z"
  at: "2001-13-01"
  au: "2001-00-01"
  av: "2001-12-14T21:59:60Z"
  aw: "2001-12-14x"
  ax: "1:2:3"
  ay: "-1:20"
  az: "1:60"
  ba: "1_0:20.5_"
  bb: "y"
  bc: "On"
  bd: "~x"
  be: "-0"
  bf: "+0"
  bg: "-"
  bh: "+"
  bi: "."
  bj: "-0x10"
  bk: "0X10"
  bl: "0B1"
  bm: "0O7"
  bn: "08"
  bo: "-0o"
  bp: "1e"
  bq: "1e5_0"
  br: "1_e5"
  bs: "5e+_1"
  bt: ".e5"
  bu: "._5"
"#,
r#"quoted_odd_numbers:
    a: 0x
    aa: "09"
    ab: "0777"
    ac: "1_000"
    ad: "2001-12-14"
    ae: "2001-1-5"
    af: 2001-02-30
    ag: "2001-12-14 21:59:43.10"
    ah: "2001-12-14t21:59:43.10-05:00"
    ai: "2002-12-14T00:00:00Z"
    aj: 2001-12-14T25:00:00Z
    ak: "2001-12-14T21:59:43+24:00"
    al: 2001-12-14T21:59:43+25:00
    am: "2001-12-14T21:59:43,5Z"
    an: "2001-12-14T1:2:3Z"
    ao: "2001-12-14  21:59:43"
    ap: "2000-02-29"
    aq: 1900-02-29
    ar: 2001-12-14T21:59Z
    as: "2001-12-14T21:59:43.1234567891234Z"
    at: 2001-13-01
    au: 2001-00-01
    av: 2001-12-14T21:59:60Z
    aw: 2001-12-14x
    ax: "1:2:3"
    ay: "-1:20"
    az: 1:60
    ba: "1_0:20.5_"
    bb: "y"
    bc: "On"
    bd: ~x
    be: "-0"
    bf: "+0"
    bg: '-'
    bh: +
    bi: .
    bj: "-0x10"
    bk: "0X10"
    bl: "0B1"
    bm: "0O7"
    bn: "08"
    bo: -0o
    bp: 1e
    bq: "1e5_0"
    br: "1_e5"
    bs: "5e+_1"
    bt: .e5
    bu: ._5
    d: "0b+1"
    e: 1e400
    f: ".5_0"
    g: "+.5"
    i: 1.5e
    j: 0x1p-2
    k: infinity
    l: nan
    m: ".Inf"
    "n": "+.inf"
    o: "1_2_3"
    p: "0x_1F"
    q: "1__0"
    s: "0o+7"
    t: "1e-400"
    u: "1E3"
    w: "0."
    "y": "+12_3.5"
    z: "1.e3"
"#,
    ),
    (
"keys_emoji:\n  \"\u{1f600}\": 1\n  \"a\u{1f600}\": 2\n  \"\u{1f600}a\": 3\n  \"Ⅻ\": 4\n  \"ⅻ1\": 5\n  \"²\": 6\n",
r#"keys_emoji:
    ²: 6
    Ⅻ: 4
    ⅻ1: 5
    "\U0001F600": 1
    "\U0001F600a": 3
    "a\U0001F600": 2
"#,
    ),
    (
r"overrides:
  t1: {a: 1}
  t2: {a: 2}
  t3: {a: 3}
  t4: {a: 4}
  t5: {a: 5}
  t6: {a: 6}
  t7: {a: 7}
  t8: {a: 8}
  t9: {a: 9}
  t10: {a: 10}
  t11: {a: 11}
  t12: {a: 12}
  t13: {a: 13}
  t20: {a: 20}
  t100: {a: 100}
  t099: {a: 99}
  t0100: {a: 100}
",
r"overrides:
    t1:
        a: 1
    t2:
        a: 2
    t3:
        a: 3
    t4:
        a: 4
    t5:
        a: 5
    t6:
        a: 6
    t7:
        a: 7
    t8:
        a: 8
    t9:
        a: 9
    t10:
        a: 10
    t11:
        a: 11
    t12:
        a: 12
    t13:
        a: 13
    t20:
        a: 20
    t099:
        a: 99
    t100:
        a: 100
    t0100:
        a: 100
",
    ),
    (
r#"deeply:
  nested:
    for:
      folding:
        purposes:
          s: "this is a long string with spaces placed at a deep indentation level so that folding kicks in"
          p: this is a long plain string with spaces placed at a deep indentation level so that folding kicks in
          q: 'this is a long single quoted string, with spaces: placed at a deep indentation level so that folding'
          l: [this is a long plain string in a sequence with spaces placed at a deep indentation level so that folding kicks in]
"#,
r"deeply:
    nested:
        for:
            folding:
                purposes:
                    l:
                        - this is a long plain string in a sequence with spaces placed at a deep indentation level so that folding kicks in
                    p: this is a long plain string with spaces placed at a deep indentation level so that folding kicks in
                    q: 'this is a long single quoted string, with spaces: placed at a deep indentation level so that folding'
                    s: this is a long string with spaces placed at a deep indentation level so that folding kicks in
",
    ),
    (
r#"mixed_seq:
  - 1
  - 1.5
  - true
  - null
  - str
  - [a]
  - {k: v}
  - "multi\nline\n"
  - ""
"#,
r#"mixed_seq:
    - 1
    - 1.5
    - true
    - null
    - str
    - - a
    - k: v
    - |
      multi
      line
    - ""
"#,
    ),
    (
r#"empty_strings:
  "": ""
  a: ""
  b: [""]
  c: {"": ""}
"#,
r#"empty_strings:
    "": ""
    a: ""
    b:
        - ""
    c:
        "": ""
"#,
    ),
    (
r#"literal_edge:
  a: "\n\nfoo"
  b: "foo\n "
  c: "foo \nbar"
  d: "foo\n\tbar"
  e: " \nbar"
  f: "foo\n\n"
  g: "a\n\nb\n"
  h: "é\n"
  i: "\u2028\n"
  j: "x\u2028"
"#,
"literal_edge:\n    a: |4-\n\n\n        foo\n    b: \"foo\\n \"\n    c: \"foo \\nbar\"\n    d: |-\n        foo\n        \tbar\n    e: \" \\nbar\"\n    f: |+\n        foo\n\n    g: |\n        a\n\n        b\n    h: |\n        é\n    i: |4+\n\u{2028}\n    j: 'x\u{2028}'\n",
    ),
    (
r"~: dropped
a: 1
",
r"a: 1
",
    ),
    (
r#"overrides:
  "12345":
    ingestion_rate: 50000
    ingestion_burst_size: 1000000
    max_global_series_per_user: 3000000
    max_global_exemplars_per_user: 100000
    ingestion_artificial_delay: 0s
    compactor_split_and_merge_shards: 2
    compactor_split_groups: 4
    store_gateway_tenant_shard_size: 0
    max_partial_query_length: 0s
    max_total_query_length: 12000h
    results_cache_ttl: 1w
    query_sharding_total_shards: 16
    query_sharding_max_sharded_queries: 128
    alertmanager_receivers_firewall_block_cidr_networks: 10.0.0.0/8,192.168.0.0/16
    ruler_alerting_rules_evaluation_enabled: true
    otel_metric_suffixes_enabled: false
  anonymous:
    ingestion_rate: 0.5
"#,
r#"overrides:
    "12345":
        alertmanager_receivers_firewall_block_cidr_networks: 10.0.0.0/8,192.168.0.0/16
        compactor_split_and_merge_shards: 2
        compactor_split_groups: 4
        ingestion_artificial_delay: 0s
        ingestion_burst_size: 1000000
        ingestion_rate: 50000
        max_global_exemplars_per_user: 100000
        max_global_series_per_user: 3000000
        max_partial_query_length: 0s
        max_total_query_length: 12000h
        otel_metric_suffixes_enabled: false
        query_sharding_max_sharded_queries: 128
        query_sharding_total_shards: 16
        results_cache_ttl: 1w
        ruler_alerting_rules_evaluation_enabled: true
        store_gateway_tenant_shard_size: 0
    anonymous:
        ingestion_rate: 0.5
"#,
    ),
    (
r##"quoted_keys:
  "a: b": 1
  "- c": 2
  "#d": 3
  "null": 4
  "yes": 5
  "1.5": 6
  "": 7
  " sp": 8
  "x\ny": 9
"##,
r#"quoted_keys:
    "": 7
    ' sp': 8
    '#d': 3
    '- c': 2
    "1.5": 6
    'a: b': 1
    "null": 4
    ? |-
        x
        y
    : 9
    "yes": 5
"#,
    ),
    (
r#"seqs:
  - - - [a, b]
      - {c: d}
    - e: [f]
      g:
        - h
        - - i
  - "- not a seq"
  - "? not a key"
  - |
    literal in seq
  - ''
"#,
r#"seqs:
    - - - - a
          - b
        - c: d
      - e:
            - f
        g:
            - h
            - - i
    - '- not a seq'
    - '? not a key'
    - |
      literal in seq
    - ""
"#,
    ),
    (
r#"rules:
  - alert: HighErrorRate
    expr: |
      sum(rate(http_requests_total{status=~"5.."}[5m])) by (job)
        /
      sum(rate(http_requests_total[5m])) by (job) > 0.05
    for: 10m
    labels: {severity: page}
    annotations:
      summary: "High error rate on {{ $labels.job }}"
      description: 'Error rate is {{ $value | humanizePercentage }}: check it'
"#,
r#"rules:
    - alert: HighErrorRate
      annotations:
        description: 'Error rate is {{ $value | humanizePercentage }}: check it'
        summary: High error rate on {{ $labels.job }}
      expr: |
        sum(rate(http_requests_total{status=~"5.."}[5m])) by (job)
          /
        sum(rate(http_requests_total[5m])) by (job) > 0.05
      for: 10m
      labels:
        severity: page
"#,
    ),
    (
r#"overrides:
  t:
    native_histograms_ingestion_enabled: true
    max_native_histogram_buckets: 160
    reduce_native_histogram_over_max_buckets: true
    otel_keep_identifying_resource_attributes: false
    otel_created_timestamp_zero_ingestion_enabled: false
    ingest_storage_read_consistency: eventual
    ingestion_partitions_tenant_shard_size: 2
    label_names_and_values_results_max_size_bytes: 419430400
    validation_scheme: utf8
    name_validation_scheme: legacy
    separate_metrics_group_label: ""
    max_cost_attribution_cardinality: 2000
    cost_attribution_cooldown: 0s
"#,
r#"overrides:
    t:
        cost_attribution_cooldown: 0s
        ingest_storage_read_consistency: eventual
        ingestion_partitions_tenant_shard_size: 2
        label_names_and_values_results_max_size_bytes: 419430400
        max_cost_attribution_cardinality: 2000
        max_native_histogram_buckets: 160
        name_validation_scheme: legacy
        native_histograms_ingestion_enabled: true
        otel_created_timestamp_zero_ingestion_enabled: false
        otel_keep_identifying_resource_attributes: false
        reduce_native_histogram_over_max_buckets: true
        separate_metrics_group_label: ""
        validation_scheme: utf8
"#,
    ),
    (
"unicode_values:\n  - \"Straße\"\n  - \"naïve café\"\n  - \"中文 测试 with spaces and a long tail that goes past eighty columns for sure 中文 测试\"\n  - \"ʼquoteʼ\"\n  - \"\u{1f600}\u{1f600}\u{1f600}\"\n  - \"a\\u00adb\"\n",
"unicode_values:\n    - Straße\n    - naïve café\n    - 中文 测试 with spaces and a long tail that goes past eighty columns for sure 中文 测试\n    - ʼquoteʼ\n    - \"\\U0001F600\\U0001F600\\U0001F600\"\n    - a\u{ad}b\n",
    ),
    ];

    #[test]
    fn matches_go_yaml_v3_marshal() {
        let mut failures = Vec::new();
        for (i, (input, want)) in GO_CASES.iter().enumerate() {
            let got = match serde_yaml::from_str::<Value>(input) {
                Ok(v) => marshal_generic(&v).unwrap_or_else(|e| format!("ERR {e}")),
                Err(e) => format!("PARSE ERR {e}"),
            };
            if got != *want {
                failures.push(format!(
                    "case {i}:\n--- input\n{input}\n--- go\n{want}\n--- rust\n{got}"
                ));
            }
        }
        assert!(failures.is_empty(), "{}", failures.join("\n"));
    }

    /// Inputs whose *plain* scalars serde_yaml types differently from yaml.v3's `resolve`.
    /// `(input, go.yaml.in/yaml/v3 output, marshal_generic output)`. The Rust side emits these
    /// values exactly as Go emits the same text when it was quoted in the source (see the
    /// `quoted_odd_numbers`, `s_*` and `quoted_keys` documents in `GO_CASES`), so the result is
    /// Go-identical whenever such values are quoted in the runtime config file.
    #[rustfmt::skip]
    const DIVERGENT_CASES: &[(&str, &str, &str)] = &[
    // plain timestamps: yaml.v3 decodes them to time.Time (re-emitted as RFC3339Nano), serde_yaml keeps the string
    (
r"overrides:
  t:
    x: 2001-12-14
    y: 2001-12-14t21:59:43.10-05:00
    z: 2001-12-14 21:59:43.10
    w: 2002-12-14T00:00:00Z
    v: 2001-02-30
    u: 2001-1-5
",
r#"overrides:
    t:
        u: 2001-01-05T00:00:00Z
        v: 2001-02-30
        w: 2002-12-14T00:00:00Z
        x: 2001-12-14T00:00:00Z
        "y": 2001-12-14T21:59:43.1-05:00
        z: 2001-12-14T21:59:43.1Z
"#,
r#"overrides:
    t:
        u: "2001-1-5"
        v: 2001-02-30
        w: "2002-12-14T00:00:00Z"
        x: "2001-12-14"
        "y": "2001-12-14t21:59:43.10-05:00"
        z: "2001-12-14 21:59:43.10"
"#,
    ),
    // plain `1_000` / `0777` / `09` / `00`: yaml.v3 resolves them to numbers (underscores stripped, legacy octal, float), serde_yaml keeps strings
    (
r"overrides:
  t:
    us: 1_000
    lead0: 0777
    lead0b: 09
    lead0c: 00
    neg0: -0
    yes_plain: yes
    no_plain: no
    on_plain: on
    hexneg: -0x10
    b60: 1:20
    tilde_str: a~b
",
r#"overrides:
    t:
        b60: "1:20"
        hexneg: -16
        lead0: 511
        lead0b: 9
        lead0c: 0
        neg0: 0
        no_plain: "no"
        on_plain: "on"
        tilde_str: a~b
        us: 1000
        yes_plain: "yes"
"#,
r#"overrides:
    t:
        b60: "1:20"
        hexneg: -16
        lead0: "0777"
        lead0b: "09"
        lead0c: "00"
        neg0: 0
        no_plain: "no"
        on_plain: "on"
        tilde_str: a~b
        us: "1_000"
        yes_plain: "yes"
"#,
    ),
    // plain `0b+1` / `.5_0` / `1_2_3` / `0x_1F` / `1__0` / `0o+7` / `+12_3.5`: yaml.v3 resolves them to numbers, serde_yaml keeps strings
    (
r"odd_numbers:
  a: 0x
  b: 0b
  c: 0o
  d: 0b+1
  e: 1e400
  f: .5_0
  g: +.5
  h: -.5
  i: 1.5e
  j: 0x1p-2
  k: infinity
  l: nan
  m: .Inf
  n: +.inf
  o: 1_2_3
  p: 0x_1F
  q: 1__0
  r: -0b101
  s: 0o+7
  t: 1e-400
  u: 1E3
  v: 1e+3
  w: 0.
  x: -.inf
  y: +12_3.5
  z: 1.e3
",
r#"odd_numbers:
    a: 0x
    b: 0b
    c: 0o
    d: 1
    e: 1e400
    f: 0.5
    g: 0.5
    h: -0.5
    i: 1.5e
    j: 0x1p-2
    k: infinity
    l: nan
    m: .inf
    "n": .inf
    o: 123
    p: 31
    q: 10
    r: -5
    s: 7
    t: 0
    u: 1000
    v: 1000
    w: 0
    x: -.inf
    "y": 123.5
    z: 1000
"#,
r#"odd_numbers:
    a: 0x
    b: 0b
    c: 0o
    d: "0b+1"
    e: 1e400
    f: ".5_0"
    g: 0.5
    h: -0.5
    i: 1.5e
    j: 0x1p-2
    k: infinity
    l: nan
    m: .inf
    "n": .inf
    o: "1_2_3"
    p: "0x_1F"
    q: "1__0"
    r: -5
    s: "0o+7"
    t: 0
    u: 1000
    v: 1000
    w: 0
    x: -.inf
    "y": "+12_3.5"
    z: 1000
"#,
    ),
    // non-string root keys: yaml.v3 keeps their source text (`0x10`, `1.50`), a Value only has the parsed number
    (
r"1: x
0x10: y
1.50: z
true: w
",
r#""0x10": "y"
"1": x
"1.50": z
"true": w
"#,
r#""1": x
"1.5": z
"16": "y"
"true": w
"#,
    ),
    ];

    #[test]
    fn documented_plain_scalar_typing_divergences() {
        for (i, (input, go, rust)) in DIVERGENT_CASES.iter().enumerate() {
            assert_ne!(go, rust, "case {i} no longer diverges; move it to GO_CASES");
            assert_eq!(marshal_str(input), *rust, "case {i}");
        }
    }

    #[test]
    fn serde_yaml_rejects_integers_beyond_u64() {
        // yaml.v3 decodes these as float64 (`1.8446744073709552e+19`); serde_yaml refuses to
        // parse the document at all, so the caller fails before computing a hash.
        assert!(serde_yaml::from_str::<Value>("a: 18446744073709551616\n").is_err());
    }

    #[test]
    fn format_float_matches_strconv_g_shortest() {
        for (f, want) in [
            (0.0, "0"),
            (-0.0, "-0"),
            (1.0, "1"),
            (100000.0, "100000"),
            (1000000.0, "1e+06"),
            (0.0001, "0.0001"),
            (0.00001, "1e-05"),
            (1234567.0, "1.234567e+06"),
            (1e21, "1e+21"),
            (1e100, "1e+100"),
            (5e-324, "5e-324"),
            (f64::MAX, "1.7976931348623157e+308"),
            (0.30000000000000004, "0.30000000000000004"),
            (12345678.9, "1.23456789e+07"),
            (-1.5e-10, "-1.5e-10"),
            (f64::INFINITY, ".inf"),
            (f64::NEG_INFINITY, "-.inf"),
            (f64::NAN, ".nan"),
        ] {
            assert_eq!(format_float(f).as_deref(), Ok(want), "{f}");
        }
    }

    #[test]
    fn resolve_tag_matches_yaml_v3() {
        for (s, want) in [
            ("", Tag::Null),
            ("~", Tag::Null),
            ("true", Tag::Bool),
            ("yes", Tag::Str),
            // The resolve table has no hint for '<', so "<<" never reaches the map lookup.
            ("<<", Tag::Str),
            ("1_000", Tag::Int),
            ("0777", Tag::Int),
            ("09", Tag::Float),
            ("0b+1", Tag::Int),
            ("1e400", Tag::Str),
            (".5_0", Tag::Float),
            ("2001-12-14", Tag::Timestamp),
            ("2001-02-30", Tag::Str),
            ("2001-12-14T21:59:43+25:00", Tag::Str),
            ("12:30", Tag::Str),
            ("-", Tag::Str),
        ] {
            assert_eq!(resolve_tag(s), want, "{s:?}");
        }
        assert!(is_base60_float("190:20:30.15"));
        assert!(!is_base60_float("1:60"));
    }

    #[test]
    fn key_order_never_panics_on_inconsistent_comparisons() {
        // NaN keys make keyList.Less inconsistent; sorting must still terminate without panicking.
        let keys = vec![
            SortKey::Float(f64::NAN),
            SortKey::Int(1),
            SortKey::Float(f64::NAN),
            SortKey::Int(0),
        ];
        let sorted = sort_by_less(keys, &key_less);
        assert_eq!(sorted.len(), 4);
    }

    #[test]
    fn natural_key_order() {
        let mut keys = vec!["a10", "a9", "a1", "1", "01", "10", "2", "b", "B", "_x"];
        keys = sort_by_less(keys, &|a: &&str, b: &&str| {
            key_less(
                &SortKey::Str(a.chars().collect()),
                &SortKey::Str(b.chars().collect()),
            )
        });
        assert_eq!(
            keys,
            ["_x", "1", "01", "2", "10", "B", "a1", "a9", "a10", "b"]
        );
    }

    #[test]
    fn marshal_struct_emits_relabel_source_labels_in_flow_style() {
        let doc: Value = serde_yaml::from_str(
            r#"
metric_relabel_configs:
  - source_labels: [__name__, job]
    separator: ;
    target_label: out
    replacement: $1
    action: replace
"#,
        )
        .unwrap();
        let out = marshal_struct(&doc).unwrap();
        assert!(
            out.contains("source_labels: [__name__, job]"),
            "expected flow sequence in marshal_struct output:\n{out}"
        );
    }
}
