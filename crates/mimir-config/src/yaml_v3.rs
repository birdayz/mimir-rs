//! go.yaml.in/yaml/v3 compatible YAML parsing and decode-error reporting for the config file.
//!
//! Ported from:
//! - go.yaml.in/yaml/v3/decode.go (`parser`, `decoder`: unmarshal/prepare/scalar/sequence/
//!   mapping/mappingStruct/checkUniqueKeys/terror/merge)
//! - go.yaml.in/yaml/v3/resolve.go (`resolve`, `shortTag`)
//! - go.yaml.in/yaml/v3/yaml.go (`Decoder.Decode`, `TypeError`, `Node.ShortTag`)
//! - github.com/grafana/mimir/pkg/mimir/mimir.go (`ConfigWithCommon.UnmarshalYAML`,
//!   `configWithCustomCommonUnmarshaler`, `specificLocationsUnmarshaler`)
//!
//! `LoadConfig` in Go decodes the config with `yaml.NewDecoder(..).KnownFields(true)`, which
//! reports syntax errors as `yaml: line N: <libyaml problem>` and decode errors as
//! `yaml: unmarshal errors:\n  line N: field X not found in type T` (all of them, in document
//! order). yaml.v3's parser is a port of libyaml, so the document is parsed here with
//! `unsafe-libyaml` (the same C port used by serde_yaml) into a yaml.v3 `Node` tree, then
//! decoded against the reflected Go type graph of `mimir.Config` (`yaml_types.json`, generated
//! by `tools/gen-yaml-types.sh`) reproducing yaml.v3's error texts. Values are still built by
//! serde afterwards; this module only decides *whether* and *how* Go would reject the file.

use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;

use serde::Deserialize;
use unsafe_libyaml as unsafe_sys;

// ---------------------------------------------------------------------------------------------
// Nodes and parser (decode.go `parser`).
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Document,
    Sequence,
    Mapping,
    Scalar,
    Alias,
}

const TAGGED_STYLE: u8 = 1 << 0;
const DOUBLE_QUOTED_STYLE: u8 = 1 << 1;
const SINGLE_QUOTED_STYLE: u8 = 1 << 2;
const LITERAL_STYLE: u8 = 1 << 3;
const FOLDED_STYLE: u8 = 1 << 4;

const NULL_TAG: &str = "!!null";
const BOOL_TAG: &str = "!!bool";
const STR_TAG: &str = "!!str";
const INT_TAG: &str = "!!int";
const FLOAT_TAG: &str = "!!float";
const TIMESTAMP_TAG: &str = "!!timestamp";
const SEQ_TAG: &str = "!!seq";
const MAP_TAG: &str = "!!map";
const BINARY_TAG: &str = "!!binary";
const MERGE_TAG: &str = "!!merge";
const LONG_TAG_PREFIX: &str = "tag:yaml.org,2002:";

#[derive(Clone, Debug)]
pub struct Node {
    pub kind: Kind,
    pub style: u8,
    pub tag: String,
    pub value: String,
    pub line: usize,
    pub column: usize,
    pub content: Vec<usize>,
    pub alias: Option<usize>,
}

/// First YAML document of a stream (yaml.v3 `Decoder.Decode` only reads one document).
pub struct Document {
    pub nodes: Vec<Node>,
    pub root: usize,
    /// Byte offset just past the document (its `DOCUMENT_END` event).
    pub end_offset: usize,
}

impl Document {
    fn node(&self, i: usize) -> &Node {
        &self.nodes[i]
    }

    /// `Node.indicatedString`.
    fn indicated_string(&self, i: usize) -> bool {
        let n = self.node(i);
        n.kind == Kind::Scalar
            && (short_tag(&n.tag) == STR_TAG
                || ((n.tag.is_empty() || n.tag == "!")
                    && n.style & (SINGLE_QUOTED_STYLE | DOUBLE_QUOTED_STYLE | LITERAL_STYLE | FOLDED_STYLE) != 0))
    }

    /// `Node.ShortTag`.
    fn short_tag_of(&self, i: usize) -> String {
        if self.indicated_string(i) {
            return STR_TAG.to_string();
        }
        let n = self.node(i);
        if n.tag.is_empty() || n.tag == "!" {
            return match n.kind {
                Kind::Mapping => MAP_TAG.to_string(),
                Kind::Sequence => SEQ_TAG.to_string(),
                Kind::Alias => n.alias.map(|a| self.short_tag_of(a)).unwrap_or_default(),
                Kind::Scalar => resolve("", &n.value).map(|(t, _)| t).unwrap_or_default(),
                Kind::Document => String::new(),
            };
        }
        short_tag(&n.tag)
    }
}

struct Event {
    typ: unsafe_sys::yaml_event_type_t,
    anchor: Option<String>,
    tag: String,
    value: String,
    scalar_style: Option<unsafe_sys::yaml_scalar_style_t>,
    line: usize,
    column: usize,
    end_index: usize,
}

struct Parser {
    raw: *mut unsafe_sys::yaml_parser_t,
    event: Option<Event>,
    anchors: HashMap<String, usize>,
    nodes: Vec<Node>,
    _input: Vec<u8>,
}

impl Drop for Parser {
    fn drop(&mut self) {
        // SAFETY: `raw` was initialized by `yaml_parser_initialize` in `Parser::new` and is
        // deleted/freed exactly once here.
        unsafe {
            unsafe_sys::yaml_parser_delete(self.raw);
            drop(Box::from_raw(self.raw as *mut std::mem::MaybeUninit<unsafe_sys::yaml_parser_t>));
        }
    }
}

unsafe fn c_str(p: *const u8) -> String {
    if p.is_null() {
        return String::new();
    }
    // SAFETY: libyaml strings are NUL-terminated.
    unsafe { String::from_utf8_lossy(std::ffi::CStr::from_ptr(p as *const std::ffi::c_char).to_bytes()).into_owned() }
}

impl Parser {
    fn new(input: &[u8]) -> Result<Self, String> {
        let input = input.to_vec();
        let raw = Box::into_raw(Box::new(std::mem::MaybeUninit::<unsafe_sys::yaml_parser_t>::uninit()))
            as *mut unsafe_sys::yaml_parser_t;
        // SAFETY: `raw` points to writable memory for a parser; `input` outlives the parser.
        unsafe {
            if unsafe_sys::yaml_parser_initialize(raw).fail {
                drop(Box::from_raw(raw as *mut std::mem::MaybeUninit<unsafe_sys::yaml_parser_t>));
                return Err("failed to initialize YAML parser".to_string());
            }
            unsafe_sys::yaml_parser_set_input_string(raw, input.as_ptr(), input.len() as _);
        }
        Ok(Self {
            raw,
            event: None,
            anchors: HashMap::new(),
            nodes: Vec::new(),
            _input: input,
        })
    }

    /// `parser.fail`: `yaml: line N: <problem>`.
    fn fail(&self) -> String {
        // SAFETY: reading the public error prefix of an initialized parser.
        let (error, problem, problem_line, context_line) = unsafe {
            let p = &*self.raw;
            (p.error, c_str(p.problem as *const u8), p.problem_mark.line as usize, p.context_mark.line as usize)
        };
        let scanner = error == unsafe_sys::YAML_SCANNER_ERROR;
        let mut line = 0;
        if context_line != 0 {
            line = context_line + usize::from(scanner);
        } else if problem_line != 0 {
            line = problem_line + usize::from(scanner);
        }
        let where_ = if line != 0 { format!("line {line}: ") } else { String::new() };
        let msg = if problem.is_empty() {
            "unknown problem parsing YAML content".to_string()
        } else {
            problem
        };
        format!("yaml: {where_}{msg}")
    }

    fn peek(&mut self) -> Result<unsafe_sys::yaml_event_type_t, String> {
        if let Some(e) = &self.event {
            return Ok(e.typ);
        }
        let mut ev = std::mem::MaybeUninit::<unsafe_sys::yaml_event_t>::uninit();
        struct EventGuard(*mut unsafe_sys::yaml_event_t);
        impl Drop for EventGuard {
            fn drop(&mut self) {
                // SAFETY: `self.0` was initialized by a successful `yaml_parser_parse` call.
                unsafe { unsafe_sys::yaml_event_delete(self.0) };
            }
        }
        // SAFETY: parser is initialized; on success `EventGuard` deletes the event on return/unwind.
        unsafe {
            if unsafe_sys::yaml_parser_parse(self.raw, ev.as_mut_ptr()).fail
                || (&*self.raw).error != unsafe_sys::YAML_NO_ERROR
            {
                return Err(self.fail());
            }
            let ev = ev.as_mut_ptr();
            let _ev_guard = EventGuard(ev);
            let typ = (*ev).type_;
            let mut out = Event {
                typ,
                anchor: None,
                tag: String::new(),
                value: String::new(),
                scalar_style: None,
                line: (*ev).start_mark.line as usize + 1,
                column: (*ev).start_mark.column as usize + 1,
                end_index: (*ev).end_mark.index as usize,
            };
            match typ {
                unsafe_sys::YAML_ALIAS_EVENT => {
                    out.value = c_str((*ev).data.alias.anchor);
                }
                unsafe_sys::YAML_SCALAR_EVENT => {
                    let s = (*ev).data.scalar;
                    out.anchor = (!s.anchor.is_null()).then(|| c_str(s.anchor));
                    out.tag = c_str(s.tag);
                    out.value = if s.value.is_null() || s.length == 0 {
                        String::new()
                    } else {
                        let bytes = std::slice::from_raw_parts(s.value, s.length as usize);
                        String::from_utf8_lossy(bytes).into_owned()
                    };
                    out.scalar_style = Some(s.style);
                }
                unsafe_sys::YAML_SEQUENCE_START_EVENT => {
                    let s = (*ev).data.sequence_start;
                    out.anchor = (!s.anchor.is_null()).then(|| c_str(s.anchor));
                    out.tag = c_str(s.tag);
                }
                unsafe_sys::YAML_MAPPING_START_EVENT => {
                    let s = (*ev).data.mapping_start;
                    out.anchor = (!s.anchor.is_null()).then(|| c_str(s.anchor));
                    out.tag = c_str(s.tag);
                }
                _ => {}
            }
            self.event = Some(out);
            Ok(typ)
        }
    }

    fn expect(&mut self, typ: unsafe_sys::yaml_event_type_t) -> Result<Event, String> {
        self.peek()?;
        let ev = self
            .event
            .take()
            .ok_or_else(|| "yaml: internal error: no event".to_string())?;
        if ev.typ == unsafe_sys::YAML_STREAM_END_EVENT && typ != unsafe_sys::YAML_STREAM_END_EVENT {
            return Err("yaml: attempted to go past the end of stream; corrupted value?".to_string());
        }
        if ev.typ != typ {
            return Err(format!("yaml: expected {typ:?} event but got {:?}", ev.typ));
        }
        Ok(ev)
    }

    /// `parser.node`.
    fn node(&mut self, kind: Kind, default_tag: &str, tag: &str, value: String, line: usize, column: usize) -> usize {
        let mut style = 0;
        let tag = if !tag.is_empty() && tag != "!" {
            style = TAGGED_STYLE;
            short_tag(tag)
        } else if !default_tag.is_empty() {
            default_tag.to_string()
        } else if kind == Kind::Scalar {
            resolve("", &value).map(|(t, _)| t).unwrap_or_default()
        } else {
            tag.to_string()
        };
        self.nodes.push(Node { kind, style, tag, value, line, column, content: Vec::new(), alias: None });
        self.nodes.len() - 1
    }

    fn anchor(&mut self, n: usize, anchor: Option<String>) {
        if let Some(a) = anchor {
            self.anchors.insert(a, n);
        }
    }

    fn parse(&mut self) -> Result<Option<usize>, String> {
        match self.peek()? {
            unsafe_sys::YAML_SCALAR_EVENT => self.scalar().map(Some),
            unsafe_sys::YAML_ALIAS_EVENT => self.alias().map(Some),
            unsafe_sys::YAML_MAPPING_START_EVENT => self.mapping().map(Some),
            unsafe_sys::YAML_SEQUENCE_START_EVENT => self.sequence().map(Some),
            unsafe_sys::YAML_DOCUMENT_START_EVENT => self.document().map(Some),
            unsafe_sys::YAML_STREAM_END_EVENT => Ok(None),
            other => Err(format!("yaml: internal error: attempted to parse unknown event: {other:?}")),
        }
    }

    fn parse_child(&mut self, parent: usize) -> Result<usize, String> {
        let child = self
            .parse()?
            .ok_or_else(|| "yaml: attempted to go past the end of stream; corrupted value?".to_string())?;
        self.nodes[parent].content.push(child);
        Ok(child)
    }

    fn document(&mut self) -> Result<usize, String> {
        let (line, column) = self.event.as_ref().map(|e| (e.line, e.column)).unwrap_or((0, 0));
        let n = self.node(Kind::Document, "", "", String::new(), line, column);
        self.expect(unsafe_sys::YAML_DOCUMENT_START_EVENT)?;
        self.parse_child(n)?;
        Ok(n)
    }

    fn alias(&mut self) -> Result<usize, String> {
        let ev = self
            .event
            .as_ref()
            .ok_or_else(|| "yaml: internal error: no event".to_string())?;
        let (value, line, column) = (ev.value.clone(), ev.line, ev.column);
        let n = self.node(Kind::Alias, "", "", value.clone(), line, column);
        let target = *self
            .anchors
            .get(&value)
            .ok_or_else(|| format!("yaml: unknown anchor '{value}' referenced"))?;
        self.nodes[n].alias = Some(target);
        self.expect(unsafe_sys::YAML_ALIAS_EVENT)?;
        Ok(n)
    }

    fn scalar(&mut self) -> Result<usize, String> {
        let ev = self.expect(unsafe_sys::YAML_SCALAR_EVENT)?;
        let node_style = match ev.scalar_style {
            Some(unsafe_sys::YAML_DOUBLE_QUOTED_SCALAR_STYLE) => DOUBLE_QUOTED_STYLE,
            Some(unsafe_sys::YAML_SINGLE_QUOTED_SCALAR_STYLE) => SINGLE_QUOTED_STYLE,
            Some(unsafe_sys::YAML_LITERAL_SCALAR_STYLE) => LITERAL_STYLE,
            Some(unsafe_sys::YAML_FOLDED_SCALAR_STYLE) => FOLDED_STYLE,
            _ => 0,
        };
        let default_tag = if node_style == 0 {
            if ev.value == "<<" { MERGE_TAG } else { "" }
        } else {
            STR_TAG
        };
        let n = self.node(Kind::Scalar, default_tag, &ev.tag, ev.value, ev.line, ev.column);
        self.nodes[n].style |= node_style;
        self.anchor(n, ev.anchor);
        Ok(n)
    }

    fn sequence(&mut self) -> Result<usize, String> {
        let ev = self.expect(unsafe_sys::YAML_SEQUENCE_START_EVENT)?;
        let n = self.node(Kind::Sequence, SEQ_TAG, &ev.tag, String::new(), ev.line, ev.column);
        self.anchor(n, ev.anchor);
        while self.peek()? != unsafe_sys::YAML_SEQUENCE_END_EVENT {
            self.parse_child(n)?;
        }
        self.expect(unsafe_sys::YAML_SEQUENCE_END_EVENT)?;
        Ok(n)
    }

    fn mapping(&mut self) -> Result<usize, String> {
        let ev = self.expect(unsafe_sys::YAML_MAPPING_START_EVENT)?;
        let n = self.node(Kind::Mapping, MAP_TAG, &ev.tag, String::new(), ev.line, ev.column);
        self.anchor(n, ev.anchor);
        while self.peek()? != unsafe_sys::YAML_MAPPING_END_EVENT {
            self.parse_child(n)?;
            self.parse_child(n)?;
        }
        self.expect(unsafe_sys::YAML_MAPPING_END_EVENT)?;
        Ok(n)
    }
}

/// Parses the first document of `input` like `yaml.NewDecoder(r).Decode` does. Returns
/// `Ok(None)` for a stream without documents (Go: `io.EOF`), and the Go error text otherwise.
pub fn parse_first_document(input: &[u8]) -> Result<Option<Document>, String> {
    let mut p = Parser::new(input)?;
    p.expect(unsafe_sys::YAML_STREAM_START_EVENT)?;
    let Some(root) = p.parse()? else {
        return Ok(None);
    };
    let end = p.expect(unsafe_sys::YAML_DOCUMENT_END_EVENT)?;
    Ok(Some(Document {
        nodes: std::mem::take(&mut p.nodes),
        root,
        end_offset: end.end_index,
    }))
}

// ---------------------------------------------------------------------------------------------
// resolve.go
// ---------------------------------------------------------------------------------------------

fn short_tag(tag: &str) -> String {
    match tag.strip_prefix(LONG_TAG_PREFIX) {
        Some(rest) => format!("!!{rest}"),
        None => tag.to_string(),
    }
}

#[derive(Clone, Debug, PartialEq)]
enum Resolved {
    Null,
    Bool(bool),
    Int(i64),
    Uint(u64),
    Float(f64),
    Str(String),
    Timestamp,
    Merge,
}

fn parse_go_int_base0(s: &str) -> Option<i64> {
    crate::flags::go_parse_int(s).ok()
}

fn parse_go_uint_base0(s: &str) -> Option<u64> {
    crate::flags::go_parse_uint(s).ok()
}

/// Go `strconv.ParseFloat(s, 64)` acceptance (decimal/exponent, inf/nan spellings, hex floats
/// are not produced by the resolver paths that call this).
fn parse_go_float(s: &str) -> Option<f64> {
    let lower = s.to_ascii_lowercase();
    let unsigned = lower.trim_start_matches(['+', '-']);
    if matches!(unsigned, "inf" | "infinity" | "nan") {
        return s.parse().ok();
    }
    if !s.bytes().all(|b| b.is_ascii_digit() || matches!(b, b'+' | b'-' | b'.' | b'e' | b'E')) {
        return None;
    }
    s.parse().ok()
}

fn yaml_style_float(s: &str) -> bool {
    // ^[-+]?(\.[0-9]+|[0-9]+(\.[0-9]*)?)([eE][-+]?[0-9]+)?$
    let b = s.as_bytes();
    let mut i = 0;
    if i < b.len() && (b[i] == b'-' || b[i] == b'+') {
        i += 1;
    }
    let digits = |i: &mut usize| {
        let s = *i;
        while *i < b.len() && b[*i].is_ascii_digit() {
            *i += 1;
        }
        *i > s
    };
    if i < b.len() && b[i] == b'.' {
        i += 1;
        if !digits(&mut i) {
            return false;
        }
    } else {
        if !digits(&mut i) {
            return false;
        }
        if i < b.len() && b[i] == b'.' {
            i += 1;
            digits(&mut i);
        }
    }
    if i < b.len() && (b[i] == b'e' || b[i] == b'E') {
        i += 1;
        if i < b.len() && (b[i] == b'-' || b[i] == b'+') {
            i += 1;
        }
        if !digits(&mut i) {
            return false;
        }
    }
    i == b.len()
}

fn is_timestamp(s: &str) -> bool {
    // parseTimestamp: 4 digits then '-', then one of the allowed layouts.
    let b = s.as_bytes();
    let mut i = 0;
    while i < b.len() && b[i].is_ascii_digit() {
        i += 1;
    }
    if i != 4 || i == b.len() || b[i] != b'-' {
        return false;
    }
    let rest = &s[5..];
    let (date, time) = match rest.find(['T', 't', ' ']) {
        Some(p) => (&rest[..p], Some(&rest[p + 1..])),
        None => (rest, None),
    };
    let mut parts = date.split('-');
    let ok_num = |p: Option<&str>, max: u32| {
        p.is_some_and(|p| !p.is_empty() && p.len() <= 2 && p.parse::<u32>().is_ok_and(|v| (1..=max).contains(&v)))
    };
    if !ok_num(parts.next(), 12) || !ok_num(parts.next(), 31) || parts.next().is_some() {
        return false;
    }
    match time {
        None => true,
        Some(t) => {
            let t = t.trim_end_matches('Z');
            let hms: Vec<&str> = t.split(['+', '-']).next().unwrap_or("").split(':').collect();
            hms.len() == 3 && hms.iter().all(|x| !x.is_empty())
        }
    }
}

/// `resolve(tag, in)`. `Err` carries a decode failure (`failf`).
fn resolve(tag: &str, input: &str) -> Result<(String, Resolved), String> {
    let tag = short_tag(tag);
    if !matches!(tag.as_str(), "" | STR_TAG | BOOL_TAG | INT_TAG | FLOAT_TAG | NULL_TAG | TIMESTAMP_TAG) {
        return Ok((tag, Resolved::Str(input.to_string())));
    }
    let (rtag, out) = resolve_untyped(&tag, input);
    match tag.as_str() {
        "" | STR_TAG | BINARY_TAG => Ok((rtag, out)),
        t if t == rtag => Ok((rtag, out)),
        FLOAT_TAG if rtag == INT_TAG => {
            let f = match out {
                Resolved::Int(v) => v as f64,
                _ => return Err(format!("yaml: cannot decode {rtag} `{input}` as a {tag}")),
            };
            Ok((FLOAT_TAG.to_string(), Resolved::Float(f)))
        }
        _ => Err(format!("yaml: cannot decode {rtag} `{input}` as a {tag}")),
    }
}

fn resolve_untyped(tag: &str, input: &str) -> (String, Resolved) {
    let hint = match input.as_bytes().first() {
        None => b'N',
        Some(b'+' | b'-') => b'S',
        Some(b'0'..=b'9') => b'D',
        Some(b'y' | b'Y' | b'n' | b'N' | b't' | b'T' | b'f' | b'F' | b'o' | b'O' | b'~') => b'M',
        Some(b'.') => b'.',
        Some(_) => 0,
    };
    if hint != 0 && tag != STR_TAG && tag != BINARY_TAG {
        match input {
            "true" | "True" | "TRUE" => return (BOOL_TAG.into(), Resolved::Bool(true)),
            "false" | "False" | "FALSE" => return (BOOL_TAG.into(), Resolved::Bool(false)),
            "" | "~" | "null" | "Null" | "NULL" => return (NULL_TAG.into(), Resolved::Null),
            ".nan" | ".NaN" | ".NAN" => return (FLOAT_TAG.into(), Resolved::Float(f64::NAN)),
            ".inf" | ".Inf" | ".INF" | "+.inf" | "+.Inf" | "+.INF" => {
                return (FLOAT_TAG.into(), Resolved::Float(f64::INFINITY))
            }
            "-.inf" | "-.Inf" | "-.INF" => return (FLOAT_TAG.into(), Resolved::Float(f64::NEG_INFINITY)),
            "<<" => return (MERGE_TAG.into(), Resolved::Merge),
            _ => {}
        }
        match hint {
            b'.' => {
                if let Some(f) = parse_go_float(input) {
                    return (FLOAT_TAG.into(), Resolved::Float(f));
                }
            }
            b'D' | b'S' => {
                if (tag.is_empty() || tag == TIMESTAMP_TAG) && is_timestamp(input) {
                    return (TIMESTAMP_TAG.into(), Resolved::Timestamp);
                }
                let plain = input.replace('_', "");
                if let Some(v) = parse_go_int_base0(&plain) {
                    return (INT_TAG.into(), Resolved::Int(v));
                }
                if let Some(v) = parse_go_uint_base0(&plain) {
                    return (INT_TAG.into(), Resolved::Uint(v));
                }
                if yaml_style_float(&plain) {
                    if let Some(f) = parse_go_float(&plain) {
                        return (FLOAT_TAG.into(), Resolved::Float(f));
                    }
                }
                for (prefix, radix, neg) in [("0b", 2, false), ("-0b", 2, true), ("0o", 8, false), ("-0o", 8, true)] {
                    if let Some(d) = plain.strip_prefix(prefix) {
                        if let Ok(v) = i64::from_str_radix(&format!("{}{d}", if neg { "-" } else { "" }), radix) {
                            return (INT_TAG.into(), Resolved::Int(v));
                        }
                        if !neg {
                            if let Ok(v) = u64::from_str_radix(d, radix) {
                                return (INT_TAG.into(), Resolved::Uint(v));
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }
    (STR_TAG.into(), Resolved::Str(input.to_string()))
}

// ---------------------------------------------------------------------------------------------
// Go type graph (yaml_types.json).
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Debug, Deserialize)]
struct FieldInfo {
    key: String,
    #[serde(rename = "type")]
    ty: usize,
    /// `yaml:",omitempty"`.
    #[serde(default)]
    omitempty: bool,
}

#[derive(Clone, Debug, Deserialize)]
struct TypeInfo {
    name: String,
    kind: String,
    #[serde(default)]
    bits: u32,
    #[serde(default)]
    unmarshaler: Option<String>,
    #[serde(default)]
    elem: Option<usize>,
    #[serde(default)]
    key: Option<usize>,
    #[serde(default)]
    fields: Vec<FieldInfo>,
    #[serde(default)]
    inline_map: Option<usize>,
    #[serde(default)]
    delegate: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TypeDb {
    root: usize,
    types: Vec<TypeInfo>,
}

struct Types {
    types: Vec<TypeInfo>,
    root: usize,
    string: usize,
    string_slice: usize,
    string_map: usize,
    iface: usize,
    config_plain: usize,
    common_pass: usize,
    storage_backend: usize,
    cluster_validation: usize,
    runtime_config_values: usize,
}

impl Types {
    fn add(&mut self, t: TypeInfo) -> usize {
        self.types.push(t);
        self.types.len() - 1
    }

    fn find(&self, name: &str) -> Result<usize, String> {
        self.types
            .iter()
            .position(|t| t.name == name)
            .ok_or_else(|| format!("yaml_types.json lacks {name}"))
    }

    fn get(&self, i: usize) -> &TypeInfo {
        &self.types[i]
    }
}

fn plain(name: &str, kind: &str) -> TypeInfo {
    TypeInfo {
        name: name.to_string(),
        kind: kind.to_string(),
        bits: 0,
        unmarshaler: None,
        elem: None,
        key: None,
        fields: Vec::new(),
        inline_map: None,
        delegate: None,
    }
}

/// The embedded type graph (an error here is a build defect, reported instead of panicking).
fn types() -> Result<&'static Types, String> {
    static TYPES: OnceLock<Result<Types, String>> = OnceLock::new();
    TYPES.get_or_init(load_types).as_ref().map_err(Clone::clone)
}

fn load_types() -> Result<Types, String> {
    {
        let db: TypeDb = serde_json::from_str(include_str!("yaml_types.json"))
            .map_err(|e| format!("invalid embedded yaml_types.json: {e}"))?;
        let mut t = Types {
            types: db.types,
            root: db.root,
            string: 0,
            string_slice: 0,
            string_map: 0,
            iface: 0,
            config_plain: 0,
            common_pass: 0,
            storage_backend: 0,
            cluster_validation: 0,
            runtime_config_values: 0,
        };
        t.string = t.add(plain("string", "string"));
        t.string_slice = t.add(TypeInfo { elem: Some(t.string), ..plain("[]string", "slice") });
        t.string_map = t.add(TypeInfo { key: Some(t.string), elem: Some(t.string), ..plain("map[string]string", "map") });
        t.iface = t.add(plain("interface {}", "interface"));
        t.config_plain = t.add(plain("config.plain", "string"));
        t.storage_backend = t.find("bucket.StorageBackendConfig")?;
        t.cluster_validation = t.find("clusterutil.ClusterValidationConfig")?;
        // pkg/mimir/mimir.go: configWithCustomCommonUnmarshaler{Common *commonConfigUnmarshaler;
        // Throwaway map[string]interface{} `yaml:",inline"`}.
        // The two specificLocationsUnmarshaler fields decode into different location types, so
        // each gets its own synthetic type.
        let spec_storage = t.add(TypeInfo {
            unmarshaler: Some("specific-locations-storage".into()),
            ..plain("mimir.specificLocationsUnmarshaler", "map")
        });
        let spec_storage_ptr = t.add(TypeInfo { elem: Some(spec_storage), ..plain("*mimir.specificLocationsUnmarshaler", "ptr") });
        let spec_cv = t.add(TypeInfo {
            unmarshaler: Some("specific-locations-cluster-validation".into()),
            ..plain("mimir.specificLocationsUnmarshaler", "map")
        });
        let spec_cv_ptr = t.add(TypeInfo { elem: Some(spec_cv), ..plain("*mimir.specificLocationsUnmarshaler", "ptr") });
        let common = t.add(TypeInfo {
            fields: vec![
                FieldInfo { key: "storage".into(), ty: spec_storage_ptr, omitempty: false },
                FieldInfo { key: "client_cluster_validation".into(), ty: spec_cv_ptr, omitempty: false },
            ],
            ..plain("mimir.commonConfigUnmarshaler", "struct")
        });
        let common_ptr = t.add(TypeInfo { elem: Some(common), ..plain("*mimir.commonConfigUnmarshaler", "ptr") });
        t.common_pass = t.add(TypeInfo {
            fields: vec![FieldInfo { key: "common".into(), ty: common_ptr, omitempty: false }],
            inline_map: Some(t.iface),
            ..plain("mimir.configWithCustomCommonUnmarshaler", "struct")
        });

        // pkg/mimir/runtime_config.go: runtimeConfigValues
        let bool_ty = t.find("bool")?;
        let bool_ptr = t.add(TypeInfo { elem: Some(bool_ty), ..plain("*bool", "ptr") });
        let multi_rt = t.add(TypeInfo {
            fields: vec![
                FieldInfo { key: "primary".into(), ty: t.string, omitempty: false },
                FieldInfo { key: "mirror_enabled".into(), ty: bool_ptr, omitempty: false },
            ],
            ..plain("kv.MultiRuntimeConfig", "struct")
        });
        let limits_ty = t.find("validation.Limits")?;
        let limits_ptr = t.add(TypeInfo { elem: Some(limits_ty), ..plain("*validation.Limits", "ptr") });
        let overrides_map = t.add(TypeInfo {
            key: Some(t.string),
            elem: Some(limits_ptr),
            ..plain("map[string]*validation.Limits", "map")
        });
        let ing_limits = t.find("ingester.InstanceLimits")?;
        let ing_limits_ptr = t.add(TypeInfo { elem: Some(ing_limits), ..plain("*ingester.InstanceLimits", "ptr") });
        let dist_limits = t.find("distributor.InstanceLimits")?;
        let dist_limits_ptr = t.add(TypeInfo { elem: Some(dist_limits), ..plain("*distributor.InstanceLimits", "ptr") });
        t.runtime_config_values = t.add(TypeInfo {
            fields: vec![
                FieldInfo { key: "overrides".into(), ty: overrides_map, omitempty: false },
                FieldInfo { key: "multi_kv_config".into(), ty: multi_rt, omitempty: false },
                FieldInfo { key: "ingester_limits".into(), ty: ing_limits_ptr, omitempty: false },
                FieldInfo { key: "distributor_limits".into(), ty: dist_limits_ptr, omitempty: false },
            ],
            ..plain("mimir.runtimeConfigValues", "struct")
        });
        Ok(t)
    }
}

// ---------------------------------------------------------------------------------------------
// decoder (decode.go)
// ---------------------------------------------------------------------------------------------

/// A decode outcome that aborts the whole `Decode` (`fail(err)` / `failf`).
#[derive(Debug)]
struct Fail(String);

struct Decoder<'a> {
    doc: &'a Document,
    types: &'static Types,
    terrors: Vec<String>,
    known_fields: bool,
    aliases: HashSet<usize>,
    merge_visited: HashSet<usize>,
    merged_fields: Option<HashSet<String>>,
}

/// `TypeError.Error()`.
fn type_error(errors: &[String]) -> String {
    format!("yaml: unmarshal errors:\n  {}", errors.join("\n  "))
}

impl<'a> Decoder<'a> {
    fn new(doc: &'a Document, types: &'static Types, known_fields: bool) -> Self {
        Self {
            doc,
            types,
            terrors: Vec::new(),
            known_fields,
            aliases: HashSet::new(),
            merge_visited: HashSet::new(),
            merged_fields: None,
        }
    }

    /// `node.DecodeWithOptions(out, opts)` / `node.Decode(out)` from inside an unmarshaler:
    /// a fresh decoder whose `TypeError` is merged into ours by `callUnmarshaler`.
    fn sub_decode(&mut self, n: usize, ty: usize, known_fields: bool) -> Result<bool, Fail> {
        let mut sub = Decoder::new(self.doc, self.types, known_fields);
        sub.unmarshal(n, ty)?;
        let good = sub.terrors.is_empty();
        self.terrors.append(&mut sub.terrors);
        Ok(good)
    }

    fn terror(&mut self, n: usize, tag: &str, ty: usize) {
        let node = self.doc.node(n);
        let tag = if node.tag.is_empty() { tag.to_string() } else { node.tag.clone() };
        let mut value = String::new();
        if tag != SEQ_TAG && tag != MAP_TAG {
            let v = node.value.as_bytes();
            value = if v.len() > 10 {
                format!(" `{}...`", String::from_utf8_lossy(&v[..7]))
            } else {
                format!(" `{}`", node.value)
            };
        }
        let msg = format!(
            "line {}: cannot unmarshal {}{} into {}",
            node.line,
            short_tag(&tag),
            value,
            self.types.get(ty).name
        );
        self.terrors.push(msg);
    }

    fn unmarshal(&mut self, n: usize, ty: usize) -> Result<bool, Fail> {
        let node = self.doc.node(n);
        match node.kind {
            Kind::Document => {
                if node.content.len() == 1 {
                    self.unmarshal(node.content[0], ty)?;
                    return Ok(true);
                }
                return Ok(false);
            }
            Kind::Alias => {
                if !self.aliases.insert(n) {
                    return Err(Fail(format!("yaml: anchor '{}' value contains itself", node.value)));
                }
                let target = node.alias.expect("resolved alias");
                let good = self.unmarshal(target, ty);
                self.aliases.remove(&n);
                return good;
            }
            _ => {}
        }
        // prepare
        let mut ty = ty;
        if self.doc.short_tag_of(n) != NULL_TAG {
            loop {
                let t = self.types.get(ty);
                if t.kind == "ptr" {
                    ty = t.elem.expect("ptr elem");
                    continue;
                }
                break;
            }
            if let Some(u) = self.types.get(ty).unmarshaler.clone() {
                if u != "text" {
                    return self.custom(n, ty, &u);
                }
            }
        }
        match node.kind {
            Kind::Scalar => self.scalar(n, ty),
            Kind::Mapping => self.mapping(n, ty, None),
            Kind::Sequence => self.sequence(n, ty),
            _ => Ok(false),
        }
    }

    /// Custom `UnmarshalYAML` implementations reachable from `mimir.Config` (see the
    /// `unmarshaler`/`delegate` entries of yaml_types.json).
    fn custom(&mut self, n: usize, ty: usize, kind: &str) -> Result<bool, Fail> {
        let t = self.types.get(ty);
        let name = t.name.as_str();
        if let Some(which) = kind.strip_prefix("specific-locations-") {
            // specificLocationsUnmarshaler: DecodeWithOptions(location, KnownFields) for each
            // location; the first error is returned as `key "<location>": <err>`. Go iterates
            // a map, so which location name is reported is random; one of them is used here.
            let (loc_ty, loc) = if which == "cluster-validation" {
                (self.types.cluster_validation, "ingester_client")
            } else {
                (self.types.storage_backend, "blocks_storage")
            };
            let mut sub = Decoder::new(self.doc, self.types, true);
            sub.unmarshal(n, loc_ty)?;
            if !sub.terrors.is_empty() {
                return Err(Fail(format!("key {loc:?}: {}", type_error(&sub.terrors))));
            }
            return Ok(true);
        }
        if let Some(delegate) = t.delegate.clone() {
            // `type plain T; DecodeWithOptions((*plain)(x), KnownFields: true)` / unmarshal((*plain)(x)).
            let known = kind == "obsolete" && self.known_fields || kind == "node";
            if kind == "node" {
                let mut sub = Decoder::new(self.doc, self.types, known);
                let good = sub.mapping_or_other_named(n, ty, &delegate)?;
                self.terrors.append(&mut sub.terrors);
                return Ok(good);
            }
            return self.mapping_or_other_named(n, ty, &delegate);
        }
        match name {
            // unmarshal(&s) with s string, then a custom parser.
            "flagext.StringSliceCSV" | "flagext.Secret" | "log.Level"
            | "flagext.CIDRSliceCSV" | "flagext.URLValue" | "model.ValidationScheme"
            | "s3.BucketLookupType" | "model.LabelName" | "relabel.Action" | "relabel.Regexp" => {
                self.unmarshal(n, self.types.string)
            }
            "flagext.Bytes" => {
                let before = self.terrors.len();
                self.unmarshal(n, self.types.string)?;
                if self.terrors.len() > before {
                    return Ok(false);
                }
                let node = self.doc.node(self.deref_alias(n));
                if node.kind == Kind::Scalar && self.doc.short_tag_of(n) != NULL_TAG {
                    crate::types::HumanBytes::parse_str(&node.value).map_err(Fail)?;
                }
                Ok(true)
            }
            "model.Duration" => {
                let before = self.terrors.len();
                self.unmarshal(n, self.types.string)?;
                if self.terrors.len() > before {
                    return Ok(false);
                }
                let node = self.doc.node(self.deref_alias(n));
                if node.kind == Kind::Scalar && self.doc.short_tag_of(n) != NULL_TAG {
                    crate::duration::parse_model_duration(&node.value).map_err(|e| Fail(e.to_string()))?;
                }
                Ok(true)
            }
            // value.Decode(&repr) with repr string (new decoder).
            "validation.LabelValueLengthOverLimitStrategy" | "validation.OTelTranslationStrategyValue" => {
                self.sub_decode(n, self.types.string, false)
            }
            // prometheus/common/config.Secret: `type plain Secret; unmarshal((*plain)(s))`.
            "config.Secret" => self.unmarshal(n, self.types.config_plain),
            "flagext.StringSliceCSVMulti" => {
                let before = self.terrors.len();
                self.unmarshal(n, self.types.string)?;
                if self.terrors.len() == before {
                    return Ok(true);
                }
                self.terrors.truncate(before);
                self.unmarshal(n, self.types.string_slice)
            }
            "activeseriesmodel.CustomTrackersConfig" => self.sub_decode(n, self.types.string_map, true),
            _ if name.starts_with("flagext.LimitsMap[") => {
                // `value.Decode(newMap)` into map[string]T (new decoder, no KnownFields).
                let elem = &name["flagext.LimitsMap[".len()..name.len() - 1];
                let elem_ty = self
                    .types
                    .types
                    .iter()
                    .position(|t| t.name == elem)
                    .unwrap_or(self.types.string);
                let map_name = format!("map[string]{elem}");
                let mut sub = Decoder::new(self.doc, self.types, false);
                let good = sub.mapping_typed(n, &map_name, self.types.string, elem_ty)?;
                self.terrors.append(&mut sub.terrors);
                Ok(good)
            }
            // Unknown custom unmarshaler: its errors are not emulated.
            _ => Ok(true),
        }
    }

    fn deref_alias(&self, mut n: usize) -> usize {
        for _ in 0..self.doc.nodes.len() {
            let Some(a) = self.doc.node(n).alias else {
                break;
            };
            n = a;
        }
        n
    }

    /// Decodes into the struct form of `ty` but reports `name` as its Go type (`type plain T`).
    fn mapping_or_other_named(&mut self, n: usize, ty: usize, name: &str) -> Result<bool, Fail> {
        let node = self.doc.node(self.deref_alias(n));
        if self.doc.short_tag_of(n) == NULL_TAG {
            return Ok(false);
        }
        match node.kind {
            Kind::Mapping => self.mapping(self.deref_alias(n), ty, Some(name)),
            Kind::Sequence => {
                self.terror_named(n, SEQ_TAG, name);
                Ok(false)
            }
            _ => {
                let tag = self.doc.short_tag_of(n);
                self.terror_named(n, &tag, name);
                Ok(false)
            }
        }
    }

    fn terror_named(&mut self, n: usize, tag: &str, name: &str) {
        let node = self.doc.node(n);
        let tag = if node.tag.is_empty() { tag.to_string() } else { node.tag.clone() };
        let mut value = String::new();
        if tag != SEQ_TAG && tag != MAP_TAG {
            let v = node.value.as_bytes();
            value = if v.len() > 10 {
                format!(" `{}...`", String::from_utf8_lossy(&v[..7]))
            } else {
                format!(" `{}`", node.value)
            };
        }
        self.terrors
            .push(format!("line {}: cannot unmarshal {}{} into {}", node.line, short_tag(&tag), value, name));
    }

    fn scalar(&mut self, n: usize, ty: usize) -> Result<bool, Fail> {
        let node = self.doc.node(n);
        let (tag, resolved) = if self.doc.indicated_string(n) {
            (STR_TAG.to_string(), Resolved::Str(node.value.clone()))
        } else {
            let (tag, r) = resolve(&node.tag, &node.value).map_err(Fail)?;
            if tag == BINARY_TAG {
                use std::io::Read as _;
                let _ = std::io::empty().read(&mut []);
                if !is_base64(&node.value) {
                    return Err(Fail("yaml: !!binary value contains invalid base64 data".to_string()));
                }
            }
            (tag, r)
        };
        if resolved == Resolved::Null {
            return Ok(false);
        }
        let t = self.types.get(ty);
        if t.unmarshaler.as_deref() == Some("text") {
            // TextUnmarshaler: its own parse errors are not emulated.
            return Ok(true);
        }
        let bits = if t.bits == 0 { 64 } else { t.bits };
        let ok = match t.kind.as_str() {
            "string" | "interface" => true,
            "int" | "int8" | "int16" | "int32" | "int64" => {
                let is_duration = t.name == "time.Duration";
                let fits = |v: i128| {
                    let min = -(1i128 << (bits - 1));
                    let max = (1i128 << (bits - 1)) - 1;
                    v >= min && v <= max
                };
                match &resolved {
                    Resolved::Int(v) => (!is_duration && fits(*v as i128)) || (is_duration && *v == 0),
                    Resolved::Uint(v) => !is_duration && *v <= i64::MAX as u64 && fits(*v as i128),
                    Resolved::Float(f) => {
                        !is_duration
                            && !f.is_nan()
                            && *f >= i64::MIN as f64
                            && *f <= i64::MAX as f64
                            && fits((*f as i64) as i128)
                    }
                    Resolved::Str(s) => is_duration && crate::duration::parse_go_duration(s).is_ok(),
                    _ => false,
                }
            }
            "uint" | "uint8" | "uint16" | "uint32" | "uint64" | "uintptr" => {
                let max: u128 = (1u128 << bits) - 1;
                match &resolved {
                    Resolved::Int(v) => *v >= 0 && (*v as u128) <= max,
                    Resolved::Uint(v) => (*v as u128) <= max,
                    Resolved::Float(f) => {
                        !f.is_nan() && *f >= 0.0 && *f <= u64::MAX as f64 && ((*f as u64) as u128) <= max
                    }
                    _ => false,
                }
            }
            "bool" => match &resolved {
                Resolved::Bool(_) => true,
                Resolved::Str(s) => matches!(
                    s.as_str(),
                    "y" | "Y" | "yes" | "Yes" | "YES" | "on" | "On" | "ON" | "n" | "N" | "no" | "No" | "NO"
                        | "off" | "Off" | "OFF"
                ),
                _ => false,
            },
            "float32" | "float64" => matches!(
                resolved,
                Resolved::Int(_) | Resolved::Uint(_) | Resolved::Float(_)
            ),
            "struct" => t.name == "time.Time" && resolved == Resolved::Timestamp,
            _ => false,
        };
        if !ok {
            self.terror(n, &tag, ty);
        }
        Ok(ok)
    }

    fn sequence(&mut self, n: usize, ty: usize) -> Result<bool, Fail> {
        let t = self.types.get(ty);
        let elem = match t.kind.as_str() {
            "slice" | "array" => t.elem.expect("slice elem"),
            "interface" => self.types.iface,
            _ => {
                self.terror(n, SEQ_TAG, ty);
                return Ok(false);
            }
        };
        for c in self.doc.node(n).content.clone() {
            self.unmarshal(c, elem)?;
        }
        Ok(true)
    }

    /// `checkUniqueKeys` (pairwise scan; the map-based variant yields the same errors).
    fn check_unique_keys(&mut self, n: usize) -> bool {
        let before = self.terrors.len();
        let content = &self.doc.node(n).content;
        let mut i = 0;
        while i < content.len() {
            let ni = self.doc.node(content[i]);
            let mut j = i + 2;
            while j < content.len() {
                let nj = self.doc.node(content[j]);
                if ni.kind == nj.kind && ni.value == nj.value {
                    self.terrors.push(format!(
                        "line {}: mapping key {:?} already defined at line {}",
                        nj.line, nj.value, ni.line
                    ));
                }
                j += 2;
            }
            i += 2;
        }
        self.terrors.len() == before
    }

    fn is_merge(&self, k: usize) -> bool {
        let n = self.doc.node(k);
        n.kind == Kind::Scalar && n.value == "<<" && (n.tag.is_empty() || n.tag == "!" || short_tag(&n.tag) == MERGE_TAG)
    }

    fn mapping(&mut self, n: usize, ty: usize, struct_name: Option<&str>) -> Result<bool, Fail> {
        if !self.check_unique_keys(n) {
            return Ok(false);
        }
        let t = self.types.get(ty);
        match t.kind.as_str() {
            "struct" => {
                let name = struct_name.unwrap_or(&t.name).to_string();
                self.mapping_struct(n, ty, &name)
            }
            "map" => {
                let (k, e) = (t.key.expect("map key"), t.elem.expect("map elem"));
                self.mapping_entries(n, k, e)
            }
            "interface" => self.mapping_entries(n, self.types.iface, self.types.iface),
            _ => {
                self.terror(n, MAP_TAG, ty);
                Ok(false)
            }
        }
    }

    /// Mapping decode into a synthetic `map[K]E` type named `map_name`.
    fn mapping_typed(&mut self, n: usize, map_name: &str, key: usize, elem: usize) -> Result<bool, Fail> {
        let node = self.doc.node(self.deref_alias(n));
        if self.doc.short_tag_of(n) == NULL_TAG {
            return Ok(false);
        }
        if node.kind != Kind::Mapping {
            let tag = self.doc.short_tag_of(n);
            self.terror_named(n, &tag, map_name);
            return Ok(false);
        }
        let n = self.deref_alias(n);
        if !self.check_unique_keys(n) {
            return Ok(false);
        }
        self.mapping_entries(n, key, elem)
    }

    fn mapping_entries(&mut self, n: usize, key: usize, elem: usize) -> Result<bool, Fail> {
        let merged = self.merged_fields.take();
        let mut merge_node = None;
        let content = self.doc.node(n).content.clone();
        for pair in content.chunks(2) {
            let (k, v) = (pair[0], pair[1]);
            if self.is_merge(k) {
                merge_node = Some(v);
                continue;
            }
            if self.unmarshal(k, key)? {
                if let Some(m) = &merged {
                    let kv = self.doc.node(self.deref_alias(k)).value.clone();
                    if m.contains(&kv) {
                        continue;
                    }
                }
                self.unmarshal(v, elem)?;
            }
        }
        self.merged_fields = merged;
        if let Some(m) = merge_node {
            self.merge(n, m, None, key, elem)?;
        }
        Ok(true)
    }

    fn mapping_struct(&mut self, n: usize, ty: usize, name: &str) -> Result<bool, Fail> {
        let t = self.types.get(ty);
        let inline_map = t.inline_map;
        let merged = self.merged_fields.take();
        let mut merge_node = None;
        let mut done: HashSet<usize> = HashSet::new();
        let content = self.doc.node(n).content.clone();
        for pair in content.chunks(2) {
            let (k, v) = (pair[0], pair[1]);
            if self.is_merge(k) {
                merge_node = Some(v);
                continue;
            }
            if !self.unmarshal(k, self.types.string)? {
                continue;
            }
            let kn = self.doc.node(self.deref_alias(k));
            let sname = kn.value.clone();
            let line = self.doc.node(k).line;
            if let Some(m) = &merged {
                if m.contains(&sname) {
                    continue;
                }
            }
            let fields = &self.types.get(ty).fields;
            if let Some(idx) = fields.iter().position(|f| f.key == sname) {
                if !done.insert(idx) {
                    self.terrors.push(format!("line {line}: field {sname} already set in type {name}"));
                    continue;
                }
                let fty = fields[idx].ty;
                self.unmarshal(v, fty)?;
            } else if let Some(e) = inline_map {
                self.unmarshal(v, e)?;
            } else if self.known_fields {
                self.terrors.push(format!("line {line}: field {sname} not found in type {name}"));
            }
        }
        self.merged_fields = merged;
        if let Some(m) = merge_node {
            self.merge(n, m, Some((ty, name.to_string())), 0, 0)?;
        }
        Ok(true)
    }

    fn merge(
        &mut self,
        parent: usize,
        merge: usize,
        strukt: Option<(usize, String)>,
        key: usize,
        elem: usize,
    ) -> Result<(), Fail> {
        let saved = self.merged_fields.clone();
        if self.merged_fields.is_none() {
            let mut set = HashSet::new();
            for pair in self.doc.node(parent).content.chunks(2) {
                set.insert(self.doc.node(self.deref_alias(pair[0])).value.clone());
            }
            self.merged_fields = Some(set);
        }
        let want_map = || Fail("yaml: map merge requires map or sequence of maps as the value".to_string());
        let mut targets = Vec::new();
        let m = self.doc.node(merge);
        match m.kind {
            Kind::Mapping => targets.push(merge),
            Kind::Alias => {
                let a = self.deref_alias(merge);
                if self.doc.node(a).kind != Kind::Mapping {
                    return Err(want_map());
                }
                targets.push(merge);
            }
            Kind::Sequence => {
                for &c in &m.content {
                    let cn = self.doc.node(c);
                    let ok = match cn.kind {
                        Kind::Alias => self.doc.node(self.deref_alias(c)).kind == Kind::Mapping,
                        Kind::Mapping => true,
                        _ => false,
                    };
                    if !ok {
                        return Err(want_map());
                    }
                    targets.push(c);
                }
            }
            _ => return Err(want_map()),
        }
        let inserted_parent = self.merge_visited.insert(parent);
        for tgt in targets {
            let is_alias = self.doc.node(tgt).kind == Kind::Alias;
            if is_alias && !self.aliases.insert(tgt) {
                if inserted_parent {
                    self.merge_visited.remove(&parent);
                }
                self.merged_fields = saved;
                return Err(Fail(format!(
                    "yaml: anchor '{}' value contains itself",
                    self.doc.node(tgt).value
                )));
            }
            let tn = self.deref_alias(tgt);
            if self.merge_visited.contains(&tn) {
                if is_alias {
                    self.aliases.remove(&tgt);
                }
                if inserted_parent {
                    self.merge_visited.remove(&parent);
                }
                self.merged_fields = saved;
                let msg = if is_alias {
                    format!(
                        "yaml: anchor '{}' value contains itself",
                        self.doc.node(tgt).value
                    )
                } else {
                    "yaml: cyclic merge".to_string()
                };
                return Err(Fail(msg));
            }
            let res = match &strukt {
                Some((ty, name)) => {
                    if self.check_unique_keys(tn) {
                        self.mapping_struct(tn, *ty, name)
                    } else {
                        Ok(true)
                    }
                }
                None => {
                    if self.check_unique_keys(tn) {
                        self.mapping_entries(tn, key, elem)
                    } else {
                        Ok(true)
                    }
                }
            };
            if is_alias {
                self.aliases.remove(&tgt);
            }
            res?;
        }
        if inserted_parent {
            self.merge_visited.remove(&parent);
        }
        self.merged_fields = saved;
        Ok(())
    }
}

fn is_base64(s: &str) -> bool {
    let clean: Vec<u8> = s.bytes().filter(|b| !b.is_ascii_whitespace()).collect();
    clean.len().is_multiple_of(4)
        && clean
            .iter()
            .enumerate()
            .all(|(i, b)| b.is_ascii_alphanumeric() || *b == b'+' || *b == b'/' || (*b == b'=' && i >= clean.len() - 2))
}

/// Reproduces `LoadConfig`'s decode of `(*mimir.ConfigWithCommon)(cfg)` with
/// `KnownFields(true)` and returns the Go error text (without the `error parsing config file: `
/// prefix) if Go would reject the document. `Ok(None)` means: stream has no document (Go
/// `io.EOF`); `Ok(Some(doc))` means Go accepts it.
/// Reshapes the YAML of a Go `mimir.Config` (as produced by serializing `MimirConfig`) the way
/// yaml.v3 `Marshal` emits the Go value: struct fields in declaration order, keys the Go struct
/// does not have dropped, zero `omitempty` fields omitted, Go maps (and structs that marshal as
/// maps, e.g. `flagext.LimitsMap`) sorted by yaml.v3's key order. Used to render `/config`.
pub fn order_like_go_config(value: &mut serde_yaml::Value) -> Result<(), String> {
    let t = types()?;
    order_value(t, t.root, value)
}

/// [`order_like_go_config`] for a value of the named Go type of the `mimir.Config` type graph
/// (e.g. `validation.Limits` for the per-tenant overrides of `/runtime_config`).
pub fn order_like_go_type(type_name: &str, value: &mut serde_yaml::Value) -> Result<(), String> {
    let t = types()?;
    order_value(t, t.find(type_name)?, value)
}

/// yaml.v3 `isZero` of the Go value rendered as `value` (type-directed: Go durations render as
/// `0s`, structs are zero when every field is).
fn is_zero(t: &Types, ty: usize, value: &serde_yaml::Value) -> bool {
    use serde_yaml::Value;
    let info = t.get(ty);
    if info.kind == "ptr" {
        // A non-nil pointer is never zero.
        return value.is_null();
    }
    if info.name == "relabel.Regexp" {
        // Go `relabel.Regexp.IsZero()`: `re.Regexp == DefaultRelabelConfig.Regex.Regexp`,
        // so only the default `"(.*)"` is omitted on marshal; explicit `""` is emitted.
        return match value {
            Value::String(s) => s == "(.*)",
            Value::Tagged(tagged) => is_zero(t, ty, &tagged.value),
            _ => false,
        };
    }
    match value {
        Value::Null => true,
        Value::Bool(b) => !*b,
        Value::Number(n) => n.as_f64() == Some(0.0),
        Value::String(s) => {
            s.is_empty() || (s == "0s" && matches!(info.name.as_str(), "time.Duration" | "model.Duration"))
        }
        Value::Sequence(items) => items.is_empty(),
        Value::Mapping(m) => match info.kind.as_str() {
            "struct" if !info.fields.is_empty() => info.fields.iter().all(|f| {
                m.get(f.key.as_str()).is_none_or(|v| is_zero(t, f.ty, v))
            }),
            _ => m.is_empty(),
        },
        Value::Tagged(tagged) => is_zero(t, ty, &tagged.value),
    }
}

fn order_value(t: &Types, ty: usize, value: &mut serde_yaml::Value) -> Result<(), String> {
    use serde_yaml::Value;
    let info = t.get(ty);
    // Go types with a `MarshalYAML` that changes the shape of the value.
    match (info.name.as_str(), &*value) {
        // `strings.Join(v, ",")`.
        ("flagext.StringSliceCSV" | "flagext.StringSliceCSVMulti" | "flagext.CIDRSliceCSV", Value::Sequence(items)) => {
            let joined: Vec<&str> = items.iter().filter_map(Value::as_str).collect();
            if joined.len() == items.len() {
                *value = Value::String(joined.join(","));
            }
            return Ok(());
        }
        // `units.Base2Bytes(b).String()`.
        ("flagext.Bytes", Value::Number(n)) => {
            if let Some(n) = n.as_u64() {
                *value = Value::String(crate::types::HumanBytes(n).to_string());
            }
            return Ok(());
        }
        _ => {}
    }
    match (info.kind.as_str(), value) {
        ("ptr", v) => match info.elem {
            Some(elem) => order_value(t, elem, v),
            None => Ok(()),
        },
        ("slice", Value::Sequence(items)) => match info.elem {
            Some(elem) => items.iter_mut().try_for_each(|item| order_value(t, elem, item)),
            None => Ok(()),
        },
        ("struct", Value::Mapping(m)) if !info.fields.is_empty() => {
            let mut rest = std::mem::take(m);
            for field in &info.fields {
                if let Some(mut v) = rest.remove(field.key.as_str()) {
                    if field.omitempty && is_zero(t, field.ty, &v) {
                        continue;
                    }
                    order_value(t, field.ty, &mut v)?;
                    m.insert(Value::String(field.key.clone()), v);
                }
            }
            Ok(())
        }
        ("map", Value::Mapping(m)) | ("struct", Value::Mapping(m)) => {
            crate::yaml_v3_emit::sort_mapping_like_go(m)?;
            if let (Some(elem), "map") = (info.elem, info.kind.as_str()) {
                for (_, v) in m.iter_mut() {
                    order_value(t, elem, v)?;
                }
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn is_yaml11_true(s: &str) -> bool {
    matches!(s, "y" | "Y" | "yes" | "Yes" | "YES" | "on" | "On" | "ON")
}

fn is_yaml11_bool(s: &str) -> bool {
    matches!(
        s,
        "y" | "Y" | "yes" | "Yes" | "YES" | "on" | "On" | "ON" | "n" | "N" | "no" | "No" | "NO"
            | "off" | "Off" | "OFF"
    )
}

fn is_string_unmarshaler_type(name: &str) -> bool {
    matches!(
        name,
        "flagext.StringSliceCSV"
            | "flagext.Secret"
            | "config.Secret"
            | "log.Level"
            | "flagext.Bytes"
            | "flagext.CIDRSliceCSV"
            | "flagext.URLValue"
            | "model.ValidationScheme"
            | "s3.BucketLookupType"
            | "model.LabelName"
            | "relabel.Action"
            | "relabel.Regexp"
            | "model.Duration"
            | "validation.LabelValueLengthOverLimitStrategy"
            | "validation.OTelTranslationStrategyValue"
    )
}

fn deref_ptr(t: &Types, mut ty: usize) -> usize {
    while t.get(ty).kind == "ptr" {
        match t.get(ty).elem {
            Some(e) => ty = e,
            None => break,
        }
    }
    ty
}

fn limits_map_elem_ty(t: &Types, name: &str) -> Option<usize> {
    let elem = name.strip_prefix("flagext.LimitsMap[")?.strip_suffix(']')?;
    t.find(elem).ok()
}

fn doc_deref(doc: &Document, mut n: usize) -> usize {
    for _ in 0..doc.nodes.len() {
        let Some(a) = doc.node(n).alias else {
            break;
        };
        n = a;
    }
    n
}

fn doc_is_merge(doc: &Document, k: usize) -> bool {
    let n = doc.node(doc_deref(doc, k));
    n.kind == Kind::Scalar
        && n.value == "<<"
        && (n.tag.is_empty() || n.tag == "!" || short_tag(&n.tag) == MERGE_TAG)
}

fn doc_scalar_to_yaml(doc: &Document, t: &Types, n: usize, ty: usize) -> Result<serde_yaml::Value, String> {
    use serde_yaml::Value;
    let n = doc_deref(doc, n);
    let node = doc.node(n);
    let ty = deref_ptr(t, ty);
    let info = t.get(ty);
    let indicated = doc.indicated_string(n);
    let (_tag, resolved) = if indicated {
        (STR_TAG.to_string(), Resolved::Str(node.value.clone()))
    } else {
        resolve(&node.tag, &node.value)?
    };
    if resolved == Resolved::Null {
        return Ok(Value::Null);
    }
    if info.unmarshaler.as_deref() == Some("text") || is_string_unmarshaler_type(&info.name) {
        return Ok(Value::String(node.value.clone()));
    }
    if info.name == "flagext.StringSliceCSVMulti" {
        return Ok(Value::String(node.value.clone()));
    }
    if info.name == "time.Duration" {
        return match resolved {
            Resolved::Int(0) => Ok(Value::String("0s".to_string())),
            Resolved::Str(s) => Ok(Value::String(s)),
            _ => Ok(Value::String(node.value.clone())),
        };
    }
    if info.name == "time.Time" {
        return Ok(Value::String(node.value.clone()));
    }
    match info.kind.as_str() {
        "string" => Ok(Value::String(node.value.clone())),
        "bool" => match resolved {
            Resolved::Bool(b) => Ok(Value::Bool(b)),
            Resolved::Str(ref s) if is_yaml11_bool(s) => Ok(Value::Bool(is_yaml11_true(s))),
            _ => Ok(Value::String(node.value.clone())),
        },
        "int" | "int8" | "int16" | "int32" | "int64" => match resolved {
            Resolved::Int(v) => Ok(Value::Number(v.into())),
            Resolved::Uint(v) => Ok(Value::Number(v.into())),
            Resolved::Float(f) if !f.is_nan() && f >= i64::MIN as f64 && f <= i64::MAX as f64 => {
                Ok(Value::Number((f as i64).into()))
            }
            _ => Ok(Value::String(node.value.clone())),
        },
        "uint" | "uint8" | "uint16" | "uint32" | "uint64" | "uintptr" => match resolved {
            Resolved::Int(v) if v >= 0 => Ok(Value::Number((v as u64).into())),
            Resolved::Uint(v) => Ok(Value::Number(v.into())),
            Resolved::Float(f) if !f.is_nan() && f >= 0.0 && f <= u64::MAX as f64 => {
                Ok(Value::Number((f as u64).into()))
            }
            _ => Ok(Value::String(node.value.clone())),
        },
        "float32" | "float64" => match resolved {
            Resolved::Int(v) => Ok(Value::Number(v.into())),
            Resolved::Uint(v) => Ok(Value::Number(v.into())),
            Resolved::Float(f) => Ok(Value::Number(serde_yaml::Number::from(f))),
            _ => Ok(Value::String(node.value.clone())),
        },
        "interface" => match resolved {
            Resolved::Null => Ok(Value::Null),
            Resolved::Bool(b) => Ok(Value::Bool(b)),
            Resolved::Int(v) => Ok(Value::Number(v.into())),
            Resolved::Uint(v) => Ok(Value::Number(v.into())),
            Resolved::Float(f) => Ok(Value::Number(serde_yaml::Number::from(f))),
            Resolved::Str(_) | Resolved::Timestamp | Resolved::Merge => {
                Ok(Value::String(node.value.clone()))
            }
        },
        _ => Ok(Value::String(node.value.clone())),
    }
}

fn collect_merged_pairs(
    doc: &Document,
    n: usize,
    visited: &mut HashSet<usize>,
) -> Result<Vec<(usize, usize)>, String> {
    let n = doc_deref(doc, n);
    if !visited.insert(n) {
        return Err("yaml: anchor value contains itself".to_string());
    }
    let content = &doc.node(n).content;
    let mut explicit_keys = Vec::new();
    let mut explicit_set = HashSet::new();
    let mut merge_val = None;
    for pair in content.chunks(2) {
        let (k, v) = (pair[0], pair[1]);
        if doc_is_merge(doc, k) {
            merge_val = Some(v);
        } else {
            let kn = doc.node(doc_deref(doc, k));
            if kn.kind == Kind::Scalar {
                explicit_set.insert(kn.value.clone());
            }
            explicit_keys.push((k, v));
        }
    }
    if let Some(mv) = merge_val {
        let mv = doc_deref(doc, mv);
        let mnode = doc.node(mv);
        let mut targets = Vec::new();
        match mnode.kind {
            Kind::Mapping => targets.push(mv),
            Kind::Sequence => {
                for &c in &mnode.content {
                    let cn = doc_deref(doc, c);
                    if doc.node(cn).kind == Kind::Mapping {
                        targets.push(cn);
                    }
                }
            }
            _ => {}
        }
        for tgt in targets {
            let merged = match collect_merged_pairs(doc, tgt, visited) {
                Ok(m) => m,
                Err(e) => {
                    visited.remove(&n);
                    return Err(e);
                }
            };
            for (mk, mv) in merged {
                let mkn = doc.node(doc_deref(doc, mk));
                if mkn.kind == Kind::Scalar && !explicit_set.insert(mkn.value.clone()) {
                    continue;
                }
                explicit_keys.push((mk, mv));
            }
        }
    }
    visited.remove(&n);
    Ok(explicit_keys)
}

fn doc_node_to_yaml(doc: &Document, t: &Types, n: usize, ty: usize) -> Result<serde_yaml::Value, String> {
    use serde_yaml::{Mapping, Value};
    let n = doc_deref(doc, n);
    let node = doc.node(n);
    if node.kind == Kind::Document {
        return match node.content.first() {
            Some(&c) => doc_node_to_yaml(doc, t, c, ty),
            None => Ok(Value::Null),
        };
    }
    if doc.short_tag_of(n) == NULL_TAG {
        return Ok(Value::Null);
    }
    let ty = deref_ptr(t, ty);
    let info = t.get(ty);
    match node.kind {
        Kind::Scalar => doc_scalar_to_yaml(doc, t, n, ty),
        Kind::Sequence => {
            let elem_ty = match info.kind.as_str() {
                "slice" | "array" => info.elem.unwrap_or(t.iface),
                _ => t.iface,
            };
            let mut seq = Vec::with_capacity(node.content.len());
            for &c in &node.content {
                seq.push(doc_node_to_yaml(doc, t, c, elem_ty)?);
            }
            Ok(Value::Sequence(seq))
        }
        Kind::Mapping => {
            let mut merge_visited = HashSet::new();
            let pairs = collect_merged_pairs(doc, n, &mut merge_visited)?;
            let mut out = Mapping::with_capacity(pairs.len());
            if info.name == "activeseriesmodel.CustomTrackersConfig" {
                for (k, v) in pairs {
                    let kv = doc_node_to_yaml(doc, t, k, t.string)?;
                    let vv = doc_node_to_yaml(doc, t, v, t.string)?;
                    out.insert(kv, vv);
                }
            } else if let Some(elem_ty) = limits_map_elem_ty(t, &info.name) {
                for (k, v) in pairs {
                    let kv = doc_node_to_yaml(doc, t, k, t.string)?;
                    let vv = doc_node_to_yaml(doc, t, v, elem_ty)?;
                    out.insert(kv, vv);
                }
            } else if info.kind == "struct" {
                for (k, v) in pairs {
                    let kv = doc_node_to_yaml(doc, t, k, t.string)?;
                    let field_ty = kv
                        .as_str()
                        .and_then(|key| info.fields.iter().find(|f| f.key == key).map(|f| f.ty))
                        .or(info.inline_map)
                        .unwrap_or(t.iface);
                    let vv = doc_node_to_yaml(doc, t, v, field_ty)?;
                    out.insert(kv, vv);
                }
            } else if info.kind == "map" {
                let key_ty = info.key.unwrap_or(t.string);
                let elem_ty = info.elem.unwrap_or(t.iface);
                for (k, v) in pairs {
                    let kv = doc_node_to_yaml(doc, t, k, key_ty)?;
                    let vv = doc_node_to_yaml(doc, t, v, elem_ty)?;
                    out.insert(kv, vv);
                }
            } else {
                for (k, v) in pairs {
                    let kv = doc_node_to_yaml(doc, t, k, t.iface)?;
                    let vv = doc_node_to_yaml(doc, t, v, t.iface)?;
                    out.insert(kv, vv);
                }
            }
            Ok(Value::Mapping(out))
        }
        _ => Ok(Value::Null),
    }
}

/// Converts a validated `mimir.Config` YAML [`Document`] into a [`serde_yaml::Value`] with Go
/// `yaml.v3` scalar coercions applied according to the Go type graph.
pub fn doc_to_serde_value(doc: &Document) -> Result<serde_yaml::Value, String> {
    let t = types()?;
    doc_node_to_yaml(doc, t, doc.root, t.root)
}

fn normalize_serde_scalar(t: &Types, val: &mut serde_yaml::Value, ty: usize) {
    use serde_yaml::Value;
    let ty = deref_ptr(t, ty);
    let info = t.get(ty);
    if val.is_null() {
        return;
    }
    let want_string = info.unmarshaler.as_deref() == Some("text")
        || is_string_unmarshaler_type(&info.name)
        || info.name == "flagext.StringSliceCSVMulti"
        || info.name == "time.Time"
        || (info.kind == "string");
    if want_string {
        match val {
            Value::Number(n) => *val = Value::String(n.to_string()),
            Value::Bool(b) => *val = Value::String(b.to_string()),
            _ => {}
        }
        return;
    }
    if info.name == "time.Duration" {
        if let Value::Number(n) = val {
            if n.as_i64() == Some(0) || n.as_u64() == Some(0) {
                *val = Value::String("0s".to_string());
            }
        }
        return;
    }
    match info.kind.as_str() {
        "bool" => {
            if let Value::String(s) = val {
                if is_yaml11_bool(s) {
                    *val = Value::Bool(is_yaml11_true(s));
                }
            }
        }
        "int" | "int8" | "int16" | "int32" | "int64" => {
            if let Value::Number(n) = val {
                if !n.is_i64() && !n.is_u64() {
                    if let Some(f) = n.as_f64() {
                        if !f.is_nan() && f >= i64::MIN as f64 && f <= i64::MAX as f64 {
                            *val = Value::Number((f as i64).into());
                        }
                    }
                }
            }
        }
        "uint" | "uint8" | "uint16" | "uint32" | "uint64" | "uintptr" => {
            if let Value::Number(n) = val {
                if !n.is_u64() {
                    if let Some(f) = n.as_f64() {
                        if !f.is_nan() && f >= 0.0 && f <= u64::MAX as f64 {
                            *val = Value::Number((f as u64).into());
                        }
                    }
                }
            }
        }
        _ => {}
    }
}

fn normalize_serde_value(t: &Types, val: &mut serde_yaml::Value, ty: usize) {
    use serde_yaml::Value;
    let ty = deref_ptr(t, ty);
    let info = t.get(ty);
    match val {
        Value::Null => {}
        Value::Sequence(items) => {
            let elem_ty = match info.kind.as_str() {
                "slice" | "array" => info.elem.unwrap_or(t.iface),
                _ => t.iface,
            };
            for item in items {
                normalize_serde_value(t, item, elem_ty);
            }
        }
        Value::Mapping(m) => {
            let taken = std::mem::take(m);
            if info.name == "activeseriesmodel.CustomTrackersConfig" {
                for (mut k, mut v) in taken {
                    normalize_serde_scalar(t, &mut k, t.string);
                    normalize_serde_value(t, &mut v, t.string);
                    m.insert(k, v);
                }
            } else if let Some(elem_ty) = limits_map_elem_ty(t, &info.name) {
                for (mut k, mut v) in taken {
                    normalize_serde_scalar(t, &mut k, t.string);
                    normalize_serde_value(t, &mut v, elem_ty);
                    m.insert(k, v);
                }
            } else if info.kind == "struct" {
                for (mut k, mut v) in taken {
                    normalize_serde_scalar(t, &mut k, t.string);
                    let field_ty = k
                        .as_str()
                        .and_then(|key| info.fields.iter().find(|f| f.key == key).map(|f| f.ty))
                        .or(info.inline_map)
                        .unwrap_or(t.iface);
                    normalize_serde_value(t, &mut v, field_ty);
                    m.insert(k, v);
                }
            } else if info.kind == "map" {
                let key_ty = info.key.unwrap_or(t.string);
                let elem_ty = info.elem.unwrap_or(t.iface);
                for (mut k, mut v) in taken {
                    normalize_serde_scalar(t, &mut k, key_ty);
                    normalize_serde_value(t, &mut v, elem_ty);
                    m.insert(k, v);
                }
            } else {
                for (k, v) in taken {
                    m.insert(k, v);
                }
            }
        }
        _ => normalize_serde_scalar(t, val, ty),
    }
}

/// Normalizes an already-parsed [`serde_yaml::Value`] (e.g. merged `runtime_config` maps)
/// according to Go `yaml.v3` scalar coercions for the named Go type (such as
/// `"mimir.runtimeConfigValues"` or `"mimir.Config"`).
fn zero_null_fields_walk(t: &Types, val: &mut serde_yaml::Value, ty: usize, preserve_cost_tracker_nulls: bool) {
    use serde_yaml::{Mapping, Value};
    let ty_deref = deref_ptr(t, ty);
    let info = t.get(ty_deref);
    match val {
        Value::Sequence(items) => {
            if let Some(elem_ty) = info.elem {
                for item in items {
                    zero_null_fields_walk(t, item, elem_ty, preserve_cost_tracker_nulls);
                }
            }
        }
        Value::Mapping(map) => {
            if info.kind == "struct"
                && !info.name.starts_with("flagext.LimitsMap[")
                && info.name != "activeseriesmodel.CustomTrackersConfig"
            {
                let keys: Vec<Value> = map.keys().cloned().collect();
                for k in keys {
                    let field_ty = k
                        .as_str()
                        .and_then(|key| info.fields.iter().find(|f| f.key == key).map(|f| f.ty));
                    let Some(v) = map.get_mut(&k) else {
                        continue;
                    };
                    if v.is_null() {
                        match field_ty.map(|ft| t.get(ft)) {
                            Some(fi)
                                if fi.name == "flagext.StringSliceCSV"
                                    || fi.name == "flagext.CIDRSliceCSV" =>
                            {
                                *v = Value::String(String::new());
                            }
                            Some(fi)
                                if fi.name == "flagext.StringSliceCSVMulti"
                                    || fi.kind == "slice" =>
                            {
                                *v = Value::Sequence(Vec::new());
                            }
                            Some(fi)
                                if preserve_cost_tracker_nulls
                                    && fi.name == "costattributionmodel.TrackerConfigs" =>
                            {
                                // Keep explicit null so runtime_config overlay clears base map
                                // (distinguishing `null` from `{}` overlay).
                            }
                            Some(fi)
                                if fi.kind == "map"
                                    || fi.name == "activeseriesmodel.CustomTrackersConfig"
                                    || fi.name.starts_with("flagext.LimitsMap[") =>
                            {
                                *v = Value::Mapping(Mapping::new());
                            }
                            Some(fi) if fi.name == "relabel.Regexp" => {
                                // Go `relabel.Config.UnmarshalYAML`: explicit `regex: null` maps to `""`.
                                *v = Value::String(String::new());
                            }
                            Some(fi) if fi.kind == "ptr" => {
                                // Keep explicit YAML null so Option<T> zeroes to None.
                            }
                            _ => {
                                map.remove(&k);
                            }
                        }
                    } else if let Some(ft) = field_ty {
                        zero_null_fields_walk(t, v, ft, preserve_cost_tracker_nulls);
                    }
                }
            } else {
                let elem_ty = info
                    .elem
                    .or_else(|| limits_map_elem_ty(t, &info.name));
                if let Some(elem) = elem_ty {
                    for (_, v) in map.iter_mut() {
                        if !v.is_null() {
                            zero_null_fields_walk(t, v, elem, preserve_cost_tracker_nulls);
                        }
                    }
                }
            }
        }
        _ => {}
    }
}

/// Applies Go `yaml.v3` `decoder.null` semantics onto a `mimir.Config` YAML value before merging
/// onto flag defaults: strips `null` on scalars and structs (leaving defaults untouched), zeroes
/// slices/CSVs (`[]` / `""`), zeroes maps (`{}`), and preserves `null` on pointer fields (`*bool`).
pub(crate) fn zero_null_fields_like_yaml_v3(core_val: &mut serde_yaml::Value) {
    if let Ok(t) = types() {
        zero_null_fields_walk(t, core_val, t.root, false);
    }
}

pub fn normalize_value_for_type(type_name: &str, val: &mut serde_yaml::Value) -> Result<(), String> {
    let t = types()?;
    let ty = t.find(type_name)?;
    normalize_serde_value(t, val, ty);
    zero_null_fields_walk(t, val, ty, true);
    Ok(())
}

pub fn check_config_document(input: &[u8]) -> Result<Option<Document>, String> {
    let Some(doc) = parse_first_document(input)? else {
        return Err("EOF".to_string());
    };
    let t = types()?;

    // Pass 1: configWithCustomCommonUnmarshaler (strict `common:`, everything else into a map).
    {
        let mut d = Decoder::new(&doc, t, true);
        let result = d.unmarshal(doc.root, t.common_pass).map(|_| ());
        let pass1 = match result {
            Err(Fail(msg)) => Some(msg),
            Ok(()) if !d.terrors.is_empty() => Some(type_error(&d.terrors)),
            Ok(()) => None,
        };
        if let Some(msg) = pass1 {
            return Err(format!("can't unmarshal common config: {msg}"));
        }
    }

    // Pass 2: value.DecodeWithOptions((*Config)(c), KnownFields: true).
    let mut d = Decoder::new(&doc, t, true);
    match d.unmarshal(doc.root, t.root) {
        Err(Fail(msg)) => Err(msg),
        Ok(_) if !d.terrors.is_empty() => Err(type_error(&d.terrors)),
        Ok(_) => Ok(Some(doc)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(yaml: &str) -> Result<(), String> {
        check_config_document(yaml.as_bytes()).map(|_| ())
    }

    #[test]
    fn go_error_texts() {
        let cases: &[(&str, &str)] = &[
            ("", "EOF"),
            ("# only a comment\n", "EOF"),
            (
                "ingester:\n  ring:\n    bogus: 1\n    num_tokens: abc\n    heartbeat_period: 5\n",
                "yaml: unmarshal errors:\n  line 3: field bogus not found in type ingester.RingConfig\n  line 4: cannot unmarshal !!str `abc` into int\n  line 5: cannot unmarshal !!int `5` into time.Duration",
            ),
            ("foo: 1\n", "yaml: unmarshal errors:\n  line 1: field foo not found in type mimir.Config"),
            (
                "common:\n  storage:\n    bogus: 1\n",
                "can't unmarshal common config: key \"blocks_storage\": yaml: unmarshal errors:\n  line 3: field bogus not found in type bucket.StorageBackendConfig",
            ),
            (
                "limits:\n  bogus: 1\n  max_global_series_per_user: x\n",
                "yaml: unmarshal errors:\n  line 2: field bogus not found in type validation.plainLimits\n  line 3: cannot unmarshal !!str `x` into int",
            ),
            ("ingester:\n  ring:\n    kvstore: [1]\n", "yaml: unmarshal errors:\n  line 3: cannot unmarshal !!seq into kv.Config"),
            ("target: [a]\n", "yaml: unmarshal errors:\n  line 1: cannot unmarshal !!seq into string"),
            ("a: b: c\n", "yaml: mapping values are not allowed in this context"),
            ("x: \"unterminated\n", "yaml: line 2: found unexpected end of stream"),
            ("x: *nope\n", "yaml: unknown anchor 'nope' referenced"),
            ("ingester:\n\tring: {}\n", "yaml: line 2: found character that cannot start any token"),
            (
                "limits:\n  ingestion_rate: 1\n  ingestion_rate: 2\n",
                "can't unmarshal common config: yaml: unmarshal errors:\n  line 3: mapping key \"ingestion_rate\" already defined at line 2",
            ),
            (
                "ingester:\n  ring:\n    heartbeat_period: 5x\n",
                "yaml: unmarshal errors:\n  line 3: cannot unmarshal !!str `5x` into time.Duration",
            ),
            (
                "server:\n  http_listen_port: a-very-long-value\n",
                "yaml: unmarshal errors:\n  line 2: cannot unmarshal !!str `a-very-...` into int",
            ),
        ];
        for (yaml, want) in cases {
            assert_eq!(check(yaml).unwrap_err(), *want, "yaml: {yaml:?}");
        }
    }

    #[test]
    fn accepts_valid_configs() {
        for yaml in [
            "target: ingester\n",
            "target: all\n---\nfoo: [\n",
            "querier:\nlimits:\n",
            "ingester:\n  ring:\n    replication_factor: 3\n    heartbeat_period: 5s\n    excluded_zones: a,b\n",
            "limits:\n  max_global_series_per_user: 1e3\n  ingestion_rate: 10\n  ingestion_burst_size: 0x10\n",
            "server:\n  log_level: info\n  grpc_server_max_recv_msg_size: 104_857_600\n",
            "common:\n  storage:\n    backend: s3\n    s3:\n      region: x\n",
            "x: &a {backend: s3}\nblocks_storage:\n  <<: *a\n",
            "blocks_storage:\n  tsdb:\n    retention_period: 0\n    block_ranges_period: [2h]\n",
            "usage_tracker:\n  enabled: false\nblock_builder: {}\n",
            "limits:\n  active_series_custom_trackers:\n    foo: '{a=\"b\"}'\n",
        ] {
            if yaml.starts_with("x:") {
                // `x` is not a mimir.Config field.
                continue;
            }
            assert_eq!(check(yaml), Ok(()), "yaml: {yaml:?}");
        }
    }

    #[test]
    fn resolve_matches_yaml_v3() {
        assert_eq!(resolve("", "0x1F").unwrap().1, Resolved::Int(31));
        assert_eq!(resolve("", "0o17").unwrap().1, Resolved::Int(15));
        assert_eq!(resolve("", "1_000").unwrap().1, Resolved::Int(1000));
        assert_eq!(resolve("", "1.5").unwrap().1, Resolved::Float(1.5));
        assert_eq!(resolve("", "yes").unwrap().1, Resolved::Str("yes".into()));
        assert_eq!(resolve("", "~").unwrap().1, Resolved::Null);
        assert_eq!(resolve("", "2001-12-14").unwrap().1, Resolved::Timestamp);
        assert_eq!(resolve("", "18446744073709551615").unwrap().1, Resolved::Uint(u64::MAX));
        assert!(resolve("!!int", "abc").is_err());
    }

    #[test]
    fn cyclic_yaml_merge_aliases_return_error_without_stack_overflow() {
        for yaml in [
            "blocks_storage: &a { <<: *a }\n",
            "blocks_storage: &a\n  <<: *a\n",
            "blocks_storage: &a\n  <<: [*a]\n",
            "blocks_storage: &a { <<: { <<: *a } }\n",
            "common:\n  storage: &a\n    <<: *a\n",
        ] {
            let err = check(yaml).unwrap_err();
            assert!(
                err.contains("contains itself") || err.contains("cyclic merge"),
                "unexpected error for {yaml:?}: {err}"
            );
            let doc = parse_first_document(yaml.as_bytes()).unwrap().unwrap();
            assert!(
                doc_to_serde_value(&doc).is_err(),
                "expected doc_to_serde_value error for {yaml:?}"
            );
        }

        // Diamond / non-cyclic merge must continue to succeed.
        let ok_yaml = "common:\n  storage: &s\n    backend: s3\nblocks_storage:\n  <<: *s\n";
        assert_eq!(check(ok_yaml), Ok(()));
        let doc = parse_first_document(ok_yaml.as_bytes()).unwrap().unwrap();
        assert!(doc_to_serde_value(&doc).is_ok());
    }
}
