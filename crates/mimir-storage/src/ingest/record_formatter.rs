//! Record formatting and parsing (`RecordFormatter` and `RecordReader`).
//!
//! Ported from github.com/twmb/franz-go/pkg/kgo/record_formatter.go

use std::cell::Cell;
use std::io::{self, BufRead, BufReader, Read};
use std::rc::Rc;
use std::sync::atomic::{AtomicI64, Ordering};

use base64::Engine;
use chrono::{DateTime, Utc};
use regex::Regex;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FormattedRecordHeader {
    pub key: String,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RecordAttrs(pub u8);

impl RecordAttrs {
    pub fn compression_type(self) -> u8 {
        self.0 & 0x07
    }
    pub fn timestamp_type(self) -> i8 {
        if (self.0 & 0x80) != 0 {
            -1
        } else if (self.0 & 0x08) != 0 {
            1
        } else {
            0
        }
    }
    pub fn is_transactional(self) -> bool {
        (self.0 & 0x10) != 0
    }
    pub fn is_control(self) -> bool {
        (self.0 & 0x20) != 0
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FormattedRecord {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub headers: Vec<FormattedRecordHeader>,
    pub timestamp_ms: i64,
    pub topic: String,
    pub partition: i32,
    pub attrs: RecordAttrs,
    pub producer_epoch: i16,
    pub producer_id: i64,
    pub leader_epoch: i32,
    pub offset: i64,
    pub delivery_count: i32,
    pub acquisition_deadline_ms: Option<i64>,
}

impl FormattedRecord {
    pub fn string(s: &str) -> Self {
        Self {
            value: s.as_bytes().to_vec(),
            ..Default::default()
        }
    }
    pub fn key_string(k: &str, v: &str) -> Self {
        Self {
            key: k.as_bytes().to_vec(),
            value: v.as_bytes().to_vec(),
            ..Default::default()
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct FormattedFetchPartition {
    pub high_watermark: i64,
    pub last_stable_offset: i64,
    pub log_start_offset: i64,
}

type FormatStep = Box<dyn Fn(&mut Vec<u8>, Option<&FormattedFetchPartition>, Option<&FormattedRecord>) + Send + Sync>;

pub struct RecordFormatter {
    fns: Vec<FormatStep>,
}

impl RecordFormatter {
    pub fn new(layout: &str) -> Result<Self, String> {
        let calls = std::sync::Arc::new(AtomicI64::new(0));
        let fns = compile_format_layout(layout, &calls)?;
        Ok(Self { fns })
    }

    pub fn append_record(&self, dst: &mut Vec<u8>, r: Option<&FormattedRecord>) {
        for f in &self.fns {
            f(dst, None, r);
        }
    }

    pub fn append_partition_record(
        &self,
        dst: &mut Vec<u8>,
        p: Option<&FormattedFetchPartition>,
        r: Option<&FormattedRecord>,
    ) {
        for f in &self.fns {
            f(dst, p, r);
        }
    }
}

fn parse_layout_slash(layout: &str) -> Result<(u8, usize), String> {
    let Some(&b) = layout.as_bytes().first() else {
        return Err("invalid slash escape at end of delim string".to_string());
    };
    match b {
        b't' => Ok((b'\t', 1)),
        b'n' => Ok((b'\n', 1)),
        b'r' => Ok((b'\r', 1)),
        b'\\' => Ok((b'\\', 1)),
        b'x' => {
            if layout.len() < 3 {
                return Err("invalid non-terminated hex escape sequence at end of delim string".to_string());
            }
            let hx = &layout[1..3];
            let v = u8::from_str_radix(hx, 16)
                .map_err(|e| format!("unable to parse hex escape sequence {hx:?}: {e}"))?;
            Ok((v, 3))
        }
        other => Err(format!("unknown slash escape sequence {:?}", other as char)),
    }
}

fn nom_open_close(src: &str) -> Result<(&str, &str), String> {
    let bytes = src.as_bytes();
    if bytes.is_empty() {
        return Err("empty layout".to_string());
    }
    let open = bytes[0];
    let mut openers = 1usize;
    while openers < bytes.len() && bytes[openers] == open {
        openers += 1;
    }
    let close = match open {
        b'{' => b'}',
        b'[' => b']',
        b'(' => b')',
        other => other,
    };
    let rest = &src[openers..];
    let end_delim: String = std::iter::repeat_n(close as char, openers).collect();
    let idx = rest
        .find(&end_delim)
        .ok_or_else(|| format!("missing end delim {end_delim:?}"))?;
    Ok((&rest[..idx], &rest[idx + end_delim.len()..]))
}

type NumWriteFn = fn(&mut Vec<u8>, i64);

fn parse_num_write_layout(layout: &str) -> Result<(NumWriteFn, usize), String> {
    let brace_end = layout
        .find('}')
        .ok_or_else(|| "missing brace end } to close number format specification".to_string())?;
    let end = brace_end + 1;
    let spec = &layout[..brace_end];
    let f: fn(&mut Vec<u8>, i64) = match spec {
        "ascii" | "number" => |b, n| b.extend_from_slice(n.to_string().as_bytes()),
        "hex64" => |b, n| b.extend_from_slice(format!("{:016x}", n as u64).as_bytes()),
        "hex32" => |b, n| b.extend_from_slice(format!("{:08x}", n as u32).as_bytes()),
        "hex16" => |b, n| b.extend_from_slice(format!("{:04x}", n as u16).as_bytes()),
        "hex8" => |b, n| b.extend_from_slice(format!("{:02x}", n as u8).as_bytes()),
        "hex4" => |b, n| b.extend_from_slice(format!("{:x}", (n as u64) & 0xf).as_bytes()),
        "hex" => |b, n| b.extend_from_slice(format!("{:x}", n as u64).as_bytes()),
        "big64" => |b, n| b.extend_from_slice(&(n as u64).to_be_bytes()),
        "big32" => |b, n| b.extend_from_slice(&(n as u32).to_be_bytes()),
        "big16" => |b, n| b.extend_from_slice(&(n as u16).to_be_bytes()),
        "byte" | "big8" | "little8" => |b, n| b.push(n as u8),
        "little64" => |b, n| b.extend_from_slice(&(n as u64).to_le_bytes()),
        "little32" => |b, n| b.extend_from_slice(&(n as u32).to_le_bytes()),
        "little16" => |b, n| b.extend_from_slice(&(n as u16).to_le_bytes()),
        "bool" => |b, n| b.extend_from_slice(if n == 0 { b"false" } else { b"true" }),
        other => return Err(format!("invalid output number layout {other:?}")),
    };
    Ok((f, end))
}

type BytesAppendFn = Box<dyn Fn(&mut Vec<u8>, &[u8]) + Send + Sync>;

fn parse_unpack(layout: &str) -> Result<BytesAppendFn, String> {
    enum Op {
        Signed(usize, bool),
        Unsigned(usize, bool),
        Char,
        Rest,
        End,
    }
    let mut ops = Vec::new();
    let mut little = true;
    let mut saw_end = false;
    for ch in layout.chars() {
        if saw_end {
            return Err("already saw end-of-input parsing character".to_string());
        }
        match ch {
            'x' => continue,
            '<' => little = true,
            '>' => little = false,
            'b' => ops.push(Op::Signed(1, little)),
            'B' => ops.push(Op::Unsigned(1, little)),
            'h' => ops.push(Op::Signed(2, little)),
            'H' => ops.push(Op::Unsigned(2, little)),
            'i' => ops.push(Op::Signed(4, little)),
            'I' => ops.push(Op::Unsigned(4, little)),
            'q' => ops.push(Op::Signed(8, little)),
            'Q' => ops.push(Op::Unsigned(8, little)),
            'c' | '.' => ops.push(Op::Char),
            's' => {
                saw_end = true;
                ops.push(Op::Rest);
            }
            '$' => {
                saw_end = true;
                ops.push(Op::End);
            }
            other => return Err(format!("invalid unpack parsing character {other}")),
        }
    }
    Ok(Box::new(move |dst, mut src| {
        for op in &ops {
            match *op {
                Op::Char => {
                    if src.is_empty() {
                        dst.extend_from_slice(b"%!c(no bytes available)");
                    } else {
                        dst.push(src[0]);
                        src = &src[1..];
                    }
                }
                Op::Rest => {
                    dst.extend_from_slice(src);
                    src = &[];
                }
                Op::End => {
                    if !src.is_empty() {
                        dst.extend_from_slice(b"%!$(not end-of-input)");
                    }
                    src = &[];
                }
                Op::Signed(need, is_little) => {
                    if src.len() < need {
                        dst.extend_from_slice(
                            format!("%!(have {} bytes, need {need})", src.len()).as_bytes(),
                        );
                        src = &[];
                        continue;
                    }
                    let val: i64 = match (need, is_little) {
                        (1, _) => (src[0] as i8) as i64,
                        (2, true) => i16::from_le_bytes([src[0], src[1]]) as i64,
                        (2, false) => i16::from_be_bytes([src[0], src[1]]) as i64,
                        (4, true) => i32::from_le_bytes([src[0], src[1], src[2], src[3]]) as i64,
                        (4, false) => i32::from_be_bytes([src[0], src[1], src[2], src[3]]) as i64,
                        (8, true) => i64::from_le_bytes(src[..8].try_into().unwrap()),
                        (8, false) => i64::from_be_bytes(src[..8].try_into().unwrap()),
                        _ => 0,
                    };
                    dst.extend_from_slice(val.to_string().as_bytes());
                    src = &src[need..];
                }
                Op::Unsigned(need, is_little) => {
                    if src.len() < need {
                        dst.extend_from_slice(
                            format!("%!(have {} bytes, need {need})", src.len()).as_bytes(),
                        );
                        src = &[];
                        continue;
                    }
                    let val: u64 = match (need, is_little) {
                        (1, _) => src[0] as u64,
                        (2, true) => u16::from_le_bytes([src[0], src[1]]) as u64,
                        (2, false) => u16::from_be_bytes([src[0], src[1]]) as u64,
                        (4, true) => u32::from_le_bytes([src[0], src[1], src[2], src[3]]) as u64,
                        (4, false) => u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as u64,
                        (8, true) => u64::from_le_bytes(src[..8].try_into().unwrap()),
                        (8, false) => u64::from_be_bytes(src[..8].try_into().unwrap()),
                        _ => 0,
                    };
                    dst.extend_from_slice(val.to_string().as_bytes());
                    src = &src[need..];
                }
            }
        }
    }))
}

fn compile_format_layout(
    mut layout: &str,
    calls: &std::sync::Arc<AtomicI64>,
) -> Result<Vec<FormatStep>, String> {
    let mut fns: Vec<FormatStep> = Vec::new();
    let mut literal: Vec<u8> = Vec::new();

    while !layout.is_empty() {
        let ch = layout.chars().next().unwrap();
        let ch_len = ch.len_utf8();
        match ch {
            '\\' => {
                layout = &layout[ch_len..];
                let (b, n) = parse_layout_slash(layout)?;
                layout = &layout[n..];
                literal.push(b);
                continue;
            }
            '%' => {
                layout = &layout[ch_len..];
            }
            _ => {
                literal.extend_from_slice(&layout.as_bytes()[..ch_len]);
                layout = &layout[ch_len..];
                continue;
            }
        }
        if layout.is_empty() {
            return Err("invalid escape sequence at end of layout string".to_string());
        }
        let next_ch = layout.chars().next().unwrap();
        if matches!(next_ch, '%' | '{' | '}') {
            literal.push(next_ch as u8);
            layout = &layout[next_ch.len_utf8()..];
            continue;
        }
        let is_open_brace = layout.len() > 2 && layout.as_bytes()[1] == b'{';
        let escaped = layout.as_bytes()[0];
        layout = &layout[1..];
        if !literal.is_empty() {
            let lit = std::mem::take(&mut literal);
            fns.push(Box::new(move |b, _, _| b.extend_from_slice(&lit)));
        }
        if is_open_brace {
            layout = &layout[1..];
        }
        let mut handled_brace = false;
        match escaped {
            b'T' | b'K' | b'V' | b'H' | b'p' | b'o' | b'e' | b'i' | b'x' | b'y' | b'D'
            | b'[' | b'|' | b']' => {
                let numfn: fn(&mut Vec<u8>, i64) = if is_open_brace {
                    handled_brace = true;
                    let (nf, n) = parse_num_write_layout(layout)?;
                    layout = &layout[n..];
                    nf
                } else {
                    |b, n| b.extend_from_slice(n.to_string().as_bytes())
                };
                match escaped {
                    b'T' => fns.push(Box::new(move |b, _, r| match r {
                        Some(r) => numfn(b, r.topic.len() as i64),
                        None => b.extend_from_slice(b"<nil>"),
                    })),
                    b'K' => fns.push(Box::new(move |b, _, r| match r {
                        Some(r) => numfn(b, r.key.len() as i64),
                        None => b.extend_from_slice(b"<nil>"),
                    })),
                    b'V' => fns.push(Box::new(move |b, _, r| match r {
                        Some(r) => numfn(b, r.value.len() as i64),
                        None => b.extend_from_slice(b"<nil>"),
                    })),
                    b'H' => fns.push(Box::new(move |b, _, r| match r {
                        Some(r) => numfn(b, r.headers.len() as i64),
                        None => b.extend_from_slice(b"<nil>"),
                    })),
                    b'p' => fns.push(Box::new(move |b, _, r| match r {
                        Some(r) => numfn(b, r.partition as i64),
                        None => b.extend_from_slice(b"<nil>"),
                    })),
                    b'o' => fns.push(Box::new(move |b, _, r| match r {
                        Some(r) => numfn(b, r.offset),
                        None => b.extend_from_slice(b"<nil>"),
                    })),
                    b'e' => fns.push(Box::new(move |b, _, r| match r {
                        Some(r) => numfn(b, r.leader_epoch as i64),
                        None => b.extend_from_slice(b"<nil>"),
                    })),
                    b'i' => {
                        let calls = std::sync::Arc::clone(calls);
                        fns.push(Box::new(move |b, _, _| {
                            numfn(b, calls.fetch_add(1, Ordering::Relaxed) + 1);
                        }));
                    }
                    b'x' => fns.push(Box::new(move |b, _, r| match r {
                        Some(r) => numfn(b, r.producer_id),
                        None => b.extend_from_slice(b"<nil>"),
                    })),
                    b'y' => fns.push(Box::new(move |b, _, r| match r {
                        Some(r) => numfn(b, r.producer_epoch as i64),
                        None => b.extend_from_slice(b"<nil>"),
                    })),
                    b'D' => fns.push(Box::new(move |b, _, r| match r {
                        Some(r) => numfn(b, r.delivery_count as i64),
                        None => b.extend_from_slice(b"<nil>"),
                    })),
                    b'[' => fns.push(Box::new(move |b, p, _| match p {
                        Some(p) => numfn(b, p.log_start_offset),
                        None => b.extend_from_slice(b"<nil>"),
                    })),
                    b'|' => fns.push(Box::new(move |b, p, _| match p {
                        Some(p) => numfn(b, p.last_stable_offset),
                        None => b.extend_from_slice(b"<nil>"),
                    })),
                    b']' => fns.push(Box::new(move |b, p, _| match p {
                        Some(p) => numfn(b, p.high_watermark),
                        None => b.extend_from_slice(b"<nil>"),
                    })),
                    _ => unreachable!(),
                }
            }
            b't' | b'k' | b'v' => {
                let append_fn: BytesAppendFn = if is_open_brace {
                    handled_brace = true;
                    if let Some(rem) = layout.strip_prefix('}') {
                        layout = rem;
                        Box::new(|b, s| b.extend_from_slice(s))
                    } else if let Some(rem) = layout.strip_prefix("base64}") {
                        layout = rem;
                        Box::new(|b, s| {
                            b.extend_from_slice(
                                base64::engine::general_purpose::STANDARD.encode(s).as_bytes(),
                            );
                        })
                    } else if let Some(rem) = layout.strip_prefix("base64raw}") {
                        layout = rem;
                        Box::new(|b, s| {
                            b.extend_from_slice(
                                base64::engine::general_purpose::STANDARD_NO_PAD
                                    .encode(s)
                                    .as_bytes(),
                            );
                        })
                    } else if let Some(rem) = layout.strip_prefix("hex}") {
                        layout = rem;
                        Box::new(|b, s| b.extend_from_slice(hex::encode(s).as_bytes()))
                    } else if let Some(after_unpack) = layout.strip_prefix("unpack") {
                        let (spec, rem) = nom_open_close(after_unpack)?;
                        let rem = rem
                            .strip_prefix('}')
                            .ok_or_else(|| format!("unpack missing closing }} in {layout:?}"))?;
                        layout = rem;
                        parse_unpack(spec)?
                    } else {
                        return Err(format!("unknown %{} escape", escaped as char));
                    }
                } else {
                    Box::new(|b, s| b.extend_from_slice(s))
                };
                match escaped {
                    b't' => fns.push(Box::new(move |b, _, r| match r {
                        Some(r) => append_fn(b, r.topic.as_bytes()),
                        None => b.extend_from_slice(b"<nil>"),
                    })),
                    b'k' => fns.push(Box::new(move |b, _, r| match r {
                        Some(r) => append_fn(b, &r.key),
                        None => b.extend_from_slice(b"<nil>"),
                    })),
                    b'v' => fns.push(Box::new(move |b, _, r| match r {
                        Some(r) => append_fn(b, &r.value),
                        None => b.extend_from_slice(b"<nil>"),
                    })),
                    _ => unreachable!(),
                }
            }
            b'a' => {
                if !is_open_brace {
                    return Err("missing open brace sequence on %a".to_string());
                }
                handled_brace = true;
                if let Some(rem) = layout.strip_prefix("compression}") {
                    layout = rem;
                    fns.push(Box::new(|b, _, r| match r {
                        Some(r) => {
                            let s = match r.attrs.compression_type() {
                                0 => "none",
                                1 => "gzip",
                                2 => "snappy",
                                3 => "lz4",
                                4 => "zstd",
                                _ => "unknown",
                            };
                            b.extend_from_slice(s.as_bytes());
                        }
                        None => b.extend_from_slice(b"<nil>"),
                    }));
                } else if let Some(rem) = layout.strip_prefix("compression;") {
                    let (nf, n) = parse_num_write_layout(rem)?;
                    layout = &rem[n..];
                    fns.push(Box::new(move |b, _, r| match r {
                        Some(r) => nf(b, r.attrs.compression_type() as i64),
                        None => b.extend_from_slice(b"<nil>"),
                    }));
                } else if let Some(rem) = layout.strip_prefix("timestamp-type}") {
                    layout = rem;
                    fns.push(Box::new(|b, _, r| match r {
                        Some(r) => b.extend_from_slice(r.attrs.timestamp_type().to_string().as_bytes()),
                        None => b.extend_from_slice(b"<nil>"),
                    }));
                } else if let Some(rem) = layout.strip_prefix("timestamp-type;") {
                    let (nf, n) = parse_num_write_layout(rem)?;
                    layout = &rem[n..];
                    fns.push(Box::new(move |b, _, r| match r {
                        Some(r) => nf(b, r.attrs.timestamp_type() as i64),
                        None => b.extend_from_slice(b"<nil>"),
                    }));
                } else if let Some(rem) = layout.strip_prefix("transactional-bit}") {
                    layout = rem;
                    fns.push(Box::new(|b, _, r| match r {
                        Some(r) => b.push(if r.attrs.is_transactional() { b'1' } else { b'0' }),
                        None => b.extend_from_slice(b"<nil>"),
                    }));
                } else if let Some(rem) = layout.strip_prefix("transactional-bit;") {
                    let (nf, n) = parse_num_write_layout(rem)?;
                    layout = &rem[n..];
                    fns.push(Box::new(move |b, _, r| match r {
                        Some(r) => nf(b, i64::from(r.attrs.is_transactional())),
                        None => b.extend_from_slice(b"<nil>"),
                    }));
                } else if let Some(rem) = layout.strip_prefix("control-bit}") {
                    layout = rem;
                    fns.push(Box::new(|b, _, r| match r {
                        Some(r) => b.push(if r.attrs.is_control() { b'1' } else { b'0' }),
                        None => b.extend_from_slice(b"<nil>"),
                    }));
                } else if let Some(rem) = layout.strip_prefix("control-bit;") {
                    let (nf, n) = parse_num_write_layout(rem)?;
                    layout = &rem[n..];
                    fns.push(Box::new(move |b, _, r| match r {
                        Some(r) => nf(b, i64::from(r.attrs.is_control())),
                        None => b.extend_from_slice(b"<nil>"),
                    }));
                } else {
                    return Err("unknown %a formatting".to_string());
                }
            }
            b'h' => {
                if !is_open_brace {
                    return Err("missing open brace sequence on %h".to_string());
                }
                handled_brace = true;
                let bytes = layout.as_bytes();
                let mut braces = 1usize;
                let mut at = 0usize;
                while braces != 0 && at < bytes.len() {
                    if bytes[at] == b'{' && (at == 0 || bytes[at - 1] != b'%') {
                        braces += 1;
                    } else if bytes[at] == b'}' && (at == 0 || bytes[at - 1] != b'%') {
                        braces -= 1;
                    }
                    at += 1;
                }
                if braces > 0 {
                    return Err(format!("invalid header specification: missing closing brace in {layout:?}"));
                }
                let spec = &layout[..at - 1];
                layout = &layout[at..];
                let inner = RecordFormatter::new(spec)?;
                fns.push(Box::new(move |b, _, r| {
                    let Some(r) = r else {
                        b.extend_from_slice(b"<nil>");
                        return;
                    };
                    for hdr in &r.headers {
                        let tmp = FormattedRecord {
                            key: hdr.key.as_bytes().to_vec(),
                            value: hdr.value.clone(),
                            ..Default::default()
                        };
                        inner.append_record(b, Some(&tmp));
                    }
                }));
            }
            b'd' | b'A' => {
                let is_deadline = escaped == b'A';
                let get_ms = move |r: &FormattedRecord| -> i64 {
                    if is_deadline {
                        r.acquisition_deadline_ms.unwrap_or(0)
                    } else {
                        r.timestamp_ms
                    }
                };
                handled_brace = is_open_brace;
                if !handled_brace {
                    fns.push(Box::new(move |b, _, r| match r {
                        Some(r) => b.extend_from_slice(get_ms(r).to_string().as_bytes()),
                        None => b.extend_from_slice(b"<nil>"),
                    }));
                } else if let Some(after_s) = layout.strip_prefix("strftime") {
                    let (tfmt, rem) = nom_open_close(after_s)?;
                    let rem = rem
                        .strip_prefix('}')
                        .ok_or_else(|| format!("strftime missing closing }} in {layout:?}"))?;
                    layout = rem;
                    let tfmt = tfmt.to_string();
                    fns.push(Box::new(move |b, _, r| match r {
                        Some(r) => {
                            let ms = get_ms(r);
                            let dt = DateTime::<Utc>::from_timestamp_millis(ms)
                                .unwrap_or(DateTime::<Utc>::UNIX_EPOCH);
                            b.extend_from_slice(dt.format(&tfmt).to_string().as_bytes());
                        }
                        None => b.extend_from_slice(b"<nil>"),
                    }));
                } else {
                    let (nf, n) = parse_num_write_layout(layout)?;
                    layout = &layout[n..];
                    fns.push(Box::new(move |b, _, r| match r {
                        Some(r) => nf(b, get_ms(r)),
                        None => b.extend_from_slice(b"<nil>"),
                    }));
                }
            }
            other => return Err(format!("unknown escape sequence %{}", other as char)),
        }
        if is_open_brace && !handled_brace {
            return Err(format!("unhandled open brace {layout:?}"));
        }
    }
    if !literal.is_empty() {
        fns.push(Box::new(move |b, _, _| b.extend_from_slice(&literal)));
    }
    Ok(fns)
}

// ---------------------------------------------------------------------------
// RecordReader
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReaderReadErr {
    Eof,
    UnexpectedEof,
    Other(String),
}

impl std::fmt::Display for ReaderReadErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Eof => write!(f, "EOF"),
            Self::UnexpectedEof => write!(f, "unexpected EOF"),
            Self::Other(s) => write!(f, "{s}"),
        }
    }
}

trait PeekBufRead: BufRead {
    fn push_front(&mut self, data: &[u8]);
}

struct PeekBufReader<R: Read> {
    inner: BufReader<R>,
    pushback: Vec<u8>,
}

impl<R: Read> PeekBufReader<R> {
    fn new(r: R) -> Self {
        Self {
            inner: BufReader::new(r),
            pushback: Vec::new(),
        }
    }
}

impl<R: Read> PeekBufRead for PeekBufReader<R> {
    fn push_front(&mut self, data: &[u8]) {
        if data.is_empty() {
            return;
        }
        let mut new_pb = data.to_vec();
        new_pb.extend_from_slice(&self.pushback);
        self.pushback = new_pb;
    }
}

impl<R: Read> Read for PeekBufReader<R> {
    fn read(&mut self, dst: &mut [u8]) -> io::Result<usize> {
        if !self.pushback.is_empty() {
            let n = dst.len().min(self.pushback.len());
            dst[..n].copy_from_slice(&self.pushback[..n]);
            self.pushback.drain(..n);
            return Ok(n);
        }
        self.inner.read(dst)
    }
}

impl<R: Read> BufRead for PeekBufReader<R> {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        if !self.pushback.is_empty() {
            return Ok(&self.pushback);
        }
        self.inner.fill_buf()
    }
    fn consume(&mut self, amt: usize) {
        if !self.pushback.is_empty() {
            let n = amt.min(self.pushback.len());
            self.pushback.drain(..n);
        } else {
            self.inner.consume(amt);
        }
    }
}

type HandoffFn = Box<dyn FnMut(&mut dyn PeekBufRead, &mut FormattedRecord) -> Result<(), ReaderReadErr>>;
type ParseFieldFn = Box<dyn Fn(&[u8], &mut FormattedRecord) -> Result<(), String>>;
type ByteDecodeFn = fn(&[u8]) -> Result<Vec<u8>, String>;

enum ReadKind {
    NoRead,
    Exact(Vec<u8>),
    Condition(Box<dyn FnMut(u8) -> i8>),
    FixedSize(usize),
    DynamicSize(Rc<Cell<u64>>),
    Handoff(HandoffFn),
    Regex(Regex),
    Delim(Vec<u8>),
}

impl ReadKind {
    fn is_empty(&self) -> bool {
        matches!(self, Self::Delim(d) if d.is_empty())
    }
}

struct ReadStep {
    kind: ReadKind,
    parse: Option<ParseFieldFn>,
}

pub struct RecordReader<R: Read> {
    r: PeekBufReader<R>,
    buf: Vec<u8>,
    steps: Vec<ReadStep>,
    done: bool,
}

impl<R: Read> RecordReader<R> {
    pub fn new(reader: R, layout: &str) -> Result<Self, String> {
        let steps = parse_read_layout(layout)?;
        Ok(Self {
            r: PeekBufReader::new(reader),
            buf: Vec::new(),
            steps,
            done: false,
        })
    }

    pub fn read_record(&mut self) -> Result<FormattedRecord, ReaderReadErr> {
        let mut rec = FormattedRecord::default();
        self.read_record_into(&mut rec)?;
        Ok(rec)
    }

    pub fn read_record_into(&mut self, rec: &mut FormattedRecord) -> Result<(), ReaderReadErr> {
        if self.done {
            return Err(ReaderReadErr::Eof);
        }
        next_record(&mut self.r, &mut self.buf, &mut self.steps, &mut self.done, rec)
    }
}

fn next_record(
    r: &mut dyn PeekBufRead,
    buf: &mut Vec<u8>,
    steps: &mut [ReadStep],
    done: &mut bool,
    rec: &mut FormattedRecord,
) -> Result<(), ReaderReadErr> {
    let total = steps.len();
    for i in 0..total {
        buf.clear();
        let res = match &mut steps[i].kind {
            ReadKind::NoRead => Ok(()),
            ReadKind::Exact(d) => {
                let d = d.clone();
                read_size_into(r, buf, d.len()).and_then(|()| {
                    if buf.as_slice() != d.as_slice() {
                        Err(ReaderReadErr::Other(format!(
                            "exact text mismatch, read {:?} when expecting {:?}",
                            String::from_utf8_lossy(buf),
                            String::from_utf8_lossy(&d)
                        )))
                    } else {
                        Ok(())
                    }
                })
            }
            ReadKind::Condition(cond) => read_condition_into(r, buf, cond.as_mut()),
            ReadKind::FixedSize(sz) => {
                let sz = *sz;
                read_size_into(r, buf, sz)
            }
            ReadKind::DynamicSize(cell) => {
                let raw_n = cell.get();
                if raw_n > isize::MAX as u64 {
                    Err(ReaderReadErr::Other(format!(
                        "invalid overflowing read size {raw_n}"
                    )))
                } else {
                    read_size_into(r, buf, raw_n as usize)
                }
            }
            ReadKind::Handoff(h) => h(r, rec),
            ReadKind::Regex(re) => read_re_into(r, buf, re),
            ReadKind::Delim(d) => read_delim_into(r, buf, d),
        };

        match res {
            Ok(()) => {}
            Err(e @ ReaderReadErr::Eof) | Err(e @ ReaderReadErr::UnexpectedEof) => {
                *done = true;
                let prev_is_noread = i > 0 && matches!(steps[i - 1].kind, ReadKind::NoRead);
                if buf.is_empty() && (i == 0 || prev_is_noread) {
                    return Err(ReaderReadErr::Eof);
                }
                if i != total - 1 || matches!(e, ReaderReadErr::UnexpectedEof) {
                    return Err(ReaderReadErr::UnexpectedEof);
                }
            }
            Err(e) => return Err(e),
        }

        if let ReadKind::FixedSize(sz) = steps[i].kind {
            if buf.len() < sz {
                return Err(ReaderReadErr::UnexpectedEof);
            }
        }
        if let Some(ref parse) = steps[i].parse {
            parse(buf, rec).map_err(ReaderReadErr::Other)?;
        }
    }
    Ok(())
}

fn read_size_into(r: &mut dyn PeekBufRead, buf: &mut Vec<u8>, n: usize) -> Result<(), ReaderReadErr> {
    const CHUNK: usize = 64 << 10;
    while buf.len() < n {
        let want = (n - buf.len()).min(CHUNK);
        let start = buf.len();
        buf.resize(start + want, 0);
        let mut filled = 0;
        while filled < want {
            match r.read(&mut buf[start + filled..]) {
                Ok(0) => {
                    buf.truncate(start + filled);
                    if buf.is_empty() {
                        return Err(ReaderReadErr::Eof);
                    }
                    return Err(ReaderReadErr::UnexpectedEof);
                }
                Ok(k) => filled += k,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => {
                    buf.truncate(start + filled);
                    return Err(ReaderReadErr::Other(e.to_string()));
                }
            }
        }
    }
    Ok(())
}

fn read_condition_into(
    r: &mut dyn PeekBufRead,
    buf: &mut Vec<u8>,
    cond: &mut dyn FnMut(u8) -> i8,
) -> Result<(), ReaderReadErr> {
    let mut ignore_eof = false;
    loop {
        let avail = r.fill_buf().map_err(|e| ReaderReadErr::Other(e.to_string()))?;
        if avail.is_empty() {
            return if ignore_eof { Ok(()) } else { Err(ReaderReadErr::Eof) };
        }
        ignore_eof = false;
        let c = avail[0];
        match cond(c) {
            -2 => return Err(ReaderReadErr::Other(format!("invalid input {:?}", c as char))),
            -1 => return Ok(()),
            0 => {
                r.consume(1);
                buf.push(c);
                return Ok(());
            }
            2 => {
                ignore_eof = true;
            }
            _ => {}
        }
        r.consume(1);
        buf.push(c);
    }
}

fn read_delim_into(r: &mut dyn PeekBufRead, buf: &mut Vec<u8>, delim: &[u8]) -> Result<(), ReaderReadErr> {
    if delim.is_empty() {
        r.read_to_end(buf)
            .map_err(|e| ReaderReadErr::Other(e.to_string()))?;
        return Err(ReaderReadErr::Eof);
    }
    loop {
        let avail = r.fill_buf().map_err(|e| ReaderReadErr::Other(e.to_string()))?;
        if avail.is_empty() {
            return Err(ReaderReadErr::UnexpectedEof);
        }
        // Accumulate until we have at least delim.len() bytes or match.
        if avail.len() >= delim.len() {
            if &avail[..delim.len()] == delim {
                r.consume(delim.len());
                return Ok(());
            }
            buf.push(avail[0]);
            r.consume(1);
        } else {
            // Pull one byte at a time via sliding window check.
            let mut tmp = vec![avail[0]];
            r.consume(1);
            while tmp.len() < delim.len() {
                let more = r.fill_buf().map_err(|e| ReaderReadErr::Other(e.to_string()))?;
                if more.is_empty() {
                    buf.extend_from_slice(&tmp);
                    return Err(ReaderReadErr::UnexpectedEof);
                }
                tmp.push(more[0]);
                r.consume(1);
            }
            if tmp.as_slice() == delim {
                return Ok(());
            }
            buf.push(tmp[0]);
            // Note: we only support 1-2 byte delims simply here; if mismatch, keep scanning.
            // To avoid losing buffered tail bytes on multi-byte delims, check buf tail.
            for &b in &tmp[1..] {
                buf.push(b);
            }
            if buf.ends_with(delim) {
                let new_len = buf.len() - delim.len();
                buf.truncate(new_len);
                return Ok(());
            }
        }
    }
}

fn read_re_into(r: &mut dyn PeekBufRead, buf: &mut Vec<u8>, re: &Regex) -> Result<(), ReaderReadErr> {
    let mut peeked = Vec::new();
    let saw_eof;
    loop {
        let avail = r.fill_buf().map_err(|e| ReaderReadErr::Other(e.to_string()))?;
        if avail.is_empty() {
            saw_eof = true;
            break;
        }
        peeked.extend_from_slice(avail);
        let n = avail.len();
        r.consume(n);
    }
    let s = std::str::from_utf8(&peeked)
        .map_err(|e| ReaderReadErr::Other(format!("non-utf8 regex input: {e}")))?;
    let Some(m) = re.find(s) else {
        if saw_eof && !peeked.is_empty() {
            return Err(ReaderReadErr::Other(format!("regexp text mismatch, saw {:?}", s)));
        }
        return Err(ReaderReadErr::Eof);
    };
    buf.extend_from_slice(&peeked[..m.end()]);
    if m.end() < peeked.len() {
        r.push_front(&peeked[m.end()..]);
        return Ok(());
    }
    if saw_eof {
        return Err(ReaderReadErr::Eof);
    }
    Ok(())
}

fn parse_read_layout(mut layout: &str) -> Result<Vec<ReadStep>, String> {
    if layout.is_empty() {
        return Err("RecordReader: invalid empty format".to_string());
    }
    let topic_size = Rc::new(Cell::new(0u64));
    let key_size = Rc::new(Cell::new(0u64));
    let value_size = Rc::new(Cell::new(0u64));
    let headers_num = Rc::new(Cell::new(0u64));

    const P_TOPIC: u8 = 1 << 0;
    const P_TOPIC_SZ: u8 = 1 << 1;
    const P_KEY: u8 = 1 << 2;
    const P_KEY_SZ: u8 = 1 << 3;
    const P_VALUE: u8 = 1 << 4;
    const P_VALUE_SZ: u8 = 1 << 5;
    const P_HEADERS: u8 = 1 << 6;
    const P_HEADERS_NUM: u8 = 1 << 7;
    let mut bits: u8 = 0;

    let mut steps: Vec<ReadStep> = Vec::new();
    let mut literal: Vec<u8> = Vec::new();

    let flush_literal = |steps: &mut Vec<ReadStep>, literal: &mut Vec<u8>| {
        if let Some(last) = steps.last_mut() {
            if last.kind.is_empty() {
                last.kind = ReadKind::Delim(std::mem::take(literal));
                return;
            }
        }
        if !literal.is_empty() {
            steps.push(ReadStep {
                kind: ReadKind::Exact(std::mem::take(literal)),
                parse: None,
            });
        }
    };

    while !layout.is_empty() {
        let ch = layout.chars().next().unwrap();
        let ch_len = ch.len_utf8();
        match ch {
            '\\' => {
                layout = &layout[ch_len..];
                let (b, n) = parse_layout_slash(layout)?;
                layout = &layout[n..];
                literal.push(b);
                continue;
            }
            '%' => {
                layout = &layout[ch_len..];
            }
            _ => {
                literal.extend_from_slice(&layout.as_bytes()[..ch_len]);
                layout = &layout[ch_len..];
                continue;
            }
        }
        if layout.is_empty() {
            literal.push(b'%');
            continue;
        }
        let next_ch = layout.chars().next().unwrap();
        if matches!(next_ch, '%' | '{' | '}') {
            literal.push(next_ch as u8);
            layout = &layout[next_ch.len_utf8()..];
            continue;
        }
        let is_open_brace = layout.len() > 2 && layout.as_bytes()[1] == b'{';
        let escaped = layout.as_bytes()[0];
        layout = &layout[1..];
        flush_literal(&mut steps, &mut literal);
        if is_open_brace {
            layout = &layout[1..];
        }
        let mut handled_brace = false;
        match escaped {
            b'T' | b'K' | b'V' | b'H' => {
                let (dst, bit) = match escaped {
                    b'T' => (Rc::clone(&topic_size), P_TOPIC_SZ),
                    b'K' => (Rc::clone(&key_size), P_KEY_SZ),
                    b'V' => (Rc::clone(&value_size), P_VALUE_SZ),
                    b'H' => (Rc::clone(&headers_num), P_HEADERS_NUM),
                    _ => unreachable!(),
                };
                if (bits & bit) != 0 {
                    return Err(format!("%{} is doubly specified", escaped as char));
                }
                if (bits & (bit >> 1)) != 0 {
                    return Err(format!(
                        "size specification %{} cannot come after value specification %{}",
                        escaped as char,
                        (escaped as char).to_ascii_lowercase()
                    ));
                }
                bits |= bit;
                let (step, n) = if is_open_brace {
                    handled_brace = true;
                    parse_read_size(layout, dst, true)?
                } else {
                    parse_read_size("ascii", dst, false)?
                };
                layout = &layout[n..];
                steps.push(step);
            }
            b'p' | b'o' | b'e' | b'd' | b'x' | b'y' => {
                let dst = Rc::new(Cell::new(0u64));
                let (mut step, n) = if is_open_brace {
                    handled_brace = true;
                    parse_read_size(layout, Rc::clone(&dst), true)?
                } else {
                    parse_read_size("ascii", Rc::clone(&dst), false)?
                };
                layout = &layout[n..];
                let num_parse = step.parse.take().unwrap();
                step.parse = Some(Box::new(move |b, rec| {
                    num_parse(b, rec)?;
                    let v = dst.get();
                    match escaped {
                        b'p' => rec.partition = v as i32,
                        b'o' => rec.offset = v as i64,
                        b'e' => rec.leader_epoch = v as i32,
                        b'd' => rec.timestamp_ms = v as i64,
                        b'x' => rec.producer_id = v as i64,
                        b'y' => rec.producer_epoch = v as i16,
                        _ => unreachable!(),
                    }
                    Ok(())
                }));
                steps.push(step);
            }
            b't' | b'k' | b'v' => {
                let mut decode_fn: Option<ByteDecodeFn> = None;
                let mut re_opt: Option<Regex> = None;
                let mut is_json = false;
                if is_open_brace {
                    handled_brace = true;
                    if let Some(rem) = layout.strip_prefix('}') {
                        layout = rem;
                    } else if let Some(rem) = layout.strip_prefix("base64}") {
                        layout = rem;
                        decode_fn = Some(|b| {
                            base64::engine::general_purpose::STANDARD
                                .decode(b)
                                .map_err(|e| e.to_string())
                        });
                    } else if let Some(rem) = layout.strip_prefix("hex}") {
                        layout = rem;
                        decode_fn = Some(|b| hex::decode(b).map_err(|e| e.to_string()));
                    } else if let Some(rem) = layout.strip_prefix("json}") {
                        layout = rem;
                        is_json = true;
                        decode_fn = Some(compact_validated_json);
                    } else if let Some(after_re) = layout.strip_prefix("re") {
                        let (mut restr, rem) = nom_open_close(after_re)?;
                        let rem = rem
                            .strip_prefix('}')
                            .ok_or_else(|| format!("re missing closing }} in {layout:?}"))?;
                        layout = rem;
                        let owned = if restr.starts_with('^') {
                            restr.to_string()
                        } else {
                            format!("^{restr}")
                        };
                        let _ = &mut restr;
                        re_opt = Some(Regex::new(&owned).map_err(|e| e.to_string())?);
                    } else {
                        return Err(format!("unknown %{} escape", escaped as char));
                    }
                }
                let (bit, bit_size, sz_cell) = match escaped {
                    b't' => (P_TOPIC, P_TOPIC_SZ, Rc::clone(&topic_size)),
                    b'k' => (P_KEY, P_KEY_SZ, Rc::clone(&key_size)),
                    b'v' => (P_VALUE, P_VALUE_SZ, Rc::clone(&value_size)),
                    _ => unreachable!(),
                };
                bits |= bit;
                let kind = if (bits & bit_size) != 0 {
                    if re_opt.is_some() {
                        return Err("cannot specify exact size and regular expression".to_string());
                    }
                    if is_json {
                        return Err("cannot specify exact size and json".to_string());
                    }
                    ReadKind::DynamicSize(sz_cell)
                } else if let Some(re) = re_opt {
                    ReadKind::Regex(re)
                } else if is_json {
                    let mut jr = JsonReader::default();
                    ReadKind::Condition(Box::new(move |c| jr.read(c)))
                } else {
                    ReadKind::Delim(Vec::new())
                };
                steps.push(ReadStep {
                    kind,
                    parse: Some(Box::new(move |b, rec| {
                        let owned;
                        let slice = if let Some(dec) = decode_fn {
                            owned = dec(b)?;
                            owned.as_slice()
                        } else {
                            b
                        };
                        match escaped {
                            b't' => rec.topic = String::from_utf8_lossy(slice).into_owned(),
                            b'k' => rec.key = slice.to_vec(),
                            b'v' => rec.value = slice.to_vec(),
                            _ => unreachable!(),
                        }
                        Ok(())
                    })),
                });
            }
            b'h' => {
                bits |= P_HEADERS;
                if (bits & P_HEADERS_NUM) == 0 {
                    return Err("missing header count specification %H before %h".to_string());
                }
                if !is_open_brace {
                    return Err("missing open brace sequence on %h".to_string());
                }
                handled_brace = true;
                let bytes = layout.as_bytes();
                let mut braces = 1usize;
                let mut at = 0usize;
                while braces != 0 && at < bytes.len() {
                    if bytes[at] == b'{' && (at == 0 || bytes[at - 1] != b'%') {
                        braces += 1;
                    } else if bytes[at] == b'}' && (at == 0 || bytes[at - 1] != b'%') {
                        braces -= 1;
                    }
                    at += 1;
                }
                if braces > 0 {
                    return Err(format!("invalid header specification: missing closing brace in {layout:?}"));
                }
                let inner_spec = &layout[..at - 1];
                layout = &layout[at..];
                let mut inner_steps = parse_read_layout(inner_spec)?;
                let hnum = Rc::clone(&headers_num);
                steps.push(ReadStep {
                    kind: ReadKind::Handoff(Box::new(move |r, rec| {
                        let saved_k = std::mem::take(&mut rec.key);
                        let saved_v = std::mem::take(&mut rec.value);
                        let count = hnum.get();
                        let mut ibuf = Vec::new();
                        let mut idone = false;
                        for _ in 0..count {
                            rec.key.clear();
                            rec.value.clear();
                            next_record(r, &mut ibuf, &mut inner_steps, &mut idone, rec)?;
                            rec.headers.push(FormattedRecordHeader {
                                key: String::from_utf8_lossy(&rec.key).into_owned(),
                                value: std::mem::take(&mut rec.value),
                            });
                        }
                        rec.key = saved_k;
                        rec.value = saved_v;
                        Ok(())
                    })),
                    parse: None,
                });
            }
            other => return Err(format!("unknown percent escape sequence {:?}", other as char)),
        }
        if is_open_brace && !handled_brace {
            return Err(format!("unhandled open brace {layout:?}"));
        }
    }

    flush_literal(&mut steps, &mut literal);

    let mut noreads = Vec::new();
    let mut reads = Vec::new();
    for s in steps {
        if matches!(s.kind, ReadKind::NoRead) {
            noreads.push(s);
        } else {
            reads.push(s);
        }
    }
    if reads.is_empty() {
        return Err("RecordReader: layout reads nothing from the input".to_string());
    }
    noreads.extend(reads);
    Ok(noreads)
}

fn parse_read_size(
    layout: &str,
    dst: Rc<Cell<u64>>,
    need_brace: bool,
) -> Result<(ReadStep, usize), String> {
    let (spec, end) = if need_brace {
        let brace_end = layout
            .find('}')
            .ok_or_else(|| "missing brace end } to close number size specification".to_string())?;
        (&layout[..brace_end], brace_end + 1)
    } else {
        (layout, 0)
    };
    let step = match spec {
        "ascii" | "number" => ReadStep {
            kind: ReadKind::Condition(Box::new(|b| if b.is_ascii_digit() { 2 } else { -1 })),
            parse: Some(Box::new(move |b, _| {
                let s = std::str::from_utf8(b).map_err(|e| e.to_string())?;
                dst.set(s.parse::<u64>().map_err(|e| e.to_string())?);
                Ok(())
            })),
        },
        "big64" => ReadStep {
            kind: ReadKind::FixedSize(8),
            parse: Some(Box::new(move |b, _| {
                dst.set(u64::from_be_bytes(b[..8].try_into().unwrap()));
                Ok(())
            })),
        },
        "big32" => ReadStep {
            kind: ReadKind::FixedSize(4),
            parse: Some(Box::new(move |b, _| {
                dst.set(u32::from_be_bytes(b[..4].try_into().unwrap()) as u64);
                Ok(())
            })),
        },
        "big16" => ReadStep {
            kind: ReadKind::FixedSize(2),
            parse: Some(Box::new(move |b, _| {
                dst.set(u16::from_be_bytes(b[..2].try_into().unwrap()) as u64);
                Ok(())
            })),
        },
        "little64" => ReadStep {
            kind: ReadKind::FixedSize(8),
            parse: Some(Box::new(move |b, _| {
                dst.set(u64::from_le_bytes(b[..8].try_into().unwrap()));
                Ok(())
            })),
        },
        "little32" => ReadStep {
            kind: ReadKind::FixedSize(4),
            parse: Some(Box::new(move |b, _| {
                dst.set(u32::from_le_bytes(b[..4].try_into().unwrap()) as u64);
                Ok(())
            })),
        },
        "little16" => ReadStep {
            kind: ReadKind::FixedSize(2),
            parse: Some(Box::new(move |b, _| {
                dst.set(u16::from_le_bytes(b[..2].try_into().unwrap()) as u64);
                Ok(())
            })),
        },
        "byte" | "big8" | "little8" => ReadStep {
            kind: ReadKind::FixedSize(1),
            parse: Some(Box::new(move |b, _| {
                dst.set(b[0] as u64);
                Ok(())
            })),
        },
        "hex64" => hex_size_step(16, dst),
        "hex32" => hex_size_step(8, dst),
        "hex16" => hex_size_step(4, dst),
        "hex8" => hex_size_step(2, dst),
        "hex4" => hex_size_step(1, dst),
        "bool" => {
            let mut state = 0u8;
            let mut last = 0u8;
            ReadStep {
                kind: ReadKind::Condition(Box::new(move |b| {
                    let res = match state {
                        0 => match b {
                            b't' => {
                                state = 1;
                                last = b;
                                1
                            }
                            b'f' => {
                                state = 2;
                                last = b;
                                1
                            }
                            _ => -1,
                        },
                        1 => {
                            if (last == b't' && b == b'r') || (last == b'r' && b == b'u') {
                                last = b;
                                1
                            } else if last == b'u' && b == b'e' {
                                0
                            } else {
                                -1
                            }
                        }
                        2 => {
                            if (last == b'f' && b == b'a')
                                || (last == b'a' && b == b'l')
                                || (last == b'l' && b == b's')
                            {
                                last = b;
                                1
                            } else if last == b's' && b == b'e' {
                                0
                            } else {
                                -1
                            }
                        }
                        _ => -1,
                    };
                    if res <= 0 {
                        state = 0;
                        last = 0;
                    }
                    res
                })),
                parse: Some(Box::new(move |b, _| {
                    match b {
                        b"true" => dst.set(1),
                        b"false" => dst.set(0),
                        _ => return Err(format!("invalid bool {}", String::from_utf8_lossy(b))),
                    }
                    Ok(())
                })),
            }
        }
        other => {
            let num: i64 = other
                .parse()
                .map_err(|e| format!("unrecognized number reading layout {other:?}: {e}"))?;
            if num <= 0 {
                return Err(format!("invalid zero or negative number {other:?}"));
            }
            ReadStep {
                kind: ReadKind::NoRead,
                parse: Some(Box::new(move |_, _| {
                    dst.set(num as u64);
                    Ok(())
                })),
            }
        }
    };
    Ok((step, end))
}

fn hex_size_step(chars: usize, dst: Rc<Cell<u64>>) -> ReadStep {
    ReadStep {
        kind: ReadKind::FixedSize(chars),
        parse: Some(Box::new(move |b, _| {
            let s = std::str::from_utf8(b).map_err(|e| e.to_string())?;
            dst.set(u64::from_str_radix(s, 16).map_err(|e| e.to_string())?);
            Ok(())
        })),
    }
}

fn compact_validated_json(b: &[u8]) -> Result<Vec<u8>, String> {
    let mut jr = JsonReader::default();
    let mut out = Vec::with_capacity(b.len());
    let mut in_str = false;
    let mut esc = false;
    let mut started = false;
    let mut finished = false;
    for &c in b {
        if finished {
            if matches!(c, b' ' | b'\t' | b'\n' | b'\r') {
                continue;
            }
            return Err("trailing non-whitespace after json".to_string());
        }
        let r = jr.read(c);
        if r == -2 {
            return Err("invalid json".to_string());
        }
        if r == -1 {
            finished = true;
            continue;
        }
        if !started && matches!(c, b' ' | b'\t' | b'\n' | b'\r') {
            continue;
        }
        started = true;
        if in_str {
            out.push(c);
            if esc {
                esc = false;
            } else if c == b'\\' {
                esc = true;
            } else if c == b'"' {
                in_str = false;
            }
        } else if !matches!(c, b' ' | b'\t' | b'\n' | b'\r') {
            out.push(c);
            if c == b'"' {
                in_str = true;
            }
        }
        if r == 0 {
            finished = true;
        }
    }
    if !started || (!finished && !jr.can_end_at_eof()) {
        return Err("incomplete json".to_string());
    }
    Ok(out)
}

#[derive(Default)]
struct JsonReader {
    state: u8,
    n: u8,
    nexts: Vec<u8>,
}

const J_ANY: u8 = 0;
const J_OBJ: u8 = 1;
const J_OBJ_SEP: u8 = 2;
const J_OBJ_FIN: u8 = 3;
const J_ARR: u8 = 4;
const J_ARR_FIN: u8 = 5;
const J_STR_BEGIN: u8 = 6;
const J_STR: u8 = 7;
const J_STR_ESC: u8 = 8;
const J_STR_ESC_U: u8 = 9;
const J_TRUE: u8 = 10;
const J_FALSE: u8 = 11;
const J_NULL: u8 = 12;
const J_NEG: u8 = 13;
const J_ONE: u8 = 14;
const J_DOT_OR_E: u8 = 15;
const J_DOT: u8 = 16;
const J_E: u8 = 17;

impl JsonReader {
    fn push_state(&mut self, next: u8, next2: u8) {
        self.nexts.push(next2);
        self.state = next;
    }
    fn pop_state(&mut self) -> i8 {
        match self.nexts.pop() {
            None => {
                self.state = J_ANY;
                0
            }
            Some(s) => {
                self.state = s;
                1
            }
        }
    }
    fn pop_state_to_start(&mut self) -> bool {
        match self.nexts.pop() {
            None => {
                self.state = J_ANY;
                false
            }
            Some(s) => {
                self.state = s;
                true
            }
        }
    }
    fn one_or_two(&self) -> i8 {
        if self.nexts.is_empty() { 2 } else { 1 }
    }
    fn can_end_at_eof(&self) -> bool {
        self.nexts.is_empty() && matches!(self.state, J_ONE | J_DOT_OR_E | J_DOT | J_E)
            && !matches!((self.state, self.n), (J_DOT, 0) | (J_E, 0) | (J_E, 1))
    }

    fn read(&mut self, c: u8) -> i8 {
        loop {
            match self.state {
                J_ANY => match c {
                    b' ' | b'\t' | b'\n' | b'\r' => return 1,
                    b'{' => {
                        self.state = J_OBJ;
                        return 1;
                    }
                    b'[' => {
                        self.state = J_ARR;
                        return 1;
                    }
                    b'"' => {
                        self.state = J_STR;
                        return 1;
                    }
                    b't' => {
                        self.state = J_TRUE;
                        self.n = 0;
                        return 1;
                    }
                    b'f' => {
                        self.state = J_FALSE;
                        self.n = 0;
                        return 1;
                    }
                    b'n' => {
                        self.state = J_NULL;
                        self.n = 0;
                        return 1;
                    }
                    b'-' => {
                        self.state = J_NEG;
                        return 1;
                    }
                    b'0' => {
                        self.state = J_DOT_OR_E;
                        return self.one_or_two();
                    }
                    b'1'..=b'9' => {
                        self.state = J_ONE;
                        return self.one_or_two();
                    }
                    _ => return -2,
                },
                J_OBJ => match c {
                    b' ' | b'\t' | b'\n' | b'\r' => return 1,
                    b'"' => {
                        self.push_state(J_STR, J_OBJ_SEP);
                        return 1;
                    }
                    b'}' => return self.pop_state(),
                    _ => return -2,
                },
                J_OBJ_SEP => match c {
                    b' ' | b'\t' | b'\n' | b'\r' => return 1,
                    b':' => {
                        self.push_state(J_ANY, J_OBJ_FIN);
                        return 1;
                    }
                    _ => return -2,
                },
                J_OBJ_FIN => match c {
                    b' ' | b'\r' | b'\t' | b'\n' => return 1,
                    b',' => {
                        self.push_state(J_STR_BEGIN, J_OBJ_SEP);
                        return 1;
                    }
                    b'}' => return self.pop_state(),
                    _ => return -2,
                },
                J_ARR => match c {
                    b' ' | b'\r' | b'\t' | b'\n' => return 1,
                    b']' => return self.pop_state(),
                    _ => {
                        self.push_state(J_ANY, J_ARR_FIN);
                        continue;
                    }
                },
                J_ARR_FIN => match c {
                    b' ' | b'\r' | b'\t' | b'\n' => return 1,
                    b',' => {
                        self.state = J_ARR;
                        return 1;
                    }
                    b']' => return self.pop_state(),
                    _ => return -2,
                },
                J_STR_BEGIN => match c {
                    b' ' | b'\r' | b'\t' | b'\n' => return 1,
                    b'"' => {
                        self.state = J_STR;
                        return 1;
                    }
                    _ => return -2,
                },
                J_STR => match c {
                    0..=31 => return -2,
                    b'"' => return self.pop_state(),
                    b'\\' => {
                        self.state = J_STR_ESC;
                        return 1;
                    }
                    _ => return 1,
                },
                J_STR_ESC => match c {
                    b'b' | b'f' | b'n' | b'r' | b't' | b'\\' | b'/' | b'"' => {
                        self.state = J_STR;
                        return 1;
                    }
                    b'u' => {
                        self.state = J_STR_ESC_U;
                        self.n = 0;
                        return 1;
                    }
                    _ => return -2,
                },
                J_STR_ESC_U => {
                    if !c.is_ascii_hexdigit() {
                        return -2;
                    }
                    self.n += 1;
                    if self.n == 4 {
                        self.state = J_STR;
                    }
                    return 1;
                }
                J_TRUE => {
                    match (self.n, c) {
                        (0, b'r') | (1, b'u') => {
                            self.n += 1;
                            return 1;
                        }
                        (2, b'e') => return self.pop_state(),
                        _ => return -2,
                    }
                }
                J_FALSE => {
                    match (self.n, c) {
                        (0, b'a') | (1, b'l') | (2, b's') => {
                            self.n += 1;
                            return 1;
                        }
                        (3, b'e') => return self.pop_state(),
                        _ => return -2,
                    }
                }
                J_NULL => {
                    match (self.n, c) {
                        (0, b'u') | (1, b'l') => {
                            self.n += 1;
                            return 1;
                        }
                        (2, b'l') => return self.pop_state(),
                        _ => return -2,
                    }
                }
                J_NEG => {
                    if c == b'0' {
                        self.state = J_DOT_OR_E;
                        return self.one_or_two();
                    } else if (b'1'..=b'9').contains(&c) {
                        self.state = J_ONE;
                        return self.one_or_two();
                    }
                    return -2;
                }
                J_ONE => {
                    if c.is_ascii_digit() {
                        return self.one_or_two();
                    }
                    self.state = J_DOT_OR_E;
                    continue;
                }
                J_DOT_OR_E => {
                    if c == b'e' || c == b'E' {
                        self.state = J_E;
                        self.n = 0;
                        return 1;
                    }
                    if c == b'.' {
                        self.state = J_DOT;
                        self.n = 0;
                        return 1;
                    }
                    if self.pop_state_to_start() {
                        continue;
                    }
                    return -1;
                }
                J_DOT => match self.n {
                    0 => {
                        if !c.is_ascii_digit() {
                            return -2;
                        }
                        self.n = 1;
                        return self.one_or_two();
                    }
                    _ => {
                        if c.is_ascii_digit() {
                            return self.one_or_two();
                        }
                        if c == b'e' || c == b'E' {
                            self.state = J_E;
                            self.n = 0;
                            return 1;
                        }
                        if self.pop_state_to_start() {
                            continue;
                        }
                        return -1;
                    }
                },
                J_E => match self.n {
                    0 => {
                        if c == b'+' || c == b'-' {
                            self.n = 1;
                            return 1;
                        }
                        self.n = 1;
                        continue;
                    }
                    1 => {
                        if !c.is_ascii_digit() {
                            return -2;
                        }
                        self.n = 2;
                        return self.one_or_two();
                    }
                    _ => {
                        if c.is_ascii_digit() {
                            return self.one_or_two();
                        }
                        if self.pop_state_to_start() {
                            continue;
                        }
                        return -1;
                    }
                },
                _ => return -2,
            }
        }
    }
}
