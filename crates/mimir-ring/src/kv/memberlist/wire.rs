//! HashiCorp Memberlist wire framing and Go-compatible MessagePack codecs.
//!
//! Ported from:
//! - github.com/hashicorp/memberlist/net.go
//! - github.com/hashicorp/memberlist/compress.go
//! - github.com/hashicorp/memberlist/label.go
//! - github.com/hashicorp/memberlist/util.go
//! - github.com/hashicorp/memberlist/state.go
//! - github.com/hashicorp/go-msgpack/v2/codec/msgpack.go

use std::io::Write;

use super::lzw::{crc32_ieee, lzw_compress, lzw_decompress};

pub const PROTOCOL_VERSION_MIN: u8 = 1;
pub const PROTOCOL_VERSION_2_COMPATIBLE: u8 = 2;
pub const PROTOCOL_VERSION_MAX: u8 = 5;

pub const META_MAX_SIZE: usize = 512;
pub const LABEL_MAX_SIZE: usize = 255;
pub const COMPOUND_HEADER_OVERHEAD: usize = 2;
pub const COMPOUND_OVERHEAD: usize = 2;
pub const USER_MSG_OVERHEAD: usize = 1;
pub const MAX_PUSH_STATE_BYTES: usize = 20 * 1024 * 1024;
pub const MAX_DECOMPRESS_BYTES: usize = MAX_PUSH_STATE_BYTES + (1 << 20);
pub const MAX_PUSH_PULL_NODES: usize = 65_536;
pub(super) const MAX_MP_DEPTH: usize = 32;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum MessageType {
    Ping = 0,
    IndirectPing = 1,
    AckResp = 2,
    Suspect = 3,
    Alive = 4,
    Dead = 5,
    PushPull = 6,
    Compound = 7,
    User = 8,
    Compress = 9,
    Encrypt = 10,
    NackResp = 11,
    HasCrc = 12,
    Err = 13,
    HasLabel = 244,
}

impl TryFrom<u8> for MessageType {
    type Error = u8;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Ping),
            1 => Ok(Self::IndirectPing),
            2 => Ok(Self::AckResp),
            3 => Ok(Self::Suspect),
            4 => Ok(Self::Alive),
            5 => Ok(Self::Dead),
            6 => Ok(Self::PushPull),
            7 => Ok(Self::Compound),
            8 => Ok(Self::User),
            9 => Ok(Self::Compress),
            10 => Ok(Self::Encrypt),
            11 => Ok(Self::NackResp),
            12 => Ok(Self::HasCrc),
            13 => Ok(Self::Err),
            244 => Ok(Self::HasLabel),
            other => Err(other),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum CompressionType {
    Lzw = 0,
    Snappy = 1,
}

impl TryFrom<u8> for CompressionType {
    type Error = u8;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Lzw),
            1 => Ok(Self::Snappy),
            other => Err(other),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum NodeStateType {
    Alive = 0,
    Suspect = 1,
    Dead = 2,
    Left = 3,
}

impl TryFrom<u8> for NodeStateType {
    type Error = u8;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Alive),
            1 => Ok(Self::Suspect),
            2 => Ok(Self::Dead),
            3 => Ok(Self::Left),
            other => Err(other),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Ping {
    pub seq_no: u32,
    pub node: String,
    pub source_addr: Vec<u8>,
    pub source_port: u16,
    pub source_node: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct IndirectPingReq {
    pub seq_no: u32,
    pub target: Vec<u8>,
    pub port: u16,
    pub node: String,
    pub nack: bool,
    pub source_addr: Vec<u8>,
    pub source_port: u16,
    pub source_node: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct AckResp {
    pub seq_no: u32,
    pub payload: Option<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct NackResp {
    pub seq_no: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct ErrResp {
    pub error: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Suspect {
    pub incarnation: u32,
    pub node: String,
    pub from: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Alive {
    pub incarnation: u32,
    pub node: String,
    pub addr: Vec<u8>,
    pub port: u16,
    pub meta: Option<Vec<u8>>,
    pub vsn: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Dead {
    pub incarnation: u32,
    pub node: String,
    pub from: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct PushPullHeader {
    pub nodes: usize,
    pub user_state_len: usize,
    pub join: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct UserMsgHeader {
    pub user_msg_len: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PushNodeState {
    pub name: String,
    pub addr: Vec<u8>,
    pub port: u16,
    pub meta: Option<Vec<u8>>,
    pub incarnation: u32,
    pub state: NodeStateType,
    pub vsn: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompressedPayload {
    pub algo: CompressionType,
    pub buf: Vec<u8>,
}

// --- Go legacy MessagePack encoding (`WriteExt = false`) ---

fn write_raw_legacy(out: &mut Vec<u8>, bytes: &[u8]) {
    let len = bytes.len();
    if len < 32 {
        out.push(0xa0 | (len as u8));
    } else if len < 65536 {
        out.push(0xda);
        out.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        out.push(0xdb);
        out.extend_from_slice(&(len as u32).to_be_bytes());
    }
    out.extend_from_slice(bytes);
}

fn write_opt_bytes_legacy(out: &mut Vec<u8>, bytes: Option<&[u8]>) {
    match bytes {
        None => out.push(0xc0), // mpNil
        Some(b) => write_raw_legacy(out, b),
    }
}

fn write_str_legacy(out: &mut Vec<u8>, s: &str) {
    write_raw_legacy(out, s.as_bytes());
}

fn write_uint(out: &mut Vec<u8>, v: u64) {
    let _ = rmp::encode::write_uint(out, v);
}

fn write_bool(out: &mut Vec<u8>, v: bool) {
    let _ = rmp::encode::write_bool(out, v);
}

fn write_map_len(out: &mut Vec<u8>, len: u32) {
    let _ = rmp::encode::write_map_len(out, len);
}

pub fn encode_ping(msg: &Ping) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    out.push(MessageType::Ping as u8);
    let mut count = 2u32;
    if !msg.source_addr.is_empty() {
        count += 1;
    }
    if msg.source_port != 0 {
        count += 1;
    }
    if !msg.source_node.is_empty() {
        count += 1;
    }
    write_map_len(&mut out, count);
    write_str_legacy(&mut out, "SeqNo");
    write_uint(&mut out, msg.seq_no as u64);
    write_str_legacy(&mut out, "Node");
    write_str_legacy(&mut out, &msg.node);
    if !msg.source_addr.is_empty() {
        write_str_legacy(&mut out, "SourceAddr");
        write_raw_legacy(&mut out, &msg.source_addr);
    }
    if msg.source_port != 0 {
        write_str_legacy(&mut out, "SourcePort");
        write_uint(&mut out, msg.source_port as u64);
    }
    if !msg.source_node.is_empty() {
        write_str_legacy(&mut out, "SourceNode");
        write_str_legacy(&mut out, &msg.source_node);
    }
    out
}

pub fn encode_indirect_ping(msg: &IndirectPingReq) -> Vec<u8> {
    let mut out = Vec::with_capacity(96);
    out.push(MessageType::IndirectPing as u8);
    let mut count = 5u32;
    if !msg.source_addr.is_empty() {
        count += 1;
    }
    if msg.source_port != 0 {
        count += 1;
    }
    if !msg.source_node.is_empty() {
        count += 1;
    }
    write_map_len(&mut out, count);
    write_str_legacy(&mut out, "SeqNo");
    write_uint(&mut out, msg.seq_no as u64);
    write_str_legacy(&mut out, "Target");
    write_raw_legacy(&mut out, &msg.target);
    write_str_legacy(&mut out, "Port");
    write_uint(&mut out, msg.port as u64);
    write_str_legacy(&mut out, "Node");
    write_str_legacy(&mut out, &msg.node);
    write_str_legacy(&mut out, "Nack");
    write_bool(&mut out, msg.nack);
    if !msg.source_addr.is_empty() {
        write_str_legacy(&mut out, "SourceAddr");
        write_raw_legacy(&mut out, &msg.source_addr);
    }
    if msg.source_port != 0 {
        write_str_legacy(&mut out, "SourcePort");
        write_uint(&mut out, msg.source_port as u64);
    }
    if !msg.source_node.is_empty() {
        write_str_legacy(&mut out, "SourceNode");
        write_str_legacy(&mut out, &msg.source_node);
    }
    out
}

pub fn encode_ack_resp(msg: &AckResp) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    out.push(MessageType::AckResp as u8);
    write_map_len(&mut out, 2);
    write_str_legacy(&mut out, "SeqNo");
    write_uint(&mut out, msg.seq_no as u64);
    write_str_legacy(&mut out, "Payload");
    write_opt_bytes_legacy(&mut out, msg.payload.as_deref());
    out
}

pub fn encode_nack_resp(msg: &NackResp) -> Vec<u8> {
    let mut out = Vec::with_capacity(16);
    out.push(MessageType::NackResp as u8);
    write_map_len(&mut out, 1);
    write_str_legacy(&mut out, "SeqNo");
    write_uint(&mut out, msg.seq_no as u64);
    out
}

pub fn encode_err_resp(msg: &ErrResp) -> Vec<u8> {
    let mut out = Vec::with_capacity(32 + msg.error.len());
    out.push(MessageType::Err as u8);
    write_map_len(&mut out, 1);
    write_str_legacy(&mut out, "Error");
    write_str_legacy(&mut out, &msg.error);
    out
}

pub fn encode_suspect(msg: &Suspect) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    out.push(MessageType::Suspect as u8);
    write_map_len(&mut out, 3);
    write_str_legacy(&mut out, "Incarnation");
    write_uint(&mut out, msg.incarnation as u64);
    write_str_legacy(&mut out, "Node");
    write_str_legacy(&mut out, &msg.node);
    write_str_legacy(&mut out, "From");
    write_str_legacy(&mut out, &msg.from);
    out
}

pub fn encode_alive(msg: &Alive) -> Vec<u8> {
    let mut out = Vec::with_capacity(96);
    out.push(MessageType::Alive as u8);
    write_map_len(&mut out, 6);
    write_str_legacy(&mut out, "Incarnation");
    write_uint(&mut out, msg.incarnation as u64);
    write_str_legacy(&mut out, "Node");
    write_str_legacy(&mut out, &msg.node);
    write_str_legacy(&mut out, "Addr");
    write_raw_legacy(&mut out, &msg.addr);
    write_str_legacy(&mut out, "Port");
    write_uint(&mut out, msg.port as u64);
    write_str_legacy(&mut out, "Meta");
    write_opt_bytes_legacy(&mut out, msg.meta.as_deref());
    write_str_legacy(&mut out, "Vsn");
    write_raw_legacy(&mut out, &msg.vsn);
    out
}

pub fn encode_dead(msg: &Dead) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    out.push(MessageType::Dead as u8);
    write_map_len(&mut out, 3);
    write_str_legacy(&mut out, "Incarnation");
    write_uint(&mut out, msg.incarnation as u64);
    write_str_legacy(&mut out, "Node");
    write_str_legacy(&mut out, &msg.node);
    write_str_legacy(&mut out, "From");
    write_str_legacy(&mut out, &msg.from);
    out
}

pub fn encode_push_pull_header(out: &mut Vec<u8>, hdr: &PushPullHeader) {
    write_map_len(out, 3);
    write_str_legacy(out, "Nodes");
    write_uint(out, hdr.nodes as u64);
    write_str_legacy(out, "UserStateLen");
    write_uint(out, hdr.user_state_len as u64);
    write_str_legacy(out, "Join");
    write_bool(out, hdr.join);
}

pub fn encode_push_node_state(out: &mut Vec<u8>, ns: &PushNodeState) {
    write_map_len(out, 7);
    write_str_legacy(out, "Name");
    write_str_legacy(out, &ns.name);
    write_str_legacy(out, "Addr");
    write_raw_legacy(out, &ns.addr);
    write_str_legacy(out, "Port");
    write_uint(out, ns.port as u64);
    write_str_legacy(out, "Meta");
    write_opt_bytes_legacy(out, ns.meta.as_deref());
    write_str_legacy(out, "Incarnation");
    write_uint(out, ns.incarnation as u64);
    write_str_legacy(out, "State");
    write_uint(out, ns.state as u64);
    write_str_legacy(out, "Vsn");
    write_raw_legacy(out, &ns.vsn);
}

pub fn encode_user_msg_header(out: &mut Vec<u8>, hdr: &UserMsgHeader) {
    write_map_len(out, 1);
    write_str_legacy(out, "UserMsgLen");
    write_uint(out, hdr.user_msg_len as u64);
}

pub fn encode_push_pull(
    nodes: &[PushNodeState],
    user_state: &[u8],
    join: bool,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(64 + nodes.len() * 64 + user_state.len());
    out.push(MessageType::PushPull as u8);
    encode_push_pull_header(
        &mut out,
        &PushPullHeader {
            nodes: nodes.len(),
            user_state_len: user_state.len(),
            join,
        },
    );
    for n in nodes {
        encode_push_node_state(&mut out, n);
    }
    out.extend_from_slice(user_state);
    out
}

pub fn encode_user_msg_stream(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(24 + payload.len());
    out.push(MessageType::User as u8);
    encode_user_msg_header(
        &mut out,
        &UserMsgHeader {
            user_msg_len: payload.len(),
        },
    );
    out.extend_from_slice(payload);
    out
}

pub fn compress_payload(algo: CompressionType, input: &[u8]) -> Result<Vec<u8>, String> {
    let encoded = match algo {
        CompressionType::Lzw => lzw_compress(input),
        CompressionType::Snappy => snap::raw::Encoder::new()
            .compress_vec(input)
            .map_err(|e| format!("snappy compress: {e}"))?,
    };
    let mut out = Vec::with_capacity(encoded.len() + 24);
    out.push(MessageType::Compress as u8);
    write_map_len(&mut out, 2);
    write_str_legacy(&mut out, "Algo");
    write_uint(&mut out, algo as u64);
    write_str_legacy(&mut out, "Buf");
    write_raw_legacy(&mut out, &encoded);
    Ok(out)
}

pub fn decompress_buffer(algo: CompressionType, buf: &[u8]) -> Result<Vec<u8>, String> {
    match algo {
        CompressionType::Lzw => lzw_decompress(buf, MAX_DECOMPRESS_BYTES),
        CompressionType::Snappy => {
            let decoded_len = snap::raw::decompress_len(buf)
                .map_err(|e| format!("snappy decompress_len: {e}"))?;
            if decoded_len > MAX_DECOMPRESS_BYTES {
                return Err(format!(
                    "memberlist: snappy-decompressed payload would exceed {MAX_DECOMPRESS_BYTES} bytes (claimed {decoded_len})"
                ));
            }
            snap::raw::Decoder::new()
                .decompress_vec(buf)
                .map_err(|e| format!("snappy decompress: {e}"))
        }
    }
}

// --- MessagePack Reader ---

struct MpReader<'a> {
    buf: &'a [u8],
}

impl<'a> MpReader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    fn remaining(&self) -> &'a [u8] {
        self.buf
    }

    fn read_byte(&mut self) -> Result<u8, String> {
        if self.buf.is_empty() {
            return Err("msgpack: unexpected EOF".to_string());
        }
        let b = self.buf[0];
        self.buf = &self.buf[1..];
        Ok(b)
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8], String> {
        if self.buf.len() < len {
            return Err(format!(
                "msgpack: unexpected EOF (need {len}, have {})",
                self.buf.len()
            ));
        }
        let (head, tail) = self.buf.split_at(len);
        self.buf = tail;
        Ok(head)
    }

    fn read_u16_be(&mut self) -> Result<u16, String> {
        let b = self.read_exact(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }

    fn read_u32_be(&mut self) -> Result<u32, String> {
        let b = self.read_exact(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn read_u64_be(&mut self) -> Result<u64, String> {
        let b = self.read_exact(8)?;
        Ok(u64::from_be_bytes(b.try_into().expect("8 bytes")))
    }

    fn read_map_len(&mut self) -> Result<usize, String> {
        let bd = self.read_byte()?;
        match bd {
            0x80..=0x8f => Ok((bd & 0x0f) as usize),
            0xde => Ok(self.read_u16_be()? as usize),
            0xdf => Ok(self.read_u32_be()? as usize),
            other => Err(format!("msgpack: expected map, got 0x{other:02x}")),
        }
    }

    fn read_u64(&mut self) -> Result<u64, String> {
        let bd = self.read_byte()?;
        match bd {
            0x00..=0x7f => Ok(bd as u64),
            0xcc => Ok(self.read_byte()? as u64),
            0xcd => Ok(self.read_u16_be()? as u64),
            0xce => Ok(self.read_u32_be()? as u64),
            0xcf => self.read_u64_be(),
            0xd0 => {
                let v = self.read_byte()? as i8;
                u64::try_from(v).map_err(|_| "negative int for uint".to_string())
            }
            0xd1 => {
                let v = self.read_u16_be()? as i16;
                u64::try_from(v).map_err(|_| "negative int for uint".to_string())
            }
            0xd2 => {
                let v = self.read_u32_be()? as i32;
                u64::try_from(v).map_err(|_| "negative int for uint".to_string())
            }
            0xd3 => {
                let v = self.read_u64_be()? as i64;
                u64::try_from(v).map_err(|_| "negative int for uint".to_string())
            }
            other => Err(format!("msgpack: expected uint, got 0x{other:02x}")),
        }
    }

    fn read_bool(&mut self) -> Result<bool, String> {
        let bd = self.read_byte()?;
        match bd {
            0xc2 | 0x00 => Ok(false),
            0xc3 | 0x01 => Ok(true),
            other => Err(format!("msgpack: expected bool, got 0x{other:02x}")),
        }
    }

    fn read_opt_bytes(&mut self) -> Result<Option<Vec<u8>>, String> {
        let bd = self.read_byte()?;
        match bd {
            0xc0 => Ok(None),
            0xa0..=0xbf => {
                let len = (bd & 0x1f) as usize;
                Ok(Some(self.read_exact(len)?.to_vec()))
            }
            0xd9 | 0xc4 => {
                let len = self.read_byte()? as usize;
                Ok(Some(self.read_exact(len)?.to_vec()))
            }
            0xda | 0xc5 => {
                let len = self.read_u16_be()? as usize;
                Ok(Some(self.read_exact(len)?.to_vec()))
            }
            0xdb | 0xc6 => {
                let len = self.read_u32_be()? as usize;
                Ok(Some(self.read_exact(len)?.to_vec()))
            }
            0x90..=0x9f => {
                let len = (bd & 0x0f) as usize;
                let mut out = Vec::with_capacity(len);
                for _ in 0..len {
                    out.push(self.read_u64()? as u8);
                }
                Ok(Some(out))
            }
            0xdc => {
                let len = self.read_u16_be()? as usize;
                let mut out = Vec::with_capacity(len);
                for _ in 0..len {
                    out.push(self.read_u64()? as u8);
                }
                Ok(Some(out))
            }
            other => Err(format!("msgpack: expected bytes/str/nil, got 0x{other:02x}")),
        }
    }

    fn read_bytes(&mut self) -> Result<Vec<u8>, String> {
        Ok(self.read_opt_bytes()?.unwrap_or_default())
    }

    fn read_string(&mut self) -> Result<String, String> {
        let b = self.read_bytes()?;
        String::from_utf8(b).map_err(|e| format!("msgpack utf8 string: {e}"))
    }

    fn skip_value(&mut self) -> Result<(), String> {
        self.skip_value_depth(0)
    }

    fn skip_value_depth(&mut self, depth: usize) -> Result<(), String> {
        if depth >= MAX_MP_DEPTH {
            return Err(format!("msgpack nesting depth exceeds max {MAX_MP_DEPTH}"));
        }
        let bd = self.read_byte()?;
        match bd {
            0x00..=0x7f | 0xc0..=0xc3 | 0xe0..=0xff => Ok(()),
            0xcc | 0xd0 => {
                self.read_exact(1)?;
                Ok(())
            }
            0xcd | 0xd1 => {
                self.read_exact(2)?;
                Ok(())
            }
            0xce | 0xd2 | 0xca => {
                self.read_exact(4)?;
                Ok(())
            }
            0xcf | 0xd3 | 0xcb => {
                self.read_exact(8)?;
                Ok(())
            }
            0xa0..=0xbf => {
                let n = (bd & 0x1f) as usize;
                self.read_exact(n)?;
                Ok(())
            }
            0xd9 | 0xc4 => {
                let n = self.read_byte()? as usize;
                self.read_exact(n)?;
                Ok(())
            }
            0xda | 0xc5 => {
                let n = self.read_u16_be()? as usize;
                self.read_exact(n)?;
                Ok(())
            }
            0xdb | 0xc6 => {
                let n = self.read_u32_be()? as usize;
                self.read_exact(n)?;
                Ok(())
            }
            0x90..=0x9f => {
                let n = (bd & 0x0f) as usize;
                if n > self.buf.len() {
                    return Err("msgpack: array truncated".to_string());
                }
                for _ in 0..n {
                    self.skip_value_depth(depth + 1)?;
                }
                Ok(())
            }
            0xdc => {
                let n = self.read_u16_be()? as usize;
                if n > self.buf.len() {
                    return Err("msgpack: array16 truncated".to_string());
                }
                for _ in 0..n {
                    self.skip_value_depth(depth + 1)?;
                }
                Ok(())
            }
            0xdd => {
                let n = self.read_u32_be()? as usize;
                if n > self.buf.len() {
                    return Err("msgpack: array32 truncated".to_string());
                }
                for _ in 0..n {
                    self.skip_value_depth(depth + 1)?;
                }
                Ok(())
            }
            0x80..=0x8f => {
                let n = (bd & 0x0f) as usize;
                if n.saturating_mul(2) > self.buf.len() {
                    return Err("msgpack: map truncated".to_string());
                }
                for _ in 0..n {
                    self.skip_value_depth(depth + 1)?;
                    self.skip_value_depth(depth + 1)?;
                }
                Ok(())
            }
            0xde => {
                let n = self.read_u16_be()? as usize;
                if n.saturating_mul(2) > self.buf.len() {
                    return Err("msgpack: map16 truncated".to_string());
                }
                for _ in 0..n {
                    self.skip_value_depth(depth + 1)?;
                    self.skip_value_depth(depth + 1)?;
                }
                Ok(())
            }
            0xdf => {
                let n = self.read_u32_be()? as usize;
                if n.saturating_mul(2) > self.buf.len() {
                    return Err("msgpack: map32 truncated".to_string());
                }
                for _ in 0..n {
                    self.skip_value_depth(depth + 1)?;
                    self.skip_value_depth(depth + 1)?;
                }
                Ok(())
            }
            0xd4 => {
                self.read_exact(2)?;
                Ok(())
            }
            0xd5 => {
                self.read_exact(3)?;
                Ok(())
            }
            0xd6 => {
                self.read_exact(5)?;
                Ok(())
            }
            0xd7 => {
                self.read_exact(9)?;
                Ok(())
            }
            0xd8 => {
                self.read_exact(17)?;
                Ok(())
            }
            0xc7 => {
                let n = self.read_byte()? as usize;
                let total = 1usize
                    .checked_add(n)
                    .ok_or_else(|| "msgpack: ext8 length overflow".to_string())?;
                self.read_exact(total)?;
                Ok(())
            }
            0xc8 => {
                let n = self.read_u16_be()? as usize;
                let total = 1usize
                    .checked_add(n)
                    .ok_or_else(|| "msgpack: ext16 length overflow".to_string())?;
                self.read_exact(total)?;
                Ok(())
            }
            0xc9 => {
                let n = self.read_u32_be()? as usize;
                let total = 1usize
                    .checked_add(n)
                    .ok_or_else(|| "msgpack: ext32 length overflow".to_string())?;
                self.read_exact(total)?;
                Ok(())
            }
        }
    }
}

pub fn decode_ping(buf: &[u8]) -> Result<Ping, String> {
    let mut r = MpReader::new(buf);
    let len = r.read_map_len()?;
    let mut out = Ping::default();
    for _ in 0..len {
        let key = r.read_string()?;
        match key.as_str() {
            "SeqNo" => out.seq_no = r.read_u64()? as u32,
            "Node" => out.node = r.read_string()?,
            "SourceAddr" => out.source_addr = r.read_bytes()?,
            "SourcePort" => out.source_port = r.read_u64()? as u16,
            "SourceNode" => out.source_node = r.read_string()?,
            _ => r.skip_value()?,
        }
    }
    Ok(out)
}

pub fn decode_indirect_ping(buf: &[u8]) -> Result<IndirectPingReq, String> {
    let mut r = MpReader::new(buf);
    let len = r.read_map_len()?;
    let mut out = IndirectPingReq::default();
    for _ in 0..len {
        let key = r.read_string()?;
        match key.as_str() {
            "SeqNo" => out.seq_no = r.read_u64()? as u32,
            "Target" => out.target = r.read_bytes()?,
            "Port" => out.port = r.read_u64()? as u16,
            "Node" => out.node = r.read_string()?,
            "Nack" => out.nack = r.read_bool()?,
            "SourceAddr" => out.source_addr = r.read_bytes()?,
            "SourcePort" => out.source_port = r.read_u64()? as u16,
            "SourceNode" => out.source_node = r.read_string()?,
            _ => r.skip_value()?,
        }
    }
    Ok(out)
}

pub fn decode_ack_resp(buf: &[u8]) -> Result<AckResp, String> {
    let mut r = MpReader::new(buf);
    let len = r.read_map_len()?;
    let mut out = AckResp::default();
    for _ in 0..len {
        let key = r.read_string()?;
        match key.as_str() {
            "SeqNo" => out.seq_no = r.read_u64()? as u32,
            "Payload" => out.payload = r.read_opt_bytes()?,
            _ => r.skip_value()?,
        }
    }
    Ok(out)
}

pub fn decode_nack_resp(buf: &[u8]) -> Result<NackResp, String> {
    let mut r = MpReader::new(buf);
    let len = r.read_map_len()?;
    let mut out = NackResp::default();
    for _ in 0..len {
        let key = r.read_string()?;
        match key.as_str() {
            "SeqNo" => out.seq_no = r.read_u64()? as u32,
            _ => r.skip_value()?,
        }
    }
    Ok(out)
}

pub fn decode_err_resp(buf: &[u8]) -> Result<ErrResp, String> {
    let mut r = MpReader::new(buf);
    let len = r.read_map_len()?;
    let mut out = ErrResp::default();
    for _ in 0..len {
        let key = r.read_string()?;
        match key.as_str() {
            "Error" => out.error = r.read_string()?,
            _ => r.skip_value()?,
        }
    }
    Ok(out)
}

pub fn decode_suspect(buf: &[u8]) -> Result<Suspect, String> {
    let mut r = MpReader::new(buf);
    let len = r.read_map_len()?;
    let mut out = Suspect::default();
    for _ in 0..len {
        let key = r.read_string()?;
        match key.as_str() {
            "Incarnation" => out.incarnation = r.read_u64()? as u32,
            "Node" => out.node = r.read_string()?,
            "From" => out.from = r.read_string()?,
            _ => r.skip_value()?,
        }
    }
    Ok(out)
}

pub fn decode_alive(buf: &[u8]) -> Result<Alive, String> {
    let mut r = MpReader::new(buf);
    let len = r.read_map_len()?;
    let mut out = Alive::default();
    for _ in 0..len {
        let key = r.read_string()?;
        match key.as_str() {
            "Incarnation" => out.incarnation = r.read_u64()? as u32,
            "Node" => out.node = r.read_string()?,
            "Addr" => out.addr = r.read_bytes()?,
            "Port" => out.port = r.read_u64()? as u16,
            "Meta" => out.meta = r.read_opt_bytes()?,
            "Vsn" => out.vsn = r.read_bytes()?,
            _ => r.skip_value()?,
        }
    }
    Ok(out)
}

pub fn decode_dead(buf: &[u8]) -> Result<Dead, String> {
    let mut r = MpReader::new(buf);
    let len = r.read_map_len()?;
    let mut out = Dead::default();
    for _ in 0..len {
        let key = r.read_string()?;
        match key.as_str() {
            "Incarnation" => out.incarnation = r.read_u64()? as u32,
            "Node" => out.node = r.read_string()?,
            "From" => out.from = r.read_string()?,
            _ => r.skip_value()?,
        }
    }
    Ok(out)
}

pub fn decode_compressed_payload(buf: &[u8]) -> Result<CompressedPayload, String> {
    let mut r = MpReader::new(buf);
    let len = r.read_map_len()?;
    let mut algo_raw: Option<u8> = None;
    let mut payload = Vec::new();
    for _ in 0..len {
        let key = r.read_string()?;
        match key.as_str() {
            "Algo" => algo_raw = Some(r.read_u64()? as u8),
            "Buf" => payload = r.read_bytes()?,
            _ => r.skip_value()?,
        }
    }
    let algo_u8 = algo_raw.ok_or_else(|| "compressedPayload missing Algo".to_string())?;
    let algo = CompressionType::try_from(algo_u8)
        .map_err(|a| format!("cannot decompress unknown algorithm {a}"))?;
    Ok(CompressedPayload { algo, buf: payload })
}

pub fn decode_push_pull(buf: &[u8]) -> Result<(bool, Vec<PushNodeState>, &[u8]), String> {
    let mut r = MpReader::new(buf);
    let hlen = r.read_map_len()?;
    let mut hdr = PushPullHeader::default();
    for _ in 0..hlen {
        let key = r.read_string()?;
        match key.as_str() {
            "Nodes" => {
                let raw = r.read_u64()?;
                if raw > MAX_PUSH_PULL_NODES as u64 {
                    return Err(format!(
                        "push/pull node count {raw} exceeds max {MAX_PUSH_PULL_NODES}"
                    ));
                }
                hdr.nodes = raw as usize;
            }
            "UserStateLen" => {
                let raw = r.read_u64()?;
                if raw > MAX_PUSH_STATE_BYTES as u64 {
                    return Err(format!("user state length {raw} exceeds limit"));
                }
                hdr.user_state_len = raw as usize;
            }
            "Join" => hdr.join = r.read_bool()?,
            _ => r.skip_value()?,
        }
    }

    let mut nodes = Vec::with_capacity(hdr.nodes.min(4096));
    for _ in 0..hdr.nodes {
        let nlen = r.read_map_len()?;
        let mut ns = PushNodeState {
            name: String::new(),
            addr: Vec::new(),
            port: 0,
            meta: None,
            incarnation: 0,
            state: NodeStateType::Alive,
            vsn: Vec::new(),
        };
        for _ in 0..nlen {
            let key = r.read_string()?;
            match key.as_str() {
                "Name" => ns.name = r.read_string()?,
                "Addr" => ns.addr = r.read_bytes()?,
                "Port" => ns.port = r.read_u64()? as u16,
                "Meta" => ns.meta = r.read_opt_bytes()?,
                "Incarnation" => ns.incarnation = r.read_u64()? as u32,
                "State" => {
                    let s = r.read_u64()? as u8;
                    ns.state = NodeStateType::try_from(s)
                        .map_err(|v| format!("invalid NodeStateType {v}"))?;
                }
                "Vsn" => ns.vsn = r.read_bytes()?,
                _ => r.skip_value()?,
            }
        }
        nodes.push(ns);
    }

    let rem = r.remaining();
    if rem.len() < hdr.user_state_len {
        return Err(format!(
            "failed to read full user state ({} / {})",
            rem.len(),
            hdr.user_state_len
        ));
    }
    Ok((hdr.join, nodes, &rem[..hdr.user_state_len]))
}

pub fn decode_user_msg_stream(buf: &[u8]) -> Result<&[u8], String> {
    let mut r = MpReader::new(buf);
    let hlen = r.read_map_len()?;
    let mut hdr = UserMsgHeader::default();
    for _ in 0..hlen {
        let key = r.read_string()?;
        match key.as_str() {
            "UserMsgLen" => hdr.user_msg_len = r.read_u64()? as usize,
            _ => r.skip_value()?,
        }
    }
    let rem = r.remaining();
    if rem.len() < hdr.user_msg_len {
        return Err(format!(
            "failed to read full user message ({} / {})",
            rem.len(),
            hdr.user_msg_len
        ));
    }
    Ok(&rem[..hdr.user_msg_len])
}

// --- Framing helpers: label, CRC32, compound ---

pub fn add_label_header(buf: &[u8], label: &str) -> Result<Vec<u8>, String> {
    if label.is_empty() {
        return Ok(buf.to_vec());
    }
    if label.len() > LABEL_MAX_SIZE {
        return Err(format!("label {label:?} is too long"));
    }
    let mut out = Vec::with_capacity(2 + label.len() + buf.len());
    out.push(MessageType::HasLabel as u8);
    out.push(label.len() as u8);
    out.extend_from_slice(label.as_bytes());
    out.extend_from_slice(buf);
    Ok(out)
}

pub fn remove_label_header(buf: &[u8]) -> Result<(&[u8], &str), String> {
    if buf.is_empty() {
        return Ok((buf, ""));
    }
    if buf[0] != MessageType::HasLabel as u8 {
        return Ok((buf, ""));
    }
    if buf.len() < 2 {
        return Err("cannot decode label; packet has been truncated".to_string());
    }
    let size = buf[1] as usize;
    if size < 1 {
        return Err("label header cannot be empty when present".to_string());
    }
    if buf.len() < 2 + size {
        return Err("cannot decode label; packet has been truncated".to_string());
    }
    let label = std::str::from_utf8(&buf[2..2 + size])
        .map_err(|e| format!("invalid utf8 label: {e}"))?;
    Ok((&buf[2 + size..], label))
}

pub fn wrap_crc32(msg: &[u8]) -> Vec<u8> {
    let crc = crc32_ieee(msg);
    let mut out = Vec::with_capacity(5 + msg.len());
    out.push(MessageType::HasCrc as u8);
    out.extend_from_slice(&crc.to_be_bytes());
    out.extend_from_slice(msg);
    out
}

pub fn unwrap_crc32(buf: &[u8]) -> Result<&[u8], String> {
    if buf.len() >= 5 && buf[0] == MessageType::HasCrc as u8 {
        let expected = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]);
        let payload = &buf[5..];
        let actual = crc32_ieee(payload);
        if actual != expected {
            return Err(format!(
                "invalid checksum for packet: {actual:x}, expected {expected:x}"
            ));
        }
        Ok(payload)
    } else {
        Ok(buf)
    }
}

pub fn make_compound_message(msgs: &[&[u8]]) -> Vec<u8> {
    let total = 2 + 2 * msgs.len() + msgs.iter().map(|m| m.len()).sum::<usize>();
    let mut out = Vec::with_capacity(total);
    out.push(MessageType::Compound as u8);
    out.push(msgs.len() as u8);
    for m in msgs {
        let _ = out.write_all(&(m.len() as u16).to_be_bytes());
    }
    for m in msgs {
        out.extend_from_slice(m);
    }
    out
}

pub fn make_compound_messages(msgs: &[Vec<u8>]) -> Vec<Vec<u8>> {
    const MAX_MSGS: usize = u8::MAX as usize;
    const MAX_MSG_LEN: usize = u16::MAX as usize;

    let mut bufs = Vec::new();
    let mut small: Vec<&[u8]> = Vec::new();

    for m in msgs {
        if m.len() <= MAX_MSG_LEN {
            small.push(m.as_slice());
        } else {
            bufs.push(m.clone());
        }
    }

    for chunk in small.chunks(MAX_MSGS) {
        bufs.push(make_compound_message(chunk));
    }
    bufs
}

pub fn decode_compound_message(buf: &[u8]) -> Result<(usize, Vec<&[u8]>), String> {
    if buf.is_empty() {
        return Err("missing compound length byte".to_string());
    }
    let num_parts = buf[0] as usize;
    let mut rest = &buf[1..];
    if rest.len() < num_parts * 2 {
        return Err("truncated len slice".to_string());
    }

    let mut lengths = Vec::with_capacity(num_parts);
    for i in 0..num_parts {
        lengths.push(u16::from_be_bytes([rest[i * 2], rest[i * 2 + 1]]) as usize);
    }
    rest = &rest[num_parts * 2..];

    let mut parts = Vec::with_capacity(num_parts);
    for (idx, msg_len) in lengths.into_iter().enumerate() {
        if rest.len() < msg_len {
            return Ok((num_parts - idx, parts));
        }
        let (part, tail) = rest.split_at(msg_len);
        parts.push(part);
        rest = tail;
    }
    Ok((0, parts))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ping_ack_roundtrip() {
        let p = Ping {
            seq_no: 42,
            node: "node-1".to_string(),
            source_addr: vec![127, 0, 0, 1],
            source_port: 7946,
            source_node: "node-0".to_string(),
        };
        let enc = encode_ping(&p);
        assert_eq!(enc[0], MessageType::Ping as u8);
        let dec = decode_ping(&enc[1..]).unwrap();
        assert_eq!(dec, p);

        let ack = AckResp {
            seq_no: 42,
            payload: Some(b"ok".to_vec()),
        };
        let aenc = encode_ack_resp(&ack);
        assert_eq!(aenc[0], MessageType::AckResp as u8);
        let adec = decode_ack_resp(&aenc[1..]).unwrap();
        assert_eq!(adec, ack);
    }

    #[test]
    fn test_alive_and_push_pull_roundtrip() {
        let ns = PushNodeState {
            name: "rust-1".to_string(),
            addr: vec![127, 0, 0, 1],
            port: 7946,
            meta: Some(vec![1, 1, 0]),
            incarnation: 3,
            state: NodeStateType::Alive,
            vsn: vec![1, 5, 2, 0, 0, 0],
        };
        let user = b"hello-user-state";
        let enc = encode_push_pull(std::slice::from_ref(&ns), user, true);
        assert_eq!(enc[0], MessageType::PushPull as u8);
        let (join, nodes, state) = decode_push_pull(&enc[1..]).unwrap();
        assert!(join);
        assert_eq!(nodes, vec![ns]);
        assert_eq!(state, user);
    }

    #[test]
    fn test_compound_and_crc_and_compress() {
        let m1 = encode_nack_resp(&NackResp { seq_no: 7 });
        let m2 = encode_dead(&Dead {
            incarnation: 9,
            node: "a".to_string(),
            from: "b".to_string(),
        });
        let comp = make_compound_message(&[&m1, &m2]);
        assert_eq!(comp[0], MessageType::Compound as u8);
        let (trunc, parts) = decode_compound_message(&comp[1..]).unwrap();
        assert_eq!(trunc, 0);
        assert_eq!(parts, vec![m1.as_slice(), m2.as_slice()]);

        let lzw = compress_payload(CompressionType::Lzw, &comp).unwrap();
        assert_eq!(lzw[0], MessageType::Compress as u8);
        let cp = decode_compressed_payload(&lzw[1..]).unwrap();
        let decomp = decompress_buffer(cp.algo, &cp.buf).unwrap();
        assert_eq!(decomp, comp);

        let crc_wrapped = wrap_crc32(&lzw);
        assert_eq!(crc_wrapped[0], MessageType::HasCrc as u8);
        let unwrapped = unwrap_crc32(&crc_wrapped).unwrap();
        assert_eq!(unwrapped, lzw.as_slice());
    }

    #[test]
    fn test_skip_value_rejects_deep_nesting_and_4gib_ext32() {
        // 64-deep nested fixarray1 (0x91) ending with 0x00.
        let mut nested = vec![0x91u8; 64];
        nested.push(0x00);
        let mut r = MpReader::new(&nested);
        let err = r.skip_value().unwrap_err();
        assert!(err.contains("depth"), "unexpected error: {err}");

        // 0xc9 ext32 claiming 4 GiB payload on a 5-byte buffer.
        let ext32_bomb = [0xc9u8, 0xff, 0xff, 0xff, 0xff];
        let mut r = MpReader::new(&ext32_bomb);
        assert!(r.skip_value().is_err());

        // 0xdd array32 claiming u32::MAX elements on a 5-byte buffer fails fast.
        let arr32_bomb = [0xddu8, 0xff, 0xff, 0xff, 0xff];
        let mut r = MpReader::new(&arr32_bomb);
        assert!(r.skip_value().is_err());
    }
}
