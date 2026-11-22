//! Streaming MessagePack readers for memberlist stream connections (never block waiting for EOF).
//!
//! Ported from github.com/hashicorp/memberlist/net.go (`readStream`, `readRemoteState`, `readUserMsg`).

use std::io;
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncReadExt};
use tracing::warn;

use crate::kv::BoxFuture;

use super::gometrics::GoMetrics;
use super::wire::{
    decompress_buffer, MessageType, NodeStateType, PushNodeState, PushPullHeader, MAX_MP_DEPTH,
    MAX_PUSH_PULL_NODES, MAX_PUSH_STATE_BYTES,
};

pub(super) fn default_hostname() -> String {
    #[cfg(target_os = "linux")]
    if let Ok(s) = std::fs::read_to_string("/proc/sys/kernel/hostname") {
        let t = s.trim();
        if !t.is_empty() {
            return t.to_string();
        }
    }
    std::env::var("HOSTNAME").unwrap_or_else(|_| "localhost".to_string())
}

// --- Streaming MessagePack readers (never block waiting for EOF) ---

/// Callback decrypting memberlist's `encryptMsg` stream payload given `(ciphertext, aad)`.
pub(crate) type StreamDecryptFn =
    Arc<dyn Fn(&[u8], &[u8]) -> Result<Vec<u8>, String> + Send + Sync>;

/// memberlist `decryptRemoteState` (`hashicorp/memberlist/net.go:1150`): reads the 4-byte
/// encrypted payload length, emits `sample_memberlist_size_remote`, reads the ciphertext, and
/// invokes `decrypt` with authenticated data `[encryptMsg, len_be[0..4], stream_label...]`.
async fn decrypt_remote_state<R: AsyncRead + Unpin + Send>(
    reader: &mut R,
    stream_label: &str,
    go_metrics: Option<&GoMetrics>,
    decrypt: &StreamDecryptFn,
) -> Result<Vec<u8>, String> {
    let mut len_buf = [0u8; 4];
    reader
        .read_exact(&mut len_buf)
        .await
        .map_err(|e| format!("read encrypted state length: {e}"))?;
    let more_bytes = u32::from_be_bytes(len_buf);
    if let Some(gm) = go_metrics {
        gm.add_sample(&["memberlist", "size", "remote"], more_bytes as f64, &[]);
    }
    if more_bytes as usize > MAX_PUSH_STATE_BYTES {
        return Err(format!(
            "remote node state is larger than limit ({more_bytes})"
        ));
    }
    if (more_bytes as f64) > (0.6 * MAX_PUSH_STATE_BYTES as f64).floor() {
        warn!(
            size = more_bytes,
            limit = MAX_PUSH_STATE_BYTES,
            "memberlist: Remote node state size approaches limit"
        );
    }
    let mut cipher_bytes = vec![0u8; more_bytes as usize];
    if !cipher_bytes.is_empty() {
        reader
            .read_exact(&mut cipher_bytes)
            .await
            .map_err(|e| format!("read encrypted state payload: {e}"))?;
    }
    let mut aad = Vec::with_capacity(5 + stream_label.len());
    aad.push(MessageType::Encrypt as u8);
    aad.extend_from_slice(&len_buf);
    aad.extend_from_slice(stream_label.as_bytes());
    decrypt(&cipher_bytes, &aad)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamFirstByte {
    Eof,
    Next,
    Byte(u8),
}

/// memberlist `RemoveLabelHeaderFromStream`: reads and strips the optional `[HasLabel, len, label]`
/// prefix on an accepted inbound stream before checking the label and before reading/decompressing.
pub async fn remove_label_header_from_stream<R: AsyncRead + Unpin + Send>(
    reader: &mut R,
) -> Result<(String, StreamFirstByte), String> {
    let mut b = [0u8; 1];
    match reader.read_exact(&mut b).await {
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
            return Ok((String::new(), StreamFirstByte::Eof));
        }
        Err(e) => return Err(format!("read stream byte: {e}")),
    }

    if b[0] != MessageType::HasLabel as u8 {
        return Ok((String::new(), StreamFirstByte::Byte(b[0])));
    }

    let mut sz = [0u8; 1];
    match reader.read_exact(&mut sz).await {
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
            return Err("cannot decode label; stream has been truncated".to_string());
        }
        Err(e) => return Err(format!("read label size: {e}")),
    }
    if sz[0] == 0 {
        return Err("label header cannot be empty when present".to_string());
    }
    let mut lbuf = vec![0u8; sz[0] as usize];
    match reader.read_exact(&mut lbuf).await {
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
            return Err("cannot decode label; stream has been truncated".to_string());
        }
        Err(e) => return Err(format!("read label: {e}")),
    }
    let label = String::from_utf8(lbuf).map_err(|e| format!("label utf8: {e}"))?;
    Ok((label, StreamFirstByte::Next))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum StreamReadError {
    Eof,
    Other(String),
}

impl std::fmt::Display for StreamReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Eof => f.write_str("EOF"),
            Self::Other(s) => f.write_str(s),
        }
    }
}

impl From<String> for StreamReadError {
    fn from(s: String) -> Self {
        Self::Other(s)
    }
}

/// memberlist `readStream`: reads the stream message type (or uses `first_byte` if already
/// peeked by `remove_label_header_from_stream`), decrypts if `Encrypt`, and decompresses if `Compress`.
/// Does NOT strip `HasLabel` (Go `readStream` never strips label headers).
#[cfg(test)]
pub(super) async fn read_stream_with_first_byte<R: AsyncRead + Unpin + Send>(
    reader: &mut R,
    first_byte: Option<u8>,
    stream_label: &str,
    go_metrics: Option<&GoMetrics>,
    decrypt: Option<&StreamDecryptFn>,
) -> Result<(MessageType, Option<Vec<u8>>), StreamReadError> {
    read_stream_with_first_byte_verified(reader, first_byte, stream_label, go_metrics, decrypt, true).await
}

pub(super) async fn read_stream_with_first_byte_verified<R: AsyncRead + Unpin + Send>(
    reader: &mut R,
    first_byte: Option<u8>,
    stream_label: &str,
    go_metrics: Option<&GoMetrics>,
    decrypt: Option<&StreamDecryptFn>,
    gossip_verify_incoming: bool,
) -> Result<(MessageType, Option<Vec<u8>>), StreamReadError> {
    let first = match first_byte {
        Some(v) => v,
        None => {
            let mut b = [0u8; 1];
            match reader.read_exact(&mut b).await {
                Ok(_) => b[0],
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    return Err(StreamReadError::Eof);
                }
                Err(e) => return Err(StreamReadError::Other(format!("read stream byte: {e}"))),
            }
        }
    };

    let mut msg_type = MessageType::try_from(first)
        .map_err(|v| StreamReadError::Other(format!("unsupported stream msgType {v}")))?;

    let mut plain_body: Option<Vec<u8>> = None;
    if msg_type == MessageType::Encrypt {
        let Some(decrypt_fn) = decrypt else {
            return Err(StreamReadError::Other(
                "remote state is encrypted and encryption is not configured".to_string(),
            ));
        };
        let plain = decrypt_remote_state(reader, stream_label, go_metrics, decrypt_fn).await?;
        if plain.is_empty() {
            return Err(StreamReadError::Other(
                "empty decrypted stream payload".to_string(),
            ));
        }
        msg_type = MessageType::try_from(plain[0])
            .map_err(|v| StreamReadError::Other(format!("unsupported inner stream msgType {v}")))?;
        plain_body = Some(plain[1..].to_vec());
    } else if decrypt.is_some() && gossip_verify_incoming {
        return Err(StreamReadError::Other(
            "encryption is configured but remote state is not encrypted".to_string(),
        ));
    }

    if msg_type == MessageType::Compress {
        let raw_cp = match plain_body {
            Some(buf) => {
                let mut cur = io::Cursor::new(buf);
                read_msgpack_map_bytes(&mut cur).await
            }
            None => read_msgpack_map_bytes(reader).await,
        };
        let cp = match raw_cp.and_then(|b| super::wire::decode_compressed_payload(&b)) {
            Ok(cp) => cp,
            Err(err) => {
                if let Some(gm) = go_metrics {
                    let labels = [("algo", "unknown")];
                    gm.incr_counter(&["memberlist", "decompress", "attempts_total"], 1.0, &labels);
                    gm.incr_counter(&["memberlist", "decompress", "errors_total"], 1.0, &labels);
                }
                return Err(StreamReadError::Other(err));
            }
        };
        let algo_label = match cp.algo {
            super::wire::CompressionType::Snappy => "snappy",
            super::wire::CompressionType::Lzw => "lzw",
        };
        let labels = [("algo", algo_label)];
        if let Some(gm) = go_metrics {
            gm.incr_counter(&["memberlist", "decompress", "attempts_total"], 1.0, &labels);
        }
        let decomp = match decompress_buffer(cp.algo, &cp.buf) {
            Ok(d) => d,
            Err(err) => {
                if let Some(gm) = go_metrics {
                    gm.incr_counter(&["memberlist", "decompress", "errors_total"], 1.0, &labels);
                }
                return Err(StreamReadError::Other(err));
            }
        };
        if decomp.is_empty() {
            return Err(StreamReadError::Other(
                "empty decompressed stream payload".to_string(),
            ));
        }
        let inner_type = MessageType::try_from(decomp[0])
            .map_err(|v| StreamReadError::Other(format!("unsupported inner stream msgType {v}")))?;
        return Ok((inner_type, Some(decomp[1..].to_vec())));
    }

    Ok((msg_type, plain_body))
}

#[cfg(test)]
pub(super) async fn read_stream_header_with_metrics<R: AsyncRead + Unpin + Send>(
    reader: &mut R,
    go_metrics: Option<&GoMetrics>,
    decrypt: Option<&StreamDecryptFn>,
) -> Result<(MessageType, String, Option<Vec<u8>>), String> {
    let (label, first) = remove_label_header_from_stream(reader).await?;
    let first_byte = match first {
        StreamFirstByte::Byte(b) => Some(b),
        StreamFirstByte::Next => None,
        StreamFirstByte::Eof => return Err("read stream byte: eof".to_string()),
    };
    let (msg_type, body) =
        read_stream_with_first_byte(reader, first_byte, &label, go_metrics, decrypt)
            .await
            .map_err(|e| e.to_string())?;
    Ok((msg_type, label, body))
}



pub(super) async fn read_push_pull_from_stream<R: AsyncRead + Unpin + Send>(
    reader: &mut R,
) -> Result<(bool, Vec<PushNodeState>, Vec<u8>), String> {
    let hdr_bytes = read_msgpack_map_bytes(reader).await?;
    let mut cumulative_bytes = hdr_bytes.len();
    let hdr = parse_push_pull_header_only(&hdr_bytes)?;
    if hdr.nodes > MAX_PUSH_PULL_NODES {
        return Err(format!(
            "push/pull node count {} exceeds max {MAX_PUSH_PULL_NODES}",
            hdr.nodes
        ));
    }
    let mut nodes = Vec::with_capacity(hdr.nodes.min(4096));
    for _ in 0..hdr.nodes {
        let remaining = MAX_PUSH_STATE_BYTES.saturating_sub(cumulative_bytes);
        let node_bytes = read_msgpack_map_bytes_with_limit(reader, remaining).await?;
        cumulative_bytes = cumulative_bytes
            .checked_add(node_bytes.len())
            .ok_or_else(|| "push/pull byte count overflow".to_string())?;
        if cumulative_bytes > MAX_PUSH_STATE_BYTES {
            return Err(format!(
                "push/pull payload size {cumulative_bytes} exceeds max {MAX_PUSH_STATE_BYTES}"
            ));
        }
        let ns = parse_push_node_state_only(&node_bytes)?;
        nodes.push(ns);
    }
    if hdr.user_state_len > MAX_PUSH_STATE_BYTES.saturating_sub(cumulative_bytes) {
        return Err(format!(
            "user state length {} exceeds limit",
            hdr.user_state_len
        ));
    }
    let mut user_buf = vec![0u8; hdr.user_state_len];
    if hdr.user_state_len > 0 {
        reader
            .read_exact(&mut user_buf)
            .await
            .map_err(|e| format!("read user state: {e}"))?;
    }
    Ok((hdr.join, nodes, user_buf))
}

pub(super) async fn read_user_msg_from_stream<R: AsyncRead + Unpin + Send>(
    reader: &mut R,
) -> Result<Vec<u8>, String> {
    let hdr_bytes = read_msgpack_map_bytes(reader).await?;
    let len = parse_user_msg_len(&hdr_bytes)?;
    if len > MAX_PUSH_STATE_BYTES {
        return Err(format!("user msg len {len} exceeds limit"));
    }
    let mut out = vec![0u8; len];
    if len > 0 {
        reader
            .read_exact(&mut out)
            .await
            .map_err(|e| format!("read user msg payload: {e}"))?;
    }
    Ok(out)
}

fn parse_push_pull_header_only(bytes: &[u8]) -> Result<PushPullHeader, String> {
    // decode_push_pull with Nodes=0? No, if Nodes > 0 it expects node maps.
    // Let's write a tiny generic msgpack header helper in wire? Or reuse decode_push_pull by patching?
    // Even cleaner: let's add a helper right here using `read_mp_item_sync` or expose `decode_push_pull_header` in wire.rs!
    // Since we can call a small parser directly on `bytes`:
    let mut pos = 0usize;
    let map_len = read_sync_map_len(bytes, &mut pos)?;
    let mut hdr = PushPullHeader::default();
    for _ in 0..map_len {
        let key = read_sync_string(bytes, &mut pos)?;
        match key.as_str() {
            "Nodes" => {
                let raw = read_sync_u64(bytes, &mut pos)?;
                if raw > MAX_PUSH_PULL_NODES as u64 {
                    return Err(format!(
                        "push/pull node count {raw} exceeds max {MAX_PUSH_PULL_NODES}"
                    ));
                }
                hdr.nodes = raw as usize;
            }
            "UserStateLen" => {
                let raw = read_sync_u64(bytes, &mut pos)?;
                if raw > MAX_PUSH_STATE_BYTES as u64 {
                    return Err(format!("user state length {raw} exceeds limit"));
                }
                hdr.user_state_len = raw as usize;
            }
            "Join" => hdr.join = read_sync_bool(bytes, &mut pos)?,
            _ => skip_sync_val(bytes, &mut pos)?,
        }
    }
    Ok(hdr)
}

fn parse_user_msg_len(bytes: &[u8]) -> Result<usize, String> {
    let mut pos = 0usize;
    let map_len = read_sync_map_len(bytes, &mut pos)?;
    let mut len = 0usize;
    for _ in 0..map_len {
        let key = read_sync_string(bytes, &mut pos)?;
        match key.as_str() {
            "UserMsgLen" => len = read_sync_u64(bytes, &mut pos)? as usize,
            _ => skip_sync_val(bytes, &mut pos)?,
        }
    }
    Ok(len)
}

fn parse_push_node_state_only(bytes: &[u8]) -> Result<PushNodeState, String> {
    let mut pos = 0usize;
    let map_len = read_sync_map_len(bytes, &mut pos)?;
    let mut ns = PushNodeState {
        name: String::new(),
        addr: Vec::new(),
        port: 0,
        meta: None,
        incarnation: 0,
        state: NodeStateType::Alive,
        vsn: Vec::new(),
    };
    for _ in 0..map_len {
        let key = read_sync_string(bytes, &mut pos)?;
        match key.as_str() {
            "Name" => ns.name = read_sync_string(bytes, &mut pos)?,
            "Addr" => ns.addr = read_sync_bytes(bytes, &mut pos)?,
            "Port" => ns.port = read_sync_u64(bytes, &mut pos)? as u16,
            "Meta" => ns.meta = read_sync_opt_bytes(bytes, &mut pos)?,
            "Incarnation" => ns.incarnation = read_sync_u64(bytes, &mut pos)? as u32,
            "State" => {
                let s = read_sync_u64(bytes, &mut pos)? as u8;
                ns.state = NodeStateType::try_from(s).map_err(|v| format!("invalid state {v}"))?;
            }
            "Vsn" => ns.vsn = read_sync_bytes(bytes, &mut pos)?,
            _ => skip_sync_val(bytes, &mut pos)?,
        }
    }
    if ns.port == 0 {
        ns.port = 7946;
    }
    Ok(ns)
}

fn read_sync_map_len(b: &[u8], pos: &mut usize) -> Result<usize, String> {
    let bd = *b.get(*pos).ok_or("eof")?;
    *pos += 1;
    match bd {
        0x80..=0x8f => Ok((bd & 0x0f) as usize),
        0xde => {
            let s = b.get(*pos..*pos + 2).ok_or("eof")?;
            *pos += 2;
            Ok(u16::from_be_bytes([s[0], s[1]]) as usize)
        }
        0xdf => {
            let s = b.get(*pos..*pos + 4).ok_or("eof")?;
            *pos += 4;
            Ok(u32::from_be_bytes([s[0], s[1], s[2], s[3]]) as usize)
        }
        _ => Err("expected map".to_string()),
    }
}

fn read_sync_u64(b: &[u8], pos: &mut usize) -> Result<u64, String> {
    let bd = *b.get(*pos).ok_or("eof")?;
    *pos += 1;
    match bd {
        0x00..=0x7f => Ok(bd as u64),
        0xcc => {
            let v = *b.get(*pos).ok_or("eof")?;
            *pos += 1;
            Ok(v as u64)
        }
        0xcd => {
            let s = b.get(*pos..*pos + 2).ok_or("eof")?;
            *pos += 2;
            Ok(u16::from_be_bytes([s[0], s[1]]) as u64)
        }
        0xce => {
            let s = b.get(*pos..*pos + 4).ok_or("eof")?;
            *pos += 4;
            Ok(u32::from_be_bytes([s[0], s[1], s[2], s[3]]) as u64)
        }
        0xcf => {
            let s = b.get(*pos..*pos + 8).ok_or("eof")?;
            *pos += 8;
            Ok(u64::from_be_bytes(s.try_into().expect("8")))
        }
        0xd0 => {
            let v = *b.get(*pos).ok_or("eof")? as i8;
            *pos += 1;
            u64::try_from(v).map_err(|_| "negative int".to_string())
        }
        0xd1 => {
            let s = b.get(*pos..*pos + 2).ok_or("eof")?;
            *pos += 2;
            let v = i16::from_be_bytes([s[0], s[1]]);
            u64::try_from(v).map_err(|_| "negative int".to_string())
        }
        0xd2 => {
            let s = b.get(*pos..*pos + 4).ok_or("eof")?;
            *pos += 4;
            let v = i32::from_be_bytes([s[0], s[1], s[2], s[3]]);
            u64::try_from(v).map_err(|_| "negative int".to_string())
        }
        0xd3 => {
            let s = b.get(*pos..*pos + 8).ok_or("eof")?;
            *pos += 8;
            let v = i64::from_be_bytes(s.try_into().expect("8"));
            u64::try_from(v).map_err(|_| "negative int".to_string())
        }
        other => Err(format!("expected uint, got 0x{other:02x}")),
    }
}

fn read_sync_bool(b: &[u8], pos: &mut usize) -> Result<bool, String> {
    let bd = *b.get(*pos).ok_or("eof")?;
    *pos += 1;
    match bd {
        0xc2 | 0x00 => Ok(false),
        0xc3 | 0x01 => Ok(true),
        _ => Err("expected bool".to_string()),
    }
}

fn read_sync_opt_bytes(b: &[u8], pos: &mut usize) -> Result<Option<Vec<u8>>, String> {
    let bd = *b.get(*pos).ok_or("eof")?;
    *pos += 1;
    let len = match bd {
        0xc0 => return Ok(None),
        0xa0..=0xbf => (bd & 0x1f) as usize,
        0xd9 | 0xc4 => {
            let l = *b.get(*pos).ok_or("eof")? as usize;
            *pos += 1;
            l
        }
        0xda | 0xc5 => {
            let s = b.get(*pos..*pos + 2).ok_or("eof")?;
            *pos += 2;
            u16::from_be_bytes([s[0], s[1]]) as usize
        }
        0xdb | 0xc6 => {
            let s = b.get(*pos..*pos + 4).ok_or("eof")?;
            *pos += 4;
            u32::from_be_bytes([s[0], s[1], s[2], s[3]]) as usize
        }
        _ => return Err("expected bytes".to_string()),
    };
    let s = b.get(*pos..*pos + len).ok_or("eof")?;
    *pos += len;
    Ok(Some(s.to_vec()))
}

fn read_sync_bytes(b: &[u8], pos: &mut usize) -> Result<Vec<u8>, String> {
    Ok(read_sync_opt_bytes(b, pos)?.unwrap_or_default())
}

fn read_sync_string(b: &[u8], pos: &mut usize) -> Result<String, String> {
    let bytes = read_sync_bytes(b, pos)?;
    String::from_utf8(bytes).map_err(|e| e.to_string())
}

fn skip_sync_val(b: &[u8], pos: &mut usize) -> Result<(), String> {
    let start = *pos;
    let bd = *b.get(start).ok_or("eof")?;
    *pos += 1;
    match bd {
        0x00..=0x7f | 0xc0..=0xc3 | 0xe0..=0xff => Ok(()),
        0xcc | 0xd0 => {
            *pos += 1;
            Ok(())
        }
        0xcd | 0xd1 => {
            *pos += 2;
            Ok(())
        }
        0xce | 0xd2 | 0xca => {
            *pos += 4;
            Ok(())
        }
        0xcf | 0xd3 | 0xcb => {
            *pos += 8;
            Ok(())
        }
        0xa0..=0xbf => {
            *pos += (bd & 0x1f) as usize;
            Ok(())
        }
        0xd9 | 0xc4 => {
            let n = *b.get(*pos).ok_or("eof")? as usize;
            *pos += 1 + n;
            Ok(())
        }
        0xda | 0xc5 => {
            let s = b.get(*pos..*pos + 2).ok_or("eof")?;
            *pos += 2 + u16::from_be_bytes([s[0], s[1]]) as usize;
            Ok(())
        }
        0xdb | 0xc6 => {
            let s = b.get(*pos..*pos + 4).ok_or("eof")?;
            *pos += 4 + u32::from_be_bytes([s[0], s[1], s[2], s[3]]) as usize;
            Ok(())
        }
        _ => Err("unsupported skip".to_string()),
    }
}

/// Reads a single complete self-delimiting MessagePack item from an async reader into a byte buffer.
pub(super) async fn read_msgpack_map_bytes<R: AsyncRead + Unpin + Send>(reader: &mut R) -> Result<Vec<u8>, String> {
    read_msgpack_map_bytes_with_limit(reader, MAX_PUSH_STATE_BYTES).await
}

async fn read_msgpack_map_bytes_with_limit<R: AsyncRead + Unpin + Send>(
    reader: &mut R,
    max_bytes: usize,
) -> Result<Vec<u8>, String> {
    let mut out = Vec::with_capacity(64);
    read_mp_item_limited(reader, &mut out, 0, max_bytes)
        .await
        .map_err(|e| e.to_string())?;
    Ok(out)
}

fn mp_invalid(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

fn ensure_capacity_within_limit(out: &[u8], extra: usize, max_bytes: usize) -> io::Result<()> {
    let new_len = out
        .len()
        .checked_add(extra)
        .ok_or_else(|| mp_invalid("msgpack item length overflow"))?;
    if new_len > max_bytes {
        return Err(mp_invalid(format!(
            "msgpack payload size {new_len} exceeds max {max_bytes}"
        )));
    }
    Ok(())
}

#[cfg(test)]
fn read_mp_item<'a, R: AsyncRead + Unpin + Send>(
    reader: &'a mut R,
    out: &'a mut Vec<u8>,
    depth: usize,
) -> BoxFuture<'a, io::Result<()>> {
    read_mp_item_limited(reader, out, depth, MAX_PUSH_STATE_BYTES)
}

fn read_mp_item_limited<'a, R: AsyncRead + Unpin + Send>(
    reader: &'a mut R,
    out: &'a mut Vec<u8>,
    depth: usize,
    max_bytes: usize,
) -> BoxFuture<'a, io::Result<()>> {
    Box::pin(async move {
        if depth >= MAX_MP_DEPTH {
            return Err(mp_invalid(format!(
                "msgpack nesting depth exceeds max {MAX_MP_DEPTH}"
            )));
        }
        ensure_capacity_within_limit(out, 1, max_bytes)?;
        let mut bd_buf = [0u8; 1];
        reader.read_exact(&mut bd_buf).await?;
        let bd = bd_buf[0];
        out.push(bd);

        match bd {
            0x00..=0x7f | 0xc0..=0xc3 | 0xe0..=0xff => Ok(()),
            0xcc | 0xd0 => read_append(reader, out, 1, max_bytes).await,
            0xcd | 0xd1 => read_append(reader, out, 2, max_bytes).await,
            0xce | 0xd2 | 0xca => read_append(reader, out, 4, max_bytes).await,
            0xcf | 0xd3 | 0xcb => read_append(reader, out, 8, max_bytes).await,
            0xa0..=0xbf => read_append(reader, out, (bd & 0x1f) as usize, max_bytes).await,
            0xd9 | 0xc4 => {
                ensure_capacity_within_limit(out, 1, max_bytes)?;
                let mut l = [0u8; 1];
                reader.read_exact(&mut l).await?;
                out.push(l[0]);
                read_append(reader, out, l[0] as usize, max_bytes).await
            }
            0xda | 0xc5 => {
                ensure_capacity_within_limit(out, 2, max_bytes)?;
                let mut l = [0u8; 2];
                reader.read_exact(&mut l).await?;
                out.extend_from_slice(&l);
                read_append(reader, out, u16::from_be_bytes(l) as usize, max_bytes).await
            }
            0xdb | 0xc6 => {
                ensure_capacity_within_limit(out, 4, max_bytes)?;
                let mut l = [0u8; 4];
                reader.read_exact(&mut l).await?;
                out.extend_from_slice(&l);
                let len = u32::from_be_bytes(l) as usize;
                read_append(reader, out, len, max_bytes).await
            }
            0x90..=0x9f => {
                let n = (bd & 0x0f) as usize;
                ensure_capacity_within_limit(out, n, max_bytes)?;
                for _ in 0..n {
                    read_mp_item_limited(reader, out, depth + 1, max_bytes).await?;
                }
                Ok(())
            }
            0xdc => {
                ensure_capacity_within_limit(out, 2, max_bytes)?;
                let mut l = [0u8; 2];
                reader.read_exact(&mut l).await?;
                out.extend_from_slice(&l);
                let n = u16::from_be_bytes(l) as usize;
                ensure_capacity_within_limit(out, n, max_bytes)?;
                for _ in 0..n {
                    read_mp_item_limited(reader, out, depth + 1, max_bytes).await?;
                }
                Ok(())
            }
            0xdd => {
                ensure_capacity_within_limit(out, 4, max_bytes)?;
                let mut l = [0u8; 4];
                reader.read_exact(&mut l).await?;
                out.extend_from_slice(&l);
                let n = u32::from_be_bytes(l) as usize;
                ensure_capacity_within_limit(out, n, max_bytes)?;
                for _ in 0..n {
                    read_mp_item_limited(reader, out, depth + 1, max_bytes).await?;
                }
                Ok(())
            }
            0x80..=0x8f => {
                let n = (bd & 0x0f) as usize;
                ensure_capacity_within_limit(out, n.saturating_mul(2), max_bytes)?;
                for _ in 0..n {
                    read_mp_item_limited(reader, out, depth + 1, max_bytes).await?;
                    read_mp_item_limited(reader, out, depth + 1, max_bytes).await?;
                }
                Ok(())
            }
            0xde => {
                ensure_capacity_within_limit(out, 2, max_bytes)?;
                let mut l = [0u8; 2];
                reader.read_exact(&mut l).await?;
                out.extend_from_slice(&l);
                let n = u16::from_be_bytes(l) as usize;
                let min_bytes = n
                    .checked_mul(2)
                    .ok_or_else(|| mp_invalid("msgpack map16 length overflow"))?;
                ensure_capacity_within_limit(out, min_bytes, max_bytes)?;
                for _ in 0..n {
                    read_mp_item_limited(reader, out, depth + 1, max_bytes).await?;
                    read_mp_item_limited(reader, out, depth + 1, max_bytes).await?;
                }
                Ok(())
            }
            0xdf => {
                ensure_capacity_within_limit(out, 4, max_bytes)?;
                let mut l = [0u8; 4];
                reader.read_exact(&mut l).await?;
                out.extend_from_slice(&l);
                let n = u32::from_be_bytes(l) as usize;
                let min_bytes = n
                    .checked_mul(2)
                    .ok_or_else(|| mp_invalid("msgpack map32 length overflow"))?;
                ensure_capacity_within_limit(out, min_bytes, max_bytes)?;
                for _ in 0..n {
                    read_mp_item_limited(reader, out, depth + 1, max_bytes).await?;
                    read_mp_item_limited(reader, out, depth + 1, max_bytes).await?;
                }
                Ok(())
            }
            0xd4 => read_append(reader, out, 2, max_bytes).await,
            0xd5 => read_append(reader, out, 3, max_bytes).await,
            0xd6 => read_append(reader, out, 5, max_bytes).await,
            0xd7 => read_append(reader, out, 9, max_bytes).await,
            0xd8 => read_append(reader, out, 17, max_bytes).await,
            0xc7 => {
                ensure_capacity_within_limit(out, 1, max_bytes)?;
                let mut l = [0u8; 1];
                reader.read_exact(&mut l).await?;
                out.push(l[0]);
                read_append(reader, out, 1 + l[0] as usize, max_bytes).await
            }
            0xc8 => {
                ensure_capacity_within_limit(out, 2, max_bytes)?;
                let mut l = [0u8; 2];
                reader.read_exact(&mut l).await?;
                out.extend_from_slice(&l);
                read_append(reader, out, 1 + u16::from_be_bytes(l) as usize, max_bytes).await
            }
            0xc9 => {
                ensure_capacity_within_limit(out, 4, max_bytes)?;
                let mut l = [0u8; 4];
                reader.read_exact(&mut l).await?;
                out.extend_from_slice(&l);
                let len = 1usize
                    .checked_add(u32::from_be_bytes(l) as usize)
                    .ok_or_else(|| mp_invalid("msgpack ext32 length overflow"))?;
                read_append(reader, out, len, max_bytes).await
            }
        }
    })
}

async fn read_append<R: AsyncRead + Unpin + Send>(
    reader: &mut R,
    out: &mut Vec<u8>,
    len: usize,
    max_bytes: usize,
) -> io::Result<()> {
    ensure_capacity_within_limit(out, len, max_bytes)?;
    let start = out.len();
    out.resize(start + len, 0);
    reader.read_exact(&mut out[start..]).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn read_mp_item_rejects_4gib_ext32_and_cumulative_overflow() {
        // 0xc9 ext32 header declaring 4 GiB payload.
        let ext32_bomb = [0xc9u8, 0xff, 0xff, 0xff, 0xff];
        let mut cursor = io::Cursor::new(&ext32_bomb[..]);
        let mut out = Vec::new();
        let err = read_mp_item(&mut cursor, &mut out, 0).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);

        // 2-element array where two bin32 items cumulatively exceed MAX_PUSH_STATE_BYTES.
        let half = (MAX_PUSH_STATE_BYTES / 2 + 64) as u32;
        let mut two_chunks = Vec::new();
        two_chunks.push(0x92); // fixarray len=2
        two_chunks.push(0xc6); // bin32
        two_chunks.extend_from_slice(&half.to_be_bytes());
        two_chunks.resize(two_chunks.len() + half as usize, 0);
        two_chunks.push(0xc6); // bin32
        two_chunks.extend_from_slice(&half.to_be_bytes());
        let mut cursor2 = io::Cursor::new(&two_chunks[..]);
        let mut out2 = Vec::new();
        let err2 = read_mp_item(&mut cursor2, &mut out2, 0).await.unwrap_err();
        assert_eq!(err2.kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn read_mp_item_rejects_deep_nesting_and_huge_array32() {
        // 64-deep nested fixarray1 (0x91) ending with 0x00.
        let mut nested = vec![0x91u8; 64];
        nested.push(0x00);
        let mut cursor = io::Cursor::new(&nested[..]);
        let mut out = Vec::new();
        let err = read_mp_item(&mut cursor, &mut out, 0).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);

        // 64-deep nested array32 (0xdd, len=1).
        let mut nested32 = Vec::new();
        for _ in 0..64 {
            nested32.push(0xdd);
            nested32.extend_from_slice(&1u32.to_be_bytes());
        }
        nested32.push(0x00);
        let mut cursor32 = io::Cursor::new(&nested32[..]);
        let mut out32 = Vec::new();
        let err32 = read_mp_item(&mut cursor32, &mut out32, 0).await.unwrap_err();
        assert_eq!(err32.kind(), io::ErrorKind::InvalidData);

        // 0xdd array32 claiming u32::MAX elements is rejected before looping.
        let arr32_bomb = [0xddu8, 0xff, 0xff, 0xff, 0xff];
        let mut cursor_arr = io::Cursor::new(&arr32_bomb[..]);
        let mut out_arr = Vec::new();
        let err_arr = read_mp_item(&mut cursor_arr, &mut out_arr, 0).await.unwrap_err();
        assert_eq!(err_arr.kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn read_push_pull_rejects_excessive_nodes_and_empty_map_flood() {
        // Header with Nodes = 100_000 (> MAX_PUSH_PULL_NODES).
        let mut buf = vec![0x83];
        buf.extend_from_slice(b"\xa5Nodes");
        buf.push(0xce);
        buf.extend_from_slice(&100_000u32.to_be_bytes());
        buf.extend_from_slice(b"\xacUserStateLen\x00\xa4Join\xc2");
        let mut cur = io::Cursor::new(buf);
        let err = read_push_pull_from_stream(&mut cur).await.unwrap_err();
        assert!(err.contains("exceeds max"), "got: {err}");
    }

    #[tokio::test]
    async fn encrypted_stream_emits_sample_memberlist_size_remote() {
        use prometheus::Registry;

        let inner = vec![MessageType::User as u8, 1, 2, 3, 4];
        let mut frame = vec![MessageType::Encrypt as u8];
        frame.extend_from_slice(&(inner.len() as u32).to_be_bytes());
        frame.extend_from_slice(&inner);

        // Unconfigured encryption rejects with Go's exact error message.
        let mut cur_unconf = io::Cursor::new(frame.clone());
        let err = read_stream_header_with_metrics(&mut cur_unconf, None, None)
            .await
            .unwrap_err();
        assert_eq!(
            err,
            "remote state is encrypted and encryption is not configured"
        );

        // Configured encryption records sample_memberlist_size_remote on GoMetrics.
        let gm = Arc::new(GoMetrics::new());
        let reg = Registry::new();
        gm.register(&reg);
        let decrypt: StreamDecryptFn = Arc::new(|cipher, aad| {
            assert_eq!(aad[0], MessageType::Encrypt as u8);
            Ok(cipher.to_vec())
        });
        let mut cur = io::Cursor::new(frame);
        let (msg_type, label, body) =
            read_stream_header_with_metrics(&mut cur, Some(&gm), Some(&decrypt))
                .await
                .unwrap();
        assert_eq!(msg_type, MessageType::User);
        assert!(label.is_empty());
        assert_eq!(body.unwrap(), vec![1, 2, 3, 4]);

        let text = prometheus::TextEncoder::new()
            .encode_to_string(&reg.gather())
            .unwrap();
        assert!(
            text.contains("# TYPE sample_memberlist_size_remote summary"),
            "missing sample_memberlist_size_remote in:
{text}"
        );
        assert!(
            text.contains("sample_memberlist_size_remote_sum 5"),
            "unexpected sample_memberlist_size_remote sum in:
{text}"
        );
    }
}

