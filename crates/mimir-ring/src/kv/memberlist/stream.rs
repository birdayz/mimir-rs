//! Streaming MessagePack readers for memberlist stream connections (never block waiting for EOF).
//!
//! Ported from github.com/hashicorp/memberlist/net.go (`readStream`, `readRemoteState`, `readUserMsg`).

use std::io;

use tokio::io::{AsyncRead, AsyncReadExt};

use crate::kv::BoxFuture;

use super::wire::{
    decompress_buffer, MessageType, NodeStateType, PushNodeState, PushPullHeader, MAX_MP_DEPTH,
    MAX_PUSH_STATE_BYTES,
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

pub(super) async fn read_stream_header<R: AsyncRead + Unpin + Send>(
    reader: &mut R,
) -> Result<(MessageType, String, Option<Vec<u8>>), String> {
    let mut b = [0u8; 1];
    reader
        .read_exact(&mut b)
        .await
        .map_err(|e| format!("read stream byte: {e}"))?;

    let mut label = String::new();
    if b[0] == MessageType::HasLabel as u8 {
        let mut sz = [0u8; 1];
        reader
            .read_exact(&mut sz)
            .await
            .map_err(|e| format!("read label size: {e}"))?;
        if sz[0] == 0 {
            return Err("empty label header".to_string());
        }
        let mut lbuf = vec![0u8; sz[0] as usize];
        reader
            .read_exact(&mut lbuf)
            .await
            .map_err(|e| format!("read label: {e}"))?;
        label = String::from_utf8(lbuf).map_err(|e| format!("label utf8: {e}"))?;
        reader
            .read_exact(&mut b)
            .await
            .map_err(|e| format!("read msg type after label: {e}"))?;
    }

    let msg_type = MessageType::try_from(b[0])
        .map_err(|v| format!("unsupported stream msgType {v}"))?;

    if msg_type == MessageType::Compress {
        let raw_cp = read_msgpack_map_bytes(reader).await?;
        let cp = super::wire::decode_compressed_payload(&raw_cp)?;
        let decomp = decompress_buffer(cp.algo, &cp.buf)?;
        if decomp.is_empty() {
            return Err("empty decompressed stream payload".to_string());
        }
        let inner_type = MessageType::try_from(decomp[0])
            .map_err(|v| format!("unsupported inner stream msgType {v}"))?;
        return Ok((inner_type, label, Some(decomp[1..].to_vec())));
    }

    Ok((msg_type, label, None))
}

pub(super) async fn read_push_pull_from_stream<R: AsyncRead + Unpin + Send>(
    reader: &mut R,
) -> Result<(bool, Vec<PushNodeState>, Vec<u8>), String> {
    let hdr_bytes = read_msgpack_map_bytes(reader).await?;
    let hdr = parse_push_pull_header_only(&hdr_bytes)?;
    let mut nodes = Vec::with_capacity(hdr.nodes.min(4096));
    for _ in 0..hdr.nodes {
        let node_bytes = read_msgpack_map_bytes(reader).await?;
        let ns = parse_push_node_state_only(&node_bytes)?;
        nodes.push(ns);
    }
    if hdr.user_state_len > MAX_PUSH_STATE_BYTES {
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
            "Nodes" => hdr.nodes = read_sync_u64(bytes, &mut pos)? as usize,
            "UserStateLen" => hdr.user_state_len = read_sync_u64(bytes, &mut pos)? as usize,
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
    let mut out = Vec::with_capacity(64);
    read_mp_item(reader, &mut out, 0)
        .await
        .map_err(|e| e.to_string())?;
    Ok(out)
}

fn mp_invalid(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

fn ensure_capacity_within_limit(out: &[u8], extra: usize) -> io::Result<()> {
    let new_len = out
        .len()
        .checked_add(extra)
        .ok_or_else(|| mp_invalid("msgpack item length overflow"))?;
    if new_len > MAX_PUSH_STATE_BYTES {
        return Err(mp_invalid(format!(
            "msgpack payload size {new_len} exceeds max {MAX_PUSH_STATE_BYTES}"
        )));
    }
    Ok(())
}

fn read_mp_item<'a, R: AsyncRead + Unpin + Send>(
    reader: &'a mut R,
    out: &'a mut Vec<u8>,
    depth: usize,
) -> BoxFuture<'a, io::Result<()>> {
    Box::pin(async move {
        if depth >= MAX_MP_DEPTH {
            return Err(mp_invalid(format!(
                "msgpack nesting depth exceeds max {MAX_MP_DEPTH}"
            )));
        }
        ensure_capacity_within_limit(out, 1)?;
        let mut bd_buf = [0u8; 1];
        reader.read_exact(&mut bd_buf).await?;
        let bd = bd_buf[0];
        out.push(bd);

        match bd {
            0x00..=0x7f | 0xc0..=0xc3 | 0xe0..=0xff => Ok(()),
            0xcc | 0xd0 => read_append(reader, out, 1).await,
            0xcd | 0xd1 => read_append(reader, out, 2).await,
            0xce | 0xd2 | 0xca => read_append(reader, out, 4).await,
            0xcf | 0xd3 | 0xcb => read_append(reader, out, 8).await,
            0xa0..=0xbf => read_append(reader, out, (bd & 0x1f) as usize).await,
            0xd9 | 0xc4 => {
                ensure_capacity_within_limit(out, 1)?;
                let mut l = [0u8; 1];
                reader.read_exact(&mut l).await?;
                out.push(l[0]);
                read_append(reader, out, l[0] as usize).await
            }
            0xda | 0xc5 => {
                ensure_capacity_within_limit(out, 2)?;
                let mut l = [0u8; 2];
                reader.read_exact(&mut l).await?;
                out.extend_from_slice(&l);
                read_append(reader, out, u16::from_be_bytes(l) as usize).await
            }
            0xdb | 0xc6 => {
                ensure_capacity_within_limit(out, 4)?;
                let mut l = [0u8; 4];
                reader.read_exact(&mut l).await?;
                out.extend_from_slice(&l);
                let len = u32::from_be_bytes(l) as usize;
                read_append(reader, out, len).await
            }
            0x90..=0x9f => {
                let n = (bd & 0x0f) as usize;
                ensure_capacity_within_limit(out, n)?;
                for _ in 0..n {
                    read_mp_item(reader, out, depth + 1).await?;
                }
                Ok(())
            }
            0xdc => {
                ensure_capacity_within_limit(out, 2)?;
                let mut l = [0u8; 2];
                reader.read_exact(&mut l).await?;
                out.extend_from_slice(&l);
                let n = u16::from_be_bytes(l) as usize;
                ensure_capacity_within_limit(out, n)?;
                for _ in 0..n {
                    read_mp_item(reader, out, depth + 1).await?;
                }
                Ok(())
            }
            0xdd => {
                ensure_capacity_within_limit(out, 4)?;
                let mut l = [0u8; 4];
                reader.read_exact(&mut l).await?;
                out.extend_from_slice(&l);
                let n = u32::from_be_bytes(l) as usize;
                ensure_capacity_within_limit(out, n)?;
                for _ in 0..n {
                    read_mp_item(reader, out, depth + 1).await?;
                }
                Ok(())
            }
            0x80..=0x8f => {
                let n = (bd & 0x0f) as usize;
                ensure_capacity_within_limit(out, n.saturating_mul(2))?;
                for _ in 0..n {
                    read_mp_item(reader, out, depth + 1).await?;
                    read_mp_item(reader, out, depth + 1).await?;
                }
                Ok(())
            }
            0xde => {
                ensure_capacity_within_limit(out, 2)?;
                let mut l = [0u8; 2];
                reader.read_exact(&mut l).await?;
                out.extend_from_slice(&l);
                let n = u16::from_be_bytes(l) as usize;
                let min_bytes = n
                    .checked_mul(2)
                    .ok_or_else(|| mp_invalid("msgpack map16 length overflow"))?;
                ensure_capacity_within_limit(out, min_bytes)?;
                for _ in 0..n {
                    read_mp_item(reader, out, depth + 1).await?;
                    read_mp_item(reader, out, depth + 1).await?;
                }
                Ok(())
            }
            0xdf => {
                ensure_capacity_within_limit(out, 4)?;
                let mut l = [0u8; 4];
                reader.read_exact(&mut l).await?;
                out.extend_from_slice(&l);
                let n = u32::from_be_bytes(l) as usize;
                let min_bytes = n
                    .checked_mul(2)
                    .ok_or_else(|| mp_invalid("msgpack map32 length overflow"))?;
                ensure_capacity_within_limit(out, min_bytes)?;
                for _ in 0..n {
                    read_mp_item(reader, out, depth + 1).await?;
                    read_mp_item(reader, out, depth + 1).await?;
                }
                Ok(())
            }
            0xd4 => read_append(reader, out, 2).await,
            0xd5 => read_append(reader, out, 3).await,
            0xd6 => read_append(reader, out, 5).await,
            0xd7 => read_append(reader, out, 9).await,
            0xd8 => read_append(reader, out, 17).await,
            0xc7 => {
                ensure_capacity_within_limit(out, 1)?;
                let mut l = [0u8; 1];
                reader.read_exact(&mut l).await?;
                out.push(l[0]);
                read_append(reader, out, 1 + l[0] as usize).await
            }
            0xc8 => {
                ensure_capacity_within_limit(out, 2)?;
                let mut l = [0u8; 2];
                reader.read_exact(&mut l).await?;
                out.extend_from_slice(&l);
                read_append(reader, out, 1 + u16::from_be_bytes(l) as usize).await
            }
            0xc9 => {
                ensure_capacity_within_limit(out, 4)?;
                let mut l = [0u8; 4];
                reader.read_exact(&mut l).await?;
                out.extend_from_slice(&l);
                let len = 1usize
                    .checked_add(u32::from_be_bytes(l) as usize)
                    .ok_or_else(|| mp_invalid("msgpack ext32 length overflow"))?;
                read_append(reader, out, len).await
            }
        }
    })
}

async fn read_append<R: AsyncRead + Unpin + Send>(
    reader: &mut R,
    out: &mut Vec<u8>,
    len: usize,
) -> io::Result<()> {
    ensure_capacity_within_limit(out, len)?;
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
}

