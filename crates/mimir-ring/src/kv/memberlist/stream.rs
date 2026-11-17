//! Streaming MessagePack readers for memberlist stream connections (never block waiting for EOF).
//!
//! Ported from github.com/hashicorp/memberlist/net.go (`readStream`, `readRemoteState`, `readUserMsg`).

use tokio::io::{AsyncRead, AsyncReadExt};

use crate::kv::BoxFuture;

use super::wire::{
    decompress_buffer, MessageType, NodeStateType, PushNodeState, PushPullHeader,
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
    read_mp_item(reader, &mut out).await?;
    Ok(out)
}

fn read_mp_item<'a, R: AsyncRead + Unpin + Send>(
    reader: &'a mut R,
    out: &'a mut Vec<u8>,
) -> BoxFuture<'a, Result<(), String>> {
    Box::pin(async move {
        let mut bd_buf = [0u8; 1];
        reader
            .read_exact(&mut bd_buf)
            .await
            .map_err(|e| format!("msgpack read: {e}"))?;
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
                let mut l = [0u8; 1];
                reader.read_exact(&mut l).await.map_err(|e| e.to_string())?;
                out.push(l[0]);
                read_append(reader, out, l[0] as usize).await
            }
            0xda | 0xc5 => {
                let mut l = [0u8; 2];
                reader.read_exact(&mut l).await.map_err(|e| e.to_string())?;
                out.extend_from_slice(&l);
                read_append(reader, out, u16::from_be_bytes(l) as usize).await
            }
            0xdb | 0xc6 => {
                let mut l = [0u8; 4];
                reader.read_exact(&mut l).await.map_err(|e| e.to_string())?;
                out.extend_from_slice(&l);
                let len = u32::from_be_bytes(l) as usize;
                if len > MAX_PUSH_STATE_BYTES {
                    return Err("msgpack bytes exceed max".to_string());
                }
                read_append(reader, out, len).await
            }
            0x90..=0x9f => {
                let n = (bd & 0x0f) as usize;
                for _ in 0..n {
                    read_mp_item(reader, out).await?;
                }
                Ok(())
            }
            0xdc => {
                let mut l = [0u8; 2];
                reader.read_exact(&mut l).await.map_err(|e| e.to_string())?;
                out.extend_from_slice(&l);
                let n = u16::from_be_bytes(l) as usize;
                for _ in 0..n {
                    read_mp_item(reader, out).await?;
                }
                Ok(())
            }
            0xdd => {
                let mut l = [0u8; 4];
                reader.read_exact(&mut l).await.map_err(|e| e.to_string())?;
                out.extend_from_slice(&l);
                let n = u32::from_be_bytes(l) as usize;
                for _ in 0..n {
                    read_mp_item(reader, out).await?;
                }
                Ok(())
            }
            0x80..=0x8f => {
                let n = (bd & 0x0f) as usize;
                for _ in 0..n {
                    read_mp_item(reader, out).await?;
                    read_mp_item(reader, out).await?;
                }
                Ok(())
            }
            0xde => {
                let mut l = [0u8; 2];
                reader.read_exact(&mut l).await.map_err(|e| e.to_string())?;
                out.extend_from_slice(&l);
                let n = u16::from_be_bytes(l) as usize;
                for _ in 0..n {
                    read_mp_item(reader, out).await?;
                    read_mp_item(reader, out).await?;
                }
                Ok(())
            }
            0xdf => {
                let mut l = [0u8; 4];
                reader.read_exact(&mut l).await.map_err(|e| e.to_string())?;
                out.extend_from_slice(&l);
                let n = u32::from_be_bytes(l) as usize;
                for _ in 0..n {
                    read_mp_item(reader, out).await?;
                    read_mp_item(reader, out).await?;
                }
                Ok(())
            }
            0xd4 => read_append(reader, out, 2).await,
            0xd5 => read_append(reader, out, 3).await,
            0xd6 => read_append(reader, out, 5).await,
            0xd7 => read_append(reader, out, 9).await,
            0xd8 => read_append(reader, out, 17).await,
            0xc7 => {
                let mut l = [0u8; 1];
                reader.read_exact(&mut l).await.map_err(|e| e.to_string())?;
                out.push(l[0]);
                read_append(reader, out, 1 + l[0] as usize).await
            }
            0xc8 => {
                let mut l = [0u8; 2];
                reader.read_exact(&mut l).await.map_err(|e| e.to_string())?;
                out.extend_from_slice(&l);
                read_append(reader, out, 1 + u16::from_be_bytes(l) as usize).await
            }
            0xc9 => {
                let mut l = [0u8; 4];
                reader.read_exact(&mut l).await.map_err(|e| e.to_string())?;
                out.extend_from_slice(&l);
                read_append(reader, out, 1 + u32::from_be_bytes(l) as usize).await
            }
        }
    })
}

async fn read_append<R: AsyncRead + Unpin + Send>(
    reader: &mut R,
    out: &mut Vec<u8>,
    len: usize,
) -> Result<(), String> {
    let start = out.len();
    out.resize(start + len, 0);
    reader
        .read_exact(&mut out[start..])
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

