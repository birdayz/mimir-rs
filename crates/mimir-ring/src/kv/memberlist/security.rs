//! Symmetric encryption (AES-GCM with optional PKCS7 padding) and Keyring.
//!
//! Ported from:
//! - github.com/hashicorp/memberlist/security.go
//! - github.com/hashicorp/memberlist/keyring.go

use parking_lot::Mutex;
use rand::RngCore;

pub const MIN_ENCRYPTION_VERSION: u8 = 0;
pub const MAX_ENCRYPTION_VERSION: u8 = 1;

const VERSION_SIZE: usize = 1;
const NONCE_SIZE: usize = 12;
const TAG_SIZE: usize = 16;
pub const BLOCK_SIZE: usize = 16;

// --- Pure Rust NIST FIPS-197 AES + SP 800-38D AES-GCM (zero unsafe, zero extra deps) ---

#[rustfmt::skip]
const SBOX: [u8; 256] = [
    0x63, 0x7c, 0x77, 0x7b, 0xf2, 0x6b, 0x6f, 0xc5, 0x30, 0x01, 0x67, 0x2b, 0xfe, 0xd7, 0xab, 0x76,
    0xca, 0x82, 0xc9, 0x7d, 0xfa, 0x59, 0x47, 0xf0, 0xad, 0xd4, 0xa2, 0xaf, 0x9c, 0xa4, 0x72, 0xc0,
    0xb7, 0xfd, 0x93, 0x26, 0x36, 0x3f, 0xf7, 0xcc, 0x34, 0xa5, 0xe5, 0xf1, 0x71, 0xd8, 0x31, 0x15,
    0x04, 0xc7, 0x23, 0xc3, 0x18, 0x96, 0x05, 0x9a, 0x07, 0x12, 0x80, 0xe2, 0xeb, 0x27, 0xb2, 0x75,
    0x09, 0x83, 0x2c, 0x1a, 0x1b, 0x6e, 0x5a, 0xa0, 0x52, 0x3b, 0xd6, 0xb3, 0x29, 0xe3, 0x2f, 0x84,
    0x53, 0xd1, 0x00, 0xed, 0x20, 0xfc, 0xb1, 0x5b, 0x6a, 0xcb, 0xbe, 0x39, 0x4a, 0x4c, 0x58, 0xcf,
    0xd0, 0xef, 0xaa, 0xfb, 0x43, 0x4d, 0x33, 0x85, 0x45, 0xf9, 0x02, 0x7f, 0x50, 0x3c, 0x9f, 0xa8,
    0x51, 0xa3, 0x40, 0x8f, 0x92, 0x9d, 0x38, 0xf5, 0xbc, 0xb6, 0xda, 0x21, 0x10, 0xff, 0xf3, 0xd2,
    0xcd, 0x0c, 0x13, 0xec, 0x5f, 0x97, 0x44, 0x17, 0xc4, 0xa7, 0x7e, 0x3d, 0x64, 0x5d, 0x19, 0x73,
    0x60, 0x81, 0x4f, 0xdc, 0x22, 0x2a, 0x90, 0x88, 0x46, 0xee, 0xb8, 0x14, 0xde, 0x5e, 0x0b, 0xdb,
    0xe0, 0x32, 0x3a, 0x0a, 0x49, 0x06, 0x24, 0x5c, 0xc2, 0xd3, 0xac, 0x62, 0x91, 0x95, 0xe4, 0x79,
    0xe7, 0xc8, 0x37, 0x6d, 0x8d, 0xd5, 0x4e, 0xa9, 0x6c, 0x56, 0xf4, 0xea, 0x65, 0x7a, 0xae, 0x08,
    0xba, 0x78, 0x25, 0x2e, 0x1c, 0xa6, 0xb4, 0xc6, 0xe8, 0xdd, 0x74, 0x1f, 0x4b, 0xbd, 0x8b, 0x8a,
    0x70, 0x3e, 0xb5, 0x66, 0x48, 0x03, 0xf6, 0x0e, 0x61, 0x35, 0x57, 0xb9, 0x86, 0xc1, 0x1d, 0x9e,
    0xe1, 0xf8, 0x98, 0x11, 0x69, 0xd9, 0x8e, 0x94, 0x9b, 0x1e, 0x87, 0xe9, 0xce, 0x55, 0x28, 0xdf,
    0x8c, 0xa1, 0x89, 0x0d, 0xbf, 0xe6, 0x42, 0x68, 0x41, 0x99, 0x2d, 0x0f, 0xb0, 0x54, 0xbb, 0x16,
];

const RCON: [u32; 11] = [
    0x0000_0000,
    0x0100_0000,
    0x0200_0000,
    0x0400_0000,
    0x0800_0000,
    0x1000_0000,
    0x2000_0000,
    0x4000_0000,
    0x8000_0000,
    0x1b00_0000,
    0x3600_0000,
];

struct AesKey {
    rounds: usize,
    rk: [u32; 60],
}

impl AesKey {
    fn new(key: &[u8]) -> Result<Self, String> {
        let (nk, rounds) = match key.len() {
            16 => (4, 10),
            24 => (6, 12),
            32 => (8, 14),
            other => return Err(format!("crypto/aes: invalid key size {other}")),
        };
        let mut rk = [0u32; 60];
        for i in 0..nk {
            rk[i] = u32::from_be_bytes([
                key[4 * i],
                key[4 * i + 1],
                key[4 * i + 2],
                key[4 * i + 3],
            ]);
        }
        let total_words = 4 * (rounds + 1);
        let sub_word = |w: u32| -> u32 {
            let b = w.to_be_bytes();
            u32::from_be_bytes([
                SBOX[b[0] as usize],
                SBOX[b[1] as usize],
                SBOX[b[2] as usize],
                SBOX[b[3] as usize],
            ])
        };
        for i in nk..total_words {
            let mut temp = rk[i - 1];
            if i % nk == 0 {
                temp = sub_word(temp.rotate_left(8)) ^ RCON[i / nk];
            } else if nk > 6 && i % nk == 4 {
                temp = sub_word(temp);
            }
            rk[i] = rk[i - nk] ^ temp;
        }
        Ok(Self { rounds, rk })
    }

    fn encrypt_block(&self, input: &[u8; 16]) -> [u8; 16] {
        // State stored in column-major order matching FIPS 197.
        let mut s = *input;
        add_round_key(&mut s, &self.rk[0..4]);

        for round in 1..self.rounds {
            sub_bytes(&mut s);
            shift_rows(&mut s);
            mix_columns(&mut s);
            add_round_key(&mut s, &self.rk[round * 4..(round + 1) * 4]);
        }

        sub_bytes(&mut s);
        shift_rows(&mut s);
        add_round_key(&mut s, &self.rk[self.rounds * 4..(self.rounds + 1) * 4]);
        s
    }
}

#[inline]
fn add_round_key(s: &mut [u8; 16], rk: &[u32]) {
    for (col, &w) in rk.iter().enumerate() {
        let kb = w.to_be_bytes();
        let base = col * 4;
        s[base] ^= kb[0];
        s[base + 1] ^= kb[1];
        s[base + 2] ^= kb[2];
        s[base + 3] ^= kb[3];
    }
}

#[inline]
fn sub_bytes(s: &mut [u8; 16]) {
    for b in s.iter_mut() {
        *b = SBOX[*b as usize];
    }
}

#[inline]
fn shift_rows(s: &mut [u8; 16]) {
    // Row 1: left shift by 1
    let t = s[1];
    s[1] = s[5];
    s[5] = s[9];
    s[9] = s[13];
    s[13] = t;
    // Row 2: left shift by 2
    s.swap(2, 10);
    s.swap(6, 14);
    // Row 3: left shift by 3 (right by 1)
    let t = s[15];
    s[15] = s[11];
    s[11] = s[7];
    s[7] = s[3];
    s[3] = t;
}

#[inline]
fn xtime(x: u8) -> u8 {
    (x << 1) ^ (((x >> 7) & 1).wrapping_neg() & 0x1b)
}

#[inline]
fn mix_columns(s: &mut [u8; 16]) {
    for col in 0..4 {
        let i = col * 4;
        let (a0, a1, a2, a3) = (s[i], s[i + 1], s[i + 2], s[i + 3]);
        let t = a0 ^ a1 ^ a2 ^ a3;
        s[i] ^= t ^ xtime(a0 ^ a1);
        s[i + 1] ^= t ^ xtime(a1 ^ a2);
        s[i + 2] ^= t ^ xtime(a2 ^ a3);
        s[i + 3] ^= t ^ xtime(a3 ^ a0);
    }
}

fn gf128_mul(x: u128, y: u128) -> u128 {
    const R: u128 = 0xe100_0000_0000_0000_0000_0000_0000_0000;
    let mut z = 0u128;
    let mut v = y;
    for i in 0..128 {
        if ((x >> (127 - i)) & 1) != 0 {
            z ^= v;
        }
        let lsb = v & 1;
        v >>= 1;
        if lsb != 0 {
            v ^= R;
        }
    }
    z
}

fn ghash(h: u128, aad: &[u8], ciphertext: &[u8]) -> [u8; 16] {
    let mut y = 0u128;
    let mut feed_blocks = |data: &[u8]| {
        for chunk in data.chunks(16) {
            let mut blk = [0u8; 16];
            blk[..chunk.len()].copy_from_slice(chunk);
            y = gf128_mul(y ^ u128::from_be_bytes(blk), h);
        }
    };
    feed_blocks(aad);
    feed_blocks(ciphertext);
    let mut len_blk = [0u8; 16];
    len_blk[..8].copy_from_slice(&((aad.len() as u64) * 8).to_be_bytes());
    len_blk[8..].copy_from_slice(&((ciphertext.len() as u64) * 8).to_be_bytes());
    y = gf128_mul(y ^ u128::from_be_bytes(len_blk), h);
    y.to_be_bytes()
}

fn gctr(aes: &AesKey, icb: &[u8; 16], input: &[u8], out: &mut [u8]) {
    let mut cb = *icb;
    let mut off = 0usize;
    while off < input.len() {
        let ks = aes.encrypt_block(&cb);
        let n = (input.len() - off).min(16);
        for i in 0..n {
            out[off + i] = input[off + i] ^ ks[i];
        }
        off += n;
        let ctr = u32::from_be_bytes([cb[12], cb[13], cb[14], cb[15]]).wrapping_add(1);
        cb[12..16].copy_from_slice(&ctr.to_be_bytes());
    }
}

fn aes_gcm_seal(key: &[u8], nonce: &[u8; 12], plaintext: &[u8], aad: &[u8]) -> Result<Vec<u8>, String> {
    let aes = AesKey::new(key)?;
    let h = u128::from_be_bytes(aes.encrypt_block(&[0u8; 16]));
    let mut j0 = [0u8; 16];
    j0[..12].copy_from_slice(nonce);
    j0[15] = 1;

    let mut icb = j0;
    icb[15] = 2;

    let mut out = vec![0u8; plaintext.len() + TAG_SIZE];
    gctr(&aes, &icb, plaintext, &mut out[..plaintext.len()]);
    let s = ghash(h, aad, &out[..plaintext.len()]);
    let ej0 = aes.encrypt_block(&j0);
    for i in 0..16 {
        out[plaintext.len() + i] = s[i] ^ ej0[i];
    }
    Ok(out)
}

fn aes_gcm_open(key: &[u8], nonce: &[u8; 12], ciphertext_and_tag: &[u8], aad: &[u8]) -> Result<Vec<u8>, String> {
    if ciphertext_and_tag.len() < TAG_SIZE {
        return Err("cipher: message authentication failed".to_string());
    }
    let aes = AesKey::new(key)?;
    let h = u128::from_be_bytes(aes.encrypt_block(&[0u8; 16]));
    let mut j0 = [0u8; 16];
    j0[..12].copy_from_slice(nonce);
    j0[15] = 1;

    let ct_len = ciphertext_and_tag.len() - TAG_SIZE;
    let ct = &ciphertext_and_tag[..ct_len];
    let tag = &ciphertext_and_tag[ct_len..];

    let s = ghash(h, aad, ct);
    let ej0 = aes.encrypt_block(&j0);
    let mut diff = 0u8;
    for i in 0..16 {
        diff |= (s[i] ^ ej0[i]) ^ tag[i];
    }
    if diff != 0 {
        return Err("cipher: message authentication failed".to_string());
    }

    let mut icb = j0;
    icb[15] = 2;
    let mut plain = vec![0u8; ct_len];
    gctr(&aes, &icb, ct, &mut plain);
    Ok(plain)
}

// --- Memberlist security.go ---

pub fn pkcs7_encode(buf: &mut Vec<u8>, ignore: usize, block_size: usize) {
    let n = buf.len() - ignore;
    let more = block_size - (n % block_size);
    for _ in 0..more {
        buf.push(more as u8);
    }
}

pub fn pkcs7_decode(buf: &[u8], _block_size: usize) -> Result<&[u8], String> {
    if buf.is_empty() {
        return Err("Cannot decode a PKCS7 buffer of zero length".to_string());
    }
    let n = buf.len();
    let last = buf[n - 1] as usize;
    if last == 0 || last > n {
        return Err("Invalid PKCS7 padding".to_string());
    }
    Ok(&buf[..n - last])
}

pub fn try_encrypt_overhead(vsn: u8) -> Result<usize, String> {
    match vsn {
        0 => Ok(45),
        1 => Ok(29),
        _ => Err(format!("unsupported encryption version {vsn}")),
    }
}

pub fn encrypt_overhead(vsn: u8) -> usize {
    try_encrypt_overhead(vsn).unwrap_or_else(|err| {
        tracing::warn!("{err}; falling back to conservative encryption overhead");
        45
    })
}

pub fn encrypted_length(vsn: u8, inp: usize) -> usize {
    if vsn >= 1 {
        return VERSION_SIZE + NONCE_SIZE + inp + TAG_SIZE;
    }
    let padding = BLOCK_SIZE - (inp % BLOCK_SIZE);
    VERSION_SIZE + NONCE_SIZE + inp + padding + TAG_SIZE
}

pub fn encrypt_payload(
    vsn: u8,
    key: &[u8],
    msg: &[u8],
    data: &[u8],
    dst: &mut Vec<u8>,
) -> Result<(), String> {
    if vsn > MAX_ENCRYPTION_VERSION {
        return Err(format!("unsupported encryption version {vsn}"));
    }
    dst.reserve(encrypted_length(vsn, msg.len()));
    dst.push(vsn);
    let mut nonce = [0u8; NONCE_SIZE];
    rand::thread_rng().fill_bytes(&mut nonce);
    dst.extend_from_slice(&nonce);

    let sealed = if vsn == 0 {
        let mut padded = msg.to_vec();
        pkcs7_encode(&mut padded, 0, BLOCK_SIZE);
        aes_gcm_seal(key, &nonce, &padded, data)?
    } else {
        aes_gcm_seal(key, &nonce, msg, data)?
    };
    dst.extend_from_slice(&sealed);
    Ok(())
}

fn decrypt_message(key: &[u8], msg: &[u8], data: &[u8]) -> Result<Vec<u8>, String> {
    let mut nonce = [0u8; NONCE_SIZE];
    nonce.copy_from_slice(&msg[VERSION_SIZE..VERSION_SIZE + NONCE_SIZE]);
    let ct = &msg[VERSION_SIZE + NONCE_SIZE..];
    aes_gcm_open(key, &nonce, ct, data)
}

pub fn decrypt_payload(keys: &[Vec<u8>], msg: &[u8], data: &[u8]) -> Result<Vec<u8>, String> {
    if msg.is_empty() {
        return Err("cannot decrypt empty payload".to_string());
    }
    let vsn = msg[0];
    if vsn > MAX_ENCRYPTION_VERSION {
        return Err(format!("unsupported encryption version {vsn}"));
    }
    if msg.len() < encrypted_length(vsn, 0) {
        return Err(format!("payload is too small to decrypt: {}", msg.len()));
    }
    for key in keys {
        if let Ok(plain) = decrypt_message(key, msg, data) {
            if vsn == 0 {
                let unpadded = pkcs7_decode(&plain, BLOCK_SIZE)?;
                return Ok(unpadded.to_vec());
            } else {
                return Ok(plain);
            }
        }
    }
    Err("no installed keys could decrypt the message".to_string())
}

// --- Memberlist keyring.go ---

pub fn validate_key(key: &[u8]) -> Result<(), String> {
    match key.len() {
        16 | 24 | 32 => Ok(()),
        _ => Err("key size must be 16, 24 or 32 bytes".to_string()),
    }
}

pub struct Keyring {
    keys: Mutex<Vec<Vec<u8>>>,
}

impl Keyring {
    pub fn new(keys: &[Vec<u8>], primary_key: Option<&[u8]>) -> Result<Self, String> {
        let kr = Self {
            keys: Mutex::new(Vec::new()),
        };
        let pk = primary_key.unwrap_or(&[]);
        if !keys.is_empty() || !pk.is_empty() {
            if pk.is_empty() {
                return Err("empty primary key not allowed".to_string());
            }
            kr.add_key(pk)?;
            for k in keys {
                kr.add_key(k)?;
            }
        }
        Ok(kr)
    }

    pub fn add_key(&self, key: &[u8]) -> Result<(), String> {
        validate_key(key)?;
        let mut guard = self.keys.lock();
        if guard.iter().any(|k| k.as_slice() == key) {
            return Ok(());
        }
        let primary = guard.first().cloned().unwrap_or_else(|| key.to_vec());
        let mut next = Vec::with_capacity(guard.len() + 1);
        next.push(primary.clone());
        for k in guard.iter() {
            if k.as_slice() != primary.as_slice() {
                next.push(k.clone());
            }
        }
        if key != primary.as_slice() {
            next.push(key.to_vec());
        }
        *guard = next;
        Ok(())
    }

    pub fn use_key(&self, key: &[u8]) -> Result<(), String> {
        let mut guard = self.keys.lock();
        if !guard.iter().any(|k| k.as_slice() == key) {
            return Err("requested key is not in the keyring".to_string());
        }
        let mut next = Vec::with_capacity(guard.len());
        next.push(key.to_vec());
        for k in guard.iter() {
            if k.as_slice() != key {
                next.push(k.clone());
            }
        }
        *guard = next;
        Ok(())
    }

    pub fn remove_key(&self, key: &[u8]) -> Result<(), String> {
        let mut guard = self.keys.lock();
        if guard.first().is_some_and(|p| p.as_slice() == key) {
            return Err("removing the primary key is not allowed".to_string());
        }
        guard.retain(|k| k.as_slice() != key);
        Ok(())
    }

    pub fn get_keys(&self) -> Vec<Vec<u8>> {
        self.keys.lock().clone()
    }

    pub fn get_primary_key(&self) -> Option<Vec<u8>> {
        self.keys.lock().first().cloned()
    }
}

// --- CIDR filtering (config.go: ParseCIDRs, IPAllowed) ---

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IpNet {
    pub ip: std::net::IpAddr,
    pub prefix_len: u8,
}

impl IpNet {
    pub fn contains(&self, ip: std::net::IpAddr) -> bool {
        let ip = match ip {
            std::net::IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
                Some(v4) => std::net::IpAddr::V4(v4),
                None => std::net::IpAddr::V6(v6),
            },
            v4 => v4,
        };
        match (self.ip, ip) {
            (std::net::IpAddr::V4(net_ip), std::net::IpAddr::V4(target)) => {
                if self.prefix_len == 0 {
                    return true;
                }
                let mask = u32::MAX.checked_shl(32 - self.prefix_len as u32).unwrap_or(0);
                (u32::from(net_ip) & mask) == (u32::from(target) & mask)
            }
            (std::net::IpAddr::V6(net_ip), std::net::IpAddr::V6(target)) => {
                if self.prefix_len == 0 {
                    return true;
                }
                let mask = u128::MAX.checked_shl(128 - self.prefix_len as u32).unwrap_or(0);
                (u128::from(net_ip) & mask) == (u128::from(target) & mask)
            }
            _ => false,
        }
    }
}

pub fn parse_cidrs(cidrs: &[&str]) -> Result<Vec<IpNet>, String> {
    let mut nets = Vec::new();
    let mut errs = Vec::new();
    for raw in cidrs {
        let p = raw.trim();
        let Some((ip_str, prefix_str)) = p.split_once('/') else {
            errs.push(format!("invalid cidr: {p}"));
            continue;
        };
        let Ok(ip) = ip_str.parse::<std::net::IpAddr>() else {
            errs.push(format!("invalid cidr: {p}"));
            continue;
        };
        let Ok(prefix_len) = prefix_str.parse::<u8>() else {
            errs.push(format!("invalid cidr: {p}"));
            continue;
        };
        let max_bits = match ip {
            std::net::IpAddr::V4(_) => 32,
            std::net::IpAddr::V6(_) => 128,
        };
        if prefix_len > max_bits {
            errs.push(format!("invalid cidr: {p}"));
            continue;
        }
        nets.push(IpNet { ip, prefix_len });
    }
    if !errs.is_empty() {
        return Err(errs.join("; "));
    }
    Ok(nets)
}

pub fn ip_allowed(cidrs: Option<&[IpNet]>, ip: std::net::IpAddr) -> Result<(), String> {
    let Some(nets) = cidrs else {
        return Ok(());
    };
    if nets.is_empty() {
        return Ok(());
    }
    for n in nets {
        if n.contains(ip) {
            return Ok(());
        }
    }
    Err(format!("{ip} is not allowed"))
}

// --- Logging & error helpers (logging.go, state.go: failedRemote) ---

pub fn log_address(addr: Option<&str>) -> String {
    match addr {
        None => "from=<unknown address>".to_string(),
        Some(a) => format!("from={a}"),
    }
}

pub fn log_conn(remote_addr: Option<std::net::SocketAddr>) -> String {
    match remote_addr {
        None => "from=<unknown address>".to_string(),
        Some(a) => format!("from={a}"),
    }
}

#[derive(Debug, Clone)]
pub struct NetOpError {
    pub net: String,
    pub op: String,
}

impl std::fmt::Display for NetOpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.op, self.net)
    }
}

impl std::error::Error for NetOpError {}

pub fn failed_remote(err: Option<&(dyn std::error::Error + 'static)>) -> bool {
    let Some(e) = err else {
        return false;
    };
    if let Some(op_err) = e.downcast_ref::<NetOpError>() {
        if op_err.net.starts_with("tcp") {
            return matches!(op_err.op.as_str(), "dial" | "read" | "write");
        }
        if op_err.net.starts_with("udp") {
            return op_err.op == "write";
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nist_aes_gcm_kat_vectors() {
        // NIST SP 800-38D Test Case 2 (AES-128, empty AAD, 16-byte zero plaintext, zero key & nonce)
        let key = [0u8; 16];
        let nonce = [0u8; 12];
        let pt = [0u8; 16];
        let sealed = aes_gcm_seal(&key, &nonce, &pt, &[]).unwrap();
        let expected_ct = [
            0x03, 0x88, 0xda, 0xce, 0x60, 0xb6, 0xa3, 0x92,
            0xf3, 0x28, 0xc2, 0xb9, 0x71, 0xb2, 0xfe, 0x78,
        ];
        let expected_tag = [
            0xab, 0x6e, 0x47, 0xd4, 0x2c, 0xec, 0x13, 0xbd,
            0xf5, 0x3a, 0x67, 0xb2, 0x12, 0x57, 0xbd, 0xdf,
        ];
        assert_eq!(&sealed[..16], &expected_ct);
        assert_eq!(&sealed[16..], &expected_tag);
        assert_eq!(aes_gcm_open(&key, &nonce, &sealed, &[]).unwrap(), pt);

        // NIST Test Case 4 (AES-128 with AAD)
        let key4 = [
            0xfe, 0xff, 0xe9, 0x92, 0x86, 0x65, 0x73, 0x1c,
            0x6d, 0x6a, 0x8f, 0x94, 0x67, 0x30, 0x83, 0x08,
        ];
        let nonce4 = [0xca, 0xfe, 0xba, 0xbe, 0xfa, 0xce, 0xdb, 0xad, 0xde, 0xca, 0xf8, 0x88];
        let pt4 = [
            0xd9, 0x31, 0x32, 0x25, 0xf8, 0x84, 0x06, 0xe5,
            0xa5, 0x59, 0x09, 0xc5, 0xaf, 0xf5, 0x26, 0x9a,
            0x86, 0xa7, 0xa9, 0x53, 0x15, 0x34, 0xf7, 0xda,
            0x2e, 0x4c, 0x30, 0x3d, 0x8a, 0x31, 0x8a, 0x72,
            0x1c, 0x3c, 0x0c, 0x95, 0x95, 0x68, 0x09, 0x53,
            0x2f, 0xcf, 0x0e, 0x24, 0x49, 0xa6, 0xb5, 0x25,
            0xb1, 0x6a, 0xed, 0xf5, 0xaa, 0x0d, 0xe6, 0x57,
            0xba, 0x63, 0x7b, 0x39,
        ];
        let aad4 = [
            0xfe, 0xed, 0xfa, 0xce, 0xde, 0xad, 0xbe, 0xef,
            0xfe, 0xed, 0xfa, 0xce, 0xde, 0xad, 0xbe, 0xef,
            0xab, 0xad, 0xda, 0xd2,
        ];
        let sealed4 = aes_gcm_seal(&key4, &nonce4, &pt4, &aad4).unwrap();
        let expected_ct4 = [
            0x42, 0x83, 0x1e, 0xc2, 0x21, 0x77, 0x74, 0x24,
            0x4b, 0x72, 0x21, 0xb7, 0x84, 0xd0, 0xd4, 0x9c,
            0xe3, 0xaa, 0x21, 0x2f, 0x2c, 0x02, 0xa4, 0xe0,
            0x35, 0xc1, 0x7e, 0x23, 0x29, 0xac, 0xa1, 0x2e,
            0x21, 0xd5, 0x14, 0xb2, 0x54, 0x66, 0x93, 0x1c,
            0x7d, 0x8f, 0x6a, 0x5a, 0xac, 0x84, 0xaa, 0x05,
            0x1b, 0xa3, 0x0b, 0x39, 0x6a, 0x0a, 0xac, 0x97,
            0x3d, 0x58, 0xe0, 0x91,
        ];
        let expected_tag4 = [
            0x5b, 0xc9, 0x4f, 0xbc, 0x32, 0x21, 0xa5, 0xdb,
            0x94, 0xfa, 0xe9, 0x5a, 0xe7, 0x12, 0x1a, 0x47,
        ];
        assert_eq!(&sealed4[..pt4.len()], &expected_ct4);
        assert_eq!(&sealed4[pt4.len()..], &expected_tag4);
        assert_eq!(aes_gcm_open(&key4, &nonce4, &sealed4, &aad4).unwrap(), pt4);
    }
}
