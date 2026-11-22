//! Ported from github.com/klauspost/compress/s2/dict.go, encode_all.go, and encode_better.go.

use std::sync::OnceLock;

use super::block::{
    decode_block_uvarint, emit_copy, emit_literal, emit_repeat, encode_block_best, hash4, hash6,
    hash7, hash8, load32, load64, max_encoded_len, put_uvarint, s2_decode_dict, INPUT_MARGIN,
    MIN_NON_LITERAL_BLOCK_SIZE,
};
use super::{S2Error, MAX_DICT_SRC_OFFSET};

pub const MIN_DICT_SIZE: usize = 16;
pub const MAX_DICT_SIZE: usize = 65536;

/// Dictionary for encoding and decoding S2 blocks.
pub struct Dict {
    pub(crate) dict: Vec<u8>,
    pub(crate) repeat: usize,
    fast_table: OnceLock<Vec<u16>>,
    better_tables: OnceLock<(Vec<u16>, Vec<u16>)>,
    best_tables: OnceLock<(Vec<u32>, Vec<u32>)>,
}

impl Dict {
    /// Reads a serialized dictionary (uvarint repeat + raw dictionary bytes).
    /// Returns `None` if invalid or if `repeat` exceeds dictionary length.
    pub fn new(raw: &[u8]) -> Option<Self> {
        if raw.is_empty() {
            return None;
        }
        let mut r: u64 = 0;
        let mut shift = 0u32;
        let mut n = 0usize;
        for (i, &b) in raw.iter().take(10).enumerate() {
            if i == 9 && b > 1 {
                return None;
            }
            r |= u64::from(b & 0x7f) << shift;
            if b < 0x80 {
                n = i + 1;
                break;
            }
            shift += 7;
        }
        if n == 0 {
            return None;
        }
        let dict_slice = &raw[n..];
        if dict_slice.len() < MIN_DICT_SIZE || dict_slice.len() > MAX_DICT_SIZE {
            return None;
        }
        // Compare as u64 to prevent overflow/wrapping on 32-bit or > i64::MAX.
        if r > dict_slice.len() as u64 {
            return None;
        }
        Some(Self {
            dict: dict_slice.to_vec(),
            repeat: r as usize,
            fast_table: OnceLock::new(),
            better_tables: OnceLock::new(),
            best_tables: OnceLock::new(),
        })
    }

    /// Creates a dictionary from `data`, optionally searching for `search_start` to set initial repeat.
    pub fn make(mut data: &[u8], search_start: &[u8]) -> Option<Self> {
        if data.is_empty() {
            return None;
        }
        if data.len() > MAX_DICT_SIZE {
            data = &data[data.len() - MAX_DICT_SIZE..];
        }
        if data.len() < MIN_DICT_SIZE {
            return None;
        }
        let mut repeat = 0usize;
        for s in (5..=search_start.len()).rev() {
            let needle = &search_start[..s];
            if let Some(idx) = data.windows(needle.len()).rposition(|w| w == needle) {
                if idx <= data.len() - 8 {
                    repeat = idx;
                    break;
                }
            }
        }
        Some(Self {
            dict: data.to_vec(),
            repeat,
            fast_table: OnceLock::new(),
            better_tables: OnceLock::new(),
            best_tables: OnceLock::new(),
        })
    }

    /// Creates a dictionary with an explicit initial repeat index.
    pub fn make_manual(data: &[u8], first_idx: u16) -> Option<Self> {
        if data.len() < MIN_DICT_SIZE
            || data.len() > MAX_DICT_SIZE
            || (first_idx as usize) >= data.len() - 8
        {
            return None;
        }
        Some(Self {
            dict: data.to_vec(),
            repeat: first_idx as usize,
            fast_table: OnceLock::new(),
            better_tables: OnceLock::new(),
            best_tables: OnceLock::new(),
        })
    }

    pub fn repeat(&self) -> usize {
        self.repeat
    }

    pub fn dict_bytes(&self) -> &[u8] {
        &self.dict
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = vec![0u8; 5 + self.dict.len()];
        let n = put_uvarint(&mut out, self.repeat as u64);
        out[n..n + self.dict.len()].copy_from_slice(&self.dict);
        out.truncate(n + self.dict.len());
        out
    }

    pub(crate) fn init_fast(&self) -> &[u16] {
        self.fast_table.get_or_init(|| {
            const TABLE_BITS: u8 = 14;
            let mut table = vec![0u16; 1 << TABLE_BITS];
            let limit = self.dict.len().saturating_sub(10);
            let mut i = 0;
            while i < limit {
                let x0 = load64(&self.dict, i);
                table[hash6(x0, TABLE_BITS)] = i as u16;
                table[hash6(x0 >> 8, TABLE_BITS)] = (i + 1) as u16;
                table[hash6(x0 >> 16, TABLE_BITS)] = (i + 2) as u16;
                i += 3;
            }
            table
        })
    }

    pub(crate) fn init_better(&self) -> (&[u16], &[u16]) {
        let (l, s) = self.better_tables.get_or_init(|| {
            const L_BITS: u8 = 17;
            const S_BITS: u8 = 14;
            let mut l_table = vec![0u16; 1 << L_BITS];
            let mut s_table = vec![0u16; 1 << S_BITS];
            let limit = self.dict.len().saturating_sub(8);
            for i in 0..limit {
                let cv = load64(&self.dict, i);
                l_table[hash7(cv, L_BITS)] = i as u16;
                s_table[hash4(cv, S_BITS)] = i as u16;
            }
            (l_table, s_table)
        });
        (l, s)
    }

    pub(crate) fn init_best(&self) -> (&[u32], &[u32]) {
        let (l, s) = self.best_tables.get_or_init(|| {
            const L_BITS: u8 = 18;
            const S_BITS: u8 = 15;
            let mut l_table = vec![0u32; 1 << L_BITS];
            let mut s_table = vec![0u32; 1 << S_BITS];
            let limit = self.dict.len().saturating_sub(8);
            for i in 0..limit {
                let cv = load64(&self.dict, i);
                let hl = hash8(cv, L_BITS);
                let hs = hash4(cv, S_BITS);
                let cl = l_table[hl];
                let cs = s_table[hs];
                l_table[hl] = (i as u32) | (cl << 16);
                s_table[hs] = (i as u32) | (cs << 16);
            }
            (l_table, s_table)
        });
        (l, s)
    }

    pub(crate) fn best_candidates(&self, cv: u64) -> ([usize; 2], [usize; 2]) {
        let (l, s) = self.init_best();
        let cl = l[hash8(cv, 18)];
        let cs = s[hash4(cv, 15)];
        (
            [(cl & 0xffff) as usize, (cl >> 16) as usize],
            [(cs & 0xffff) as usize, (cs >> 16) as usize],
        )
    }

    pub fn encode(&self, src: &[u8]) -> Vec<u8> {
        let mel = max_encoded_len(src.len() as i64);
        if mel < 0 {
            panic!("s2: input too large");
        }
        let mut dst = vec![0u8; mel as usize];
        let mut d = put_uvarint(&mut dst, src.len() as u64);
        if src.is_empty() {
            dst.truncate(d);
            return dst;
        }
        if src.len() < MIN_NON_LITERAL_BLOCK_SIZE {
            d += emit_literal(&mut dst[d..], src);
            dst.truncate(d);
            return dst;
        }
        let n = self.encode_block_fast(&mut dst[d..], src);
        if n > 0 {
            d += n;
        } else {
            d += emit_literal(&mut dst[d..], src);
        }
        dst.truncate(d);
        dst
    }

    pub fn encode_better(&self, src: &[u8]) -> Vec<u8> {
        let mel = max_encoded_len(src.len() as i64);
        if mel < 0 {
            panic!("s2: input too large");
        }
        let mut dst = vec![0u8; mel as usize];
        let mut d = put_uvarint(&mut dst, src.len() as u64);
        if src.is_empty() {
            dst.truncate(d);
            return dst;
        }
        if src.len() < MIN_NON_LITERAL_BLOCK_SIZE {
            d += emit_literal(&mut dst[d..], src);
            dst.truncate(d);
            return dst;
        }
        let n = self.encode_block_better(&mut dst[d..], src);
        if n > 0 {
            d += n;
        } else {
            d += emit_literal(&mut dst[d..], src);
        }
        dst.truncate(d);
        dst
    }

    pub fn encode_best(&self, src: &[u8]) -> Vec<u8> {
        let mel = max_encoded_len(src.len() as i64);
        if mel < 0 {
            panic!("s2: input too large");
        }
        let mut dst = vec![0u8; mel as usize];
        let mut d = put_uvarint(&mut dst, src.len() as u64);
        if src.is_empty() {
            dst.truncate(d);
            return dst;
        }
        if src.len() < MIN_NON_LITERAL_BLOCK_SIZE {
            d += emit_literal(&mut dst[d..], src);
            dst.truncate(d);
            return dst;
        }
        let n = encode_block_best(&mut dst[d..], src, Some(self));
        if n > 0 {
            d += n;
        } else {
            d += emit_literal(&mut dst[d..], src);
        }
        dst.truncate(d);
        dst
    }

    pub fn decode(&self, src: &[u8]) -> Result<Vec<u8>, S2Error> {
        let (d_len, hdr) = decode_block_uvarint(src)?;
        let mut dst = vec![0u8; d_len];
        s2_decode_dict(&mut dst, &src[hdr..], Some(self))?;
        Ok(dst)
    }

    pub fn encode_block_fast(&self, dst: &mut [u8], src: &[u8]) -> usize {
        if src.len() < MIN_NON_LITERAL_BLOCK_SIZE {
            return 0;
        }
        const TABLE_BITS: u8 = 14;
        const MAX_AHEAD: usize = 8;
        let dict_fast = self.init_fast();
        let mut table = vec![0u32; 1 << TABLE_BITS];

        let mut s_limit = (src.len() - INPUT_MARGIN).min(MAX_DICT_SRC_OFFSET - MAX_AHEAD);
        let dst_limit = src.len() - (src.len() >> 5) - 5;
        let mut d = 0usize;
        let mut next_emit = 0usize;
        let mut s = 0usize;
        let mut repeat = self.dict.len() - self.repeat;
        let mut cv = load64(src, 0);

        'search_dict: loop {
            let next_s = s + ((s - next_emit) >> 6) + 4;
            if next_s > s_limit {
                break 'search_dict;
            }
            let h0 = hash6(cv, TABLE_BITS);
            let h1 = hash6(cv >> 8, TABLE_BITS);
            let mut cand_dict = dict_fast[h0] as usize;
            let cand_dict2 = dict_fast[h1] as usize;
            let cand2 = table[h1] as usize;
            let mut cand = table[h0] as usize;
            table[h0] = s as u32;
            table[h1] = (s + 1) as u32;
            let h2 = hash6(cv >> 16, TABLE_BITS);

            const CHECK_REP: usize = 1;
            if repeat > s {
                let dc_signed = self.dict.len() as isize - repeat as isize + s as isize;
                if dc_signed >= 0 {
                    let mut dc = dc_signed as usize;
                    if repeat - s >= 4 && dc + 4 <= self.dict.len() && (cv as u32) == load32(&self.dict, dc) {
                    let mut base = s;
                    let mut i = dc;
                    while base > next_emit && i > 0 && self.dict[i - 1] == src[base - 1] {
                        i -= 1;
                        base -= 1;
                    }
                    if d + (base - next_emit) > dst_limit {
                        return 0;
                    }
                    d += emit_literal(&mut dst[d..], &src[next_emit..base]);
                    s += 4;
                    dc += 4;
                    while dc <= self.dict.len().saturating_sub(8) && s <= src.len() - 8 {
                        let diff = load64(src, s) ^ load64(&self.dict, dc);
                        if diff != 0 {
                            s += (diff.trailing_zeros() >> 3) as usize;
                            break;
                        }
                        s += 8;
                        dc += 8;
                    }
                    d += emit_repeat(&mut dst[d..], repeat, s - base);
                    next_emit = s;
                    if s >= s_limit {
                        break 'search_dict;
                    }
                    cv = load64(src, s);
                    continue;
                }
                }
            } else if repeat > 0
                && s + CHECK_REP >= repeat
                && ((cv >> (CHECK_REP * 8)) as u32) == load32(src, s - repeat + CHECK_REP)
            {
                let mut base = s + CHECK_REP;
                let mut i = base - repeat;
                while base > next_emit && i > 0 && src[i - 1] == src[base - 1] {
                    i -= 1;
                    base -= 1;
                }
                if d + (base - next_emit) > dst_limit {
                    return 0;
                }
                d += emit_literal(&mut dst[d..], &src[next_emit..base]);
                let mut cand_pos = s - repeat + 4 + CHECK_REP;
                s += 4 + CHECK_REP;
                while s <= s_limit {
                    let diff = load64(src, s) ^ load64(src, cand_pos);
                    if diff != 0 {
                        s += (diff.trailing_zeros() >> 3) as usize;
                        break;
                    }
                    s += 8;
                    cand_pos += 8;
                }
                if next_emit > 0 {
                    d += emit_repeat(&mut dst[d..], repeat, s - base);
                } else {
                    d += emit_copy(&mut dst[d..], repeat, s - base);
                }
                next_emit = s;
                if s >= s_limit {
                    break 'search_dict;
                }
                cv = load64(src, s);
                continue;
            }

            if s == 0 {
                cv = load64(src, next_s);
                s = next_s;
                continue;
            }

            enum Action {
                Match,
                Dict,
                None,
            }
            let mut action = Action::None;
            if (cv as u32) == load32(src, cand) {
                action = Action::Match;
            } else {
                cand = table[h2] as usize;
                if ((cv >> 8) as u32) == load32(src, cand2) {
                    table[h2] = (s + 2) as u32;
                    cand = cand2;
                    s += 1;
                    action = Action::Match;
                } else if cand_dict + 8 <= self.dict.len() && cv == load64(&self.dict, cand_dict) {
                    table[h2] = (s + 2) as u32;
                    action = Action::Dict;
                } else {
                    cand_dict = dict_fast[h2] as usize;
                    if cand_dict2 >= 1
                        && cand_dict2 - 1 + 8 <= self.dict.len()
                        && (cv ^ load64(&self.dict, cand_dict2 - 1)) < (1 << 8)
                    {
                        table[h2] = (s + 2) as u32;
                        cand_dict = cand_dict2;
                        s += 1;
                        action = Action::Dict;
                    } else {
                        table[h2] = (s + 2) as u32;
                        if ((cv >> 16) as u32) == load32(src, cand) {
                            s += 2;
                            action = Action::Match;
                        } else if cand_dict >= 2
                            && cand_dict - 2 + 8 <= self.dict.len()
                            && (cv ^ load64(&self.dict, cand_dict - 2)) < (1 << 16)
                        {
                            s += 2;
                            action = Action::Dict;
                        }
                    }
                }
            }

            match action {
                Action::None => {
                    cv = load64(src, next_s);
                    s = next_s;
                }
                Action::Dict => {
                    while cand_dict > 0 && s > next_emit && self.dict[cand_dict - 1] == src[s - 1] {
                        cand_dict -= 1;
                        s -= 1;
                    }
                    if d + (s - next_emit) > dst_limit {
                        return 0;
                    }
                    d += emit_literal(&mut dst[d..], &src[next_emit..s]);
                    let base = s;
                    repeat = s + self.dict.len() - cand_dict;
                    s += 4;
                    cand_dict += 4;
                    while s <= src.len() - 8 && self.dict.len() - cand_dict >= 8 {
                        let diff = load64(src, s) ^ load64(&self.dict, cand_dict);
                        if diff != 0 {
                            s += (diff.trailing_zeros() >> 3) as usize;
                            break;
                        }
                        s += 8;
                        cand_dict += 8;
                    }
                    if s <= s_limit || s - base < 8 {
                        d += emit_copy(&mut dst[d..], repeat, s - base);
                    } else {
                        d += emit_copy(&mut dst[d..], repeat, 4);
                        d += emit_repeat(&mut dst[d..], repeat, s - base - 4);
                    }
                    next_emit = s;
                    if s >= s_limit {
                        break 'search_dict;
                    }
                    if d > dst_limit {
                        return 0;
                    }
                    let x = load64(src, s - 2);
                    table[hash6(x, TABLE_BITS)] = (s - 2) as u32;
                    table[hash6(x >> 8, TABLE_BITS)] = (s - 1) as u32;
                    cv = load64(src, s);
                }
                Action::Match => {
                    while cand > 0 && s > next_emit && src[cand - 1] == src[s - 1] {
                        cand -= 1;
                        s -= 1;
                    }
                    if d + (s - next_emit) > dst_limit {
                        return 0;
                    }
                    d += emit_literal(&mut dst[d..], &src[next_emit..s]);
                    loop {
                        let base = s;
                        repeat = base - cand;
                        s += 4;
                        cand += 4;
                        while s <= src.len() - 8 {
                            let diff = load64(src, s) ^ load64(src, cand);
                            if diff != 0 {
                                s += (diff.trailing_zeros() >> 3) as usize;
                                break;
                            }
                            s += 8;
                            cand += 8;
                        }
                        d += emit_copy(&mut dst[d..], repeat, s - base);
                        next_emit = s;
                        if s >= s_limit {
                            break 'search_dict;
                        }
                        if d > dst_limit {
                            return 0;
                        }
                        let x = load64(src, s - 2);
                        let m2h = hash6(x, TABLE_BITS);
                        let ch = hash6(x >> 16, TABLE_BITS);
                        cand = table[ch] as usize;
                        table[m2h] = (s - 2) as u32;
                        table[ch] = s as u32;
                        if ((x >> 16) as u32) != load32(src, cand) {
                            cv = load64(src, s + 1);
                            s += 1;
                            break;
                        }
                    }
                }
            }
        }

        // Continue searching without dict for remainder of block > 64KB
        let _ = repeat;
        s_limit = src.len() - INPUT_MARGIN;
        if s < s_limit {
            cv = load64(src, s);
            'no_dict: loop {
                let mut cand;
                loop {
                    let next_s = s + ((s - next_emit) >> 6) + 4;
                    if next_s > s_limit {
                        break 'no_dict;
                    }
                    let h0 = hash6(cv, TABLE_BITS);
                    let h1 = hash6(cv >> 8, TABLE_BITS);
                    cand = table[h0] as usize;
                    let cand2 = table[h1] as usize;
                    table[h0] = s as u32;
                    table[h1] = (s + 1) as u32;
                    let h2 = hash6(cv >> 16, TABLE_BITS);

                    if (cv as u32) == load32(src, cand) {
                        break;
                    }
                    cand = table[h2] as usize;
                    if ((cv >> 8) as u32) == load32(src, cand2) {
                        table[h2] = (s + 2) as u32;
                        cand = cand2;
                        s += 1;
                        break;
                    }
                    table[h2] = (s + 2) as u32;
                    if ((cv >> 16) as u32) == load32(src, cand) {
                        s += 2;
                        break;
                    }
                    cv = load64(src, next_s);
                    s = next_s;
                }
                while cand > 0 && s > next_emit && src[cand - 1] == src[s - 1] {
                    cand -= 1;
                    s -= 1;
                }
                if d + (s - next_emit) > dst_limit {
                    return 0;
                }
                d += emit_literal(&mut dst[d..], &src[next_emit..s]);
                loop {
                    let base = s;
                    repeat = base - cand;
                    s += 4;
                    cand += 4;
                    while s <= src.len() - 8 {
                        let diff = load64(src, s) ^ load64(src, cand);
                        if diff != 0 {
                            s += (diff.trailing_zeros() >> 3) as usize;
                            break;
                        }
                        s += 8;
                        cand += 8;
                    }
                    d += emit_copy(&mut dst[d..], repeat, s - base);
                    next_emit = s;
                    if s >= s_limit {
                        break 'no_dict;
                    }
                    if d > dst_limit {
                        return 0;
                    }
                    let x = load64(src, s - 2);
                    let m2h = hash6(x, TABLE_BITS);
                    let ch = hash6(x >> 16, TABLE_BITS);
                    cand = table[ch] as usize;
                    table[m2h] = (s - 2) as u32;
                    table[ch] = s as u32;
                    if ((x >> 16) as u32) != load32(src, cand) {
                        cv = load64(src, s + 1);
                        s += 1;
                        break;
                    }
                }
            }
        }

        if next_emit < src.len() {
            if d + (src.len() - next_emit) > dst_limit {
                return 0;
            }
            d += emit_literal(&mut dst[d..], &src[next_emit..]);
        }
        d
    }

    pub fn encode_block_better(&self, dst: &mut [u8], src: &[u8]) -> usize {
        if src.len() < MIN_NON_LITERAL_BLOCK_SIZE {
            return 0;
        }
        const L_BITS: u8 = 17;
        const S_BITS: u8 = 14;
        const MAX_AHEAD: usize = 8;
        let (dict_l, dict_s) = self.init_better();
        let mut l_table = vec![0u32; 1 << L_BITS];
        let mut s_table = vec![0u32; 1 << S_BITS];

        let mut s_limit = (src.len() - INPUT_MARGIN).min(MAX_DICT_SRC_OFFSET - MAX_AHEAD);
        let dst_limit = src.len() - (src.len() >> 5) - 6;
        let mut d = 0usize;
        let mut next_emit = 0usize;
        let mut s = 0usize;
        let mut cv = load64(src, s);
        let mut repeat = self.dict.len() - self.repeat;

        'search_dict: loop {
            let mut next_s;
            let mut cand_l;
            enum Action {
                Dict,
                Match,
            }
            let act: Action;
            loop {
                next_s = s + ((s - next_emit) >> 7) + 1;
                if next_s > s_limit {
                    break 'search_dict;
                }
                let hl = hash7(cv, L_BITS);
                let hs = hash4(cv, S_BITS);
                cand_l = l_table[hl] as usize;
                let cand_s = s_table[hs] as usize;
                let dl = dict_l[hl] as usize;
                let ds = dict_s[hs] as usize;
                l_table[hl] = s as u32;
                s_table[hs] = s as u32;

                let val_long = load64(src, cand_l);
                let val_short = load64(src, cand_s);
                if s != 0 {
                    if cv == val_long {
                        act = Action::Match;
                        break;
                    }
                    if cv == val_short {
                        cand_l = cand_s;
                        act = Action::Match;
                        break;
                    }
                }

                if repeat >= s + 4 {
                    let dc_signed = self.dict.len() as isize - repeat as isize + s as isize;
                    if dc_signed > 0 {
                    let mut dc = dc_signed as usize;
                    if dc + 4 <= self.dict.len() && (cv as u32) == load32(&self.dict, dc) {
                        let mut base = s;
                        let mut i = dc;
                        while base > next_emit && i > 0 && self.dict[i - 1] == src[base - 1] {
                            i -= 1;
                            base -= 1;
                        }
                        if d + (base - next_emit) > dst_limit {
                            return 0;
                        }
                        d += emit_literal(&mut dst[d..], &src[next_emit..base]);
                        s += 4;
                        dc += 4;
                        while dc <= self.dict.len().saturating_sub(8) && s <= src.len() - 8 {
                            let diff = load64(src, s) ^ load64(&self.dict, dc);
                            if diff != 0 {
                                s += (diff.trailing_zeros() >> 3) as usize;
                                break;
                            }
                            s += 8;
                            dc += 8;
                        }
                        d += emit_repeat(&mut dst[d..], repeat, s - base);
                        next_emit = s;
                        if s >= s_limit {
                            break 'search_dict;
                        }
                        cv = load64(src, s);
                        continue;
                    }
                    }
                }

                if s == 0 {
                    cv = load64(src, next_s);
                    s = next_s;
                    continue;
                }

                if (cv as u32) == (val_long as u32) {
                    act = Action::Match;
                    break;
                }
                if dl + 4 <= self.dict.len() && (cv as u32) == load32(&self.dict, dl) {
                    cand_l = dl;
                    act = Action::Dict;
                    break;
                }
                if (cv as u32) == (val_short as u32) {
                    let hl2 = hash7(cv >> 8, L_BITS);
                    let cl2 = l_table[hl2] as usize;
                    l_table[hl2] = (s + 1) as u32;
                    if ((cv >> 8) as u32) == load32(src, cl2) {
                        s += 1;
                        cand_l = cl2;
                        act = Action::Match;
                        break;
                    }
                    cand_l = cand_s;
                    act = Action::Match;
                    break;
                }
                if ds + 4 <= self.dict.len() && (cv as u32) == load32(&self.dict, ds) {
                    let hl2 = hash7(cv >> 8, L_BITS);
                    let cl2 = l_table[hl2] as usize;
                    l_table[hl2] = (s + 1) as u32;
                    if ((cv >> 8) as u32) == load32(src, cl2) {
                        s += 1;
                        cand_l = cl2;
                        act = Action::Match;
                        break;
                    }
                    cand_l = ds;
                    act = Action::Dict;
                    break;
                }

                cv = load64(src, next_s);
                s = next_s;
            }

            match act {
                Action::Dict => {
                    while cand_l > 0 && s > next_emit && self.dict[cand_l - 1] == src[s - 1] {
                        cand_l -= 1;
                        s -= 1;
                    }
                    if d + (s - next_emit) > dst_limit {
                        return 0;
                    }
                    d += emit_literal(&mut dst[d..], &src[next_emit..s]);
                    let base = s;
                    let offset = s + self.dict.len() - cand_l;
                    s += 4;
                    cand_l += 4;
                    while s <= src.len() - 8 && self.dict.len() - cand_l >= 8 {
                        let diff = load64(src, s) ^ load64(&self.dict, cand_l);
                        if diff != 0 {
                            s += (diff.trailing_zeros() >> 3) as usize;
                            break;
                        }
                        s += 8;
                        cand_l += 8;
                    }
                    if repeat == offset {
                        d += emit_repeat(&mut dst[d..], offset, s - base);
                    } else {
                        if s <= s_limit || s - base < 8 {
                            d += emit_copy(&mut dst[d..], offset, s - base);
                        } else {
                            d += emit_copy(&mut dst[d..], offset, 4);
                            d += emit_repeat(&mut dst[d..], offset, s - base - 4);
                        }
                        repeat = offset;
                    }
                    next_emit = s;
                    if s >= s_limit {
                        break 'search_dict;
                    }
                    if d > dst_limit {
                        return 0;
                    }
                    cv = load64(src, s);
                }
                Action::Match => {
                    while cand_l > 0 && s > next_emit && src[cand_l - 1] == src[s - 1] {
                        cand_l -= 1;
                        s -= 1;
                    }
                    if d + (s - next_emit) > dst_limit {
                        return 0;
                    }
                    let base = s;
                    let offset = base - cand_l;
                    s += 4;
                    cand_l += 4;
                    while s < src.len() {
                        if src.len() - s < 8 {
                            if src[s] == src[cand_l] {
                                s += 1;
                                cand_l += 1;
                                continue;
                            }
                            break;
                        }
                        let diff = load64(src, s) ^ load64(src, cand_l);
                        if diff != 0 {
                            s += (diff.trailing_zeros() >> 3) as usize;
                            break;
                        }
                        s += 8;
                        cand_l += 8;
                    }
                    if offset > 65535 && s - base <= 5 && repeat != offset {
                        s = next_s + 1;
                        if s >= s_limit {
                            break 'search_dict;
                        }
                        cv = load64(src, s);
                        continue;
                    }
                    d += emit_literal(&mut dst[d..], &src[next_emit..base]);
                    if repeat == offset {
                        d += emit_repeat(&mut dst[d..], offset, s - base);
                    } else {
                        d += emit_copy(&mut dst[d..], offset, s - base);
                        repeat = offset;
                    }
                    next_emit = s;
                    if s >= s_limit {
                        break 'search_dict;
                    }
                    if d > dst_limit {
                        return 0;
                    }
                    cv = load64(src, s);
                }
            }
        }

        // Tail past MaxDictSrcOffset
        if repeat > s {
            repeat = 0;
        }
        s_limit = src.len() - INPUT_MARGIN;
        if s < s_limit {
            cv = load64(src, s);
            'tail: loop {
                let mut cand_l;
                let mut next_s;
                loop {
                    next_s = s + (((s - next_emit) >> 7) + 1).min(100);
                    if next_s > s_limit {
                        break 'tail;
                    }
                    let hl = hash7(cv, L_BITS);
                    let hs = hash4(cv, S_BITS);
                    cand_l = l_table[hl] as usize;
                    let cand_s = s_table[hs] as usize;
                    l_table[hl] = s as u32;
                    s_table[hs] = s as u32;
                    let val_l = load64(src, cand_l);
                    let val_s = load64(src, cand_s);
                    if cv == val_l {
                        break;
                    }
                    if cv == val_s {
                        cand_l = cand_s;
                        break;
                    }
                    if (cv as u32) == (val_l as u32) {
                        break;
                    }
                    if (cv as u32) == (val_s as u32) {
                        cand_l = cand_s;
                        break;
                    }
                    cv = load64(src, next_s);
                    s = next_s;
                }
                while cand_l > 0 && s > next_emit && src[cand_l - 1] == src[s - 1] {
                    cand_l -= 1;
                    s -= 1;
                }
                if d + (s - next_emit) > dst_limit {
                    return 0;
                }
                let base = s;
                let offset = base - cand_l;
                s += 4;
                cand_l += 4;
                while s < src.len() {
                    if src.len() - s < 8 {
                        if src[s] == src[cand_l] {
                            s += 1;
                            cand_l += 1;
                            continue;
                        }
                        break;
                    }
                    let diff = load64(src, s) ^ load64(src, cand_l);
                    if diff != 0 {
                        s += (diff.trailing_zeros() >> 3) as usize;
                        break;
                    }
                    s += 8;
                    cand_l += 8;
                }
                if offset > 65535 && s - base <= 5 && repeat != offset {
                    s = next_s + 1;
                    if s >= s_limit {
                        break 'tail;
                    }
                    cv = load64(src, s);
                    continue;
                }
                d += emit_literal(&mut dst[d..], &src[next_emit..base]);
                if repeat == offset {
                    d += emit_repeat(&mut dst[d..], offset, s - base);
                } else {
                    d += emit_copy(&mut dst[d..], offset, s - base);
                    repeat = offset;
                }
                next_emit = s;
                if s >= s_limit {
                    break 'tail;
                }
                if d > dst_limit {
                    return 0;
                }
                cv = load64(src, s);
            }
        }

        if next_emit < src.len() {
            if d + (src.len() - next_emit) > dst_limit {
                return 0;
            }
            d += emit_literal(&mut dst[d..], &src[next_emit..]);
        }
        d
    }
}
