//! Syntax errors of `github.com/grafana/regexp/syntax.Parse(s, syntax.Perl|syntax.DotNL)` (the
//! `regexp` fork Prometheus vendors), which `labels.NewFastRegexMatcher` uses to validate regex
//! matchers. Go and the Rust `regex` crate
//! accept slightly different syntaxes and report errors differently; this module reproduces the
//! exact Go error (`error parsing regexp: <code>: `<expr>``) so the error text that reaches the
//! client (e.g. the gRPC status message of a read RPC) is byte-identical.
//!
//! Ported from the error paths of github.com/grafana/regexp/syntax/parse.go (`parse`, `repeat`,
//! `repeatIsValid`, `parseRepeat`, `parseInt`, `parsePerlFlags`, `isValidCaptureName`,
//! `parseRightParen`, `parseEscape`, `parseClassChar`, `parsePerlClassEscape`, `parseNamedClass`,
//! `parseUnicodeClass`, `unicodeTable`, `parseClass`) and its perl_groups.go, as vendored by
//! `.ref/mimir` (`v0.0.0-20250905101755-5eb4f3acbf71`). Unlike Go 1.25+'s `regexp/syntax`, this
//! fork matches `\p{...}` names exactly (no `canonicalName` / `CategoryAliases`); the Unicode
//! tables are those of the Go 1.27 toolchain of `.ref/mimir/go.mod`.
//!
//! Only what decides *whether and where* Go fails is modelled: the parse stack is reduced to one
//! frame per open group with the threshold `repeatIsValid` needs for the last item, so the check
//! is a single allocation-free pass (one `Vec` push per open group). Not modelled: the
//! `ErrLarge` / `ErrNestingDepth` limits (`checkLimits`, only reached by expressions with more
//! than 1000 nested or millions of compiled instructions) and `ErrInvalidUTF8` (Rust strings
//! are always valid UTF-8).

use std::fmt;

/// A Go `*syntax.Error`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GoSyntaxError {
    /// Go `syntax.ErrorCode` text.
    pub code: &'static str,
    /// Go `Error.Expr`: the offending part of the expression.
    pub expr: String,
}

impl fmt::Display for GoSyntaxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "error parsing regexp: {}: `{}`", self.code, self.expr)
    }
}

impl std::error::Error for GoSyntaxError {}

const ERR_INVALID_CHAR_RANGE: &str = "invalid character class range";
const ERR_INVALID_ESCAPE: &str = "invalid escape sequence";
const ERR_INVALID_NAMED_CAPTURE: &str = "invalid named capture";
const ERR_INVALID_PERL_OP: &str = "invalid or unsupported Perl syntax";
const ERR_INVALID_REPEAT_OP: &str = "invalid nested repetition operator";
const ERR_INVALID_REPEAT_SIZE: &str = "invalid repeat count";
const ERR_MISSING_BRACKET: &str = "missing closing ]";
const ERR_MISSING_PAREN: &str = "missing closing )";
const ERR_MISSING_REPEAT_ARGUMENT: &str = "missing argument to repetition operator";
const ERR_TRAILING_BACKSLASH: &str = "trailing backslash at end of expression";
const ERR_UNEXPECTED_PAREN: &str = "unexpected )";

/// Go `maxRepeat` / the `n` of `repeatIsValid(re, 1000)`.
const MAX_REPEAT: u64 = 1000;

/// Go `unicode.MaxRune`.
const MAX_RUNE: i64 = 0x10FFFF;

/// Names accepted by grafana/regexp `unicodeTable`: `Any` and the `unicode.Categories` and
/// `unicode.Scripts` keys (Go 1.27), matched exactly. Sorted for binary search.
const UNICODE_TABLE_NAMES: &[&str] = &[
    "Adlam", "Ahom", "Anatolian_Hieroglyphs", "Any", "Arabic", "Armenian",
    "Avestan", "Balinese", "Bamum", "Bassa_Vah", "Batak", "Bengali",
    "Beria_Erfe", "Bhaiksuki", "Bopomofo", "Brahmi", "Braille", "Buginese",
    "Buhid", "C", "Canadian_Aboriginal", "Carian", "Caucasian_Albanian", "Cc",
    "Cf", "Chakma", "Cham", "Cherokee", "Chorasmian", "Cn",
    "Co", "Common", "Coptic", "Cs", "Cuneiform", "Cypriot",
    "Cypro_Minoan", "Cyrillic", "Deseret", "Devanagari", "Dives_Akuru", "Dogra",
    "Duployan", "Egyptian_Hieroglyphs", "Elbasan", "Elymaic", "Ethiopic", "Garay",
    "Georgian", "Glagolitic", "Gothic", "Grantha", "Greek", "Gujarati",
    "Gunjala_Gondi", "Gurmukhi", "Gurung_Khema", "Han", "Hangul", "Hanifi_Rohingya",
    "Hanunoo", "Hatran", "Hebrew", "Hiragana", "Imperial_Aramaic", "Inherited",
    "Inscriptional_Pahlavi", "Inscriptional_Parthian", "Javanese", "Kaithi", "Kannada", "Katakana",
    "Kawi", "Kayah_Li", "Kharoshthi", "Khitan_Small_Script", "Khmer", "Khojki",
    "Khudawadi", "Kirat_Rai", "L", "LC", "Lao", "Latin",
    "Lepcha", "Limbu", "Linear_A", "Linear_B", "Lisu", "Ll",
    "Lm", "Lo", "Lt", "Lu", "Lycian", "Lydian",
    "M", "Mahajani", "Makasar", "Malayalam", "Mandaic", "Manichaean",
    "Marchen", "Masaram_Gondi", "Mc", "Me", "Medefaidrin", "Meetei_Mayek",
    "Mende_Kikakui", "Meroitic_Cursive", "Meroitic_Hieroglyphs", "Miao", "Mn", "Modi",
    "Mongolian", "Mro", "Multani", "Myanmar", "N", "Nabataean",
    "Nag_Mundari", "Nandinagari", "Nd", "New_Tai_Lue", "Newa", "Nko",
    "Nl", "No", "Nushu", "Nyiakeng_Puachue_Hmong", "Ogham", "Ol_Chiki",
    "Ol_Onal", "Old_Hungarian", "Old_Italic", "Old_North_Arabian", "Old_Permic", "Old_Persian",
    "Old_Sogdian", "Old_South_Arabian", "Old_Turkic", "Old_Uyghur", "Oriya", "Osage",
    "Osmanya", "P", "Pahawh_Hmong", "Palmyrene", "Pau_Cin_Hau", "Pc",
    "Pd", "Pe", "Pf", "Phags_Pa", "Phoenician", "Pi",
    "Po", "Ps", "Psalter_Pahlavi", "Rejang", "Runic", "S",
    "Samaritan", "Saurashtra", "Sc", "Sharada", "Shavian", "Siddham",
    "Sidetic", "SignWriting", "Sinhala", "Sk", "Sm", "So",
    "Sogdian", "Sora_Sompeng", "Soyombo", "Sundanese", "Sunuwar", "Syloti_Nagri",
    "Syriac", "Tagalog", "Tagbanwa", "Tai_Le", "Tai_Tham", "Tai_Viet",
    "Tai_Yo", "Takri", "Tamil", "Tangsa", "Tangut", "Telugu",
    "Thaana", "Thai", "Tibetan", "Tifinagh", "Tirhuta", "Todhri",
    "Tolong_Siki", "Toto", "Tulu_Tigalari", "Ugaritic", "Vai", "Vithkuqi",
    "Wancho", "Warang_Citi", "Yezidi", "Yi", "Z", "Zanabazar_Square",
    "Zl", "Zp", "Zs",
];

/// Go `perlGroup` keys.
const PERL_GROUPS: &[&str] = &[r"\d", r"\D", r"\s", r"\S", r"\w", r"\W"];

/// Go `posixGroup` keys.
const POSIX_GROUPS: &[&str] = &[
    "[:alnum:]", "[:^alnum:]", "[:alpha:]", "[:^alpha:]", "[:ascii:]", "[:^ascii:]",
    "[:blank:]", "[:^blank:]", "[:cntrl:]", "[:^cntrl:]", "[:digit:]", "[:^digit:]",
    "[:graph:]", "[:^graph:]", "[:lower:]", "[:^lower:]", "[:print:]", "[:^print:]",
    "[:punct:]", "[:^punct:]", "[:space:]", "[:^space:]", "[:upper:]", "[:^upper:]",
    "[:word:]", "[:^word:]", "[:xdigit:]", "[:^xdigit:]",
];

fn err(code: &'static str, expr: &str) -> GoSyntaxError {
    GoSyntaxError {
        code,
        expr: expr.to_string(),
    }
}

/// Result of translating a Go `regexp/syntax` (`Perl|DotNL`) pattern into an equivalent
/// Rust `regex` pattern.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TranslatedRegex {
    pub pattern: String,
    pub has_case_insensitive: bool,
}

/// One open group (or the whole expression) of the Go parse stack.
#[derive(Default)]
struct Frame {
    /// `repeatIsValid` threshold of the topmost item, `None` when the stack top is a pseudo
    /// operator (`(` or `|`) or the stack is empty.
    last: Option<u64>,
    /// Maximum threshold of every other item and alternative of the group.
    rest: u64,
    /// Whether `(?i)` (FoldCase) is currently active in this group frame.
    fold_case: bool,
}

impl Frame {
    fn push(&mut self, threshold: u64) {
        if let Some(prev) = self.last.replace(threshold) {
            self.rest = self.rest.max(prev);
        }
    }

    /// Threshold of the whole group (`concat` + `alternate`).
    fn close(&self) -> u64 {
        self.rest.max(self.last.unwrap_or(0))
    }
}

/// Validates `s` like Go `syntax.Parse(s, syntax.Perl|syntax.DotNL)`, returning Go's error.
///
/// `repeatIsValid(re, n)` is evaluated through thresholds: the smallest `n` for which a subtree
/// is valid is 0 for leaves, the maximum over the children of concatenations, alternations,
/// groups and `*`/`+`/`?`, and `max(m, m * T(sub))` for `x{min,max}` with `m = max` (or `min`
/// when unbounded; 0 for `max == 0`, which Go accepts without looking at the sub-expression).
pub fn check(s: &str) -> Result<(), GoSyntaxError> {
    translate_to_rust(s).map(|_| ())
}

/// Validates `s` like Go `syntax.Parse(s, syntax.Perl|syntax.DotNL)` and translates Go-specific
/// RE2 syntax (`\<`, `\>`, flat `[...]` classes without set ops, `\Q...\E`, octal escapes,
/// ASCII `\d`/`\s`/`\w`, non-repeat `{`, etc.) into an equivalent Rust `regex` pattern.
pub fn translate_to_rust(s: &str) -> Result<TranslatedRegex, GoSyntaxError> {
    let b = s.as_bytes();
    let len = b.len();
    let mut frames: Vec<Frame> = vec![Frame::default()];
    let mut out = String::with_capacity(s.len());
    let mut has_case_insensitive = false;
    // Start offset of the previous repetition operator (Go `lastRepeat`).
    let mut last_repeat: Option<usize> = None;
    let mut pos = 0;
    while pos < len {
        let mut repeat: Option<usize> = None;
        match b[pos] {
            b'(' => {
                if len - pos >= 2 && b[pos + 1] == b'?' {
                    let next = parse_perl_flags(s, pos, &mut frames)?;
                    out.push_str(&s[pos..next]);
                    if top(&mut frames).fold_case {
                        has_case_insensitive = true;
                    }
                    pos = next;
                } else {
                    let parent_fold = top(&mut frames).fold_case;
                    frames.push(Frame {
                        fold_case: parent_fold,
                        ..Frame::default()
                    });
                    out.push('(');
                    pos += 1;
                }
            }
            b'|' => {
                let cur = top(&mut frames);
                cur.rest = cur.close();
                cur.last = None;
                out.push('|');
                pos += 1;
            }
            b')' => {
                if frames.len() < 2 {
                    return Err(err(ERR_UNEXPECTED_PAREN, s));
                }
                let threshold = frames.pop().map_or(0, |f| f.close());
                top(&mut frames).push(threshold);
                out.push(')');
                pos += 1;
            }
            b'^' | b'$' | b'.' => {
                top(&mut frames).push(0);
                out.push(b[pos] as char);
                pos += 1;
            }
            b'[' => {
                if top(&mut frames).fold_case {
                    has_case_insensitive = true;
                }
                pos = parse_class_into(s, pos, &mut out)?;
                top(&mut frames).push(0);
            }
            b'*' | b'+' | b'?' => {
                let before = pos;
                pos = apply_repeat(s, &mut frames, None, before, pos + 1, last_repeat)?;
                out.push_str(&s[before..pos]);
                repeat = Some(before);
            }
            b'{' => {
                let before = pos;
                match parse_repeat(b, pos) {
                    None => {
                        // Not a repetition: `{` is a literal in Go (e.g. `a{01}`, `a{,2}`, `x{1001`).
                        top(&mut frames).push(0);
                        out.push_str(r"\{");
                        pos += 1;
                    }
                    Some((min, max, after)) => {
                        if !(0..=1000).contains(&min) || max > 1000 || (max >= 0 && min > max) {
                            return Err(err(ERR_INVALID_REPEAT_SIZE, &s[before..after]));
                        }
                        pos = apply_repeat(s, &mut frames, Some((min, max)), before, after, last_repeat)?;
                        out.push_str(&s[before..pos]);
                        repeat = Some(before);
                    }
                }
            }
            b'\\' => {
                let fold = top(&mut frames).fold_case;
                let (next, used_lit) = parse_top_level_escape_into(s, pos, &mut frames, &mut out)?;
                if fold && used_lit {
                    has_case_insensitive = true;
                }
                pos = next;
            }
            _ => {
                let clen = next_char_len(s, pos);
                let ch_str = &s[pos..pos + clen];
                if top(&mut frames).fold_case && ch_str.chars().any(char::is_alphabetic) {
                    has_case_insensitive = true;
                }
                out.push_str(ch_str);
                pos += clen;
                top(&mut frames).push(0);
            }
        }
        last_repeat = repeat;
    }
    if frames.len() != 1 {
        return Err(err(ERR_MISSING_PAREN, s));
    }
    Ok(TranslatedRegex {
        pattern: out,
        has_case_insensitive,
    })
}

fn top(frames: &mut [Frame]) -> &mut Frame {
    // The root frame is never popped (`)` without an open group fails first).
    let idx = frames.len().saturating_sub(1);
    &mut frames[idx]
}

fn next_char_len(s: &str, pos: usize) -> usize {
    s[pos..].chars().next().map_or(1, char::len_utf8)
}

fn is_alnum(c: char) -> bool {
    c.is_ascii_alphanumeric()
}

/// Go `parser.repeat`: `bounds` is `Some((min, max))` for `{min,max}` (OpRepeat), `None` for
/// `*`, `+` and `?`.
fn apply_repeat(
    s: &str,
    frames: &mut [Frame],
    bounds: Option<(i64, i64)>,
    before: usize,
    mut after: usize,
    last_repeat: Option<usize>,
) -> Result<usize, GoSyntaxError> {
    let b = s.as_bytes();
    // PerlX: non-greedy suffix, no stacked repetition operators.
    if after < b.len() && b[after] == b'?' {
        after += 1;
    }
    if let Some(lr) = last_repeat {
        return Err(err(ERR_INVALID_REPEAT_OP, &s[lr..after]));
    }
    let cur = top(frames);
    let Some(sub) = cur.last else {
        return Err(err(ERR_MISSING_REPEAT_ARGUMENT, &s[before..after]));
    };
    let threshold = match bounds {
        None => sub,
        Some((min, max)) => {
            let threshold = if max == 0 {
                0
            } else {
                let m = if max < 0 { min } else { max } as u64;
                if m == 0 {
                    sub
                } else {
                    m.max(m.saturating_mul(sub))
                }
            };
            if (min >= 2 || max >= 2) && threshold > MAX_REPEAT {
                return Err(err(ERR_INVALID_REPEAT_SIZE, &s[before..after]));
            }
            threshold
        }
    };
    cur.last = Some(threshold);
    Ok(after)
}

/// Go `parser.parseInt`: `None` when `s` does not start with a decimal integer (leading zeros
/// rejected); `-1` when the value reaches 1e8.
fn parse_int(b: &[u8], mut pos: usize) -> Option<(i64, usize)> {
    if pos >= b.len() || !b[pos].is_ascii_digit() {
        return None;
    }
    if b.len() - pos >= 2 && b[pos] == b'0' && b[pos + 1].is_ascii_digit() {
        return None;
    }
    let start = pos;
    while pos < b.len() && b[pos].is_ascii_digit() {
        pos += 1;
    }
    let mut n: i64 = 0;
    for &d in &b[start..pos] {
        if n >= 100_000_000 {
            n = -1;
            break;
        }
        n = n * 10 + i64::from(d - b'0');
    }
    Some((n, pos))
}

/// Go `parser.parseRepeat`: `{min}`, `{min,}` (max -1) or `{min,max}`.
fn parse_repeat(b: &[u8], pos: usize) -> Option<(i64, i64, usize)> {
    if pos >= b.len() || b[pos] != b'{' {
        return None;
    }
    let (mut min, mut p) = parse_int(b, pos + 1)?;
    if p >= b.len() {
        return None;
    }
    let max;
    if b[p] != b',' {
        max = min;
    } else {
        p += 1;
        if p >= b.len() {
            return None;
        }
        if b[p] == b'}' {
            max = -1;
        } else {
            let (m, np) = parse_int(b, p)?;
            p = np;
            max = m;
            if max < 0 {
                min = -1;
            }
        }
    }
    if p >= b.len() || b[p] != b'}' {
        return None;
    }
    Some((min, max, p + 1))
}

/// Go `parser.parsePerlFlags`; `pos` is at `(?`.
fn parse_perl_flags(s: &str, pos: usize, frames: &mut Vec<Frame>) -> Result<usize, GoSyntaxError> {
    let t = &s[pos..];
    let tb = t.as_bytes();
    let starts_with_p = tb.len() > 4 && tb[2] == b'P' && tb[3] == b'<';
    let starts_with_name = tb.len() > 3 && tb[2] == b'<';
    if starts_with_p || starts_with_name {
        let expr_start = if starts_with_name { 3 } else { 4 };
        let Some(end) = t.find('>') else {
            return Err(err(ERR_INVALID_NAMED_CAPTURE, t));
        };
        let capture = &t[..=end];
        let name = &t[expr_start..end];
        if name.is_empty() || !name.chars().all(|c| c == '_' || is_alnum(c)) {
            return Err(err(ERR_INVALID_NAMED_CAPTURE, capture));
        }
        let parent_fold = top(frames).fold_case;
        frames.push(Frame {
            fold_case: parent_fold,
            ..Frame::default()
        });
        return Ok(pos + end + 1);
    }

    let mut i = pos + 2;
    let mut negated = false;
    let mut saw_flag = false;
    let mut next_fold = top(frames).fold_case;
    while let Some(c) = s[i..].chars().next() {
        i += c.len_utf8();
        match c {
            'i' => {
                saw_flag = true;
                next_fold = !negated;
            }
            'm' | 's' | 'U' => saw_flag = true,
            '-' => {
                if negated {
                    break;
                }
                negated = true;
                saw_flag = false;
            }
            ':' | ')' => {
                if negated && !saw_flag {
                    break;
                }
                if c == ':' {
                    // Non-capturing group.
                    frames.push(Frame {
                        fold_case: next_fold,
                        ..Frame::default()
                    });
                } else {
                    top(frames).fold_case = next_fold;
                }
                return Ok(i);
            }
            _ => break,
        }
    }
    Err(err(ERR_INVALID_PERL_OP, &s[pos..i]))
}

#[inline]
fn is_regex_meta(c: char) -> bool {
    matches!(
        c,
        '\\' | '+' | '.' | '*' | '?' | '(' | ')' | '|' | '[' | ']' | '{' | '}' | '^' | '$'
    )
}

fn push_outside_char(out: &mut String, c: char) {
    if is_regex_meta(c) {
        out.push('\\');
    }
    out.push(c);
}

fn push_outside_rune(out: &mut String, r: i64) {
    if let Some(c) = u32::try_from(r).ok().and_then(char::from_u32) {
        if (0x20..=0x7E).contains(&r) {
            push_outside_char(out, c);
            return;
        }
    }
    use std::fmt::Write;
    let _ = write!(out, "\\x{{{r:X}}}");
}

fn push_class_rune(out: &mut String, r: i64) {
    if let Some(c) = u32::try_from(r).ok().and_then(char::from_u32) {
        if matches!(c, '\\' | '-' | '[' | ']' | '^' | '&' | '~') {
            out.push('\\');
            out.push(c);
            return;
        }
        if (0x20..=0x7E).contains(&r) {
            out.push(c);
            return;
        }
    }
    use std::fmt::Write;
    let _ = write!(out, "\\x{{{r:X}}}");
}

/// The `\` case of Go `parse`; `pos` is at the backslash. Returns `(next_pos, emitted_literal_or_class)`.
fn parse_top_level_escape_into(
    s: &str,
    pos: usize,
    frames: &mut [Frame],
    out: &mut String,
) -> Result<(usize, bool), GoSyntaxError> {
    let b = s.as_bytes();
    if b.len() - pos >= 2 {
        match b[pos + 1] {
            b'A' | b'z' => {
                top(frames).push(0);
                out.push('\\');
                out.push(b[pos + 1] as char);
                return Ok((pos + 2, false));
            }
            b'b' | b'B' => {
                top(frames).push(0);
                // Prevent Rust `\b{start}` / `\B{...}` syntax when followed by `{`.
                if b.get(pos + 2) == Some(&b'{') {
                    out.push_str("(?:\\");
                    out.push(b[pos + 1] as char);
                    out.push(')');
                } else {
                    out.push('\\');
                    out.push(b[pos + 1] as char);
                }
                return Ok((pos + 2, false));
            }
            // Any byte: not supported.
            b'C' => return Err(err(ERR_INVALID_ESCAPE, &s[pos..pos + 2])),
            b'Q' => {
                // `\Q ... \E`: the text is literal (up to the end without `\E`).
                let rest = &s[pos + 2..];
                let (lit, end) = match rest.find(r"\E") {
                    Some(i) => (&rest[..i], pos + 2 + i + 2),
                    None => (rest, s.len()),
                };
                if !lit.is_empty() {
                    top(frames).push(0);
                    for c in lit.chars() {
                        push_outside_char(out, c);
                    }
                    return Ok((end, true));
                }
                out.push_str("(?:)");
                return Ok((end, false));
            }
            _ => {}
        }
    }
    if let Some(end) = parse_unicode_class(s, pos)? {
        top(frames).push(0);
        out.push_str(&s[pos..end]);
        return Ok((end, true));
    }
    if let Some(end) = parse_perl_class_escape(s, pos) {
        top(frames).push(0);
        // Go `Perl` mode `\d`, `\s`, `\w` are strictly ASCII, whereas Rust `regex`
        // defaults to Unicode mode for `\d`/`\s`/`\w`.
        match &s[pos..end] {
            r"\d" => out.push_str("[0-9]"),
            r"\D" => out.push_str("[^0-9]"),
            r"\s" => out.push_str(r"[\t\n\f\r ]"),
            r"\S" => out.push_str(r"[^\t\n\f\r ]"),
            r"\w" => out.push_str("[0-9A-Z_a-z]"),
            r"\W" => out.push_str("[^0-9A-Z_a-z]"),
            other => out.push_str(other),
        }
        return Ok((end, true));
    }
    let (r, end) = parse_escape(s, pos)?;
    top(frames).push(0);
    push_outside_rune(out, r);
    Ok((end, true))
}

/// Go `parser.parsePerlClassEscape` (`\d`, `\s`, `\w` and negations).
fn parse_perl_class_escape(s: &str, pos: usize) -> Option<usize> {
    let t = s.get(pos..pos + 2)?;
    PERL_GROUPS.contains(&t).then_some(pos + 2)
}

fn unhex(c: char) -> i64 {
    c.to_digit(16).map_or(-1, i64::from)
}

/// Go `parser.parseEscape`; `pos` is at the backslash. Returns the rune and the end offset.
fn parse_escape(s: &str, pos: usize) -> Result<(i64, usize), GoSyntaxError> {
    let b = s.as_bytes();
    let mut t = pos + 1;
    let Some(c) = s[t..].chars().next() else {
        return Err(err(ERR_TRAILING_BACKSLASH, ""));
    };
    t += c.len_utf8();
    let invalid = |t: usize| Err(err(ERR_INVALID_ESCAPE, &s[pos..t]));
    match c {
        '1'..='7' if t >= b.len() || !(b'0'..=b'7').contains(&b[t]) => {
            // A single non-zero digit is a backreference: not supported.
            invalid(t)
        }
        '0'..='7' => {
            let mut r = i64::from(c as u8 - b'0');
            for _ in 1..3 {
                if t >= b.len() || !(b'0'..=b'7').contains(&b[t]) {
                    break;
                }
                r = r * 8 + i64::from(b[t] - b'0');
                t += 1;
            }
            Ok((r, t))
        }
        'x' => {
            let Some(c) = s[t..].chars().next() else {
                return invalid(t);
            };
            t += c.len_utf8();
            if c == '{' {
                let mut nhex = 0;
                let mut r: i64 = 0;
                loop {
                    let Some(c) = s[t..].chars().next() else {
                        return invalid(t);
                    };
                    t += c.len_utf8();
                    if c == '}' {
                        break;
                    }
                    let v = unhex(c);
                    if v < 0 {
                        return invalid(t);
                    }
                    r = r * 16 + v;
                    if r > MAX_RUNE {
                        return invalid(t);
                    }
                    nhex += 1;
                }
                if nhex == 0 {
                    return invalid(t);
                }
                return Ok((r, t));
            }
            let x = unhex(c);
            // Go `nextRune("")` yields `utf8.RuneError` without consuming anything.
            let y = match s[t..].chars().next() {
                Some(c) => {
                    t += c.len_utf8();
                    unhex(c)
                }
                None => -1,
            };
            if x < 0 || y < 0 {
                return invalid(t);
            }
            Ok((x * 16 + y, t))
        }
        'a' => Ok((0x07, t)),
        'f' => Ok((0x0C, t)),
        'n' => Ok((0x0A, t)),
        'r' => Ok((0x0D, t)),
        't' => Ok((0x09, t)),
        'v' => Ok((0x0B, t)),
        // Escaped non-word ASCII characters are always themselves.
        c if c.is_ascii() && !is_alnum(c) => Ok((i64::from(c as u8), t)),
        _ => invalid(t),
    }
}

/// Go `parser.parseUnicodeClass` (`\pN`, `\p{Name}`, `\PN`, `\p{^Name}`); `pos` is at the
/// backslash. `Ok(None)` when `s[pos..]` is not a Unicode class.
fn parse_unicode_class(s: &str, pos: usize) -> Result<Option<usize>, GoSyntaxError> {
    let b = s.as_bytes();
    if b.len() - pos < 2 || b[pos] != b'\\' || (b[pos + 1] != b'p' && b[pos + 1] != b'P') {
        return Ok(None);
    }
    let mut t = pos + 2;
    let (seq, name) = match s[t..].chars().next() {
        Some('{') => {
            let Some(end) = s[pos..].find('}') else {
                return Err(err(ERR_INVALID_CHAR_RANGE, &s[pos..]));
            };
            t = pos + end + 1;
            (&s[pos..t], &s[pos + 3..pos + end])
        }
        Some(c) => {
            t += c.len_utf8();
            (&s[pos..t], &s[pos + 2..t])
        }
        // Go `nextRune("")` consumes nothing: the name is empty.
        None => (&s[pos..t], ""),
    };
    let name = name.strip_prefix('^').unwrap_or(name);
    if UNICODE_TABLE_NAMES.binary_search(&name).is_err() {
        return Err(err(ERR_INVALID_CHAR_RANGE, seq));
    }
    Ok(Some(t))
}

/// Go `parser.parseNamedClass` (`[:alnum:]` ...); `pos` is at `[`.
fn parse_named_class(s: &str, pos: usize) -> Result<Option<usize>, GoSyntaxError> {
    let t = &s[pos..];
    if !t.starts_with("[:") {
        return Ok(None);
    }
    let Some(i) = t[2..].find(":]") else {
        return Ok(None);
    };
    let end = i + 4;
    let name = &t[..end];
    if !POSIX_GROUPS.contains(&name) {
        return Err(err(ERR_INVALID_CHAR_RANGE, name));
    }
    Ok(Some(pos + end))
}

/// Go `parser.parseClassChar`; `class_start` is the offset of the class' `[`.
fn parse_class_char(s: &str, pos: usize, class_start: usize) -> Result<(i64, usize), GoSyntaxError> {
    match s[pos..].chars().next() {
        None => Err(err(ERR_MISSING_BRACKET, &s[class_start..])),
        Some('\\') => parse_escape(s, pos),
        Some(c) => Ok((i64::from(u32::from(c)), pos + c.len_utf8())),
    }
}

/// Go `parser.parseClass`; `pos` is at `[`. Returns the offset after the closing `]`
/// while writing an equivalent flat Rust `regex` character class to `out`.
fn parse_class_into(s: &str, pos: usize, out: &mut String) -> Result<usize, GoSyntaxError> {
    let b = s.as_bytes();
    let mut t = pos + 1;
    out.push('[');
    if t < b.len() && b[t] == b'^' {
        out.push('^');
        t += 1;
    }
    // `]` and `-` are okay as first char in class (Perl mode: `-` anywhere).
    let mut first = true;
    while t >= b.len() || b[t] != b']' || first {
        first = false;
        if b.len() - t > 2 && b[t] == b'[' && b[t + 1] == b':' {
            if let Some(nt) = parse_named_class(s, t)? {
                out.push_str(&s[t..nt]);
                t = nt;
                continue;
            }
        }
        if let Some(nt) = parse_unicode_class(s, t)? {
            out.push_str(&s[t..nt]);
            t = nt;
            continue;
        }
        if let Some(nt) = parse_perl_class_escape(s, t) {
            match &s[t..nt] {
                r"\d" => out.push_str("0-9"),
                r"\D" => out.push_str(r"\x{0}-\x{2F}\x{3A}-\x{10FFFF}"),
                r"\s" => out.push_str(r"\t\n\f\r "),
                r"\S" => out.push_str(r"\x{0}-\x{08}\x{0E}-\x{1F}\x{21}-\x{10FFFF}"),
                r"\w" => out.push_str("0-9A-Z_a-z"),
                r"\W" => out.push_str(r"\x{0}-\x{2F}\x{3A}-\x{40}\x{5B}-\x{5E}\x{60}\x{7B}-\x{10FFFF}"),
                other => out.push_str(other),
            }
            t = nt;
            continue;
        }
        let rng = t;
        let (lo, nt) = parse_class_char(s, t, pos)?;
        t = nt;
        // `[a-]` means `a|-`, so check for the final `]`.
        if b.len() - t >= 2 && b[t] == b'-' && b[t + 1] != b']' {
            let (hi, nt) = parse_class_char(s, t + 1, pos)?;
            t = nt;
            if hi < lo {
                return Err(err(ERR_INVALID_CHAR_RANGE, &s[rng..t]));
            }
            push_class_rune(out, lo);
            out.push('-');
            push_class_rune(out, hi);
        } else {
            push_class_rune(out, lo);
        }
    }
    out.push(']');
    Ok(t + 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unicode_table_names_are_sorted() {
        assert!(UNICODE_TABLE_NAMES.windows(2).all(|w| w[0] < w[1]));
    }

    fn msg(s: &str) -> String {
        check(s).map_or_else(|e| format!("{}: `{}`", e.code, e.expr), |()| "ok".to_string())
    }

    /// Cases from go/src/regexp/syntax/parse_test.go (`invalidRegexps`, `onlyPerl`,
    /// `TestParseInvalidRegexps`) plus the error texts checked against Go below.
    #[test]
    fn go_valid_and_invalid_expressions() {
        for ok in [
            "", "a", "abc", "a|b", "a*", "a+?", "a{2}", "a{2,}", "a{2,5}?", "a{,2}", "x{1001",
            "(a)", "(?:a)", "(?i)a", "(?i:a)", "(?i-m:a)", "(?P<name>a)", "(?<name>a)", "[a-z]",
            "[^a]", "[]a]", "[-a]", "[a-]", "[[:alpha:]]", "[[:^word:]]", r"\d", r"\pL",
            r"\p{Greek}", r"\p{^Greek}", r"\PL", r"\Qa*b\E",
            r"\Qab", r"\x41", r"\x{10FFFF}", r"\101", r"\0", r"\_", r"\.", r"\A\b\B\z", "^*", "$+",
            "(?:)*", "a*(?i)*", "{", "a{", "a{1", "a{1,", "a{1,x}", "a{01}", "(a{2}){500}", "(?s).",
            "[\\d-z]", "\u{e9}+",
        ] {
            assert_eq!(msg(ok), "ok", "{ok:?} must be valid");
        }
        let cases = [
            ("(", "missing closing ): `(`"),
            ("a(", "missing closing ): `a(`"),
            ("(a|b", "missing closing ): `(a|b`"),
            (")", "unexpected ): `)`"),
            ("a)", "unexpected ): `a)`"),
            ("(a))", "unexpected ): `(a))`"),
            ("*", "missing argument to repetition operator: `*`"),
            ("+", "missing argument to repetition operator: `+`"),
            ("?", "missing argument to repetition operator: `?`"),
            ("a|*", "missing argument to repetition operator: `*`"),
            ("(*)", "missing argument to repetition operator: `*`"),
            ("(?i)*", "missing argument to repetition operator: `*`"),
            ("{1}", "missing argument to repetition operator: `{1}`"),
            ("a**", "invalid nested repetition operator: `**`"),
            ("a*+", "invalid nested repetition operator: `*+`"),
            ("a??", "ok"),
            ("a???", "invalid nested repetition operator: `???`"),
            ("a*??", "invalid nested repetition operator: `*??`"),
            ("a{2}*", "invalid nested repetition operator: `{2}*`"),
            ("a{1001}", "invalid repeat count: `{1001}`"),
            ("a{2,1}", "invalid repeat count: `{2,1}`"),
            ("a{100000000}", "invalid repeat count: `{100000000}`"),
            ("(a{500}){3}", "invalid repeat count: `{3}`"),
            ("((a{2}){2}){251}", "invalid repeat count: `{251}`"),
            ("(?P<name", "invalid named capture: `(?P<name`"),
            ("(?P<x y>a)", "invalid named capture: `(?P<x y>`"),
            ("(?P<>a)", "invalid named capture: `(?P<>`"),
            ("(?<>a)", "invalid named capture: `(?<>`"),
            ("(?P=name)", "invalid or unsupported Perl syntax: `(?P`"),
            ("(?=a)", "invalid or unsupported Perl syntax: `(?=`"),
            ("(?!a)", "invalid or unsupported Perl syntax: `(?!`"),
            ("(?<=a)", "invalid named capture: `(?<=a)`"),
            ("(?i-)", "invalid or unsupported Perl syntax: `(?i-)`"),
            ("(?--i)", "invalid or unsupported Perl syntax: `(?--`"),
            ("(?", "invalid or unsupported Perl syntax: `(?`"),
            ("(?i", "invalid or unsupported Perl syntax: `(?i`"),
            ("[a", "missing closing ]: `[a`"),
            ("x[a", "missing closing ]: `[a`"),
            ("[]", "missing closing ]: `[]`"),
            ("[^]", "missing closing ]: `[^]`"),
            ("[z-a]", "invalid character class range: `z-a`"),
            ("[[:foo:]]", "invalid character class range: `[:foo:]`"),
            (r"\p{Foo}", "invalid character class range: `\\p{Foo}`"),
            (r"\pX", "invalid character class range: `\\pX`"),
            (r"\p{Letter}", "invalid character class range: `\\p{Letter}`"),
            (r"\pl", "invalid character class range: `\\pl`"),
            (r"\p{greek}", "invalid character class range: `\\p{greek}`"),
            (r"\p{Greek", "invalid character class range: `\\p{Greek`"),
            (r"\p", "invalid character class range: `\\p`"),
            (r"[\p{Foo}]", "invalid character class range: `\\p{Foo}`"),
            (r"\C", "invalid escape sequence: `\\C`"),
            (r"\Z", "invalid escape sequence: `\\Z`"),
            (r"\1", "invalid escape sequence: `\\1`"),
            (r"\8", "invalid escape sequence: `\\8`"),
            (r"\x", "invalid escape sequence: `\\x`"),
            (r"\xZ1", "invalid escape sequence: `\\xZ1`"),
            (r"\x1", "invalid escape sequence: `\\x1`"),
            (r"\x{", "invalid escape sequence: `\\x{`"),
            (r"\x{}", "invalid escape sequence: `\\x{}`"),
            (r"\x{110000}", "invalid escape sequence: `\\x{110000`"),
            (r"\x{12g}", "invalid escape sequence: `\\x{12g`"),
            (r"[\b]", "invalid escape sequence: `\\b`"),
            ("\\\u{e9}", "invalid escape sequence: `\\\u{e9}`"),
            ("a\\", "trailing backslash at end of expression: ``"),
            ("[a\\", "trailing backslash at end of expression: ``"),
        ];
        for (re, want) in cases {
            assert_eq!(msg(re), want, "{re:?}");
        }
    }
}
