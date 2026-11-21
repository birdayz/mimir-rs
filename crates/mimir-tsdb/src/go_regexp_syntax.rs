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
//! is a single allocation-free pass (one `Vec` push per open group).
//!
//! `checkLimits` (`ErrLarge`, `ErrNestingDepth`) is modelled through the compiled size and
//! height of every node, behind the same gate as Go: sizes only count once
//! `numRegexp >= maxSize / repeats` (see [`Limits`]), so literals and alternations of any length
//! stay valid until the repetition counts multiply up. It is an approximation where Go's result
//! depends on the exact shape of its parse tree:
//! - `numRegexp` is under-approximated (only repetition and capture nodes are counted), so an
//!   expression Go rejects with `ErrLarge` because it allocated thousands of nodes next to a
//!   large repetition can pass here;
//! - alternations are sized and measured as written, not after `parser.factor`: once sizes are
//!   tracked, a multi-megabyte alternation whose alternatives share prefixes can be `ErrLarge`
//!   here and valid in Go, and a long chain of alternatives that are prefixes of each other can
//!   be `ErrNestingDepth` in Go and valid here;
//! - `numRunes` of `\p{...}` classes is estimated.
//!
//! Not modelled: `ErrInvalidUTF8` (Rust strings are always valid UTF-8), and the errors of the
//! second parse `labels.NewFastRegexMatcher` runs on `"^(?s:" + parsed.String() + ")$"` (one
//! more level of nesting, reported with that rewritten expression).

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
const ERR_NESTING_DEPTH: &str = "expression nests too deeply";
const ERR_LARGE: &str = "expression too large";

/// Go `maxRepeat` / the `n` of `repeatIsValid(re, 1000)`.
const MAX_REPEAT: u64 = 1000;

/// Go `syntax/parse.go` `maxHeight`, `maxSize`, `maxRunes` (`checkLimits`).
const MAX_HEIGHT: u32 = 1000;
const MAX_SIZE: u64 = (128 << 20) / 40; // 3_355_443
const MAX_RUNES: u64 = (128 << 20) / 4; // 33_554_432

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

/// The Go `Regexp` ops `parser.collapse` flattens into a parent of the same op.
#[derive(Clone, Copy, PartialEq, Eq)]
enum NodeOp {
    Other,
    Concat,
    Alternate,
}

/// What Go `checkLimits` looks at in a parsed node.
#[derive(Clone, Copy)]
struct Node {
    /// Go `calcSize`.
    size: u64,
    /// Go `calcHeight`.
    height: u32,
    /// The count Go `checkSize` multiplies `parser.repeats` by every time this node goes through
    /// `checkLimits`: `n` for an `OpRepeat` node (`x{min,max}`), 0 otherwise.
    repeat_n: u64,
    op: NodeOp,
}

impl Node {
    /// A leaf (`^`, `$`, `.`, a character class, ...), or the empty expression.
    const LEAF: Node = Node {
        size: 1,
        height: 1,
        repeat_n: 0,
        op: NodeOp::Other,
    };

    /// Size and height this node contributes to a `parent` concatenation or alternation:
    /// `parser.collapse` splices the children of a node of the same op into the parent.
    #[inline]
    fn as_sub(self, parent: NodeOp) -> (u64, u32) {
        if self.op == parent {
            (self.size, self.height.saturating_sub(1))
        } else {
            (self.size, self.height)
        }
    }
}

#[derive(Clone, Copy)]
enum ItemKind {
    /// A run of literal runes Go merges into one `OpLiteral` node (`maybeConcat`).
    ///
    /// `cached` is the stale size Go keeps for that node once it tracks sizes (`parser.size`):
    /// the node is sized when its first rune is pushed (or when tracking starts), and the runes
    /// merged into it afterwards do not update the cached value. 0 means not cached: the size
    /// is `len`.
    Literal { len: u64, fold: bool, cached: u64 },
    Other(Node),
}

impl ItemKind {
    /// The node as Go sizes it when it is pushed (again) on its own: a forced `calcSize`.
    #[inline]
    fn node(self) -> Node {
        match self {
            ItemKind::Literal { len, .. } => Node {
                size: len.max(1),
                ..Node::LEAF
            },
            ItemKind::Other(node) => Node {
                size: node.size.max(1),
                ..node
            },
        }
    }

    /// Size and height contributed to a multi-item concatenation (cached sizes).
    #[inline]
    fn as_concat_sub(self) -> (u64, u32) {
        match self {
            ItemKind::Literal { len, cached, .. } => {
                (if cached != 0 { cached } else { len.max(1) }, 1)
            }
            ItemKind::Other(node) => node.as_sub(NodeOp::Concat),
        }
    }
}

/// Go `parser.numRunes` plus the state of the `checkSize` gate.
///
/// Go only starts computing compiled sizes (`parser.size != nil`) once
/// `numRegexp >= maxSize / repeats`, where `repeats` is the product of the counts of every
/// `OpRepeat` node that went through `checkLimits` — including each time such a node is pushed
/// again as the only item of a concatenation, the only alternative of a group, or the content of
/// a non-capturing group. Before that, no expression is ever `ErrLarge` because of its size:
/// multi-megabyte literals and alternations parse fine as long as their repetition counts are
/// small.
///
/// `numRegexp` (the number of `Regexp` nodes Go allocated, free-list reuse excluded) is not
/// reproduced exactly; [`Limits::num_regexp_lower_bound`] under-approximates it, so this check
/// starts tracking no earlier than Go does and never rejects an expression through a gate Go
/// keeps closed.
#[derive(Default)]
struct Limits {
    num_runes: u64,
    /// Go `parser.repeats` (0 until the first `checkSize`).
    repeats: u64,
    /// Go `parser.size != nil`.
    tracking: bool,
    /// Repetition and capture nodes currently alive in the parse stack.
    live_ops: u64,
    /// Maximum of `live_ops` so far (`numRegexp` never decreases).
    peak_ops: u64,
}

impl Limits {
    /// Every repetition or capture node alive at the same time is a distinct allocation, and so
    /// is at least one more node (the innermost operand, or the node being checked).
    #[inline]
    fn num_regexp_lower_bound(&self) -> u64 {
        self.peak_ops.saturating_add(1)
    }

    #[inline]
    fn add_op(&mut self) {
        self.live_ops = self.live_ops.saturating_add(1);
        self.peak_ops = self.peak_ops.max(self.live_ops);
    }

    /// Go `parser.checkLimits(re)`.
    fn check(&mut self, s: &str, node: Node) -> Result<(), GoSyntaxError> {
        let Node { size, height, repeat_n, .. } = node;
        if self.num_runes > MAX_RUNES {
            return Err(err(ERR_LARGE, s));
        }
        if !self.tracking {
            if self.repeats == 0 {
                self.repeats = 1;
            }
            if repeat_n > 0 {
                if repeat_n > MAX_SIZE / self.repeats {
                    self.repeats = MAX_SIZE;
                } else {
                    self.repeats *= repeat_n;
                }
            }
            if self.num_regexp_lower_bound() >= MAX_SIZE / self.repeats {
                self.tracking = true;
            }
        }
        if self.tracking && size > MAX_SIZE {
            return Err(err(ERR_LARGE, s));
        }
        // Go skips the height check until `numRegexp >= maxHeight`, which a tree higher than
        // `maxHeight` always satisfies.
        if height > MAX_HEIGHT {
            return Err(err(ERR_NESTING_DEPTH, s));
        }
        Ok(())
    }
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
    /// Whether this frame corresponds to a capturing group (`OpCapture`).
    is_capturing: bool,
    /// Topmost AST item in the current concatenation branch.
    last_item: Option<ItemKind>,
    /// Concatenation items flushed prior to `last_item`.
    concat_count: u32,
    concat_sum_size: u64,
    concat_max_height: u32,
    /// Completed alternation branches prior to the current one.
    alt_count: u32,
    alt_sum_size: u64,
    alt_max_height: u32,
    /// Repetition and capture nodes alive inside this group ([`Limits::live_ops`]).
    ops: u64,
}

impl Frame {
    /// Moves the topmost item below a new one: it becomes one more item of a concatenation.
    fn flush_item(&mut self, prev_item: ItemKind) {
        let (psz, ph) = prev_item.as_concat_sub();
        self.concat_count = self.concat_count.saturating_add(1);
        self.concat_sum_size = self.concat_sum_size.saturating_add(psz);
        self.concat_max_height = self.concat_max_height.max(ph);
    }

    fn push_other(
        &mut self,
        s: &str,
        threshold: u64,
        node: Node,
        lim: &mut Limits,
        rune_delta: u64,
    ) -> Result<(), GoSyntaxError> {
        lim.num_runes = lim.num_runes.saturating_add(rune_delta);
        if let Some(prev) = self.last.replace(threshold) {
            self.rest = self.rest.max(prev);
        }
        if let Some(prev_item) = self.last_item.replace(ItemKind::Other(node)) {
            self.flush_item(prev_item);
        }
        lim.check(s, node)
    }

    /// Go `parser.literal` for `add_len` runes: each one is pushed (and checked) as a one-rune
    /// `OpLiteral` and then merged into the literal below it.
    fn push_literal_run(
        &mut self,
        s: &str,
        add_len: u64,
        fold: bool,
        lim: &mut Limits,
    ) -> Result<(), GoSyntaxError> {
        lim.num_runes = lim.num_runes.saturating_add(add_len);
        let was_tracking = lim.tracking;
        if let Some(ItemKind::Literal { len, fold: prev_fold, cached }) = self.last_item {
            if prev_fold == fold {
                self.last = Some(0);
                let res = lim.check(s, Node::LEAF);
                // Tracking started on this push: Go sizes the run as it is on the stack.
                let cached = if cached == 0 && !was_tracking && lim.tracking {
                    len
                } else {
                    cached
                };
                self.last_item = Some(ItemKind::Literal {
                    len: len.saturating_add(add_len),
                    fold,
                    cached,
                });
                return res;
            }
        }
        if let Some(prev) = self.last.replace(0) {
            self.rest = self.rest.max(prev);
        }
        if let Some(prev_item) = self.last_item.take() {
            self.flush_item(prev_item);
        }
        let res = lim.check(s, Node::LEAF);
        self.last_item = Some(ItemKind::Literal {
            len: add_len,
            fold,
            cached: u64::from(lim.tracking),
        });
        res
    }

    /// Go `parser.concat`: collapses the items of the current branch and pushes the result,
    /// which is the item itself (pushed, and checked, once more) when there is only one.
    fn finalize_branch(&mut self, s: &str, lim: &mut Limits) -> Result<Node, GoSyntaxError> {
        let total = self.concat_count + u32::from(self.last_item.is_some());
        let node = match (total, self.last_item) {
            (0, _) | (_, None) => Node::LEAF,
            (1, Some(item)) => item.node(),
            (_, Some(item)) => {
                let (lsz, lh) = item.as_concat_sub();
                Node {
                    size: self.concat_sum_size.saturating_add(lsz).max(1),
                    height: self.concat_max_height.max(lh).saturating_add(1),
                    repeat_n: 0,
                    op: NodeOp::Concat,
                }
            }
        };
        self.last_item = None;
        self.concat_count = 0;
        self.concat_sum_size = 0;
        self.concat_max_height = 0;
        lim.check(s, node)?;
        Ok(node)
    }

    fn push_alt(&mut self, s: &str, lim: &mut Limits) -> Result<(), GoSyntaxError> {
        let (bsz, bh) = self.finalize_branch(s, lim)?.as_sub(NodeOp::Alternate);
        self.alt_count = self.alt_count.saturating_add(1);
        self.alt_sum_size = self.alt_sum_size.saturating_add(bsz);
        self.alt_max_height = self.alt_max_height.max(bh);
        self.rest = self.close_threshold();
        self.last = None;
        Ok(())
    }

    /// Go `parser.concat` + `parser.alternate` at a `)` or at the end of the expression, plus the
    /// `OpCapture` node of a capturing group. Returns the group's threshold and node; the
    /// caller pushes it onto the parent frame.
    fn close_group(&mut self, s: &str, lim: &mut Limits) -> Result<(u64, Node), GoSyntaxError> {
        let threshold = self.close_threshold();
        let branch = self.finalize_branch(s, lim)?;
        let inner = if self.alt_count > 0 {
            let (bsz, bh) = branch.as_sub(NodeOp::Alternate);
            let total_alts = self.alt_count.saturating_add(1);
            let alt = Node {
                size: self
                    .alt_sum_size
                    .saturating_add(bsz)
                    .saturating_add(u64::from(total_alts - 1))
                    .max(1),
                height: self.alt_max_height.max(bh).saturating_add(1),
                repeat_n: 0,
                op: NodeOp::Alternate,
            };
            // `collapse(subs, OpAlternate)` factors common prefixes and may free any node of
            // the alternatives: none of them counts as alive any more.
            lim.live_ops = lim.live_ops.saturating_sub(self.ops);
            self.ops = 0;
            lim.check(s, alt)?;
            alt
        } else {
            // `collapse` of a single alternative returns it as is, and `alternate` pushes (and
            // checks) it again.
            lim.check(s, branch)?;
            branch
        };
        if self.is_capturing {
            let capture = Node {
                size: inner.size.saturating_add(2),
                height: inner.height.saturating_add(1),
                repeat_n: 0,
                op: NodeOp::Other,
            };
            self.ops = self.ops.saturating_add(1);
            lim.add_op();
            lim.check(s, capture)?;
            Ok((threshold, capture))
        } else {
            Ok((threshold, inner))
        }
    }

    /// Threshold of the whole group (`concat` + `alternate`).
    fn close_threshold(&self) -> u64 {
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
    let mut lim = Limits::default();
    // Start offset of the previous repetition operator (Go `lastRepeat`).
    let mut last_repeat: Option<usize> = None;
    let mut pos = 0;
    while pos < len {
        let mut repeat: Option<usize> = None;
        match b[pos] {
            b'(' => {
                if len - pos >= 2 && b[pos + 1] == b'?' {
                    let next = parse_perl_flags(s, pos, &mut frames, &mut out)?;
                    if top(&mut frames).fold_case {
                        has_case_insensitive = true;
                    }
                    pos = next;
                } else {
                    let parent_fold = top(&mut frames).fold_case;
                    frames.push(Frame {
                        fold_case: parent_fold,
                        is_capturing: true,
                        ..Frame::default()
                    });
                    out.push('(');
                    pos += 1;
                }
            }
            b'|' => {
                top(&mut frames).push_alt(s, &mut lim)?;
                out.push('|');
                pos += 1;
            }
            b')' => {
                if frames.len() < 2 {
                    return Err(err(ERR_UNEXPECTED_PAREN, s));
                }
                let mut popped = frames.pop().unwrap_or_default();
                let (threshold, group) = popped.close_group(s, &mut lim)?;
                // Go `parseRightParen` pushes the group content (or its `OpCapture`) back.
                let parent = top(&mut frames);
                parent.ops = parent.ops.saturating_add(popped.ops);
                parent.push_other(s, threshold, group, &mut lim, 0)?;
                out.push(')');
                pos += 1;
            }
            b'^' | b'$' | b'.' => {
                top(&mut frames).push_other(s, 0, Node::LEAF, &mut lim, 0)?;
                out.push(b[pos] as char);
                pos += 1;
            }
            b'[' => {
                let fold_case = top(&mut frames).fold_case;
                if fold_case {
                    has_case_insensitive = true;
                }
                let (next_pos, class_runes) = parse_class_into(s, pos, fold_case, &mut out)?;
                pos = next_pos;
                top(&mut frames).push_other(s, 0, Node::LEAF, &mut lim, class_runes)?;
            }
            b'*' | b'+' | b'?' => {
                let before = pos;
                let op = match b[pos] {
                    b'*' => RepeatKind::Star,
                    b'+' => RepeatKind::Plus,
                    _ => RepeatKind::Quest,
                };
                pos = apply_repeat(
                    s,
                    &mut frames,
                    op,
                    before,
                    pos + 1,
                    last_repeat,
                    &mut lim,
                )?;
                out.push_str(&s[before..pos]);
                repeat = Some(before);
            }
            b'{' => {
                let before = pos;
                match parse_repeat(b, pos) {
                    None => {
                        // Not a repetition: `{` is a literal in Go (e.g. `a{01}`, `a{,2}`, `x{1001`).
                        let fold = top(&mut frames).fold_case;
                        top(&mut frames).push_literal_run(s, 1, fold, &mut lim)?;
                        out.push_str(r"\{");
                        pos += 1;
                    }
                    Some((min, max, after)) => {
                        if !(0..=1000).contains(&min) || max > 1000 || (max >= 0 && min > max) {
                            return Err(err(ERR_INVALID_REPEAT_SIZE, &s[before..after]));
                        }
                        pos = apply_repeat(
                            s,
                            &mut frames,
                            RepeatKind::Bounded(min, max),
                            before,
                            after,
                            last_repeat,
                            &mut lim,
                        )?;
                        out.push_str(&s[before..pos]);
                        repeat = Some(before);
                    }
                }
            }
            b'\\' => {
                let fold = top(&mut frames).fold_case;
                let (next, used_lit) =
                    parse_top_level_escape_into(s, pos, &mut frames, &mut out, &mut lim)?;
                if fold && used_lit {
                    has_case_insensitive = true;
                }
                pos = next;
            }
            _ => {
                let clen = next_char_len(s, pos);
                let ch_str = &s[pos..pos + clen];
                let fold = top(&mut frames).fold_case;
                if fold && ch_str.chars().any(char::is_alphabetic) {
                    has_case_insensitive = true;
                }
                out.push_str(ch_str);
                pos += clen;
                top(&mut frames).push_literal_run(s, 1, fold, &mut lim)?;
            }
        }
        last_repeat = repeat;
    }
    if frames.len() != 1 {
        return Err(err(ERR_MISSING_PAREN, s));
    }
    let mut root = frames.pop().unwrap_or_default();
    root.close_group(s, &mut lim)?;
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

#[derive(Clone, Copy)]
enum RepeatKind {
    Star,
    Plus,
    Quest,
    Bounded(i64, i64),
}

/// Go `parser.repeat`.
fn apply_repeat(
    s: &str,
    frames: &mut [Frame],
    kind: RepeatKind,
    before: usize,
    mut after: usize,
    last_repeat: Option<usize>,
    lim: &mut Limits,
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
    // The operator applies to the last rune of a literal run only: the runes before it stay
    // behind as an item of their own.
    if let Some(ItemKind::Literal { len, fold, cached }) = cur.last_item {
        if len > 1 {
            cur.flush_item(ItemKind::Literal {
                len: len - 1,
                fold,
                cached: cached.min(len - 1),
            });
        }
    }
    let (sub_sz, sub_h) = match cur.last_item {
        Some(ItemKind::Other(node)) => (node.size.max(1), node.height),
        Some(ItemKind::Literal { .. }) | None => (1, 1),
    };
    let (new_sz, new_h, threshold) = match kind {
        RepeatKind::Star => (sub_sz.saturating_add(2), sub_h.saturating_add(1), sub),
        RepeatKind::Plus | RepeatKind::Quest => {
            (sub_sz.saturating_add(1), sub_h.saturating_add(1), sub)
        }
        RepeatKind::Bounded(min, max) => {
            let new_sz = if max < 0 {
                if min == 0 {
                    sub_sz.saturating_add(2)
                } else {
                    1u64.saturating_add((min as u64).saturating_mul(sub_sz))
                }
            } else {
                (max as u64)
                    .saturating_mul(sub_sz)
                    .saturating_add((max - min) as u64)
                    .max(1)
            };
            let new_h = sub_h.saturating_add(1);
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
            (new_sz, new_h, threshold)
        }
    };
    // Go `checkSize`: `n := re.Max; if n == -1 { n = re.Min }; if n <= 0 { n = 1 }` (OpRepeat only).
    let repeat_n = match kind {
        RepeatKind::Bounded(min, max) => (if max < 0 { min } else { max }).max(1) as u64,
        RepeatKind::Star | RepeatKind::Plus | RepeatKind::Quest => 0,
    };
    cur.ops = cur.ops.saturating_add(1);
    lim.add_op();
    let node = Node {
        size: new_sz,
        height: new_h,
        repeat_n,
        op: NodeOp::Other,
    };
    lim.check(s, node)?;
    if let RepeatKind::Bounded(min, max) = kind {
        if (min >= 2 || max >= 2) && threshold > MAX_REPEAT {
            return Err(err(ERR_INVALID_REPEAT_SIZE, &s[before..after]));
        }
    }
    cur.last_item = Some(ItemKind::Other(node));
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
fn parse_perl_flags(
    s: &str,
    pos: usize,
    frames: &mut Vec<Frame>,
    out: &mut String,
) -> Result<usize, GoSyntaxError> {
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
            is_capturing: true,
            ..Frame::default()
        });
        // FastRegexMatcher never inspects capture names or numbers; emit a non-capturing
        // group so Go-valid digit-leading or duplicate capture names compile in Rust `regex`.
        out.push_str("(?:");
        return Ok(pos + end + 1);
    }

    let mut i = pos + 2;
    let mut negated = false;
    let mut saw_flag = false;
    let mut pos_mask: u8 = 0;
    let mut neg_mask: u8 = 0;
    let mut next_fold = top(frames).fold_case;
    while let Some(c) = s[i..].chars().next() {
        i += c.len_utf8();
        match c {
            'i' | 'm' | 's' | 'U' => {
                saw_flag = true;
                let bit = match c {
                    'i' => 1,
                    'm' => 2,
                    's' => 4,
                    'U' => 8,
                    _ => unreachable!(),
                };
                if negated {
                    neg_mask |= bit;
                    pos_mask &= !bit;
                } else {
                    pos_mask |= bit;
                    neg_mask &= !bit;
                }
                if c == 'i' {
                    next_fold = !negated;
                }
            }
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
                    frames.push(Frame {
                        fold_case: next_fold,
                        ..Frame::default()
                    });
                } else {
                    top(frames).fold_case = next_fold;
                }
                // Normalize duplicate/toggled flags (e.g. `(?ii)`, `(?i-i)`, `(?)`) so Rust
                // `regex` accepts every flag group Go `regexp/syntax` accepts.
                if pos_mask != 0 || neg_mask != 0 || c == ':' {
                    out.push_str("(?");
                    for (bit, ch) in [(1, 'i'), (2, 'm'), (4, 's'), (8, 'U')] {
                        if (pos_mask & bit) != 0 {
                            out.push(ch);
                        }
                    }
                    if neg_mask != 0 {
                        out.push('-');
                        for (bit, ch) in [(1, 'i'), (2, 'm'), (4, 's'), (8, 'U')] {
                            if (neg_mask & bit) != 0 {
                                out.push(ch);
                            }
                        }
                    }
                    out.push(c);
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
    if (0xD800..=0xDFFF).contains(&r) {
        // Surrogates cannot appear in valid UTF-8 strings.
        out.push_str(r"[^\x{0}-\x{10FFFF}]");
        return;
    }
    if let Some(c) = u32::try_from(r).ok().and_then(char::from_u32) {
        if (0x20..=0x7E).contains(&r) {
            push_outside_char(out, c);
            return;
        }
    }
    use std::fmt::Write;
    let _ = write!(out, "\\x{{{r:X}}}");
}

fn push_class_range_no_surrogates(out: &mut String, lo: i64, hi: i64) -> bool {
    if lo >= 0xD800 && hi <= 0xDFFF {
        return false;
    }
    if lo == hi {
        push_class_rune(out, lo);
        return true;
    }
    let mut emitted = false;
    if lo < 0xD800 {
        let r_hi = hi.min(0xD7FF);
        if lo == r_hi {
            push_class_rune(out, lo);
        } else {
            push_class_rune(out, lo);
            out.push('-');
            push_class_rune(out, r_hi);
        }
        emitted = true;
    }
    if hi > 0xDFFF {
        let r_lo = lo.max(0xE000);
        if r_lo == hi {
            push_class_rune(out, hi);
        } else {
            push_class_rune(out, r_lo);
            out.push('-');
            push_class_rune(out, hi);
        }
        emitted = true;
    }
    emitted
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
fn unicode_class_rune_estimate(name: &str) -> u64 {
    match name {
        "L" | "LC" | "Lo" | "Lu" | "Ll" => 1292,
        _ => 32,
    }
}

fn parse_top_level_escape_into(
    s: &str,
    pos: usize,
    frames: &mut [Frame],
    out: &mut String,
    lim: &mut Limits,
) -> Result<(usize, bool), GoSyntaxError> {
    let b = s.as_bytes();
    if b.len() - pos >= 2 {
        match b[pos + 1] {
            b'A' | b'z' => {
                top(frames).push_other(s, 0, Node::LEAF, lim, 0)?;
                out.push('\\');
                out.push(b[pos + 1] as char);
                return Ok((pos + 2, false));
            }
            b'b' => {
                top(frames).push_other(s, 0, Node::LEAF, lim, 0)?;
                // Go prog.go:IsWordChar is strictly ASCII [0-9A-Za-z_].
                out.push_str(r"(?-u:\b)");
                return Ok((pos + 2, false));
            }
            b'B' => {
                top(frames).push_other(s, 0, Node::LEAF, lim, 0)?;
                out.push_str(r"(?-u:\B)");
                return Ok((pos + 2, false));
            }
            // Any byte: not supported.
            b'C' => return Err(err(ERR_INVALID_ESCAPE, &s[pos..pos + 2])),
            b'Q' => {
                // `\Q ... \E`: the text is literal (up to the end without `\E`).
                // Empty `\Q\E` pushes nothing onto the parse stack in Go parse.go.
                let rest = &s[pos + 2..];
                let (lit, end) = match rest.find(r"\E") {
                    Some(i) => (&rest[..i], pos + 2 + i + 2),
                    None => (rest, s.len()),
                };
                if !lit.is_empty() {
                    let fold = top(frames).fold_case;
                    let cnt = lit.chars().count() as u64;
                    top(frames).push_literal_run(s, cnt, fold, lim)?;
                    for c in lit.chars() {
                        push_outside_char(out, c);
                    }
                    return Ok((end, true));
                }
                return Ok((end, false));
            }
            _ => {}
        }
    }
    if let Some((end, negated, name)) = parse_unicode_class(s, pos)? {
        let est = unicode_class_rune_estimate(name);
        top(frames).push_other(s, 0, Node::LEAF, lim, est)?;
        push_unicode_class_outside(out, &s[pos..end], negated, name);
        return Ok((end, true));
    }
    if let Some(end) = parse_perl_class_escape(s, pos) {
        top(frames).push_other(s, 0, Node::LEAF, lim, 6)?;
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
    let fold = top(frames).fold_case;
    top(frames).push_literal_run(s, 1, fold, lim)?;
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
fn parse_unicode_class(s: &str, pos: usize) -> Result<Option<(usize, bool, &str)>, GoSyntaxError> {
    let b = s.as_bytes();
    if b.len() - pos < 2 || b[pos] != b'\\' || (b[pos + 1] != b'p' && b[pos + 1] != b'P') {
        return Ok(None);
    }
    let mut t = pos + 2;
    let (seq, raw_name) = match s[t..].chars().next() {
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
    let (has_caret, name) = match raw_name.strip_prefix('^') {
        Some(rest) => (true, rest),
        None => (false, raw_name),
    };
    if UNICODE_TABLE_NAMES.binary_search(&name).is_err() {
        return Err(err(ERR_INVALID_CHAR_RANGE, seq));
    }
    let negated = (b[pos + 1] == b'P') ^ has_caret;
    Ok(Some((t, negated, name)))
}

fn unicode_17_script_ranges(name: &str) -> Option<&'static str> {
    match name {
        "Beria_Erfe" => Some(r"\x{16EA0}-\x{16EB8}\x{16EBB}-\x{16ED3}"),
        "Sidetic" => Some(r"\x{10940}-\x{10959}"),
        "Tai_Yo" => Some(r"\x{1E6C0}-\x{1E6DE}\x{1E6E0}-\x{1E6F5}\x{1E6FE}-\x{1E6FF}"),
        "Tolong_Siki" => Some(r"\x{11DB0}-\x{11DDB}\x{11DE0}-\x{11DE9}"),
        _ => None,
    }
}

fn push_unicode_class_outside(out: &mut String, orig: &str, negated: bool, name: &str) {
    if name == "Cs" {
        if negated {
            out.push_str(r"[\x{0}-\x{10FFFF}]");
        } else {
            out.push_str(r"[^\x{0}-\x{10FFFF}]");
        }
        return;
    }
    if let Some(ranges) = unicode_17_script_ranges(name) {
        out.push('[');
        if negated {
            out.push('^');
        }
        out.push_str(ranges);
        out.push(']');
        return;
    }
    out.push_str(orig);
}

fn push_unicode_class_in_class(out: &mut String, orig: &str, negated: bool, name: &str) -> bool {
    if name == "Cs" {
        if negated {
            out.push_str(r"\x{0}-\x{10FFFF}");
            return true;
        }
        return false;
    }
    if let Some(ranges) = unicode_17_script_ranges(name) {
        if negated {
            out.push('[');
            out.push('^');
            out.push_str(ranges);
            out.push(']');
        } else {
            out.push_str(ranges);
        }
        return true;
    }
    out.push_str(orig);
    true
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
fn parse_class_into(
    s: &str,
    pos: usize,
    fold_case: bool,
    out: &mut String,
) -> Result<(usize, u64), GoSyntaxError> {
    let b = s.as_bytes();
    let mut t = pos + 1;
    let class_start = out.len();
    out.push('[');
    let mut negated = false;
    if t < b.len() && b[t] == b'^' {
        negated = true;
        out.push('^');
        t += 1;
    }
    let mut any_item = false;
    let mut class_runes: u64 = 0;
    // `]` and `-` are okay as first char in class (Perl mode: `-` anywhere).
    let mut first = true;
    while t >= b.len() || b[t] != b']' || first {
        first = false;
        if b.len() - t > 2 && b[t] == b'[' && b[t + 1] == b':' {
            if let Some(nt) = parse_named_class(s, t)? {
                let name = &s[t..nt];
                class_runes = class_runes.saturating_add(16);
                if fold_case {
                    // Under (?i), Go folds POSIX groups before negating (so U+017F 'ſ' and U+212A 'K'
                    // are excluded from negated classes that exclude 's'/'k'), whereas Rust regex folds
                    // positive ranges before matching/negating. Exclude 0x017F & 0x212A here.
                    match name {
                        "[:^alnum:]" => out.push_str(r"\x{0}-\x{2F}\x{3A}-\x{40}\x{5B}-\x{60}\x{7B}-\x{17E}\x{180}-\x{2129}\x{212B}-\x{10FFFF}"),
                        "[:^alpha:]" => out.push_str(r"\x{0}-\x{40}\x{5B}-\x{60}\x{7B}-\x{17E}\x{180}-\x{2129}\x{212B}-\x{10FFFF}"),
                        "[:^ascii:]" => out.push_str(r"\x{80}-\x{17E}\x{180}-\x{2129}\x{212B}-\x{10FFFF}"),
                        "[:^graph:]" => out.push_str(r"\x{0}-\x{20}\x{7F}-\x{17E}\x{180}-\x{2129}\x{212B}-\x{10FFFF}"),
                        "[:^lower:]" | "[:^upper:]" => out.push_str(r"\x{0}-\x{40}\x{5B}-\x{60}\x{7B}-\x{17E}\x{180}-\x{2129}\x{212B}-\x{10FFFF}"),
                        "[:^print:]" => out.push_str(r"\x{0}-\x{1F}\x{7F}-\x{17E}\x{180}-\x{2129}\x{212B}-\x{10FFFF}"),
                        "[:^word:]" => out.push_str(r"\x{0}-\x{2F}\x{3A}-\x{40}\x{5B}-\x{5E}\x{60}\x{7B}-\x{17E}\x{180}-\x{2129}\x{212B}-\x{10FFFF}"),
                        _ => out.push_str(name),
                    }
                } else {
                    out.push_str(name);
                }
                any_item = true;
                t = nt;
                continue;
            }
        }
        if let Some((nt, u_neg, u_name)) = parse_unicode_class(s, t)? {
            class_runes = class_runes.saturating_add(unicode_class_rune_estimate(u_name));
            if push_unicode_class_in_class(out, &s[t..nt], u_neg, u_name) {
                any_item = true;
            }
            t = nt;
            continue;
        }
        if let Some(nt) = parse_perl_class_escape(s, t) {
            class_runes = class_runes.saturating_add(6);
            match &s[t..nt] {
                r"\d" => out.push_str("0-9"),
                r"\D" => out.push_str(r"\x{0}-\x{2F}\x{3A}-\x{10FFFF}"),
                r"\s" => out.push_str(r"\t\n\f\r "),
                r"\S" => out.push_str(r"\x{0}-\x{08}\x{0B}\x{0E}-\x{1F}\x{21}-\x{10FFFF}"),
                r"\w" => out.push_str("0-9A-Z_a-z"),
                r"\W" => {
                    if fold_case {
                        // Exclude U+017F (ſ) and U+212A (K) so (?i)[\W] / (?i)[^\W] never
                        // fold them onto ASCII s/S/k/K (matching Go appendGroup).
                        out.push_str(r"\x{0}-\x{2F}\x{3A}-\x{40}\x{5B}-\x{5E}\x{60}\x{7B}-\x{17E}\x{180}-\x{2129}\x{212B}-\x{10FFFF}");
                    } else {
                        out.push_str(r"\x{0}-\x{2F}\x{3A}-\x{40}\x{5B}-\x{5E}\x{60}\x{7B}-\x{10FFFF}");
                    }
                }
                other => out.push_str(other),
            }
            any_item = true;
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
            class_runes = class_runes.saturating_add(2);
            if push_class_range_no_surrogates(out, lo, hi) {
                any_item = true;
            }
        } else {
            class_runes = class_runes.saturating_add(2);
            if push_class_range_no_surrogates(out, lo, lo) {
                any_item = true;
            }
        }
    }
    if !any_item {
        out.truncate(class_start);
        if negated {
            out.push_str(r"[\x{0}-\x{10FFFF}]");
        } else {
            out.push_str(r"[^\x{0}-\x{10FFFF}]");
        }
    } else {
        out.push(']');
    }
    Ok((t + 1, class_runes))
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

    #[test]
    fn all_unicode_table_names_compile_in_rust_regex() {
        for name in UNICODE_TABLE_NAMES {
            for pat in [
                format!(r"\p{{{name}}}"),
                format!(r"\P{{{name}}}"),
                format!(r"[\p{{{name}}}]"),
                format!(r"[\P{{{name}}}]"),
            ] {
                let tr = translate_to_rust(&pat).unwrap_or_else(|e| panic!("{pat}: {e}"));
                let anchored = format!(r"\A(?s:{})\z", tr.pattern);
                assert!(
                    regex::Regex::new(&anchored).is_ok(),
                    "pattern {pat:?} translated to {anchored:?} failed regex::Regex::new"
                );
            }
        }
    }
}
