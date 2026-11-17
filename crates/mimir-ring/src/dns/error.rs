//! Error type of the DNS provider/resolver.
//!
//! Ported from:
//! - github.com/grafana/dskit/dns/resolver.go (error messages)
//! - github.com/grafana/dskit/multierror/multierror.go (`nonNilMultiError.Error`)
//! - strconv.Quote from the Go standard library (`%q` formatting)

use std::fmt;
use std::sync::Arc;

/// Errors returned by [`super::Resolver`] and [`super::Provider`]. The
/// `Display` output matches the Go error strings produced by dskit.
#[derive(Debug, Clone)]
pub enum DnsError {
    /// `errors.Errorf("missing port in address given for dns lookup: %v", name)`
    MissingPort { name: String },
    /// `errors.Errorf("invalid lookup scheme %q", qtype)`
    InvalidLookupScheme { qtype: String },
    /// `errors.Wrapf(err, "lookup IP addresses %q", host)`
    LookupIpAddresses {
        host: String,
        source: Arc<dyn std::error::Error + Send + Sync + 'static>,
    },
    /// `errors.Wrapf(err, "lookup SRV records %q", host)`
    LookupSrvRecords {
        host: String,
        source: Arc<dyn std::error::Error + Send + Sync + 'static>,
    },
    /// dskit `multierror.MultiError` (as returned by `MultiError.Err()`).
    /// Never empty when returned by this module.
    Multi(Vec<DnsError>),
    /// Any other error (custom resolvers, setup errors). Displayed verbatim.
    Other(String),
}

impl DnsError {
    /// Builds a dskit-style multierror from `errs`, flattening nested
    /// multierrors like `MultiError.Add`. Returns `None` for an empty list
    /// (`MultiError.Err()` returning nil).
    pub fn multi(errs: impl IntoIterator<Item = DnsError>) -> Option<DnsError> {
        let mut out = Vec::new();
        for e in errs {
            match e {
                DnsError::Multi(inner) => out.extend(inner),
                other => out.push(other),
            }
        }
        if out.is_empty() {
            None
        } else {
            Some(DnsError::Multi(out))
        }
    }
}

impl fmt::Display for DnsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DnsError::MissingPort { name } => {
                write!(f, "missing port in address given for dns lookup: {name}")
            }
            DnsError::InvalidLookupScheme { qtype } => {
                write!(f, "invalid lookup scheme {}", go_quote(qtype))
            }
            DnsError::LookupIpAddresses { host, source } => {
                write!(f, "lookup IP addresses {}: {source}", go_quote(host))
            }
            DnsError::LookupSrvRecords { host, source } => {
                write!(f, "lookup SRV records {}: {source}", go_quote(host))
            }
            DnsError::Multi(errs) => {
                if errs.len() > 1 {
                    write!(f, "{} errors: ", errs.len())?;
                }
                for (i, e) in errs.iter().enumerate() {
                    if i != 0 {
                        f.write_str("; ")?;
                    }
                    write!(f, "{e}")?;
                }
                Ok(())
            }
            DnsError::Other(msg) => f.write_str(msg),
        }
    }
}

impl std::error::Error for DnsError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            DnsError::LookupIpAddresses { source, .. }
            | DnsError::LookupSrvRecords { source, .. } => {
                Some(source.as_ref() as &(dyn std::error::Error + 'static))
            }
            _ => None,
        }
    }
}

/// Port of Go's `strconv.Quote` (used by `%q`).
pub fn go_quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{7}' => out.push_str("\\a"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{b}' => out.push_str("\\v"),
            c if is_go_print(c) => out.push(c),
            c if (c as u32) < 0x20 || c as u32 == 0x7f => {
                out.push_str(&format!("\\x{:02x}", c as u32));
            }
            c if (c as u32) < 0x10000 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push_str(&format!("\\U{:08x}", c as u32)),
        }
    }
    out.push('"');
    out
}

/// Approximation of Go's `strconv.IsPrint` (exact for ASCII).
fn is_go_print(c: char) -> bool {
    if c.is_ascii() {
        return (' '..='~').contains(&c);
    }
    !c.is_control() && !c.is_whitespace() && c != '\u{ad}'
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn go_quote_matches_strconv_quote() {
        assert_eq!(go_quote("invalid"), "\"invalid\"");
        assert_eq!(go_quote("a\"b\\c"), "\"a\\\"b\\\\c\"");
        assert_eq!(go_quote("a\nb\x00"), "\"a\\nb\\x00\"");
        assert_eq!(go_quote("héllo"), "\"héllo\"");
    }

    #[test]
    fn multierror_formatting_matches_dskit() {
        let one = DnsError::multi([DnsError::Other("a".into())]).unwrap();
        assert_eq!(one.to_string(), "a");
        let two = DnsError::multi([
            DnsError::Other("a".into()),
            DnsError::multi([DnsError::Other("b".into()), DnsError::Other("c".into())]).unwrap(),
        ])
        .unwrap();
        assert_eq!(two.to_string(), "3 errors: a; b; c");
        assert!(DnsError::multi(Vec::new()).is_none());
    }
}
