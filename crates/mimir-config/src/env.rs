//! Environment variable expansion for configuration files.
//!
//! Ported from github.com/grafana/mimir/cmd/mimir/main.go (`expandEnvironmentVariables`)
//! and Go stdlib `os.Expand`.

/// Expands `${VAR}`, `$VAR`, and `${VAR:default}` in `config` using process environment variables.
/// Strips `\n` from substituted values, matching `expandEnvironmentVariables` in `cmd/mimir/main.go`.
pub fn expand_environment_variables(config: &[u8]) -> Vec<u8> {
    expand_environment_variables_with(config, |key| std::env::var(key).ok())
}

/// Expands `${VAR}`, `$VAR`, and `${VAR:default}` using the supplied lookup function.
pub fn expand_environment_variables_with<F>(config: &[u8], getenv: F) -> Vec<u8>
where
    F: Fn(&str) -> Option<String>,
{
    let s = String::from_utf8_lossy(config);
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    let mut j = 0;

    while i < bytes.len() {
        if bytes[i] == b'$' && i + 1 < bytes.len() {
            out.push_str(&s[j..i]);
            let (name, w) = get_shell_name(&s[i + 1..]);
            if name.is_empty() && w > 0 {
                // Bad syntax such as `${}`, eaten just like Go os.Expand.
            } else if name.is_empty() {
                // Bare `$` not followed by valid shell name (e.g. `$ `).
                out.push('$');
            } else {
                let expanded = expand_key(name, &getenv);
                out.push_str(&expanded);
            }
            i += 1 + w;
            j = i;
        } else {
            i += 1;
        }
    }

    if j == 0 {
        return config.to_vec();
    }
    out.push_str(&s[j..]);
    out.into_bytes()
}

fn expand_key<F>(key: &str, getenv: &F) -> String
where
    F: Fn(&str) -> Option<String>,
{
    let (var_name, default_val) = match key.split_once(':') {
        Some((k, d)) => (k, Some(d)),
        None => (key, None),
    };

    let mut v = getenv(var_name).unwrap_or_default();
    if v.is_empty() {
        if let Some(d) = default_val {
            v = d.to_string();
        }
    }

    if v.contains('\n') {
        v.replace('\n', "")
    } else {
        v
    }
}

/// Ported from Go stdlib `os.getShellName`.
fn get_shell_name(s: &str) -> (&str, usize) {
    let bytes = s.as_bytes();
    if bytes.is_empty() {
        return ("", 0);
    }

    if bytes[0] == b'{' {
        if bytes.len() > 2 && is_shell_special_var(bytes[1]) && bytes[2] == b'}' {
            return (&s[1..2], 3);
        }
        for i in 1..bytes.len() {
            if bytes[i] == b'}' {
                if i == 1 {
                    return ("", 2);
                }
                return (&s[1..i], i + 1);
            }
        }
        return ("", 1);
    }

    if is_shell_special_var(bytes[0]) {
        return (&s[0..1], 1);
    }

    let mut i = 0;
    while i < bytes.len() && is_alpha_num(bytes[i]) {
        i += 1;
    }
    (&s[..i], i)
}

fn is_shell_special_var(c: u8) -> bool {
    matches!(c, b'*' | b'#' | b'$' | b'@' | b'!' | b'?' | b'-' | b'0'..=b'9')
}

fn is_alpha_num(c: u8) -> bool {
    c == b'_' || c.is_ascii_alphanumeric()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn expands_vars_defaults_and_strips_newlines() {
        let mut map = HashMap::new();
        map.insert("HOST", "example.com\n");
        map.insert("EMPTY", "");
        map.insert("PORT", "9095");
        let lookup = |k: &str| map.get(k).map(|v| (*v).to_string());

        let input = b"addr: ${HOST}:${PORT}\nmissing: $MISSING\ndef: ${EMPTY:fallback}\ndef2: ${UNSET:a:b}\nbare: $ \n";
        let out = expand_environment_variables_with(input, lookup);
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "addr: example.com:9095\nmissing: \ndef: fallback\ndef2: a:b\nbare: $ \n"
        );
    }
}
