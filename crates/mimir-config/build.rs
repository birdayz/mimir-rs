//! Captures the compiler version for the `go version:` line of `-version` (and the
//! `goversion` build-info label), the Rust equivalent of Go's `runtime.Version()`.

use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=RUSTC");
    let rustc = std::env::var("RUSTC").unwrap_or_else(|_| "rustc".to_string());
    // `rustc 1.86.0 (05f9846f8 2025-03-31)` -> `rustc1.86.0` (mirrors Go's `go1.25.3`).
    let version = Command::new(rustc)
        .arg("--version")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .and_then(|s| s.split_whitespace().nth(1).map(|v| format!("rustc{v}")))
        .unwrap_or_else(|| "rustc-unknown".to_string());
    println!("cargo:rustc-env=MIMIR_RUSTC_VERSION={version}");
}
