//! Differential conformance and benchmark harness against upstream Go Mimir.
//!
//! Provides Tier 1 (`go-oracle`), Tier 2 (`go-mimir` twin-process), and Tier 3
//! (mixed Go+Rust cluster & Kafka ingest) verification helpers.

pub mod oracle;
pub mod twin;

/// Allocates a free TCP listener port from `15000..32000` — strictly below Linux's default
/// ephemeral range (`32768..60999`) so outgoing client connections spawned by child Go/Rust
/// processes cannot steal a port between probe and server bind.
pub fn free_test_port() -> u16 {
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicU32, Ordering};

    const LOW: u32 = 15_000;
    const SPAN: u32 = 17_000;
    static NEXT: AtomicU32 = AtomicU32::new(0);

    let offset = (std::process::id().wrapping_mul(997)) % SPAN;
    for _ in 0..SPAN {
        let port = (LOW + (offset + NEXT.fetch_add(1, Ordering::Relaxed)) % SPAN) as u16;
        let free = TcpListener::bind(("127.0.0.1", port)).is_ok()
            && TcpListener::bind(("0.0.0.0", port)).is_ok()
            && TcpListener::bind(("::", port)).is_ok();
        if free {
            return port;
        }
    }
    panic!("no free TCP ports available in {LOW}..{}", LOW + SPAN);
}
