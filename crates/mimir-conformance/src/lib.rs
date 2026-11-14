//! Differential conformance and benchmark harness against upstream Go Mimir.
//!
//! Provides Tier 1 (`go-oracle`), Tier 2 (`go-mimir` twin-process), and Tier 3
//! (mixed Go+Rust cluster & Kafka ingest) verification helpers.

pub mod oracle;
