//! Mimir binary entrypoint, module DAG manager, HTTP + gRPC server, and admin endpoints.
//!
//! Ported from:
//! - github.com/grafana/mimir/cmd/mimir/main.go
//! - github.com/grafana/mimir/pkg/mimir/mimir.go
//! - github.com/grafana/mimir/pkg/mimir/modules.go
//! - github.com/grafana/mimir/pkg/api/*
//! - github.com/grafana/dskit/server/*

pub mod app;
pub mod cli;
pub mod expfmt;
pub mod grpc;
pub mod http;
pub mod metrics;
pub mod process_collector;

pub use app::{init_logging, run_mimir};
pub use cli::{
    diff_yaml_values, dump_yaml, effective_config_yaml, parse_cli_for_run, run_cli_test_mode,
    CliOutcome,
};
