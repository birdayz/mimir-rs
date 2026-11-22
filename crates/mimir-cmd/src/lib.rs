//! Mimir binary entrypoint, module DAG manager, HTTP + gRPC server, and admin endpoints.
//!
//! Ported from:
//! - github.com/grafana/mimir/cmd/mimir/main.go
//! - github.com/grafana/mimir/pkg/mimir/mimir.go
//! - github.com/grafana/mimir/pkg/mimir/modules.go
//! - github.com/grafana/mimir/pkg/api/*
//! - github.com/grafana/dskit/server/*

pub mod activity_tracking;
pub mod api_error;
pub mod app;
pub mod cli;
pub mod dskit_log;
pub mod grpc_call_check;
pub mod protobuf_codec;
pub mod server_service;
pub use mimir_ingester::expfmt;
pub mod grpc;
pub mod http;
pub mod memberlist_status;
pub mod metrics;
pub mod modules;
pub mod pprof;
pub mod process_collector;
pub mod runtime_config_metrics;
pub mod sanity_check;
pub mod server;
pub mod usagestats;

pub use app::{init_logging, run_mimir};
pub use cli::{
    diff_yaml_values, dump_yaml, effective_config_yaml, parse_cli_for_run, run_cli_test_mode,
    CliOutcome,
};
