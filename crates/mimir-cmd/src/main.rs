//! Entrypoint for the `mimir` binary.
//!
//! Ported from github.com/grafana/mimir/cmd/mimir/main.go

#[cfg(target_os = "linux")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() {
    // Module initialization and server startup will be wired in Wave 3 (mimir-cmd).
    eprintln!("mimir-rs: binary entrypoint initialized (targets wired in Wave 3)");
    std::process::exit(1);
}
