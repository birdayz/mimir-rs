//! Entrypoint for the `mimir` binary.
//!
//! Ported from github.com/grafana/mimir/cmd/mimir/main.go

#[cfg(target_os = "linux")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use mimir_cmd::{parse_cli_for_run, run_mimir};
use mimir_config::CliAction;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let prog_name = args.first().map(String::as_str).unwrap_or("mimir");
    let rest = if args.len() > 1 { &args[1..] } else { &[] };

    let outcome = parse_cli_for_run(prog_name, rest);
    if !outcome.stdout.is_empty() {
        print!("{}", outcome.stdout);
    }
    if !outcome.stderr.is_empty() {
        eprint!("{}", outcome.stderr);
    }
    if outcome.exit_code != 0 {
        std::process::exit(outcome.exit_code);
    }

    if !matches!(outcome.action, Some(CliAction::Run)) {
        return;
    }

    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(err) => {
            eprintln!("error initializing tokio runtime: {err}");
            std::process::exit(1);
        }
    };

    if let Err(err) = rt.block_on(run_mimir(outcome)) {
        eprintln!("error running mimir: {err:#}");
        std::process::exit(1);
    }
}
