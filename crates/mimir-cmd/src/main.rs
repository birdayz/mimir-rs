//! Entrypoint for the `mimir` binary.
//!
//! Ported from github.com/grafana/mimir/cmd/mimir/main.go

#[cfg(target_os = "linux")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(target_os = "linux")]
#[repr(transparent)]
struct MallocConfPtr(*const std::ffi::c_char);
#[cfg(target_os = "linux")]
unsafe impl Sync for MallocConfPtr {}

#[cfg(target_os = "linux")]
#[allow(non_upper_case_globals)]
#[export_name = "_rjem_malloc_conf"]
static _rjem_malloc_conf_override: MallocConfPtr = MallocConfPtr(
    c"background_thread:true,dirty_decay_ms:250,muzzy_decay_ms:0,percpu_arena:percpu".as_ptr(),
);

#[cfg(target_os = "linux")]
fn purge_jemalloc_arenas() {
    extern "C" {
        fn _rjem_mallctl(
            name: *const std::ffi::c_char,
            oldp: *mut std::ffi::c_void,
            oldlenp: *mut usize,
            newp: *mut std::ffi::c_void,
            newlen: usize,
        ) -> std::ffi::c_int;
    }
    // MALLCTL_ARENAS_ALL == 4096 in jemalloc: purges dirty pages across all arenas immediately.
    // SAFETY: valid NUL-terminated static C string and null optional pointers per jemalloc mallctl contract.
    unsafe {
        let _ = _rjem_mallctl(
            c"arena.4096.purge".as_ptr(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            0,
        );
    }
}

use mimir_cmd::{init_logging, parse_cli_for_run, run_mimir};
use mimir_common::logging::check_fatal;
use mimir_config::CliAction;

fn main() {
    #[cfg(target_os = "linux")]
    mimir_common::register_allocator_purge_hook(purge_jemalloc_arenas);

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

    // util_log.InitLogger: from here on every log line (including all `tracing` events) is
    // rendered in go-kit logfmt/json on stderr, honouring -log.level, -log.format and
    // -log.rate-limit-*.
    init_logging(&outcome);

    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(err) => check_fatal("initializing application", &err.to_string()),
    };

    if let Err(err) = rt.block_on(run_mimir(outcome)) {
        // util_log.CheckFatal("running application", err): logs and exits 1.
        check_fatal("running application", &format!("{err:#}"));
    }
}
