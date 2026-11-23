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
    c"background_thread:true,dirty_decay_ms:250,muzzy_decay_ms:0,narenas:8".as_ptr(),
);

#[cfg(target_os = "linux")]
extern "C" {
    fn _rjem_mallctl(
        name: *const std::ffi::c_char,
        oldp: *mut std::ffi::c_void,
        oldlenp: *mut usize,
        newp: *mut std::ffi::c_void,
        newlen: usize,
    ) -> std::ffi::c_int;
}

#[cfg(target_os = "linux")]
fn purge_jemalloc_arenas() {
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

#[cfg(target_os = "linux")]
fn read_jemalloc_stats() -> Option<mimir_common::JemallocStats> {
    fn read_usize(mib: &std::ffi::CStr) -> Option<usize> {
        let mut val: usize = 0;
        let mut sz: usize = std::mem::size_of::<usize>();
        // SAFETY: valid NUL-terminated C string and properly aligned/sized out-pointer per jemalloc mallctl contract.
        let rc = unsafe {
            _rjem_mallctl(
                mib.as_ptr(),
                (&mut val as *mut usize).cast::<std::ffi::c_void>(),
                &mut sz,
                std::ptr::null_mut(),
                0,
            )
        };
        (rc == 0).then_some(val)
    }

    // Advance jemalloc's cached statistics epoch before reading stats.* values.
    let mut epoch: u64 = 1;
    let mut epoch_sz: usize = std::mem::size_of::<u64>();
    // SAFETY: `epoch` is a RW uint64_t mallctl; passing non-zero `newp` refreshes stats.
    let rc = unsafe {
        _rjem_mallctl(
            c"epoch".as_ptr(),
            (&mut epoch as *mut u64).cast::<std::ffi::c_void>(),
            &mut epoch_sz,
            (&mut epoch as *mut u64).cast::<std::ffi::c_void>(),
            std::mem::size_of::<u64>(),
        )
    };
    if rc != 0 {
        return None;
    }

    Some(mimir_common::JemallocStats {
        allocated: read_usize(c"stats.allocated")?,
        active: read_usize(c"stats.active")?,
        resident: read_usize(c"stats.resident")?,
        mapped: read_usize(c"stats.mapped")?,
        retained: read_usize(c"stats.retained")?,
        metadata: read_usize(c"stats.metadata")?,
    })
}

use mimir_cmd::{init_logging, parse_cli_for_run, run_mimir};
use mimir_common::logging::check_fatal;
use mimir_config::CliAction;

fn main() {
    #[cfg(target_os = "linux")]
    {
        mimir_common::register_allocator_purge_hook(purge_jemalloc_arenas);
        mimir_common::register_jemalloc_stats_hook(read_jemalloc_stats);
    }

    let args: Vec<String> = std::env::args().collect();
    let prog_name = args.first().map(String::as_str).unwrap_or("mimir");
    let rest = if args.len() > 1 { &args[1..] } else { &[] };

    let outcome = parse_cli_for_run(prog_name, rest);
    if outcome.exit_code != 0
        || matches!(
            outcome.action,
            Some(CliAction::PrintHelp { .. } | CliAction::PrintVersion)
        )
    {
        if !outcome.stdout.is_empty() {
            print!("{}", outcome.stdout);
        }
        if !outcome.stderr.is_empty() {
            eprint!("{}", outcome.stderr);
        }
        if outcome.exit_code != 0 {
            std::process::exit(outcome.exit_code);
        }
        return;
    }

    // util_log.InitLogger: from here on every log line (including all `tracing` events) is
    // rendered in go-kit logfmt/json on stderr, honouring -log.level, -log.format and
    // -log.rate-limit-*.
    init_logging(&outcome);

    // Go `mimir.New` rejects empty `-target` before `-print.config` or `-modules` via
    // `util_log.CheckFatal("initializing application", err)`.
    if outcome
        .config
        .as_ref()
        .is_some_and(|cfg| cfg.target.0.is_empty())
    {
        check_fatal(
            "initializing application",
            "no target module specified, the -target flag must be set to a valid module (e.g. \"all\")",
        );
    }

    if !outcome.stdout.is_empty() {
        print!("{}", outcome.stdout);
    }
    if !outcome.stderr.is_empty() {
        eprint!("{}", outcome.stderr);
    }
    if !matches!(outcome.action, Some(CliAction::Run)) {
        return;
    }

    let main_workers = std::env::var("TOKIO_WORKER_THREADS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(4, |n| n.get().clamp(2, 16)));

    let rt = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(main_workers)
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
