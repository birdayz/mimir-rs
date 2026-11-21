//! Startup sanity check module (`sanity-check`): directory read/write access and object storage
//! connectivity. In Go the `Server` module depends on `SanityCheck`, so nothing is served (and
//! no other module starts) until it passes; if it fails the process exits with an error.
//!
//! Ported from github.com/grafana/mimir/pkg/mimir/sanity_check.go

use std::path::{Path, PathBuf};
use std::time::Duration;

use mimir_common::backoff::{Backoff, BackoffConfig};
use mimir_common::logging::{LogLevel, Logger};
use mimir_config::{BucketConfig, MimirConfig};
use mimir_storage::bucket::BucketClient;

/// `errObjectStorage`.
pub const ERR_OBJECT_STORAGE: &str = "unable to successfully send a request to object storage";

/// `compartments.ReadCompartmentIDPlaceholder`.
const READ_COMPARTMENT_ID_PLACEHOLDER: &str = "<read-compartment-id>";

const MAX_RETRIES: usize = 20;
const OBJECT_STORE_CHECK_TIMEOUT: Duration = Duration::from_secs(10);

/// `runSanityCheck`.
pub async fn run_sanity_check(cfg: &MimirConfig, logger: &dyn Logger) -> Result<(), String> {
    logger.log(LogLevel::Info, &[("msg", "Checking directories read/write access")]);
    if let Err(err) = check_directories_read_write_access(cfg, dir_exists, is_dir_read_writable) {
        logger.log(
            LogLevel::Error,
            &[("msg", "Unable to access directory"), ("err", &err)],
        );
        return Err(err);
    }
    logger.log(
        LogLevel::Info,
        &[("msg", "Directories read/write access successfully checked")],
    );

    logger.log(LogLevel::Info, &[("msg", "Checking object storage config")]);
    if let Err(err) = check_object_stores_config_with_retries(cfg, logger).await {
        logger.log(
            LogLevel::Error,
            &[
                ("msg", "Unable to successfully connect to configured object storage"),
                ("err", &err),
            ],
        );
        return Err(err);
    }
    logger.log(
        LogLevel::Info,
        &[("msg", "Object storage config successfully checked")],
    );
    Ok(())
}

/// `fs.DirExists`.
pub fn dir_exists(dir: &Path) -> Result<bool, String> {
    match std::fs::metadata(dir) {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(go_path_error("stat", dir, &e)),
    }
}

/// `fs.IsDirReadWritable`.
pub fn is_dir_read_writable(dir: &Path) -> Result<(), String> {
    let f = dir.join(".check");
    std::fs::write(&f, b"").map_err(|e| go_path_error("open", &f, &e))?;
    let res = std::fs::read(&f).map(|_| ()).map_err(|e| go_path_error("open", &f, &e));
    let _ = std::fs::remove_file(&f);
    res
}

/// Formats an I/O error like Go's `*fs.PathError` (`open /path: permission denied`).
fn go_path_error(op: &str, path: &Path, err: &std::io::Error) -> String {
    let reason = match err.kind() {
        std::io::ErrorKind::PermissionDenied => "permission denied".to_string(),
        std::io::ErrorKind::NotFound => "no such file or directory".to_string(),
        std::io::ErrorKind::ReadOnlyFilesystem => "read-only file system".to_string(),
        _ => {
            // Strip Rust's " (os error N)" suffix.
            let s = err.to_string();
            match s.find(" (os error") {
                Some(i) => s[..i].to_lowercase(),
                None => s,
            }
        }
    };
    format!("{op} {}: {reason}", path.display())
}

/// `checkDirectoriesReadWriteAccess` (only the components implemented by mimir-rs can be
/// enabled, i.e. the ingester).
pub fn check_directories_read_write_access(
    cfg: &MimirConfig,
    dir_exists_fn: impl Fn(&Path) -> Result<bool, String>,
    is_dir_read_writable_fn: impl Fn(&Path) -> Result<(), String>,
) -> Result<(), String> {
    let mut errs = Vec::new();
    if cfg.is_ingester_enabled() {
        if let Err(e) = check_dir_read_write_access(
            &cfg.blocks_storage.tsdb.dir,
            &dir_exists_fn,
            &is_dir_read_writable_fn,
        ) {
            errs.push(format!("ingester: {e}"));
        }
    }
    multi_error(errs)
}

/// `checkDirReadWriteAccess`: walks up to the closest existing ancestor and checks it.
pub fn check_dir_read_write_access(
    dir: &str,
    dir_exists_fn: impl Fn(&Path) -> Result<bool, String>,
    is_dir_read_writable_fn: impl Fn(&Path) -> Result<(), String>,
) -> Result<(), String> {
    let mut d = PathBuf::from(dir);
    loop {
        if dir_exists_fn(&d)? {
            break;
        }
        let next = go_filepath_dir(&d);
        if next == d {
            break; // Root path reached.
        }
        d = next;
    }
    is_dir_read_writable_fn(&d).map_err(|e| format!("failed to access directory {dir}: {e}"))
}

/// Go's `filepath.Dir`: the parent of a relative single-element path is `.`, and the parent of
/// `.` / `/` is itself.
fn go_filepath_dir(p: &Path) -> PathBuf {
    match p.parent() {
        Some(parent) if parent.as_os_str().is_empty() => PathBuf::from("."),
        Some(parent) => parent.to_path_buf(),
        None => p.to_path_buf(),
    }
}

/// `dskit/multierror.MultiError.Err()` formatting.
fn multi_error(errs: Vec<String>) -> Result<(), String> {
    match errs.len() {
        0 => Ok(()),
        1 => Err(errs.into_iter().next().unwrap_or_default()),
        _ => Err(errs.join("; ")),
    }
}

/// `checkObjectStoresConfigWithRetries`.
async fn check_object_stores_config_with_retries(
    cfg: &MimirConfig,
    logger: &dyn Logger,
) -> Result<(), String> {
    let mut backoff = Backoff::new(BackoffConfig {
        min_backoff: Duration::from_secs(1),
        max_backoff: Duration::from_secs(5),
        max_retries: MAX_RETRIES,
    });

    let mut last_err = String::new();
    while backoff.ongoing() {
        match check_object_stores_config(cfg).await {
            Ok(()) => return Ok(()),
            Err(err) => {
                if backoff.num_retries() < MAX_RETRIES - 1 {
                    logger.log(
                        LogLevel::Warn,
                        &[
                            (
                                "msg",
                                "Unable to successfully connect to configured object storage (will retry)",
                            ),
                            ("err", &err),
                        ],
                    );
                } else {
                    logger.log(
                        LogLevel::Warn,
                        &[
                            ("msg", "Unable to successfully connect to configured object storage"),
                            ("err", &err),
                        ],
                    );
                }
                last_err = err;
            }
        }
        backoff.wait().await;
    }
    Err(last_err)
}

/// `checkObjectStoresConfig` (blocks storage is used by the ingester, the only implemented
/// component).
async fn check_object_stores_config(cfg: &MimirConfig) -> Result<(), String> {
    let mut errs = Vec::new();
    if cfg.is_ingester_enabled() {
        if let Err(e) = check_blocks_storage_config(cfg).await {
            errs.push(format!("blocks storage: {e}"));
        }
    }
    multi_error(errs)
}

/// `checkBlocksStorageConfig`.
async fn check_blocks_storage_config(cfg: &MimirConfig) -> Result<(), String> {
    let bucket_cfg = &cfg.blocks_storage.bucket;
    if !cfg.compartments.enabled || !bucket_cfg.bucket_name().ends_with(READ_COMPARTMENT_ID_PLACEHOLDER)
    {
        return check_object_store_config(bucket_cfg).await;
    }
    let mut errs = Vec::new();
    for c in 0..cfg.compartments.read.num_compartments {
        if let Err(e) = check_object_store_config(&read_compartment_config(bucket_cfg, c)).await {
            errs.push(format!("read compartment {c}: {e}"));
        }
    }
    multi_error(errs)
}

/// `bucket.Config.ReadCompartmentConfig`.
fn read_compartment_config(cfg: &BucketConfig, read_compartment_id: i64) -> BucketConfig {
    let mut cfg = cfg.clone();
    let id = read_compartment_id.to_string();
    let b = &mut cfg.backend_cfg;
    match b.backend.as_str() {
        "s3" => b.s3.bucket_name = b.s3.bucket_name.replace(READ_COMPARTMENT_ID_PLACEHOLDER, &id),
        "gcs" => b.gcs.bucket_name = b.gcs.bucket_name.replace(READ_COMPARTMENT_ID_PLACEHOLDER, &id),
        "azure" => {
            b.azure.container_name = b.azure.container_name.replace(READ_COMPARTMENT_ID_PLACEHOLDER, &id)
        }
        "swift" => {
            b.swift.container_name = b.swift.container_name.replace(READ_COMPARTMENT_ID_PLACEHOLDER, &id)
        }
        "filesystem" => {
            b.filesystem.dir = b.filesystem.dir.replace(READ_COMPARTMENT_ID_PLACEHOLDER, &id)
        }
        _ => {}
    }
    cfg
}

/// `checkObjectStoreConfig`: reads an object we don't expect to exist, just to check that the
/// object storage replies with the expected "not found" error.
pub async fn check_object_store_config(cfg: &BucketConfig) -> Result<(), String> {
    check_object_store_config_with_timeout(cfg, OBJECT_STORE_CHECK_TIMEOUT).await
}

async fn check_object_store_config_with_timeout(
    cfg: &BucketConfig,
    timeout_dur: Duration,
) -> Result<(), String> {
    let cfg = cfg.clone();
    tokio::time::timeout(timeout_dur, async move {
        let client = tokio::task::spawn_blocking(move || {
            BucketClient::new_client(&cfg, "sanity-check", None)
        })
        .await
        .map_err(|e| format!("{ERR_OBJECT_STORAGE}: {e}"))?
        .map_err(|e| format!("{ERR_OBJECT_STORAGE}: {e}"))?;
        match client.download("sanity-check-at-startup").await {
            Ok(_) => Ok(()),
            Err(e) if e.is_not_found() => Ok(()),
            Err(e) => Err(format!("{ERR_OBJECT_STORAGE}: {e}")),
        }
    })
    .await
    .map_err(|_| format!("{ERR_OBJECT_STORAGE}: context deadline exceeded"))?
}

#[cfg(test)]
mod tests {
    use super::*;
    use mimir_config::StringSliceCSV;

    fn ingester_cfg(dir: &str) -> MimirConfig {
        let mut cfg = MimirConfig::default();
        cfg.target = StringSliceCSV(vec!["ingester".to_string()]);
        cfg.blocks_storage.tsdb.dir = dir.to_string();
        cfg
    }

    // Upstream: pkg/mimir/sanity_check_test.go::TestCheckDirectoryReadWriteAccess (ingester).
    #[test]
    fn test_check_directory_read_write_access() {
        const CONFIGURED: &str = "/path/to/dir";
        type ExistsFn = Box<dyn Fn(&Path) -> Result<bool, String>>;
        type RwFn = Box<dyn Fn(&Path) -> Result<(), String>>;
        let cases: Vec<(&str, ExistsFn, RwFn, &str)> = vec![
            (
                "should fail on directory without write access",
                Box::new(|_| Ok(true)),
                Box::new(|_| Err("read only".to_string())),
                "failed to access directory /path/to/dir: read only",
            ),
            (
                "should pass on directory with read-write access",
                Box::new(|_| Ok(true)),
                Box::new(|_| Ok(())),
                "",
            ),
            (
                "should pass if directory doesn't exist but parent existing folder has read-write access",
                Box::new(|d| Ok(d == Path::new("/"))),
                Box::new(|d| {
                    if d == Path::new("/") {
                        Ok(())
                    } else {
                        Err("not exists".to_string())
                    }
                }),
                "",
            ),
            (
                "should fail if directory doesn't exist and parent existing folder has no read-write access",
                Box::new(|d| Ok(d == Path::new("/"))),
                Box::new(|d| {
                    if d == Path::new("/") {
                        Err("read only".to_string())
                    } else {
                        Err("not exists".to_string())
                    }
                }),
                "failed to access directory /path/to/dir: read only",
            ),
        ];
        for (name, exists, rw, expected) in cases {
            let cfg = ingester_cfg(CONFIGURED);
            let res = check_directories_read_write_access(&cfg, exists, rw);
            if expected.is_empty() {
                assert!(res.is_ok(), "{name}: {res:?}");
            } else {
                let err = res.expect_err(name);
                assert!(err.contains(expected), "{name}: {err}");
                assert!(err.starts_with("ingester: "), "{name}: {err}");
            }
        }
    }

    #[test]
    fn real_directory_checks() {
        let tmp = tempfile::tempdir().unwrap();
        let missing = tmp.path().join("a/b/c");
        let cfg = ingester_cfg(missing.to_str().unwrap());
        check_directories_read_write_access(&cfg, dir_exists, is_dir_read_writable).unwrap();
        assert!(!tmp.path().join(".check").exists());
    }

    // Upstream: TestCheckObjectStoresConfig "should succeed with filesystem backend and
    // non-existent directory".
    #[tokio::test]
    async fn filesystem_backend_with_missing_object_succeeds() {
        let tmp = tempfile::tempdir().unwrap();
        let mut cfg = ingester_cfg(tmp.path().join("tsdb").to_str().unwrap());
        cfg.blocks_storage.bucket.backend_cfg.backend = "filesystem".to_string();
        cfg.blocks_storage.bucket.backend_cfg.filesystem.dir =
            tmp.path().join("blocks").to_string_lossy().into_owned();
        check_object_stores_config(&cfg).await.unwrap();
    }

    #[tokio::test]
    async fn unreachable_s3_fails_with_err_object_storage() {
        let tmp = tempfile::tempdir().unwrap();
        let mut cfg = ingester_cfg(tmp.path().join("tsdb").to_str().unwrap());
        let b = &mut cfg.blocks_storage.bucket.backend_cfg;
        b.backend = "s3".to_string();
        b.s3.endpoint = "127.0.0.1:1".to_string();
        b.s3.bucket_name = "blocks".to_string();
        b.s3.access_key_id = "x".to_string();
        b.s3.secret_access_key = "y".to_string();
        b.s3.insecure = true;
        let err = check_object_stores_config(&cfg).await.unwrap_err();
        assert!(
            err.starts_with(&format!("blocks storage: {ERR_OBJECT_STORAGE}: ")),
            "{err}"
        );
    }

    #[test]
    fn read_compartment_placeholder_is_replaced() {
        let mut cfg = BucketConfig::default();
        cfg.backend_cfg.backend = "s3".to_string();
        cfg.backend_cfg.s3.bucket_name = "blocks-<read-compartment-id>".to_string();
        assert_eq!(read_compartment_config(&cfg, 3).backend_cfg.s3.bucket_name, "blocks-3");
    }

    #[tokio::test]
    async fn w23_csc_06_sanity_check_timeout_preempts_slow_store_constructor() {
        // Bind a TCP listener that accepts and holds the connection silently so an Azure/S3
        // HTTP client stalls on connect/probe, and verify check_object_store_config_with_timeout
        // returns context deadline exceeded within the configured budget without blocking Tokio.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _accept_task = tokio::spawn(async move {
            let mut conns = Vec::new();
            while let Ok((stream, _)) = listener.accept().await {
                conns.push(stream);
            }
        });

        let mut cfg = BucketConfig::default();
        cfg.backend_cfg.backend = "s3".to_string();
        cfg.backend_cfg.s3.endpoint = addr.to_string();
        cfg.backend_cfg.s3.bucket_name = "blocks".to_string();
        cfg.backend_cfg.s3.access_key_id = "x".to_string();
        cfg.backend_cfg.s3.secret_access_key = "y".to_string();
        cfg.backend_cfg.s3.insecure = true;

        let started = std::time::Instant::now();
        let err = check_object_store_config_with_timeout(&cfg, Duration::from_millis(80))
            .await
            .unwrap_err();
        assert!(
            err.contains("context deadline exceeded"),
            "expected deadline exceeded, got: {err}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "timeout took {:?}",
            started.elapsed()
        );
    }
}
