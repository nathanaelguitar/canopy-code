//! Best-effort private diagnostics for Rust panics and Node ACP children.
//!
//! Reports written by the Rust panic hook contain a timestamp, process ID, and
//! symbolized backtrace. Panic payloads and the explicit panic location are
//! omitted because they can contain user supplied text or paths. No environment
//! variables, network data, or credentials are collected.

use std::backtrace::Backtrace;
use std::collections::BTreeMap;
use std::env;
use std::ffi::{OsStr, OsString};
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

pub const FATAL_REPORT_DIR_ENV: &str = "CANOPY_FATAL_REPORT_DIR";
pub const DISABLE_FATAL_REPORTS_ENV: &str = "CANOPY_DISABLE_FATAL_REPORTS";

static REPORT_DIRECTORY: OnceLock<Mutex<Option<PathBuf>>> = OnceLock::new();
static PANIC_HOOK_INSTALLED: OnceLock<()> = OnceLock::new();
static REPORT_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Optional inputs corresponding to the Node implementation's injectable
/// environment and home-directory settings.
#[derive(Clone, Debug, Default)]
pub struct FatalDiagnosticReportOptions {
    /// When omitted, reads the current process environment.
    pub env: Option<BTreeMap<OsString, OsString>>,
    /// When omitted, uses the platform's home-directory environment setting.
    pub home_directory: Option<PathBuf>,
    /// When omitted, uses the process current directory for relative paths.
    pub current_directory: Option<PathBuf>,
}

fn env_value(options: &FatalDiagnosticReportOptions, key: &str) -> Option<OsString> {
    match &options.env {
        Some(values) => values.get(OsStr::new(key)).cloned(),
        None => env::var_os(key),
    }
}

fn home_directory() -> io::Result<PathBuf> {
    #[cfg(windows)]
    {
        if let Some(home) = env::var_os("USERPROFILE") {
            return Ok(PathBuf::from(home));
        }
        if let (Some(drive), Some(path)) = (env::var_os("HOMEDRIVE"), env::var_os("HOMEPATH")) {
            let mut home = PathBuf::from(drive);
            home.push(path);
            return Ok(home);
        }
    }

    #[cfg(not(windows))]
    if let Some(home) = env::var_os("HOME") {
        return Ok(PathBuf::from(home));
    }

    Err(io::Error::new(
        io::ErrorKind::NotFound,
        "home directory is unavailable",
    ))
}

fn absolute_lexical(path: &Path, current_directory: &Path) -> PathBuf {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        current_directory.join(path)
    };
    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                // `PathBuf::pop` does not remove a platform root.
                normalized.pop();
            }
            other => normalized.push(other.as_os_str()),
        }
    }
    normalized
}

/// Resolve the private report directory using the environment override or the
/// per-user `~/.canopy/fatal-reports` default.
pub fn resolve_fatal_diagnostic_report_directory(
    options: &FatalDiagnosticReportOptions,
) -> io::Result<PathBuf> {
    let cwd = match &options.current_directory {
        Some(cwd) => cwd.clone(),
        None => env::current_dir()?,
    };
    let configured = env_value(options, FATAL_REPORT_DIR_ENV);
    let directory = match configured {
        Some(configured) if !configured.to_string_lossy().trim().is_empty() => {
            PathBuf::from(configured)
        }
        _ => {
            let home = match &options.home_directory {
                Some(home) => home.clone(),
                None => home_directory()?,
            };
            home.join(".canopy").join("fatal-reports")
        }
    };
    Ok(absolute_lexical(&directory, &cwd))
}

/// Return startup flags for a Node child so its fatal reports use the same
/// private directory and exclude environment and network data.
pub fn get_node_fatal_diagnostic_report_exec_args(
    options: &FatalDiagnosticReportOptions,
) -> Vec<OsString> {
    if env_value(options, DISABLE_FATAL_REPORTS_ENV).as_deref() == Some(OsStr::new("1")) {
        return Vec::new();
    }
    let Ok(directory) = resolve_fatal_diagnostic_report_directory(options) else {
        return Vec::new();
    };
    vec![
        OsString::from("--report-on-fatalerror"),
        OsString::from("--report-exclude-env"),
        OsString::from("--report-exclude-network"),
        OsString::from(format!("--report-directory={}", directory.display())),
    ]
}

/// Enable private Rust panic reports, returning the selected directory.
///
/// Directory creation, permission tightening, and hook installation are
/// best-effort so diagnostics never become a startup dependency. The hook
/// delegates to Rust's prior hook after attempting to write its sanitized
/// report.
pub fn configure_fatal_diagnostic_reports(
    options: &FatalDiagnosticReportOptions,
) -> Option<PathBuf> {
    if env_value(options, DISABLE_FATAL_REPORTS_ENV).as_deref() == Some(OsStr::new("1")) {
        return None;
    }
    let directory = resolve_fatal_diagnostic_report_directory(options).ok()?;
    if fs::create_dir_all(&directory).is_err() {
        return None;
    }
    set_private_directory_permissions(&directory);

    let active_directory = REPORT_DIRECTORY.get_or_init(|| Mutex::new(None));
    *active_directory.lock().ok()? = Some(directory.clone());

    let _ = std::panic::catch_unwind(|| {
        PANIC_HOOK_INSTALLED.get_or_init(|| {
            let previous_hook = std::panic::take_hook();
            std::panic::set_hook(Box::new(move |panic_info| {
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    write_panic_report();
                }));
                previous_hook(panic_info);
            }));
        });
    });

    Some(directory)
}

#[cfg(unix)]
fn set_private_directory_permissions(directory: &Path) {
    use std::os::unix::fs::PermissionsExt;

    // Match the source implementation: try to enforce owner-only access, but
    // tolerate ACL-managed or read-only directories.
    let _ = fs::set_permissions(directory, fs::Permissions::from_mode(0o700));
}

#[cfg(not(unix))]
fn set_private_directory_permissions(_directory: &Path) {
    // The standard library has no portable owner-only ACL API. The configured
    // directory remains caller-controlled on platforms without Unix modes.
}

#[cfg(unix)]
fn open_private_report(path: &Path) -> io::Result<fs::File> {
    use std::os::unix::fs::OpenOptionsExt;

    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
}

#[cfg(not(unix))]
fn open_private_report(path: &Path) -> io::Result<fs::File> {
    OpenOptions::new().write(true).create_new(true).open(path)
}

fn write_panic_report() {
    let directory = REPORT_DIRECTORY
        .get()
        .and_then(|active| active.lock().ok()?.clone());
    let Some(directory) = directory else {
        return;
    };

    let timestamp_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let sequence = REPORT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let report_path = directory.join(format!(
        "canopy-rust-panic-{}-{timestamp_ms}-{sequence}.txt",
        std::process::id()
    ));
    let Ok(mut report) = open_private_report(&report_path) else {
        return;
    };

    let backtrace = Backtrace::force_capture();
    let _ = writeln!(report, "Canopy Rust panic diagnostic");
    let _ = writeln!(report, "unix_timestamp_ms: {timestamp_ms}");
    let _ = writeln!(report, "process_id: {}", std::process::id());
    let _ = writeln!(report, "panic_payload: omitted for privacy");
    let _ = writeln!(report, "backtrace:\n{backtrace}");
}
