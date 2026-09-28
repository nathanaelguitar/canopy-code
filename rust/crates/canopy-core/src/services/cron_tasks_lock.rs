//! Cross-session owner lock for the durable cron scheduler.
//!
//! Source: `packages/core/src/services/cronTasksLock.ts`. This lock is distinct
//! from the short-lived read/modify/write lock in `cron_tasks_file.rs`: it
//! selects the one session allowed to fire shared per-project tasks.

use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;

use crate::storage::Storage;

pub const CRON_OWNER_LOCK_FILENAME: &str = "scheduled_tasks.lock";
static STALE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LockContent {
    pid: u32,
    session_id: String,
    #[serde(flatten)]
    extra: Map<String, Value>,
}

#[derive(Debug, Error)]
pub enum CronOwnerLockError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("Could not resolve process liveness for PID {0}")]
    ProcessProbe(u32),
}

/// Return the machine-local scheduler-owner lock path for a project.
pub fn get_cron_owner_lock_path(project_root: impl AsRef<Path>) -> PathBuf {
    Storage::new(project_root.as_ref())
        .get_project_temp_dir()
        .join(CRON_OWNER_LOCK_FILENAME)
}

/// Try to acquire the durable scheduler owner lock.
///
/// Acquisition is idempotent for the same process/session/lock ID. A live
/// foreign PID is respected. Dead or malformed locks are renamed aside before
/// replacement; a moved lock is rechecked and restored if it belongs to a live
/// process. The source makes two attempts so a racing stale-lock cleanup gets
/// one retry without creating an unbounded acquisition loop.
pub fn try_acquire_lock(
    project_root: impl AsRef<Path>,
    session_id: &str,
    lock_id: Option<&str>,
) -> Result<bool, CronOwnerLockError> {
    let lock_path = get_cron_owner_lock_path(project_root);
    let parent = lock_path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let mut extra = Map::new();
    if let Some(lock_id) = lock_id {
        extra.insert("lockId".to_owned(), Value::String(lock_id.to_owned()));
    }
    let content = LockContent {
        pid: std::process::id(),
        session_id: session_id.to_owned(),
        extra,
    };
    let bytes = serde_json::to_vec(&content).expect("lock record serializes");

    for _attempt in 0..2 {
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&lock_path)
        {
            Ok(mut file) => {
                file.write_all(&bytes)?;
                return Ok(true);
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }

        let existing = match fs::read(&lock_path) {
            Ok(raw) => match serde_json::from_slice::<LockContent>(&raw) {
                Ok(existing) => Some(existing),
                Err(_) => None,
            },
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };

        if let Some(existing) = &existing {
            if existing.pid == content.pid
                && existing.session_id == session_id
                && lock_ids_match(&existing, lock_id)
            {
                return Ok(true);
            }
            if is_process_alive(existing.pid)? {
                return Ok(false);
            }
        }

        let sequence = STALE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let stale_path = PathBuf::from(format!(
            "{}.stale.{}.{}",
            lock_path.display(),
            std::process::id(),
            sequence
        ));
        if let Err(error) = fs::rename(&lock_path, &stale_path) {
            if error.kind() == io::ErrorKind::NotFound {
                continue;
            }
            return Ok(false);
        }

        let moved_is_live = match fs::read(&stale_path) {
            Ok(raw) => match serde_json::from_slice::<LockContent>(&raw) {
                Ok(moved) => is_process_alive(moved.pid)?,
                Err(_) => false,
            },
            // An unreadable moved file is not safe to steal. A malformed JSON
            // record is handled above as stale, matching the source.
            Err(_) => true,
        };

        if moved_is_live {
            // link() is create-if-absent: never overwrite a newer lock that a
            // racing scheduler may have installed at the canonical path.
            let _ = fs::hard_link(&stale_path, &lock_path);
            let _ = fs::remove_file(&stale_path);
            return Ok(false);
        }
        let _ = fs::remove_file(&stale_path);
    }

    Ok(false)
}

/// Release the lock only when the caller still owns its current contents.
/// Cleanup is best-effort, as in the TypeScript shutdown path.
pub fn release_lock(project_root: impl AsRef<Path>, session_id: &str, lock_id: Option<&str>) {
    let lock_path = get_cron_owner_lock_path(project_root);
    let Ok(raw) = fs::read(&lock_path) else {
        return;
    };
    let Ok(existing) = serde_json::from_slice::<LockContent>(&raw) else {
        return;
    };
    if existing.pid == std::process::id()
        && existing.session_id == session_id
        && lock_ids_match(&existing, lock_id)
    {
        let _ = fs::remove_file(lock_path);
    }
}

fn lock_ids_match(content: &LockContent, requested: Option<&str>) -> bool {
    match (content.extra.get("lockId"), requested) {
        (None, None) => true,
        (Some(Value::String(stored)), Some(requested)) => stored == requested,
        _ => false,
    }
}

#[cfg(unix)]
fn is_process_alive(pid: u32) -> Result<bool, CronOwnerLockError> {
    use nix::errno::Errno;
    use nix::sys::signal::kill;
    use nix::unistd::Pid;

    let Ok(pid) = i32::try_from(pid) else {
        return Ok(false);
    };
    match kill(Pid::from_raw(pid), None) {
        Ok(()) => Ok(true),
        Err(Errno::EPERM) => Ok(true),
        Err(Errno::ESRCH) => Ok(false),
        Err(_) => Ok(false),
    }
}

#[cfg(windows)]
fn is_process_alive(pid: u32) -> Result<bool, CronOwnerLockError> {
    use std::process::Command;

    // Avoid shell parsing user-controlled input: PID is generated by the
    // operating system and passed as a single filter argument.
    let output = Command::new("tasklist")
        .args(["/FI", &format!("PID eq {pid}"), "/FO", "CSV", "/NH"])
        .output()
        .map_err(CronOwnerLockError::Io)?;
    if !output.status.success() {
        return Err(CronOwnerLockError::ProcessProbe(pid));
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let pid_text = pid.to_string();
    Ok(stdout.lines().any(|line| {
        line.trim_start_matches('"')
            .split("\",\"")
            .nth(1)
            .is_some_and(|field| field.trim_matches('"') == pid_text)
    }))
}

#[cfg(not(any(unix, windows)))]
fn is_process_alive(_pid: u32) -> Result<bool, CronOwnerLockError> {
    Err(CronOwnerLockError::ProcessProbe(0))
}
