//! Cross-process index of live Canopy sessions.
//!
//! Port of the TypeScript session registry and process-liveness helpers.
//! Linux records carry a boot-scoped process-start token and PID namespace
//! inode. On macOS the source implementation has neither identity value and
//! consequently degrades to kill(pid, 0) liveness.

use std::fs::{self, File};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use regex::Regex;
use serde::Serialize;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use unicode_normalization::UnicodeNormalization;

use crate::storage::Storage;
use crate::utils::atomic_file_write::{AtomicWriteOptions, SymlinkPolicy, atomic_write_file};

pub const SESSION_REGISTRY_SCHEMA_VERSION: u32 = 1;
const REGISTRY_DIR_MODE: u32 = 0o700;
const REGISTRY_FILE_MODE: u32 = 0o600;
const MAX_RECORD_BYTES: u64 = 64 * 1024;
const TEMP_MAX_AGE: Duration = Duration::from_secs(5 * 60);

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionRegistryRecord {
    pub schema_version: f64,
    pub pid: i64,
    pub proc_start: Option<String>,
    pub pid_ns: Option<f64>,
    pub session_id: String,
    pub cwd: String,
    pub name: String,
    pub started_at: f64,
    pub canopy_version: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegisterSessionFields {
    pub session_id: String,
    pub cwd: String,
    pub canopy_version: Option<String>,
}

impl RegisterSessionFields {
    pub fn new(session_id: impl Into<String>, cwd: impl Into<String>) -> Self {
        Self {
            session_id: session_id.into(),
            cwd: cwd.into(),
            canopy_version: None,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct SessionRegistryPatch {
    pub session_id: Option<String>,
    pub cwd: Option<String>,
    pub name: Option<String>,
    pub started_at: Option<f64>,
    /// None leaves the field unchanged; Some(None) writes JSON null.
    pub canopy_version: Option<Option<String>>,
}

/// Injectable OS/path seam for deterministic registry and identity tests.
pub trait SessionRegistryEnvironment: Send + Sync {
    fn global_canopy_dir(&self) -> io::Result<PathBuf>;
    fn current_pid(&self) -> i64;
    fn is_linux(&self) -> bool;
    fn read_proc_start_token(&self, pid: i64) -> Option<String>;
    fn read_pid_namespace_id(&self) -> Option<f64>;
    fn read_local_boot_id(&self) -> Option<String>;
    fn is_pid_alive(&self, pid: i64) -> bool;
    fn now_ms(&self) -> f64;

    fn is_same_process(&self, pid: i64, proc_start: Option<&str>) -> bool {
        if !self.is_pid_alive(pid) {
            return false;
        }
        let Some(recorded) = proc_start else {
            return true;
        };
        self.read_proc_start_token(pid)
            .as_deref()
            .is_none_or(|current| current == recorded)
    }
}

#[derive(Default)]
pub struct SystemSessionRegistryEnvironment;

impl SessionRegistryEnvironment for SystemSessionRegistryEnvironment {
    fn global_canopy_dir(&self) -> io::Result<PathBuf> {
        Ok(Storage::get_global_canopy_dir())
    }
    fn current_pid(&self) -> i64 {
        i64::from(std::process::id())
    }
    fn is_linux(&self) -> bool {
        cfg!(target_os = "linux")
    }
    fn read_proc_start_token(&self, pid: i64) -> Option<String> {
        read_proc_start_token(pid)
    }
    fn read_pid_namespace_id(&self) -> Option<f64> {
        read_pid_namespace_id()
    }
    fn read_local_boot_id(&self) -> Option<String> {
        read_local_boot_id()
    }
    fn is_pid_alive(&self, pid: i64) -> bool {
        is_pid_alive(pid)
    }
    fn now_ms(&self) -> f64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as f64
    }
}

#[derive(Clone)]
pub struct SessionRegistry {
    environment: Arc<dyn SessionRegistryEnvironment>,
    registered_record_path: Arc<Mutex<Option<PathBuf>>>,
}

impl Default for SessionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionRegistry {
    pub fn new() -> Self {
        Self::with_environment(Arc::new(SystemSessionRegistryEnvironment))
    }

    pub fn with_environment(environment: Arc<dyn SessionRegistryEnvironment>) -> Self {
        Self {
            environment,
            registered_record_path: Arc::new(Mutex::new(None)),
        }
    }

    pub fn registry_dir(&self) -> io::Result<PathBuf> {
        Ok(self.environment.global_canopy_dir()?.join("sessions"))
    }

    pub fn session_record_path(&self) -> io::Result<PathBuf> {
        Ok(self
            .registry_dir()?
            .join(format!("{}.json", self.environment.current_pid())))
    }

    /// Best-effort registration: filesystem, identity, and home failures do
    /// not prevent startup. Linux refuses tokenless or namespace-less writes.
    pub fn register_session(&self, fields: RegisterSessionFields) -> bool {
        let pid = self.environment.current_pid();
        let mut proc_start = self.environment.read_proc_start_token(pid);
        if self.environment.is_linux() && proc_start.is_none() {
            proc_start = self.environment.read_proc_start_token(pid);
            if proc_start.is_none() {
                return false;
            }
        }
        let mut pid_ns = self.environment.read_pid_namespace_id();
        if self.environment.is_linux() && pid_ns.is_none() {
            pid_ns = self.environment.read_pid_namespace_id();
            if pid_ns.is_none() {
                return false;
            }
        }
        let record = SessionRegistryRecord {
            schema_version: f64::from(SESSION_REGISTRY_SCHEMA_VERSION),
            pid,
            proc_start,
            pid_ns,
            name: derive_session_name(&fields.cwd, &fields.session_id),
            session_id: fields.session_id,
            cwd: fields.cwd,
            started_at: self.environment.now_ms(),
            canopy_version: fields.canopy_version,
        };
        let result = (|| {
            let dir = self.registry_dir().ok()?;
            let file_path = dir.join(format!("{}.json", self.environment.current_pid()));
            match read_record(&file_path) {
                RecordRead::ReadError | RecordRead::UnsupportedVersion => return None,
                RecordRead::Ok(existing) if !self.matches_local_identity(&existing) => return None,
                RecordRead::Ok(_) | RecordRead::Unreadable => {}
            }
            ensure_private_registry_dir(&dir).ok()?;
            write_record(&file_path, &record).ok()?;
            Some(file_path)
        })();
        if let Some(file_path) = result {
            *lock_unpoisoned(&self.registered_record_path) = Some(file_path);
            true
        } else {
            false
        }
    }

    /// Merge the supported mutable fields into an existing local record.
    pub fn patch_session_record(&self, patch: SessionRegistryPatch) {
        let result = (|| {
            let file_path = self.this_process_record_path().ok()?;
            let RecordRead::Ok(mut record) = read_record(&file_path) else {
                return None;
            };
            if !self.matches_local_identity(&record) {
                return None;
            }
            let current_token = self
                .environment
                .read_proc_start_token(self.environment.current_pid());
            if record.proc_start.is_some() && record.proc_start != current_token {
                return None;
            }
            if let Some(value) = patch.session_id {
                record.session_id = value;
            }
            if let Some(value) = patch.cwd {
                record.cwd = value;
            }
            if let Some(value) = patch.name {
                record.name = value;
            }
            if let Some(value) = patch.started_at {
                if !value.is_finite() {
                    return None;
                }
                record.started_at = value;
            }
            if let Some(value) = patch.canopy_version {
                record.canopy_version = value;
            }
            write_record(&file_path, &record).ok()
        })();
        let _ = result;
    }

    /// Remove only a record this process can positively identify as its own.
    pub fn unregister_session(&self) {
        let file_path = self.this_process_record_path();
        *lock_unpoisoned(&self.registered_record_path) = None;
        let Ok(file_path) = file_path else { return };
        match read_record(&file_path) {
            RecordRead::ReadError | RecordRead::UnsupportedVersion => return,
            RecordRead::Ok(record) if !self.matches_local_identity(&record) => return,
            RecordRead::Ok(_) | RecordRead::Unreadable => {}
        }
        let _ = fs::remove_file(file_path);
    }

    /// Enumerate live sessions newest first. Only same-namespace and same-
    /// boot records are eligible for a stale sweep.
    pub fn list_live_sessions(&self) -> Vec<SessionRegistryRecord> {
        let Ok(dir) = self.registry_dir() else {
            return Vec::new();
        };
        let Ok(entries) = fs::read_dir(&dir) else {
            return Vec::new();
        };
        let own_namespace = self.environment.read_pid_namespace_id();
        let own_boot_id = self.environment.read_local_boot_id();
        let mut live = Vec::new();
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            let path = entry.path();
            if !is_record_filename(name) {
                sweep_orphaned_temp_file(&path, name, self.environment.now_ms());
                continue;
            }
            let RecordRead::Ok(record) = read_record(&path) else {
                continue;
            };
            if format!("{}.json", record.pid) != name {
                continue;
            }
            if record.pid_ns != own_namespace {
                continue;
            }
            let record_boot_id = record.proc_start.as_deref().and_then(boot_id_of);
            if record_boot_id.is_some() && record_boot_id.as_deref() != own_boot_id.as_deref() {
                continue;
            }
            if self
                .environment
                .is_same_process(record.pid, record.proc_start.as_deref())
            {
                live.push(record);
                continue;
            }
            // Guard against a fresh incarnation replacing the stale record
            // between the liveness check and unlink.
            let RecordRead::Ok(reread) = read_record(&path) else {
                continue;
            };
            if reread.pid != record.pid
                || reread.pid_ns != record.pid_ns
                || reread.proc_start != record.proc_start
                || reread.started_at != record.started_at
            {
                continue;
            }
            let _ = fs::remove_file(path);
        }
        live.sort_by(|left, right| {
            right
                .started_at
                .partial_cmp(&left.started_at)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        live
    }

    #[cfg(test)]
    pub fn reset_registered_record_path_for_test(&self) {
        *lock_unpoisoned(&self.registered_record_path) = None;
    }

    fn this_process_record_path(&self) -> io::Result<PathBuf> {
        if let Some(path) = lock_unpoisoned(&self.registered_record_path).clone() {
            return Ok(path);
        }
        self.session_record_path()
    }

    fn matches_local_identity(&self, record: &SessionRegistryRecord) -> bool {
        if record.pid != self.environment.current_pid()
            || record.pid_ns != self.environment.read_pid_namespace_id()
        {
            return false;
        }
        let Some(proc_start) = record.proc_start.as_deref() else {
            return true;
        };
        let record_boot_id = boot_id_of(proc_start);
        let own_boot_id = self.environment.read_local_boot_id();
        if own_boot_id.is_none() {
            return record_boot_id.is_none();
        }
        record_boot_id.is_none() || record_boot_id == own_boot_id
    }
}

pub fn derive_session_name(cwd: &str, session_id: &str) -> String {
    static NON_NAME_CHARS: OnceLock<Regex> = OnceLock::new();
    let regex = NON_NAME_CHARS
        .get_or_init(|| Regex::new(r"[^\p{L}\p{M}\p{N}._-]+").expect("valid session-name regex"));
    let basename = Path::new(cwd)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    let normalized: String = basename.nfc().collect();
    let filtered = regex.replace_all(&normalized, "-");
    let filtered = filtered.trim_matches('-');
    let base: String = filtered.chars().take(32).collect();
    let base = if base.is_empty() { "session" } else { &base };
    let digest = Sha256::digest(session_id.as_bytes());
    format!("{base}-{:02x}", digest[0])
}

/// Linux process token: boot UUID plus /proc starttime (field 22).
pub fn read_proc_start_token(pid: i64) -> Option<String> {
    if !cfg!(target_os = "linux") || pid <= 0 {
        return None;
    }
    let boot_id = read_local_boot_id()?;
    let raw = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let start_time = proc_stat_starttime(&raw)?;
    if start_time.is_empty() || !start_time.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    Some(format!("{boot_id}:{start_time}"))
}

pub fn read_local_boot_id() -> Option<String> {
    static CACHED_BOOT_ID: OnceLock<String> = OnceLock::new();
    if let Some(value) = CACHED_BOOT_ID.get() {
        return Some(value.clone());
    }
    let value = fs::read_to_string("/proc/sys/kernel/random/boot_id")
        .ok()?
        .trim()
        .to_owned();
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() || byte == b'-')
    {
        return None;
    }
    let _ = CACHED_BOOT_ID.set(value.clone());
    Some(value)
}

pub fn read_pid_namespace_id() -> Option<f64> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::MetadataExt;
        return fs::metadata("/proc/self/ns/pid")
            .ok()
            .map(|metadata| metadata.ino() as f64);
    }
    #[allow(unreachable_code)]
    None
}

/// True for an existing non-zombie process; EPERM/EACCES mean alive.
pub fn is_pid_alive(pid: i64) -> bool {
    if pid <= 0 || pid > i64::from(i32::MAX) {
        return false;
    }
    #[cfg(unix)]
    {
        use nix::errno::Errno;
        use nix::sys::signal::kill;
        use nix::unistd::Pid;
        match kill(Pid::from_raw(pid as i32), None) {
            Ok(()) => !is_zombie(pid),
            Err(Errno::EPERM | Errno::EACCES) => !is_zombie(pid),
            Err(_) => false,
        }
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        // There is no safe dependency-free Windows equivalent in this port.
        // Unknown is conservatively treated as not live; macOS uses nix/kill.
        false
    }
}

fn is_zombie(pid: i64) -> bool {
    if !cfg!(target_os = "linux") {
        return false;
    }
    let Ok(raw) = fs::read_to_string(format!("/proc/{pid}/stat")) else {
        return false;
    };
    proc_stat_state(&raw).is_some_and(|state| state.starts_with('Z'))
}

fn proc_stat_state(raw: &str) -> Option<&str> {
    let comm_end = raw.rfind(')')?;
    raw[comm_end + 1..].split_whitespace().next()
}

fn proc_stat_starttime(raw: &str) -> Option<&str> {
    let comm_end = raw.rfind(')')?;
    raw[comm_end + 1..].split_whitespace().nth(19)
}

#[derive(Debug)]
enum RecordRead {
    Ok(SessionRegistryRecord),
    Unreadable,
    ReadError,
    UnsupportedVersion,
}

fn read_record(path: &Path) -> RecordRead {
    let metadata = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return RecordRead::Unreadable,
        Err(_) => return RecordRead::ReadError,
    };
    if !metadata.is_file() || metadata.len() > MAX_RECORD_BYTES {
        return RecordRead::Unreadable;
    }
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return RecordRead::Unreadable,
        Err(_) => return RecordRead::ReadError,
    };
    let mut bytes = Vec::with_capacity(metadata.len().min(MAX_RECORD_BYTES) as usize);
    if file
        .by_ref()
        .take(MAX_RECORD_BYTES + 1)
        .read_to_end(&mut bytes)
        .is_err()
    {
        return RecordRead::ReadError;
    }
    if bytes.len() as u64 > MAX_RECORD_BYTES {
        return RecordRead::Unreadable;
    }
    let raw = String::from_utf8_lossy(&bytes);
    let Ok(value) = serde_json::from_str::<Value>(&raw) else {
        return RecordRead::Unreadable;
    };
    let Some(object) = value.as_object() else {
        return RecordRead::Unreadable;
    };
    parse_record_object(object)
}

fn parse_record_object(value: &Map<String, Value>) -> RecordRead {
    let Some(schema_version) = value.get("schemaVersion").and_then(Value::as_f64) else {
        return RecordRead::Unreadable;
    };
    if schema_version > f64::from(SESSION_REGISTRY_SCHEMA_VERSION) {
        return RecordRead::UnsupportedVersion;
    }
    let Some(pid) = value.get("pid").and_then(Value::as_f64) else {
        return RecordRead::Unreadable;
    };
    if !pid.is_finite() || pid.fract() != 0.0 || pid <= 0.0 || pid > i64::MAX as f64 {
        return RecordRead::Unreadable;
    }
    let Some(session_id) = value.get("sessionId").and_then(Value::as_str) else {
        return RecordRead::Unreadable;
    };
    let Some(cwd) = value.get("cwd").and_then(Value::as_str) else {
        return RecordRead::Unreadable;
    };
    let Some(name) = value.get("name").and_then(Value::as_str) else {
        return RecordRead::Unreadable;
    };
    let Some(started_at) = value.get("startedAt").and_then(Value::as_f64) else {
        return RecordRead::Unreadable;
    };
    if !started_at.is_finite() {
        return RecordRead::Unreadable;
    }
    let proc_start = value
        .get("procStart")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let pid_ns = value
        .get("pidNs")
        .and_then(Value::as_f64)
        .filter(|n| n.is_finite());
    let canopy_version = value
        .get("canopyVersion")
        .and_then(Value::as_str)
        .map(str::to_owned);
    RecordRead::Ok(SessionRegistryRecord {
        schema_version,
        pid: pid as i64,
        proc_start,
        pid_ns,
        session_id: session_id.to_owned(),
        cwd: cwd.to_owned(),
        name: name.to_owned(),
        started_at,
        canopy_version,
    })
}

fn write_record(path: &Path, record: &SessionRegistryRecord) -> io::Result<()> {
    let bytes = serde_json::to_vec_pretty(record)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    atomic_write_file(
        path,
        &bytes,
        &AtomicWriteOptions {
            mode: Some(REGISTRY_FILE_MODE),
            force_mode: true,
            flush: true,
            symlink_policy: SymlinkPolicy::NoFollow,
            ..AtomicWriteOptions::default()
        },
    )
}

fn ensure_private_registry_dir(path: &Path) -> io::Result<()> {
    fs::create_dir_all(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Err(error) = fs::set_permissions(path, fs::Permissions::from_mode(REGISTRY_DIR_MODE))
        {
            if error.kind() != io::ErrorKind::Unsupported {
                return Err(error);
            }
        }
    }
    Ok(())
}

fn is_record_filename(name: &str) -> bool {
    let Some(pid) = name.strip_suffix(".json") else {
        return false;
    };
    !pid.is_empty() && pid.bytes().all(|byte| byte.is_ascii_digit())
}

fn is_temp_filename(name: &str) -> bool {
    // TypeScript temp format: <pid>.json.<12 lowercase hex>.tmp.
    let source_format = name.strip_suffix(".tmp").is_some_and(|stem| {
        let Some((record, suffix)) = stem.rsplit_once('.') else {
            return false;
        };
        suffix.len() == 12
            && suffix
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
            && is_record_filename(record)
    });
    if source_format {
        return true;
    }
    // Shared Rust atomic writer format: .<pid>.json.canopy-<32 hex>.tmp.
    let Some(stem) = name
        .strip_prefix('.')
        .and_then(|value| value.strip_suffix(".tmp"))
    else {
        return false;
    };
    let Some((record, suffix)) = stem.rsplit_once(".canopy-") else {
        return false;
    };
    suffix.len() == 32
        && suffix
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        && is_record_filename(record)
}

fn sweep_orphaned_temp_file(path: &Path, name: &str, now_ms: f64) {
    if !is_temp_filename(name) {
        return;
    }
    let Ok(metadata) = fs::metadata(path) else {
        return;
    };
    if !metadata.is_file() {
        return;
    }
    let Ok(modified) = metadata.modified() else {
        return;
    };
    let age_ms = if now_ms.is_finite() {
        now_ms - system_time_ms(modified)
    } else {
        system_time_ms(SystemTime::now()) - system_time_ms(modified)
    };
    if age_ms < TEMP_MAX_AGE.as_secs_f64() * 1000.0 {
        return;
    }
    let _ = fs::remove_file(path);
}

fn system_time_ms(time: SystemTime) -> f64 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
        * 1000.0
}

fn boot_id_of(proc_start: &str) -> Option<String> {
    let separator = proc_start.find(':')?;
    Some(proc_start[..separator].to_owned())
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, Ordering};

    const BOOT: &str = "aabbccdd-1111-2222-3333-0123456789ab";
    const PID_NS: f64 = 4026531836.0;

    struct FakeEnvironment {
        root: Mutex<PathBuf>,
        token: Mutex<Option<String>>,
        namespace: Mutex<Option<f64>>,
        boot: Mutex<Option<String>>,
        alive: Mutex<HashMap<i64, bool>>,
        linux: bool,
        now: Mutex<f64>,
        token_reads: Mutex<usize>,
        namespace_reads: Mutex<usize>,
        root_fails: AtomicBool,
        replace_on_alive: Mutex<Option<(i64, PathBuf, Value)>>,
    }

    impl FakeEnvironment {
        fn new(root: PathBuf) -> Self {
            Self {
                root: Mutex::new(root),
                token: Mutex::new(Some(format!("{BOOT}:100"))),
                namespace: Mutex::new(Some(PID_NS)),
                boot: Mutex::new(Some(BOOT.to_owned())),
                alive: Mutex::new(HashMap::from([(4242, true)])),
                linux: true,
                now: Mutex::new(system_time_ms(SystemTime::now())),
                token_reads: Mutex::new(0),
                namespace_reads: Mutex::new(0),
                root_fails: AtomicBool::new(false),
                replace_on_alive: Mutex::new(None),
            }
        }

        fn raw_record(root: &Path, filename: &str, value: &Value) -> PathBuf {
            let dir = root.join("sessions");
            fs::create_dir_all(&dir).unwrap();
            let path = dir.join(filename);
            fs::write(&path, serde_json::to_vec(value).unwrap()).unwrap();
            path
        }

        fn value(
            pid: i64,
            started_at: f64,
            proc_start: Option<&str>,
            pid_ns: Option<f64>,
        ) -> Value {
            serde_json::json!({
                "schemaVersion": 1,
                "pid": pid,
                "procStart": proc_start,
                "pidNs": pid_ns,
                "sessionId": format!("session-{pid}"),
                "cwd": "/work/app",
                "name": "app-aa",
                "startedAt": started_at,
                "canopyVersion": null
            })
        }

        fn set_alive(&self, pid: i64, alive: bool) {
            lock_unpoisoned(&self.alive).insert(pid, alive);
        }
    }

    impl SessionRegistryEnvironment for FakeEnvironment {
        fn global_canopy_dir(&self) -> io::Result<PathBuf> {
            if self.root_fails.load(Ordering::SeqCst) {
                return Err(io::Error::other("home unavailable"));
            }
            Ok(lock_unpoisoned(&self.root).clone())
        }

        fn current_pid(&self) -> i64 {
            4242
        }
        fn is_linux(&self) -> bool {
            self.linux
        }

        fn read_proc_start_token(&self, _pid: i64) -> Option<String> {
            *lock_unpoisoned(&self.token_reads) += 1;
            lock_unpoisoned(&self.token).clone()
        }

        fn read_pid_namespace_id(&self) -> Option<f64> {
            *lock_unpoisoned(&self.namespace_reads) += 1;
            *lock_unpoisoned(&self.namespace)
        }

        fn read_local_boot_id(&self) -> Option<String> {
            lock_unpoisoned(&self.boot).clone()
        }

        fn is_pid_alive(&self, pid: i64) -> bool {
            let replacement = lock_unpoisoned(&self.replace_on_alive).take();
            if let Some((target_pid, path, value)) = replacement {
                if target_pid == pid {
                    let _ = fs::write(path, serde_json::to_vec(&value).unwrap());
                } else {
                    *lock_unpoisoned(&self.replace_on_alive) = Some((target_pid, path, value));
                }
            }
            lock_unpoisoned(&self.alive)
                .get(&pid)
                .copied()
                .unwrap_or(false)
        }

        fn now_ms(&self) -> f64 {
            *lock_unpoisoned(&self.now)
        }
    }

    fn setup() -> (PathBuf, Arc<FakeEnvironment>, SessionRegistry) {
        let root = std::env::temp_dir().join(format!(
            "canopy-session-registry-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        ));
        let _ = fs::remove_dir_all(&root);
        let environment = Arc::new(FakeEnvironment::new(root.clone()));
        let registry = SessionRegistry::with_environment(environment.clone());
        (root, environment, registry)
    }

    fn cleanup(root: &Path) {
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn names_match_unicode_normalization_and_codepoint_limit_contract() {
        assert_eq!(
            derive_session_name("/work/cafe\u{301}", "s1"),
            derive_session_name("/work/caf\u{e9}", "s1")
        );
        assert!(derive_session_name("/work/项目", "s1").starts_with("项目-"));
        assert!(derive_session_name("/work/!!!", "s1").starts_with("session-"));
        let path = format!("/work/{}𠀀", "a".repeat(31));
        assert!(derive_session_name(&path, "s1").starts_with(&format!("{}𠀀-", "a".repeat(31))));
        assert_eq!(derive_session_name("/", "s1").len(), "session-".len() + 2);
    }

    #[test]
    fn register_writes_schema_private_modes_and_lists_current_record() {
        let (root, _environment, registry) = setup();
        let mut fields = RegisterSessionFields::new("s1", "/work/app");
        fields.canopy_version = Some("1.2.3".to_owned());
        assert!(registry.register_session(fields));
        let path = root.join("sessions/4242.json");
        let value: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(value["schemaVersion"].as_f64(), Some(1.0));
        assert_eq!(value["pid"], 4242);
        assert_eq!(value["procStart"], format!("{BOOT}:100"));
        assert_eq!(value["pidNs"], PID_NS);
        assert_eq!(value["canopyVersion"], "1.2.3");
        assert!(value.get("startedAt").and_then(Value::as_f64).is_some());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(root.join("sessions"))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o700
            );
            assert_eq!(
                fs::metadata(path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        let live = registry.list_live_sessions();
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].session_id, "s1");
        assert_eq!(live[0].canopy_version.as_deref(), Some("1.2.3"));
        cleanup(&root);
    }

    #[test]
    fn register_refuses_foreign_or_newer_records_without_modifying_them() {
        let (root, _environment, registry) = setup();
        let path = FakeEnvironment::raw_record(
            &root,
            "4242.json",
            &FakeEnvironment::value(4242, 1.0, Some(&format!("{BOOT}:100")), Some(88.0)),
        );
        let original = fs::read(&path).unwrap();
        assert!(!registry.register_session(RegisterSessionFields::new("new", "/work")));
        assert_eq!(fs::read(&path).unwrap(), original);
        fs::write(&path, br#"{"schemaVersion":2,"pid":4242}"#).unwrap();
        let original = fs::read(&path).unwrap();
        assert!(!registry.register_session(RegisterSessionFields::new("new", "/work")));
        assert_eq!(fs::read(&path).unwrap(), original);
        cleanup(&root);
    }

    #[test]
    fn linux_registration_retries_token_and_namespace_reads_then_refuses() {
        let (root, environment, registry) = setup();
        *lock_unpoisoned(&environment.token) = None;
        assert!(!registry.register_session(RegisterSessionFields::new("s", "/work")));
        assert_eq!(*lock_unpoisoned(&environment.token_reads), 2);
        assert!(!root.join("sessions/4242.json").exists());
        *lock_unpoisoned(&environment.token) = Some(format!("{BOOT}:100"));
        *lock_unpoisoned(&environment.namespace) = None;
        assert!(!registry.register_session(RegisterSessionFields::new("s", "/work")));
        assert_eq!(*lock_unpoisoned(&environment.namespace_reads), 2);
        cleanup(&root);
    }

    #[test]
    fn patch_and_unregister_use_registered_path_after_global_root_changes() {
        let (root, environment, registry) = setup();
        assert!(registry.register_session(RegisterSessionFields::new("before", "/work")));
        let moved_root = root.with_extension("moved");
        *lock_unpoisoned(&environment.root) = moved_root.clone();
        registry.patch_session_record(SessionRegistryPatch {
            session_id: Some("after".to_owned()),
            canopy_version: Some(None),
            ..SessionRegistryPatch::default()
        });
        let value: Value =
            serde_json::from_slice(&fs::read(root.join("sessions/4242.json")).unwrap()).unwrap();
        assert_eq!(value["sessionId"], "after");
        assert_eq!(value["canopyVersion"], Value::Null);
        assert!(!moved_root.join("sessions/4242.json").exists());
        registry.unregister_session();
        assert!(!root.join("sessions/4242.json").exists());
        registry.reset_registered_record_path_for_test();
        cleanup(&root);
        cleanup(&moved_root);
    }

    #[test]
    fn list_sweeps_dead_local_records_but_leaves_other_namespace_and_boot_alone() {
        let (root, environment, registry) = setup();
        let dir = root.join("sessions");
        fs::create_dir_all(&dir).unwrap();
        FakeEnvironment::raw_record(
            &root,
            "4242.json",
            &FakeEnvironment::value(4242, 20.0, Some(&format!("{BOOT}:100")), Some(PID_NS)),
        );
        FakeEnvironment::raw_record(
            &root,
            "4243.json",
            &FakeEnvironment::value(4243, 30.0, Some(&format!("{BOOT}:100")), Some(99.0)),
        );
        FakeEnvironment::raw_record(
            &root,
            "4244.json",
            &FakeEnvironment::value(4244, 40.0, Some("foreign-boot:1"), Some(PID_NS)),
        );
        environment.set_alive(4243, false);
        environment.set_alive(4244, false);
        let live = registry.list_live_sessions();
        assert_eq!(
            live.iter().map(|item| item.pid).collect::<Vec<_>>(),
            vec![4242]
        );
        assert!(dir.join("4242.json").exists());
        assert!(dir.join("4243.json").exists());
        assert!(dir.join("4244.json").exists());
        cleanup(&root);
    }

    #[test]
    fn list_sweeps_recycled_pid_token_and_sorts_live_records_newest_first() {
        let (root, environment, registry) = setup();
        let dir = root.join("sessions");
        fs::create_dir_all(&dir).unwrap();
        FakeEnvironment::raw_record(
            &root,
            "4242.json",
            &FakeEnvironment::value(4242, 10.0, Some(&format!("{BOOT}:100")), Some(PID_NS)),
        );
        FakeEnvironment::raw_record(
            &root,
            "4243.json",
            &FakeEnvironment::value(4243, 30.0, Some(&format!("{BOOT}:100")), Some(PID_NS)),
        );
        FakeEnvironment::raw_record(
            &root,
            "4244.json",
            &FakeEnvironment::value(4244, 20.0, Some(&format!("{BOOT}:old")), Some(PID_NS)),
        );
        environment.set_alive(4243, true);
        environment.set_alive(4244, false);
        let live = registry.list_live_sessions();
        assert_eq!(
            live.iter().map(|item| item.pid).collect::<Vec<_>>(),
            vec![4243, 4242]
        );
        assert!(!dir.join("4244.json").exists());
        cleanup(&root);
    }

    #[test]
    fn listing_rejects_strange_names_malformed_future_and_oversized_records() {
        let (root, environment, registry) = setup();
        let dir = root.join("sessions");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("2026-planning-notes.json"), b"garbage").unwrap();
        fs::write(dir.join("01.json"), b"garbage").unwrap();
        fs::write(dir.join("4242.json"), br#"{"schemaVersion":2}"#).unwrap();
        fs::write(
            dir.join("4243.json"),
            vec![b'x'; (MAX_RECORD_BYTES + 1) as usize],
        )
        .unwrap();
        FakeEnvironment::raw_record(
            &root,
            "4244.json",
            &FakeEnvironment::value(4244, 1.0, Some(&format!("{BOOT}:1")), None),
        );
        let mut invalid = serde_json::json!({
            "schemaVersion": 1, "pid": 4245, "procStart": null, "pidNs": PID_NS,
            "sessionId": "x", "cwd": "/w", "name": "x", "startedAt": "not-number", "canopyVersion": null
        });
        invalid["extra"] = Value::Bool(true);
        FakeEnvironment::raw_record(&root, "4245.json", &invalid);
        // The filename/contents mismatch and PID 0 must be left untouched,
        // even when their shape would otherwise look like a record.
        FakeEnvironment::raw_record(
            &root,
            "42.json",
            &FakeEnvironment::value(43, 2.0, None, Some(PID_NS)),
        );
        FakeEnvironment::raw_record(
            &root,
            "0.json",
            &FakeEnvironment::value(0, 2.0, None, Some(PID_NS)),
        );
        assert!(registry.list_live_sessions().is_empty());
        for name in [
            "2026-planning-notes.json",
            "01.json",
            "4242.json",
            "4243.json",
            "4244.json",
            "4245.json",
            "42.json",
            "0.json",
        ] {
            assert!(dir.join(name).exists(), "{name}");
        }
        let _ = environment;
        cleanup(&root);
    }

    #[test]
    fn list_normalizes_wrong_optional_types_and_drops_unknown_fields() {
        let (root, _environment, _registry) = setup();
        let mut mac = FakeEnvironment::new(root.clone());
        mac.linux = false;
        *lock_unpoisoned(&mac.token) = None;
        *lock_unpoisoned(&mac.namespace) = None;
        *lock_unpoisoned(&mac.boot) = None;
        let registry = SessionRegistry::with_environment(Arc::new(mac));
        let mut value = serde_json::json!({
            "schemaVersion": 1, "pid": 4242, "procStart": 3, "pidNs": "bad",
            "sessionId": "s", "cwd": "/w", "name": "w", "startedAt": 1,
            "canopyVersion": false, "future": { "field": true }
        });
        FakeEnvironment::raw_record(&root, "4242.json", &value);
        value["procStart"] = Value::Null;
        value["pidNs"] = Value::Null;
        // Wrong optional types are already normalized to null by the reader.
        let live = registry.list_live_sessions();
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].proc_start, None);
        assert_eq!(live[0].pid_ns, None);
        assert_eq!(live[0].canopy_version, None);
        assert!(
            !serde_json::to_value(&live[0])
                .unwrap()
                .as_object()
                .unwrap()
                .contains_key("future")
        );
        cleanup(&root);
    }

    #[test]
    fn home_failure_is_best_effort_for_every_entrypoint() {
        let (root, environment, registry) = setup();
        environment.root_fails.store(true, Ordering::SeqCst);
        assert!(!registry.register_session(RegisterSessionFields::new("s", "/w")));
        registry.patch_session_record(SessionRegistryPatch::default());
        registry.unregister_session();
        assert!(registry.list_live_sessions().is_empty());
        cleanup(&root);
    }

    #[cfg(unix)]
    #[test]
    fn registration_replaces_symlink_without_following_it() {
        use std::os::unix::fs::symlink;
        let (root, _environment, registry) = setup();
        let outside = root.with_extension("outside");
        fs::write(&outside, b"preserve").unwrap();
        let dir = root.join("sessions");
        fs::create_dir_all(&dir).unwrap();
        symlink(&outside, dir.join("4242.json")).unwrap();
        assert!(registry.register_session(RegisterSessionFields::new("s", "/w")));
        assert_eq!(fs::read(outside).unwrap(), b"preserve");
        assert!(
            !fs::symlink_metadata(dir.join("4242.json"))
                .unwrap()
                .file_type()
                .is_symlink()
        );
        cleanup(&root);
    }

    #[test]
    fn unregister_preserves_foreign_record_and_removes_corrupt_local_slot() {
        let (root, _environment, registry) = setup();
        let path = FakeEnvironment::raw_record(
            &root,
            "4242.json",
            &FakeEnvironment::value(4242, 1.0, Some(&format!("{BOOT}:100")), Some(88.0)),
        );
        registry.unregister_session();
        assert!(path.exists());
        fs::write(&path, b"truncated").unwrap();
        registry.unregister_session();
        assert!(!path.exists());
        cleanup(&root);
    }

    #[test]
    fn temp_sweep_accepts_only_exact_formats_and_removes_only_old_files() {
        assert!(is_temp_filename("4242.json.abcdef012345.tmp"));
        assert!(is_temp_filename(
            ".4242.json.canopy-0123456789abcdef0123456789abcdef.tmp"
        ));
        assert!(!is_temp_filename(
            "2026-planning-notes.json.abcdef012345.tmp"
        ));
        assert!(!is_temp_filename("4242.json.ABCDEF012345.tmp"));
        assert!(!is_temp_filename("4242.json.abcdef01234.tmp"));
        let (root, environment, registry) = setup();
        let dir = root.join("sessions");
        fs::create_dir_all(&dir).unwrap();
        let old = dir.join("4242.json.abcdef012345.tmp");
        let young = dir.join("4242.json.abcdef012346.tmp");
        fs::write(&old, b"old").unwrap();
        fs::write(&young, b"young").unwrap();
        let now = SystemTime::now();
        *lock_unpoisoned(&environment.now) = system_time_ms(now);
        File::open(&old)
            .unwrap()
            .set_modified(now - TEMP_MAX_AGE - Duration::from_secs(1))
            .unwrap();
        File::open(&young).unwrap().set_modified(now).unwrap();
        registry.list_live_sessions();
        assert!(!old.exists());
        assert!(young.exists());
        cleanup(&root);
    }

    #[test]
    fn linux_stat_parser_anchors_after_last_parenthesis() {
        let fields = [
            "S", "1", "2", "3", "4", "-1", "4194304", "100", "0", "200", "0", "10", "20", "30",
            "40", "20", "0", "1", "0", "987654",
        ];
        let raw = format!("4242 (a ) complicated comm) {}", fields.join(" "));
        assert_eq!(proc_stat_starttime(&raw), Some("987654"));
        assert_eq!(proc_stat_state(&raw), Some("S"));
        let zombie = raw.replacen(") S ", ") Z ", 1);
        assert_eq!(proc_stat_state(&zombie), Some("Z"));
    }

    #[test]
    fn live_pid_is_kept_when_its_token_is_temporarily_unreadable() {
        let (root, environment, registry) = setup();
        FakeEnvironment::raw_record(
            &root,
            "4242.json",
            &FakeEnvironment::value(4242, 1.0, Some(&format!("{BOOT}:old")), Some(PID_NS)),
        );
        *lock_unpoisoned(&environment.token) = None;
        assert_eq!(registry.list_live_sessions().len(), 1);
        assert!(root.join("sessions/4242.json").exists());
        cleanup(&root);
    }

    #[test]
    fn stale_sweep_rechecks_before_deleting_a_replacement() {
        let (root, environment, registry) = setup();
        let path = FakeEnvironment::raw_record(
            &root,
            "4243.json",
            &FakeEnvironment::value(4243, 1.0, Some(&format!("{BOOT}:old")), Some(PID_NS)),
        );
        environment.set_alive(4243, true);
        *lock_unpoisoned(&environment.replace_on_alive) = Some((
            4243,
            path.clone(),
            FakeEnvironment::value(4243, 2.0, Some(&format!("{BOOT}:100")), Some(PID_NS)),
        ));
        assert!(registry.list_live_sessions().is_empty());
        let replacement: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(replacement["startedAt"], 2.0);
        cleanup(&root);
    }

    #[test]
    fn host_liveness_and_current_linux_identity_follow_platform_contract() {
        let pid = i64::from(std::process::id());
        #[cfg(unix)]
        {
            assert!(is_pid_alive(pid));
            assert!(!is_pid_alive(0));
            assert!(!is_pid_alive(-1));
        }
        if cfg!(target_os = "linux") {
            assert!(read_local_boot_id().is_some());
            assert!(read_pid_namespace_id().is_some());
            assert!(read_proc_start_token(pid).is_some());
        } else {
            assert_eq!(read_local_boot_id(), None);
            assert_eq!(read_pid_namespace_id(), None);
            assert_eq!(read_proc_start_token(pid), None);
        }
    }
}
