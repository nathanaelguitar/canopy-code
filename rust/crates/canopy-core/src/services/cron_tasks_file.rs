//! Durable, per-project scheduled-task persistence.
//!
//! This mirrors `packages/core/src/services/cronTasksFile.ts`. Tasks are kept
//! under the user's runtime directory, keyed by the canonical project hash, so
//! scheduled automation never becomes project-shared repository content.
//! Unknown task and run fields are retained for forward compatibility.

use std::collections::{HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;
use uuid::Uuid;

use crate::storage::Storage;
use crate::utils::atomic_file_write::{AtomicWriteOptions, SymlinkPolicy, atomic_write_file};

pub const MAX_TASK_RUNS: usize = 20;
pub const MAX_CHANNEL_DELIVERY_NAME_LENGTH: usize = 2048;
pub const MAX_CHANNEL_DELIVERY_TARGET_ID_LENGTH: usize = 2048;
pub const TASKS_FILENAME: &str = "scheduled_tasks.json";
pub const CRON_TASKS_DISPLAY_PATH: &str = "~/.canopy/tmp/<project-hash>/scheduled_tasks.json";

const UPDATE_LOCK_RETRY: Duration = Duration::from_millis(15);
const UPDATE_LOCK_STALE: Duration = Duration::from_secs(2);
const UPDATE_LOCK_TIMEOUT: Duration = Duration::from_secs(3);

static LOCK_SEQUENCE: AtomicU64 = AtomicU64::new(0);
static UPDATE_MUTEXES: OnceLock<Mutex<HashMap<PathBuf, Arc<Mutex<()>>>>> = OnceLock::new();

/// One validated scheduled task, retaining properties written by newer
/// versions so a read/modify/write by this version does not erase them.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(transparent)]
pub struct CronTask(Map<String, Value>);

impl CronTask {
    pub fn from_value(value: Value) -> Result<Self, ()> {
        let Value::Object(object) = value else {
            return Err(());
        };
        if !is_valid_task(&object) {
            return Err(());
        }
        Ok(Self(object))
    }

    pub fn as_value(&self) -> Value {
        Value::Object(self.0.clone())
    }

    pub fn id(&self) -> &str {
        self.0.get("id").and_then(Value::as_str).unwrap_or_default()
    }

    pub fn cron(&self) -> &str {
        self.0
            .get("cron")
            .and_then(Value::as_str)
            .unwrap_or_default()
    }

    pub fn prompt(&self) -> &str {
        self.0
            .get("prompt")
            .and_then(Value::as_str)
            .unwrap_or_default()
    }

    pub fn recurring(&self) -> bool {
        self.0
            .get("recurring")
            .and_then(Value::as_bool)
            .unwrap_or(false)
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        self.0.get(key)
    }

    pub fn insert(&mut self, key: impl Into<String>, value: Value) -> Option<Value> {
        self.0.insert(key.into(), value)
    }

    pub fn remove(&mut self, key: &str) -> Option<Value> {
        self.0.remove(key)
    }

    pub fn has_legacy_condition(&self) -> bool {
        self.0
            .get("condition")
            .and_then(Value::as_str)
            .is_some_and(|condition| !condition.is_empty())
    }

    pub fn has_legacy_run_mode(&self) -> bool {
        self.0.get("runMode").and_then(Value::as_str) == Some("isolated")
    }
}

/// Truncate a run-history ring to the newest twenty records. A foreign or
/// absent value is treated as an empty history, matching the source helper.
pub fn append_cron_run(runs: Option<&Value>, entry: Value) -> Vec<Value> {
    let mut next = runs.and_then(Value::as_array).cloned().unwrap_or_default();
    next.push(entry);
    if next.len() > MAX_TASK_RUNS {
        next.drain(..next.len() - MAX_TASK_RUNS);
    }
    next
}

/// Create an eight-character base-36 task ID. As in the TypeScript source,
/// uniqueness within one small task file is the requirement, not secrecy.
pub fn generate_cron_task_id() -> String {
    let mut value = Uuid::new_v4().as_u128() % 2_821_109_907_456; // 36^8
    let alphabet = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let mut output = [b'a'; 8];
    for digit in output.iter_mut().rev() {
        *digit = alphabet[(value % 36) as usize];
        value /= 36;
    }
    String::from_utf8(output.to_vec()).expect("base-36 alphabet is valid UTF-8")
}

/// Path and persistence API for one project's durable scheduled tasks.
#[derive(Clone, Debug)]
pub struct CronTasksStore {
    file_path: PathBuf,
}

impl CronTasksStore {
    pub fn new(project_root: impl AsRef<Path>) -> Self {
        let storage = Storage::new(project_root.as_ref());
        Self {
            file_path: storage.get_project_temp_dir().join(TASKS_FILENAME),
        }
    }

    /// Construct a store at an explicit file path, useful to native hosts that
    /// already resolve the Canopy runtime directory.
    pub fn at_path(file_path: impl Into<PathBuf>) -> Self {
        Self {
            file_path: file_path.into(),
        }
    }

    pub fn file_path(&self) -> &Path {
        &self.file_path
    }

    pub fn read(&self) -> Result<Vec<CronTask>, CronTasksError> {
        let raw = match fs::read(&self.file_path) {
            Ok(raw) => raw,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error.into()),
        };
        let parsed: Value =
            serde_json::from_slice(&raw).map_err(|_| CronTasksError::MalformedJson {
                path: self.file_path.clone(),
            })?;
        let Value::Array(entries) = parsed else {
            return Err(CronTasksError::ExpectedArray {
                path: self.file_path.clone(),
            });
        };
        entries
            .into_iter()
            .enumerate()
            .map(|(index, entry)| {
                CronTask::from_value(entry).map_err(|()| CronTasksError::InvalidTask {
                    index,
                    path: self.file_path.clone(),
                })
            })
            .collect()
    }

    pub fn write(&self, tasks: &[CronTask]) -> Result<(), CronTasksError> {
        for (index, task) in tasks.iter().enumerate() {
            if !is_valid_task(&task.0) {
                return Err(CronTasksError::InvalidTask {
                    index,
                    path: self.file_path.clone(),
                });
            }
        }
        let parent = self.file_path.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent)?;
        let bytes = serde_json::to_vec_pretty(tasks)?;
        let options = AtomicWriteOptions {
            symlink_policy: SymlinkPolicy::NoFollow,
            ..AtomicWriteOptions::default()
        };
        atomic_write_file(&self.file_path, &bytes, &options)?;
        Ok(())
    }

    /// Run a serialized read/modify/write cycle. Return `true` from the
    /// mutation closure only when the file should be replaced.
    pub fn update<F>(&self, mutate: F) -> Result<(), CronTasksError>
    where
        F: FnOnce(&mut Vec<CronTask>) -> Result<bool, CronTasksError>,
    {
        let mutex = update_mutex(&self.file_path);
        let _process_guard = mutex
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _file_lock = UpdateFileLock::acquire(&self.file_path)?;
        let mut tasks = self.read()?;
        if mutate(&mut tasks)? {
            self.write(&tasks)?;
        }
        Ok(())
    }

    pub fn add(&self, task: CronTask) -> Result<(), CronTasksError> {
        self.update(move |tasks| {
            tasks.push(task);
            Ok(true)
        })
    }

    /// Remove matching task IDs, returning the number removed. A miss is
    /// side-effect free and does not create the runtime directory or lock.
    pub fn remove_ids(&self, ids: &[String]) -> Result<usize, CronTasksError> {
        let wanted: HashSet<&str> = ids.iter().map(String::as_str).collect();
        if !self.read()?.iter().any(|task| wanted.contains(task.id())) {
            return Ok(0);
        }
        let mut removed = 0;
        self.update(|tasks| {
            let old_len = tasks.len();
            tasks.retain(|task| !wanted.contains(task.id()));
            removed = old_len - tasks.len();
            Ok(removed != 0)
        })?;
        Ok(removed)
    }
}

#[derive(Debug, Error)]
pub enum CronTasksError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),
    #[error(
        "Malformed JSON in {path} — fix or delete the file; refusing to treat it as an empty schedule."
    )]
    MalformedJson { path: PathBuf },
    #[error(
        "Expected a JSON array in {path} — fix or delete the file; refusing to treat it as an empty schedule."
    )]
    ExpectedArray { path: PathBuf },
    #[error(
        "Invalid task entry at index {index} in {path} — fix or delete the entry; refusing to drop it from the schedule."
    )]
    InvalidTask { index: usize, path: PathBuf },
    #[error("Timed out waiting for scheduled-tasks lock ({0})")]
    LockTimeout(PathBuf),
}

fn is_valid_task(object: &Map<String, Value>) -> bool {
    object.get("id").and_then(Value::as_str).is_some()
        && object.get("cron").and_then(Value::as_str).is_some()
        && object.get("prompt").and_then(Value::as_str).is_some()
        && object.get("recurring").and_then(Value::as_bool).is_some()
        && object.get("createdAt").is_some_and(is_finite_number)
        && object
            .get("lastFiredAt")
            .is_some_and(|value| value.is_null() || is_finite_number(value))
        && optional_string(object, "name")
        && optional_bool(object, "enabled")
        && optional_bool(object, "disabledByArchive")
        && object.get("sessionId").is_none_or(|value| {
            value
                .as_str()
                .is_some_and(|session_id| !session_id.is_empty())
        })
        && object.get("delivery").is_none_or(is_valid_delivery)
        && object.get("runs").is_none_or(is_valid_runs)
}

fn is_finite_number(value: &Value) -> bool {
    value.as_f64().is_some_and(f64::is_finite)
}

fn optional_string(object: &Map<String, Value>, key: &str) -> bool {
    object.get(key).is_none_or(Value::is_string)
}

fn optional_bool(object: &Map<String, Value>, key: &str) -> bool {
    object.get(key).is_none_or(Value::is_boolean)
}

fn is_valid_runs(value: &Value) -> bool {
    let Some(runs) = value.as_array() else {
        return false;
    };
    runs.iter().all(|entry| {
        let Some(run) = entry.as_object() else {
            return false;
        };
        run.get("at").is_some_and(is_finite_number)
            && optional_string(run, "kind")
            && optional_string(run, "sessionId")
            && optional_bool(run, "withheld")
    })
}

fn is_valid_delivery(value: &Value) -> bool {
    let Some(delivery) = value.as_object() else {
        return false;
    };
    if delivery.len() != 2 || delivery.get("kind").and_then(Value::as_str) != Some("channel") {
        return false;
    }
    let Some(target) = delivery.get("target").and_then(Value::as_object) else {
        return false;
    };
    if target.len() != 3 {
        return false;
    }
    let Some(channel_name) = target.get("channelName").and_then(Value::as_str) else {
        return false;
    };
    let Some(target_id) = target.get("id").and_then(Value::as_str) else {
        return false;
    };
    channel_name.trim().len() > 0
        && channel_name.encode_utf16().count() <= MAX_CHANNEL_DELIVERY_NAME_LENGTH
        && target_id.trim().len() > 0
        && target_id.encode_utf16().count() <= MAX_CHANNEL_DELIVERY_TARGET_ID_LENGTH
        && matches!(
            target.get("type").and_then(Value::as_str),
            Some("user" | "chat")
        )
}

fn update_mutex(path: &Path) -> Arc<Mutex<()>> {
    let registry = UPDATE_MUTEXES.get_or_init(|| Mutex::new(HashMap::new()));
    let mut registry = registry
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    registry
        .entry(path.to_path_buf())
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

struct UpdateFileLock {
    path: PathBuf,
    marker: Vec<u8>,
}

impl UpdateFileLock {
    fn acquire(tasks_path: &Path) -> Result<Self, CronTasksError> {
        let lock_path = PathBuf::from(format!("{}.lock", tasks_path.display()));
        if let Some(parent) = lock_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let deadline = SystemTime::now() + UPDATE_LOCK_TIMEOUT;
        loop {
            let sequence = LOCK_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let marker = format!("{}:{sequence}", std::process::id()).into_bytes();
            match OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&lock_path)
            {
                Ok(mut lock_file) => {
                    lock_file.write_all(&marker)?;
                    lock_file.sync_all()?;
                    return Ok(Self {
                        path: lock_path,
                        marker,
                    });
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error.into()),
            }

            if SystemTime::now() > deadline {
                return Err(CronTasksError::LockTimeout(lock_path));
            }

            match fs::metadata(&lock_path) {
                Ok(metadata) if is_stale(&metadata) => {
                    let stale_path = PathBuf::from(format!(
                        "{}.stale.{}.{}",
                        lock_path.display(),
                        std::process::id(),
                        sequence
                    ));
                    if fs::rename(&lock_path, &stale_path).is_ok() {
                        let moved_is_fresh = fs::metadata(&stale_path)
                            .map(|moved| !is_stale(&moved))
                            .unwrap_or(false);
                        if moved_is_fresh {
                            let _ = fs::hard_link(&stale_path, &lock_path);
                        }
                        let _ = fs::remove_file(stale_path);
                        continue;
                    }
                }
                Ok(_) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(_) => {}
            }
            thread::sleep(UPDATE_LOCK_RETRY);
        }
    }
}

impl Drop for UpdateFileLock {
    fn drop(&mut self) {
        if fs::read(&self.path).is_ok_and(|current| current == self.marker) {
            let _ = fs::remove_file(&self.path);
        }
    }
}

fn is_stale(metadata: &fs::Metadata) -> bool {
    metadata
        .modified()
        .ok()
        .and_then(|modified| SystemTime::now().duration_since(modified).ok())
        .is_some_and(|age| age > UPDATE_LOCK_STALE)
}

/// Validate and wrap a task record generated by a native UI or API.
pub fn cron_task_from_value(value: Value) -> Result<CronTask, CronTasksError> {
    CronTask::from_value(value).map_err(|()| CronTasksError::InvalidTask {
        index: 0,
        path: PathBuf::from("<input>"),
    })
}

/// Serialize a task to a JSON object for daemon/API boundaries.
pub fn cron_task_to_value(task: &CronTask) -> Value {
    task.as_value()
}

/// Resolve the scheduled-tasks file path for an explicit runtime root.
pub fn cron_tasks_path(runtime_base_dir: &Path, project_root: &Path) -> PathBuf {
    runtime_base_dir
        .join("tmp")
        .join(crate::session_paths::get_project_hash(project_root))
        .join(TASKS_FILENAME)
}

/// Open the explicit path form while retaining the same validation and safety
/// rules as [`CronTasksStore::new`].
pub fn cron_tasks_store_at_runtime(runtime_base_dir: &Path, project_root: &Path) -> CronTasksStore {
    CronTasksStore::at_path(cron_tasks_path(runtime_base_dir, project_root))
}

/// A valid delivery object as it appears in the persisted JSON contract.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CronTaskDelivery {
    pub kind: String,
    pub target: CronTaskDeliveryTarget,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CronTaskDeliveryTarget {
    pub channel_name: String,
    #[serde(rename = "type")]
    pub target_type: String,
    pub id: String,
}

impl TryFrom<CronTaskDelivery> for Value {
    type Error = CronTasksError;

    fn try_from(delivery: CronTaskDelivery) -> Result<Self, Self::Error> {
        let value = serde_json::to_value(delivery)?;
        if is_valid_delivery(&value) {
            Ok(value)
        } else {
            Err(CronTasksError::InvalidTask {
                index: 0,
                path: PathBuf::from("<input>"),
            })
        }
    }
}

/// Build a validated run-history item while preserving forward-compatible
/// fields supplied by callers.
pub fn cron_run_from_value(value: Value) -> Result<Value, CronTasksError> {
    let valid = value.as_object().is_some_and(|run| {
        run.get("at").is_some_and(is_finite_number)
            && optional_string(run, "kind")
            && optional_string(run, "sessionId")
            && optional_bool(run, "withheld")
    });
    if valid {
        Ok(value)
    } else {
        Err(CronTasksError::InvalidTask {
            index: 0,
            path: PathBuf::from("<input>"),
        })
    }
}
