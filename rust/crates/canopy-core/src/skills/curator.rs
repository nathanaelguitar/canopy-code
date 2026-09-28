//! Lifecycle management for project auto-skills.
//!
//! Port of `packages/core/src/skills/skill-curator.ts`. The filesystem and
//! lock operations sit behind [`AutoSkillCuratorRuntime`] so the runtime can
//! supply platform-specific locking and durable writes without changing the
//! state machine.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::Metadata;
use std::io;
use std::path::{Component, Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::io::AsyncReadExt;

use super::paths::{SKILL_FILE_NAME, get_archived_skills_root, get_project_skills_root};
use super::types::{SkillConfig, SkillLevel, validate_skill_name};

pub const AUTO_SKILL_CURATOR_INTERVAL_MS: i64 = 7 * 24 * 60 * 60 * 1000;
pub const AUTO_SKILL_STALE_AFTER_MS: i64 = 30 * 24 * 60 * 60 * 1000;
pub const AUTO_SKILL_ARCHIVE_AFTER_MS: i64 = 90 * 24 * 60 * 60 * 1000;
pub const MAX_STATE_FILE_BYTES: u64 = 1024 * 1024;
pub const MAX_MANIFEST_BYTES: u64 = 4 * 1024 * 1024;

const AUTO_SKILL_PREFIX: &str = "auto-skill-";
const CURATOR_STATE_VERSION: u8 = 1;
const CURATOR_STATE_FILE: &str = "skill-curator.json";
const CURATOR_LOCK_FILE: &str = "skill-curator.lock";
const LOCK_STALE_AFTER: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AutoSkillState {
    Active,
    Stale,
    Archived,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AutoSkillCuratorEntry {
    pub directory_name: String,
    pub skill_name: String,
    pub state: AutoSkillState,
    pub last_activity_at: String,
    pub use_count: u64,
    pub pinned: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AutoSkillCuratorStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_run_at: Option<String>,
    pub active: Vec<AutoSkillCuratorEntry>,
    pub stale: Vec<AutoSkillCuratorEntry>,
    pub archived: Vec<AutoSkillCuratorEntry>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AutoSkillCuratorRunResult {
    pub dry_run: bool,
    pub checked: usize,
    pub seeded: Vec<String>,
    pub marked_stale: Vec<String>,
    pub reactivated: Vec<String>,
    pub archived: Vec<String>,
    pub skipped_collisions: Vec<String>,
    pub skipped_errors: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum AutoSkillCuratorAutomaticResult {
    Seeded { checked: usize },
    NotDue,
    Ran { result: AutoSkillCuratorRunResult },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CuratorDirectoryEntry {
    pub name: String,
    pub is_directory: bool,
}

/// Host operations used by the curator.
///
/// Implementations must make `atomic_write` replace a file atomically, reject
/// symlink state-file targets, and make `read_regular_file` open without
/// following the final path component. `acquire_lock` returns an owned guard
/// whose lifetime covers the state transaction.
#[allow(async_fn_in_trait)]
pub trait AutoSkillCuratorRuntime: Send + Sync {
    type LockGuard: Send;

    async fn acquire_lock(&self, lock_path: &Path) -> io::Result<Self::LockGuard>;
    async fn create_dir_all(&self, path: &Path) -> io::Result<()>;
    async fn symlink_metadata(&self, path: &Path) -> io::Result<Metadata>;
    async fn read_dir(&self, path: &Path) -> io::Result<Vec<CuratorDirectoryEntry>>;
    async fn read_regular_file(
        &self,
        path: &Path,
        max_bytes: u64,
    ) -> io::Result<(Vec<u8>, Metadata)>;
    async fn rename(&self, source: &Path, destination: &Path) -> io::Result<()>;
    async fn atomic_write(&self, path: &Path, bytes: &[u8], mode: u32) -> io::Result<()>;
}

/// Default filesystem implementation. It uses a heartbeat lock directory,
/// `O_NOFOLLOW`/`O_NONBLOCK` reads on Unix, bounded reads, and same-directory
/// temporary-file replacement for state writes.
#[derive(Clone, Copy, Debug, Default)]
pub struct FsAutoSkillCuratorRuntime;

pub struct FsCuratorLockGuard {
    lock_dir: PathBuf,
    heartbeat: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for FsCuratorLockGuard {
    fn drop(&mut self) {
        if let Some(task) = self.heartbeat.take() {
            task.abort();
        }
        // This is only cleanup of the uniquely-owned lock directory. It is
        // intentionally synchronous because Drop cannot await.
        let _ = std::fs::remove_dir_all(&self.lock_dir);
    }
}

impl AutoSkillCuratorRuntime for FsAutoSkillCuratorRuntime {
    type LockGuard = FsCuratorLockGuard;

    async fn acquire_lock(&self, lock_path: &Path) -> io::Result<Self::LockGuard> {
        let lock_dir = PathBuf::from(format!("{}.lock", lock_path.display()));
        let mut delay_ms = 25_u64;
        for attempt in 0..9 {
            match tokio::fs::create_dir(&lock_dir).await {
                Ok(()) => {
                    let heartbeat_path = lock_dir.join("heartbeat");
                    let heartbeat = tokio::spawn(async move {
                        loop {
                            tokio::time::sleep(Duration::from_secs(2)).await;
                            let _ = tokio::fs::write(&heartbeat_path, b"alive").await;
                        }
                    });
                    return Ok(FsCuratorLockGuard {
                        lock_dir,
                        heartbeat: Some(heartbeat),
                    });
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    if lock_is_stale(&lock_dir).await {
                        let metadata = tokio::fs::symlink_metadata(&lock_dir).await;
                        if metadata.is_ok_and(|metadata| {
                            metadata.is_dir() && !metadata.file_type().is_symlink()
                        }) {
                            let _ = tokio::fs::remove_dir_all(&lock_dir).await;
                            continue;
                        }
                    }
                    if attempt == 8 {
                        return Err(io::Error::new(
                            io::ErrorKind::WouldBlock,
                            format!("timed out waiting for curator lock {}", lock_dir.display()),
                        ));
                    }
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    delay_ms = (delay_ms * 2).min(500);
                }
                Err(error) => return Err(error),
            }
        }
        Err(io::Error::other("curator lock retry loop exhausted"))
    }

    async fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        tokio::fs::create_dir_all(path).await
    }

    async fn symlink_metadata(&self, path: &Path) -> io::Result<Metadata> {
        tokio::fs::symlink_metadata(path).await
    }

    async fn read_dir(&self, path: &Path) -> io::Result<Vec<CuratorDirectoryEntry>> {
        let mut dir = tokio::fs::read_dir(path).await?;
        let mut entries = Vec::new();
        while let Some(entry) = dir.next_entry().await? {
            let file_type = entry.file_type().await?;
            entries.push(CuratorDirectoryEntry {
                name: entry.file_name().to_string_lossy().into_owned(),
                is_directory: file_type.is_dir(),
            });
        }
        Ok(entries)
    }

    async fn read_regular_file(
        &self,
        path: &Path,
        max_bytes: u64,
    ) -> io::Result<(Vec<u8>, Metadata)> {
        let options = no_follow_open_options();
        let file = options.open(path).await?;
        let metadata = file.metadata().await?;
        if !metadata.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("refusing non-regular file {}", path.display()),
            ));
        }
        if metadata.len() > max_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("file too large: {}", path.display()),
            ));
        }
        let mut bytes = Vec::with_capacity(metadata.len().min(max_bytes) as usize);
        file.take(max_bytes.saturating_add(1))
            .read_to_end(&mut bytes)
            .await?;
        if bytes.len() as u64 > max_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("file too large: {}", path.display()),
            ));
        }
        Ok((bytes, metadata))
    }

    async fn rename(&self, source: &Path, destination: &Path) -> io::Result<()> {
        tokio::fs::rename(source, destination).await
    }

    async fn atomic_write(&self, path: &Path, bytes: &[u8], mode: u32) -> io::Result<()> {
        match tokio::fs::symlink_metadata(path).await {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("refusing unsafe state path {}", path.display()),
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        let parent = path.parent().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "state path has no parent")
        })?;
        let sequence = TEMP_FILE_SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let temp_path = parent.join(format!(
            ".{}.{}.{}.tmp",
            path.file_name().unwrap_or_default().to_string_lossy(),
            std::process::id(),
            sequence
        ));
        let mut options = tokio::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            options
                .mode(mode)
                .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
        }
        let result = async {
            let mut file = options.open(&temp_path).await?;
            tokio::io::AsyncWriteExt::write_all(&mut file, bytes).await?;
            file.sync_all().await?;
            drop(file);
            tokio::fs::rename(&temp_path, path).await?;
            Ok::<(), io::Error>(())
        }
        .await;
        if result.is_err() {
            let _ = tokio::fs::remove_file(&temp_path).await;
        }
        result
    }
}

static TEMP_FILE_SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[cfg(unix)]
fn no_follow_open_options() -> tokio::fs::OpenOptions {
    let mut options = tokio::fs::OpenOptions::new();
    options
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    options
}

#[cfg(not(unix))]
fn no_follow_open_options() -> tokio::fs::OpenOptions {
    let mut options = tokio::fs::OpenOptions::new();
    options.read(true);
    options
}

async fn lock_is_stale(path: &Path) -> bool {
    let heartbeat = path.join("heartbeat");
    let metadata = tokio::fs::metadata(&heartbeat)
        .await
        .or_else(|_| std::fs::metadata(path))
        .ok();
    metadata
        .and_then(|metadata| metadata.modified().ok())
        .and_then(|modified| SystemTime::now().duration_since(modified).ok())
        .is_some_and(|age| age > LOCK_STALE_AFTER)
}

#[derive(Debug, Error)]
pub enum CuratorError {
    #[error("{0}")]
    Message(String),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("Rollback failed: {rollback_message}")]
    Rollback {
        rollback_message: String,
        #[source]
        cause: Box<CuratorError>,
    },
}

type CuratorResult<T> = Result<T, CuratorError>;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AutoSkillRecord {
    skill_name: String,
    first_seen_at: String,
    last_activity_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_used_at: Option<String>,
    use_count: u64,
    state: AutoSkillState,
    #[serde(default)]
    pinned: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    archived_at: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AutoSkillCuratorState {
    version: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_run_at: Option<String>,
    skills: BTreeMap<String, AutoSkillRecord>,
}

impl Default for AutoSkillCuratorState {
    fn default() -> Self {
        Self {
            version: CURATOR_STATE_VERSION,
            last_run_at: None,
            skills: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug)]
struct CuratorPaths {
    canopy_root: PathBuf,
    skills_root: PathBuf,
    archive_root: PathBuf,
    state_path: PathBuf,
    lock_path: PathBuf,
}

#[derive(Clone, Debug)]
struct ManagedAutoSkill {
    directory_name: String,
    skill_name: String,
    directory_path: PathBuf,
    manifest_path: PathBuf,
    modified_at: String,
}

fn curator_paths(project_root: &Path) -> CuratorPaths {
    let canopy_root = project_root.join(".canopy");
    CuratorPaths {
        skills_root: get_project_skills_root(project_root),
        archive_root: get_archived_skills_root(project_root),
        state_path: canopy_root.join(CURATOR_STATE_FILE),
        lock_path: canopy_root.join(CURATOR_LOCK_FILE),
        canopy_root,
    }
}

fn parse_timestamp(value: &str) -> Option<i64> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|date| date.timestamp_millis())
}

fn iso_timestamp(value: SystemTime) -> String {
    DateTime::<Utc>::from(value).to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn require_timestamp(value: &Value, field: &str) -> CuratorResult<String> {
    let Some(text) = value.as_str() else {
        return Err(CuratorError::Message(format!(
            "Invalid auto-skill curator state: {field} is invalid."
        )));
    };
    if parse_timestamp(text).is_none() {
        return Err(CuratorError::Message(format!(
            "Invalid auto-skill curator state: {field} is invalid."
        )));
    }
    Ok(text.to_owned())
}

fn parse_state(raw: Value, state_path: &Path) -> CuratorResult<AutoSkillCuratorState> {
    let invalid = || {
        CuratorError::Message(format!(
            "Invalid auto-skill curator state at {}.",
            state_path.display()
        ))
    };
    let Some(input) = raw.as_object() else {
        return Err(invalid());
    };
    if input.get("version").and_then(Value::as_u64) != Some(u64::from(CURATOR_STATE_VERSION)) {
        return Err(CuratorError::Message(format!(
            "Unsupported auto-skill curator state version at {}.",
            state_path.display()
        )));
    }
    let Some(raw_skills) = input.get("skills").and_then(Value::as_object) else {
        return Err(invalid());
    };
    let mut skills = BTreeMap::new();
    for (directory_name, raw_record) in raw_skills {
        let Some(record) = raw_record.as_object() else {
            return Err(CuratorError::Message(format!(
                "Invalid auto-skill curator record for {directory_name}."
            )));
        };
        let bad_record = || {
            CuratorError::Message(format!(
                "Invalid auto-skill curator record for {directory_name}."
            ))
        };
        let skill_name = record
            .get("skillName")
            .and_then(Value::as_str)
            .ok_or_else(bad_record)?
            .to_owned();
        let use_count = record
            .get("useCount")
            .and_then(Value::as_u64)
            .ok_or_else(bad_record)?;
        let pinned = match record.get("pinned") {
            None => false,
            Some(Value::Bool(value)) => *value,
            Some(_) => return Err(bad_record()),
        };
        let state = match record.get("state").and_then(Value::as_str) {
            Some("active") => AutoSkillState::Active,
            Some("stale") => AutoSkillState::Stale,
            Some("archived") => AutoSkillState::Archived,
            _ => return Err(bad_record()),
        };
        skills.insert(
            directory_name.clone(),
            AutoSkillRecord {
                skill_name,
                first_seen_at: require_timestamp(
                    record.get("firstSeenAt").unwrap_or(&Value::Null),
                    &format!("{directory_name}.firstSeenAt"),
                )?,
                last_activity_at: require_timestamp(
                    record.get("lastActivityAt").unwrap_or(&Value::Null),
                    &format!("{directory_name}.lastActivityAt"),
                )?,
                last_used_at: record
                    .get("lastUsedAt")
                    .filter(|value| !value.is_null())
                    .map(|value| require_timestamp(value, &format!("{directory_name}.lastUsedAt")))
                    .transpose()?,
                use_count,
                state,
                pinned,
                archived_at: record
                    .get("archivedAt")
                    .filter(|value| !value.is_null())
                    .map(|value| require_timestamp(value, &format!("{directory_name}.archivedAt")))
                    .transpose()?,
            },
        );
    }
    let last_run_at = input
        .get("lastRunAt")
        .filter(|value| !value.is_null())
        .map(|value| require_timestamp(value, "lastRunAt"))
        .transpose()?;
    Ok(AutoSkillCuratorState {
        version: CURATOR_STATE_VERSION,
        last_run_at,
        skills,
    })
}

async fn read_state<R: AutoSkillCuratorRuntime>(
    runtime: &R,
    state_path: &Path,
) -> CuratorResult<AutoSkillCuratorState> {
    let metadata = match runtime.symlink_metadata(state_path).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(AutoSkillCuratorState::default());
        }
        Err(error) => return Err(error.into()),
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(CuratorError::Message(format!(
            "Auto-skill curator refuses unsafe path {}.",
            state_path.display()
        )));
    }
    if metadata.len() > MAX_STATE_FILE_BYTES {
        return Err(CuratorError::Message(format!(
            "Invalid auto-skill curator state at {}.",
            state_path.display()
        )));
    }
    let (bytes, _) = match runtime
        .read_regular_file(state_path, MAX_STATE_FILE_BYTES)
        .await
    {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(AutoSkillCuratorState::default());
        }
        Err(error) => {
            return Err(CuratorError::Message(format!(
                "Auto-skill curator refuses unsafe path {}: {error}",
                state_path.display()
            )));
        }
    };
    let raw: Value = serde_json::from_slice(&bytes).map_err(|_| {
        CuratorError::Message(format!(
            "Invalid auto-skill curator state at {}.",
            state_path.display()
        ))
    })?;
    parse_state(raw, state_path)
}

async fn write_state<R: AutoSkillCuratorRuntime>(
    runtime: &R,
    state_path: &Path,
    state: &AutoSkillCuratorState,
) -> CuratorResult<()> {
    let bytes = serde_json::to_vec(state).map_err(|error| {
        CuratorError::Message(format!(
            "Could not encode auto-skill curator state: {error}"
        ))
    })?;
    runtime.atomic_write(state_path, &bytes, 0o600).await?;
    Ok(())
}

fn is_managed_directory_name(directory_name: &str) -> bool {
    directory_name.starts_with(AUTO_SKILL_PREFIX)
        && !directory_name.contains('/')
        && !directory_name.contains('\\')
        && !directory_name.is_empty()
        && directory_name.chars().all(|character| {
            character.is_alphanumeric() || matches!(character, '_' | ':' | '.' | '-')
        })
}

fn parse_auto_skill_name(content: &str) -> Option<String> {
    let normalized = content
        .strip_prefix('\u{feff}')
        .unwrap_or(content)
        .replace("\r\n", "\n")
        .replace('\r', "\n");
    let rest = normalized.strip_prefix("---")?;
    let rest = rest.trim_start_matches([' ', '\t']);
    let rest = rest.strip_prefix('\n')?;
    let mut search_from = 0;
    while let Some(offset) = rest.get(search_from..)?.find("\n---") {
        let delimiter_start = search_from + offset + 1;
        let after_delimiter = delimiter_start + 3;
        let trailing = rest.get(after_delimiter..)?;
        let trailing = trailing.trim_start_matches([' ', '\t']);
        if trailing.is_empty() || trailing.starts_with('\n') {
            let frontmatter_end = delimiter_start.saturating_sub(1);
            let frontmatter = rest.get(..frontmatter_end)?;
            let fields = super::skill_load::parse_yaml_frontmatter(frontmatter).ok()?;
            if fields.get("source").and_then(Value::as_str) != Some("auto-skill") {
                return None;
            }
            let name = fields.get("name")?.as_str()?.to_owned();
            validate_skill_name(&name).ok()?;
            return Some(name);
        }
        search_from = delimiter_start + 1;
    }
    None
}

async fn ensure_safe_directory<R: AutoSkillCuratorRuntime>(
    runtime: &R,
    directory: &Path,
) -> CuratorResult<()> {
    runtime.create_dir_all(directory).await?;
    let metadata = runtime.symlink_metadata(directory).await?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(CuratorError::Message(format!(
            "Auto-skill curator refuses unsafe path {}.",
            directory.display()
        )));
    }
    Ok(())
}

async fn ensure_safe_canopy_root<R: AutoSkillCuratorRuntime>(
    runtime: &R,
    paths: &CuratorPaths,
) -> CuratorResult<()> {
    ensure_safe_directory(runtime, &paths.canopy_root).await
}

async fn read_managed_skill<R: AutoSkillCuratorRuntime>(
    runtime: &R,
    root: &Path,
    directory_name: &str,
) -> Option<ManagedAutoSkill> {
    if !is_managed_directory_name(directory_name) {
        return None;
    }
    let directory_path = root.join(directory_name);
    let manifest_path = directory_path.join(SKILL_FILE_NAME);
    let directory_metadata = runtime.symlink_metadata(&directory_path).await.ok()?;
    if directory_metadata.file_type().is_symlink() || !directory_metadata.is_dir() {
        return None;
    }
    let (bytes, metadata) = runtime
        .read_regular_file(&manifest_path, MAX_MANIFEST_BYTES)
        .await
        .ok()?;
    let content = String::from_utf8(bytes).ok()?;
    let skill_name = parse_auto_skill_name(&content)?;
    let modified_at = iso_timestamp(metadata.modified().unwrap_or(UNIX_EPOCH));
    Some(ManagedAutoSkill {
        directory_name: directory_name.to_owned(),
        skill_name,
        directory_path,
        manifest_path,
        modified_at,
    })
}

async fn scan_managed_skills<R: AutoSkillCuratorRuntime>(
    runtime: &R,
    root: &Path,
) -> CuratorResult<Vec<ManagedAutoSkill>> {
    let root_metadata = match runtime.symlink_metadata(root).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    if !root_metadata.is_dir() || root_metadata.file_type().is_symlink() {
        return Err(CuratorError::Message(format!(
            "Auto-skill curator refuses unsafe path {}.",
            root.display()
        )));
    }
    let entries = match runtime.read_dir(root).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    let mut skills = Vec::new();
    for entry in entries {
        if !entry.is_directory || !is_managed_directory_name(&entry.name) {
            continue;
        }
        if let Some(skill) = read_managed_skill(runtime, root, &entry.name).await {
            skills.push(skill);
        }
    }
    // Rust's Unicode scalar ordering is stable, but differs from ICU
    // localeCompare for a few punctuation/case combinations.
    skills.sort_by(|left, right| left.directory_name.cmp(&right.directory_name));
    Ok(skills)
}

fn record_for_skill<'a>(
    state: &'a mut AutoSkillCuratorState,
    skill: &ManagedAutoSkill,
    first_seen_at: Option<&str>,
) -> &'a mut AutoSkillRecord {
    let record = state
        .skills
        .entry(skill.directory_name.clone())
        .or_insert_with(|| {
            let first_seen_at = first_seen_at.unwrap_or(&skill.modified_at).to_owned();
            AutoSkillRecord {
                skill_name: skill.skill_name.clone(),
                first_seen_at: first_seen_at.clone(),
                last_activity_at: first_seen_at,
                last_used_at: None,
                use_count: 0,
                state: AutoSkillState::Active,
                pinned: false,
                archived_at: None,
            }
        });
    record.skill_name.clone_from(&skill.skill_name);
    record
}

fn last_activity_ms(skill: &ManagedAutoSkill, record: &AutoSkillRecord, now_ms: i64) -> i64 {
    let mtime = parse_timestamp(&skill.modified_at).unwrap_or(0);
    [
        if mtime <= now_ms { mtime } else { 0 },
        parse_timestamp(&record.first_seen_at).unwrap_or(0),
        parse_timestamp(&record.last_activity_at).unwrap_or(0),
        record
            .last_used_at
            .as_deref()
            .and_then(parse_timestamp)
            .unwrap_or(0),
    ]
    .into_iter()
    .max()
    .unwrap_or(0)
}

fn entry_for(
    skill: &ManagedAutoSkill,
    record: &AutoSkillRecord,
    state: AutoSkillState,
    now_ms: i64,
) -> AutoSkillCuratorEntry {
    let activity = last_activity_ms(skill, record, now_ms).max(0) as u64;
    let activity = UNIX_EPOCH + Duration::from_millis(activity);
    AutoSkillCuratorEntry {
        directory_name: skill.directory_name.clone(),
        skill_name: skill.skill_name.clone(),
        state,
        last_activity_at: iso_timestamp(activity),
        use_count: record.use_count,
        pinned: record.pinned,
    }
}

fn empty_result(dry_run: bool, checked: usize) -> AutoSkillCuratorRunResult {
    AutoSkillCuratorRunResult {
        dry_run,
        checked,
        ..AutoSkillCuratorRunResult::default()
    }
}

async fn raw_directories<R: AutoSkillCuratorRuntime>(runtime: &R, root: &Path) -> BTreeSet<String> {
    runtime
        .read_dir(root)
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|entry| entry.is_directory)
        .map(|entry| entry.name)
        .collect()
}

async fn rollback_moves<R: AutoSkillCuratorRuntime>(
    runtime: &R,
    moved: &[(PathBuf, PathBuf)],
) -> Vec<String> {
    let mut errors = Vec::new();
    for (source, destination) in moved.iter().rev() {
        if let Err(error) = runtime.rename(destination, source).await {
            errors.push(error.to_string());
        }
    }
    errors
}

async fn run_locked<R: AutoSkillCuratorRuntime>(
    runtime: &R,
    paths: &CuratorPaths,
    mut state: AutoSkillCuratorState,
    now: DateTime<Utc>,
) -> CuratorResult<AutoSkillCuratorRunResult> {
    let skills = scan_managed_skills(runtime, &paths.skills_root).await?;
    let now_ms = now.timestamp_millis();
    let now_iso = now.to_rfc3339_opts(SecondsFormat::Millis, true);
    let mut result = empty_result(false, skills.len());
    let mut moved: Vec<(PathBuf, PathBuf)> = Vec::new();

    let attempt: CuratorResult<()> = async {
        for scanned in &skills {
            let existed = state.skills.contains_key(&scanned.directory_name);
            let record = record_for_skill(&mut state, scanned, Some(&now_iso));
            if !existed {
                result.seeded.push(scanned.directory_name.clone());
                continue;
            }
            if record.pinned {
                continue;
            }
            let inactivity_ms = now_ms - last_activity_ms(scanned, record, now_ms);
            if inactivity_ms >= AUTO_SKILL_ARCHIVE_AFTER_MS {
                let Some(current) =
                    read_managed_skill(runtime, &paths.skills_root, &scanned.directory_name).await
                else {
                    continue;
                };
                let current_inactivity_ms = now_ms - last_activity_ms(&current, record, now_ms);
                if current_inactivity_ms < AUTO_SKILL_ARCHIVE_AFTER_MS {
                    if current_inactivity_ms >= AUTO_SKILL_STALE_AFTER_MS {
                        if record.state != AutoSkillState::Stale {
                            record.state = AutoSkillState::Stale;
                            record.archived_at = None;
                            result.marked_stale.push(scanned.directory_name.clone());
                        }
                    } else if record.state != AutoSkillState::Active {
                        record.state = AutoSkillState::Active;
                        record.archived_at = None;
                        result.reactivated.push(scanned.directory_name.clone());
                    }
                    continue;
                }
                ensure_safe_directory(runtime, &paths.archive_root).await?;
                let destination = paths.archive_root.join(&scanned.directory_name);
                match runtime.symlink_metadata(&destination).await {
                    Ok(_) => {
                        result
                            .skipped_collisions
                            .push(scanned.directory_name.clone());
                        continue;
                    }
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
                if runtime
                    .rename(&current.directory_path, &destination)
                    .await
                    .is_err()
                {
                    result.skipped_errors.push(scanned.directory_name.clone());
                    continue;
                }
                moved.push((current.directory_path.clone(), destination));
                record.state = AutoSkillState::Archived;
                record.archived_at = Some(now_iso.clone());
                result.archived.push(scanned.directory_name.clone());
            } else if inactivity_ms >= AUTO_SKILL_STALE_AFTER_MS {
                if record.state != AutoSkillState::Stale {
                    record.state = AutoSkillState::Stale;
                    record.archived_at = None;
                    result.marked_stale.push(scanned.directory_name.clone());
                }
            } else if record.state != AutoSkillState::Active {
                record.state = AutoSkillState::Active;
                record.archived_at = None;
                result.reactivated.push(scanned.directory_name.clone());
            }
        }

        let live_dirs = raw_directories(runtime, &paths.skills_root).await;
        let archive_dirs = raw_directories(runtime, &paths.archive_root).await;
        state
            .skills
            .retain(|name, _| live_dirs.contains(name) || archive_dirs.contains(name));
        state.last_run_at = Some(now_iso);
        write_state(runtime, &paths.state_path, &state).await?;
        Ok(())
    }
    .await;

    if let Err(error) = attempt {
        let rollback_errors = rollback_moves(runtime, &moved).await;
        if !rollback_errors.is_empty() {
            return Err(CuratorError::Rollback {
                rollback_message: rollback_errors.join("; "),
                cause: Box::new(error),
            });
        }
        return Err(error);
    }
    Ok(result)
}

async fn preview_run<R: AutoSkillCuratorRuntime>(
    runtime: &R,
    project_root: &Path,
    now: DateTime<Utc>,
) -> CuratorResult<AutoSkillCuratorRunResult> {
    let paths = curator_paths(project_root);
    let mut state = read_state(runtime, &paths.state_path).await?;
    let skills = scan_managed_skills(runtime, &paths.skills_root).await?;
    let mut result = empty_result(true, skills.len());
    let now_ms = now.timestamp_millis();
    for skill in &skills {
        if !state.skills.contains_key(&skill.directory_name) {
            result.seeded.push(skill.directory_name.clone());
            continue;
        }
        let record = record_for_skill(&mut state, skill, None);
        if record.pinned {
            continue;
        }
        let inactivity_ms = now_ms - last_activity_ms(skill, record, now_ms);
        if inactivity_ms >= AUTO_SKILL_ARCHIVE_AFTER_MS {
            match runtime
                .symlink_metadata(&paths.archive_root.join(&skill.directory_name))
                .await
            {
                Ok(_) => result.skipped_collisions.push(skill.directory_name.clone()),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    result.archived.push(skill.directory_name.clone())
                }
                Err(error) => return Err(error.into()),
            }
        } else if inactivity_ms >= AUTO_SKILL_STALE_AFTER_MS
            && record.state != AutoSkillState::Stale
        {
            result.marked_stale.push(skill.directory_name.clone());
        } else if inactivity_ms < AUTO_SKILL_STALE_AFTER_MS
            && record.state != AutoSkillState::Active
        {
            result.reactivated.push(skill.directory_name.clone());
        }
    }
    Ok(result)
}

pub async fn run_auto_skill_curator<R: AutoSkillCuratorRuntime>(
    runtime: &R,
    project_root: impl AsRef<Path>,
    dry_run: bool,
    now: DateTime<Utc>,
) -> CuratorResult<AutoSkillCuratorRunResult> {
    let project_root = project_root.as_ref();
    if dry_run {
        return preview_run(runtime, project_root, now).await;
    }
    let paths = curator_paths(project_root);
    ensure_safe_canopy_root(runtime, &paths).await?;
    let _lock = runtime.acquire_lock(&paths.lock_path).await?;
    let state = read_state(runtime, &paths.state_path).await?;
    run_locked(runtime, &paths, state, now).await
}

pub async fn maybe_run_auto_skill_curator<R: AutoSkillCuratorRuntime>(
    runtime: &R,
    project_root: impl AsRef<Path>,
    now: DateTime<Utc>,
) -> CuratorResult<AutoSkillCuratorAutomaticResult> {
    let project_root = project_root.as_ref();
    let paths = curator_paths(project_root);
    let unlocked_state = read_state(runtime, &paths.state_path).await?;
    if let Some(last_run_at) = &unlocked_state.last_run_at {
        let last_run_ms = parse_timestamp(last_run_at).expect("validated timestamp");
        if now.timestamp_millis() - last_run_ms < AUTO_SKILL_CURATOR_INTERVAL_MS {
            return Ok(AutoSkillCuratorAutomaticResult::NotDue);
        }
    }
    ensure_safe_canopy_root(runtime, &paths).await?;
    let _lock = runtime.acquire_lock(&paths.lock_path).await?;
    let mut state = read_state(runtime, &paths.state_path).await?;
    if state.last_run_at.is_none() {
        let now_iso = now.to_rfc3339_opts(SecondsFormat::Millis, true);
        let skills = scan_managed_skills(runtime, &paths.skills_root).await?;
        if skills.is_empty() && state.skills.is_empty() {
            return Ok(AutoSkillCuratorAutomaticResult::Seeded { checked: 0 });
        }
        for skill in &skills {
            let existing = state.skills.get(&skill.directory_name).cloned();
            state.skills.insert(
                skill.directory_name.clone(),
                AutoSkillRecord {
                    skill_name: skill.skill_name.clone(),
                    first_seen_at: existing
                        .as_ref()
                        .map_or_else(|| now_iso.clone(), |record| record.first_seen_at.clone()),
                    last_activity_at: existing
                        .as_ref()
                        .map_or_else(|| now_iso.clone(), |record| record.last_activity_at.clone()),
                    use_count: existing.as_ref().map_or(0, |record| record.use_count),
                    state: AutoSkillState::Active,
                    pinned: existing.as_ref().is_some_and(|record| record.pinned),
                    last_used_at: existing.and_then(|record| record.last_used_at),
                    archived_at: None,
                },
            );
        }
        state.last_run_at = Some(now_iso);
        write_state(runtime, &paths.state_path, &state).await?;
        return Ok(AutoSkillCuratorAutomaticResult::Seeded {
            checked: skills.len(),
        });
    }
    let last_run_ms = parse_timestamp(state.last_run_at.as_deref().expect("checked"))
        .expect("validated timestamp");
    if now.timestamp_millis() - last_run_ms < AUTO_SKILL_CURATOR_INTERVAL_MS {
        return Ok(AutoSkillCuratorAutomaticResult::NotDue);
    }
    let result = run_locked(runtime, &paths, state, now).await?;
    Ok(AutoSkillCuratorAutomaticResult::Ran { result })
}

fn absolute_lexical(path: &Path) -> io::Result<PathBuf> {
    let path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                if !normalized.pop() && !normalized.has_root() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "path traverses above root",
                    ));
                }
            }
            component => normalized.push(component.as_os_str()),
        }
    }
    Ok(normalized)
}

pub async fn record_auto_skill_usage<R: AutoSkillCuratorRuntime>(
    runtime: &R,
    project_root: impl AsRef<Path>,
    skill: &SkillConfig,
    now: DateTime<Utc>,
) -> CuratorResult<bool> {
    if skill.level != SkillLevel::Project {
        return Ok(false);
    }
    let project_root = project_root.as_ref();
    let paths = curator_paths(project_root);
    let resolved_manifest = absolute_lexical(&skill.file_path)?;
    let directory_path = resolved_manifest.parent().ok_or_else(|| {
        CuratorError::Message("skill manifest has no parent directory".to_owned())
    })?;
    let resolved_skills_root = absolute_lexical(&paths.skills_root)?;
    if directory_path.parent() != Some(resolved_skills_root.as_path()) {
        return Ok(false);
    }
    let directory_name = directory_path
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();
    let Some(candidate) = read_managed_skill(runtime, &paths.skills_root, &directory_name).await
    else {
        return Ok(false);
    };
    if candidate.manifest_path != resolved_manifest {
        return Ok(false);
    }
    ensure_safe_canopy_root(runtime, &paths).await?;
    let _lock = runtime.acquire_lock(&paths.lock_path).await?;
    let Some(managed) = read_managed_skill(runtime, &paths.skills_root, &directory_name).await
    else {
        return Ok(false);
    };
    if managed.manifest_path != resolved_manifest {
        return Ok(false);
    }
    let mut state = read_state(runtime, &paths.state_path).await?;
    let now_iso = now.to_rfc3339_opts(SecondsFormat::Millis, true);
    let record = record_for_skill(&mut state, &managed, Some(&now_iso));
    record.last_activity_at = now_iso.clone();
    record.last_used_at = Some(now_iso);
    record.use_count = record.use_count.saturating_add(1);
    record.state = AutoSkillState::Active;
    record.archived_at = None;
    write_state(runtime, &paths.state_path, &state).await?;
    Ok(true)
}

pub async fn set_auto_skill_pinned<R: AutoSkillCuratorRuntime>(
    runtime: &R,
    project_root: impl AsRef<Path>,
    directory_name: &str,
    pinned: bool,
    now: DateTime<Utc>,
) -> CuratorResult<()> {
    if !is_managed_directory_name(directory_name) {
        let quoted = serde_json::to_string(directory_name).unwrap_or_else(|_| "\"\"".to_owned());
        return Err(CuratorError::Message(format!(
            "Managed auto-skill not found: {quoted}."
        )));
    }
    let paths = curator_paths(project_root.as_ref());
    ensure_safe_canopy_root(runtime, &paths).await?;
    let _lock = runtime.acquire_lock(&paths.lock_path).await?;
    let Some(skill) = read_managed_skill(runtime, &paths.skills_root, directory_name).await else {
        return Err(CuratorError::Message(format!(
            "Managed auto-skill not found: {directory_name}."
        )));
    };
    let mut state = read_state(runtime, &paths.state_path).await?;
    let record = record_for_skill(
        &mut state,
        &skill,
        Some(&now.to_rfc3339_opts(SecondsFormat::Millis, true)),
    );
    record.pinned = pinned;
    write_state(runtime, &paths.state_path, &state).await
}

pub async fn get_auto_skill_curator_status<R: AutoSkillCuratorRuntime>(
    runtime: &R,
    project_root: impl AsRef<Path>,
    now: DateTime<Utc>,
) -> CuratorResult<AutoSkillCuratorStatus> {
    let paths = curator_paths(project_root.as_ref());
    let state = read_state(runtime, &paths.state_path).await?;
    let live_skills = scan_managed_skills(runtime, &paths.skills_root).await?;
    let archived_skills = scan_managed_skills(runtime, &paths.archive_root).await?;
    let now_ms = now.timestamp_millis();
    let now_iso = now.to_rfc3339_opts(SecondsFormat::Millis, true);
    let mut status = AutoSkillCuratorStatus {
        last_run_at: state.last_run_at.clone(),
        ..AutoSkillCuratorStatus::default()
    };
    for skill in &live_skills {
        let mut state = state.clone();
        let record = record_for_skill(&mut state, skill, Some(&now_iso)).clone();
        let inactivity = now_ms - last_activity_ms(skill, &record, now_ms);
        let effective_state = if record.pinned {
            if record.state == AutoSkillState::Stale {
                AutoSkillState::Stale
            } else {
                AutoSkillState::Active
            }
        } else if inactivity >= AUTO_SKILL_STALE_AFTER_MS {
            AutoSkillState::Stale
        } else {
            AutoSkillState::Active
        };
        let entry = entry_for(skill, &record, effective_state, now_ms);
        match effective_state {
            AutoSkillState::Active => status.active.push(entry),
            AutoSkillState::Stale => status.stale.push(entry),
            AutoSkillState::Archived => status.archived.push(entry),
        }
    }
    let live_names: BTreeSet<_> = live_skills
        .iter()
        .map(|skill| skill.directory_name.as_str())
        .collect();
    for skill in &archived_skills {
        if live_names.contains(skill.directory_name.as_str()) {
            continue;
        }
        let mut state = state.clone();
        let record = record_for_skill(&mut state, skill, None).clone();
        status
            .archived
            .push(entry_for(skill, &record, AutoSkillState::Archived, now_ms));
    }
    Ok(status)
}

pub async fn restore_archived_auto_skill<R: AutoSkillCuratorRuntime>(
    runtime: &R,
    project_root: impl AsRef<Path>,
    directory_name: &str,
    now: DateTime<Utc>,
) -> CuratorResult<()> {
    let paths = curator_paths(project_root.as_ref());
    ensure_safe_canopy_root(runtime, &paths).await?;
    let _lock = runtime.acquire_lock(&paths.lock_path).await?;
    if !is_managed_directory_name(directory_name) {
        let quoted = serde_json::to_string(directory_name).unwrap_or_else(|_| "\"\"".to_owned());
        return Err(CuratorError::Message(format!(
            "Archived auto-skill not found: {quoted}."
        )));
    }
    let archive_root_metadata = match runtime.symlink_metadata(&paths.archive_root).await {
        Ok(metadata) => Some(metadata),
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };
    let Some(archive_root_metadata) = archive_root_metadata else {
        return Err(CuratorError::Message(format!(
            "Archived auto-skill not found: {directory_name}."
        )));
    };
    if !archive_root_metadata.is_dir() || archive_root_metadata.file_type().is_symlink() {
        return Err(CuratorError::Message(format!(
            "Auto-skill curator refuses unsafe path {}.",
            paths.archive_root.display()
        )));
    }
    let Some(archived) = read_managed_skill(runtime, &paths.archive_root, directory_name).await
    else {
        let exists = runtime
            .symlink_metadata(&paths.archive_root.join(directory_name))
            .await
            .is_ok();
        return Err(CuratorError::Message(if exists {
            format!("Archived auto-skill {directory_name} is not an eligible managed skill.")
        } else {
            format!("Archived auto-skill not found: {directory_name}.")
        }));
    };
    let destination = paths.skills_root.join(directory_name);
    match runtime.symlink_metadata(&destination).await {
        Ok(_) => {
            return Err(CuratorError::Message(format!(
                "Cannot restore {directory_name}: an active directory already exists."
            )));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    ensure_safe_directory(runtime, &paths.skills_root).await?;
    let mut state = read_state(runtime, &paths.state_path).await?;
    let now_iso = now.to_rfc3339_opts(SecondsFormat::Millis, true);
    let record = record_for_skill(&mut state, &archived, None);
    record.state = AutoSkillState::Active;
    record.last_activity_at = now_iso;
    record.archived_at = None;
    runtime
        .rename(&archived.directory_path, &destination)
        .await?;
    if let Err(error) = write_state(runtime, &paths.state_path, &state).await {
        if let Err(rollback_error) = runtime.rename(&destination, &archived.directory_path).await {
            return Err(CuratorError::Rollback {
                rollback_message: rollback_error.to_string(),
                cause: Box::new(error),
            });
        }
        return Err(error);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[derive(Clone, Default)]
    struct TestRuntime {
        fail_next_write: std::sync::Arc<AtomicBool>,
    }

    impl AutoSkillCuratorRuntime for TestRuntime {
        type LockGuard = FsCuratorLockGuard;

        async fn acquire_lock(&self, path: &Path) -> io::Result<Self::LockGuard> {
            FsAutoSkillCuratorRuntime.acquire_lock(path).await
        }
        async fn create_dir_all(&self, path: &Path) -> io::Result<()> {
            FsAutoSkillCuratorRuntime.create_dir_all(path).await
        }
        async fn symlink_metadata(&self, path: &Path) -> io::Result<Metadata> {
            FsAutoSkillCuratorRuntime.symlink_metadata(path).await
        }
        async fn read_dir(&self, path: &Path) -> io::Result<Vec<CuratorDirectoryEntry>> {
            FsAutoSkillCuratorRuntime.read_dir(path).await
        }
        async fn read_regular_file(
            &self,
            path: &Path,
            max: u64,
        ) -> io::Result<(Vec<u8>, Metadata)> {
            FsAutoSkillCuratorRuntime.read_regular_file(path, max).await
        }
        async fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
            FsAutoSkillCuratorRuntime.rename(from, to).await
        }
        async fn atomic_write(&self, path: &Path, bytes: &[u8], mode: u32) -> io::Result<()> {
            if self.fail_next_write.swap(false, Ordering::SeqCst) {
                return Err(io::Error::other("simulated persistence failure"));
            }
            FsAutoSkillCuratorRuntime
                .atomic_write(path, bytes, mode)
                .await
        }
    }

    fn now() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-07-27T00:00:00.000Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn marker_uses_shared_yaml_for_quoted_colon_and_block_scalars() {
        assert_eq!(
            parse_auto_skill_name(
                "---\nname: \"quoted:skill\"\nsource: 'auto-skill' # curator marker\n---\n"
            )
            .as_deref(),
            Some("quoted:skill")
        );
        assert_eq!(
            parse_auto_skill_name("---\nname: team:skill\nsource: auto-skill\n---\n").as_deref(),
            Some("team:skill")
        );
        assert_eq!(
            parse_auto_skill_name("---\nname: team: skill\nsource: auto-skill\n---\n"),
            None
        );
        assert_eq!(
            parse_auto_skill_name("---\nsource: |-\n  auto-skill\nname: >-\n  block:skill\n---\n")
                .as_deref(),
            Some("block:skill")
        );
    }

    #[test]
    fn marker_preserves_yaml_fallback_for_malformed_frontmatter() {
        assert_eq!(
            parse_auto_skill_name(
                "---\nname: legacy-skill\nsource: auto-skill\nextra: {malformed\n---\n"
            )
            .as_deref(),
            Some("legacy-skill")
        );
        assert_eq!(
            parse_auto_skill_name("---\nname: legacy-skill\nsource: auto-skill\n"),
            None
        );
        assert_eq!(
            parse_auto_skill_name("---\nname: legacy-skill\nsource: learned\n---\n"),
            None
        );
    }

    fn temp_root(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "canopy-curator-{label}-{}-{}",
            std::process::id(),
            TEMP_FILE_SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ))
    }

    async fn write_skill(
        root: &Path,
        name: &str,
        source: &str,
        modified: DateTime<Utc>,
    ) -> PathBuf {
        let directory = root.join(".canopy/skills").join(name);
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let manifest = directory.join(SKILL_FILE_NAME);
        tokio::fs::write(
            &manifest,
            format!(
                "---\nname: {}\ndescription: test skill\nsource: {source}\n---\n\n# Skill\n",
                name.trim_start_matches(AUTO_SKILL_PREFIX)
            ),
        )
        .await
        .unwrap();
        let modified: SystemTime = modified.into();
        std::fs::File::open(&manifest)
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(modified))
            .unwrap();
        manifest
    }

    async fn seed_old_skill(runtime: &TestRuntime, root: &Path, directory_name: &str) -> PathBuf {
        let old = now() - chrono::Duration::days(100);
        let manifest = write_skill(root, directory_name, "auto-skill", old).await;
        let path = curator_paths(root);
        ensure_safe_canopy_root(runtime, &path).await.unwrap();
        let _lock = runtime.acquire_lock(&path.lock_path).await.unwrap();
        let skill = read_managed_skill(runtime, &path.skills_root, directory_name)
            .await
            .unwrap();
        let mut state = read_state(runtime, &path.state_path).await.unwrap();
        let iso = old.to_rfc3339_opts(SecondsFormat::Millis, true);
        let record = record_for_skill(&mut state, &skill, Some(&iso));
        record.last_activity_at = iso;
        write_state(runtime, &path.state_path, &state)
            .await
            .unwrap();
        manifest
    }

    #[tokio::test]
    async fn manages_only_doubly_marked_safe_directory_names() {
        let runtime = TestRuntime::default();
        let root = temp_root("managed");
        let old = now() - chrono::Duration::days(100);
        write_skill(&root, "auto-skill-managed", "auto-skill", old).await;
        write_skill(&root, "auto-skill-learned", "learned", old).await;
        write_skill(&root, "hand-authored", "auto-skill", old).await;
        let result = run_auto_skill_curator(&runtime, &root, true, now())
            .await
            .unwrap();
        assert_eq!(result.checked, 1);
        assert_eq!(result.seeded, ["auto-skill-managed"]);
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn automatic_first_observation_seeds_then_runs_after_interval() {
        let runtime = TestRuntime::default();
        let root = temp_root("automatic");
        let old = now() - chrono::Duration::days(200);
        write_skill(&root, "auto-skill-aged", "auto-skill", old).await;
        assert_eq!(
            maybe_run_auto_skill_curator(&runtime, &root, now())
                .await
                .unwrap(),
            AutoSkillCuratorAutomaticResult::Seeded { checked: 1 }
        );
        let not_due =
            maybe_run_auto_skill_curator(&runtime, &root, now() + chrono::Duration::days(6))
                .await
                .unwrap();
        assert_eq!(not_due, AutoSkillCuratorAutomaticResult::NotDue);
        let ran = maybe_run_auto_skill_curator(&runtime, &root, now() + chrono::Duration::days(91))
            .await
            .unwrap();
        assert!(
            matches!(ran, AutoSkillCuratorAutomaticResult::Ran { result } if result.archived == ["auto-skill-aged"])
        );
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn preview_does_not_write_or_create_archive_root() {
        let runtime = TestRuntime::default();
        let root = temp_root("preview");
        let old = now() - chrono::Duration::days(100);
        write_skill(&root, "auto-skill-old", "auto-skill", old).await;
        let result = run_auto_skill_curator(&runtime, &root, true, now())
            .await
            .unwrap();
        assert!(result.dry_run);
        assert_eq!(result.seeded, ["auto-skill-old"]);
        assert!(!root.join(".canopy/skill-curator.json").exists());
        assert!(
            !root
                .join(super::super::paths::ARCHIVED_SKILLS_RELATIVE_DIR)
                .exists()
        );
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn run_stales_then_archives_and_restore_preserves_skill_files() {
        let runtime = TestRuntime::default();
        let root = temp_root("lifecycle");
        let old = now() - chrono::Duration::days(100);
        let manifest = write_skill(&root, "auto-skill-old", "auto-skill", old).await;
        let support = manifest.parent().unwrap().join("references/note.md");
        tokio::fs::create_dir_all(support.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&support, b"keep").await.unwrap();
        let _manifest = seed_old_skill(&runtime, &root, "auto-skill-old").await;
        let result = run_auto_skill_curator(&runtime, &root, false, now())
            .await
            .unwrap();
        assert_eq!(result.archived, ["auto-skill-old"]);
        assert_eq!(
            tokio::fs::read(root.join(".canopy/archived-skills/auto-skill-old/references/note.md"))
                .await
                .unwrap(),
            b"keep"
        );
        restore_archived_auto_skill(&runtime, &root, "auto-skill-old", now())
            .await
            .unwrap();
        assert_eq!(tokio::fs::read(&support).await.unwrap(), b"keep");
        let status = get_auto_skill_curator_status(&runtime, &root, now())
            .await
            .unwrap();
        assert_eq!(status.active[0].directory_name, "auto-skill-old");
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn pinned_auto_skill_is_protected_until_unpinned() {
        let runtime = TestRuntime::default();
        let root = temp_root("pinned");
        seed_old_skill(&runtime, &root, "auto-skill-pinned").await;
        set_auto_skill_pinned(&runtime, &root, "auto-skill-pinned", true, now())
            .await
            .unwrap();
        let result = run_auto_skill_curator(&runtime, &root, false, now())
            .await
            .unwrap();
        assert!(result.archived.is_empty());
        set_auto_skill_pinned(&runtime, &root, "auto-skill-pinned", false, now())
            .await
            .unwrap();
        let result = run_auto_skill_curator(&runtime, &root, false, now())
            .await
            .unwrap();
        assert_eq!(result.archived, ["auto-skill-pinned"]);
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn marks_stale_then_reactivates_when_the_manifest_is_edited() {
        let runtime = TestRuntime::default();
        let root = temp_root("stale-reactivation");
        let old = now() - chrono::Duration::days(40);
        let manifest = write_skill(&root, "auto-skill-revived", "auto-skill", old).await;
        seed_old_skill(&runtime, &root, "auto-skill-revived").await;
        std::fs::File::open(&manifest)
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(SystemTime::from(old)))
            .unwrap();
        let stale = run_auto_skill_curator(&runtime, &root, false, now())
            .await
            .unwrap();
        assert_eq!(stale.marked_stale, ["auto-skill-revived"]);
        assert!(stale.archived.is_empty());

        std::fs::File::open(&manifest)
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(SystemTime::from(now())))
            .unwrap();
        let active = run_auto_skill_curator(&runtime, &root, false, now())
            .await
            .unwrap();
        assert_eq!(active.reactivated, ["auto-skill-revived"]);
        assert!(active.archived.is_empty());
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn failed_state_write_rolls_back_every_archive_move() {
        let runtime = TestRuntime::default();
        let root = temp_root("rollback");
        seed_old_skill(&runtime, &root, "auto-skill-one").await;
        seed_old_skill(&runtime, &root, "auto-skill-two").await;
        runtime.fail_next_write.store(true, Ordering::SeqCst);
        let error = run_auto_skill_curator(&runtime, &root, false, now())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("simulated persistence failure"));
        assert!(root.join(".canopy/skills/auto-skill-one/SKILL.md").exists());
        assert!(root.join(".canopy/skills/auto-skill-two/SKILL.md").exists());
        assert!(!root.join(".canopy/archived-skills/auto-skill-one").exists());
        assert!(!root.join(".canopy/archived-skills/auto-skill-two").exists());
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn failed_restore_state_write_moves_skill_back_to_archive() {
        let runtime = TestRuntime::default();
        let root = temp_root("restore-rollback");
        seed_old_skill(&runtime, &root, "auto-skill-restore").await;
        run_auto_skill_curator(&runtime, &root, false, now())
            .await
            .unwrap();
        runtime.fail_next_write.store(true, Ordering::SeqCst);
        let error = restore_archived_auto_skill(&runtime, &root, "auto-skill-restore", now())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("simulated persistence failure"));
        assert!(!root.join(".canopy/skills/auto-skill-restore").exists());
        assert!(
            root.join(".canopy/archived-skills/auto-skill-restore/SKILL.md")
                .exists()
        );
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn corrupt_state_fails_closed() {
        let runtime = TestRuntime::default();
        let root = temp_root("state-safety");
        tokio::fs::create_dir_all(root.join(".canopy"))
            .await
            .unwrap();
        tokio::fs::write(root.join(".canopy/skill-curator.json"), b"{broken")
            .await
            .unwrap();
        let error = get_auto_skill_curator_status(&runtime, &root, now())
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Invalid auto-skill curator state")
        );
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn refuses_a_symlinked_state_file() {
        use std::os::unix::fs::symlink;
        let runtime = TestRuntime::default();
        let root = temp_root("state-symlink");
        tokio::fs::create_dir_all(root.join(".canopy"))
            .await
            .unwrap();
        tokio::fs::write(
            root.join("external-state.json"),
            br#"{"version":1,"skills":{}}"#,
        )
        .await
        .unwrap();
        symlink(
            root.join("external-state.json"),
            root.join(".canopy/skill-curator.json"),
        )
        .unwrap();
        let error = get_auto_skill_curator_status(&runtime, &root, now())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("refuses unsafe path"));
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn re_read_activity_downgrades_an_archive_candidate() {
        let runtime = TestRuntime::default();
        let root = temp_root("reread");
        seed_old_skill(&runtime, &root, "auto-skill-reread").await;
        let manifest = root.join(".canopy/skills/auto-skill-reread/SKILL.md");
        let fresh: SystemTime = (now() - chrono::Duration::days(40)).into();
        std::fs::File::open(&manifest)
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(fresh))
            .unwrap();
        let result = run_auto_skill_curator(&runtime, &root, false, now())
            .await
            .unwrap();
        assert!(result.archived.is_empty());
        assert_eq!(result.marked_stale, ["auto-skill-reread"]);
        assert!(manifest.exists());
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn usage_rejects_non_project_and_outside_manifests() {
        let runtime = TestRuntime::default();
        let root = temp_root("usage");
        let manifest = write_skill(&root, "auto-skill-usage", "auto-skill", now()).await;
        let mut skill = super::super::types::SkillConfig {
            name: "usage".to_owned(),
            description: "test".to_owned(),
            allowed_tools: None,
            hooks: None,
            model: None,
            level: SkillLevel::User,
            file_path: manifest.clone(),
            skill_root: None,
            body: String::new(),
            extension_name: None,
            argument_hint: None,
            when_to_use: None,
            disable_model_invocation: None,
            user_invocable: None,
            paths: None,
            priority: None,
        };
        assert!(
            !record_auto_skill_usage(&runtime, &root, &skill, now())
                .await
                .unwrap()
        );
        skill.level = SkillLevel::Project;
        assert!(
            record_auto_skill_usage(&runtime, &root, &skill, now())
                .await
                .unwrap()
        );
        let status = get_auto_skill_curator_status(&runtime, &root, now())
            .await
            .unwrap();
        assert_eq!(status.active[0].use_count, 1);
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn refuses_manifest_symlinks_and_control_character_directory_names() {
        use std::os::unix::fs::symlink;
        let runtime = TestRuntime::default();
        let root = temp_root("symlink");
        let real = write_skill(&root, "auto-skill-real", "auto-skill", now()).await;
        let linked_dir = root.join(".canopy/skills/auto-skill-linked");
        tokio::fs::create_dir_all(&linked_dir).await.unwrap();
        symlink(&real, linked_dir.join(SKILL_FILE_NAME)).unwrap();
        let malicious_name = "auto-skill-\u{1b}[31mevil";
        let malicious_dir = root.join(".canopy/skills").join(malicious_name);
        tokio::fs::create_dir_all(&malicious_dir).await.unwrap();
        tokio::fs::write(
            malicious_dir.join(SKILL_FILE_NAME),
            b"---\nname: evil\nsource: auto-skill\n---\n",
        )
        .await
        .unwrap();
        let status = get_auto_skill_curator_status(&runtime, &root, now())
            .await
            .unwrap();
        let names: Vec<_> = status
            .active
            .iter()
            .map(|entry| entry.directory_name.as_str())
            .collect();
        assert_eq!(names, ["auto-skill-real"]);
        let _ = tokio::fs::remove_dir_all(root).await;
    }
}
