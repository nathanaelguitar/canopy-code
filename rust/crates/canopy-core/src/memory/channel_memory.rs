//! Channel-scoped memory persistence and CRUD operations.
//!
//! This ports `packages/core/src/memory/channel-memory.ts`, including its
//! versioned JSON store, legacy Markdown migration, compare-and-swap updates,
//! and atomic writes.

use super::channel_memory_document::{
    ChannelMemoryDocument, ChannelMemoryDocumentError, ChannelMemoryEntry,
    normalize_channel_memory_text,
};
use super::secret_scanner::scan_for_secrets;
use crate::storage::Storage;
use chrono::{SecondsFormat, Utc};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error as ThisError;
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

pub const CHANNEL_MEMORY_FILE_NAME: &str = "CHANNEL.json";
pub const LEGACY_CHANNEL_MEMORY_FILE_NAME: &str = "CHANNEL.md";
pub const MAX_CHANNEL_MEMORY_BYTES: usize = 1024 * 1024;
const LOCK_STALE_AFTER: Duration = Duration::from_secs(5);
const LOCK_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(2_500);
const LOCK_RETRIES: usize = 12;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChannelMemoryTarget {
    pub channel_name: String,
    pub chat_id: String,
    pub thread_id: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelMemoryMutationResult {
    pub changed: bool,
    pub file_path: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AddChannelMemoryResult {
    pub changed: bool,
    pub file_path: PathBuf,
    pub added: Vec<ChannelMemoryEntry>,
    pub duplicate_ids: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateChannelMemoryResult {
    pub changed: bool,
    pub file_path: PathBuf,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entry: Option<ChannelMemoryEntry>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoveChannelMemoryResult {
    pub changed: bool,
    pub file_path: PathBuf,
    pub removed: Vec<ChannelMemoryEntry>,
}

#[derive(Debug, ThisError)]
pub enum ChannelMemoryError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Document(#[from] ChannelMemoryDocumentError),
    #[error("{0}")]
    Message(String),
}

impl ChannelMemoryError {
    fn message(message: impl Into<String>) -> Self {
        Self::Message(message.into())
    }
}

#[derive(Clone, Debug)]
struct LoadedChannelMemory {
    document: ChannelMemoryDocument,
    legacy_bytes: Option<Vec<u8>>,
    legacy_has_entries: bool,
}

struct Mutation<T> {
    changed: bool,
    result: T,
}

type LocalLockMap = HashMap<PathBuf, Weak<AsyncMutex<()>>>;
static LOCAL_LOCKS: OnceLock<Mutex<LocalLockMap>> = OnceLock::new();

/// Filesystem-backed channel memory store rooted at the user's global Canopy
/// directory. `with_global_canopy_dir` lets embedders and tests pin that root.
#[derive(Clone, Debug)]
pub struct ChannelMemoryStore {
    global_canopy_dir: PathBuf,
}

impl Default for ChannelMemoryStore {
    fn default() -> Self {
        Self::with_global_canopy_dir(Storage::get_global_canopy_dir())
    }
}

impl ChannelMemoryStore {
    pub fn with_global_canopy_dir(path: impl Into<PathBuf>) -> Self {
        Self {
            global_canopy_dir: path.into(),
        }
    }

    pub fn file_path(&self, target: &ChannelMemoryTarget) -> PathBuf {
        self.directory(target).join(CHANNEL_MEMORY_FILE_NAME)
    }

    pub fn legacy_file_path(&self, target: &ChannelMemoryTarget) -> PathBuf {
        self.directory(target).join(LEGACY_CHANNEL_MEMORY_FILE_NAME)
    }

    fn directory(&self, target: &ChannelMemoryTarget) -> PathBuf {
        self.global_canopy_dir
            .join("channels")
            .join("memory")
            .join(safe_channel_name(&target.channel_name))
            .join(hashed_thread_path(target))
    }

    pub async fn list_entries(
        &self,
        target: &ChannelMemoryTarget,
    ) -> Result<Vec<ChannelMemoryEntry>, ChannelMemoryError> {
        let file_path = self.file_path(target);
        let legacy_path = self.legacy_file_path(target);
        Ok(load_channel_memory(&file_path, &legacy_path)
            .await?
            .document
            .entries)
    }

    pub async fn read(&self, target: &ChannelMemoryTarget) -> Result<String, ChannelMemoryError> {
        let entries = self.list_entries(target).await?;
        Ok(super::channel_memory_document::render_channel_memory_recall(&entries))
    }

    pub async fn revision(
        &self,
        target: &ChannelMemoryTarget,
    ) -> Result<String, ChannelMemoryError> {
        let canonical_path = self.file_path(target);
        let legacy_path = self.legacy_file_path(target);
        let (canonical, legacy) =
            tokio::try_join!(file_revision(&canonical_path), file_revision(&legacy_path))?;
        let mut hash = Sha256::new();
        hash.update(canonical.as_bytes());
        hash.update([0]);
        hash.update(legacy.as_bytes());
        Ok(hex(&hash.finalize()))
    }

    pub async fn add_entries(
        &self,
        target: &ChannelMemoryTarget,
        texts: &[String],
        created_by: Option<&str>,
    ) -> Result<AddChannelMemoryResult, ChannelMemoryError> {
        if texts.len() > super::channel_memory_document::MAX_CHANNEL_MEMORY_ENTRIES_PER_REQUEST {
            return Err(ChannelMemoryError::message(
                "Channel memory accepts at most 10 entries per request",
            ));
        }
        assert_no_channel_memory_secrets(texts.iter().map(String::as_str))?;

        let file_path = self.file_path(target);
        self.mutate(target, |document, _source_has_entries| {
            let mut entries_by_normalized_text = HashMap::new();
            for entry in &document.entries {
                entries_by_normalized_text
                    .insert(normalize_channel_memory_text(&entry.text), entry.clone());
            }
            let mut ids = document
                .entries
                .iter()
                .map(|entry| entry.id.clone())
                .collect::<HashSet<_>>();
            let mut added = Vec::new();
            let mut duplicate_ids = Vec::new();

            for text in texts {
                let normalized_text = normalize_channel_memory_text(text);
                if normalized_text.is_empty() {
                    continue;
                }
                if let Some(existing) = entries_by_normalized_text.get(&normalized_text) {
                    duplicate_ids.push(existing.id.clone());
                    continue;
                }

                let random_hex = loop {
                    let candidate = Uuid::new_v4().simple().to_string()[..12].to_owned();
                    let id = format!("m-{candidate}");
                    if !ids.contains(&id) {
                        break candidate;
                    }
                };
                let now = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
                let entry = super::channel_memory_document::create_channel_memory_entry(
                    super::channel_memory_document::NewChannelMemoryEntry {
                        text,
                        created_by,
                        now: &now,
                        random_hex: &random_hex,
                    },
                )?;
                ids.insert(entry.id.clone());
                entries_by_normalized_text.insert(normalized_text, entry.clone());
                document.entries.push(entry.clone());
                added.push(entry);
            }

            Ok(Mutation {
                changed: !added.is_empty(),
                result: AddChannelMemoryResult {
                    changed: !added.is_empty(),
                    file_path,
                    added,
                    duplicate_ids,
                },
            })
        })
        .await
    }

    pub async fn append(
        &self,
        target: &ChannelMemoryTarget,
        text: &str,
    ) -> Result<ChannelMemoryMutationResult, ChannelMemoryError> {
        let result = self.add_entries(target, &[text.to_owned()], None).await?;
        Ok(ChannelMemoryMutationResult {
            changed: result.changed,
            file_path: result.file_path,
        })
    }

    pub async fn update_entry(
        &self,
        target: &ChannelMemoryTarget,
        id: &str,
        text: &str,
        expected_text: Option<&str>,
    ) -> Result<UpdateChannelMemoryResult, ChannelMemoryError> {
        assert_no_channel_memory_secrets([text])?;
        let file_path = self.file_path(target);
        self.mutate(target, |document, _source_has_entries| {
            let Some(index) = document.entries.iter().position(|entry| entry.id == id) else {
                if expected_text.is_some() {
                    return Err(ChannelMemoryError::message("Channel memory entry changed"));
                }
                return Ok(Mutation {
                    changed: false,
                    result: UpdateChannelMemoryResult {
                        changed: false,
                        file_path,
                        entry: None,
                    },
                });
            };
            if expected_text.is_some_and(|expected| document.entries[index].text != expected) {
                return Err(ChannelMemoryError::message("Channel memory entry changed"));
            }

            let now = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
            let replacement = super::channel_memory_document::create_channel_memory_entry(
                super::channel_memory_document::NewChannelMemoryEntry {
                    text,
                    created_by: None,
                    now: &now,
                    random_hex: "000000000000",
                },
            )?;
            if document
                .entries
                .iter()
                .enumerate()
                .any(|(other_index, candidate)| {
                    other_index != index
                        && normalize_channel_memory_text(&candidate.text)
                            == normalize_channel_memory_text(&replacement.text)
                })
            {
                return Err(ChannelMemoryError::message(
                    "Channel memory entry already exists",
                ));
            }
            let entry = &mut document.entries[index];
            entry.text = replacement.text;
            entry.updated_at = replacement.updated_at;
            Ok(Mutation {
                changed: true,
                result: UpdateChannelMemoryResult {
                    changed: true,
                    file_path,
                    entry: Some(entry.clone()),
                },
            })
        })
        .await
    }

    pub async fn remove_entries(
        &self,
        target: &ChannelMemoryTarget,
        ids: &[String],
        expected_text_by_id: Option<&HashMap<String, String>>,
    ) -> Result<RemoveChannelMemoryResult, ChannelMemoryError> {
        let file_path = self.file_path(target);
        self.mutate(target, |document, _source_has_entries| {
            let requested_ids = ids.iter().cloned().collect::<HashSet<_>>();
            if let Some(expected) = expected_text_by_id {
                for id in &requested_ids {
                    if let Some(expected_text) = expected.get(id) {
                        let current_text = document
                            .entries
                            .iter()
                            .find(|entry| entry.id.as_str() == id.as_str())
                            .map(|entry| entry.text.as_str());
                        if current_text != Some(expected_text.as_str()) {
                            return Err(ChannelMemoryError::message(
                                "Channel memory entry changed",
                            ));
                        }
                    }
                }
            }
            let removed = document
                .entries
                .iter()
                .filter(|entry| requested_ids.contains(&entry.id))
                .cloned()
                .collect::<Vec<_>>();
            if removed.is_empty() {
                return Ok(Mutation {
                    changed: false,
                    result: RemoveChannelMemoryResult {
                        changed: false,
                        file_path,
                        removed,
                    },
                });
            }
            document
                .entries
                .retain(|entry| !requested_ids.contains(&entry.id));
            Ok(Mutation {
                changed: true,
                result: RemoveChannelMemoryResult {
                    changed: true,
                    file_path,
                    removed,
                },
            })
        })
        .await
    }

    pub async fn clear(
        &self,
        target: &ChannelMemoryTarget,
    ) -> Result<ChannelMemoryMutationResult, ChannelMemoryError> {
        let file_path = self.file_path(target);
        self.mutate(target, |document, source_has_entries| {
            if !source_has_entries {
                return Ok(Mutation {
                    changed: false,
                    result: ChannelMemoryMutationResult {
                        changed: false,
                        file_path,
                    },
                });
            }
            document.entries.clear();
            Ok(Mutation {
                changed: true,
                result: ChannelMemoryMutationResult {
                    changed: true,
                    file_path,
                },
            })
        })
        .await
    }

    async fn mutate<T, F>(
        &self,
        target: &ChannelMemoryTarget,
        apply: F,
    ) -> Result<T, ChannelMemoryError>
    where
        F: FnOnce(&mut ChannelMemoryDocument, bool) -> Result<Mutation<T>, ChannelMemoryError>,
    {
        let file_path = self.file_path(target);
        let directory = file_path.parent().unwrap_or_else(|| Path::new("."));
        let legacy_path = self.legacy_file_path(target);
        let local_lock = local_lock(directory);
        let _local_guard = local_lock.lock().await;
        tokio::fs::create_dir_all(directory).await?;

        // Retain the compatibility marker file expected by older TypeScript
        // workers, whose proper-lockfile instance locks this marker's `.lock`
        // directory.
        let marker_path = directory.join(".channel-memory.lock");
        OpenOptions::new()
            .create(true)
            .append(true)
            .mode_owner_only()
            .open(&marker_path)?;
        let _canonical_guard = acquire_file_lock(&marker_path).await?;
        let _legacy_guard = match tokio::fs::metadata(&legacy_path).await {
            Ok(_) => Some(acquire_file_lock(&legacy_path).await?),
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };

        let mut loaded = load_channel_memory(&file_path, &legacy_path).await?;
        let source_has_entries = !loaded.document.entries.is_empty() || loaded.legacy_has_entries;
        let mutation = apply(&mut loaded.document, source_has_entries)?;
        if !mutation.changed {
            return Ok(mutation.result);
        }

        let serialized =
            super::channel_memory_document::serialize_channel_memory_document(&loaded.document)?;
        let serialized_bytes = serialized.as_bytes();
        if serialized_bytes.len() > MAX_CHANNEL_MEMORY_BYTES {
            return Err(ChannelMemoryError::message(
                "Channel memory exceeds maximum size",
            ));
        }
        write_channel_memory(&file_path, serialized_bytes)?;
        if let Some(legacy_bytes) = loaded.legacy_bytes.take() {
            cleanup_legacy_after_commit(&legacy_path, &legacy_bytes).await;
        }
        Ok(mutation.result)
    }
}

/// Return the canonical channel-memory path using the active Canopy home.
pub fn get_channel_memory_file_path(target: &ChannelMemoryTarget) -> PathBuf {
    ChannelMemoryStore::default().file_path(target)
}

/// Return the legacy Markdown path using the active Canopy home.
pub fn get_legacy_channel_memory_file_path(target: &ChannelMemoryTarget) -> PathBuf {
    ChannelMemoryStore::default().legacy_file_path(target)
}

pub async fn list_channel_memory_entries(
    target: &ChannelMemoryTarget,
) -> Result<Vec<ChannelMemoryEntry>, ChannelMemoryError> {
    ChannelMemoryStore::default().list_entries(target).await
}

pub async fn read_channel_memory(
    target: &ChannelMemoryTarget,
) -> Result<String, ChannelMemoryError> {
    ChannelMemoryStore::default().read(target).await
}

pub async fn get_channel_memory_revision(
    target: &ChannelMemoryTarget,
) -> Result<String, ChannelMemoryError> {
    ChannelMemoryStore::default().revision(target).await
}

pub async fn add_channel_memory_entries(
    target: &ChannelMemoryTarget,
    texts: &[String],
    created_by: Option<&str>,
) -> Result<AddChannelMemoryResult, ChannelMemoryError> {
    ChannelMemoryStore::default()
        .add_entries(target, texts, created_by)
        .await
}

pub async fn append_channel_memory(
    target: &ChannelMemoryTarget,
    text: &str,
) -> Result<ChannelMemoryMutationResult, ChannelMemoryError> {
    ChannelMemoryStore::default().append(target, text).await
}

pub async fn update_channel_memory_entry(
    target: &ChannelMemoryTarget,
    id: &str,
    text: &str,
    expected_text: Option<&str>,
) -> Result<UpdateChannelMemoryResult, ChannelMemoryError> {
    ChannelMemoryStore::default()
        .update_entry(target, id, text, expected_text)
        .await
}

pub async fn remove_channel_memory_entries(
    target: &ChannelMemoryTarget,
    ids: &[String],
    expected_text_by_id: Option<&HashMap<String, String>>,
) -> Result<RemoveChannelMemoryResult, ChannelMemoryError> {
    ChannelMemoryStore::default()
        .remove_entries(target, ids, expected_text_by_id)
        .await
}

pub async fn clear_channel_memory(
    target: &ChannelMemoryTarget,
) -> Result<ChannelMemoryMutationResult, ChannelMemoryError> {
    ChannelMemoryStore::default().clear(target).await
}

fn safe_channel_name(channel_name: &str) -> String {
    let mut slug = channel_name
        .encode_utf16()
        .map(|unit| {
            if unit <= 0x7f {
                let character = unit as u8 as char;
                if character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-') {
                    character
                } else {
                    '_'
                }
            } else {
                '_'
            }
        })
        .take(20)
        .collect::<String>();
    if slug.is_empty() {
        slug.push('_');
    }
    let digest = Sha256::digest(channel_name.as_bytes());
    format!("{slug}-{}", hex(&digest)[..16].to_owned())
}

fn hashed_thread_path(target: &ChannelMemoryTarget) -> String {
    let mut hash = Sha256::new();
    hash.update(target.chat_id.as_bytes());
    hash.update([0]);
    hash.update(target.thread_id.as_deref().unwrap_or_default().as_bytes());
    hex(&hash.finalize())[..32].to_owned()
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        result.push(HEX[(byte >> 4) as usize] as char);
        result.push(HEX[(byte & 0x0f) as usize] as char);
    }
    result
}

fn local_lock(path: &Path) -> Arc<AsyncMutex<()>> {
    let locks = LOCAL_LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut locks = locks
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    locks.retain(|_, lock| lock.strong_count() > 0);
    if let Some(lock) = locks.get(path).and_then(Weak::upgrade) {
        return lock;
    }
    let lock = Arc::new(AsyncMutex::new(()));
    locks.insert(path.to_path_buf(), Arc::downgrade(&lock));
    lock
}

struct FileLockGuard {
    directory: PathBuf,
    heartbeat: tokio::task::JoinHandle<()>,
}

impl Drop for FileLockGuard {
    fn drop(&mut self) {
        self.heartbeat.abort();
        // Removal is deliberately best-effort, matching proper-lockfile's
        // non-fatal stale lock cleanup after the protected operation commits.
        let _ = fs::remove_dir(&self.directory);
    }
}

async fn acquire_file_lock(target: &Path) -> io::Result<FileLockGuard> {
    let lock_directory = append_suffix(target, ".lock");
    for attempt in 0..=LOCK_RETRIES {
        match tokio::fs::create_dir(&lock_directory).await {
            Ok(()) => {
                if let Err(error) = touch_directory(&lock_directory) {
                    let _ = tokio::fs::remove_dir(&lock_directory).await;
                    return Err(error);
                }
                let heartbeat_directory = lock_directory.clone();
                let heartbeat = tokio::spawn(async move {
                    loop {
                        tokio::time::sleep(LOCK_HEARTBEAT_INTERVAL).await;
                        if let Err(error) = touch_directory(&heartbeat_directory) {
                            eprintln!("channel memory lock heartbeat failed: {error}");
                            return;
                        }
                    }
                });
                return Ok(FileLockGuard {
                    directory: lock_directory,
                    heartbeat,
                });
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                if lock_is_stale(&lock_directory).await {
                    let _ = tokio::fs::remove_dir_all(&lock_directory).await;
                }
                if attempt == LOCK_RETRIES {
                    break;
                }
                tokio::time::sleep(lock_retry_delay(attempt)).await;
            }
            Err(error) => return Err(error),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::WouldBlock,
        format!(
            "timed out acquiring channel memory lock for {}",
            target.display()
        ),
    ))
}

fn append_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut value: OsString = path.as_os_str().to_owned();
    value.push(suffix);
    PathBuf::from(value)
}

async fn lock_is_stale(path: &Path) -> bool {
    let Ok(metadata) = tokio::fs::metadata(path).await else {
        return false;
    };
    let Ok(modified) = metadata.modified() else {
        return false;
    };
    SystemTime::now()
        .duration_since(modified)
        .is_ok_and(|age| age > LOCK_STALE_AFTER)
}

fn lock_retry_delay(attempt: usize) -> Duration {
    let base_ms = 50_u64
        .saturating_mul(2_u64.saturating_pow(attempt.min(5) as u32))
        .min(1_000);
    let jitter = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as u64
        % (base_ms + 1);
    Duration::from_millis((base_ms / 2 + jitter).min(1_000))
}

fn touch_directory(path: &Path) -> io::Result<()> {
    let directory = File::open(path)?;
    directory.set_times(std::fs::FileTimes::new().set_modified(SystemTime::now()))
}

async fn read_file_if_exists(path: &Path) -> Result<Option<Vec<u8>>, ChannelMemoryError> {
    match tokio::fs::read(path).await {
        Ok(bytes) => Ok(Some(bytes)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

async fn load_channel_memory(
    file_path: &Path,
    legacy_path: &Path,
) -> Result<LoadedChannelMemory, ChannelMemoryError> {
    let (initial_json_bytes, legacy_bytes) = tokio::try_join!(
        read_file_if_exists(file_path),
        read_file_if_exists(legacy_path)
    )?;
    let json_bytes = match initial_json_bytes {
        Some(bytes) => Some(bytes),
        None if legacy_bytes.is_none() => read_file_if_exists(file_path).await?,
        None => None,
    };

    if let Some(json_bytes) = json_bytes {
        if json_bytes.len() > MAX_CHANNEL_MEMORY_BYTES {
            return Err(ChannelMemoryError::message(
                "Channel memory exceeds maximum size",
            ));
        }
        let source = std::str::from_utf8(&json_bytes).map_err(|error| {
            ChannelMemoryError::Io(io::Error::new(io::ErrorKind::InvalidData, error))
        })?;
        let document = super::channel_memory_document::parse_channel_memory_document(source)?;
        verify_dual_file_state(&document, legacy_bytes.as_deref())?;
        let legacy_has_entries = match legacy_bytes.as_deref() {
            Some(bytes) => !super::channel_memory_document::parse_legacy_channel_memory(bytes)?
                .entries
                .is_empty(),
            None => false,
        };
        Ok(LoadedChannelMemory {
            document,
            legacy_bytes,
            legacy_has_entries,
        })
    } else if let Some(legacy_bytes) = legacy_bytes {
        let document = super::channel_memory_document::parse_legacy_channel_memory(&legacy_bytes)?;
        Ok(LoadedChannelMemory {
            legacy_has_entries: !document.entries.is_empty(),
            document,
            legacy_bytes: Some(legacy_bytes),
        })
    } else {
        Ok(LoadedChannelMemory {
            document: ChannelMemoryDocument::default(),
            legacy_bytes: None,
            legacy_has_entries: false,
        })
    }
}

fn verify_dual_file_state(
    document: &ChannelMemoryDocument,
    legacy_bytes: Option<&[u8]>,
) -> Result<(), ChannelMemoryError> {
    if let Some(legacy_bytes) = legacy_bytes {
        let expected_hash = hex(&Sha256::digest(legacy_bytes));
        if document
            .migration
            .as_ref()
            .is_none_or(|migration| migration.legacy_sha256 != expected_hash)
        {
            return Err(ChannelMemoryError::message(
                "Channel memory migration conflict",
            ));
        }
    }
    Ok(())
}

fn assert_no_channel_memory_secrets<'a>(
    texts: impl IntoIterator<Item = &'a str>,
) -> Result<(), ChannelMemoryError> {
    if texts
        .into_iter()
        .any(|text| !scan_for_secrets(text).is_empty())
    {
        return Err(ChannelMemoryError::message(
            "Channel memory cannot store detected credentials",
        ));
    }
    Ok(())
}

async fn file_revision(path: &Path) -> Result<String, ChannelMemoryError> {
    let metadata = match tokio::fs::metadata(path).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok("missing".to_owned()),
        Err(error) => return Err(error.into()),
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Ok(format!(
            "{}:{}:{}:{}:{}",
            metadata.dev(),
            metadata.ino(),
            metadata.size(),
            metadata.mtime() as i128 * 1_000_000_000 + metadata.mtime_nsec() as i128,
            metadata.ctime() as i128 * 1_000_000_000 + metadata.ctime_nsec() as i128,
        ))
    }
    #[cfg(not(unix))]
    {
        let modified = metadata
            .modified()
            .ok()
            .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();
        Ok(format!("0:0:{}:{modified}:{modified}", metadata.len()))
    }
}

async fn cleanup_legacy_after_commit(path: &Path, expected_bytes: &[u8]) {
    let Ok(current_bytes) = tokio::fs::read(path).await else {
        return;
    };
    if Sha256::digest(&current_bytes) != Sha256::digest(expected_bytes)
        || current_bytes != expected_bytes
    {
        return;
    }
    let _ = tokio::fs::remove_file(path).await;
}

fn write_channel_memory(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let parent = path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let file_name = path
        .file_name()
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "channel memory path has no filename",
            )
        })?
        .to_string_lossy();
    let mut temporary = None;
    for _ in 0..16 {
        let candidate = parent.join(format!(
            "{file_name}.{}.{}.tmp",
            std::process::id(),
            Uuid::new_v4().simple().to_string()[..12].to_owned(),
        ));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        match options.open(&candidate) {
            Ok(file) => {
                temporary = Some((candidate, file));
                break;
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
    let (temporary_path, mut file) = temporary.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::AlreadyExists,
            "could not allocate a channel-memory temporary file",
        )
    })?;
    let result = (|| {
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temporary_path, path)?;
        // The rename is the commit point. Directory syncing improves power
        // loss durability where supported but must not report a failed write
        // after the canonical file has already changed.
        let _ = File::open(parent).and_then(|directory| directory.sync_all());
        Ok(())
    })();
    if result.is_err() {
        if let Err(cleanup_error) = fs::remove_file(&temporary_path) {
            if cleanup_error.kind() != io::ErrorKind::NotFound {
                return Err(cleanup_error);
            }
        }
    }
    result
}

trait OpenOptionsOwnerOnly {
    fn mode_owner_only(&mut self) -> &mut Self;
}

impl OpenOptionsOwnerOnly for OpenOptions {
    fn mode_owner_only(&mut self) -> &mut Self {
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            self.mode(0o600);
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static NEXT_TEMP: AtomicUsize = AtomicUsize::new(0);

    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            let sequence = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "canopy-channel-memory-{}-{sequence}",
                std::process::id()
            ));
            let _ = fs::remove_dir_all(&path);
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        fn store(&self) -> ChannelMemoryStore {
            ChannelMemoryStore::with_global_canopy_dir(&self.0)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn target() -> ChannelMemoryTarget {
        ChannelMemoryTarget {
            channel_name: "prod".to_owned(),
            chat_id: "chat-1".to_owned(),
            thread_id: None,
        }
    }

    #[tokio::test]
    async fn paths_hash_identifiers_and_keep_a_safe_channel_slug() {
        let temp = TempDir::new();
        let store = temp.store();
        let actual = store.file_path(&ChannelMemoryTarget {
            channel_name: "ops/alerts".to_owned(),
            chat_id: "raw-chat-id".to_owned(),
            thread_id: Some("raw-thread-id".to_owned()),
        });
        assert!(actual.starts_with(&temp.0));
        assert!(!actual.to_string_lossy().contains("raw-chat-id"));
        assert!(!actual.to_string_lossy().contains("raw-thread-id"));
        assert!(actual.to_string_lossy().contains("ops_alerts-"));
        assert_eq!(
            actual.file_name().unwrap().to_string_lossy(),
            "CHANNEL.json"
        );
        assert_ne!(
            store.file_path(&target()),
            store.file_path(&ChannelMemoryTarget {
                thread_id: Some("thread-1".to_owned()),
                ..target()
            })
        );
    }

    #[tokio::test]
    async fn reads_legacy_deterministically_then_migrates_on_write() {
        let temp = TempDir::new();
        let store = temp.store();
        let target = target();
        let legacy_path = store.legacy_file_path(&target);
        fs::create_dir_all(legacy_path.parent().unwrap()).unwrap();
        fs::write(&legacy_path, "Use staging\nUse staging\n Run tests \n").unwrap();

        let entries = store.list_entries(&target).await.unwrap();
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.text.as_str())
                .collect::<Vec<_>>(),
            ["Use staging", " Run tests "]
        );
        assert!(!store.file_path(&target).exists());

        let result = store
            .add_entries(&target, &["Review diff".to_owned()], Some("alice"))
            .await
            .unwrap();
        assert!(result.changed);
        assert_eq!(result.added[0].created_by.as_deref(), Some("alice"));
        assert!(!legacy_path.exists());
        assert_eq!(
            store.read(&target).await.unwrap(),
            "Use staging\n Run tests \nReview diff\n"
        );
    }

    #[tokio::test]
    async fn normalized_duplicates_and_secret_rejection_match_the_mutation_contract() {
        let temp = TempDir::new();
        let store = temp.store();
        let target = target();
        let first = store
            .add_entries(&target, &["Use staging".to_owned()], Some("alice"))
            .await
            .unwrap();
        let duplicate = store
            .add_entries(&target, &[" use   STAGING ".to_owned()], None)
            .await
            .unwrap();
        assert!(!duplicate.changed);
        assert_eq!(duplicate.duplicate_ids, vec![first.added[0].id.clone()]);
        let before = fs::read(store.file_path(&target)).unwrap();
        let error = store
            .add_entries(&target, &[format!("ghp_{}", "a".repeat(36))], None)
            .await
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "Channel memory cannot store detected credentials"
        );
        assert_eq!(fs::read(store.file_path(&target)).unwrap(), before);
    }

    #[tokio::test]
    async fn rejects_overlarge_requests_and_invalid_entry_text_before_writing() {
        let temp = TempDir::new();
        let store = temp.store();
        let target = target();
        let too_many = vec!["entry".to_owned(); 11];
        assert_eq!(
            store
                .add_entries(&target, &too_many, None)
                .await
                .unwrap_err()
                .to_string(),
            "Channel memory accepts at most 10 entries per request"
        );
        assert_eq!(
            store
                .add_entries(&target, &["a".repeat(2_001)], None)
                .await
                .unwrap_err()
                .to_string(),
            "Invalid channel memory entry"
        );
        assert!(!store.file_path(&target).exists());
    }

    #[tokio::test]
    async fn update_remove_clear_preserve_identity_and_enforce_compare_and_swap() {
        let temp = TempDir::new();
        let store = temp.store();
        let target = target();
        let added = store
            .add_entries(&target, &["Use staging".to_owned()], Some("alice"))
            .await
            .unwrap();
        let original = added.added[0].clone();
        tokio::time::sleep(Duration::from_millis(2)).await;
        let updated = store
            .update_entry(&target, &original.id, "Use production", Some("Use staging"))
            .await
            .unwrap()
            .entry
            .unwrap();
        assert_eq!(updated.id, original.id);
        assert_eq!(updated.created_at, original.created_at);
        assert_eq!(updated.created_by, original.created_by);
        assert_eq!(updated.text, "Use production");
        assert_ne!(updated.updated_at, original.updated_at);
        assert_eq!(
            store
                .update_entry(&target, &original.id, "lost", Some("Use staging"))
                .await
                .unwrap_err()
                .to_string(),
            "Channel memory entry changed"
        );

        let removed = store
            .remove_entries(&target, std::slice::from_ref(&original.id), None)
            .await
            .unwrap();
        assert_eq!(removed.removed, vec![updated]);
        assert!(!store.clear(&target).await.unwrap().changed);
        store
            .add_entries(&target, &["Clear me".to_owned()], None)
            .await
            .unwrap();
        assert!(store.clear(&target).await.unwrap().changed);
        assert!(store.list_entries(&target).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn rejects_dual_file_conflicts_and_oversized_documents() {
        let temp = TempDir::new();
        let store = temp.store();
        let target = target();
        let legacy_path = store.legacy_file_path(&target);
        fs::create_dir_all(legacy_path.parent().unwrap()).unwrap();
        fs::write(&legacy_path, "Use staging\n").unwrap();
        fs::write(
            store.file_path(&target),
            r#"{"version":1,"migration":{"legacySha256":"0000000000000000000000000000000000000000000000000000000000000000"},"entries":[]}"#,
        )
        .unwrap();
        assert_eq!(
            store.list_entries(&target).await.unwrap_err().to_string(),
            "Channel memory migration conflict"
        );

        fs::write(
            store.file_path(&target),
            vec![b'x'; MAX_CHANNEL_MEMORY_BYTES + 1],
        )
        .unwrap();
        assert_eq!(
            store.list_entries(&target).await.unwrap_err().to_string(),
            "Channel memory exceeds maximum size"
        );

        fs::remove_file(store.file_path(&target)).unwrap();
        let oversized_created_by = "x".repeat(MAX_CHANNEL_MEMORY_BYTES);
        let error = store
            .add_entries(&target, &["entry".to_owned()], Some(&oversized_created_by))
            .await
            .unwrap_err();
        assert_eq!(error.to_string(), "Channel memory exceeds maximum size");
        assert!(!store.file_path(&target).exists());
    }

    #[tokio::test]
    async fn revisions_are_stable_for_missing_files_and_change_after_mutations() {
        let temp = TempDir::new();
        let store = temp.store();
        let target = target();
        let missing = store.revision(&target).await.unwrap();
        assert_eq!(store.revision(&target).await.unwrap(), missing);
        store
            .add_entries(&target, &["Use staging".to_owned()], None)
            .await
            .unwrap();
        assert_ne!(store.revision(&target).await.unwrap(), missing);
    }

    #[tokio::test]
    async fn concurrent_adds_are_serialized_and_atomic() {
        let temp = TempDir::new();
        let store = Arc::new(temp.store());
        let target = Arc::new(target());
        let mut tasks = Vec::new();
        for index in 0..12 {
            let store = store.clone();
            let target = target.clone();
            tasks.push(tokio::spawn(async move {
                store
                    .add_entries(&target, &[format!("entry {index}")], None)
                    .await
            }));
        }
        for task in tasks {
            task.await.unwrap().unwrap();
        }
        let entries = store.list_entries(&target).await.unwrap();
        assert_eq!(entries.len(), 12);
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.id.as_str())
                .collect::<HashSet<_>>()
                .len(),
            entries.len()
        );
        assert!(store.file_path(&target).exists());
        assert!(
            !fs::read_dir(store.file_path(&target).parent().unwrap())
                .unwrap()
                .any(|entry| entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .ends_with(".tmp"))
        );
    }
}
