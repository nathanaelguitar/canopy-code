//! Append-only per-group message history with recovery and compaction.
//!
//! Port of `packages/channels/base/src/group-history-store.ts`. Records are
//! JSON Lines so an interrupted append does not prevent replaying later
//! valid records. The module is intentionally self-contained; the parent
//! crate module export is coordinated separately.

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

pub const DEFAULT_MAX_KEYS: usize = 1_000;
pub const DEFAULT_COMPACT_AFTER_RECORDS: usize = 1_000;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupHistoryEntry {
    pub sender_id: String,
    pub sender_name: String,
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,
    pub timestamp: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GroupHistoryStoreOptions {
    pub max_keys: usize,
    pub compact_after_records: usize,
}

impl Default for GroupHistoryStoreOptions {
    fn default() -> Self {
        Self {
            max_keys: DEFAULT_MAX_KEYS,
            compact_after_records: DEFAULT_COMPACT_AFTER_RECORDS,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct MessageRecord<'a> {
    #[serde(rename = "type")]
    record_type: &'static str,
    key: &'a str,
    limit: f64,
    entry: &'a GroupHistoryEntry,
    recorded_at: u64,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ClearRecord<'a> {
    #[serde(rename = "type")]
    record_type: &'static str,
    key: &'a str,
    recorded_at: u64,
}

#[derive(Clone, Debug)]
enum GroupHistoryRecord {
    Message {
        key: String,
        limit: f64,
        entry: GroupHistoryEntry,
    },
    Clear {
        key: String,
    },
}

#[derive(Default)]
struct LoadedState {
    entries: IndexMap<String, Vec<GroupHistoryEntry>>,
    limits: IndexMap<String, f64>,
    record_count: usize,
    had_invalid_records: bool,
}

/// A file-backed group message history store.
///
/// Each operation reloads the JSONL log, matching the source implementation's
/// cross-instance persistence behavior. A key becomes newest whenever a
/// message is recorded for it; when `max_keys` is exceeded, the oldest key is
/// removed. The backing directory and file are made private on a best-effort
/// basis (`0700` and `0600` on Unix).
pub struct GroupHistoryStore {
    file_path: PathBuf,
    max_keys: usize,
    compact_after_records: usize,
}

impl GroupHistoryStore {
    pub fn new(path: impl Into<PathBuf>, options: GroupHistoryStoreOptions) -> Self {
        Self {
            file_path: path.into(),
            max_keys: options.max_keys,
            compact_after_records: options.compact_after_records,
        }
    }

    pub fn with_defaults(path: impl Into<PathBuf>) -> Self {
        Self::new(path, GroupHistoryStoreOptions::default())
    }

    /// Append one entry. Non-finite and non-positive limits are ignored;
    /// positive fractional limits are floored, like the TypeScript store.
    pub fn record(&self, key: &str, entry: GroupHistoryEntry, limit: f64) -> io::Result<()> {
        let normalized_limit = normalize_limit(limit);
        if normalized_limit <= 0.0 {
            return Ok(());
        }

        let mut loaded = self.load_state()?;
        let current = loaded.entries.shift_remove(key).unwrap_or_default();
        let mut current = current;
        current.push(entry.clone());
        truncate_to_limit(&mut current, normalized_limit);
        loaded.entries.insert(key.to_owned(), current);
        loaded.limits.insert(key.to_owned(), normalized_limit);
        let evicted = evict_old_keys(&mut loaded.entries, &mut loaded.limits, self.max_keys);

        self.append_message(key, &entry, normalized_limit)?;

        if evicted
            || loaded.had_invalid_records
            || loaded.record_count.saturating_add(1) >= self.compact_after_records
        {
            self.compact(&loaded.entries, &loaded.limits)?;
        }
        Ok(())
    }

    /// Return the newest `limit` entries and clear the stored key if present.
    pub fn drain(&self, key: &str, limit: f64) -> io::Result<Vec<GroupHistoryEntry>> {
        let normalized_limit = normalize_limit(limit);
        let mut loaded = self.load_state()?;
        let entries = if normalized_limit > 0.0 {
            loaded
                .entries
                .get(key)
                .map(|items| {
                    let count = normalized_limit as usize;
                    items[items.len().saturating_sub(count)..].to_vec()
                })
                .unwrap_or_default()
        } else {
            Vec::new()
        };

        if loaded.entries.shift_remove(key).is_some() {
            loaded.limits.shift_remove(key);
            self.append_clear(key)?;
        }
        Ok(entries)
    }

    pub fn clear(&self, key: &str) -> io::Result<()> {
        let mut loaded = self.load_state()?;
        if loaded.entries.shift_remove(key).is_none() {
            return Ok(());
        }
        loaded.limits.shift_remove(key);
        self.append_clear(key)?;
        self.compact(&loaded.entries, &loaded.limits)
    }

    pub fn clear_all(&self) -> io::Result<()> {
        let loaded = self.load_state()?;
        if loaded.entries.is_empty() {
            return Ok(());
        }
        let recorded_at = now_millis();
        for key in loaded.entries.keys() {
            self.append_clear_at(key, recorded_at)?;
        }
        self.compact(&IndexMap::new(), &IndexMap::new())
    }

    /// Return the number of retained entries for `key`, or retained keys when
    /// `key` is `None`.
    pub fn size(&self, key: Option<&str>) -> io::Result<usize> {
        let loaded = self.load_state()?;
        Ok(match key {
            Some(key) => loaded.entries.get(key).map_or(0, Vec::len),
            None => loaded.entries.len(),
        })
    }

    fn load_state(&self) -> io::Result<LoadedState> {
        let (records, had_invalid_records) = self.read_records()?;
        let mut loaded = LoadedState {
            record_count: records.len(),
            had_invalid_records,
            ..LoadedState::default()
        };

        for record in records {
            match record {
                GroupHistoryRecord::Clear { key } => {
                    loaded.entries.shift_remove(&key);
                    loaded.limits.shift_remove(&key);
                }
                GroupHistoryRecord::Message { key, limit, entry } => {
                    let mut current = loaded.entries.shift_remove(&key).unwrap_or_default();
                    current.push(entry);
                    // The source parser accepts any JSON number in `limit`.
                    // Its splice behavior retains ceil(limit) entries for a
                    // positive fractional value and none for non-positive.
                    truncate_to_limit(&mut current, limit.ceil().max(0.0));
                    loaded.entries.insert(key.clone(), current);
                    loaded.limits.insert(key, limit);
                    evict_old_keys(&mut loaded.entries, &mut loaded.limits, self.max_keys);
                }
            }
        }
        Ok(loaded)
    }

    fn read_records(&self) -> io::Result<(Vec<GroupHistoryRecord>, bool)> {
        let mut file = match File::open(&self.file_path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok((Vec::new(), false));
            }
            Err(error) => return Err(error),
        };
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        let data = String::from_utf8_lossy(&bytes);
        let mut records = Vec::new();
        let mut had_invalid_records = false;
        for line in data.split('\n') {
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str::<Value>(line)
                .ok()
                .and_then(parse_record)
            {
                Some(record) => records.push(record),
                None => had_invalid_records = true,
            }
        }
        Ok((records, had_invalid_records))
    }

    fn append_message(&self, key: &str, entry: &GroupHistoryEntry, limit: f64) -> io::Result<()> {
        let record = MessageRecord {
            record_type: "message",
            key,
            limit,
            entry,
            recorded_at: now_millis(),
        };
        let mut bytes = serde_json::to_vec(&record).map_err(json_io_error)?;
        bytes.push(b'\n');
        self.append_bytes(&bytes)
    }

    fn append_clear(&self, key: &str) -> io::Result<()> {
        self.append_clear_at(key, now_millis())
    }

    fn append_clear_at(&self, key: &str, recorded_at: u64) -> io::Result<()> {
        let record = ClearRecord {
            record_type: "clear",
            key,
            recorded_at,
        };
        let mut bytes = serde_json::to_vec(&record).map_err(json_io_error)?;
        bytes.push(b'\n');
        self.append_bytes(&bytes)
    }

    fn append_bytes(&self, bytes: &[u8]) -> io::Result<()> {
        let dir = parent_dir(&self.file_path);
        create_private_dir(dir)?;
        let mut options = OpenOptions::new();
        options.create(true).append(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&self.file_path)?;
        file.write_all(bytes)?;
        chmod_private(&self.file_path, 0o600);
        Ok(())
    }

    fn compact(
        &self,
        entries: &IndexMap<String, Vec<GroupHistoryEntry>>,
        limits: &IndexMap<String, f64>,
    ) -> io::Result<()> {
        let dir = parent_dir(&self.file_path);
        create_private_dir(dir)?;

        let mut data = Vec::new();
        let recorded_at = now_millis();
        for (key, values) in entries {
            let limit = limits.get(key).copied().unwrap_or(values.len() as f64);
            for entry in values {
                let record = MessageRecord {
                    record_type: "message",
                    key,
                    limit,
                    entry,
                    recorded_at,
                };
                serde_json::to_writer(&mut data, &record).map_err(json_io_error)?;
                data.push(b'\n');
            }
        }

        let temp_path = create_temp_path(dir);
        let write_result = (|| {
            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut file = options.open(&temp_path)?;
            file.write_all(&data)?;
            file.flush()?;
            drop(file);
            chmod_private(&temp_path, 0o600);
            fs::rename(&temp_path, &self.file_path)?;
            chmod_private(&self.file_path, 0o600);
            Ok(())
        })();
        if write_result.is_err() {
            let _ = fs::remove_file(&temp_path);
        }
        write_result
    }
}

fn parse_record(value: Value) -> Option<GroupHistoryRecord> {
    let object = value.as_object()?;
    let key = object.get("key")?.as_str()?.to_owned();
    match object.get("type")?.as_str()? {
        "clear" => Some(GroupHistoryRecord::Clear { key }),
        "message" => {
            let limit = object.get("limit")?.as_f64()?;
            let entry = object.get("entry")?.as_object()?;
            let sender_id = entry.get("senderId")?.as_str()?.to_owned();
            let sender_name = entry.get("senderName")?.as_str()?.to_owned();
            let text = entry.get("text")?.as_str()?.to_owned();
            let timestamp = entry.get("timestamp")?.as_f64()?;
            let message_id = entry
                .get("messageId")
                .and_then(Value::as_str)
                .map(str::to_owned);
            Some(GroupHistoryRecord::Message {
                key,
                limit,
                entry: GroupHistoryEntry {
                    sender_id,
                    sender_name,
                    text,
                    message_id,
                    timestamp,
                },
            })
        }
        _ => None,
    }
}

fn normalize_limit(limit: f64) -> f64 {
    if !limit.is_finite() || limit <= 0.0 {
        0.0
    } else {
        limit.floor()
    }
}

fn truncate_to_limit(entries: &mut Vec<GroupHistoryEntry>, limit: f64) {
    if limit <= 0.0 {
        entries.clear();
    } else if limit < entries.len() as f64 {
        let keep = limit as usize;
        entries.drain(..entries.len() - keep);
    }
}

fn evict_old_keys(
    entries: &mut IndexMap<String, Vec<GroupHistoryEntry>>,
    limits: &mut IndexMap<String, f64>,
    max_keys: usize,
) -> bool {
    let mut evicted = false;
    while entries.len() > max_keys {
        let Some((oldest, _)) = entries.shift_remove_index(0) else {
            break;
        };
        limits.shift_remove(&oldest);
        evicted = true;
    }
    evicted
}

fn parent_dir(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."))
}

fn create_private_dir(path: &Path) -> io::Result<()> {
    fs::create_dir_all(path)?;
    chmod_private(path, 0o700);
    Ok(())
}

fn chmod_private(path: &Path, mode: u32) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Ok(metadata) = fs::metadata(path) {
            let mut permissions = metadata.permissions();
            permissions.set_mode(mode);
            let _ = fs::set_permissions(path, permissions);
        }
    }
    #[cfg(not(unix))]
    let _ = (path, mode);
}

fn create_temp_path(dir: &Path) -> PathBuf {
    static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);
    let sequence = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
    dir.join(format!(
        "{}-{}-{}.tmp",
        now_millis(),
        std::process::id(),
        sequence
    ))
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

fn json_io_error(error: serde_json::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

#[cfg(test)]
mod tests {
    use super::{GroupHistoryEntry, GroupHistoryStore, GroupHistoryStoreOptions};
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn path() -> PathBuf {
        static NEXT_DIR: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "canopy-group-history-{}-{}",
            std::process::id(),
            NEXT_DIR.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        dir.join("history.jsonl")
    }

    fn entry(text: &str) -> GroupHistoryEntry {
        GroupHistoryEntry {
            sender_id: "u1".to_owned(),
            sender_name: "U1".to_owned(),
            text: text.to_owned(),
            message_id: None,
            timestamp: 1.0,
        }
    }

    #[test]
    fn zero_or_negative_limit_does_not_create_file() {
        let file = path();
        let store = GroupHistoryStore::with_defaults(&file);
        store.record("k", entry("a"), 0.0).unwrap();
        store.record("k", entry("b"), -1.0).unwrap();
        assert!(!file.exists());
        assert_eq!(store.size(Some("k")).unwrap(), 0);
        assert!(store.drain("k", 10.0).unwrap().is_empty());
    }

    #[test]
    fn keeps_and_replays_only_latest_entries_per_limit() {
        let file = path();
        let store = GroupHistoryStore::with_defaults(&file);
        for text in ["a", "b", "c"] {
            store.record("k", entry(text), 2.0).unwrap();
        }
        let replayed = GroupHistoryStore::with_defaults(&file)
            .drain("k", 2.0)
            .unwrap();
        assert_eq!(
            replayed
                .iter()
                .map(|item| item.text.as_str())
                .collect::<Vec<_>>(),
            ["b", "c"]
        );
    }

    #[test]
    fn malformed_lines_are_skipped_then_removed_by_compaction() {
        let file = path();
        fs::write(
            &file,
            "{\"type\":\"message\",\"key\":\"k\",\"limit\":10,\"entry\":{\"senderId\":\"u1\",\"senderName\":\"U1\",\"text\":\"a\",\"timestamp\":1},\"recordedAt\":1}\n{bad json\n",
        )
        .unwrap();
        let store = GroupHistoryStore::with_defaults(&file);
        store.record("k", entry("b"), 10.0).unwrap();
        let text = fs::read_to_string(&file).unwrap();
        assert!(!text.contains("{bad json"));
        let loaded = GroupHistoryStore::with_defaults(&file)
            .drain("k", 10.0)
            .unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].text, "a");
        assert_eq!(loaded[1].text, "b");
    }

    #[test]
    fn drain_clear_and_clear_all_persist() {
        let file = path();
        let store = GroupHistoryStore::with_defaults(&file);
        store.record("a", entry("a"), 10.0).unwrap();
        store.record("b", entry("b"), 10.0).unwrap();
        assert_eq!(store.drain("a", 0.0).unwrap().len(), 0);
        assert_eq!(
            GroupHistoryStore::with_defaults(&file)
                .size(Some("a"))
                .unwrap(),
            0
        );
        store.clear("b").unwrap();
        assert_eq!(store.size(None).unwrap(), 0);

        store.record("c", entry("c"), 10.0).unwrap();
        store.record("d", entry("d"), 10.0).unwrap();
        store.clear_all().unwrap();
        assert_eq!(
            GroupHistoryStore::with_defaults(&file).size(None).unwrap(),
            0
        );
        assert_eq!(fs::read_to_string(&file).unwrap(), "");
    }

    #[test]
    fn evicts_oldest_key_and_refreshes_key_order() {
        let file = path();
        let store = GroupHistoryStore::new(
            &file,
            GroupHistoryStoreOptions {
                max_keys: 2,
                compact_after_records: 1_000,
            },
        );
        store.record("a", entry("a1"), 10.0).unwrap();
        store.record("b", entry("b"), 10.0).unwrap();
        store.record("a", entry("a2"), 10.0).unwrap();
        store.record("c", entry("c"), 10.0).unwrap();
        assert_eq!(store.size(Some("a")).unwrap(), 2);
        assert_eq!(store.size(Some("b")).unwrap(), 0);
        assert_eq!(store.size(Some("c")).unwrap(), 1);
        let replayed = GroupHistoryStore::new(
            &file,
            GroupHistoryStoreOptions {
                max_keys: 2,
                compact_after_records: 1_000,
            },
        );
        assert_eq!(replayed.size(Some("a")).unwrap(), 2);
        assert_eq!(replayed.size(Some("b")).unwrap(), 0);
    }

    #[test]
    fn threshold_compaction_keeps_jsonl_state_and_private_modes() {
        let file = path();
        let store = GroupHistoryStore::new(
            &file,
            GroupHistoryStoreOptions {
                max_keys: 1_000,
                compact_after_records: 2,
            },
        );
        store.record("k", entry("a"), 10.0).unwrap();
        store.record("k", entry("b"), 10.0).unwrap();
        store.record("k", entry("c"), 10.0).unwrap();
        assert_eq!(fs::read_to_string(&file).unwrap().lines().count(), 3);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&file).unwrap().permissions().mode() & 0o777,
                0o600
            );
            assert_eq!(
                fs::metadata(file.parent().unwrap())
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o700
            );
        }
    }
}
