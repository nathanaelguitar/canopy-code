use std::collections::HashMap;
use std::fs::Metadata;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

const MAX_CACHE_ENTRIES: usize = 4_096;

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
struct FileIdentity {
    device: u64,
    inode: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Fingerprint {
    modified_seconds: i64,
    modified_nanoseconds: i64,
    size: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileReadEntry {
    pub path: PathBuf,
    pub size_bytes: u64,
    pub last_read_was_full: bool,
    pub last_read_cacheable: bool,
    pub read_resident_in_history: bool,
    pub last_read_at: Option<u64>,
    pub last_write_at: Option<u64>,
    fingerprint: Fingerprint,
    last_used: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FileReadCheckResult {
    Fresh(FileReadEntry),
    Stale(FileReadEntry),
    Unverifiable,
    Unknown,
}

#[derive(Default)]
struct CacheState {
    entries: HashMap<FileIdentity, FileReadEntry>,
    counter: u64,
}

/// Tracks file identities and their metadata snapshots for one live session.
/// The cache is bounded and cloneable so read-only tools can share it safely.
#[derive(Clone, Default)]
pub struct FileReadCache {
    state: Arc<Mutex<CacheState>>,
}

impl FileReadCache {
    pub fn record_read(
        &self,
        path: impl AsRef<Path>,
        metadata: &Metadata,
        full: bool,
        cacheable: bool,
    ) {
        let Some((identity, fingerprint)) = identity_and_fingerprint(metadata) else {
            return;
        };
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        let now = system_time_millis();
        let tick = next_tick(&mut state);
        let existing = state.entries.get_mut(&identity);
        if let Some(entry) = existing {
            let same_fingerprint = entry.fingerprint == fingerprint;
            entry.path = path.as_ref().to_path_buf();
            entry.fingerprint = fingerprint;
            entry.size_bytes = fingerprint.size;
            entry.last_read_at = Some(now);
            entry.last_used = tick;
            if full {
                entry.read_resident_in_history = true;
            }
            if same_fingerprint {
                entry.last_read_was_full |= full;
                entry.last_read_cacheable |= cacheable;
            } else {
                entry.last_read_was_full = full;
                entry.last_read_cacheable = cacheable;
            }
            return;
        }
        evict_if_full(&mut state);
        state.entries.insert(
            identity,
            FileReadEntry {
                path: path.as_ref().to_path_buf(),
                size_bytes: fingerprint.size,
                last_read_was_full: full,
                last_read_cacheable: cacheable,
                read_resident_in_history: full,
                last_read_at: Some(now),
                last_write_at: None,
                fingerprint,
                last_used: tick,
            },
        );
    }

    pub fn record_write(&self, path: impl AsRef<Path>, metadata: &Metadata, cacheable: bool) {
        let Some((identity, fingerprint)) = identity_and_fingerprint(metadata) else {
            return;
        };
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        let tick = next_tick(&mut state);
        if !state.entries.contains_key(&identity) {
            evict_if_full(&mut state);
        }
        let now = system_time_millis();
        let entry = state
            .entries
            .entry(identity)
            .or_insert_with(|| FileReadEntry {
                path: path.as_ref().to_path_buf(),
                size_bytes: fingerprint.size,
                last_read_was_full: true,
                last_read_cacheable: cacheable,
                read_resident_in_history: true,
                last_read_at: Some(now),
                last_write_at: Some(now),
                fingerprint,
                last_used: tick,
            });
        entry.path = path.as_ref().to_path_buf();
        entry.size_bytes = fingerprint.size;
        entry.fingerprint = fingerprint;
        entry.last_read_was_full = true;
        entry.last_read_cacheable = cacheable;
        entry.read_resident_in_history = true;
        entry.last_read_at = Some(now);
        entry.last_write_at = Some(now);
        entry.last_used = tick;
    }

    pub fn check(&self, metadata: &Metadata) -> FileReadCheckResult {
        let Some((identity, fingerprint)) = identity_and_fingerprint(metadata) else {
            return FileReadCheckResult::Unverifiable;
        };
        let Ok(mut state) = self.state.lock() else {
            return FileReadCheckResult::Unknown;
        };
        let tick = next_tick(&mut state);
        let Some(entry) = state.entries.get_mut(&identity) else {
            return FileReadCheckResult::Unknown;
        };
        entry.last_used = tick;
        if entry.fingerprint == fingerprint {
            FileReadCheckResult::Fresh(entry.clone())
        } else {
            FileReadCheckResult::Stale(entry.clone())
        }
    }

    pub fn mark_read_evicted_from_history(&self, metadata: &Metadata) -> bool {
        let Some((identity, _)) = identity_and_fingerprint(metadata) else {
            return false;
        };
        let Ok(mut state) = self.state.lock() else {
            return false;
        };
        let Some(entry) = state.entries.get_mut(&identity) else {
            return false;
        };
        entry.read_resident_in_history = false;
        true
    }

    pub fn invalidate(&self, metadata: &Metadata) -> bool {
        let Some((identity, _)) = identity_and_fingerprint(metadata) else {
            return false;
        };
        self.state
            .lock()
            .map(|mut state| state.entries.remove(&identity).is_some())
            .unwrap_or(false)
    }

    /// Best-effort fallback for callers that have a path but can no longer
    /// resolve the file's original inode. Like `path.resolve` in the source,
    /// this normalizes `.` and `..` without following symlinks.
    pub fn invalidate_by_path(&self, path: impl AsRef<Path>) -> bool {
        let target = normalized_absolute_path(path.as_ref());
        let Ok(mut state) = self.state.lock() else {
            return false;
        };
        let before = state.entries.len();
        state
            .entries
            .retain(|_, entry| normalized_absolute_path(&entry.path) != target);
        before != state.entries.len()
    }

    pub fn len(&self) -> usize {
        self.state
            .lock()
            .map(|state| state.entries.len())
            .unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Remove every tracked file. Used during shutdown and hard memory
    /// pressure, matching `FileReadCache.clear()` in the TypeScript runtime.
    pub fn clear(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.entries.clear();
        }
    }

    /// Evict entries whose most recent read or write is older than `minutes`.
    /// Recently accessed file identity records stay available to the file
    /// mutation guards. Non-finite values and values below one minute are
    /// ignored, matching the source cache's validation.
    pub fn evict_not_accessed_since(&self, minutes: f64) -> usize {
        if !minutes.is_finite() || minutes < 1.0 {
            return 0;
        }
        let cutoff = system_time_millis().saturating_sub((minutes * 60_000.0) as u64);
        self.evict_not_accessed_before(cutoff)
    }

    fn evict_not_accessed_before(&self, cutoff_ms: u64) -> usize {
        let Ok(mut state) = self.state.lock() else {
            return 0;
        };
        let before = state.entries.len();
        state.entries.retain(|_, entry| {
            entry
                .last_read_at
                .is_none_or(|last_read| last_read >= cutoff_ms)
        });
        before - state.entries.len()
    }

    /// Verify a file has been read as text and has not changed since that read.
    /// A missing file is allowed for create-new-file operations.
    pub fn ensure_prior_read(&self, path: impl AsRef<Path>, verb: &str) -> Result<(), String> {
        let path = path.as_ref();
        let metadata = match std::fs::metadata(path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => {
                return Err(format!(
                    "Could not inspect {} to verify the prior read ({error}). Re-read it before {verb} it.",
                    path.display()
                ));
            }
        };
        if metadata.is_dir() {
            return Err(format!(
                "{} is a directory. File tools only operate on regular files.",
                path.display()
            ));
        }
        if !metadata.is_file() {
            return Err(format!(
                "{} is not a regular file and cannot be {verb} by a file tool.",
                path.display()
            ));
        }
        match self.check(&metadata) {
            FileReadCheckResult::Fresh(entry)
                if entry.last_read_at.is_some() && entry.last_read_cacheable =>
            {
                Ok(())
            }
            FileReadCheckResult::Fresh(entry) if entry.last_read_at.is_some() => Err(format!(
                "{} was read as a non-text payload and cannot be {verb} as text.",
                path.display()
            )),
            FileReadCheckResult::Stale(_) => Err(format!(
                "{} has changed since it was read. Re-read it before {verb} it.",
                path.display()
            )),
            FileReadCheckResult::Unverifiable => Err(format!(
                "The filesystem cannot verify the identity of {}. Use another method to {verb} it.",
                path.display()
            )),
            FileReadCheckResult::Unknown | FileReadCheckResult::Fresh(_) => Err(format!(
                "Read {} with the read_file tool before {verb} it.",
                path.display()
            )),
        }
    }

    /// Verify that a structured document such as a notebook was fully read
    /// and has not changed. Structured reads are intentionally non-text, so
    /// they must not satisfy the ordinary text-edit guard.
    pub fn ensure_prior_full_read(&self, path: impl AsRef<Path>, verb: &str) -> Result<(), String> {
        let path = path.as_ref();
        let metadata = match std::fs::metadata(path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(format!(
                    "{} disappeared after it was read. Re-read it before {verb} it.",
                    path.display()
                ));
            }
            Err(error) => {
                return Err(format!(
                    "Could not inspect {} to verify the prior full read ({error}). Re-read it before {verb} it.",
                    path.display()
                ));
            }
        };
        if !metadata.is_file() {
            return Err(format!(
                "{} is not a regular file and cannot be {verb} by a file tool.",
                path.display()
            ));
        }
        match self.check(&metadata) {
            FileReadCheckResult::Fresh(entry)
                if entry.last_read_at.is_some() && entry.last_read_was_full =>
            {
                Ok(())
            }
            FileReadCheckResult::Fresh(_) => Err(format!(
                "{} was not fully read. Read the complete document before {verb} it.",
                path.display()
            )),
            FileReadCheckResult::Stale(_) => Err(format!(
                "{} has changed since it was read. Re-read it before {verb} it.",
                path.display()
            )),
            FileReadCheckResult::Unverifiable => Err(format!(
                "The filesystem cannot verify the identity of {}. Use another method to {verb} it.",
                path.display()
            )),
            FileReadCheckResult::Unknown => Err(format!(
                "Read {} with the read_file tool before {verb} it.",
                path.display()
            )),
        }
    }
}

fn next_tick(state: &mut CacheState) -> u64 {
    state.counter = state.counter.saturating_add(1);
    state.counter
}

fn system_time_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn normalized_absolute_path(path: &Path) -> PathBuf {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(path)
    };
    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                if !normalized.pop() && !normalized.has_root() {
                    normalized.push(component.as_os_str());
                }
            }
            other => normalized.push(other.as_os_str()),
        }
    }
    normalized
}

fn evict_if_full(state: &mut CacheState) {
    if state.entries.len() < MAX_CACHE_ENTRIES {
        return;
    }
    if let Some(oldest) = state
        .entries
        .iter()
        .min_by_key(|(_, entry)| entry.last_used)
        .map(|(identity, _)| *identity)
    {
        state.entries.remove(&oldest);
    }
}

fn identity_and_fingerprint(metadata: &Metadata) -> Option<(FileIdentity, Fingerprint)> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let inode = metadata.ino();
        if inode == 0 {
            return None;
        }
        Some((
            FileIdentity {
                device: metadata.dev(),
                inode,
            },
            Fingerprint {
                modified_seconds: metadata.mtime(),
                modified_nanoseconds: metadata.mtime_nsec(),
                size: metadata.len(),
            },
        ))
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    struct TempWorkspace(PathBuf);

    impl TempWorkspace {
        fn new() -> Self {
            let root =
                std::env::temp_dir().join(format!("canopy-file-read-cache-{}", Uuid::new_v4()));
            std::fs::create_dir_all(&root).unwrap();
            Self(root)
        }
    }

    impl Drop for TempWorkspace {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn tracks_a_read_by_file_identity_and_detects_metadata_drift() {
        let workspace = TempWorkspace::new();
        let path = workspace.0.join("file.txt");
        std::fs::write(&path, "current content").unwrap();
        let cache = FileReadCache::default();

        cache.ensure_prior_read(&path, "editing").unwrap_err();
        let metadata = std::fs::metadata(&path).unwrap();
        cache.record_read(&path, &metadata, false, true);
        cache.ensure_prior_read(&path, "editing").unwrap();
        assert_eq!(cache.len(), 1);

        std::fs::write(&path, "external change has different size").unwrap();
        assert!(
            cache
                .ensure_prior_read(&path, "overwriting")
                .unwrap_err()
                .contains("changed since it was read")
        );
    }

    #[test]
    fn distinguishes_non_text_reads_and_allows_new_file_creation() {
        let workspace = TempWorkspace::new();
        let path = workspace.0.join("binary.dat");
        std::fs::write(&path, [0_u8, 1, 2, 3]).unwrap();
        let cache = FileReadCache::default();
        let metadata = std::fs::metadata(&path).unwrap();
        cache.record_read(&path, &metadata, false, false);
        assert!(
            cache
                .ensure_prior_read(&path, "editing")
                .unwrap_err()
                .contains("non-text payload")
        );

        let new_path = workspace.0.join("new.txt");
        cache.ensure_prior_read(&new_path, "writing").unwrap();
    }

    #[test]
    fn rejects_directories_and_supports_invalidating_a_tracked_file() {
        let workspace = TempWorkspace::new();
        let directory = workspace.0.join("folder");
        std::fs::create_dir(&directory).unwrap();
        let cache = FileReadCache::default();
        assert!(
            cache
                .ensure_prior_read(&directory, "editing")
                .unwrap_err()
                .contains("directory")
        );

        let path = workspace.0.join("file.txt");
        std::fs::write(&path, "text").unwrap();
        let metadata = std::fs::metadata(&path).unwrap();
        cache.record_read(&path, &metadata, true, true);
        assert!(cache.invalidate(&metadata));
        assert_eq!(cache.check(&metadata), FileReadCheckResult::Unknown);
    }

    #[test]
    fn evicts_only_reads_older_than_the_requested_memory_pressure_window() {
        let workspace = TempWorkspace::new();
        let old_path = workspace.0.join("old.txt");
        let recent_path = workspace.0.join("recent.txt");
        std::fs::write(&old_path, "old").unwrap();
        std::fs::write(&recent_path, "recent").unwrap();
        let cache = FileReadCache::default();
        let old_metadata = std::fs::metadata(&old_path).unwrap();
        let recent_metadata = std::fs::metadata(&recent_path).unwrap();
        cache.record_read(&old_path, &old_metadata, true, true);
        cache.record_read(&recent_path, &recent_metadata, true, true);

        {
            let mut state = cache.state.lock().unwrap();
            state
                .entries
                .get_mut(&identity_and_fingerprint(&old_metadata).unwrap().0)
                .unwrap()
                .last_read_at = Some(1);
        }

        assert_eq!(cache.evict_not_accessed_since(f64::NAN), 0);
        assert_eq!(cache.evict_not_accessed_since(0.5), 0);
        assert_eq!(cache.evict_not_accessed_since(1.0), 1);
        assert_eq!(cache.check(&old_metadata), FileReadCheckResult::Unknown);
        assert!(matches!(
            cache.check(&recent_metadata),
            FileReadCheckResult::Fresh(_)
        ));
    }

    #[test]
    fn clear_removes_every_tracked_file() {
        let workspace = TempWorkspace::new();
        let path = workspace.0.join("file.txt");
        std::fs::write(&path, "text").unwrap();
        let cache = FileReadCache::default();
        let metadata = std::fs::metadata(&path).unwrap();
        cache.record_read(&path, &metadata, true, true);

        cache.clear();

        assert!(cache.is_empty());
        assert_eq!(cache.check(&metadata), FileReadCheckResult::Unknown);
    }

    #[test]
    fn invalidates_entries_by_normalized_path_when_inode_is_unavailable() {
        let workspace = TempWorkspace::new();
        let path = workspace.0.join("nested/file.txt");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, "text").unwrap();
        let cache = FileReadCache::default();
        let metadata = std::fs::metadata(&path).unwrap();
        cache.record_read(&path, &metadata, true, true);

        let alias = workspace.0.join("nested/../nested/file.txt");
        assert!(cache.invalidate_by_path(alias));
        assert!(!cache.invalidate_by_path(path));
        assert!(cache.is_empty());
    }
}
