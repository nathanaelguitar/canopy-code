//! Per-session backups of files changed through Canopy's file tools.
//!
//! This Rust tranche of `fileHistoryService.ts` covers persisted snapshots,
//! restoration and validation, pre-edit tracking, backup inheritance, bounded
//! retention, memory-capped diff summaries, and file rewind. The global
//! history root, session ID, and working directory are explicit so this
//! service does not depend on CLI process state.

use std::collections::{HashSet, VecDeque};
use std::io;
use std::path::{Component, Path, PathBuf};

use chrono::{DateTime, SecondsFormat, Utc};
use indexmap::{IndexMap, IndexSet};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};
use similar::{ChangeTag, TextDiff};
use tokio::fs;
use tokio::io::AsyncReadExt;

pub const MAX_SNAPSHOTS: usize = 100;
pub const FILE_HISTORY_DIR: &str = "file-history";
/// Maximum endpoint size loaded for text diff generation. Large files are
/// surfaced as oversized rows without reading their full contents.
pub const MAX_DIFF_SIZE_BYTES: u64 = 1_000_000;
/// Maximum number of candidate paths inspected for one turn diff.
pub const MAX_TURN_DIFF_FILES: usize = 500;
const BINARY_SNIFF_BYTES: usize = 8 * 1024;

/// One file backup referenced by a session snapshot.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileHistoryBackup {
    /// `None` means the file did not exist at this snapshot (or the source
    /// disappeared while it was being copied).
    pub backup_file_name: Option<String>,
    #[serde(with = "json_number")]
    pub version: f64,
    #[serde(with = "iso_millis")]
    pub backup_time: DateTime<Utc>,
    /// True when the per-file backup attempt failed. False is omitted from
    /// serialized snapshots, matching the TypeScript JSONL schema.
    #[serde(default, skip_serializing_if = "is_false")]
    pub failed: bool,
}

/// The exact snapshot object persisted in session history records.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileHistorySnapshot {
    pub prompt_id: String,
    pub tracked_file_backups: IndexMap<String, FileHistoryBackup>,
    #[serde(with = "iso_millis")]
    pub timestamp: DateTime<Utc>,
}

/// Aggregate line-change counts for a snapshot compared with the live tree.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiffStats {
    pub files_changed: Vec<String>,
    pub insertions: usize,
    pub deletions: usize,
}

/// Result of applying a file-history snapshot during rewind.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RewindResult {
    pub files_changed: Vec<String>,
    pub files_failed: Vec<String>,
}

/// A unified-diff hunk. `lines` contain the leading space, `+`, or `-`
/// marker, with line terminators removed as in the TypeScript `diff` Hunk.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiffHunk {
    pub old_start: usize,
    pub old_lines: usize,
    pub new_start: usize,
    pub new_lines: usize,
    pub lines: Vec<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnFileDiff {
    pub file_path: String,
    pub hunks: Vec<DiffHunk>,
    pub is_new_file: bool,
    pub is_deleted: bool,
    pub lines_added: usize,
    pub lines_removed: usize,
    pub oversized: bool,
    pub is_binary: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnDiffStats {
    pub files_changed: usize,
    pub lines_added: usize,
    pub lines_removed: usize,
    pub files_omitted: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnDiff {
    pub prompt_id: String,
    #[serde(with = "iso_millis")]
    pub timestamp: DateTime<Utc>,
    pub files: Vec<TurnFileDiff>,
    pub stats: TurnDiffStats,
}

/// Resolve a generated backup name below the per-session history directory.
///
/// Names are restricted to the format emitted by `getBackupFileName`, and
/// both the session ID and name must be single path components. This keeps
/// restored/caller-supplied strings from escaping the injected history root
/// lexically. Filesystem callers must also use the checked helpers below,
/// which reject symlinks in the history/session directory and destination.
pub fn resolve_backup_path(
    global_history_root: &Path,
    session_id: &str,
    backup_file_name: &str,
) -> Result<PathBuf, String> {
    validate_component(session_id).map_err(|_| "invalid file-history session ID".to_owned())?;
    validate_backup_file_name(backup_file_name)?;

    let base_dir = global_history_root.join(FILE_HISTORY_DIR).join(session_id);
    let backup_path = base_dir.join(backup_file_name);
    if backup_path.parent() != Some(base_dir.as_path()) {
        return Err(format!(
            "backupFileName escapes base directory: {backup_file_name}"
        ));
    }
    Ok(backup_path)
}

/// Stateful file history for one session.
#[derive(Debug)]
pub struct FileHistoryService {
    global_history_root: PathBuf,
    session_id: String,
    cwd: PathBuf,
    enabled: bool,
    snapshots: Vec<FileHistorySnapshot>,
    tracked_files: IndexSet<String>,
    pending_snapshot_updates: VecDeque<FileHistorySnapshot>,
}

impl FileHistoryService {
    pub fn new(
        global_history_root: impl Into<PathBuf>,
        session_id: impl Into<String>,
        cwd: impl Into<PathBuf>,
        enabled: bool,
    ) -> Self {
        let cwd = cwd.into();
        let cwd = std::fs::canonicalize(&cwd).unwrap_or(cwd);
        Self {
            global_history_root: global_history_root.into(),
            session_id: session_id.into(),
            cwd,
            enabled,
            snapshots: Vec::new(),
            tracked_files: IndexSet::new(),
            pending_snapshot_updates: VecDeque::new(),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn get_snapshots(&self) -> &[FileHistorySnapshot] {
        &self.snapshots
    }

    /// Drain snapshots whose latest state should be persisted by the caller.
    pub fn take_pending_snapshot_updates(&mut self) -> Vec<FileHistorySnapshot> {
        std::mem::take(&mut self.pending_snapshot_updates)
            .into_iter()
            .collect()
    }

    fn queue_snapshot_update(&mut self, snapshot: FileHistorySnapshot) {
        self.pending_snapshot_updates.push_back(snapshot);
        if self.pending_snapshot_updates.len() > MAX_SNAPSHOTS {
            self.pending_snapshot_updates.pop_front();
        }
    }

    /// Restore persisted snapshots, normalize their file keys for this
    /// working directory, and rebuild the insertion-ordered tracked-file set.
    /// Older snapshots are dropped if the input exceeds the service limit;
    /// backups referenced only by those dropped snapshots are cleaned up on a
    /// best-effort basis.
    pub async fn restore_from_snapshots(&mut self, snapshots: Vec<FileHistorySnapshot>) {
        self.pending_snapshot_updates.clear();
        let mut restored = Vec::with_capacity(snapshots.len().min(MAX_SNAPSHOTS));
        for snapshot in snapshots {
            let mut tracked_file_backups = IndexMap::new();
            for (path, backup) in snapshot.tracked_file_backups {
                let tracking_path = self.maybe_shorten_file_path(&path);
                tracked_file_backups.insert(tracking_path, backup);
            }
            restored.push(FileHistorySnapshot {
                prompt_id: snapshot.prompt_id,
                tracked_file_backups,
                timestamp: snapshot.timestamp,
            });
        }

        let overflow = restored.len().saturating_sub(MAX_SNAPSHOTS);
        let removed: Vec<FileHistorySnapshot> = restored.drain(..overflow).collect();
        let mut tracked_files = IndexSet::new();
        for snapshot in &restored {
            tracked_files.extend(snapshot.tracked_file_backups.keys().cloned());
        }

        self.snapshots = restored;
        self.tracked_files = tracked_files;
        if !removed.is_empty() {
            self.cleanup_orphaned_backups(&removed).await;
        }
    }

    /// Restore the snapshot form decoded by the session-history accumulator.
    /// Snapshot order and timestamps are preserved. Its backup maps use a
    /// `HashMap`, so file-key order is reconstructed in sorted order as a
    /// deterministic best effort.
    pub async fn restore_from_session_snapshots(
        &mut self,
        snapshots: Vec<crate::services::session_file_history_state::FileHistorySnapshot>,
    ) {
        let snapshots = snapshots
            .into_iter()
            .map(|snapshot| {
                let mut entries: Vec<_> = snapshot.tracked_file_backups.into_iter().collect();
                entries.sort_by(|(left, _), (right, _)| left.cmp(right));
                let mut tracked_file_backups = IndexMap::with_capacity(entries.len());
                for (path, backup) in entries {
                    let version = backup.version.unwrap_or_else(|| {
                        backup
                            .backup_file_name
                            .as_deref()
                            .and_then(version_from_backup_name)
                            .unwrap_or(0.0)
                    });
                    tracked_file_backups.insert(
                        path,
                        FileHistoryBackup {
                            backup_file_name: backup.backup_file_name,
                            version,
                            backup_time: backup.backup_time,
                            failed: backup.failed.unwrap_or(false),
                        },
                    );
                }
                FileHistorySnapshot {
                    prompt_id: snapshot.prompt_id,
                    tracked_file_backups,
                    timestamp: snapshot.timestamp,
                }
            })
            .collect();
        self.restore_from_snapshots(snapshots).await;
    }

    /// Check every unique persisted backup path once. Missing, unreadable,
    /// malformed, and symlinked backups are marked failed in every snapshot
    /// that refers to them. Opening and reading one byte also catches files
    /// that can be statted but cannot be read, without loading whole backups.
    pub async fn validate_restored_snapshots(&mut self) {
        let unique_names = self.unique_restored_backup_names();
        if unique_names.is_empty() {
            return;
        }

        let mut invalid_names = HashSet::new();
        for backup_file_name in unique_names {
            let Ok(path) = checked_backup_path_async(
                &self.global_history_root,
                &self.session_id,
                &backup_file_name,
                false,
            )
            .await
            else {
                invalid_names.insert(backup_file_name);
                continue;
            };

            let readable = async {
                let mut file = fs::File::open(path).await?;
                let mut byte = [0_u8; 1];
                let bytes_read = file.read(&mut byte).await?;
                if bytes_read > byte.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "file read returned more bytes than requested",
                    ));
                }
                Ok::<(), io::Error>(())
            }
            .await
            .is_ok();
            if !readable {
                invalid_names.insert(backup_file_name);
            }
        }

        if invalid_names.is_empty() {
            return;
        }
        let mut changed_snapshots = Vec::new();
        for snapshot in &mut self.snapshots {
            let mut changed = false;
            for backup in snapshot.tracked_file_backups.values_mut() {
                if backup
                    .backup_file_name
                    .as_ref()
                    .is_some_and(|name| invalid_names.contains(name))
                {
                    backup.failed = true;
                    changed = true;
                }
            }
            if changed {
                changed_snapshots.push(snapshot.clone());
            }
        }
        for snapshot in changed_snapshots {
            self.queue_snapshot_update(snapshot);
        }
    }

    fn unique_restored_backup_names(&self) -> HashSet<String> {
        self.snapshots
            .iter()
            .flat_map(|snapshot| snapshot.tracked_file_backups.values())
            .filter(|backup| !backup.failed)
            .filter_map(|backup| backup.backup_file_name.clone())
            .collect()
    }

    /// Save the file's pre-edit contents on the latest snapshot. Filesystem
    /// failures are intentionally swallowed so file tools can continue, as
    /// they are in the TypeScript implementation.
    pub fn track_edit(&mut self, file_path: &str) {
        if !self.enabled {
            return;
        }
        let Some(snapshot_index) = self.snapshots.len().checked_sub(1) else {
            return;
        };

        let tracking_path = self.maybe_shorten_file_path(file_path);
        if self.snapshots[snapshot_index]
            .tracked_file_backups
            .get(&tracking_path)
            .is_some_and(|backup| !backup.failed)
        {
            return;
        }

        let version = self.max_version(&tracking_path) + 1.0;
        let source_path = self.resolve_working_path(file_path);
        let hash_path = source_path.to_string_lossy();
        let Ok(backup) = self.create_backup_sync(&source_path, &hash_path, version) else {
            return;
        };

        let updated = {
            // Recheck the slot to retain the source's retry rule if the entry
            // has already been populated by another operation.
            let snapshot = &mut self.snapshots[snapshot_index];
            if snapshot
                .tracked_file_backups
                .get(&tracking_path)
                .is_none_or(|existing| existing.failed)
            {
                snapshot
                    .tracked_file_backups
                    .insert(tracking_path.clone(), backup);
                true
            } else {
                false
            }
        };
        if updated {
            self.tracked_files.insert(tracking_path);
            let snapshot_update = self.snapshots[snapshot_index].clone();
            self.queue_snapshot_update(snapshot_update);
        }
    }

    /// Create the next turn snapshot, inheriting confirmed backups for files
    /// that have not changed. Per-file backup errors become failed entries;
    /// they do not fail the enclosing turn.
    pub async fn make_snapshot(&mut self, prompt_id: impl Into<String>) {
        if !self.enabled {
            return;
        }

        let prompt_id = prompt_id.into();
        let previous = self.snapshots.last().cloned();
        let tracked_paths: Vec<String> = self.tracked_files.iter().cloned().collect();
        let mut tracked_file_backups = IndexMap::new();

        for tracking_path in &tracked_paths {
            let file_path = self.expand_file_path(tracking_path);
            let version = self.max_version(tracking_path) + 1.0;
            let previous_backup = previous
                .as_ref()
                .and_then(|snapshot| snapshot.tracked_file_backups.get(tracking_path));

            match fs::metadata(&file_path).await {
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    tracked_file_backups.insert(
                        tracking_path.clone(),
                        FileHistoryBackup {
                            backup_file_name: None,
                            version,
                            backup_time: Utc::now(),
                            failed: false,
                        },
                    );
                }
                Err(_) => {
                    tracked_file_backups.insert(
                        tracking_path.clone(),
                        failed_backup(previous_backup, version),
                    );
                }
                Ok(source_metadata) => {
                    if let Some(previous_backup) = previous_backup {
                        if !previous_backup.failed {
                            if let Some(backup_name) = previous_backup.backup_file_name.as_deref() {
                                if !self
                                    .origin_file_changed(&file_path, &source_metadata, backup_name)
                                    .await
                                {
                                    tracked_file_backups
                                        .insert(tracking_path.clone(), previous_backup.clone());
                                    continue;
                                }
                            }
                        }
                    }

                    let source_for_hash = file_path.to_string_lossy();
                    let backup = self
                        .create_backup(&file_path, &source_for_hash, version)
                        .await
                        .unwrap_or_else(|_| failed_backup(previous_backup, version));
                    tracked_file_backups.insert(tracking_path.clone(), backup);
                }
            }
        }

        // Keep entries inherited from older restored state even if a path
        // somehow fell out of the insertion-ordered tracked set.
        if let Some(previous) = previous {
            for (tracking_path, backup) in previous.tracked_file_backups {
                tracked_file_backups.entry(tracking_path).or_insert(backup);
            }
        }

        self.snapshots.push(FileHistorySnapshot {
            prompt_id,
            tracked_file_backups,
            timestamp: Utc::now(),
        });

        if self.snapshots.len() > MAX_SNAPSHOTS {
            let overflow = self.snapshots.len() - MAX_SNAPSHOTS;
            let removed: Vec<FileHistorySnapshot> = self.snapshots.drain(..overflow).collect();
            self.cleanup_orphaned_backups(&removed).await;
        }

        if let Some(snapshot) = self.snapshots.last().cloned() {
            self.queue_snapshot_update(snapshot);
        }
    }

    /// Restore files to the state captured for `prompt_id`.
    ///
    /// When `truncate_history` is true, later snapshots are removed only if
    /// every tracked file was restored successfully. Missing snapshots return
    /// the same error text as the TypeScript implementation. A disabled
    /// service is a no-op, including when the prompt is unknown.
    pub async fn rewind(
        &mut self,
        prompt_id: &str,
        truncate_history: bool,
    ) -> Result<RewindResult, String> {
        if !self.enabled {
            return Ok(RewindResult::default());
        }

        let Some(target_index) = self.find_snapshot_index(prompt_id) else {
            return Err("The selected snapshot was not found".to_owned());
        };
        let target_snapshot = self.snapshots[target_index].clone();
        let result = self.apply_snapshot(&target_snapshot).await;

        if truncate_history && result.files_failed.is_empty() {
            // Re-resolve after the awaited file operations, matching the
            // source's guard against truncating a snapshot no longer present.
            if let Some(target_index) = self.find_snapshot_index(prompt_id) {
                let removed: Vec<FileHistorySnapshot> =
                    self.snapshots.drain(target_index + 1..).collect();
                self.tracked_files = self
                    .snapshots
                    .iter()
                    .flat_map(|snapshot| snapshot.tracked_file_backups.keys().cloned())
                    .collect();
                self.cleanup_orphaned_backups(&removed).await;
            }
        }

        Ok(result)
    }

    async fn apply_snapshot(&self, target_snapshot: &FileHistorySnapshot) -> RewindResult {
        let mut result = RewindResult::default();

        // Keep the source's insertion order and isolate each path: one broken
        // backup must not prevent other files from being restored.
        let tracked_paths: Vec<String> = self.tracked_files.iter().cloned().collect();
        for tracking_path in tracked_paths {
            let file_path = self.expand_file_path(&tracking_path);
            let target_backup = target_snapshot.tracked_file_backups.get(&tracking_path);

            if target_backup.is_some_and(|backup| backup.failed) {
                result
                    .files_failed
                    .push(file_path.to_string_lossy().into_owned());
                continue;
            }

            let backup_file_name = match target_backup {
                Some(backup) => Some(backup.backup_file_name.as_deref()),
                None => self.first_version_backup(&tracking_path),
            };
            let Some(backup_file_name) = backup_file_name else {
                result
                    .files_failed
                    .push(file_path.to_string_lossy().into_owned());
                continue;
            };

            match backup_file_name {
                None => match fs::remove_file(&file_path).await {
                    Ok(()) => result
                        .files_changed
                        .push(file_path.to_string_lossy().into_owned()),
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                    Err(_) => result
                        .files_failed
                        .push(file_path.to_string_lossy().into_owned()),
                },
                Some(backup_file_name) => {
                    let Ok(source_metadata) = fs::metadata(&file_path).await else {
                        // A missing/unreadable worktree file needs to be
                        // restored from the selected snapshot.
                        match self.restore_backup(&file_path, backup_file_name).await {
                            Ok(true) => result
                                .files_changed
                                .push(file_path.to_string_lossy().into_owned()),
                            Ok(false) | Err(_) => result
                                .files_failed
                                .push(file_path.to_string_lossy().into_owned()),
                        }
                        continue;
                    };
                    if !self
                        .origin_file_changed(&file_path, &source_metadata, backup_file_name)
                        .await
                    {
                        continue;
                    }
                    match self.restore_backup(&file_path, backup_file_name).await {
                        Ok(true) => result
                            .files_changed
                            .push(file_path.to_string_lossy().into_owned()),
                        Ok(false) | Err(_) => result
                            .files_failed
                            .push(file_path.to_string_lossy().into_owned()),
                    }
                }
            }
        }

        result
    }

    /// Compare a snapshot with the current worktree and return changed paths
    /// plus added/deleted line counts. Reads stay capped at `MAX_DIFF_SIZE_BYTES`;
    /// oversized files use a streaming line-count estimate so memory remains
    /// bounded independently of their size.
    pub async fn get_diff_stats(&self, prompt_id: &str) -> Option<DiffStats> {
        if !self.enabled {
            return None;
        }
        let target_index = self.find_snapshot_index(prompt_id)?;
        let target = &self.snapshots[target_index];
        let mut stats = DiffStats::default();

        for tracking_path in &self.tracked_files {
            let target_backup = target.tracked_file_backups.get(tracking_path);
            if target_backup.is_some_and(|backup| backup.failed) {
                continue;
            }
            let backup_name = match target_backup {
                Some(backup) => Some(backup.backup_file_name.as_deref()),
                None => self.first_version_backup(tracking_path),
            };
            let Some(backup_name) = backup_name else {
                continue;
            };

            let file_path = self.expand_file_path(tracking_path);
            let backup_path = match backup_name {
                Some(name) => checked_backup_path_async(
                    &self.global_history_root,
                    &self.session_id,
                    name,
                    false,
                )
                .await
                .ok(),
                None => None,
            };
            let before = self.read_endpoint(backup_name, None).await;
            let after = self.read_endpoint(None, Some(&file_path)).await;

            let (insertions, deletions, has_content_change) = match (&before, &after) {
                (EndpointRead::Oversized { .. }, _) | (_, EndpointRead::Oversized { .. }) => {
                    let before_lines = endpoint_line_count(&before, backup_path.as_deref()).await;
                    let after_lines = endpoint_line_count(&after, Some(&file_path)).await;
                    if before_lines.is_none() && after_lines.is_none() {
                        (0, 0, false)
                    } else {
                        let after_path = after.exists().then_some(file_path.as_path());
                        let same = match (backup_path.as_deref(), after_path) {
                            (Some(before_path), Some(after_path)) => {
                                files_equal(before_path, after_path).await.unwrap_or(false)
                            }
                            _ => false,
                        };
                        if same {
                            (0, 0, false)
                        } else {
                            let before_lines = before_lines.unwrap_or(0);
                            let after_lines = after_lines.unwrap_or(0);
                            let added = after_lines.saturating_sub(before_lines);
                            let removed = before_lines.saturating_sub(after_lines);
                            // A line-for-line rewrite in a large file has
                            // zero net line delta. Preserve a useful
                            // changed-file signal with a minimal 1/1
                            // estimate when streaming comparison proved it
                            // differs, while documenting the aggregate as
                            // best effort for oversized endpoints.
                            if added == 0 && removed == 0 {
                                (1, 1, true)
                            } else {
                                (added, removed, true)
                            }
                        }
                    }
                }
                _ => {
                    let before_content = endpoint_content(&before);
                    let after_content = endpoint_content(&after);
                    if before_content.is_none() && after_content.is_none() {
                        (0, 0, false)
                    } else {
                        let diff = TextDiff::from_lines(
                            before_content.unwrap_or_default(),
                            after_content.unwrap_or_default(),
                        );
                        let mut added = 0;
                        let mut removed = 0;
                        for change in diff.iter_all_changes() {
                            match change.tag() {
                                ChangeTag::Insert => added += 1,
                                ChangeTag::Delete => removed += 1,
                                ChangeTag::Equal => {}
                            }
                        }
                        (added, removed, added > 0 || removed > 0)
                    }
                }
            };

            let file_was_new = backup_name.is_none() && after.exists();
            if has_content_change || file_was_new {
                stats
                    .files_changed
                    .push(file_path.to_string_lossy().into_owned());
                stats.insertions += insertions;
                stats.deletions += deletions;
            }
        }
        Some(stats)
    }

    /// Return the file changes made during one turn. A retained snapshot is
    /// compared with its successor, or with the live worktree for the latest
    /// turn. At most `MAX_TURN_DIFF_FILES` candidates are read, with each
    /// endpoint independently bounded to `MAX_DIFF_SIZE_BYTES`.
    pub async fn get_turn_diff(&self, prompt_id: &str) -> Option<TurnDiff> {
        if !self.enabled {
            return None;
        }
        let target_index = self.find_snapshot_index(prompt_id)?;
        let target = &self.snapshots[target_index];
        let after = self.snapshots.get(target_index + 1);
        let mut candidates: Vec<&String> = target.tracked_file_backups.keys().collect();
        candidates.sort();
        let files_omitted = candidates.len().saturating_sub(MAX_TURN_DIFF_FILES);
        candidates.truncate(MAX_TURN_DIFF_FILES);

        // Process sequentially: each file retains at most two 1 MB endpoints
        // plus its patch in memory, regardless of the candidate count.
        let mut files = Vec::new();
        for tracking_path in candidates {
            if let Some(file) = self
                .compute_turn_file_diff(tracking_path, target, after)
                .await
            {
                files.push(file);
            }
        }
        files.sort_by(|left, right| left.file_path.cmp(&right.file_path));
        let lines_added = files.iter().map(|file| file.lines_added).sum();
        let lines_removed = files.iter().map(|file| file.lines_removed).sum();

        Some(TurnDiff {
            prompt_id: prompt_id.to_owned(),
            timestamp: target.timestamp,
            stats: TurnDiffStats {
                files_changed: files.len(),
                lines_added,
                lines_removed,
                files_omitted,
            },
            files,
        })
    }

    async fn compute_turn_file_diff(
        &self,
        tracking_path: &str,
        before: &FileHistorySnapshot,
        after: Option<&FileHistorySnapshot>,
    ) -> Option<TurnFileDiff> {
        let absolute_path = self.expand_file_path(tracking_path);
        let before_backup = before.tracked_file_backups.get(tracking_path);
        if before_backup.is_some_and(|backup| backup.failed) {
            return None;
        }
        let after_backup =
            after.and_then(|snapshot| snapshot.tracked_file_backups.get(tracking_path));
        if after.is_some() && after_backup.is_some_and(|backup| backup.failed) {
            return None;
        }

        let after_from_worktree = after.is_none();
        if !after_from_worktree
            && let (Some(before_backup), Some(after_backup)) = (before_backup, after_backup)
            && before_backup.backup_file_name == after_backup.backup_file_name
            && before_backup.version == after_backup.version
        {
            return None;
        }

        let before_read = self
            .read_endpoint(
                before_backup.and_then(|backup| backup.backup_file_name.as_deref()),
                None,
            )
            .await;
        if matches!(before_read, EndpointRead::Unreadable) {
            return None;
        }
        let after_read = if after_from_worktree {
            self.read_endpoint(None, Some(&absolute_path)).await
        } else {
            self.read_endpoint(
                after_backup.and_then(|backup| backup.backup_file_name.as_deref()),
                None,
            )
            .await
        };
        if matches!(after_read, EndpointRead::Unreadable) {
            return None;
        }

        let (before_exists, after_exists) = (before_read.exists(), after_read.exists());
        if matches!(before_read, EndpointRead::Oversized { .. })
            || matches!(after_read, EndpointRead::Oversized { .. })
        {
            return Some(TurnFileDiff {
                file_path: tracking_path.to_owned(),
                hunks: Vec::new(),
                is_new_file: !before_exists && after_exists,
                is_deleted: before_exists && !after_exists,
                lines_added: 0,
                lines_removed: 0,
                oversized: true,
                is_binary: false,
            });
        }

        let before_content = endpoint_content(&before_read).unwrap_or_default();
        let after_content = endpoint_content(&after_read).unwrap_or_default();
        if before_content == after_content && before_exists == after_exists {
            return None;
        }
        if looks_binary(before_content) || looks_binary(after_content) {
            return Some(TurnFileDiff {
                file_path: tracking_path.to_owned(),
                hunks: Vec::new(),
                is_new_file: !before_exists && after_exists,
                is_deleted: before_exists && !after_exists,
                lines_added: 0,
                lines_removed: 0,
                oversized: false,
                is_binary: true,
            });
        }

        // Lossy UTF-8 replacement can expand a capped on-disk byte sequence
        // past the TypeScript Buffer.byteLength limit. Keep the second guard
        // so those inputs also return a stats-only row.
        if before_content.len() as u64 > MAX_DIFF_SIZE_BYTES
            || after_content.len() as u64 > MAX_DIFF_SIZE_BYTES
        {
            let before_lines = if before_exists {
                count_lines(before_content)
            } else {
                0
            };
            let after_lines = if after_exists {
                count_lines(after_content)
            } else {
                0
            };
            return Some(TurnFileDiff {
                file_path: tracking_path.to_owned(),
                hunks: Vec::new(),
                is_new_file: !before_exists && after_exists,
                is_deleted: before_exists && !after_exists,
                lines_added: after_lines.saturating_sub(before_lines),
                lines_removed: before_lines.saturating_sub(after_lines),
                oversized: true,
                is_binary: false,
            });
        }

        let diff = TextDiff::from_lines(before_content, after_content);
        let mut hunks = Vec::new();
        let mut lines_added = 0;
        let mut lines_removed = 0;
        for hunk in diff.unified_diff().context_radius(3).iter_hunks() {
            let ops = hunk.ops();
            let first = ops.first()?;
            let last = ops.last()?;
            let old_range = first.old_range().start..last.old_range().end;
            let new_range = first.new_range().start..last.new_range().end;
            let mut lines = Vec::new();
            for change in hunk.iter_changes() {
                match change.tag() {
                    ChangeTag::Equal => {
                        lines.push(format!(" {}", without_line_ending(change.value_ref())))
                    }
                    ChangeTag::Delete => {
                        lines_removed += 1;
                        lines.push(format!("-{}", without_line_ending(change.value_ref())));
                    }
                    ChangeTag::Insert => {
                        lines_added += 1;
                        lines.push(format!("+{}", without_line_ending(change.value_ref())));
                    }
                }
            }
            hunks.push(DiffHunk {
                old_start: unified_range_start(old_range.start, old_range.end),
                old_lines: old_range.end - old_range.start,
                new_start: unified_range_start(new_range.start, new_range.end),
                new_lines: new_range.end - new_range.start,
                lines,
            });
        }
        if hunks.is_empty() && lines_added == 0 && lines_removed == 0 {
            return None;
        }
        Some(TurnFileDiff {
            file_path: tracking_path.to_owned(),
            hunks,
            is_new_file: !before_exists && after_exists,
            is_deleted: before_exists && !after_exists,
            lines_added,
            lines_removed,
            oversized: false,
            is_binary: false,
        })
    }

    async fn read_endpoint(
        &self,
        backup_file_name: Option<&str>,
        worktree_path: Option<&Path>,
    ) -> EndpointRead {
        if let Some(worktree_path) = worktree_path {
            return read_path_with_size_guard(worktree_path, false).await;
        }
        let Some(backup_name) = backup_file_name else {
            return EndpointRead::ok_missing();
        };
        let Ok(path) = checked_backup_path_async(
            &self.global_history_root,
            &self.session_id,
            backup_name,
            false,
        )
        .await
        else {
            return EndpointRead::Unreadable;
        };
        read_path_with_size_guard(&path, true).await
    }

    fn find_snapshot_index(&self, prompt_id: &str) -> Option<usize> {
        self.snapshots
            .iter()
            .rposition(|snapshot| snapshot.prompt_id == prompt_id)
    }

    fn first_version_backup(&self, tracking_path: &str) -> Option<Option<&str>> {
        self.snapshots
            .iter()
            .find_map(|snapshot| {
                snapshot
                    .tracked_file_backups
                    .get(tracking_path)
                    .filter(|backup| backup.version == 1.0)
            })
            .map(|backup| backup.backup_file_name.as_deref())
    }

    fn max_version(&self, tracking_path: &str) -> f64 {
        self.snapshots
            .iter()
            .filter_map(|snapshot| {
                snapshot
                    .tracked_file_backups
                    .get(tracking_path)
                    .map(|backup| backup.version)
            })
            .fold(0.0, f64::max)
    }

    fn create_backup_sync(
        &self,
        file_path: &Path,
        hash_path: &str,
        version: f64,
    ) -> io::Result<FileHistoryBackup> {
        let backup_file_name = get_backup_file_name(hash_path, version);

        if let Err(error) = std::fs::metadata(file_path) {
            if error.kind() == io::ErrorKind::NotFound {
                return Ok(FileHistoryBackup {
                    backup_file_name: None,
                    version,
                    backup_time: Utc::now(),
                    failed: false,
                });
            }
            return Err(error);
        }

        let backup_path = checked_backup_path_sync(
            &self.global_history_root,
            &self.session_id,
            &backup_file_name,
            true,
        )?;

        if !copy_file_best_effort_sync(file_path, &backup_path)? {
            return Ok(FileHistoryBackup {
                backup_file_name: None,
                version,
                backup_time: Utc::now(),
                failed: false,
            });
        }

        Ok(FileHistoryBackup {
            backup_file_name: Some(backup_file_name),
            version,
            backup_time: Utc::now(),
            failed: false,
        })
    }

    async fn create_backup(
        &self,
        file_path: &Path,
        hash_path: &str,
        version: f64,
    ) -> io::Result<FileHistoryBackup> {
        let backup_file_name = get_backup_file_name(hash_path, version);

        if let Err(error) = fs::metadata(file_path).await {
            if error.kind() == io::ErrorKind::NotFound {
                return Ok(FileHistoryBackup {
                    backup_file_name: None,
                    version,
                    backup_time: Utc::now(),
                    failed: false,
                });
            }
            return Err(error);
        }

        let backup_path = checked_backup_path_async(
            &self.global_history_root,
            &self.session_id,
            &backup_file_name,
            true,
        )
        .await?;

        if !copy_file_best_effort(file_path, &backup_path).await? {
            return Ok(FileHistoryBackup {
                backup_file_name: None,
                version,
                backup_time: Utc::now(),
                failed: false,
            });
        }

        Ok(FileHistoryBackup {
            backup_file_name: Some(backup_file_name),
            version,
            backup_time: Utc::now(),
            failed: false,
        })
    }

    async fn origin_file_changed(
        &self,
        file_path: &Path,
        source_metadata: &std::fs::Metadata,
        backup_file_name: &str,
    ) -> bool {
        let Ok(backup_path) = checked_backup_path_async(
            &self.global_history_root,
            &self.session_id,
            backup_file_name,
            false,
        )
        .await
        else {
            return true;
        };
        let Ok(backup_metadata) = fs::metadata(&backup_path).await else {
            return true;
        };

        if file_mode(source_metadata) != file_mode(&backup_metadata)
            || source_metadata.len() != backup_metadata.len()
        {
            return true;
        }
        if let (Ok(original_time), Ok(backup_time)) =
            (source_metadata.modified(), backup_metadata.modified())
        {
            if original_time < backup_time {
                return false;
            }
        }

        match files_equal(file_path, &backup_path).await {
            Ok(equal) => !equal,
            Err(_) => true,
        }
    }

    async fn restore_backup(&self, file_path: &Path, backup_file_name: &str) -> io::Result<bool> {
        let backup_path = checked_backup_path_async(
            &self.global_history_root,
            &self.session_id,
            backup_file_name,
            false,
        )
        .await?;
        let backup_metadata = match fs::metadata(&backup_path).await {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error),
        };

        if !copy_file_with_parent_fallback(&backup_path, file_path).await? {
            return Ok(false);
        }
        // `fs::copy` normally carries permission bits, but explicitly restore
        // them as TypeScript does with chmod and as a guard across platforms.
        fs::set_permissions(file_path, backup_metadata.permissions()).await?;
        Ok(true)
    }

    async fn cleanup_orphaned_backups(&self, removed: &[FileHistorySnapshot]) {
        let live_names: HashSet<&str> = self
            .snapshots
            .iter()
            .flat_map(|snapshot| snapshot.tracked_file_backups.values())
            .filter_map(|backup| backup.backup_file_name.as_deref())
            .collect();
        let orphaned: HashSet<&str> = removed
            .iter()
            .flat_map(|snapshot| snapshot.tracked_file_backups.values())
            .filter_map(|backup| backup.backup_file_name.as_deref())
            .filter(|name| !live_names.contains(name))
            .collect();

        for name in orphaned {
            if let Ok(path) =
                checked_backup_path_async(&self.global_history_root, &self.session_id, name, false)
                    .await
            {
                // Retention cleanup is best effort. A missing backup or an
                // unwritable history directory must not reject a new turn.
                let _ = fs::remove_file(path).await;
            }
        }
    }

    fn maybe_shorten_file_path(&self, file_path: &str) -> String {
        if !Path::new(file_path).is_absolute() {
            return file_path.to_owned();
        }
        let file_path = normalize_working_path(Path::new(file_path));
        let file_path = file_path.to_string_lossy();
        let cwd = self.cwd.to_string_lossy();
        if file_path == cwd {
            return String::new();
        }
        let cwd_prefix = format!("{}{}", cwd, std::path::MAIN_SEPARATOR);
        file_path
            .strip_prefix(&cwd_prefix)
            .map(str::to_owned)
            .unwrap_or_else(|| file_path.into_owned())
    }

    fn expand_file_path(&self, tracking_path: &str) -> PathBuf {
        let path = Path::new(tracking_path);
        if path.is_absolute() {
            normalize_working_path(path)
        } else {
            normalize_working_path(&self.cwd.join(path))
        }
    }

    fn resolve_working_path(&self, file_path: &str) -> PathBuf {
        self.expand_file_path(file_path)
    }
}

/// Normalize path aliases such as macOS `/var` → `/private/var`. If the file
/// is new, normalize its existing parent and append its name so `track_edit`
/// and the following turn snapshot derive the same backup key.
fn normalize_working_path(path: &Path) -> PathBuf {
    if let Ok(path) = std::fs::canonicalize(path) {
        return path;
    }

    // A new file can be nested under directories that do not exist yet. Walk
    // upward to the nearest existing ancestor so aliases such as macOS `/var`
    // and `/private/var` still resolve to the same key, then append the
    // missing suffix in its original order.
    let mut ancestor = path;
    let mut missing_suffix = Vec::new();
    loop {
        if let Ok(mut normalized_ancestor) = std::fs::canonicalize(ancestor) {
            for component in missing_suffix.iter().rev() {
                normalized_ancestor.push(component);
            }
            return normalized_ancestor;
        }
        let Some(file_name) = ancestor.file_name() else {
            return path.to_path_buf();
        };
        missing_suffix.push(file_name.to_os_string());
        let Some(parent) = ancestor.parent() else {
            return path.to_path_buf();
        };
        ancestor = parent;
    }
}

#[derive(Debug)]
enum EndpointRead {
    Ok { content: String, exists: bool },
    Unreadable,
    Oversized { exists: bool },
}

impl EndpointRead {
    fn ok_missing() -> Self {
        Self::Ok {
            content: String::new(),
            exists: false,
        }
    }

    fn exists(&self) -> bool {
        match self {
            Self::Ok { exists, .. } | Self::Oversized { exists } => *exists,
            Self::Unreadable => false,
        }
    }
}

/// Open, fstat, then read at most the configured byte cap plus one byte on
/// the same file descriptor. The extra byte catches files that grew after
/// fstat without ever retaining an unbounded allocation.
async fn read_path_with_size_guard(path: &Path, backup: bool) -> EndpointRead {
    let file = match fs::File::open(path).await {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound && !backup => {
            return EndpointRead::ok_missing();
        }
        Err(_) => return EndpointRead::Unreadable,
    };
    let Ok(metadata) = file.metadata().await else {
        return EndpointRead::Unreadable;
    };
    if metadata.len() > MAX_DIFF_SIZE_BYTES {
        return EndpointRead::Oversized { exists: true };
    }

    let capacity = usize::try_from(metadata.len())
        .unwrap_or(MAX_DIFF_SIZE_BYTES as usize)
        .min(MAX_DIFF_SIZE_BYTES as usize);
    let mut bytes = Vec::with_capacity(capacity);
    let mut capped_file = file.take(MAX_DIFF_SIZE_BYTES + 1);
    if capped_file.read_to_end(&mut bytes).await.is_err() {
        return EndpointRead::Unreadable;
    }
    if bytes.len() as u64 > MAX_DIFF_SIZE_BYTES {
        return EndpointRead::Oversized { exists: true };
    }
    EndpointRead::Ok {
        content: String::from_utf8_lossy(&bytes).into_owned(),
        exists: true,
    }
}

fn endpoint_content(endpoint: &EndpointRead) -> Option<&str> {
    match endpoint {
        EndpointRead::Ok {
            content,
            exists: true,
        } => Some(content),
        EndpointRead::Ok { exists: false, .. }
        | EndpointRead::Unreadable
        | EndpointRead::Oversized { .. } => None,
    }
}

async fn endpoint_line_count(endpoint: &EndpointRead, path: Option<&Path>) -> Option<usize> {
    match endpoint {
        EndpointRead::Ok {
            content,
            exists: true,
        } => Some(count_lines(content)),
        EndpointRead::Ok { exists: false, .. } => Some(0),
        EndpointRead::Oversized { exists: true } => {
            let path = path?;
            count_file_lines(path).await.ok()
        }
        EndpointRead::Oversized { exists: false } => Some(0),
        EndpointRead::Unreadable => None,
    }
}

/// Count UTF-8 text lines from disk with a fixed-size buffer. Newlines are
/// ASCII bytes and therefore have the same positions in valid and lossy
/// UTF-8 decoding.
async fn count_file_lines(path: &Path) -> io::Result<usize> {
    const CHUNK_SIZE: usize = 16 * 1024;

    let mut file = fs::File::open(path).await?;
    let mut buffer = [0; CHUNK_SIZE];
    let mut newline_count = 0_usize;
    let mut byte_count = 0_u64;
    let mut last_byte = None;
    loop {
        let read = file.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        newline_count += buffer[..read].iter().filter(|byte| **byte == b'\n').count();
        byte_count += read as u64;
        last_byte = Some(buffer[read - 1]);
    }
    if byte_count == 0 {
        return Ok(0);
    }
    if last_byte != Some(b'\n') {
        newline_count += 1;
    }
    Ok(newline_count)
}

fn count_lines(content: &str) -> usize {
    if content.is_empty() {
        return 0;
    }
    let newlines = content.bytes().filter(|byte| *byte == b'\n').count();
    if content.ends_with('\n') {
        newlines
    } else {
        newlines + 1
    }
}

fn looks_binary(content: &str) -> bool {
    let utf16_len = content.encode_utf16().count();
    if utf16_len == 0 {
        return false;
    }
    if content
        .encode_utf16()
        .take(BINARY_SNIFF_BYTES)
        .any(|unit| unit == 0)
    {
        return true;
    }
    if utf16_len > BINARY_SNIFF_BYTES {
        let tail_start = utf16_len
            .saturating_sub(BINARY_SNIFF_BYTES)
            .max(BINARY_SNIFF_BYTES);
        return content
            .encode_utf16()
            .skip(tail_start)
            .any(|unit| unit == 0);
    }
    false
}

fn without_line_ending(line: &str) -> &str {
    let line = line.strip_suffix('\n').unwrap_or(line);
    line.strip_suffix('\r').unwrap_or(line)
}

fn unified_range_start(start: usize, end: usize) -> usize {
    if start == end { start } else { start + 1 }
}

/// Compare file contents with fixed-size buffers so snapshot checks do not
/// allocate memory proportional to the largest tracked file.
async fn files_equal(left_path: &Path, right_path: &Path) -> io::Result<bool> {
    const CHUNK_SIZE: usize = 16 * 1024;

    let mut left = fs::File::open(left_path).await?;
    let mut right = fs::File::open(right_path).await?;
    let mut left_buffer = [0; CHUNK_SIZE];
    let mut right_buffer = [0; CHUNK_SIZE];

    loop {
        let left_read = left.read(&mut left_buffer).await?;
        let right_read = right.read(&mut right_buffer).await?;
        if left_read != right_read || left_buffer[..left_read] != right_buffer[..right_read] {
            return Ok(false);
        }
        if left_read == 0 {
            return Ok(true);
        }
    }
}

/// Create (when requested) and validate the history/session directory chain.
/// The configured root itself is treated as trusted input and may be a
/// symlink; descendants created by this service may not be symlinks. The
/// canonicalized paths must remain direct children of their expected parent.
///
/// These checks close static symlink escapes. They cannot prevent an
/// adversarial process from replacing a checked path between validation and
/// the subsequent filesystem operation; doing that requires descriptor-based
/// `openat`/`O_NOFOLLOW` operations on every platform.
fn checked_session_dir_sync(
    global_history_root: &Path,
    session_id: &str,
    create: bool,
) -> io::Result<PathBuf> {
    validate_component(session_id).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid file-history session ID",
        )
    })?;
    if create {
        std::fs::create_dir_all(global_history_root)?;
    }
    let canonical_root = std::fs::canonicalize(global_history_root)?;
    let history_dir = canonical_root.join(FILE_HISTORY_DIR);
    validate_directory_sync(&history_dir, create)?;
    let canonical_history_dir = std::fs::canonicalize(&history_dir)?;
    if canonical_history_dir.parent() != Some(canonical_root.as_path()) {
        return Err(unsafe_history_path(
            "file-history directory escapes history root",
        ));
    }

    let session_dir = canonical_history_dir.join(session_id);
    validate_directory_sync(&session_dir, create)?;
    let canonical_session_dir = std::fs::canonicalize(&session_dir)?;
    if canonical_session_dir.parent() != Some(canonical_history_dir.as_path()) {
        return Err(unsafe_history_path(
            "file-history session directory escapes history root",
        ));
    }
    Ok(canonical_session_dir)
}

async fn checked_session_dir_async(
    global_history_root: &Path,
    session_id: &str,
    create: bool,
) -> io::Result<PathBuf> {
    validate_component(session_id).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid file-history session ID",
        )
    })?;
    if create {
        fs::create_dir_all(global_history_root).await?;
    }
    let canonical_root = fs::canonicalize(global_history_root).await?;
    let history_dir = canonical_root.join(FILE_HISTORY_DIR);
    validate_directory_async(&history_dir, create).await?;
    let canonical_history_dir = fs::canonicalize(&history_dir).await?;
    if canonical_history_dir.parent() != Some(canonical_root.as_path()) {
        return Err(unsafe_history_path(
            "file-history directory escapes history root",
        ));
    }

    let session_dir = canonical_history_dir.join(session_id);
    validate_directory_async(&session_dir, create).await?;
    let canonical_session_dir = fs::canonicalize(&session_dir).await?;
    if canonical_session_dir.parent() != Some(canonical_history_dir.as_path()) {
        return Err(unsafe_history_path(
            "file-history session directory escapes history root",
        ));
    }
    Ok(canonical_session_dir)
}

fn validate_directory_sync(path: &Path, create: bool) -> io::Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => check_directory_metadata(path, &metadata),
        Err(error) if error.kind() == io::ErrorKind::NotFound && create => {
            match std::fs::create_dir(path) {
                Ok(()) => {}
                // Recheck the winner if another process created the directory
                // concurrently; this also rejects a symlink race at creation.
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error),
            }
            let metadata = std::fs::symlink_metadata(path)?;
            check_directory_metadata(path, &metadata)
        }
        Err(error) => Err(error),
    }
}

async fn validate_directory_async(path: &Path, create: bool) -> io::Result<()> {
    match fs::symlink_metadata(path).await {
        Ok(metadata) => check_directory_metadata(path, &metadata),
        Err(error) if error.kind() == io::ErrorKind::NotFound && create => {
            match fs::create_dir(path).await {
                Ok(()) => {}
                // Recheck the winner if another process created the directory
                // concurrently; this also rejects a symlink race at creation.
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error),
            }
            let metadata = fs::symlink_metadata(path).await?;
            check_directory_metadata(path, &metadata)
        }
        Err(error) => Err(error),
    }
}

fn check_directory_metadata(path: &Path, metadata: &std::fs::Metadata) -> io::Result<()> {
    if metadata.file_type().is_symlink() {
        return Err(unsafe_history_path(&format!(
            "file-history directory is a symlink: {}",
            path.display()
        )));
    }
    if !metadata.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("file-history path is not a directory: {}", path.display()),
        ));
    }
    Ok(())
}

fn unsafe_history_path(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, message.to_owned())
}

fn checked_backup_path_sync(
    global_history_root: &Path,
    session_id: &str,
    backup_file_name: &str,
    create_session_dir: bool,
) -> io::Result<PathBuf> {
    validate_backup_file_name(backup_file_name)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    let session_dir =
        checked_session_dir_sync(global_history_root, session_id, create_session_dir)?;
    let backup_path = session_dir.join(backup_file_name);
    if backup_path.parent() != Some(session_dir.as_path()) {
        return Err(unsafe_history_path(
            "backup path escapes file-history session directory",
        ));
    }
    validate_backup_destination_sync(&backup_path)?;
    Ok(backup_path)
}

async fn checked_backup_path_async(
    global_history_root: &Path,
    session_id: &str,
    backup_file_name: &str,
    create_session_dir: bool,
) -> io::Result<PathBuf> {
    validate_backup_file_name(backup_file_name)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    let session_dir =
        checked_session_dir_async(global_history_root, session_id, create_session_dir).await?;
    let backup_path = session_dir.join(backup_file_name);
    if backup_path.parent() != Some(session_dir.as_path()) {
        return Err(unsafe_history_path(
            "backup path escapes file-history session directory",
        ));
    }
    validate_backup_destination_async(&backup_path).await?;
    Ok(backup_path)
}

fn validate_backup_destination_sync(path: &Path) -> io::Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(unsafe_history_path(&format!(
            "file-history backup destination is a symlink: {}",
            path.display()
        ))),
        Ok(metadata) if !metadata.is_file() => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "file-history backup destination is not a regular file: {}",
                path.display()
            ),
        )),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

async fn validate_backup_destination_async(path: &Path) -> io::Result<()> {
    match fs::symlink_metadata(path).await {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(unsafe_history_path(&format!(
            "file-history backup destination is a symlink: {}",
            path.display()
        ))),
        Ok(metadata) if !metadata.is_file() => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "file-history backup destination is not a regular file: {}",
                path.display()
            ),
        )),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn get_backup_file_name(file_path: &str, version: f64) -> String {
    let digest = Sha256::digest(file_path.as_bytes());
    let full_hash = format!("{digest:x}");
    format!("{}@v{version}", &full_hash[..16])
}

fn version_from_backup_name(backup_file_name: &str) -> Option<f64> {
    let (_, version) = backup_file_name.split_once("@v")?;
    version
        .parse::<f64>()
        .ok()
        .filter(|version| version.is_finite() && *version >= 0.0)
}

fn failed_backup(previous: Option<&FileHistoryBackup>, version: f64) -> FileHistoryBackup {
    FileHistoryBackup {
        backup_file_name: previous.and_then(|backup| backup.backup_file_name.clone()),
        version,
        backup_time: Utc::now(),
        failed: true,
    }
}

async fn copy_file_best_effort(source: &Path, destination: &Path) -> io::Result<bool> {
    match fs::copy(source, destination).await {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => match fs::metadata(source).await {
            Err(source_error) if source_error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(source_error) => Err(source_error),
            // The checked caller has already created and validated the
            // destination directory. If it disappears, don't recreate it
            // through a path that could have been replaced with a symlink.
            Ok(_) => Err(error),
        },
        Err(error) => Err(error),
    }
}

/// Copy a backup over a worktree file, creating its parent directory only
/// after the source has been confirmed to still exist. This matches the
/// TypeScript restore helper's missing-source distinction and nested-path
/// behavior.
async fn copy_file_with_parent_fallback(source: &Path, destination: &Path) -> io::Result<bool> {
    match fs::copy(source, destination).await {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => match fs::metadata(source).await {
            Err(source_error) if source_error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(source_error) => Err(source_error),
            Ok(_) => {
                if let Some(parent) = destination.parent() {
                    fs::create_dir_all(parent).await?;
                }
                match fs::copy(source, destination).await {
                    Ok(_) => Ok(true),
                    Err(copy_error) if copy_error.kind() == io::ErrorKind::NotFound => {
                        match fs::metadata(source).await {
                            Err(source_error) if source_error.kind() == io::ErrorKind::NotFound => {
                                Ok(false)
                            }
                            Err(source_error) => Err(source_error),
                            Ok(_) => Err(copy_error),
                        }
                    }
                    Err(copy_error) => Err(copy_error),
                }
            }
        },
        Err(error) => Err(error),
    }
}

fn copy_file_best_effort_sync(source: &Path, destination: &Path) -> io::Result<bool> {
    match std::fs::copy(source, destination) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => match std::fs::metadata(source) {
            Err(source_error) if source_error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(source_error) => Err(source_error),
            // The checked caller has already created and validated the
            // destination directory. If it disappears, don't recreate it
            // through a path that could have been replaced with a symlink.
            Ok(_) => Err(error),
        },
        Err(error) => Err(error),
    }
}

fn validate_component(value: &str) -> Result<(), ()> {
    if value.is_empty()
        || value == "."
        || value == ".."
        || value.contains('/')
        || value.contains('\\')
        || Path::new(value).components().count() != 1
        || Path::new(value)
            .components()
            .any(|part| !matches!(part, Component::Normal(_)))
    {
        return Err(());
    }
    Ok(())
}

fn validate_backup_file_name(value: &str) -> Result<(), String> {
    let Some((hash, version)) = value.split_once("@v") else {
        return Err(format!("invalid backupFileName: {value}"));
    };
    let parsed_version = version.parse::<f64>();
    if hash.len() != 16
        || !hash.bytes().all(|byte| byte.is_ascii_hexdigit())
        || version.is_empty()
        || !version
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'.' | b'e' | b'E' | b'+' | b'-'))
        || !parsed_version.is_ok_and(|number| number.is_finite() && number >= 0.0)
    {
        return Err(format!("invalid backupFileName: {value}"));
    }
    validate_component(value).map_err(|_| format!("backupFileName escapes base directory: {value}"))
}

fn file_mode(metadata: &std::fs::Metadata) -> u32 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        metadata.mode()
    }
    #[cfg(not(unix))]
    {
        u32::from(metadata.permissions().readonly())
    }
}

fn is_false(value: &bool) -> bool {
    !value
}

mod iso_millis {
    use super::*;

    pub fn serialize<S>(date: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&date.to_rfc3339_opts(SecondsFormat::Millis, true))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Ok(DateTime::parse_from_rfc3339(&value)
            .map(|date| date.with_timezone(&Utc))
            .unwrap_or_else(|_| {
                DateTime::from_timestamp(0, 0).expect("Unix epoch is representable")
            }))
    }
}

mod json_number {
    use super::*;

    pub fn serialize<S>(value: &f64, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // TypeScript writes integral versions as JSON integers. Keep that
        // representation while preserving non-integral source values too.
        if value.fract() == 0.0 && *value >= i64::MIN as f64 && *value < -(i64::MIN as f64) {
            serializer.serialize_i64(*value as i64)
        } else if value.fract() == 0.0 && *value >= 0.0 && *value < u64::MAX as f64 {
            serializer.serialize_u64(*value as u64)
        } else {
            serializer.serialize_f64(*value)
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<f64, D::Error>
    where
        D: Deserializer<'de>,
    {
        f64::deserialize(deserializer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(label: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after Unix epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "canopy-file-history-{label}-{}-{nonce}",
            std::process::id()
        ));
        std::fs::create_dir_all(&path).expect("create test directory");
        path
    }

    fn service(root: &Path) -> FileHistoryService {
        FileHistoryService::new(root.join("global"), "session-a", root.join("project"), true)
    }

    fn snapshot_with_backup(
        prompt_id: impl Into<String>,
        tracking_path: impl Into<String>,
        backup_file_name: Option<String>,
        failed: bool,
    ) -> FileHistorySnapshot {
        let now = Utc::now();
        let mut tracked_file_backups = IndexMap::new();
        tracked_file_backups.insert(
            tracking_path.into(),
            FileHistoryBackup {
                backup_file_name,
                version: 1.0,
                backup_time: now,
                failed,
            },
        );
        FileHistorySnapshot {
            prompt_id: prompt_id.into(),
            tracked_file_backups,
            timestamp: now,
        }
    }

    #[test]
    fn snapshot_serialization_matches_the_jsonl_schema() {
        let timestamp = DateTime::parse_from_rfc3339("2026-08-03T12:34:56.789Z")
            .expect("valid date")
            .with_timezone(&Utc);
        let mut backups = IndexMap::new();
        backups.insert(
            "src/main.rs".to_owned(),
            FileHistoryBackup {
                backup_file_name: Some("0123456789abcdef@v2".to_owned()),
                version: 2.0,
                backup_time: timestamp,
                failed: false,
            },
        );
        backups.insert(
            "gone.txt".to_owned(),
            FileHistoryBackup {
                backup_file_name: None,
                version: 3.25,
                backup_time: timestamp,
                failed: true,
            },
        );
        let snapshot = FileHistorySnapshot {
            prompt_id: "prompt-7".to_owned(),
            tracked_file_backups: backups,
            timestamp,
        };

        assert_eq!(
            serde_json::to_value(snapshot).expect("serialize snapshot"),
            json!({
                "promptId": "prompt-7",
                "timestamp": "2026-08-03T12:34:56.789Z",
                "trackedFileBackups": {
                    "src/main.rs": {
                        "backupFileName": "0123456789abcdef@v2",
                        "version": 2,
                        "backupTime": "2026-08-03T12:34:56.789Z"
                    },
                    "gone.txt": {
                        "backupFileName": null,
                        "version": 3.25,
                        "backupTime": "2026-08-03T12:34:56.789Z",
                        "failed": true
                    }
                }
            })
        );
    }

    #[test]
    fn backup_paths_reject_traversal_and_accept_generated_names() {
        let root = Path::new("/tmp/canopy");
        let path = resolve_backup_path(root, "session-a", "0123456789abcdef@v12")
            .expect("generated backup name is accepted");
        assert_eq!(
            path,
            root.join("file-history/session-a/0123456789abcdef@v12")
        );
        for name in [
            "../outside@v1",
            "subdir/0123456789abcdef@v1",
            "0123456789abcdef@v0/../../outside",
            "/tmp/0123456789abcdef@v1",
            "not-a-hash@v1",
        ] {
            assert!(
                resolve_backup_path(root, "session-a", name).is_err(),
                "{name}"
            );
        }
        assert!(resolve_backup_path(root, "session-a", "0123456789abcdef@v2.5").is_ok());
        assert!(resolve_backup_path(root, "../elsewhere", "0123456789abcdef@v1").is_err());
    }

    #[tokio::test]
    async fn track_edit_stores_a_pre_edit_backup_once() {
        let root = temp_dir("track");
        let cwd = root.join("project");
        fs::create_dir_all(&cwd).await.expect("create cwd");
        let file = cwd.join("a.txt");
        fs::write(&file, b"before edit")
            .await
            .expect("create source");
        let mut service = service(&root);
        service.make_snapshot("p1").await;

        service.track_edit(file.to_str().expect("UTF-8 test path"));
        let snapshot = &service.get_snapshots()[0];
        let backup = &snapshot.tracked_file_backups["a.txt"];
        let backup_name = backup
            .backup_file_name
            .clone()
            .expect("source file was backed up");
        let backup_path = resolve_backup_path(&root.join("global"), "session-a", &backup_name)
            .expect("stored name is safe");
        assert_eq!(
            fs::read(backup_path).await.expect("read backup"),
            b"before edit"
        );
        assert_eq!(backup.version, 1.0);

        service.track_edit(file.to_str().expect("UTF-8 test path"));
        assert_eq!(
            service.get_snapshots()[0].tracked_file_backups["a.txt"].version,
            1.0
        );
        let updates = service.take_pending_snapshot_updates();
        assert_eq!(updates.len(), 2);
        assert!(updates[0].tracked_file_backups.is_empty());
        assert_eq!(
            updates[1].tracked_file_backups["a.txt"].backup_file_name,
            Some(backup_name)
        );
        assert!(service.take_pending_snapshot_updates().is_empty());
        std::fs::remove_dir_all(root).expect("clean test directory");
    }

    #[tokio::test]
    async fn snapshots_inherit_unchanged_backup_then_capture_new_version() {
        let root = temp_dir("inherit");
        let cwd = root.join("project");
        fs::create_dir_all(&cwd).await.expect("create cwd");
        let file = cwd.join("a.txt");
        fs::write(&file, b"original").await.expect("create source");
        let mut service = service(&root);
        service.make_snapshot("p0").await;
        service.track_edit(file.to_str().expect("UTF-8 test path"));
        let first = service.get_snapshots()[0].tracked_file_backups["a.txt"].clone();

        service.make_snapshot("p1").await;
        assert_eq!(
            service.get_snapshots()[1].tracked_file_backups["a.txt"],
            first,
            "an unchanged file inherits the existing backup"
        );

        fs::write(&file, b"changed content")
            .await
            .expect("modify source");
        service.make_snapshot("p2").await;
        let changed = &service.get_snapshots()[2].tracked_file_backups["a.txt"];
        assert_eq!(changed.version, 2.0);
        assert_ne!(changed.backup_file_name, first.backup_file_name);
        std::fs::remove_dir_all(root).expect("clean test directory");
    }

    #[tokio::test]
    async fn rewind_restores_snapshot_and_truncates_later_history() {
        let root = temp_dir("rewind-truncate");
        let cwd = root.join("project");
        fs::create_dir_all(&cwd).await.expect("create cwd");
        let file = cwd.join("src/nested/a.txt");
        fs::create_dir_all(file.parent().expect("file parent"))
            .await
            .expect("create nested parent");
        fs::write(&file, b"before edit\n")
            .await
            .expect("create source file");
        let mut service = service(&root);

        service.make_snapshot("p1").await;
        service.track_edit(file.to_str().expect("UTF-8 test path"));
        let first_backup = service.snapshots[0].tracked_file_backups["src/nested/a.txt"]
            .backup_file_name
            .clone()
            .expect("capture initial file");
        fs::write(&file, b"after edit\n")
            .await
            .expect("edit source file");
        service.make_snapshot("p2").await;
        let later_backup = service.snapshots[1].tracked_file_backups["src/nested/a.txt"]
            .backup_file_name
            .clone()
            .expect("capture edited file");
        fs::remove_dir_all(file.parent().expect("file parent"))
            .await
            .expect("remove worktree file and its parents");

        let result = service
            .rewind("p1", true)
            .await
            .expect("target snapshot exists");

        assert_eq!(
            result.files_changed,
            vec![
                service
                    .expand_file_path("src/nested/a.txt")
                    .to_string_lossy()
                    .into_owned()
            ]
        );
        assert!(result.files_failed.is_empty());
        assert_eq!(
            fs::read(&file).await.expect("rewound file exists"),
            b"before edit\n"
        );
        assert_eq!(
            service
                .get_snapshots()
                .iter()
                .map(|snapshot| snapshot.prompt_id.as_str())
                .collect::<Vec<_>>(),
            ["p1"]
        );
        let session_dir = root.join("global/file-history/session-a");
        assert!(session_dir.join(first_backup).exists());
        assert!(!session_dir.join(later_backup).exists());

        std::fs::remove_dir_all(root).expect("clean test directory");
    }

    #[tokio::test]
    async fn rewind_removes_a_file_absent_from_the_target_snapshot() {
        let root = temp_dir("rewind-new-file");
        let cwd = root.join("project");
        fs::create_dir_all(&cwd).await.expect("create cwd");
        let file = cwd.join("new/deep/file.txt");
        let mut service = service(&root);

        service.make_snapshot("before-file").await;
        service.track_edit(file.to_str().expect("UTF-8 test path"));
        fs::create_dir_all(file.parent().expect("file parent"))
            .await
            .expect("create nested parent");
        fs::write(&file, b"created during turn")
            .await
            .expect("create file");
        service.make_snapshot("after-file").await;

        let result = service
            .rewind("before-file", false)
            .await
            .expect("target snapshot exists");

        assert_eq!(
            result.files_changed,
            vec![
                service
                    .expand_file_path("new/deep/file.txt")
                    .to_string_lossy()
                    .into_owned()
            ]
        );
        assert!(result.files_failed.is_empty());
        assert!(!file.exists());
        assert_eq!(service.get_snapshots().len(), 2, "history was retained");

        std::fs::remove_dir_all(root).expect("clean test directory");
    }

    #[tokio::test]
    async fn rewind_failure_reports_file_and_keeps_later_history() {
        let root = temp_dir("rewind-failed-backup");
        let cwd = root.join("project");
        fs::create_dir_all(&cwd).await.expect("create cwd");
        let file = cwd.join("a.txt");
        fs::write(&file, b"before")
            .await
            .expect("create source file");
        let mut service = service(&root);

        service.make_snapshot("p1").await;
        service.track_edit(file.to_str().expect("UTF-8 test path"));
        let backup_name = service.snapshots[0].tracked_file_backups["a.txt"]
            .backup_file_name
            .clone()
            .expect("capture initial file");
        fs::write(&file, b"after").await.expect("edit source file");
        service.make_snapshot("p2").await;
        let backup_path = resolve_backup_path(&root.join("global"), "session-a", &backup_name)
            .expect("resolve backup path");
        fs::remove_file(backup_path)
            .await
            .expect("remove selected backup");

        let result = service
            .rewind("p1", true)
            .await
            .expect("snapshot selection itself succeeds");

        assert!(result.files_changed.is_empty());
        assert_eq!(
            result.files_failed,
            vec![
                service
                    .expand_file_path("a.txt")
                    .to_string_lossy()
                    .into_owned()
            ]
        );
        assert_eq!(fs::read(&file).await.expect("live file remains"), b"after");
        assert_eq!(
            service.get_snapshots().len(),
            2,
            "failed rewind keeps history"
        );

        std::fs::remove_dir_all(root).expect("clean test directory");
    }

    #[tokio::test]
    async fn rewind_reports_missing_snapshots_but_disabled_service_is_a_noop() {
        let root = temp_dir("rewind-missing");
        let mut service = service(&root);
        let error = service
            .rewind("missing", true)
            .await
            .expect_err("unknown prompt is rejected");
        assert_eq!(error, "The selected snapshot was not found");

        let mut disabled = FileHistoryService::new(
            root.join("global"),
            "session-a",
            root.join("project"),
            false,
        );
        assert_eq!(
            disabled
                .rewind("missing", true)
                .await
                .expect("disabled service is a no-op"),
            RewindResult::default()
        );
        std::fs::remove_dir_all(root).expect("clean test directory");
    }

    #[tokio::test]
    async fn missing_source_is_recorded_without_failing_the_snapshot() {
        let root = temp_dir("missing");
        let cwd = root.join("project");
        fs::create_dir_all(&cwd).await.expect("create cwd");
        let file = cwd.join("will-appear.txt");
        let mut service = service(&root);
        service.make_snapshot("p0").await;
        service.track_edit(file.to_str().expect("UTF-8 test path"));
        assert_eq!(
            service.get_snapshots()[0].tracked_file_backups["will-appear.txt"].backup_file_name,
            None
        );

        fs::write(&file, b"new file")
            .await
            .expect("create source later");
        service.make_snapshot("p1").await;
        let backup = &service.get_snapshots()[1].tracked_file_backups["will-appear.txt"];
        assert_eq!(backup.version, 2.0);
        assert!(backup.backup_file_name.is_some());
        assert!(!backup.failed);
        std::fs::remove_dir_all(root).expect("clean test directory");
    }

    #[tokio::test]
    async fn snapshot_history_retains_only_the_latest_one_hundred() {
        let root = temp_dir("retention");
        let mut service = service(&root);
        for index in 0..105 {
            service.make_snapshot(format!("p{index}")).await;
        }
        let snapshots = service.get_snapshots();
        assert_eq!(snapshots.len(), MAX_SNAPSHOTS);
        assert_eq!(snapshots[0].prompt_id, "p5");
        assert_eq!(snapshots[99].prompt_id, "p104");
        let updates = service.take_pending_snapshot_updates();
        assert_eq!(updates.len(), MAX_SNAPSHOTS);
        assert_eq!(updates[0].prompt_id, "p5");
        assert_eq!(updates[MAX_SNAPSHOTS - 1].prompt_id, "p104");
        std::fs::remove_dir_all(root).expect("clean test directory");
    }

    #[tokio::test]
    async fn restore_normalizes_paths_and_rebuilds_tracked_files() {
        let root = temp_dir("restore");
        let cwd = root.join("project");
        std::fs::create_dir_all(&cwd).expect("create cwd");
        let absolute_path = cwd.join("src/main.rs");
        let mut service = service(&root);
        service.make_snapshot("discarded").await;

        service
            .restore_from_snapshots(vec![snapshot_with_backup(
                "restored",
                absolute_path.to_str().expect("UTF-8 test path"),
                None,
                false,
            )])
            .await;

        assert_eq!(service.snapshots.len(), 1);
        assert!(
            service.snapshots[0]
                .tracked_file_backups
                .contains_key("src/main.rs")
        );
        assert_eq!(
            service.tracked_files.iter().cloned().collect::<Vec<_>>(),
            vec!["src/main.rs".to_owned()]
        );
        assert!(service.take_pending_snapshot_updates().is_empty());
        std::fs::remove_dir_all(root).expect("clean test directory");
    }

    #[tokio::test]
    async fn restore_from_session_snapshots_converts_optional_fields_and_preserves_data() {
        use crate::services::session_file_history_state::{
            FileHistoryBackup as SessionBackup, FileHistorySnapshot as SessionSnapshot,
        };

        let root = temp_dir("restore-session-snapshots");
        let timestamp = DateTime::parse_from_rfc3339("2026-08-03T12:34:56.789Z")
            .expect("valid date")
            .with_timezone(&Utc);
        let session_snapshot = SessionSnapshot {
            prompt_id: "restored-session-prompt".to_owned(),
            tracked_file_backups: std::collections::HashMap::from([
                (
                    "src/a.rs".to_owned(),
                    SessionBackup {
                        backup_file_name: Some("0123456789abcdef@v1".to_owned()),
                        version: Some(2.25),
                        backup_time: timestamp,
                        failed: Some(true),
                    },
                ),
                (
                    "src/z.rs".to_owned(),
                    SessionBackup {
                        backup_file_name: Some("fedcba9876543210@v4.5".to_owned()),
                        version: None,
                        backup_time: timestamp,
                        failed: None,
                    },
                ),
            ]),
            timestamp,
        };
        let mut service = service(&root);

        service
            .restore_from_session_snapshots(vec![session_snapshot])
            .await;

        let restored = &service.snapshots[0];
        assert_eq!(restored.prompt_id, "restored-session-prompt");
        assert_eq!(restored.timestamp, timestamp);
        assert_eq!(
            restored
                .tracked_file_backups
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            ["src/a.rs", "src/z.rs"]
        );
        let explicit_version = &restored.tracked_file_backups["src/a.rs"];
        assert_eq!(explicit_version.version, 2.25);
        assert!(explicit_version.failed);
        assert_eq!(explicit_version.backup_time, timestamp);
        let migrated_version = &restored.tracked_file_backups["src/z.rs"];
        assert_eq!(migrated_version.version, 4.5);
        assert!(!migrated_version.failed);
        assert_eq!(migrated_version.backup_time, timestamp);
        std::fs::remove_dir_all(root).expect("clean test directory");
    }

    #[tokio::test]
    async fn restored_snapshots_deduplicate_validation_and_keep_latest_one_hundred() {
        let root = temp_dir("restore-retention");
        let global = root.join("global");
        checked_session_dir_sync(&global, "session-a", true)
            .expect("create checked session directory");
        let backup_name = "0123456789abcdef@v1".to_owned();
        let snapshots = (0..105)
            .map(|index| {
                snapshot_with_backup(
                    format!("p{index}"),
                    format!("file-{index}.txt"),
                    Some(backup_name.clone()),
                    false,
                )
            })
            .collect();
        let mut service = service(&root);

        service.restore_from_snapshots(snapshots).await;

        assert_eq!(service.snapshots.len(), MAX_SNAPSHOTS);
        assert_eq!(service.snapshots[0].prompt_id, "p5");
        assert_eq!(service.snapshots[99].prompt_id, "p104");
        assert_eq!(service.tracked_files.len(), MAX_SNAPSHOTS);
        assert_eq!(service.unique_restored_backup_names().len(), 1);

        service.validate_restored_snapshots().await;
        assert!(service.snapshots.iter().all(|snapshot| {
            snapshot
                .tracked_file_backups
                .values()
                .next()
                .unwrap()
                .failed
        }));
        std::fs::remove_dir_all(root).expect("clean test directory");
    }

    #[tokio::test]
    async fn validation_marks_a_missing_restored_backup_failed() {
        let root = temp_dir("restore-missing-backup");
        let global = root.join("global");
        let session_dir = checked_session_dir_sync(&global, "session-a", true)
            .expect("create checked session directory");
        let existing_name = "abcdef0123456789@v4";
        std::fs::write(session_dir.join(existing_name), b"valid backup")
            .expect("create readable backup");
        let mut service = service(&root);
        service
            .restore_from_snapshots(vec![
                snapshot_with_backup("p1", "a.txt", Some("0123456789abcdef@v2".to_owned()), false),
                snapshot_with_backup("p2", "b.txt", Some(existing_name.to_owned()), false),
            ])
            .await;

        service.validate_restored_snapshots().await;

        let backup = &service.snapshots[0].tracked_file_backups["a.txt"];
        assert!(backup.failed);
        assert_eq!(
            backup.backup_file_name.as_deref(),
            Some("0123456789abcdef@v2")
        );
        assert!(!service.snapshots[1].tracked_file_backups["b.txt"].failed);
        let updates = service.take_pending_snapshot_updates();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].prompt_id, "p1");
        assert!(updates[0].tracked_file_backups["a.txt"].failed);
        std::fs::remove_dir_all(root).expect("clean test directory");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn validation_rejects_traversal_and_symlinked_restored_backups() {
        use std::os::unix::fs::symlink;

        let root = temp_dir("restore-unsafe-backup");
        let global = root.join("global");
        let session_dir = checked_session_dir_sync(&global, "session-a", true)
            .expect("create checked session directory");
        let outside_file = root.join("outside.txt");
        std::fs::write(&outside_file, b"outside remains unchanged").expect("create outside file");
        let symlink_name = "fedcba9876543210@v3";
        let symlink_path = session_dir.join(symlink_name);
        symlink(&outside_file, &symlink_path).expect("create backup symlink");

        let mut service = service(&root);
        service
            .restore_from_snapshots(vec![
                snapshot_with_backup(
                    "p1",
                    "traversal.txt",
                    Some("../outside@v1".to_owned()),
                    false,
                ),
                snapshot_with_backup("p2", "symlink.txt", Some(symlink_name.to_owned()), false),
            ])
            .await;

        service.validate_restored_snapshots().await;

        assert!(service.snapshots[0].tracked_file_backups["traversal.txt"].failed);
        assert!(service.snapshots[1].tracked_file_backups["symlink.txt"].failed);
        assert!(
            std::fs::symlink_metadata(&symlink_path)
                .expect("symlink remains")
                .file_type()
                .is_symlink()
        );
        assert_eq!(
            std::fs::read(&outside_file).expect("read outside file"),
            b"outside remains unchanged"
        );
        std::fs::remove_dir_all(root).expect("clean test directory");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn track_edit_rejects_a_preexisting_session_directory_symlink() {
        use std::os::unix::fs::symlink;

        let root = temp_dir("session-symlink");
        let global = root.join("global");
        let history_dir = global.join(FILE_HISTORY_DIR);
        let outside_dir = root.join("outside");
        std::fs::create_dir_all(&history_dir).expect("create history directory");
        std::fs::create_dir_all(&outside_dir).expect("create outside directory");
        symlink(&outside_dir, history_dir.join("session-a")).expect("create session symlink");

        let cwd = root.join("project");
        std::fs::create_dir_all(&cwd).expect("create cwd");
        let file = cwd.join("a.txt");
        std::fs::write(&file, b"before edit").expect("create source file");
        let mut service = FileHistoryService::new(global.clone(), "session-a", cwd.clone(), true);
        service.make_snapshot("p0").await;

        service.track_edit(file.to_str().expect("UTF-8 test path"));

        assert!(
            outside_dir
                .read_dir()
                .expect("read outside directory")
                .next()
                .is_none()
        );
        assert!(service.get_snapshots()[0].tracked_file_backups.is_empty());
        std::fs::remove_dir_all(root).expect("clean test directory");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn backup_symlinks_are_rejected_for_write_read_and_retention_cleanup() {
        use std::os::unix::fs::symlink;

        let root = temp_dir("backup-symlink");
        let global = root.join("global");
        let cwd = root.join("project");
        fs::create_dir_all(&cwd).await.expect("create cwd");
        let file = cwd.join("a.txt");
        fs::write(&file, b"working content")
            .await
            .expect("create source file");
        let outside_file = root.join("outside.txt");
        fs::write(&outside_file, b"keep this content")
            .await
            .expect("create outside file");

        let session_dir = checked_session_dir_sync(&global, "session-a", true)
            .expect("create checked session directory");
        let normalized_file = normalize_working_path(&file);
        let backup_name = get_backup_file_name(&normalized_file.to_string_lossy(), 1.0);
        let backup_path = session_dir.join(&backup_name);
        symlink(&outside_file, &backup_path).expect("create backup symlink");

        let mut service = FileHistoryService::new(global.clone(), "session-a", cwd.clone(), true);
        service.make_snapshot("p0").await;
        service.track_edit(file.to_str().expect("UTF-8 test path"));
        assert!(service.get_snapshots()[0].tracked_file_backups.is_empty());

        let source_metadata = std::fs::metadata(&file).expect("stat source file");
        assert!(
            service
                .origin_file_changed(&file, &source_metadata, &backup_name)
                .await
        );
        let mut removed_backups = IndexMap::new();
        removed_backups.insert(
            "a.txt".to_owned(),
            FileHistoryBackup {
                backup_file_name: Some(backup_name),
                version: 1.0,
                backup_time: Utc::now(),
                failed: false,
            },
        );
        let removed_snapshot = FileHistorySnapshot {
            prompt_id: "expired".to_owned(),
            tracked_file_backups: removed_backups,
            timestamp: Utc::now(),
        };
        service.cleanup_orphaned_backups(&[removed_snapshot]).await;

        assert!(
            std::fs::symlink_metadata(&backup_path)
                .expect("backup symlink remains")
                .file_type()
                .is_symlink()
        );
        assert_eq!(
            fs::read(&outside_file).await.expect("read outside file"),
            b"keep this content"
        );
        std::fs::remove_dir_all(root).expect("clean test directory");
    }

    #[tokio::test]
    async fn get_diff_stats_compares_snapshot_backup_to_current_worktree() {
        let root = temp_dir("diff-stats");
        let cwd = root.join("project");
        fs::create_dir_all(&cwd).await.expect("create cwd");
        let file = cwd.join("stats.txt");
        fs::write(&file, b"keep\nold\n")
            .await
            .expect("create source file");
        let mut service = service(&root);

        service.make_snapshot("p1").await;
        service.track_edit(file.to_str().expect("UTF-8 test path"));
        fs::write(&file, b"keep\nnew\nextra\n")
            .await
            .expect("edit source file");
        service.make_snapshot("p2").await;

        let stats = service.get_diff_stats("p1").await.expect("snapshot exists");
        assert_eq!(
            stats.files_changed,
            vec![
                service
                    .expand_file_path("stats.txt")
                    .to_string_lossy()
                    .into_owned()
            ]
        );
        assert_eq!(stats.insertions, 2);
        assert_eq!(stats.deletions, 1);
        assert!(service.get_diff_stats("missing").await.is_none());

        std::fs::remove_dir_all(root).expect("clean test directory");
    }

    #[tokio::test]
    async fn get_turn_diff_matches_next_snapshot_and_returns_context_hunks() {
        let root = temp_dir("turn-diff");
        let cwd = root.join("project");
        fs::create_dir_all(&cwd).await.expect("create cwd");
        let file = cwd.join("turn.txt");
        fs::write(&file, b"first\nsecond\n")
            .await
            .expect("create source file");
        let mut service = service(&root);

        service.make_snapshot("p1").await;
        service.track_edit(file.to_str().expect("UTF-8 test path"));
        fs::write(&file, b"first\nchanged\n")
            .await
            .expect("edit source file");
        service.make_snapshot("p2").await;

        let diff = service.get_turn_diff("p1").await.expect("snapshot exists");
        assert_eq!(diff.stats.files_changed, 1);
        assert_eq!(diff.stats.files_omitted, 0);
        assert_eq!(diff.files[0].file_path, "turn.txt");
        assert_eq!(diff.files[0].lines_added, 1);
        assert_eq!(diff.files[0].lines_removed, 1);
        assert!(!diff.files[0].is_new_file);
        assert!(!diff.files[0].is_deleted);
        assert!(!diff.files[0].is_binary);
        assert!(!diff.files[0].oversized);
        assert_eq!(diff.files[0].hunks.len(), 1);
        assert!(
            diff.files[0].hunks[0]
                .lines
                .iter()
                .any(|line| line == "-second")
        );
        assert!(
            diff.files[0].hunks[0]
                .lines
                .iter()
                .any(|line| line == "+changed")
        );

        std::fs::remove_dir_all(root).expect("clean test directory");
    }

    #[tokio::test]
    async fn get_turn_diff_handles_new_deleted_and_binary_files() {
        let root = temp_dir("turn-diff-kinds");
        let cwd = root.join("project");
        fs::create_dir_all(&cwd).await.expect("create cwd");
        let deleted = cwd.join("deleted.txt");
        let binary = cwd.join("binary.dat");
        let created = cwd.join("created.txt");
        fs::write(&deleted, b"will be removed\n")
            .await
            .expect("create deleted source");
        fs::write(&binary, b"before\0binary")
            .await
            .expect("create binary source");
        let mut service = service(&root);

        service.make_snapshot("p1").await;
        service.track_edit(deleted.to_str().expect("UTF-8 deleted path"));
        service.track_edit(binary.to_str().expect("UTF-8 binary path"));
        service.track_edit(created.to_str().expect("UTF-8 created path"));
        fs::remove_file(&deleted).await.expect("delete source");
        fs::write(&binary, b"after\0binary")
            .await
            .expect("edit binary source");
        fs::write(&created, b"new file\n")
            .await
            .expect("create new file");
        service.make_snapshot("p2").await;

        let diff = service.get_turn_diff("p1").await.expect("snapshot exists");
        let lookup = |path: &str| {
            diff.files
                .iter()
                .find(|entry| entry.file_path == path)
                .expect("file appears in diff")
        };
        assert!(lookup("deleted.txt").is_deleted);
        assert!(lookup("created.txt").is_new_file);
        assert!(lookup("binary.dat").is_binary);
        assert!(lookup("binary.dat").hunks.is_empty());

        std::fs::remove_dir_all(root).expect("clean test directory");
    }

    #[tokio::test]
    async fn get_turn_diff_omits_oversized_endpoints_without_loading_them() {
        let root = temp_dir("turn-diff-oversized");
        let cwd = root.join("project");
        fs::create_dir_all(&cwd).await.expect("create cwd");
        let file = cwd.join("large.txt");
        fs::write(&file, b"small baseline\n")
            .await
            .expect("create source file");
        let mut service = service(&root);

        service.make_snapshot("p1").await;
        service.track_edit(file.to_str().expect("UTF-8 test path"));
        fs::write(&file, vec![b'x'; MAX_DIFF_SIZE_BYTES as usize + 1])
            .await
            .expect("grow source file");

        let diff = service.get_turn_diff("p1").await.expect("snapshot exists");
        assert_eq!(diff.stats.files_changed, 1);
        assert!(diff.files[0].oversized);
        assert!(diff.files[0].hunks.is_empty());
        assert_eq!(diff.files[0].lines_added, 0);
        assert_eq!(diff.files[0].lines_removed, 0);

        std::fs::remove_dir_all(root).expect("clean test directory");
    }

    #[tokio::test]
    async fn get_turn_diff_caps_candidate_paths_and_reports_omitted_count() {
        let root = temp_dir("turn-diff-cap");
        let mut tracked_file_backups = IndexMap::new();
        for index in 0..MAX_TURN_DIFF_FILES + 5 {
            tracked_file_backups.insert(
                format!("missing-{index:04}.txt"),
                FileHistoryBackup {
                    backup_file_name: None,
                    version: 1.0,
                    backup_time: Utc::now(),
                    failed: false,
                },
            );
        }
        let snapshot = FileHistorySnapshot {
            prompt_id: "p1".to_owned(),
            tracked_file_backups,
            timestamp: Utc::now(),
        };
        let mut service = service(&root);
        service.restore_from_snapshots(vec![snapshot]).await;

        let diff = service.get_turn_diff("p1").await.expect("snapshot exists");
        assert_eq!(diff.stats.files_omitted, 5);
        assert_eq!(diff.stats.files_changed, 0);
        assert!(diff.files.is_empty());

        std::fs::remove_dir_all(root).expect("clean test directory");
    }
}
