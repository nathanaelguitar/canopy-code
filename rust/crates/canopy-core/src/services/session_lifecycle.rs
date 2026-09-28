//! Persisted-session removal, archive, restore, and title updates.
//!
//! This is the lifecycle slice of `packages/core/src/services/sessionService.ts`.
//! Transcript paths are derived from `SessionPaths`, transcript opens refuse
//! symlinks on Unix, and title records retain the JSONL parent chain.

use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::{SecondsFormat, Utc};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::jsonl;
use crate::session_paths::{SessionArchiveState, SessionPaths, is_valid_session_id};
use crate::storage::Storage;

const TAIL_READ_SIZE: u64 = 64 * 1024;
const MAX_USAGE_SALVAGE_BYTES: u64 = 64 * 1024 * 1024;

/// Error attached to one item in a bulk lifecycle operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionLifecycleItemError {
    pub session_id: String,
    pub error: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RemoveSessionsResult {
    pub removed: Vec<String>,
    pub not_found: Vec<String>,
    pub errors: Vec<SessionLifecycleItemError>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ArchiveSessionsResult {
    pub archived: Vec<String>,
    pub already_archived: Vec<String>,
    pub not_found: Vec<String>,
    pub errors: Vec<SessionLifecycleItemError>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct UnarchiveSessionsResult {
    pub unarchived: Vec<String>,
    pub already_active: Vec<String>,
    pub not_found: Vec<String>,
    pub errors: Vec<SessionLifecycleItemError>,
}

/// Supplying a known location skips the extra project-head scan, matching
/// `ArchiveSessionsOptions` / `UnarchiveSessionsOptions` in the TypeScript API.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ArchiveSessionsOptions {
    pub known_location: Option<SessionArchiveState>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct UnarchiveSessionsOptions {
    pub known_location: Option<SessionArchiveState>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionLocation {
    Active,
    Archived,
    Conflict,
}

/// Organization metadata is stored by a separate service. This hook keeps
/// lifecycle deletion ordered after transcript deletion, while allowing the
/// organization-store port to own its locking and schema validation.
pub trait SessionOrganizationCleanup: Send + Sync {
    fn remove_session(&self, project_root: &Path, session_id: &str) -> Result<(), String>;
    fn remove_sessions(&self, project_root: &Path, session_ids: &[String]) -> Result<(), String>;
}

pub type SessionLifecycleWarning = Arc<dyn Fn(String) + Send + Sync>;

#[derive(Clone)]
pub struct SessionLifecycle {
    paths: SessionPaths,
    organization: Option<Arc<dyn SessionOrganizationCleanup>>,
    on_warning: Option<SessionLifecycleWarning>,
}

impl std::fmt::Debug for SessionLifecycle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SessionLifecycle")
            .field("paths", &self.paths)
            .field("has_organization_cleanup", &self.organization.is_some())
            .field("has_warning_handler", &self.on_warning.is_some())
            .finish()
    }
}

impl SessionLifecycle {
    pub fn new(paths: SessionPaths) -> Self {
        Self {
            paths,
            organization: None,
            on_warning: None,
        }
    }

    pub fn with_organization_cleanup(
        mut self,
        organization: Arc<dyn SessionOrganizationCleanup>,
    ) -> Self {
        self.organization = Some(organization);
        self
    }

    pub fn with_warning_handler(mut self, on_warning: SessionLifecycleWarning) -> Self {
        self.on_warning = Some(on_warning);
        self
    }

    pub fn paths(&self) -> &SessionPaths {
        &self.paths
    }

    /// Return the persisted location only when the session belongs to this
    /// project. Invalid IDs and missing/foreign sessions return `None`.
    pub fn get_session_location(
        &self,
        session_id: &str,
    ) -> Result<Option<SessionLocation>, io::Error> {
        if !is_valid_session_id(session_id) {
            return Ok(None);
        }
        let active = self.read_project_head(session_id, SessionArchiveState::Active)?;
        let archived = self.read_project_head(session_id, SessionArchiveState::Archived)?;
        Ok(match (active.is_some(), archived.is_some()) {
            (true, true) => Some(SessionLocation::Conflict),
            (true, false) => Some(SessionLocation::Active),
            (false, true) => Some(SessionLocation::Archived),
            (false, false) => None,
        })
    }

    /// Remove a session transcript and its associated sidecars. Usage salvage
    /// is best-effort and never prevents deletion.
    pub fn remove_session(&self, session_id: &str) -> Result<bool, io::Error> {
        let removed = self.remove_session_files(session_id)?;
        if removed {
            self.remove_session_organization(session_id);
        }
        Ok(removed)
    }

    /// Remove several sessions independently. Duplicate IDs are ignored and
    /// results retain first-occurrence input order.
    pub fn remove_sessions(&self, session_ids: &[String]) -> RemoveSessionsResult {
        let unique_ids = dedupe(session_ids);
        let mut result = RemoveSessionsResult::default();
        for session_id in unique_ids {
            match self.remove_session_files(&session_id) {
                Ok(true) => result.removed.push(session_id),
                Ok(false) => result.not_found.push(session_id),
                Err(error) => result
                    .errors
                    .push(item_error(session_id, error.to_string())),
            }
        }
        self.remove_session_organizations(&result.removed);
        result
    }

    pub fn archive_sessions(&self, session_ids: &[String]) -> ArchiveSessionsResult {
        self.archive_sessions_with_options(session_ids, ArchiveSessionsOptions::default())
    }

    pub fn archive_sessions_with_options(
        &self,
        session_ids: &[String],
        options: ArchiveSessionsOptions,
    ) -> ArchiveSessionsResult {
        let mut result = ArchiveSessionsResult::default();
        for session_id in dedupe(session_ids) {
            let attempt = self.archive_one(&session_id, options);
            match attempt {
                Ok(ArchiveDisposition::Archived) => result.archived.push(session_id),
                Ok(ArchiveDisposition::AlreadyArchived) => result.already_archived.push(session_id),
                Ok(ArchiveDisposition::NotFound) => result.not_found.push(session_id),
                Err(error) => result.errors.push(item_error(session_id, error)),
            }
        }
        result
    }

    pub fn unarchive_sessions(&self, session_ids: &[String]) -> UnarchiveSessionsResult {
        self.unarchive_sessions_with_options(session_ids, UnarchiveSessionsOptions::default())
    }

    pub fn unarchive_sessions_with_options(
        &self,
        session_ids: &[String],
        options: UnarchiveSessionsOptions,
    ) -> UnarchiveSessionsResult {
        let mut result = UnarchiveSessionsResult::default();
        for session_id in dedupe(session_ids) {
            let attempt = self.unarchive_one(&session_id, options);
            match attempt {
                Ok(UnarchiveDisposition::Unarchived) => result.unarchived.push(session_id),
                Ok(UnarchiveDisposition::AlreadyActive) => result.already_active.push(session_id),
                Ok(UnarchiveDisposition::NotFound) => result.not_found.push(session_id),
                Err(error) => result.errors.push(item_error(session_id, error)),
            }
        }
        result
    }

    /// Append a linked custom-title system record. The first record provides
    /// cwd/version, and the tail UUID is used as `parentUuid` so history
    /// reconstruction continues through the title record.
    pub fn rename_session(&self, session_id: &str, title: &str) -> Result<bool, io::Error> {
        self.rename_session_with_options(
            session_id,
            title,
            TitleSource::Manual,
            SessionArchiveState::Active,
        )
    }

    pub fn rename_session_with_options(
        &self,
        session_id: &str,
        title: &str,
        title_source: TitleSource,
        archive_state: SessionArchiveState,
    ) -> Result<bool, io::Error> {
        if !is_valid_session_id(session_id) {
            return Ok(false);
        }
        let file_path = self.transcript_path(session_id, archive_state);
        let Some(first_record) = self.read_first_record(&file_path)? else {
            return Ok(false);
        };
        let Some(record_cwd) = first_record.get("cwd").and_then(Value::as_str) else {
            return Ok(false);
        };
        if !self.session_belongs_to_current_project(session_id, record_cwd)? {
            return Ok(false);
        }

        let title_record = make_title_record(
            session_id,
            title,
            title_source,
            &first_record,
            read_last_record_uuid(&file_path).as_deref(),
        );
        append_json_line_no_follow(&file_path, &title_record)?;
        Ok(true)
    }

    fn archive_one(
        &self,
        session_id: &str,
        options: ArchiveSessionsOptions,
    ) -> Result<ArchiveDisposition, String> {
        if !is_valid_session_id(session_id) {
            return Ok(ArchiveDisposition::NotFound);
        }
        if options.known_location != Some(SessionArchiveState::Active) {
            match self
                .get_session_location(session_id)
                .map_err(|error| error.to_string())?
            {
                None => return Ok(ArchiveDisposition::NotFound),
                Some(SessionLocation::Archived) => {
                    return Ok(ArchiveDisposition::AlreadyArchived);
                }
                Some(SessionLocation::Conflict) => {
                    return Err(format!("Session archive conflict: {session_id}"));
                }
                Some(SessionLocation::Active) => {}
            }
        }

        let source = self.transcript_path(session_id, SessionArchiveState::Active);
        let target = self.transcript_path(session_id, SessionArchiveState::Archived);
        if path_entry_exists(&target).map_err(|error| error.to_string())? {
            return Err(format!("Session archive conflict: {session_id}"));
        }
        fs::create_dir_all(parent_dir(&target)).map_err(|error| error.to_string())?;
        self.ensure_regular_nosymlink_transcript(&source)
            .map_err(|error| error.to_string())?;
        fs::rename(&source, &target).map_err(|error| move_error("archive", &error))?;

        let active_sidecar = self.worktree_sidecar_path(session_id, SessionArchiveState::Active);
        let archived_sidecar =
            self.worktree_sidecar_path(session_id, SessionArchiveState::Archived);
        if let Err(error) = move_optional_file(&active_sidecar, &archived_sidecar) {
            self.warn(format!(
                "archiveSessions: failed to move worktree sidecar for {session_id} from {} to {}: {error}",
                active_sidecar.display(), archived_sidecar.display()
            ));
        }
        Ok(ArchiveDisposition::Archived)
    }

    fn unarchive_one(
        &self,
        session_id: &str,
        options: UnarchiveSessionsOptions,
    ) -> Result<UnarchiveDisposition, String> {
        if !is_valid_session_id(session_id) {
            return Ok(UnarchiveDisposition::NotFound);
        }
        if options.known_location != Some(SessionArchiveState::Archived) {
            match self
                .get_session_location(session_id)
                .map_err(|error| error.to_string())?
            {
                None => return Ok(UnarchiveDisposition::NotFound),
                Some(SessionLocation::Active) => {
                    return Ok(UnarchiveDisposition::AlreadyActive);
                }
                Some(SessionLocation::Conflict) => {
                    return Err(format!("Session archive conflict: {session_id}"));
                }
                Some(SessionLocation::Archived) => {}
            }
        }

        let source = self.transcript_path(session_id, SessionArchiveState::Archived);
        let target = self.transcript_path(session_id, SessionArchiveState::Active);
        if path_entry_exists(&target).map_err(|error| error.to_string())? {
            return Err(format!("Session archive conflict: {session_id}"));
        }
        self.ensure_regular_nosymlink_transcript(&source)
            .map_err(|error| error.to_string())?;
        fs::create_dir_all(parent_dir(&target)).map_err(|error| error.to_string())?;
        fs::rename(&source, &target).map_err(|error| move_error("unarchive", &error))?;

        let archived_sidecar =
            self.worktree_sidecar_path(session_id, SessionArchiveState::Archived);
        let active_sidecar = self.worktree_sidecar_path(session_id, SessionArchiveState::Active);
        if let Err(error) = move_optional_file(&archived_sidecar, &active_sidecar) {
            self.warn(format!(
                "unarchiveSessions: failed to move worktree sidecar for {session_id} from {} to {}: {error}",
                archived_sidecar.display(), active_sidecar.display()
            ));
        }
        Ok(UnarchiveDisposition::Unarchived)
    }

    fn remove_session_files(&self, session_id: &str) -> Result<bool, io::Error> {
        if !is_valid_session_id(session_id) {
            return Ok(false);
        }

        let active_path = self.transcript_path(session_id, SessionArchiveState::Active);
        match self.read_project_head(session_id, SessionArchiveState::Active) {
            Ok(Some(_)) => {
                self.salvage_usage_best_effort(&active_path);
                remove_file_if_exists(&active_path)?;

                let archived_path = self.transcript_path(session_id, SessionArchiveState::Archived);
                if archived_path.exists() {
                    self.salvage_usage_best_effort(&archived_path);
                    remove_file_if_exists(&archived_path)?;
                }
                self.remove_worktree_sidecars(session_id)?;
                self.remove_file_history_backups(session_id)?;
                Ok(true)
            }
            Ok(None) => {
                let archived_path = self.transcript_path(session_id, SessionArchiveState::Archived);
                match self.read_project_head(session_id, SessionArchiveState::Archived) {
                    Ok(None) => Ok(false),
                    Ok(Some(_)) => {
                        self.salvage_usage_best_effort(&archived_path);
                        remove_file_if_exists(&archived_path)?;
                        self.remove_worktree_sidecars(session_id)?;
                        self.remove_file_history_backups(session_id)?;
                        Ok(true)
                    }
                    Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
                    Err(error) => Err(error),
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error),
        }
    }

    fn read_project_head(
        &self,
        session_id: &str,
        state: SessionArchiveState,
    ) -> Result<Option<Value>, io::Error> {
        let path = self.transcript_path(session_id, state);
        let first_record = match self.read_first_record(&path) {
            Ok(record) => record,
            Err(error) => {
                self.warn(format!(
                    "readProjectSessionHead: failed to read {}: {error}",
                    path.display()
                ));
                return Err(error);
            }
        };
        let Some(first_record) = first_record else {
            return Ok(None);
        };
        let Some(record_cwd) = first_record.get("cwd").and_then(Value::as_str) else {
            return Ok(None);
        };
        if self.session_belongs_to_current_project(session_id, record_cwd)? {
            Ok(Some(first_record))
        } else {
            Ok(None)
        }
    }

    fn read_first_record(&self, path: &Path) -> Result<Option<Value>, io::Error> {
        let file = match open_transcript_readonly(path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let mut reader = BufReader::new(file);
        loop {
            let Some(mut line) = read_bounded_line(&mut reader)? else {
                return Ok(None);
            };
            trim_jsonl_line_endings(&mut line);
            let line = String::from_utf8_lossy(&line);
            if let Some(record) = jsonl::parse_line_tolerant(line.trim()).into_iter().next() {
                return Ok(Some(record));
            }
        }
    }

    fn session_belongs_to_current_project(
        &self,
        session_id: &str,
        record_cwd: &str,
    ) -> Result<bool, io::Error> {
        let expected_hash = project_hash_from_cwd(&self.paths.project_root().to_string_lossy());
        if project_hash_from_cwd(record_cwd) == expected_hash {
            return Ok(true);
        }

        // Worktree sessions use <repo>/.canopy/worktrees/<slug> as cwd. Use
        // the innermost marker so nested worktrees resolve to their workspace.
        let marker = format!(
            "{sep}.canopy{sep}worktrees{sep}",
            sep = std::path::MAIN_SEPARATOR
        );
        if let Some(index) = record_cwd.rfind(&marker)
            && index > 0
            && project_hash_from_cwd(&record_cwd[..index]) == expected_hash
        {
            return Ok(true);
        }

        let status_path = Storage::with_runtime_base_dir(
            self.paths.project_root(),
            self.paths.runtime_base_dir(),
        )
        .get_runtime_status_path(session_id);
        Ok(read_runtime_work_dir(&status_path, session_id)
            .as_deref()
            .is_some_and(|work_dir| project_hash_from_cwd(work_dir) == expected_hash))
    }

    fn transcript_path(&self, session_id: &str, state: SessionArchiveState) -> PathBuf {
        self.paths
            .transcript_path(session_id, state, cfg!(windows))
            .expect("session path requested only after ID validation")
    }

    fn worktree_sidecar_path(&self, session_id: &str, state: SessionArchiveState) -> PathBuf {
        let transcript = self.transcript_path(session_id, state);
        transcript.with_file_name(format!("{session_id}.worktree.json"))
    }

    fn ensure_regular_nosymlink_transcript(&self, path: &Path) -> io::Result<()> {
        let file = open_transcript_readonly(path)?;
        if file.metadata()?.is_file() {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "session transcript is not a regular file",
            ))
        }
    }

    fn salvage_usage_best_effort(&self, transcript_path: &Path) {
        if let Err(error) = salvage_usage_from_safe_snapshot(transcript_path)
            && error.kind() != io::ErrorKind::NotFound
        {
            self.warn(format!(
                "usage salvage failed for {}: {error}; deleting anyway",
                transcript_path.display()
            ));
        }
    }

    fn remove_worktree_sidecars(&self, session_id: &str) -> Result<(), io::Error> {
        for state in [SessionArchiveState::Active, SessionArchiveState::Archived] {
            remove_file_if_exists(&self.worktree_sidecar_path(session_id, state))?;
        }
        Ok(())
    }

    fn remove_file_history_backups(&self, session_id: &str) -> Result<(), io::Error> {
        let backup_path = Storage::get_global_canopy_dir()
            .join("file-history")
            .join(session_id);
        match fs::symlink_metadata(&backup_path) {
            Ok(metadata) if metadata.file_type().is_dir() => fs::remove_dir_all(backup_path),
            Ok(_) => fs::remove_file(backup_path),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }

    fn remove_session_organization(&self, session_id: &str) {
        let Some(organization) = &self.organization else {
            return;
        };
        if let Err(error) = organization.remove_session(self.paths.project_root(), session_id) {
            self.warn(format!(
                "removeSession: failed to clear session organization for {session_id}: {error}"
            ));
        }
    }

    fn remove_session_organizations(&self, session_ids: &[String]) {
        if session_ids.is_empty() {
            return;
        }
        let Some(organization) = &self.organization else {
            return;
        };
        if let Err(error) = organization.remove_sessions(self.paths.project_root(), session_ids) {
            self.warn(format!(
                "removeSessions: failed to clear session organization for {}: {error}",
                session_ids.join(", ")
            ));
        }
    }

    fn warn(&self, message: String) {
        if let Some(handler) = &self.on_warning {
            handler(message);
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TitleSource {
    Manual,
    Auto,
}

impl TitleSource {
    fn as_str(self) -> &'static str {
        match self {
            Self::Manual => "manual",
            Self::Auto => "auto",
        }
    }
}

enum ArchiveDisposition {
    Archived,
    AlreadyArchived,
    NotFound,
}

enum UnarchiveDisposition {
    Unarchived,
    AlreadyActive,
    NotFound,
}

fn dedupe(session_ids: &[String]) -> Vec<String> {
    let mut unique = Vec::with_capacity(session_ids.len());
    let mut seen = HashSet::with_capacity(session_ids.len());
    for session_id in session_ids {
        if seen.insert(session_id) {
            unique.push(session_id.clone());
        }
    }
    unique
}

fn item_error(session_id: String, error: String) -> SessionLifecycleItemError {
    SessionLifecycleItemError { session_id, error }
}

fn parent_dir(path: &Path) -> &Path {
    path.parent().unwrap_or_else(|| Path::new("."))
}

fn path_entry_exists(path: &Path) -> io::Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

fn remove_file_if_exists(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn salvage_usage_from_safe_snapshot(transcript_path: &Path) -> io::Result<()> {
    let source = open_transcript_readonly(transcript_path)?;
    if source.metadata()?.len() > MAX_USAGE_SALVAGE_BYTES {
        return Ok(());
    }
    let snapshot_path =
        parent_dir(transcript_path).join(format!(".canopy-usage-salvage-{}.jsonl", Uuid::new_v4()));
    let mut snapshot = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&snapshot_path)?;
    let result = (|| {
        let copied = io::copy(
            &mut source.take(MAX_USAGE_SALVAGE_BYTES.saturating_add(1)),
            &mut snapshot,
        )?;
        if copied <= MAX_USAGE_SALVAGE_BYTES {
            snapshot.sync_all()?;
            let _ = crate::services::usage_history::persist_usage_before_transcript_deletion(
                Storage::get_global_canopy_dir(),
                &snapshot_path,
            );
        }
        Ok(())
    })();
    drop(snapshot);
    let _ = fs::remove_file(snapshot_path);
    result
}

fn move_optional_file(source: &Path, target: &Path) -> io::Result<bool> {
    if !source.exists() {
        return Ok(false);
    }
    if path_entry_exists(target)? {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "Archive sidecar conflict: destination already exists",
        ));
    }
    fs::create_dir_all(parent_dir(target))?;
    fs::rename(source, target)?;
    Ok(true)
}

fn move_error(action: &str, error: &io::Error) -> String {
    let code = match error.kind() {
        io::ErrorKind::NotFound => "ENOENT".to_owned(),
        io::ErrorKind::PermissionDenied => "EACCES".to_owned(),
        io::ErrorKind::AlreadyExists => "EEXIST".to_owned(),
        io::ErrorKind::InvalidInput => "EINVAL".to_owned(),
        _ => error
            .raw_os_error()
            .map(|code| code.to_string())
            .unwrap_or_else(|| "unknown error".to_owned()),
    };
    format!("Failed to {action} session file: {code}")
}

fn open_transcript_readonly(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = options.open(path)?;
    if !file.metadata()?.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "session transcript is not a regular file",
        ));
    }
    Ok(file)
}

fn read_bounded_line<R: BufRead>(reader: &mut R) -> io::Result<Option<Vec<u8>>> {
    let mut line = Vec::new();
    let mut started = false;
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return Ok(started.then_some(line));
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let content_len = newline.unwrap_or(available.len());
        if line.len().saturating_add(content_len) > jsonl::MAX_JSONL_RECORD_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "JSONL physical line exceeds configured record limit",
            ));
        }
        line.extend_from_slice(&available[..content_len]);
        started = true;
        let consumed = newline.map_or(content_len, |index| index + 1);
        reader.consume(consumed);
        if newline.is_some() {
            return Ok(Some(line));
        }
    }
}

fn trim_jsonl_line_endings(line: &mut Vec<u8>) {
    if line.last() == Some(&b'\n') {
        line.pop();
    }
    if line.last() == Some(&b'\r') {
        line.pop();
    }
}

fn project_hash_from_cwd(cwd: &str) -> String {
    let normalized = if cfg!(windows) {
        cwd.to_lowercase()
    } else {
        cwd.to_owned()
    };
    format!("{:x}", Sha256::digest(normalized.as_bytes()))
}

fn read_runtime_work_dir(path: &Path, expected_session_id: &str) -> Option<String> {
    let file = open_transcript_readonly(path).ok()?;
    if file.metadata().ok()?.len() > 1024 * 1024 {
        return None;
    }
    let value: Value = serde_json::from_reader(file).ok()?;
    if value.get("schema_version")?.as_i64()? != 1
        || value.get("session_id")?.as_str()? != expected_session_id
        || value.get("pid")?.as_i64().is_none()
        || value.get("hostname")?.as_str().is_none()
        || !value.get("started_at")?.as_f64()?.is_finite()
    {
        return None;
    }
    let canopy_version = value.get("canopy_version")?;
    if !(canopy_version.is_null() || canopy_version.is_string()) {
        return None;
    }
    value.get("work_dir")?.as_str().map(str::to_owned)
}

fn read_last_record_uuid(path: &Path) -> Option<String> {
    let mut file = open_transcript_readonly(path).ok()?;
    let file_size = file.metadata().ok()?.len();
    let read_start = file_size.saturating_sub(TAIL_READ_SIZE);
    let read_length = file_size.min(TAIL_READ_SIZE) as usize;
    let mut buffer = vec![0_u8; read_length];
    file.seek(SeekFrom::Start(read_start)).ok()?;
    file.read_exact(&mut buffer).ok()?;

    let first_segment_is_partial = if read_start > 0 {
        file.seek(SeekFrom::Start(read_start - 1)).ok()?;
        let mut peek = [0_u8; 1];
        file.read_exact(&mut peek).ok()?;
        peek[0] != b'\n'
    } else {
        false
    };

    let mut lines: Vec<&[u8]> = buffer.split(|byte| *byte == b'\n').collect();
    if first_segment_is_partial && !lines.is_empty() {
        lines.remove(0);
    }
    for line in lines.into_iter().rev() {
        let line = String::from_utf8_lossy(line);
        let records = jsonl::parse_line_tolerant(line.trim());
        for record in records.into_iter().rev() {
            if let Some(uuid) = record
                .get("uuid")
                .and_then(Value::as_str)
                .filter(|uuid| !uuid.is_empty())
            {
                return Some(uuid.to_owned());
            }
        }
    }
    None
}

fn make_title_record(
    session_id: &str,
    title: &str,
    title_source: TitleSource,
    first_record: &Value,
    parent_uuid: Option<&str>,
) -> Value {
    let mut record = json!({
        "uuid": Uuid::new_v4().to_string(),
        "parentUuid": parent_uuid,
        "sessionId": session_id,
        "timestamp": Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
        "type": "system",
        "subtype": "custom_title",
        "systemPayload": { "customTitle": title, "titleSource": title_source.as_str() },
    });
    if let Some(cwd) = first_record.get("cwd") {
        record["cwd"] = cwd.clone();
    }
    if let Some(version) = first_record.get("version") {
        record["version"] = version.clone();
    }
    record
}

fn append_json_line_no_follow(path: &Path, value: &Value) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let encoded = jsonl::encode_line(value)?;
    let mut options = OpenOptions::new();
    options.create(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let mut file = options.open(path)?;
    if !file.metadata()?.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "session transcript is not a regular file",
        ));
    }
    file.write_all(&encoded)?;
    file.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn test_root() -> PathBuf {
        let tick = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("canopy-session-lifecycle-{tick}"))
    }

    fn lifecycle(root: &Path) -> SessionLifecycle {
        SessionLifecycle::new(SessionPaths::new(root.join("state"), "/workspace/project"))
    }

    fn record(session_id: &str, uuid: &str) -> Value {
        json!({
            "uuid": uuid,
            "parentUuid": null,
            "sessionId": session_id,
            "timestamp": "2026-01-01T00:00:00.000Z",
            "type": "user",
            "cwd": "/workspace/project",
            "version": "test",
            "message": {"role":"user", "parts":[{"text":"hello"}]}
        })
    }

    fn write_session(lifecycle: &SessionLifecycle, session_id: &str) -> PathBuf {
        let path = lifecycle.transcript_path(session_id, SessionArchiveState::Active);
        jsonl::write(&path, &[record(session_id, "tail-uuid")]).unwrap();
        path
    }

    #[test]
    fn archive_unarchive_moves_transcript_and_worktree_sidecar() {
        let root = test_root();
        let lifecycle = lifecycle(&root);
        let session_id = Uuid::new_v4().to_string();
        let active = write_session(&lifecycle, &session_id);
        let active_sidecar =
            lifecycle.worktree_sidecar_path(&session_id, SessionArchiveState::Active);
        fs::write(&active_sidecar, "worktree").unwrap();

        let archived = lifecycle.archive_sessions(std::slice::from_ref(&session_id));
        assert_eq!(
            archived.archived.as_slice(),
            std::slice::from_ref(&session_id)
        );
        let archive_path = lifecycle.transcript_path(&session_id, SessionArchiveState::Archived);
        assert!(archive_path.exists());
        assert!(!active.exists());
        assert!(
            lifecycle
                .worktree_sidecar_path(&session_id, SessionArchiveState::Archived)
                .exists()
        );

        let restored = lifecycle.unarchive_sessions(std::slice::from_ref(&session_id));
        assert_eq!(
            restored.unarchived.as_slice(),
            std::slice::from_ref(&session_id)
        );
        assert!(active.exists());
        assert!(
            lifecycle
                .worktree_sidecar_path(&session_id, SessionArchiveState::Active)
                .exists()
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rename_appends_a_title_record_chained_to_the_tail_uuid() {
        let root = test_root();
        let lifecycle = lifecycle(&root);
        let session_id = Uuid::new_v4().to_string();
        let path = write_session(&lifecycle, &session_id);

        assert!(
            lifecycle
                .rename_session_with_options(
                    &session_id,
                    "A title",
                    TitleSource::Manual,
                    SessionArchiveState::Active
                )
                .unwrap()
        );
        let records = jsonl::read(&path).unwrap();
        let title = records.last().unwrap();
        assert_eq!(title["parentUuid"], "tail-uuid");
        assert_eq!(title["sessionId"], session_id);
        assert_eq!(title["cwd"], "/workspace/project");
        assert_eq!(title["version"], "test");
        assert_eq!(title["subtype"], "custom_title");
        assert_eq!(title["systemPayload"]["customTitle"], "A title");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rename_anchors_after_last_recovered_record_on_a_glued_jsonl_line() {
        let root = test_root();
        let lifecycle = lifecycle(&root);
        let session_id = Uuid::new_v4().to_string();
        let path = write_session(&lifecycle, &session_id);
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(b"{\"uuid\":\"older\"}{\"uuid\":\"latest\"}\n")
            .unwrap();

        assert!(lifecycle.rename_session(&session_id, "A title").unwrap());
        let records = jsonl::read(&path).unwrap();
        assert_eq!(records.last().unwrap()["parentUuid"], "latest");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn foreign_project_heads_do_not_appear_or_accept_renames() {
        let root = test_root();
        let lifecycle = lifecycle(&root);
        let session_id = Uuid::new_v4().to_string();
        let path = lifecycle.transcript_path(&session_id, SessionArchiveState::Active);
        let mut foreign = record(&session_id, "foreign");
        foreign["cwd"] = json!("/another/project");
        jsonl::write(&path, &[foreign]).unwrap();

        assert_eq!(lifecycle.get_session_location(&session_id).unwrap(), None);
        assert!(!lifecycle.rename_session(&session_id, "unsafe").unwrap());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn archive_reports_conflicting_active_and_archived_copies_per_id() {
        let root = test_root();
        let lifecycle = lifecycle(&root);
        let session_id = Uuid::new_v4().to_string();
        let _active = write_session(&lifecycle, &session_id);
        let archived = lifecycle.transcript_path(&session_id, SessionArchiveState::Archived);
        jsonl::write(&archived, &[record(&session_id, "archive-copy")]).unwrap();

        assert_eq!(
            lifecycle.get_session_location(&session_id).unwrap(),
            Some(SessionLocation::Conflict)
        );
        let result = lifecycle.archive_sessions(std::slice::from_ref(&session_id));
        assert!(result.archived.is_empty());
        assert_eq!(result.errors.len(), 1);
        assert_eq!(result.errors[0].session_id, session_id);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn bulk_remove_deduplicates_and_reports_invalid_ids_as_not_found() {
        let root = test_root();
        let lifecycle = lifecycle(&root);
        let session_id = Uuid::new_v4().to_string();
        let path = write_session(&lifecycle, &session_id);
        let results = lifecycle.remove_sessions(&[
            session_id.clone(),
            session_id.clone(),
            "../invalid".to_owned(),
        ]);
        assert_eq!(results.removed, [session_id]);
        assert_eq!(results.not_found, ["../invalid"]);
        assert!(results.errors.is_empty());
        assert!(!path.exists());
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn project_head_and_rename_refuse_transcript_symlinks() {
        use std::os::unix::fs::symlink;

        let root = test_root();
        let lifecycle = lifecycle(&root);
        let session_id = Uuid::new_v4().to_string();
        let path = lifecycle.transcript_path(&session_id, SessionArchiveState::Active);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let outside = root.join("outside.jsonl");
        jsonl::write(&outside, &[record(&session_id, "outside")]).unwrap();
        symlink(&outside, &path).unwrap();
        assert!(
            lifecycle
                .rename_session_with_options(
                    &session_id,
                    "unsafe",
                    TitleSource::Manual,
                    SessionArchiveState::Active
                )
                .is_err()
        );
        assert!(lifecycle.get_session_location(&session_id).is_err());
        fs::remove_dir_all(root).unwrap();
    }
}
