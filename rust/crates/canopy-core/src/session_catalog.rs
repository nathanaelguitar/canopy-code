//! Bounded session metadata listing, mirroring `SessionService.listSessions`.
//!
//! Listing reads at most ten transcript records per candidate and only scans
//! a fixed head/tail window for mutable titles. It deliberately does not count
//! messages or load full transcripts.

use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use serde::Serialize;
use serde_json::Value;

use crate::jsonl;
use crate::session_paths::{SessionArchiveState, SessionPaths, get_project_hash};

pub const MAX_SESSION_FILES_TO_PROCESS: usize = 10_000;
pub const MAX_PROMPT_SCAN_RECORDS: usize = 10;
const TITLE_WINDOW_BYTES: usize = 64 * 1024;
const MAX_RUNTIME_STATUS_BYTES: u64 = 64 * 1024;
const MAX_RETAINED_CANDIDATES: usize = MAX_SESSION_FILES_TO_PROCESS + 1;

#[derive(Clone, Debug, Default)]
pub struct ListSessionsOptions {
    /// Return sessions whose modification time is strictly less than this.
    pub cursor: Option<f64>,
    /// Maximum matching sessions to return. Defaults to 20.
    pub size: Option<usize>,
    pub archive_state: SessionArchiveState,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionListItem {
    pub session_id: String,
    pub cwd: String,
    pub start_time: String,
    pub mtime: f64,
    pub prompt: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub git_branch: Option<String>,
    pub file_path: PathBuf,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub custom_title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title_source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_id: Option<String>,
    pub is_archived: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ListSessionsResult {
    pub items: Vec<SessionListItem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<f64>,
    pub has_more: bool,
}

#[derive(Clone, Debug)]
struct Candidate {
    file_name_id: String,
    path: PathBuf,
    mtime: f64,
}

impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        self.mtime.total_cmp(&other.mtime) == Ordering::Equal
            && self.file_name_id == other.file_name_id
    }
}

impl Eq for Candidate {}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.mtime
            .total_cmp(&other.mtime)
            .then_with(|| self.file_name_id.cmp(&other.file_name_id))
    }
}

#[derive(Default)]
struct CreationMetadata {
    parent_session_id: Option<String>,
    source_type: Option<String>,
    source_id: Option<String>,
}

#[derive(Default)]
struct TitleInfo {
    title: Option<String>,
    source: Option<String>,
}

#[derive(Clone, Debug)]
pub struct SessionCatalog {
    paths: SessionPaths,
}

impl SessionCatalog {
    pub fn new(runtime_base_dir: impl Into<PathBuf>, project_root: impl Into<PathBuf>) -> Self {
        Self {
            paths: SessionPaths::new(runtime_base_dir, project_root),
        }
    }

    pub fn from_paths(paths: SessionPaths) -> Self {
        Self { paths }
    }

    pub fn list_sessions(&self, options: ListSessionsOptions) -> io::Result<ListSessionsResult> {
        let size = options.size.unwrap_or(20);
        let directory = self
            .paths
            .chats_directory(options.archive_state, cfg!(windows));
        let mut files = collect_candidates(&directory, options.cursor)?;
        files.sort_by(|left, right| {
            right
                .mtime
                .total_cmp(&left.mtime)
                .then_with(|| left.file_name_id.cmp(&right.file_name_id))
        });

        let mut result = ListSessionsResult::default();
        let mut last_processed_mtime = None;
        for (processed, file) in files.into_iter().enumerate() {
            if processed >= MAX_SESSION_FILES_TO_PROCESS {
                result.has_more = true;
                break;
            }
            if result.items.len() >= size {
                result.has_more = true;
                break;
            }

            last_processed_mtime = Some(file.mtime);
            if let Some(item) = self.read_candidate(&file, options.archive_state)? {
                result.items.push(item);
            }
        }

        if result.has_more {
            result.next_cursor = last_processed_mtime;
        }
        Ok(result)
    }

    /// Read one session directly by ID without scanning or listing the
    /// project's full transcript directory. Invalid IDs, missing files, and
    /// symlink/non-file entries return `Ok(None)`.
    pub fn get_session(
        &self,
        session_id: &str,
        archive_state: SessionArchiveState,
    ) -> io::Result<Option<SessionListItem>> {
        if !is_session_filename_id(session_id) {
            return Ok(None);
        }
        let Some(path) = self
            .paths
            .transcript_path(session_id, archive_state, cfg!(windows))
        else {
            return Ok(None);
        };
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => metadata,
            Ok(_) => return Ok(None),
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let modified = metadata.modified().unwrap_or(UNIX_EPOCH);
        let mtime = modified
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs_f64() * 1000.0)
            .unwrap_or_default();
        let item = self.read_candidate(
            &Candidate {
                file_name_id: session_id.to_owned(),
                path,
                mtime,
            },
            archive_state,
        )?;
        Ok(item.filter(|item| item.session_id == session_id))
    }

    fn read_candidate(
        &self,
        candidate: &Candidate,
        archive_state: SessionArchiveState,
    ) -> io::Result<Option<SessionListItem>> {
        let records = jsonl::read_lines(&candidate.path, MAX_PROMPT_SCAN_RECORDS)?;
        let Some(first) = records.first() else {
            return Ok(None);
        };
        let session_id = first
            .get("sessionId")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let Some(cwd) = first.get("cwd").and_then(Value::as_str) else {
            return Ok(None);
        };
        if session_id.is_empty() || !self.belongs_to_project(session_id, cwd) {
            return Ok(None);
        }

        let (prompt, creation) = extract_prompt_and_creation_metadata(&records);
        let title = read_title_info(&candidate.path);
        Ok(Some(SessionListItem {
            session_id: session_id.to_owned(),
            cwd: cwd.to_owned(),
            start_time: first
                .get("timestamp")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            mtime: candidate.mtime,
            prompt,
            git_branch: first
                .get("gitBranch")
                .and_then(Value::as_str)
                .map(str::to_owned),
            file_path: candidate.path.clone(),
            custom_title: title.title,
            title_source: title.source,
            parent_session_id: creation.parent_session_id,
            source_type: creation.source_type,
            source_id: creation.source_id,
            is_archived: archive_state == SessionArchiveState::Archived,
        }))
    }

    fn belongs_to_project(&self, session_id: &str, cwd: &str) -> bool {
        if get_project_hash(Path::new(cwd)) == self.paths.project_hash() {
            return true;
        }

        let separator = std::path::MAIN_SEPARATOR;
        let marker = format!("{separator}.canopy{separator}worktrees{separator}");
        if let Some((repo_root, _)) = cwd.rsplit_once(&marker)
            && !repo_root.is_empty()
            && get_project_hash(Path::new(repo_root)) == self.paths.project_hash()
        {
            return true;
        }
        if !crate::session_paths::is_valid_session_id(session_id) {
            return false;
        }
        read_runtime_status_work_dir(&self.paths, session_id).is_some_and(|work_dir| {
            get_project_hash(Path::new(&work_dir)) == self.paths.project_hash()
        })
    }
}

fn read_runtime_status_work_dir(paths: &SessionPaths, session_id: &str) -> Option<String> {
    let sidecar = paths
        .chats_directory(SessionArchiveState::Active, cfg!(windows))
        .join(format!("{session_id}.runtime.json"));
    let file = open_metadata_file(&sidecar).ok()?;
    if !file.metadata().ok()?.is_file() || file.metadata().ok()?.len() > MAX_RUNTIME_STATUS_BYTES {
        return None;
    }
    let mut bytes = Vec::with_capacity(MAX_RUNTIME_STATUS_BYTES as usize);
    file.take(MAX_RUNTIME_STATUS_BYTES + 1)
        .read_to_end(&mut bytes)
        .ok()?;
    if bytes.len() as u64 > MAX_RUNTIME_STATUS_BYTES {
        return None;
    }
    let data: Value = serde_json::from_slice(&bytes).ok()?;
    if data.get("schema_version").and_then(Value::as_u64) != Some(1)
        || data.get("session_id").and_then(Value::as_str) != Some(session_id)
    {
        return None;
    }
    data.get("work_dir")
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn collect_candidates(directory: &Path, cursor: Option<f64>) -> io::Result<Vec<Candidate>> {
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    // Keep the newest page plus one sentinel. The TypeScript catalog
    // processes at most MAX_SESSION_FILES_TO_PROCESS candidates per call;
    // retaining every filename would make the Rust picker grow without a
    // memory bound on a workspace with a large shared chat directory.
    let mut files: BinaryHeap<Reverse<Candidate>> =
        BinaryHeap::with_capacity(MAX_RETAINED_CANDIDATES);
    for entry in entries {
        let Ok(entry) = entry else { continue };
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let Some(session_id) = name.strip_suffix(".jsonl") else {
            continue;
        };
        if !is_session_filename_id(session_id) {
            continue;
        }
        let metadata = match fs::symlink_metadata(entry.path()) {
            Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => metadata,
            _ => continue,
        };
        let modified = metadata.modified().unwrap_or(UNIX_EPOCH);
        let mtime = modified
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs_f64() * 1000.0)
            .unwrap_or_default();
        if cursor.is_some_and(|cursor| mtime >= cursor) {
            continue;
        }
        let candidate = Candidate {
            file_name_id: session_id.to_owned(),
            path: entry.path(),
            mtime,
        };
        retain_newest_candidate(&mut files, candidate);
    }
    Ok(files.into_iter().map(|candidate| candidate.0).collect())
}

fn retain_newest_candidate(files: &mut BinaryHeap<Reverse<Candidate>>, candidate: Candidate) {
    if files.len() < MAX_RETAINED_CANDIDATES {
        files.push(Reverse(candidate));
    } else if files
        .peek()
        .is_some_and(|oldest_retained| candidate > oldest_retained.0)
    {
        files.pop();
        files.push(Reverse(candidate));
    }
}

fn is_session_filename_id(session_id: &str) -> bool {
    (32..=36).contains(&session_id.len())
        && session_id
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() || byte == b'-')
}

fn extract_prompt_and_creation_metadata(records: &[Value]) -> (String, CreationMetadata) {
    let mut metadata = CreationMetadata::default();
    let mut prompt = String::new();
    for record in records {
        if record.get("type").and_then(Value::as_str) == Some("system") {
            match record.get("subtype").and_then(Value::as_str) {
                Some("parent_session") if metadata.parent_session_id.is_none() => {
                    metadata.parent_session_id = record
                        .pointer("/systemPayload/parentSessionId")
                        .and_then(Value::as_str)
                        .filter(|value| !value.is_empty())
                        .map(str::to_owned);
                }
                Some("session_source") if metadata.source_type.is_none() => {
                    metadata.source_type = record
                        .pointer("/systemPayload/sourceType")
                        .and_then(Value::as_str)
                        .filter(|value| !value.is_empty())
                        .map(str::to_owned);
                    metadata.source_id = record
                        .pointer("/systemPayload/sourceId")
                        .and_then(Value::as_str)
                        .map(str::to_owned);
                }
                _ => {}
            }
        }

        if !prompt.is_empty()
            || record.get("type").and_then(Value::as_str) != Some("user")
            || record.get("subtype").is_some()
        {
            continue;
        }
        if let Some(display_text) = record
            .pointer("/systemPayload/displayText")
            .and_then(Value::as_str)
        {
            if !display_text.is_empty() {
                prompt = truncate_prompt(display_text);
                break;
            }
            continue;
        }
        if let Some(parts) = record.pointer("/message/parts").and_then(Value::as_array) {
            for part in parts {
                if let Some(text) = part.get("text").and_then(Value::as_str) {
                    if !text.is_empty() {
                        prompt = truncate_prompt(text);
                    }
                    break;
                }
            }
        }
    }
    (prompt, metadata)
}

fn truncate_prompt(text: &str) -> String {
    let mut chars = text.chars();
    let prompt: String = chars.by_ref().take(200).collect();
    if chars.next().is_some() {
        format!("{prompt}...")
    } else {
        prompt
    }
}

fn read_title_info(path: &Path) -> TitleInfo {
    let mut info = TitleInfo::default();
    let Ok(mut file) = open_metadata_file(path) else {
        return info;
    };
    let Ok(size) = file.metadata().map(|metadata| metadata.len()) else {
        return info;
    };
    if size == 0 {
        return info;
    }

    let tail_length = size.min(TITLE_WINDOW_BYTES as u64) as usize;
    if let Some(records) = read_window(&mut file, size - tail_length as u64, tail_length, true) {
        info = find_title_info(&records);
        if info.title.is_some() {
            return info;
        }
    }
    if size > TITLE_WINDOW_BYTES as u64
        && let Some(records) = read_window(&mut file, 0, TITLE_WINDOW_BYTES, false)
    {
        info = find_title_info(&records);
    }
    info
}

fn open_metadata_file(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    options.open(path)
}

fn read_window(file: &mut File, offset: u64, length: usize, tail: bool) -> Option<Vec<Value>> {
    let mut bytes = vec![0; length];
    file.seek(SeekFrom::Start(offset)).ok()?;
    let count = file.read(&mut bytes).ok()?;
    bytes.truncate(count);
    if count == 0 {
        return Some(Vec::new());
    }
    if tail && offset > 0 {
        if let Some(newline) = bytes.iter().position(|byte| *byte == b'\n') {
            bytes.drain(..=newline);
        } else {
            return Some(Vec::new());
        }
    } else if !tail && count == TITLE_WINDOW_BYTES {
        if let Some(newline) = bytes.iter().rposition(|byte| *byte == b'\n') {
            bytes.truncate(newline + 1);
        } else {
            bytes.clear();
        }
    }
    let text = String::from_utf8_lossy(&bytes);
    let mut records = Vec::new();
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        records.extend(jsonl::parse_line_tolerant(line.trim()));
    }
    Some(records)
}

fn find_title_info(records: &[Value]) -> TitleInfo {
    let mut info = TitleInfo::default();
    for record in records {
        if record.get("type").and_then(Value::as_str) != Some("system")
            || record.get("subtype").and_then(Value::as_str) != Some("custom_title")
        {
            continue;
        }
        let Some(title) = record
            .pointer("/systemPayload/customTitle")
            .and_then(Value::as_str)
            .filter(|title| !title.is_empty())
        else {
            continue;
        };
        info.title = Some(title.to_owned());
        info.source = record
            .pointer("/systemPayload/titleSource")
            .and_then(Value::as_str)
            .filter(|source| matches!(*source, "auto" | "manual"))
            .map(str::to_owned);
    }
    info
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use uuid::Uuid;

    fn temp_dir() -> PathBuf {
        std::env::temp_dir().join(format!("canopy-session-catalog-{}", Uuid::new_v4()))
    }

    fn write_session(catalog: &SessionCatalog, id: &str, cwd: &str, prompt: &str) -> PathBuf {
        let path = catalog
            .paths
            .transcript_path(id, SessionArchiveState::Active, cfg!(windows))
            .unwrap();
        jsonl::write(
            &path,
            &[
                json!({"uuid":"a","sessionId":id,"timestamp":"2026-09-24T00:00:00Z","type":"user","cwd":cwd,"message":{"parts":[{"text":prompt}]}}),
                json!({"uuid":"b","sessionId":id,"timestamp":"2026-09-24T00:00:01Z","type":"system","subtype":"custom_title","cwd":cwd,"systemPayload":{"customTitle":"A named session","titleSource":"manual"}}),
                json!({"uuid":"c","sessionId":id,"timestamp":"2026-09-24T00:00:02Z","type":"system","subtype":"parent_session","cwd":cwd,"systemPayload":{"parentSessionId":"parent-1"}}),
                json!({"uuid":"d","sessionId":id,"timestamp":"2026-09-24T00:00:03Z","type":"system","subtype":"session_source","cwd":cwd,"systemPayload":{"sourceType":"test","sourceId":"case-1"}}),
            ],
        )
        .unwrap();
        path
    }

    #[test]
    fn lists_project_sessions_with_bounded_prompt_and_creation_metadata() {
        let root = temp_dir();
        let workspace = root.join("workspace");
        fs::create_dir_all(&workspace).unwrap();
        let catalog = SessionCatalog::new(root.join("state"), &workspace);
        let id = "00000000-0000-4000-8000-000000000001";
        let prompt = "🦀".repeat(205);
        let path = write_session(&catalog, id, workspace.to_str().unwrap(), &prompt);

        let result = catalog
            .list_sessions(ListSessionsOptions::default())
            .unwrap();

        assert_eq!(result.items.len(), 1);
        let item = &result.items[0];
        assert_eq!(item.session_id, id);
        assert_eq!(item.prompt, format!("{}...", "🦀".repeat(200)));
        assert_eq!(item.custom_title.as_deref(), Some("A named session"));
        assert_eq!(item.title_source.as_deref(), Some("manual"));
        assert_eq!(item.parent_session_id.as_deref(), Some("parent-1"));
        assert_eq!(item.source_type.as_deref(), Some("test"));
        assert_eq!(item.source_id.as_deref(), Some("case-1"));
        assert_eq!(item.file_path, path);
        assert!(!result.has_more);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn uses_matching_runtime_status_for_legacy_session_paths() {
        let root = temp_dir();
        let workspace = root.join("workspace");
        let old_path = root.join("old-worktree-location");
        fs::create_dir_all(&workspace).unwrap();
        fs::create_dir_all(&old_path).unwrap();
        let catalog = SessionCatalog::new(root.join("state"), &workspace);
        let id = "00000000-0000-4000-8000-000000000001";
        write_session(&catalog, id, old_path.to_str().unwrap(), "legacy path");
        let runtime_status = catalog
            .paths
            .chats_directory(SessionArchiveState::Active, cfg!(windows))
            .join(format!("{id}.runtime.json"));
        fs::write(
            runtime_status,
            serde_json::to_vec(&json!({
                "schema_version": 1,
                "pid": 100,
                "session_id": id,
                "work_dir": workspace.to_str().unwrap(),
                "hostname": "test",
                "started_at": 1.0,
                "canopy_version": null
            }))
            .unwrap(),
        )
        .unwrap();

        let result = catalog
            .list_sessions(ListSessionsOptions::default())
            .unwrap();

        assert_eq!(result.items.len(), 1);
        assert_eq!(result.items[0].session_id, id);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn skips_other_projects_and_honors_exclusive_cursor_and_archive_state() {
        let root = temp_dir();
        let workspace = root.join("workspace");
        let other = root.join("other");
        fs::create_dir_all(&workspace).unwrap();
        fs::create_dir_all(&other).unwrap();
        let catalog = SessionCatalog::new(root.join("state"), &workspace);
        let first = write_session(
            &catalog,
            "00000000-0000-4000-8000-000000000001",
            workspace.to_str().unwrap(),
            "first",
        );
        std::thread::sleep(std::time::Duration::from_millis(5));
        write_session(
            &catalog,
            "00000000-0000-4000-8000-000000000002",
            other.to_str().unwrap(),
            "other project",
        );
        let first_mtime = collect_candidates(first.parent().unwrap(), None)
            .unwrap()
            .into_iter()
            .find(|candidate| candidate.file_name_id.ends_with("0001"))
            .unwrap()
            .mtime;

        let page = catalog
            .list_sessions(ListSessionsOptions {
                cursor: Some(first_mtime + 1.0),
                size: Some(10),
                archive_state: SessionArchiveState::Active,
            })
            .unwrap();
        assert!(
            page.items
                .iter()
                .all(|item| item.cwd == workspace.to_string_lossy())
        );
        assert_eq!(page.items.len(), 1);
        let exclusive_page = catalog
            .list_sessions(ListSessionsOptions {
                cursor: Some(first_mtime),
                size: Some(10),
                archive_state: SessionArchiveState::Active,
            })
            .unwrap();
        assert!(exclusive_page.items.is_empty());

        let archive_path = catalog
            .paths
            .transcript_path(
                "00000000-0000-4000-8000-000000000003",
                SessionArchiveState::Archived,
                cfg!(windows),
            )
            .unwrap();
        jsonl::write(
            &archive_path,
            &[json!({"uuid":"z","sessionId":"00000000-0000-4000-8000-000000000003","timestamp":"2026-09-24T00:00:00Z","type":"user","cwd":workspace.to_str().unwrap(),"message":{"parts":[{"text":"archived"}]}})],
        )
        .unwrap();
        let archived = catalog
            .list_sessions(ListSessionsOptions {
                archive_state: SessionArchiveState::Archived,
                ..ListSessionsOptions::default()
            })
            .unwrap();
        assert_eq!(archived.items.len(), 1);
        assert!(archived.items[0].is_archived);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn candidate_index_retains_only_the_newest_scan_window() {
        let mut candidates = BinaryHeap::new();
        for index in 0..=(MAX_RETAINED_CANDIDATES + 99) {
            retain_newest_candidate(
                &mut candidates,
                Candidate {
                    file_name_id: format!("{index:032x}"),
                    path: PathBuf::new(),
                    mtime: index as f64,
                },
            );
        }
        assert_eq!(candidates.len(), MAX_RETAINED_CANDIDATES);
        assert_eq!(candidates.peek().unwrap().0.mtime, 100.0);
    }
}
