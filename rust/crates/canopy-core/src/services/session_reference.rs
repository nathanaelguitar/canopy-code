//! Deterministically slim a persisted session for read-only prompt reference.
//!
//! Port of `packages/core/src/services/session-reference-service.ts`. This
//! service reads only the active transcript, follows its selected parent chain,
//! projects visible user/assistant text, replaces tool results with status-only
//! summaries, and retains the newest lines that fit the requested token budget.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;

use crate::services::token_estimation::estimate_content_tokens;
use crate::session_paths::get_project_hash;
use crate::session_store::{SessionStore, SessionStoreError};
use crate::transcript::{
    TranscriptRecord, TranscriptRecordType, project_user_transcript_for_display,
};

pub const SESSION_REF_TOKEN_BUDGET: f64 = 8_000.0;
const TITLE_MAX_LENGTH: usize = 80;
const TITLE_WINDOW_BYTES: usize = 64 * 1024;
const EARLIER_TURNS_OMITTED: &str = "[earlier turns omitted]";

/// Count and one ID are sufficient for the CLI to distinguish no match, one
/// match, and ambiguity without retaining every matching session in memory.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ActiveSessionTitleMatches {
    pub first_session_id: Option<String>,
    pub count: usize,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct SessionReferenceOptions {
    pub budget_tokens: Option<f64>,
    /// A mention supplied by title is kept as the displayed title even when
    /// the transcript has since been renamed.
    pub title: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SlimmedSessionReferenceMeta {
    pub session_id: String,
    pub title: String,
    pub message_count: usize,
    pub approx_tokens: f64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SlimmedSessionReference {
    pub text: String,
    pub meta: SlimmedSessionReferenceMeta,
    pub truncated: bool,
}

#[derive(Debug, Error)]
pub enum SessionReferenceError {
    #[error(transparent)]
    Store(#[from] SessionStoreError),
    #[error(transparent)]
    Transcript(#[from] crate::transcript::TranscriptRecordPreparationError),
    #[error("session transcript could not be serialized for reference projection")]
    Serialization,
}

/// Project-scoped reader for prior sessions. A missing, empty, malformed-only,
/// invalid-ID, or foreign-project session resolves to `Ok(None)`. I/O and
/// transcript-projection errors remain errors so a caller can display a useful
/// diagnostic without aborting the prompt.
#[derive(Clone, Debug)]
pub struct SessionReferenceService {
    store: SessionStore,
    project_root: PathBuf,
}

impl SessionReferenceService {
    pub fn new(runtime_base_dir: impl Into<PathBuf>, project_root: impl Into<PathBuf>) -> Self {
        let project_root = project_root.into();
        Self {
            store: SessionStore::new(runtime_base_dir, project_root.clone()),
            project_root,
        }
    }

    /// Load and slim one active session. Corrupt JSONL fragments are recovered
    /// or skipped by the shared reader; malformed identity/projection records
    /// are skipped by transcript preparation. No provider or model call occurs.
    pub async fn resolve(
        &self,
        session_id: &str,
        options: SessionReferenceOptions,
    ) -> Result<Option<SlimmedSessionReference>, SessionReferenceError> {
        let raw_records = match self.store.read_transcript(
            session_id,
            crate::session_paths::SessionArchiveState::Active,
        ) {
            Ok(records) => records,
            // Like SessionService.loadSession, an invalid filename token is a
            // normal miss. The store rejects it before touching the filesystem.
            Err(SessionStoreError::InvalidSessionId(_)) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if raw_records.is_empty() {
            return Ok(None);
        }

        // SessionService checks the first physical record's cwd before it
        // reconstructs history. Keep that project boundary here, including
        // durable worktree paths whose repo root is this project.
        if !session_belongs_to_project(
            &raw_records[0],
            session_id,
            self.store.paths().chats_directory(
                crate::session_paths::SessionArchiveState::Active,
                cfg!(windows),
            ),
            &self.project_root,
        )
        .await
        {
            return Ok(None);
        }

        let prepared =
            crate::transcript::prepare_transcript_records(&Value::Array(raw_records), None)?;
        if prepared.session_id.as_deref() != Some(session_id) || prepared.records.is_empty() {
            return Ok(None);
        }
        let records = prepared.records;
        let lines = records_to_lines(&records)?;
        let budget = options.budget_tokens.unwrap_or(SESSION_REF_TOKEN_BUDGET);
        let title = options
            .title
            .or_else(|| derive_title(&records))
            .unwrap_or_else(|| session_id.to_owned());
        let header = format!("--- Referenced session \"{title}\" (slimmed, read-only) ---");
        let header_cost = estimate_lines(std::slice::from_ref(&header));

        // Estimate each candidate line once, then retain from newest to oldest.
        let per_line_cost = lines
            .iter()
            .map(|line| estimate_lines(std::slice::from_ref(line)))
            .collect::<Vec<_>>();
        let mut total = header_cost;
        let mut start = lines.len();
        while start > 0 && total + per_line_cost[start - 1] <= budget {
            total += per_line_cost[start - 1];
            start -= 1;
        }
        // Even when the newest line alone exceeds the budget, preserve it.
        if start == lines.len() && !lines.is_empty() {
            start = lines.len() - 1;
        }
        let kept = &lines[start..];
        let truncated = start > 0;
        let overhead = header_cost
            + if truncated {
                estimate_lines(&[EARLIER_TURNS_OMITTED.to_owned()])
            } else {
                0.0
            };
        let mut body = String::new();
        if truncated {
            body.push_str(EARLIER_TURNS_OMITTED);
            body.push('\n');
        }
        body.push_str(&kept.join("\n"));
        let text = if trim_js(&body).is_empty() {
            format!("{header}\n(no textual content)")
        } else {
            format!("{header}\n{body}")
        };

        Ok(Some(SlimmedSessionReference {
            text,
            meta: SlimmedSessionReferenceMeta {
                session_id: session_id.to_owned(),
                title,
                message_count: records.len(),
                approx_tokens: estimate_lines(kept) + overhead,
            },
            truncated,
        }))
    }

    /// Find an active session by custom title using bounded reads per file.
    /// The directory is streamed entry by entry; title metadata is read from
    /// at most one 64 KiB tail and, on a miss, one 64 KiB head. Matching files
    /// need only their first JSONL record for the project ownership check.
    ///
    /// Unlike the general-purpose session catalog page, this search has no
    /// 10,000-file cap, so ambiguity is computed across all active sessions
    /// while memory use stays bounded independently of transcript size and
    /// match count.
    pub async fn find_active_sessions_by_title(
        &self,
        title: &str,
    ) -> io::Result<ActiveSessionTitleMatches> {
        let normalized_title = title.trim().to_lowercase();
        let chats_dir = self.store.paths().chats_directory(
            crate::session_paths::SessionArchiveState::Active,
            cfg!(windows),
        );
        let entries = match fs::read_dir(&chats_dir) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(ActiveSessionTitleMatches::default());
            }
            Err(error) => return Err(error),
        };

        let mut matches = ActiveSessionTitleMatches::default();
        for entry in entries.flatten() {
            let Some(file_name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            let Some(session_file_id) = file_name.strip_suffix(".jsonl") else {
                continue;
            };
            if !crate::session_paths::is_valid_session_id(session_file_id) {
                continue;
            }
            let file_path = entry.path();
            if !fs::symlink_metadata(&file_path)
                .is_ok_and(|metadata| metadata.is_file() && !metadata.file_type().is_symlink())
            {
                continue;
            }
            let Some(candidate_title) = read_custom_title(&file_path) else {
                continue;
            };
            if candidate_title.trim().to_lowercase() != normalized_title {
                continue;
            }

            let Some(first_record) = read_first_transcript_record(&file_path) else {
                continue;
            };
            let Some(session_id) = first_record
                .get("sessionId")
                .and_then(Value::as_str)
                .filter(|session_id| crate::session_paths::is_valid_session_id(session_id))
            else {
                continue;
            };
            if !session_belongs_to_project(
                &first_record,
                session_id,
                chats_dir.clone(),
                &self.project_root,
            )
            .await
            {
                continue;
            }

            matches.count = matches.count.saturating_add(1);
            if matches.first_session_id.is_none() {
                matches.first_session_id = Some(session_id.to_owned());
            }
        }
        Ok(matches)
    }
}

fn read_custom_title(path: &Path) -> Option<String> {
    let mut file = open_transcript_metadata(path).ok()?;
    let size = file.metadata().ok()?.len();
    if size == 0 {
        return None;
    }

    let tail_length = size.min(TITLE_WINDOW_BYTES as u64) as usize;
    if let Some(title) = read_title_window(&mut file, size - tail_length as u64, tail_length, true)
        && !title.is_empty()
    {
        return Some(title);
    }
    if size <= TITLE_WINDOW_BYTES as u64 {
        return None;
    }
    read_title_window(&mut file, 0, TITLE_WINDOW_BYTES, false).filter(|title| !title.is_empty())
}

fn open_transcript_metadata(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    options.open(path)
}

fn read_title_window(file: &mut File, offset: u64, length: usize, tail: bool) -> Option<String> {
    let mut bytes = vec![0; length];
    file.seek(SeekFrom::Start(offset)).ok()?;
    let count = file.read(&mut bytes).ok()?;
    bytes.truncate(count);
    if count == 0 {
        return None;
    }
    if tail && offset > 0 {
        if let Some(newline) = bytes.iter().position(|byte| *byte == b'\n') {
            bytes.drain(..=newline);
        } else {
            return None;
        }
    } else if !tail && count == TITLE_WINDOW_BYTES {
        if let Some(newline) = bytes.iter().rposition(|byte| *byte == b'\n') {
            bytes.truncate(newline + 1);
        } else {
            bytes.clear();
        }
    }

    let text = String::from_utf8_lossy(&bytes);
    let mut latest_title = None;
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        for record in crate::jsonl::parse_line_tolerant(line.trim()) {
            if record.get("type").and_then(Value::as_str) == Some("system")
                && record.get("subtype").and_then(Value::as_str) == Some("custom_title")
                && let Some(title) = record
                    .pointer("/systemPayload/customTitle")
                    .and_then(Value::as_str)
                    .filter(|title| !title.is_empty())
            {
                latest_title = Some(title.to_owned());
            }
        }
    }
    latest_title
}

fn read_first_transcript_record(path: &Path) -> Option<Value> {
    let file = open_transcript_metadata(path).ok()?;
    let mut reader = BufReader::new(file);
    let mut bytes = Vec::new();
    reader
        .by_ref()
        .take((crate::jsonl::MAX_JSONL_RECORD_BYTES + 1) as u64)
        .read_until(b'\n', &mut bytes)
        .ok()?;
    if bytes.len() > crate::jsonl::MAX_JSONL_RECORD_BYTES {
        return None;
    }
    if bytes.last() == Some(&b'\n') {
        bytes.pop();
    }
    if bytes.last() == Some(&b'\r') {
        bytes.pop();
    }
    crate::jsonl::parse_line_tolerant(&String::from_utf8_lossy(&bytes))
        .into_iter()
        .next()
}

async fn session_belongs_to_project(
    record: &Value,
    session_id: &str,
    chats_dir: PathBuf,
    project_root: &Path,
) -> bool {
    let Some(record_cwd) = record.get("cwd").and_then(Value::as_str) else {
        return false;
    };
    if get_project_hash(Path::new(record_cwd)) == get_project_hash(project_root) {
        return true;
    }

    // Worktree sessions record the worktree cwd, which hashes differently
    // from the configured project. The innermost marker handles nested trees.
    let marker = if cfg!(windows) {
        "\\.canopy\\worktrees\\"
    } else {
        "/.canopy/worktrees/"
    };
    if record_cwd.rfind(marker).is_some_and(|index| {
        get_project_hash(Path::new(&record_cwd[..index])) == get_project_hash(project_root)
    }) {
        return true;
    }

    let status_path = chats_dir.join(format!("{session_id}.runtime.json"));
    crate::utils::runtime_status::read_runtime_status(status_path, None)
        .await
        .ok()
        .flatten()
        .is_some_and(|status| {
            status.session_id == session_id
                && get_project_hash(Path::new(&status.work_dir)) == get_project_hash(project_root)
        })
}

fn records_to_lines(records: &[TranscriptRecord]) -> Result<Vec<String>, SessionReferenceError> {
    let mut lines = Vec::new();
    for record in records {
        let message = record
            .message
            .as_ref()
            .map(serde_json::to_value)
            .transpose()
            .map_err(|_| SessionReferenceError::Serialization)?;
        if record.record_type == TranscriptRecordType::User {
            let payload = record.extra.get("systemPayload");
            let projection = project_user_transcript_for_display(message.as_ref(), payload);
            let text = projection
                .display_text
                .as_deref()
                .map(trim_js)
                .map(str::to_owned)
                .unwrap_or_else(|| visible_text_parts(&projection.parts));
            if !text.is_empty() {
                lines.push(format!("User: {text}"));
            }
        } else if record.record_type == TranscriptRecordType::Assistant {
            let text = message
                .as_ref()
                .and_then(|value| value.get("parts"))
                .and_then(Value::as_array)
                .map(|parts| visible_text_parts(parts))
                .unwrap_or_default();
            if !text.is_empty() {
                lines.push(format!("Assistant: {text}"));
            }
        }

        // Only the response side is authoritative for status. Never serialize
        // the result body into the reference block.
        let tool_result = record.extra.get("toolCallResult");
        for name in function_response_names(message.as_ref()) {
            let status = if tool_result
                .and_then(|result| result.get("error"))
                .is_some_and(js_truthy)
            {
                "error".to_owned()
            } else {
                match tool_result.and_then(|result| result.get("status")) {
                    None | Some(Value::Null) => "ok".to_owned(),
                    Some(Value::String(value)) if value == "success" => "ok".to_owned(),
                    Some(value) => js_string(value),
                }
            };
            lines.push(format!("[tool: {name} — {status}]"));
        }
    }
    Ok(lines)
}

fn visible_text_parts(parts: &[Value]) -> String {
    let mut text = String::new();
    for part in parts {
        if part.get("thought").is_some_and(js_truthy) {
            continue;
        }
        if let Some(value) = part.get("text").and_then(Value::as_str) {
            if !value.is_empty() {
                text.push_str(value);
            }
        }
    }
    trim_js(&text).to_owned()
}

fn function_response_names(message: Option<&Value>) -> Vec<String> {
    message
        .and_then(|message| message.get("parts"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|part| {
            part.get("functionResponse")
                .and_then(|response| response.get("name"))
                .and_then(Value::as_str)
                .filter(|name| !name.is_empty())
                .map(str::to_owned)
        })
        .collect()
}

fn derive_title(records: &[TranscriptRecord]) -> Option<String> {
    // The latest nonempty custom title wins, matching append-only rename rows.
    let mut custom_title = None;
    for record in records {
        if record.record_type == TranscriptRecordType::System
            && record.subtype.as_deref() == Some("custom_title")
        {
            if let Some(title) = record
                .extra
                .get("systemPayload")
                .and_then(|payload| payload.get("customTitle"))
                .and_then(Value::as_str)
                .filter(|title| !title.is_empty())
            {
                custom_title = Some(title.to_owned());
            }
        }
    }
    if custom_title.is_some() {
        return custom_title;
    }
    for record in records {
        if record.record_type != TranscriptRecordType::User {
            continue;
        }
        let message = record
            .message
            .as_ref()
            .and_then(|message| serde_json::to_value(message).ok());
        let projection = project_user_transcript_for_display(
            message.as_ref(),
            record.extra.get("systemPayload"),
        );
        let text = projection
            .display_text
            .as_deref()
            .map(trim_js)
            .map(str::to_owned)
            .unwrap_or_else(|| visible_text_parts(&projection.parts));
        let Some(first_line) = text
            .lines()
            .next()
            .map(trim_js)
            .filter(|line| !line.is_empty())
        else {
            continue;
        };
        return Some(truncate_title(first_line));
    }
    None
}

fn truncate_title(title: &str) -> String {
    let units = title.encode_utf16().count();
    if units <= TITLE_MAX_LENGTH {
        return title.to_owned();
    }
    let mut truncated = String::new();
    let mut used = 0;
    for character in title.chars() {
        let character_units = character.len_utf16();
        if used + character_units > TITLE_MAX_LENGTH - 3 {
            break;
        }
        used += character_units;
        truncated.push(character);
    }
    truncated.push_str("...");
    truncated
}

fn estimate_lines(lines: &[String]) -> f64 {
    let joined = lines.join("\n");
    estimate_content_tokens(&[json!({"role":"user", "parts":[{"text":joined}]})])
}

fn js_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_f64().is_some_and(|number| number != 0.0),
        Value::String(value) => !value.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}

fn js_string(value: &Value) -> String {
    match value {
        Value::Null => "null".to_owned(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => value.clone(),
        Value::Array(values) => values
            .iter()
            .map(|value| {
                if value.is_null() {
                    String::new()
                } else {
                    js_string(value)
                }
            })
            .collect::<Vec<_>>()
            .join(","),
        Value::Object(_) => "[object Object]".to_owned(),
    }
}

fn trim_js(value: &str) -> &str {
    value.trim_matches(|character: char| {
        matches!(
            character,
            '\u{0009}'
                | '\u{000A}'
                | '\u{000B}'
                | '\u{000C}'
                | '\u{000D}'
                | '\u{0020}'
                | '\u{00A0}'
                | '\u{1680}'
                | '\u{2000}'
                ..='\u{200A}'
                    | '\u{2028}'
                    | '\u{2029}'
                    | '\u{202F}'
                    | '\u{205F}'
                    | '\u{3000}'
                    | '\u{FEFF}'
        )
    })
}
