//! Session transcript indexing, paging, and restore projections.
//!
//! This ports the durable parts of `packages/core/src/services/session-transcript-reader.ts`.
//! JSONL recovery and record validation are delegated to the shared JSONL and
//! transcript modules. The reader keeps parent-chain order, fragment order,
//! and the distinction between runtime history and replay history.

use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::Sha256;
use thiserror::Error;

use crate::acp_bridge::session_artifacts::{
    RebuiltSessionArtifactSnapshot, rebuild_session_artifact_snapshot_with_warnings,
};
use crate::branch_points::resolve_branch_points;
use crate::jsonl::{JsonlParseDiagnostic, parse_line_with_diagnostic};
use crate::services::session_file_history_state::{
    SessionFileHistoryAccumulator, serialize_snapshot,
};
use crate::services::session_turn_state::{SessionTurnState, SessionTurnStateAccumulator};
use crate::session_api_history::{
    BuildApiHistoryOptions, SessionApiHistoryAccumulator, SessionApiHistoryError,
};
use crate::session_paths::{SessionArchiveState, SessionPaths};
use crate::session_resume_token_counts::{ResumeTokenCounts, ResumeTokenCountsAccumulator};
use crate::transcript::{
    DiagnosticSeverity, TranscriptProjectionDiagnostic, TranscriptRecord,
    aggregate_transcript_record_fragments, is_transcript_artifact_record,
    is_transcript_conversation_record, validate_transcript_record, walk_transcript_uuid_chain,
};

pub const SESSION_TRANSCRIPT_DEFAULT_LIMIT: usize = 100;
pub const SESSION_TRANSCRIPT_MAX_LIMIT: usize = 500;
pub const SESSION_TRANSCRIPT_CURSOR_VERSION: u8 = 1;
pub const SESSION_TRANSCRIPT_MAX_INDEX_BYTES: u64 = 256 * 1024 * 1024;
pub const SESSION_TRANSCRIPT_MAX_PAGE_BYTES: usize = 4 * 1024 * 1024;
const CURSOR_HMAC_KEY_BYTES: usize = 32;
const CURSOR_HMAC_KEY_FILENAME: &str = "session-transcript-cursor-key";
const MID_TURN_USER_SUBTYPES: &[&str] = &[
    "goal_runtime",
    "notification",
    "cron",
    "mid_turn_user_message",
];

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SessionTranscriptDirection {
    #[default]
    Forward,
    Backward,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionTranscriptFileIdentity {
    pub dev: u64,
    pub ino: u64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionTranscriptCursorState {
    pub v: u8,
    pub session_id: String,
    pub file_identity: SessionTranscriptFileIdentity,
    pub snapshot_size: u64,
    pub position: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub direction: Option<SessionTranscriptDirection>,
    pub leaf_uuid: String,
    pub start_time: String,
    pub last_updated: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replay: Option<Value>,
}

/// HMAC codec for workspace-scoped transcript cursors. Hosts may inject a
/// codec backed by their own key store; `SessionTranscriptReader` otherwise
/// creates the same private project-local key file as the TypeScript reader.
#[derive(Clone)]
pub struct SessionTranscriptCursorCodec {
    key: Vec<u8>,
}

impl SessionTranscriptCursorCodec {
    pub fn new(key: impl Into<Vec<u8>>) -> Result<Self, SessionTranscriptReaderError> {
        let key = key.into();
        if key.len() != CURSOR_HMAC_KEY_BYTES {
            return Err(SessionTranscriptReaderError::InvalidCursor);
        }
        Ok(Self { key })
    }

    pub fn encode(
        &self,
        state: &SessionTranscriptCursorState,
    ) -> Result<String, SessionTranscriptReaderError> {
        if state.v != SESSION_TRANSCRIPT_CURSOR_VERSION
            || state.direction == Some(SessionTranscriptDirection::Forward)
        {
            return Err(SessionTranscriptReaderError::InvalidCursor);
        }
        let payload =
            serde_json::to_vec(state).map_err(|_| SessionTranscriptReaderError::InvalidCursor)?;
        let mut mac = HmacSha256::new_from_slice(&self.key)
            .map_err(|_| SessionTranscriptReaderError::InvalidCursor)?;
        mac.update(&payload);
        let signature = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
        let mut envelope = serde_json::Map::new();
        let Value::Object(fields) =
            serde_json::to_value(state).map_err(|_| SessionTranscriptReaderError::InvalidCursor)?
        else {
            return Err(SessionTranscriptReaderError::InvalidCursor);
        };
        envelope.extend(fields);
        envelope.insert("mac".to_owned(), Value::String(signature));
        Ok(URL_SAFE_NO_PAD.encode(
            serde_json::to_vec(&Value::Object(envelope))
                .map_err(|_| SessionTranscriptReaderError::InvalidCursor)?,
        ))
    }

    pub fn decode(
        &self,
        encoded: &str,
    ) -> Result<SessionTranscriptCursorState, SessionTranscriptReaderError> {
        let decoded = URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| SessionTranscriptReaderError::InvalidCursor)?;
        let value: Value = serde_json::from_slice(&decoded)
            .map_err(|_| SessionTranscriptReaderError::InvalidCursor)?;
        let signature = value
            .get("mac")
            .and_then(Value::as_str)
            .ok_or(SessionTranscriptReaderError::InvalidCursor)?
            .to_owned();
        let state: SessionTranscriptCursorState = serde_json::from_value(value)
            .map_err(|_| SessionTranscriptReaderError::InvalidCursor)?;
        if state.v != SESSION_TRANSCRIPT_CURSOR_VERSION
            || state.position > SESSION_TRANSCRIPT_MAX_INDEX_BYTES as usize
            || state.direction == Some(SessionTranscriptDirection::Forward)
        {
            return Err(SessionTranscriptReaderError::InvalidCursor);
        }
        let signature = URL_SAFE_NO_PAD
            .decode(&signature)
            .map_err(|_| SessionTranscriptReaderError::InvalidCursor)?;
        let payload =
            serde_json::to_vec(&state).map_err(|_| SessionTranscriptReaderError::InvalidCursor)?;
        let mut mac = HmacSha256::new_from_slice(&self.key)
            .map_err(|_| SessionTranscriptReaderError::InvalidCursor)?;
        mac.update(&payload);
        mac.verify_slice(&signature)
            .map_err(|_| SessionTranscriptReaderError::InvalidCursor)?;
        Ok(state)
    }
}

#[derive(Debug, Error)]
pub enum SessionTranscriptReaderError {
    #[error("invalid transcript session id")]
    InvalidSessionId,
    #[error("transcript snapshot is unavailable for session {0}")]
    SnapshotUnavailable(String),
    #[error(
        "transcript snapshot for session {session_id} is too large ({snapshot_size} bytes, max {max_bytes} bytes)"
    )]
    TranscriptTooLarge {
        session_id: String,
        snapshot_size: u64,
        max_bytes: u64,
    },
    #[error("transcript page byte limit exceeded ({page_bytes} bytes, max {max_bytes} bytes)")]
    PageTooLarge { page_bytes: usize, max_bytes: usize },
    #[error("invalid transcript cursor")]
    InvalidCursor,
    #[error("invalid transcript limit")]
    InvalidLimit,
    #[error("transcript page byte limit must be a positive integer")]
    InvalidPageByteLimit,
    #[error("malformed compressed session history")]
    InvalidCompressedHistory,
    #[error("transcript I/O error: {0}")]
    Io(#[from] io::Error),
}

impl From<SessionApiHistoryError> for SessionTranscriptReaderError {
    fn from(value: SessionApiHistoryError) -> Self {
        match value {
            SessionApiHistoryError::InvalidCompressedHistory => Self::InvalidCompressedHistory,
        }
    }
}

#[derive(Clone, Debug)]
struct TranscriptFragment {
    record: TranscriptRecord,
    physical_bytes: usize,
}

#[derive(Clone, Debug)]
pub struct SessionTranscriptSnapshot {
    pub session_id: String,
    pub file_path: PathBuf,
    pub file_identity: SessionTranscriptFileIdentity,
    pub snapshot_size: u64,
    pub leaf_uuid: String,
    /// Full parent-linked chain used to rebuild runtime state.
    pub runtime_records: Vec<Value>,
    /// Replay chain after side-task boundary and inherited-record filtering.
    pub replay_records: Vec<Value>,
    /// Side artifacts selected from physical records around the active chain.
    pub artifact_records: Vec<Value>,
    pub gaps: Vec<crate::transcript::TranscriptReplayGap>,
    pub diagnostics: Vec<TranscriptProjectionDiagnostic>,
    pub start_time: String,
    pub restore_start_time: String,
    pub last_updated: String,
    record_bytes: HashMap<String, usize>,
}

impl SessionTranscriptSnapshot {
    fn bytes_for(&self, uuid: &str) -> usize {
        self.record_bytes.get(uuid).copied().unwrap_or_default()
    }
}

/// Reader rooted in one project/runtime pair. Construction captures paths so
/// later runtime-directory changes cannot move this reader's transcript set.
#[derive(Clone)]
pub struct SessionTranscriptReader {
    paths: SessionPaths,
    archive_state: SessionArchiveState,
    windows_paths: bool,
    cursor_codec: Option<SessionTranscriptCursorCodec>,
}

impl SessionTranscriptReader {
    pub fn new(runtime_base_dir: impl Into<PathBuf>, project_root: impl Into<PathBuf>) -> Self {
        Self {
            paths: SessionPaths::new(runtime_base_dir, project_root),
            archive_state: SessionArchiveState::Active,
            windows_paths: cfg!(windows),
            cursor_codec: None,
        }
    }

    pub fn with_archive_state(mut self, state: SessionArchiveState) -> Self {
        self.archive_state = state;
        self
    }

    pub fn with_windows_paths(mut self, windows_paths: bool) -> Self {
        self.windows_paths = windows_paths;
        self
    }

    pub fn with_cursor_codec(mut self, codec: SessionTranscriptCursorCodec) -> Self {
        self.cursor_codec = Some(codec);
        self
    }

    pub fn transcript_path(
        &self,
        session_id: &str,
    ) -> Result<PathBuf, SessionTranscriptReaderError> {
        self.paths
            .transcript_path(session_id, self.archive_state, self.windows_paths)
            .ok_or(SessionTranscriptReaderError::InvalidSessionId)
    }

    pub fn read_snapshot(
        &self,
        session_id: &str,
    ) -> Result<SessionTranscriptSnapshot, SessionTranscriptReaderError> {
        self.read_snapshot_at(session_id, None, None, None)
    }

    /// Sign a cursor after a host has advanced its replay-specific state.
    /// Paging callers normally use the cursor produced by `read_page`; replay
    /// projections use this method to bind their updated pending-tool and
    /// usage state to the same frozen transcript snapshot.
    pub fn encode_cursor_state(
        &self,
        state: &SessionTranscriptCursorState,
    ) -> Result<String, SessionTranscriptReaderError> {
        self.codec()?.encode(state)
    }

    /// Read a forward or backward page. Cursors are signed and retain the
    /// frozen byte prefix, so appends do not alter a previously issued page
    /// chain. `before_record_id` is exclusive and always selects backward.
    pub fn read_page(
        &self,
        session_id: &str,
        options: SessionTranscriptReadPageOptions<'_>,
    ) -> Result<SessionTranscriptRecordPage, SessionTranscriptReaderError> {
        let limit = normalize_limit(options.limit)?;
        let max_bytes = normalize_max_bytes(options.max_bytes)?;
        if options.cursor.is_some()
            && (options.before_record_id.is_some() || options.direction.is_some())
        {
            return Err(SessionTranscriptReaderError::InvalidCursor);
        }
        let cursor = match options.cursor {
            Some(encoded) => Some(self.codec()?.decode(encoded)?),
            None => None,
        };
        if cursor
            .as_ref()
            .is_some_and(|state| state.session_id != session_id)
        {
            return Err(SessionTranscriptReaderError::InvalidCursor);
        }
        let path = self.transcript_path(session_id)?;
        let cursor_ref = cursor.as_ref();
        let snapshot = self.read_snapshot_at(
            session_id,
            cursor_ref.map(|state| state.snapshot_size),
            cursor_ref.map(|state| &state.file_identity),
            cursor_ref.map(|state| state.last_updated.as_str()),
        )?;
        if cursor_ref.is_some_and(|state| state.leaf_uuid != snapshot.leaf_uuid) {
            return Err(SessionTranscriptReaderError::SnapshotUnavailable(
                session_id.to_owned(),
            ));
        }

        let direction = cursor_ref
            .and_then(|state| state.direction)
            .or(options.direction)
            .unwrap_or_else(|| {
                if options.before_record_id.is_some() {
                    SessionTranscriptDirection::Backward
                } else {
                    SessionTranscriptDirection::Forward
                }
            });
        let replay_len = snapshot.replay_records.len();
        let mut position = cursor_ref.map_or_else(
            || match direction {
                SessionTranscriptDirection::Forward => 0,
                SessionTranscriptDirection::Backward => replay_len,
            },
            |state| state.position,
        );
        if let Some(before) = options.before_record_id {
            if before.is_empty() {
                return Err(SessionTranscriptReaderError::InvalidCursor);
            }
            position = snapshot
                .replay_records
                .iter()
                .position(|record| record.get("uuid").and_then(Value::as_str) == Some(before))
                .ok_or(SessionTranscriptReaderError::InvalidCursor)?;
        }
        if position > replay_len {
            return Err(SessionTranscriptReaderError::InvalidCursor);
        }

        let (indexes, next_position) = match direction {
            SessionTranscriptDirection::Forward => {
                let selected = select_forward_indexes(&snapshot, position, limit, max_bytes);
                let next = position + selected.len();
                (selected, next)
            }
            SessionTranscriptDirection::Backward => {
                select_backward_indexes(&snapshot, position, limit, max_bytes)
            }
        };
        let records = indexes
            .into_iter()
            .filter_map(|index| snapshot.replay_records.get(index).cloned())
            .collect::<Vec<_>>();
        let page_uuids = records
            .iter()
            .filter_map(|record| record.get("uuid").and_then(Value::as_str))
            .collect::<HashSet<_>>();
        let branch_points_by_assistant_uuid = resolve_branch_points(&snapshot.replay_records)
            .into_values()
            .filter(|point| page_uuids.contains(point.assistant_record_uuid.as_str()))
            .map(|point| (point.assistant_record_uuid, point.checkpoint_uuid))
            .collect::<IndexMap<_, _>>();
        let branch_points_by_assistant_uuid = (!branch_points_by_assistant_uuid.is_empty())
            .then_some(branch_points_by_assistant_uuid);
        let has_more = match direction {
            SessionTranscriptDirection::Forward => next_position < replay_len,
            SessionTranscriptDirection::Backward => next_position > 0,
        };
        let next_cursor_state = has_more.then(|| SessionTranscriptCursorState {
            v: SESSION_TRANSCRIPT_CURSOR_VERSION,
            session_id: session_id.to_owned(),
            file_identity: snapshot.file_identity.clone(),
            snapshot_size: snapshot.snapshot_size,
            position: next_position,
            direction: (direction == SessionTranscriptDirection::Backward)
                .then_some(SessionTranscriptDirection::Backward),
            leaf_uuid: snapshot.leaf_uuid.clone(),
            start_time: snapshot.start_time.clone(),
            last_updated: snapshot.last_updated.clone(),
            replay: cursor_ref.and_then(|state| state.replay.clone()),
        });
        let next_cursor = next_cursor_state
            .as_ref()
            .map(|state| self.codec().and_then(|codec| codec.encode(state)))
            .transpose()?;

        Ok(SessionTranscriptRecordPage {
            session_id: session_id.to_owned(),
            file_path: path,
            records,
            gaps: snapshot.gaps,
            has_more,
            direction,
            replay: cursor_ref.and_then(|state| state.replay.clone()),
            next_cursor_state,
            next_cursor,
            start_time: snapshot.start_time,
            last_updated: snapshot.last_updated,
            branch_points_by_assistant_uuid,
        })
    }

    /// Rebuild the deterministic runtime state needed by a cold session
    /// restore. Compression records replace earlier API history through the
    /// existing accumulator; turn counters and file snapshots use their
    /// existing source-parity helpers.
    pub fn read_restore_projection(
        &self,
        session_id: &str,
        replay: SessionRestoreReplaySelection,
    ) -> Result<SessionRestoreProjection, SessionTranscriptReaderError> {
        let snapshot = self.read_snapshot(session_id)?;
        let mut api_history = SessionApiHistoryAccumulator::default();
        let mut token_counts = ResumeTokenCountsAccumulator::default();
        let mut turn_state = SessionTurnStateAccumulator::new(session_id);
        let mut file_history = SessionFileHistoryAccumulator::new();
        let mut ui_telemetry = Vec::new();
        let mut attribution_snapshot = None;
        let mut parent_session_id = None;
        let mut source_type = None;
        let mut source_id = None;
        let mut custom_title = None;
        let mut title_source = None;
        let mut goal_records = Vec::new();

        for record in &snapshot.runtime_records {
            api_history.add(record.clone());
            token_counts.add(record);
            turn_state.add(record);
            // The TypeScript loader treats malformed individual file-history
            // checkpoints as best-effort and continues with later records.
            let _ = file_history.add(record);
            match record.get("subtype").and_then(Value::as_str) {
                Some("ui_telemetry") => {
                    if let Some(event) = record
                        .get("systemPayload")
                        .and_then(|payload| payload.get("uiEvent"))
                        .filter(|event| js_truthy(event))
                    {
                        ui_telemetry.push(event.clone());
                    }
                }
                Some("attribution_snapshot") => {
                    let snapshot = record
                        .get("systemPayload")
                        .and_then(|payload| payload.get("snapshot"))
                        .filter(|value| value.is_object());
                    if let Some(snapshot) = snapshot {
                        attribution_snapshot = Some(snapshot.clone());
                    }
                }
                Some("parent_session") => {
                    parent_session_id = record
                        .get("systemPayload")
                        .and_then(|payload| payload.get("parentSessionId"))
                        .and_then(Value::as_str)
                        .map(str::to_owned);
                }
                Some("session_source") => {
                    let payload = record.get("systemPayload");
                    source_type = payload
                        .and_then(|payload| payload.get("sourceType"))
                        .and_then(Value::as_str)
                        .map(str::to_owned);
                    source_id = payload
                        .and_then(|payload| payload.get("sourceId"))
                        .and_then(Value::as_str)
                        .map(str::to_owned);
                }
                Some("custom_title") => {
                    let payload = record.get("systemPayload");
                    custom_title = payload
                        .and_then(|payload| payload.get("customTitle"))
                        .and_then(Value::as_str)
                        .map(str::to_owned);
                    title_source = payload
                        .and_then(|payload| payload.get("titleSource"))
                        .and_then(Value::as_str)
                        .map(str::to_owned);
                }
                Some("goal_state") => goal_records.push(record.clone()),
                Some("slash_command") if has_goal_status(record) => {
                    goal_records.push(record.clone());
                }
                _ => {}
            }
        }

        let runtime = SessionRuntimeResumeState {
            last_completed_uuid: snapshot.leaf_uuid.clone(),
            api_history: api_history.finish(BuildApiHistoryOptions::default())?,
            resume_token_counts: token_counts.finish(),
            turn_state: turn_state.finish(),
            file_history_snapshots: file_history
                .finish()
                .map(|snapshots| snapshots.iter().map(serialize_snapshot).collect()),
            artifact_snapshot: rebuild_session_artifact_snapshot_with_warnings(
                &snapshot.artifact_records,
                Some(session_id),
            ),
            ui_telemetry_events: ui_telemetry,
            attribution_snapshot,
            parent_session_id,
            source_type,
            source_id,
            custom_title,
            title_source,
            goal_records,
        };
        let replay_page = select_restore_replay(&snapshot, replay)?;
        self.assert_snapshot_unchanged(&snapshot)?;
        Ok(SessionRestoreProjection {
            session_id: session_id.to_owned(),
            file_path: snapshot.file_path.clone(),
            start_time: snapshot.restore_start_time.clone(),
            last_updated: snapshot.last_updated.clone(),
            runtime,
            replay: replay_page,
            artifact_records: snapshot.artifact_records,
            gaps: snapshot.gaps,
            diagnostics: snapshot.diagnostics,
        })
    }

    fn codec(&self) -> Result<SessionTranscriptCursorCodec, SessionTranscriptReaderError> {
        if let Some(codec) = &self.cursor_codec {
            return Ok(codec.clone());
        }
        let key_path = self
            .paths
            .project_directory(self.windows_paths)
            .join(CURSOR_HMAC_KEY_FILENAME);
        let key = load_or_create_cursor_key(&key_path)?;
        SessionTranscriptCursorCodec::new(key)
    }

    fn assert_snapshot_unchanged(
        &self,
        snapshot: &SessionTranscriptSnapshot,
    ) -> Result<(), SessionTranscriptReaderError> {
        let metadata = fs::metadata(&snapshot.file_path).map_err(|_| {
            SessionTranscriptReaderError::SnapshotUnavailable(snapshot.session_id.clone())
        })?;
        if metadata.len() != snapshot.snapshot_size
            || file_identity(&metadata) != snapshot.file_identity
            || timestamp(&metadata) != snapshot.last_updated
        {
            return Err(SessionTranscriptReaderError::SnapshotUnavailable(
                snapshot.session_id.clone(),
            ));
        }
        Ok(())
    }

    fn read_snapshot_at(
        &self,
        session_id: &str,
        frozen_size: Option<u64>,
        frozen_identity: Option<&SessionTranscriptFileIdentity>,
        frozen_updated: Option<&str>,
    ) -> Result<SessionTranscriptSnapshot, SessionTranscriptReaderError> {
        let path = self.transcript_path(session_id)?;
        let before = fs::metadata(&path).map_err(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                SessionTranscriptReaderError::SnapshotUnavailable(session_id.to_owned())
            } else {
                SessionTranscriptReaderError::Io(error)
            }
        })?;
        let identity = file_identity(&before);
        let snapshot_size = frozen_size.unwrap_or(before.len());
        if let Some(expected) = frozen_identity {
            if identity != *expected || before.len() < snapshot_size {
                return Err(SessionTranscriptReaderError::SnapshotUnavailable(
                    session_id.to_owned(),
                ));
            }
        }
        if snapshot_size > SESSION_TRANSCRIPT_MAX_INDEX_BYTES {
            return Err(SessionTranscriptReaderError::TranscriptTooLarge {
                session_id: session_id.to_owned(),
                snapshot_size,
                max_bytes: SESSION_TRANSCRIPT_MAX_INDEX_BYTES,
            });
        }
        if snapshot_size == 0 {
            return Err(SessionTranscriptReaderError::SnapshotUnavailable(
                session_id.to_owned(),
            ));
        }
        let before_updated = timestamp(&before);
        let mut file = File::open(&path).map_err(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                SessionTranscriptReaderError::SnapshotUnavailable(session_id.to_owned())
            } else {
                SessionTranscriptReaderError::Io(error)
            }
        })?;
        let mut bytes = vec![0; snapshot_size as usize];
        file.read_exact(&mut bytes).map_err(|error| {
            if error.kind() == io::ErrorKind::UnexpectedEof {
                SessionTranscriptReaderError::SnapshotUnavailable(session_id.to_owned())
            } else {
                SessionTranscriptReaderError::Io(error)
            }
        })?;
        let frozen = bytes.as_slice();
        let mut fragments = Vec::new();
        let mut diagnostics = Vec::new();
        let mut first_timestamp = None;
        let mut first_record_seen = false;

        for (line_index, raw_line) in frozen.split(|byte| *byte == b'\n').enumerate() {
            let line = raw_line.strip_suffix(b"\r").unwrap_or(raw_line);
            let text = String::from_utf8_lossy(line);
            let text = text.trim();
            if text.is_empty() {
                continue;
            }
            let parsed = parse_line_with_diagnostic(text);
            match parsed.diagnostic {
                Some(JsonlParseDiagnostic::MalformedLine) => diagnostics.push(diagnostic(
                    "malformed_jsonl_line",
                    "Skipped a malformed JSONL transcript line.",
                    true,
                    Some(line_index),
                    None,
                    Some(&path),
                )),
                Some(JsonlParseDiagnostic::RecoveredObjects { count }) => {
                    diagnostics.push(diagnostic(
                        "recovered_jsonl_line",
                        &format!("Recovered {count} JSON object(s) from a damaged or glued line."),
                        false,
                        Some(line_index),
                        None,
                        Some(&path),
                    ))
                }
                Some(JsonlParseDiagnostic::NonObjectValue) | None => {}
            }
            for raw in parsed.records {
                let record_index = fragments.len();
                let (record, record_diagnostics) =
                    validate_transcript_record(&raw, Some(record_index));
                diagnostics.extend(record_diagnostics);
                let Some(record) = record else { continue };
                if record.session_id != session_id {
                    return Err(SessionTranscriptReaderError::SnapshotUnavailable(
                        session_id.to_owned(),
                    ));
                }
                if !first_record_seen {
                    first_timestamp = record.timestamp.clone();
                    first_record_seen = true;
                }
                fragments.push(TranscriptFragment {
                    record,
                    physical_bytes: line.len(),
                });
            }
        }

        let after = fs::metadata(&path).map_err(|_| {
            SessionTranscriptReaderError::SnapshotUnavailable(session_id.to_owned())
        })?;
        if file_identity(&after) != identity
            || after.len() < snapshot_size
            || (frozen_size.is_none()
                && after.len() == before.len()
                && timestamp(&after) != before_updated)
        {
            return Err(SessionTranscriptReaderError::SnapshotUnavailable(
                session_id.to_owned(),
            ));
        }
        if fragments.is_empty() {
            return Err(SessionTranscriptReaderError::SnapshotUnavailable(
                session_id.to_owned(),
            ));
        }

        let mut all_fragments: HashMap<String, Vec<TranscriptRecord>> = HashMap::new();
        let mut first_by_uuid: HashMap<String, TranscriptRecord> = HashMap::new();
        let mut raw_physical = Vec::with_capacity(fragments.len());
        let mut record_bytes = HashMap::<String, usize>::new();
        let mut leaf_uuid = None;
        let mut active_start_time = None;
        for fragment in &fragments {
            let record = &fragment.record;
            raw_physical.push(record.clone());
            all_fragments
                .entry(record.uuid.clone())
                .or_default()
                .push(record.clone());
            record_bytes
                .entry(record.uuid.clone())
                .and_modify(|bytes| *bytes = bytes.saturating_add(fragment.physical_bytes))
                .or_insert(fragment.physical_bytes);
            if is_transcript_conversation_record(record) {
                leaf_uuid = Some(record.uuid.clone());
                if active_start_time.is_none() {
                    active_start_time = record.timestamp.clone();
                }
                first_by_uuid
                    .entry(record.uuid.clone())
                    .or_insert_with(|| record.clone());
            }
        }
        let leaf_uuid = leaf_uuid.ok_or_else(|| {
            SessionTranscriptReaderError::SnapshotUnavailable(session_id.to_owned())
        })?;
        for fragment in &fragments {
            if !is_transcript_conversation_record(&fragment.record) {
                continue;
            }
            let first = &first_by_uuid[&fragment.record.uuid];
            if first.parent_uuid != fragment.record.parent_uuid {
                diagnostics.push(diagnostic(
                    "conflicting_parent_uuid",
                    "Duplicate transcript fragments disagree on parentUuid.",
                    true,
                    None,
                    Some(&fragment.record.uuid),
                    Some(&path),
                ));
            }
        }
        let chain = walk_transcript_uuid_chain(&leaf_uuid, |uuid| first_by_uuid.get(uuid));
        for gap in &chain.gaps {
            diagnostics.push(diagnostic(
                "history_gap",
                "The active transcript chain is missing a parent record.",
                true,
                None,
                Some(&gap.child_uuid),
                Some(&path),
            ));
        }
        if let Some(cycle) = chain.cycle_uuid.as_deref() {
            diagnostics.push(diagnostic(
                "parent_cycle",
                "The active transcript chain contains a parent cycle.",
                true,
                None,
                Some(cycle),
                Some(&path),
            ));
        }

        let mut records_by_uuid = HashMap::new();
        for (uuid, group) in &all_fragments {
            if let Ok(record) = aggregate_transcript_record_fragments(group) {
                if let Ok(value) = serde_json::to_value(record) {
                    records_by_uuid.insert(uuid.clone(), value);
                }
            }
        }
        let runtime_records = chain
            .uuids
            .iter()
            .filter_map(|uuid| records_by_uuid.get(uuid).cloned())
            .collect::<Vec<_>>();
        let source_boundary = runtime_records.iter().position(|record| {
            record.get("type").and_then(Value::as_str) == Some("system")
                && record.get("subtype").and_then(Value::as_str) == Some("session_source")
                && record
                    .get("systemPayload")
                    .and_then(|payload| payload.get("sourceType"))
                    .and_then(Value::as_str)
                    == Some("side_task")
        });
        let replay_base = source_boundary.map_or(runtime_records.as_slice(), |index| {
            &runtime_records[index..]
        });
        let replay_records = replay_base
            .iter()
            .filter(|record| record.get("forkedFrom").is_none())
            .cloned()
            .collect::<Vec<_>>();
        let artifact_ids = select_artifact_uuids(&raw_physical, &runtime_records);
        let mut seen_artifacts = HashSet::<String>::new();
        let artifact_records = artifact_ids
            .iter()
            .filter(|uuid| seen_artifacts.insert(uuid.as_str().to_owned()))
            .filter_map(|uuid| records_by_uuid.get(uuid).cloned())
            .collect();

        let last_updated = frozen_updated
            .map(str::to_owned)
            .unwrap_or(timestamp(&after));
        let start_time = active_start_time
            .as_deref()
            .unwrap_or(&last_updated)
            .to_owned();
        let restore_start_time = first_timestamp.as_deref().unwrap_or(&start_time).to_owned();
        Ok(SessionTranscriptSnapshot {
            session_id: session_id.to_owned(),
            file_path: path,
            file_identity: identity,
            snapshot_size,
            leaf_uuid,
            runtime_records,
            replay_records,
            artifact_records,
            gaps: chain.gaps,
            diagnostics,
            start_time,
            restore_start_time,
            last_updated,
            record_bytes,
        })
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SessionTranscriptReadPageOptions<'a> {
    pub cursor: Option<&'a str>,
    pub before_record_id: Option<&'a str>,
    pub direction: Option<SessionTranscriptDirection>,
    pub limit: Option<usize>,
    pub max_bytes: Option<usize>,
}

#[derive(Clone, Debug)]
pub struct SessionTranscriptRecordPage {
    pub session_id: String,
    pub file_path: PathBuf,
    pub records: Vec<Value>,
    pub gaps: Vec<crate::transcript::TranscriptReplayGap>,
    pub has_more: bool,
    pub direction: SessionTranscriptDirection,
    /// Replay state from the incoming cursor. Kept even when this is the last
    /// page and no continuation cursor is produced.
    pub replay: Option<Value>,
    pub next_cursor_state: Option<SessionTranscriptCursorState>,
    pub next_cursor: Option<String>,
    pub start_time: String,
    pub last_updated: String,
    /// Maps assistant records on this page to their validated durable branch
    /// checkpoint UUIDs, matching the TypeScript page projection.
    pub branch_points_by_assistant_uuid: Option<IndexMap<String, String>>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SessionRestoreReplaySelection {
    None,
    All {
        hide_inherited_history: bool,
    },
    Recent {
        limit: usize,
        hide_inherited_history: bool,
    },
}

#[derive(Clone, Debug)]
pub struct SessionRuntimeResumeState {
    pub last_completed_uuid: String,
    pub api_history: Vec<Value>,
    pub resume_token_counts: Option<ResumeTokenCounts>,
    pub turn_state: SessionTurnState,
    pub file_history_snapshots: Option<Vec<Value>>,
    pub artifact_snapshot: Option<RebuiltSessionArtifactSnapshot>,
    pub ui_telemetry_events: Vec<Value>,
    pub attribution_snapshot: Option<Value>,
    pub parent_session_id: Option<String>,
    pub source_type: Option<String>,
    pub source_id: Option<String>,
    pub custom_title: Option<String>,
    pub title_source: Option<String>,
    pub goal_records: Vec<Value>,
}

#[derive(Clone, Debug)]
pub struct SessionRestoreReplayPage {
    pub records: Vec<Value>,
    pub gaps: Vec<crate::transcript::TranscriptReplayGap>,
    pub has_more: bool,
    pub anchor_record_id: Option<String>,
}

#[derive(Clone, Debug)]
pub struct SessionRestoreProjection {
    pub session_id: String,
    pub file_path: PathBuf,
    pub start_time: String,
    pub last_updated: String,
    pub runtime: SessionRuntimeResumeState,
    pub replay: Option<SessionRestoreReplayPage>,
    pub artifact_records: Vec<Value>,
    pub gaps: Vec<crate::transcript::TranscriptReplayGap>,
    pub diagnostics: Vec<TranscriptProjectionDiagnostic>,
}

fn select_restore_replay(
    snapshot: &SessionTranscriptSnapshot,
    selection: SessionRestoreReplaySelection,
) -> Result<Option<SessionRestoreReplayPage>, SessionTranscriptReaderError> {
    let (records, has_more) = match selection {
        SessionRestoreReplaySelection::None => return Ok(None),
        SessionRestoreReplaySelection::All {
            hide_inherited_history,
        } => {
            let records = replay_records(snapshot, hide_inherited_history);
            (records, false)
        }
        SessionRestoreReplaySelection::Recent {
            limit,
            hide_inherited_history,
        } => {
            let limit = normalize_limit(Some(limit))?;
            let base = replay_records(snapshot, hide_inherited_history);
            let (indexes, start) = select_backward_for_records(
                snapshot,
                &base,
                base.len(),
                limit,
                Some(SESSION_TRANSCRIPT_MAX_PAGE_BYTES),
            );
            let records = indexes
                .into_iter()
                .filter_map(|index| base.get(index).cloned())
                .collect();
            (records, start > 0)
        }
    };
    let anchor = has_more
        .then(|| records.first())
        .flatten()
        .and_then(|record| record.get("uuid"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    Ok(Some(SessionRestoreReplayPage {
        records,
        gaps: snapshot.gaps.clone(),
        has_more,
        anchor_record_id: anchor,
    }))
}

fn replay_records(snapshot: &SessionTranscriptSnapshot, hide_inherited: bool) -> Vec<Value> {
    snapshot
        .replay_records
        .iter()
        .filter(|record| !hide_inherited || record.get("forkedFrom").is_none())
        .cloned()
        .collect()
}

fn select_forward_indexes(
    snapshot: &SessionTranscriptSnapshot,
    position: usize,
    limit: usize,
    max_bytes: Option<usize>,
) -> Vec<usize> {
    let end = snapshot
        .replay_records
        .len()
        .min(position.saturating_add(limit));
    let mut selected = Vec::new();
    let mut bytes = 0usize;
    for index in position..end {
        let uuid = snapshot.replay_records[index]
            .get("uuid")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let next_bytes = snapshot.bytes_for(uuid);
        if !selected.is_empty()
            && max_bytes.is_some_and(|budget| bytes.saturating_add(next_bytes) > budget)
        {
            break;
        }
        selected.push(index);
        bytes = bytes.saturating_add(next_bytes);
    }
    selected
}

fn select_backward_indexes(
    snapshot: &SessionTranscriptSnapshot,
    position: usize,
    limit: usize,
    max_bytes: Option<usize>,
) -> (Vec<usize>, usize) {
    select_backward_for_records(
        snapshot,
        &snapshot.replay_records,
        position,
        limit,
        max_bytes,
    )
}

fn select_backward_for_records(
    snapshot: &SessionTranscriptSnapshot,
    records: &[Value],
    position: usize,
    limit: usize,
    max_bytes: Option<usize>,
) -> (Vec<usize>, usize) {
    if position == 0 {
        return (Vec::new(), 0);
    }
    let mut start = position.saturating_sub(limit);
    if let Some(boundary) = (start..position).find(|index| is_replay_turn_start(&records[*index])) {
        start = boundary;
    }
    let expansion_floor = position.saturating_sub(limit.saturating_mul(2));
    if let Some(boundary) = (expansion_floor..=start)
        .rev()
        .find(|index| is_replay_turn_start(&records[*index]))
    {
        start = boundary;
    }
    let mut selected_start = position;
    let mut bytes = 0usize;
    for index in (start..position).rev() {
        let uuid = records[index]
            .get("uuid")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let size = snapshot.bytes_for(uuid);
        if selected_start < position
            && max_bytes.is_some_and(|budget| bytes.saturating_add(size) > budget)
        {
            break;
        }
        selected_start = index;
        bytes = bytes.saturating_add(size);
    }
    // Align to a user turn start when a bounded boundary is available and the
    // expansion stays within one additional page budget.
    let expansion_budget = max_bytes.map_or(SESSION_TRANSCRIPT_MAX_PAGE_BYTES * 4, |budget| {
        budget
            .saturating_mul(2)
            .min(SESSION_TRANSCRIPT_MAX_PAGE_BYTES * 4)
    });
    let aligned_boundary =
        (selected_start..position).find(|index| is_replay_turn_start(&records[*index]));
    if let Some(boundary) = aligned_boundary {
        selected_start = boundary;
        if selected_start > 0 {
            let has_previous_boundary = (0..selected_start)
                .rev()
                .any(|index| is_replay_turn_start(&records[index]));
            if !has_previous_boundary
                && expansion_floor == 0
                && bytes_between(snapshot, records, 0, position) <= expansion_budget
            {
                selected_start = 0;
            }
        }
    } else if let Some(boundary) = (expansion_floor..selected_start)
        .rev()
        .find(|index| is_replay_turn_start(&records[*index]))
    {
        if bytes_between(snapshot, records, boundary, position) <= expansion_budget {
            selected_start = boundary;
        }
    }
    if selection_orphans_tool_result(records, selected_start, position) && selected_start > 0 {
        let floor = selected_start.saturating_sub(limit);
        if let Some(owner) = (floor..selected_start)
            .rev()
            .find(|index| is_replay_page_start(&records[*index]))
        {
            let added = (owner + 1..selected_start)
                .map(|index| {
                    records[index]
                        .get("uuid")
                        .and_then(Value::as_str)
                        .map(|uuid| snapshot.bytes_for(uuid))
                        .unwrap_or_default()
                })
                .sum::<usize>();
            if added <= expansion_budget {
                selected_start = owner;
            }
        }
    }
    ((selected_start..position).collect(), selected_start)
}

fn bytes_between(
    snapshot: &SessionTranscriptSnapshot,
    records: &[Value],
    start: usize,
    end: usize,
) -> usize {
    (start..end)
        .map(|index| {
            records[index]
                .get("uuid")
                .and_then(Value::as_str)
                .map(|uuid| snapshot.bytes_for(uuid))
                .unwrap_or_default()
        })
        .sum()
}

fn selection_orphans_tool_result(records: &[Value], start: usize, end: usize) -> bool {
    (start..end).any(|index| {
        if records[index].get("type").and_then(Value::as_str) != Some("tool_result") {
            return false;
        }
        !(start..index)
            .rev()
            .any(|owner| is_replay_page_start(&records[owner]))
    })
}

fn is_replay_turn_start(record: &Value) -> bool {
    record.get("type").and_then(Value::as_str) == Some("user")
        && record
            .get("subtype")
            .and_then(Value::as_str)
            .is_none_or(|subtype| !MID_TURN_USER_SUBTYPES.contains(&subtype))
}

fn is_replay_page_start(record: &Value) -> bool {
    record.get("subtype").and_then(Value::as_str) != Some("realtime_message")
        && (record.get("type").and_then(Value::as_str) == Some("assistant")
            || is_replay_turn_start(record))
}

fn select_artifact_uuids(records: &[TranscriptRecord], active_records: &[Value]) -> Vec<String> {
    let active: HashSet<String> = active_records
        .iter()
        .filter_map(|record| {
            record
                .get("uuid")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .collect();
    let first_active_index = active_records
        .first()
        .and_then(|first| first.get("uuid"))
        .and_then(Value::as_str)
        .and_then(|uuid| records.iter().position(|record| record.uuid == uuid));
    let mut next_active_at = HashMap::<usize, String>::new();
    let mut next_blocking_at = HashSet::<usize>::new();
    let mut next_active: Option<String> = None;
    let mut next_blocking: Option<String> = None;
    for index in (0..records.len()).rev() {
        if let Some(uuid) = &next_active {
            next_active_at.insert(index, uuid.clone());
        }
        if next_blocking.is_some() {
            next_blocking_at.insert(index);
        }
        let record = &records[index];
        if active.contains(&record.uuid) {
            next_active = Some(record.uuid.clone());
            next_blocking = None;
        } else if !is_transcript_artifact_record(record)
            && !(record.record_type == crate::transcript::TranscriptRecordType::System
                && record.subtype.as_deref() == Some("custom_title"))
        {
            next_blocking = Some(record.uuid.clone());
        }
    }
    let mut selected = Vec::new();
    let mut included_side_artifacts = HashSet::new();
    let mut previous_active: Option<String> = None;
    for (index, record) in records.iter().enumerate() {
        if active.contains(&record.uuid) {
            previous_active = Some(record.uuid.clone());
            continue;
        }
        if !is_transcript_artifact_record(record) {
            continue;
        }
        let next_uuid = next_active_at.get(&index);
        let in_active_segment = !next_blocking_at.contains(&index)
            && (next_uuid.is_some() || previous_active.is_some());
        let parent = record.parent_uuid.as_deref();
        if parent.is_some_and(|parent| {
            (active.contains(parent) || included_side_artifacts.contains(parent))
                && in_active_segment
                && (previous_active.as_deref() == Some(parent)
                    || included_side_artifacts.contains(parent))
        }) || (parent.is_none()
            && first_active_index.is_some_and(|first| index < first)
            && in_active_segment)
        {
            selected.push(record.uuid.clone());
            included_side_artifacts.insert(record.uuid.clone());
        }
    }
    selected
}

/// Select the artifact side records belonging to the active transcript
/// lineage. Artifact records are excluded from `PreparedTranscriptRecords`,
/// so resume callers use this projection before rebuilding the live store.
pub fn select_active_session_artifact_records(
    raw_records: &[Value],
    active_records: &[TranscriptRecord],
) -> Vec<Value> {
    let physical = raw_records
        .iter()
        .filter_map(|value| validate_transcript_record(value, None).0)
        .collect::<Vec<_>>();
    let active_values = active_records
        .iter()
        .filter_map(|record| serde_json::to_value(record).ok())
        .collect::<Vec<_>>();
    let selected_ids = select_artifact_uuids(&physical, &active_values);
    let selected = selected_ids.iter().cloned().collect::<HashSet<_>>();
    let mut groups = HashMap::<String, Vec<TranscriptRecord>>::new();
    for record in physical {
        if selected.contains(&record.uuid) {
            groups.entry(record.uuid.clone()).or_default().push(record);
        }
    }
    selected_ids
        .into_iter()
        .filter_map(|uuid| groups.get(&uuid))
        .filter_map(|fragments| aggregate_transcript_record_fragments(fragments).ok())
        .filter_map(|record| serde_json::to_value(record).ok())
        .collect()
}

fn has_goal_status(record: &Value) -> bool {
    record
        .get("systemPayload")
        .and_then(|payload| payload.get("phase"))
        .and_then(Value::as_str)
        == Some("result")
        && record
            .get("systemPayload")
            .and_then(|payload| payload.get("outputHistoryItems"))
            .and_then(Value::as_array)
            .is_some_and(|items| {
                items
                    .iter()
                    .any(|item| item.get("type").and_then(Value::as_str) == Some("goal_status"))
            })
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

fn normalize_limit(limit: Option<usize>) -> Result<usize, SessionTranscriptReaderError> {
    let limit = limit.unwrap_or(SESSION_TRANSCRIPT_DEFAULT_LIMIT);
    if !(1..=SESSION_TRANSCRIPT_MAX_LIMIT).contains(&limit) {
        return Err(SessionTranscriptReaderError::InvalidLimit);
    }
    Ok(limit)
}

fn normalize_max_bytes(
    max_bytes: Option<usize>,
) -> Result<Option<usize>, SessionTranscriptReaderError> {
    if max_bytes == Some(0) {
        return Err(SessionTranscriptReaderError::InvalidPageByteLimit);
    }
    Ok(max_bytes)
}

fn diagnostic(
    code: &str,
    message: &str,
    affects_completeness: bool,
    record_index: Option<usize>,
    record_id: Option<&str>,
    path: Option<&Path>,
) -> TranscriptProjectionDiagnostic {
    TranscriptProjectionDiagnostic {
        code: code.to_owned(),
        severity: if affects_completeness {
            DiagnosticSeverity::Warning
        } else {
            DiagnosticSeverity::Info
        },
        message: message.to_owned(),
        affects_completeness,
        record_index,
        record_id: record_id.map(str::to_owned),
        path: path.map(|path| path.to_string_lossy().into_owned()),
    }
}

fn file_identity(metadata: &fs::Metadata) -> SessionTranscriptFileIdentity {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        SessionTranscriptFileIdentity {
            dev: metadata.dev(),
            ino: metadata.ino(),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        SessionTranscriptFileIdentity { dev: 0, ino: 0 }
    }
}

fn timestamp(metadata: &fs::Metadata) -> String {
    let modified = metadata.modified().unwrap_or(UNIX_EPOCH);
    let duration = modified.duration_since(UNIX_EPOCH).unwrap_or_default();
    DateTime::<Utc>::from_timestamp(duration.as_secs() as i64, duration.subsec_nanos())
        .unwrap_or_else(|| DateTime::<Utc>::from_timestamp(0, 0).expect("Unix epoch is valid"))
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn load_or_create_cursor_key(path: &Path) -> Result<Vec<u8>, SessionTranscriptReaderError> {
    if let Ok(encoded) = fs::read_to_string(path) {
        if let Ok(key) = URL_SAFE_NO_PAD.decode(encoded.trim()) {
            if key.len() == CURSOR_HMAC_KEY_BYTES {
                return Ok(key);
            }
        }
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    // UUID v4 obtains cryptographic randomness from the platform provider.
    let mut key = Vec::with_capacity(CURSOR_HMAC_KEY_BYTES);
    key.extend_from_slice(uuid::Uuid::new_v4().as_bytes());
    key.extend_from_slice(uuid::Uuid::new_v4().as_bytes());
    let encoded = format!("{}\n", URL_SAFE_NO_PAD.encode(&key));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    match options.open(path) {
        Ok(mut file) => {
            file.write_all(encoded.as_bytes())?;
            file.sync_all()?;
            Ok(key)
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            if let Ok(encoded) = fs::read_to_string(path) {
                if let Ok(existing) = URL_SAFE_NO_PAD.decode(encoded.trim()) {
                    if existing.len() == CURSOR_HMAC_KEY_BYTES {
                        return Ok(existing);
                    }
                }
            }
            let mut file = OpenOptions::new().write(true).truncate(true).open(path)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
            }
            file.write_all(encoded.as_bytes())?;
            file.sync_all()?;
            Ok(key)
        }
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn record(uuid: &str, parent: Value, record_type: &str, text: &str) -> Value {
        json!({
            "uuid": uuid,
            "parentUuid": parent,
            "sessionId": "550e8400-e29b-41d4-a716-446655440000",
            "timestamp": "2026-01-01T00:00:00.000Z",
            "type": record_type,
            "message": {"role": if record_type == "assistant" {"model"} else {"user"}, "parts": [{"text": text}]}
        })
    }

    #[test]
    fn turn_start_and_realtime_rules_match_transcript_replay() {
        assert!(is_replay_turn_start(&json!({"type":"user"})));
        assert!(is_replay_turn_start(
            &json!({"type":"user","subtype":"realtime_message"})
        ));
        for subtype in [
            "goal_runtime",
            "notification",
            "cron",
            "mid_turn_user_message",
        ] {
            assert!(!is_replay_turn_start(
                &json!({"type":"user","subtype":subtype})
            ));
        }
        assert!(!is_replay_page_start(
            &json!({"type":"user","subtype":"realtime_message"})
        ));
    }

    #[test]
    fn cursor_round_trip_is_authenticated_and_tampering_fails() {
        let codec = SessionTranscriptCursorCodec::new(vec![7; 32]).unwrap();
        let state = SessionTranscriptCursorState {
            v: SESSION_TRANSCRIPT_CURSOR_VERSION,
            session_id: "session".to_owned(),
            file_identity: SessionTranscriptFileIdentity { dev: 1, ino: 2 },
            snapshot_size: 40,
            position: 3,
            direction: Some(SessionTranscriptDirection::Backward),
            leaf_uuid: "leaf".to_owned(),
            start_time: "start".to_owned(),
            last_updated: "updated".to_owned(),
            replay: None,
        };
        let encoded = codec.encode(&state).unwrap();
        assert_eq!(codec.decode(&encoded).unwrap(), state);
        let mut tampered = encoded.into_bytes();
        let last = tampered.len() - 1;
        tampered[last] = if tampered[last] == b'A' { b'B' } else { b'A' };
        assert!(
            codec
                .decode(std::str::from_utf8(&tampered).unwrap())
                .is_err()
        );
    }

    #[test]
    fn compressed_summary_history_replaces_older_prompt_history() {
        let mut accumulator = SessionApiHistoryAccumulator::default();
        accumulator.add(record("u1", Value::Null, "user", "old"));
        accumulator.add(json!({
            "type":"system",
            "subtype":"chat_compression",
            "systemPayload":{"compressedHistory":[{"role":"user","parts":[{"text":"summary"}]}]}
        }));
        accumulator.add(record("u2", json!("u1"), "user", "new"));
        let history = accumulator
            .finish(BuildApiHistoryOptions::default())
            .unwrap();
        assert_eq!(history.len(), 2);
        assert_eq!(history[0]["parts"][0]["text"], "summary");
        assert_eq!(history[1]["parts"][0]["text"], "new");
    }

    #[test]
    fn record_conversion_preserves_prompt_and_tool_ordering() {
        let values = vec![
            record("u1", Value::Null, "user", "prompt"),
            json!({"uuid":"a1","parentUuid":"u1","sessionId":"s","type":"assistant","message":{"role":"model","parts":[{"functionCall":{"id":"c1"}}]}}),
            json!({"uuid":"t1","parentUuid":"a1","sessionId":"s","type":"tool_result","message":{"role":"user","parts":[{"functionResponse":{"id":"c1"}}]}}),
        ];
        let serialized = serde_json::to_string(&values).unwrap();
        assert!(serialized.find("prompt").unwrap() < serialized.find("functionCall").unwrap());
        assert!(
            serialized.find("functionCall").unwrap() < serialized.find("functionResponse").unwrap()
        );
    }

    fn fixture() -> (SessionTranscriptReader, PathBuf) {
        let root =
            std::env::temp_dir().join(format!("canopy-transcript-reader-{}", uuid::Uuid::new_v4()));
        let reader = SessionTranscriptReader::new(root.join("runtime"), root.join("project"));
        (reader, root)
    }

    fn write_transcript(reader: &SessionTranscriptReader, contents: &str) {
        let path = reader
            .transcript_path("550e8400-e29b-41d4-a716-446655440000")
            .unwrap();
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, contents).unwrap();
    }

    #[test]
    fn reader_recovers_glued_fragments_and_uses_only_the_active_parent_chain() {
        let (reader, root) = fixture();
        let sid = "550e8400-e29b-41d4-a716-446655440000";
        let first = record("u1", Value::Null, "user", "hello");
        let fragment = record("u1", Value::Null, "user", " world");
        let assistant = record("a1", json!("u1"), "assistant", "answer");
        let abandoned = record("u-branch", json!("u1"), "user", "abandoned");
        let leaf = record("a2", json!("a1"), "assistant", "active tail");
        let artifact = json!({
            "uuid":"artifact",
            "parentUuid":"a2",
            "sessionId":sid,
            "type":"system",
            "subtype":"session_artifact_event",
            "systemPayload":{"changes":[]}
        });
        let glued = format!(
            "{}{}",
            serde_json::to_string(&first).unwrap(),
            serde_json::to_string(&fragment).unwrap()
        );
        let content = format!(
            "broken json\n{glued}\n{}\n{}\n{}\n{}\n",
            serde_json::to_string(&assistant).unwrap(),
            serde_json::to_string(&abandoned).unwrap(),
            serde_json::to_string(&leaf).unwrap(),
            serde_json::to_string(&artifact).unwrap(),
        );
        write_transcript(&reader, &content);

        let snapshot = reader.read_snapshot(sid).unwrap();
        let uuids = snapshot
            .runtime_records
            .iter()
            .filter_map(|item| item.get("uuid").and_then(Value::as_str))
            .collect::<Vec<_>>();
        assert_eq!(uuids, vec!["u1", "a1", "a2"]);
        assert_eq!(
            snapshot.runtime_records[0]["message"]["parts"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        assert_eq!(snapshot.leaf_uuid, "a2");
        assert_eq!(snapshot.artifact_records.len(), 1);
        assert!(
            snapshot
                .diagnostics
                .iter()
                .any(|item| item.code == "malformed_jsonl_line")
        );
        assert!(
            snapshot
                .diagnostics
                .iter()
                .any(|item| item.code == "recovered_jsonl_line")
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn restore_projection_keeps_compressed_summary_and_prompt_turn_state() {
        let (reader, root) = fixture();
        let sid = "550e8400-e29b-41d4-a716-446655440000";
        let mut prompt = record("u1", Value::Null, "user", "prompt");
        prompt["promptId"] = json!(format!("{sid}########7"));
        let assistant = record("a1", json!("u1"), "assistant", "old answer");
        let compression = json!({
            "uuid":"compress",
            "parentUuid":"a1",
            "sessionId":sid,
            "type":"system",
            "subtype":"chat_compression",
            "systemPayload":{"compressedHistory":[{"role":"user","parts":[{"text":"summary"}]}]}
        });
        let next_prompt = record("u2", json!("compress"), "user", "next prompt");
        let content = [prompt, assistant, compression, next_prompt]
            .iter()
            .map(serde_json::to_string)
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .join("\n");
        write_transcript(&reader, &format!("{content}\n"));

        let projection = reader
            .read_restore_projection(sid, SessionRestoreReplaySelection::None)
            .unwrap();
        assert_eq!(projection.runtime.last_completed_uuid, "u2");
        assert_eq!(projection.runtime.turn_state.initial_turn, 7.0);
        assert_eq!(
            projection.runtime.api_history[0]["parts"][0]["text"],
            "summary"
        );
        assert_eq!(
            projection.runtime.api_history[1]["parts"][0]["text"],
            "next prompt"
        );
        assert!(projection.replay.is_none());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn restore_projection_rebuilds_selected_durable_artifact_records() {
        let (reader, root) = fixture();
        let sid = "550e8400-e29b-41d4-a716-446655440000";
        let user = record("u1", Value::Null, "user", "prompt");
        let assistant = record("a1", json!("u1"), "assistant", "answer");
        let artifact = json!({
            "uuid":"artifact-event",
            "parentUuid":"a1",
            "sessionId":sid,
            "type":"system",
            "subtype":"session_artifact_event",
            "systemPayload":{
                "v":2,
                "sessionId":sid,
                "sequence":1,
                "recordedAt":"2026-01-01T00:00:01.000Z",
                "changes":[{
                    "action":"created",
                    "artifactId":"artifact-1",
                    "artifact":{
                        "id":"artifact-1",
                        "kind":"file",
                        "storage":"workspace",
                        "source":"tool",
                        "status":"available",
                        "title":"report.txt",
                        "workspacePath":"report.txt",
                        "retention":"restorable",
                        "clientRetained":false,
                        "createdAt":"2026-01-01T00:00:01.000Z",
                        "updatedAt":"2026-01-01T00:00:01.000Z"
                    }
                }]
            }
        });
        let contents = [user, assistant, artifact]
            .iter()
            .map(serde_json::to_string)
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .join("\n");
        write_transcript(&reader, &format!("{contents}\n"));

        let projection = reader
            .read_restore_projection(sid, SessionRestoreReplaySelection::None)
            .unwrap();
        let rebuilt = projection.runtime.artifact_snapshot.unwrap();
        assert_eq!(projection.artifact_records.len(), 1);
        assert_eq!(rebuilt.snapshot.session_id, sid);
        assert_eq!(rebuilt.snapshot.sequence, 1);
        assert_eq!(rebuilt.snapshot.artifacts.len(), 1);
        assert_eq!(rebuilt.snapshot.artifacts[0].id, "artifact-1");
        assert!(rebuilt.warnings.is_empty());
        fs::remove_dir_all(root).unwrap();
    }
}
