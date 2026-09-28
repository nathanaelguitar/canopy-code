//! Durable session journal entry construction and append behavior.
//!
//! The current Canopy recorder has many UI and integration side paths. This
//! first Rust vertical slice covers the core model-facing record forms and
//! their persisted JSON contract. It writes synchronously through a
//! `SessionWriterLease`, so a successful method return means the JSONL line has
//! been synced to disk.

use crate::acp_bridge::session_artifacts::{
    ArtifactRetention, ArtifactSource, SESSION_ARTIFACT_PERSISTENCE_VERSION,
    SESSION_ARTIFACT_SNAPSHOT_INTERVAL, SessionArtifactEventRecordPayload, SessionArtifactInput,
    SessionArtifactSnapshotRecordPayload, SessionArtifactStore,
    rebuild_session_artifact_snapshot_with_warnings,
};
use crate::genai_compat::{ContentConversionError, create_model_content, create_user_content};
use crate::services::commit_attribution::AttributionSnapshot;
use crate::services::file_history::FileHistorySnapshot;
use crate::session_writer::{SessionWriterError, SessionWriterErrorKind, SessionWriterLease};
use crate::tool_display::sanitize_tool_call_result_for_recording;
use chrono::{SecondsFormat, Utc};
use serde_json::{Map, Value};
use std::collections::VecDeque;
use std::path::PathBuf;
use uuid::Uuid;

const MAX_TITLE_USER_DISPLAY_TEXTS: usize = 20;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChatRecordType {
    User,
    Assistant,
    ToolResult,
    System,
}

impl ChatRecordType {
    fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Assistant => "assistant",
            Self::ToolResult => "tool_result",
            Self::System => "system",
        }
    }

    fn provenance(self) -> &'static str {
        match self {
            Self::User => "real_user",
            Self::Assistant => "assistant_output",
            Self::ToolResult => "tool_result",
            Self::System => "system",
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct SessionRecordOptions {
    pub subtype: Option<String>,
    pub provenance: Option<String>,
    pub goal_context: Option<Value>,
    pub message: Option<Value>,
    pub system_payload: Option<Value>,
    pub model: Option<String>,
    pub usage_metadata: Option<Value>,
    pub context_window_size: Option<u64>,
    pub tool_call_result: Option<Value>,
    pub extra: Map<String, Value>,
}

#[derive(Clone, Debug)]
pub struct SessionRecorderOptions {
    pub cwd: String,
    pub version: String,
    pub git_branch: Option<String>,
    pub restored_last_record_uuid: Option<String>,
    /// Artifact event/snapshot records selected from the resumed transcript.
    pub restored_artifact_records: Vec<Value>,
}

impl SessionRecorderOptions {
    pub fn new(cwd: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            cwd: cwd.into(),
            version: version.into(),
            git_branch: None,
            restored_last_record_uuid: None,
            restored_artifact_records: Vec::new(),
        }
    }
}

#[derive(Debug)]
pub struct SessionRecorder {
    lease: SessionWriterLease,
    options: SessionRecorderOptions,
    last_record_uuid: Option<String>,
    last_persisted_record_uuid: Option<String>,
    write_failure: Option<(SessionWriterErrorKind, String)>,
    title_user_display_texts: VecDeque<Option<String>>,
    last_attribution_snapshot_json: Option<String>,
    artifact_store: SessionArtifactStore,
    artifact_restore_warnings: Vec<String>,
    artifact_events_since_snapshot: u64,
}

impl SessionRecorder {
    pub fn new(lease: SessionWriterLease, options: SessionRecorderOptions) -> Self {
        let last_record_uuid = options.restored_last_record_uuid.clone();
        let restored_artifact_snapshot = rebuild_session_artifact_snapshot_with_warnings(
            &options.restored_artifact_records,
            Some(&lease.session_id),
        );
        let (artifact_store, artifact_restore_warnings) =
            if let Some(rebuilt) = restored_artifact_snapshot.as_ref() {
                let mut store = SessionArtifactStore::new_with_external_persistence(
                    lease.session_id.clone(),
                    PathBuf::from(options.cwd.clone()),
                    None,
                );
                let warnings = store.restore_snapshot(rebuilt, false);
                (store, warnings)
            } else {
                (
                    SessionArtifactStore::new_with_external_persistence(
                        lease.session_id.clone(),
                        PathBuf::from(options.cwd.clone()),
                        None,
                    ),
                    Vec::new(),
                )
            };
        Self {
            lease,
            options,
            last_record_uuid: last_record_uuid.clone(),
            last_persisted_record_uuid: last_record_uuid,
            write_failure: None,
            title_user_display_texts: VecDeque::with_capacity(MAX_TITLE_USER_DISPLAY_TEXTS),
            last_attribution_snapshot_json: None,
            artifact_store,
            artifact_restore_warnings,
            artifact_events_since_snapshot: 0,
        }
    }

    pub fn session_id(&self) -> &str {
        &self.lease.session_id
    }

    pub fn runtime_base_dir(&self) -> &std::path::Path {
        &self.lease.runtime_base_dir
    }

    pub fn artifact_restore_warnings(&self) -> &[String] {
        &self.artifact_restore_warnings
    }

    pub fn last_record_uuid(&self) -> Option<&str> {
        self.last_record_uuid.as_deref()
    }

    pub fn last_persisted_record_uuid(&self) -> Option<&str> {
        self.last_persisted_record_uuid.as_deref()
    }

    pub fn title_user_display_texts(&self) -> impl Iterator<Item = Option<&str>> {
        self.title_user_display_texts
            .iter()
            .map(|value| value.as_deref())
    }

    pub fn record_user_message(
        &mut self,
        message: Value,
        goal_context: Option<Value>,
        prompt_payload: Option<Value>,
    ) -> Result<String, SessionWriterError> {
        let display_text = prompt_payload
            .as_ref()
            .and_then(|payload| payload.get("displayText"))
            .and_then(Value::as_str)
            .map(str::to_owned);
        self.track_title_user_text(display_text);
        let mut options = SessionRecordOptions {
            goal_context,
            message: Some(create_user_content(message).map_err(content_error_to_writer_error)?),
            system_payload: prompt_payload,
            ..SessionRecordOptions::default()
        };
        options.provenance = Some("real_user".to_owned());
        self.record(ChatRecordType::User, options)
    }

    pub fn record_goal_runtime_message(
        &mut self,
        message: Value,
        goal_context: Value,
    ) -> Result<String, SessionWriterError> {
        self.record(
            ChatRecordType::User,
            SessionRecordOptions {
                subtype: Some("goal_runtime".to_owned()),
                provenance: Some("goal_runtime".to_owned()),
                goal_context: Some(goal_context),
                message: Some(create_user_content(message).map_err(content_error_to_writer_error)?),
                ..SessionRecordOptions::default()
            },
        )
    }

    pub fn record_mid_turn_user_message(
        &mut self,
        message: Value,
        display_text: impl Into<String>,
        goal_context: Option<Value>,
        media_references: Option<Value>,
    ) -> Result<String, SessionWriterError> {
        let mut payload = Map::new();
        payload.insert("displayText".to_owned(), Value::String(display_text.into()));
        if let Some(media_references) = media_references {
            payload.insert("mediaReferences".to_owned(), media_references);
        }
        self.record(
            ChatRecordType::User,
            SessionRecordOptions {
                subtype: Some("mid_turn_user_message".to_owned()),
                goal_context,
                message: Some(create_user_content(message).map_err(content_error_to_writer_error)?),
                system_payload: Some(Value::Object(payload)),
                ..SessionRecordOptions::default()
            },
        )
    }

    pub fn record_assistant_turn(
        &mut self,
        model: impl Into<String>,
        message: Option<Value>,
        usage_metadata: Option<Value>,
        context_window_size: Option<u64>,
        goal_context: Option<Value>,
    ) -> Result<String, SessionWriterError> {
        let message = message
            .map(create_model_content)
            .transpose()
            .map_err(content_error_to_writer_error)?;
        self.record(
            ChatRecordType::Assistant,
            SessionRecordOptions {
                model: Some(model.into()),
                goal_context,
                message,
                usage_metadata,
                context_window_size,
                ..SessionRecordOptions::default()
            },
        )
    }

    pub fn record_tool_result(
        &mut self,
        message: Value,
        tool_call_result: Option<Value>,
        goal_context: Option<Value>,
        provenance: Option<String>,
    ) -> Result<String, SessionWriterError> {
        let artifact_metadata = tool_call_result
            .as_ref()
            .and_then(|result| result.get("artifacts"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let tool_call_id = tool_call_result
            .as_ref()
            .and_then(|result| result.get("callId"))
            .and_then(Value::as_str)
            .map(str::to_owned);
        let tool_name = tool_call_result
            .as_ref()
            .and_then(|result| result.get("name"))
            .and_then(Value::as_str)
            .map(str::to_owned);
        let tool_call_result = tool_call_result.map(sanitize_tool_call_result_for_recording);
        let record_id = self.record(
            ChatRecordType::ToolResult,
            SessionRecordOptions {
                goal_context,
                message: Some(create_user_content(message).map_err(content_error_to_writer_error)?),
                tool_call_result,
                provenance,
                ..SessionRecordOptions::default()
            },
        )?;
        self.record_tool_artifact_metadata(
            artifact_metadata,
            tool_call_id.as_deref(),
            tool_name.as_deref(),
        )?;
        Ok(record_id)
    }

    fn record_tool_artifact_metadata(
        &mut self,
        artifacts: Vec<Value>,
        tool_call_id: Option<&str>,
        tool_name: Option<&str>,
    ) -> Result<(), SessionWriterError> {
        for value in artifacts.into_iter().take(100) {
            let Ok(mut input) = serde_json::from_value::<SessionArtifactInput>(value) else {
                continue;
            };
            input.source = Some(ArtifactSource::Tool);
            input.retention = Some(ArtifactRetention::Restorable);
            input.client_retained = Some(false);
            input.tool_call_id = tool_call_id.map(str::to_owned);
            input.tool_name = tool_name.map(str::to_owned);
            let Ok(change) = self.artifact_store.upsert(input, false) else {
                continue;
            };
            if change.artifact.is_none()
                || !matches!(change.action.as_str(), "created" | "updated" | "removed")
            {
                continue;
            }
            let payload = SessionArtifactEventRecordPayload {
                v: SESSION_ARTIFACT_PERSISTENCE_VERSION,
                session_id: self.lease.session_id.clone(),
                sequence: self.artifact_store_sequence().saturating_add(1),
                recorded_at: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
                changes: vec![change],
            };
            self.record_session_artifact_event(payload)?;
            self.artifact_events_since_snapshot =
                self.artifact_events_since_snapshot.saturating_add(1);
            if self.artifact_events_since_snapshot >= SESSION_ARTIFACT_SNAPSHOT_INTERVAL {
                self.record_current_session_artifact_snapshot()?;
            }
        }
        Ok(())
    }

    fn artifact_store_sequence(&self) -> u64 {
        self.artifact_store.sequence()
    }

    pub fn record_session_artifact_event(
        &mut self,
        payload: SessionArtifactEventRecordPayload,
    ) -> Result<String, SessionWriterError> {
        if payload.session_id != self.lease.session_id
            || payload.sequence <= self.artifact_store.sequence()
        {
            return Err(SessionWriterError::new(
                SessionWriterErrorKind::Unavailable,
                "Session artifact event has an invalid session ID or sequence.",
            ));
        }
        let sequence = payload.sequence;
        let payload = serde_json::to_value(payload).map_err(|error| {
            SessionWriterError::new(
                SessionWriterErrorKind::Unavailable,
                format!("Failed to serialize session artifact event: {error}"),
            )
        })?;
        let record_id = self.record_side_system_event("session_artifact_event", payload)?;
        self.artifact_store.set_sequence(sequence);
        Ok(record_id)
    }

    pub fn record_session_artifact_snapshot(
        &mut self,
        payload: SessionArtifactSnapshotRecordPayload,
    ) -> Result<String, SessionWriterError> {
        if payload.session_id != self.lease.session_id
            || payload.sequence <= self.artifact_store.sequence()
        {
            return Err(SessionWriterError::new(
                SessionWriterErrorKind::Unavailable,
                "Session artifact snapshot has an invalid session ID or sequence.",
            ));
        }
        let sequence = payload.sequence;
        let payload = serde_json::to_value(payload).map_err(|error| {
            SessionWriterError::new(
                SessionWriterErrorKind::Unavailable,
                format!("Failed to serialize session artifact snapshot: {error}"),
            )
        })?;
        let record_id = self.record_side_system_event("session_artifact_snapshot", payload)?;
        self.artifact_store.set_sequence(sequence);
        Ok(record_id)
    }

    /// Append metadata beside the active conversation tail without making it
    /// the next model record's parent. Transcript projection treats artifact
    /// records as side records, matching ChatRecordingService's
    /// `updateActiveTail: false` behavior.
    fn record_side_system_event(
        &mut self,
        subtype: &str,
        payload: Value,
    ) -> Result<String, SessionWriterError> {
        let active_tail = self.last_record_uuid.clone();
        let record_id = self.record_system_event(subtype, payload)?;
        self.last_record_uuid = active_tail;
        Ok(record_id)
    }

    fn record_current_session_artifact_snapshot(&mut self) -> Result<(), SessionWriterError> {
        let sequence = self.artifact_store.sequence().saturating_add(1);
        let snapshot = self.artifact_store.snapshot_payload(sequence);
        self.record_session_artifact_snapshot(snapshot)?;
        self.artifact_events_since_snapshot = 0;
        Ok(())
    }

    pub fn record_system_event(
        &mut self,
        subtype: impl Into<String>,
        payload: Value,
    ) -> Result<String, SessionWriterError> {
        self.record(
            ChatRecordType::System,
            SessionRecordOptions {
                subtype: Some(subtype.into()),
                system_payload: Some(payload),
                ..SessionRecordOptions::default()
            },
        )
    }

    /// Persist one file-history snapshot using Canopy's session JSONL schema.
    ///
    /// Serialization delegates to `FileHistorySnapshot` so its ISO timestamp,
    /// camelCase keys, numeric versions, and omitted `failed: false` values
    /// match the TypeScript recorder. The shared system-event path preserves
    /// record UUID chaining and synchronous durable append semantics.
    pub fn record_file_history_snapshot(
        &mut self,
        snapshot: &FileHistorySnapshot,
    ) -> Result<String, SessionWriterError> {
        let snapshot = serde_json::to_value(snapshot).map_err(|error| {
            SessionWriterError::new(
                SessionWriterErrorKind::Unavailable,
                format!("Failed to serialize file-history snapshot: {error}"),
            )
        })?;
        let mut payload = Map::new();
        payload.insert("snapshots".to_owned(), Value::Array(vec![snapshot]));
        self.record_system_event("file_history_snapshot", Value::Object(payload))
    }

    /// Persist an attribution snapshot using the session JSONL contract.
    /// Consecutive identical snapshots are deduplicated to avoid rewriting the
    /// whole per-file map on each turn when nothing changed.
    pub fn record_attribution_snapshot(
        &mut self,
        snapshot: &AttributionSnapshot,
    ) -> Result<Option<String>, SessionWriterError> {
        let snapshot = serde_json::to_value(snapshot).map_err(|error| {
            SessionWriterError::new(
                SessionWriterErrorKind::Unavailable,
                format!("Failed to serialize attribution snapshot: {error}"),
            )
        })?;
        let snapshot_json = serde_json::to_string(&snapshot).map_err(|error| {
            SessionWriterError::new(
                SessionWriterErrorKind::Unavailable,
                format!("Failed to encode attribution snapshot: {error}"),
            )
        })?;
        if self.last_attribution_snapshot_json.as_deref() == Some(&snapshot_json) {
            return Ok(None);
        }

        let previous = self
            .last_attribution_snapshot_json
            .replace(snapshot_json.clone());
        let result = self.record_system_event(
            "attribution_snapshot",
            serde_json::json!({"snapshot": snapshot}),
        );
        match result {
            Ok(record_id) => Ok(Some(record_id)),
            Err(error) => {
                self.last_attribution_snapshot_json = previous;
                Err(error)
            }
        }
    }

    /// Synchronous writes leave no pending queue. This is the recorder's
    /// barrier equivalent: ownership and transcript bytes are revalidated.
    pub fn flush(&mut self) -> Result<(), SessionWriterError> {
        self.check_previous_failure()?;
        self.lease.assert_owned_and_unchanged()
    }

    pub fn close(&mut self) -> Result<(), SessionWriterError> {
        self.flush()?;
        self.lease.release()
    }

    pub fn close_for_handoff(&mut self) -> Result<(), SessionWriterError> {
        self.flush()?;
        self.lease.seal_for_handoff()
    }

    pub fn has_write_ownership(&self) -> bool {
        !self.lease.is_released() && self.write_failure.is_none()
    }

    fn record(
        &mut self,
        record_type: ChatRecordType,
        options: SessionRecordOptions,
    ) -> Result<String, SessionWriterError> {
        self.check_previous_failure()?;
        let record_id = Uuid::new_v4().to_string();
        let record = self.build_record(&record_id, record_type, options);
        match self.lease.append_json_line(&record) {
            Ok(()) => {
                self.last_record_uuid = Some(record_id.clone());
                self.last_persisted_record_uuid = Some(record_id.clone());
                Ok(record_id)
            }
            Err(error) => {
                self.last_record_uuid = self.last_persisted_record_uuid.clone();
                self.write_failure = Some((error.kind, error.to_string()));
                Err(error)
            }
        }
    }

    fn build_record(
        &self,
        record_id: &str,
        record_type: ChatRecordType,
        options: SessionRecordOptions,
    ) -> Value {
        let mut record = Map::new();
        record.insert("uuid".to_owned(), Value::String(record_id.to_owned()));
        record.insert(
            "parentUuid".to_owned(),
            self.last_record_uuid
                .clone()
                .map_or(Value::Null, Value::String),
        );
        record.insert(
            "sessionId".to_owned(),
            Value::String(self.lease.session_id.clone()),
        );
        record.insert(
            "timestamp".to_owned(),
            Value::String(Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)),
        );
        record.insert(
            "type".to_owned(),
            Value::String(record_type.as_str().to_owned()),
        );
        record.insert(
            "provenance".to_owned(),
            Value::String(
                options
                    .provenance
                    .clone()
                    .unwrap_or_else(|| record_type.provenance().to_owned()),
            ),
        );
        record.insert("cwd".to_owned(), Value::String(self.options.cwd.clone()));
        record.insert(
            "version".to_owned(),
            Value::String(self.options.version.clone()),
        );
        if let Some(branch) = &self.options.git_branch {
            record.insert("gitBranch".to_owned(), Value::String(branch.clone()));
        }
        if let Some(subtype) = &options.subtype {
            record.insert("subtype".to_owned(), Value::String(subtype.clone()));
        }
        if record_type == ChatRecordType::Assistant && options.subtype.is_none() {
            if let Some(model) = options.model.clone() {
                record.insert("model".to_owned(), Value::String(model));
            }
        }
        if let Some(goal_context) = options.goal_context {
            record.insert("goalContext".to_owned(), goal_context);
        }
        if let Some(message) = options.message {
            record.insert("message".to_owned(), message);
        }
        if record_type == ChatRecordType::Assistant && options.subtype.is_some() {
            if let Some(model) = options.model {
                record.insert("model".to_owned(), Value::String(model));
            }
        }
        if let Some(usage_metadata) = options.usage_metadata {
            record.insert("usageMetadata".to_owned(), usage_metadata);
        }
        if let Some(context_window_size) = options.context_window_size {
            record.insert(
                "contextWindowSize".to_owned(),
                Value::Number(context_window_size.into()),
            );
        }
        if let Some(tool_call_result) = options.tool_call_result {
            record.insert("toolCallResult".to_owned(), tool_call_result);
        }
        if let Some(system_payload) = options.system_payload {
            record.insert("systemPayload".to_owned(), system_payload);
        }
        for (key, value) in options.extra {
            record.insert(key, value);
        }
        Value::Object(record)
    }

    fn track_title_user_text(&mut self, display_text: Option<String>) {
        if self.title_user_display_texts.len() == MAX_TITLE_USER_DISPLAY_TEXTS {
            self.title_user_display_texts.pop_front();
        }
        self.title_user_display_texts.push_back(display_text);
    }

    fn check_previous_failure(&self) -> Result<(), SessionWriterError> {
        if let Some((kind, message)) = &self.write_failure {
            return Err(SessionWriterError::new(*kind, message.clone()));
        }
        Ok(())
    }
}

fn content_error_to_writer_error(error: ContentConversionError) -> SessionWriterError {
    SessionWriterError::new(SessionWriterErrorKind::Unavailable, error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jsonl;
    use crate::services::file_history::FileHistoryBackup;
    use crate::session_writer::AcquireSessionWriterLeaseOptions;
    use chrono::DateTime;
    use indexmap::IndexMap;
    use serde_json::json;
    use std::fs;
    use std::path::PathBuf;

    fn temp_dir() -> PathBuf {
        std::env::temp_dir().join(format!("canopy-session-recorder-{}", Uuid::new_v4()))
    }

    fn create_recorder(root: &PathBuf) -> (SessionRecorder, PathBuf) {
        let transcript = root.join("sessions").join("session-a.jsonl");
        let lease = SessionWriterLease::acquire(AcquireSessionWriterLeaseOptions::new(
            root,
            "session-a",
            &transcript,
        ))
        .unwrap();
        let mut options = SessionRecorderOptions::new("/work/project", "1.2.3");
        options.git_branch = Some("main".to_owned());
        (SessionRecorder::new(lease, options), transcript)
    }

    #[test]
    fn user_and_assistant_records_chain_and_persist_before_return() {
        let root = temp_dir();
        let (mut recorder, transcript) = create_recorder(&root);
        let user_id = recorder
            .record_user_message(
                json!("hello"),
                None,
                Some(json!({"displayText":"hello","hookContext":""})),
            )
            .unwrap();
        let assistant_id = recorder
            .record_assistant_turn(
                "model-x",
                Some(json!([{"text":"hi"}])),
                None,
                Some(64_000),
                None,
            )
            .unwrap();
        assert_eq!(recorder.last_record_uuid(), Some(assistant_id.as_str()));
        assert_eq!(
            recorder.last_persisted_record_uuid(),
            Some(assistant_id.as_str())
        );

        let records = jsonl::read(&transcript).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["uuid"], user_id);
        assert_eq!(records[0]["parentUuid"], Value::Null);
        assert_eq!(
            records[0]["message"],
            json!({"role":"user","parts":[{"text":"hello"}]})
        );
        assert_eq!(records[1]["parentUuid"], user_id);
        assert_eq!(records[1]["message"]["role"], "model");
        assert_eq!(records[1]["contextWindowSize"], 64_000);
        recorder.close().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn file_history_snapshot_uses_serialized_schema_and_persists_in_record_chain() {
        let root = temp_dir();
        let (mut recorder, transcript) = create_recorder(&root);
        let previous_id = recorder
            .record_system_event("before_snapshot", json!({"ready":true}))
            .unwrap();
        let timestamp = |value: &str| {
            DateTime::parse_from_rfc3339(value)
                .unwrap()
                .with_timezone(&Utc)
        };
        let snapshot = FileHistorySnapshot {
            prompt_id: "prompt-42".to_owned(),
            tracked_file_backups: IndexMap::from([
                (
                    "src/main.rs".to_owned(),
                    FileHistoryBackup {
                        backup_file_name: Some("abc123-v1.bak".to_owned()),
                        version: 1.0,
                        backup_time: timestamp("2026-09-24T12:34:56.789Z"),
                        failed: false,
                    },
                ),
                (
                    "src/missing.rs".to_owned(),
                    FileHistoryBackup {
                        backup_file_name: None,
                        version: 2.5,
                        backup_time: timestamp("2026-09-24T12:35:00.001Z"),
                        failed: true,
                    },
                ),
            ]),
            timestamp: timestamp("2026-09-24T12:35:01.234Z"),
        };

        let snapshot_id = recorder.record_file_history_snapshot(&snapshot).unwrap();
        assert_eq!(recorder.last_record_uuid(), Some(snapshot_id.as_str()));
        assert_eq!(
            recorder.last_persisted_record_uuid(),
            Some(snapshot_id.as_str())
        );

        // Reading the transcript immediately proves this synchronous API has
        // persisted the record before it returns.
        let records = jsonl::read(&transcript).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[1]["uuid"], snapshot_id);
        assert_eq!(records[1]["parentUuid"], previous_id);
        assert_eq!(records[1]["type"], "system");
        assert_eq!(records[1]["subtype"], "file_history_snapshot");
        assert_eq!(records[1]["provenance"], "system");
        assert_eq!(
            records[1]["systemPayload"],
            json!({
                "snapshots": [{
                    "promptId": "prompt-42",
                    "timestamp": "2026-09-24T12:35:01.234Z",
                    "trackedFileBackups": {
                        "src/main.rs": {
                            "backupFileName": "abc123-v1.bak",
                            "version": 1,
                            "backupTime": "2026-09-24T12:34:56.789Z"
                        },
                        "src/missing.rs": {
                            "backupFileName": null,
                            "version": 2.5,
                            "backupTime": "2026-09-24T12:35:00.001Z",
                            "failed": true
                        }
                    }
                }]
            })
        );
        recorder.close().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn write_failure_latches_and_does_not_advance_tail() {
        let root = temp_dir();
        let (mut recorder, transcript) = create_recorder(&root);
        let first = recorder
            .record_user_message(json!("one"), None, None)
            .unwrap();
        fs::write(&transcript, b"externally replaced\n").unwrap();
        assert_eq!(
            recorder
                .record_assistant_turn("model-x", None, None, None, None)
                .unwrap_err()
                .kind,
            SessionWriterErrorKind::TranscriptChanged,
        );
        assert_eq!(recorder.last_record_uuid(), Some(first.as_str()));
        assert_eq!(
            recorder
                .record_system_event("custom_title", json!({"customTitle":"x"}))
                .unwrap_err()
                .kind,
            SessionWriterErrorKind::TranscriptChanged,
        );
        assert!(!recorder.has_write_ownership());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn title_input_cache_is_bounded_to_recent_twenty_prompts() {
        let root = temp_dir();
        let (mut recorder, _) = create_recorder(&root);
        for index in 0..25 {
            recorder
                .record_user_message(
                    json!(format!("prompt-{index}")),
                    None,
                    Some(json!({"displayText":format!("prompt-{index}")})),
                )
                .unwrap();
        }
        let texts = recorder
            .title_user_display_texts()
            .map(|value| value.unwrap().to_owned())
            .collect::<Vec<_>>();
        assert_eq!(texts.len(), 20);
        assert_eq!(texts[0], "prompt-5");
        let _ = recorder.close();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn long_recording_keeps_only_bounded_prompt_cache_and_current_tail() {
        let root = temp_dir();
        let (mut recorder, transcript) = create_recorder(&root);
        for index in 0..1_000 {
            recorder
                .record_user_message(
                    json!(format!("prompt-{index}")),
                    None,
                    Some(json!({"displayText":format!("prompt-{index}")})),
                )
                .unwrap();
        }
        assert_eq!(
            recorder.title_user_display_texts.len(),
            MAX_TITLE_USER_DISPLAY_TEXTS
        );
        assert!(recorder.last_record_uuid().is_some());
        assert_eq!(jsonl::count_lines(&transcript).unwrap(), 1_000);
        recorder.close().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn tool_result_recording_removes_ephemeral_and_large_subagent_fields() {
        let root = temp_dir();
        let (mut recorder, transcript) = create_recorder(&root);
        recorder
            .record_tool_result(
                json!([{"functionResponse":{"name":"task","response":{"output":"done"}}}]),
                Some(json!({
                    "callId":"call-1",
                    "persistedOutputFiles":["/tmp/output"],
                    "boundaryArtifact":{"state":"reusable"},
                    "resultDisplay":{
                        "type":"task_execution",
                        "taskDescription":"run",
                        "taskPrompt":"prompt",
                        "toolCalls":[{"callId":"nested","args":{"large":"value"},"responseParts":[{"text":"large"}],"result":"full result"}]
                    }
                })),
                None,
                None,
            )
            .unwrap();
        let records = jsonl::read(&transcript).unwrap();
        let recorded_result = &records[0]["toolCallResult"];
        assert!(recorded_result.get("persistedOutputFiles").is_none());
        assert!(recorded_result.get("boundaryArtifact").is_none());
        assert_eq!(recorded_result["resultDisplay"]["toolCalls"], json!([]));
        recorder.close().unwrap();
        let _ = fs::remove_dir_all(root);
    }
}
