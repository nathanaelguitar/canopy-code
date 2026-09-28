//! Per-project session file access, writer ownership, and safe resume setup.
//!
//! This is the foundational filesystem slice of `services/sessionService.ts`.
//! It keeps the existing directory and JSONL contracts while placing a hard
//! cap on one in-memory resume projection until the paged transcript reader is
//! ported.

use std::fs::{File, OpenOptions};
use std::io::{self, BufReader};
use std::path::PathBuf;

use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

use crate::jsonl;
use crate::recording::{SessionRecorder, SessionRecorderOptions};
use crate::services::session_transcript_reader::select_active_session_artifact_records;
use crate::session_api_history::SessionApiHistoryError;
use crate::session_paths::{SessionArchiveState, SessionPaths};
use crate::session_recovery::{
    HistoryGap, SessionRecoveryOptions, SessionRecoveryPlan, build_session_recovery_plan,
};
use crate::session_resume_token_counts::{ResumeTokenCounts, get_resume_token_counts};
use crate::session_writer::{
    AcquireSessionWriterLeaseOptions, SessionWriterError, SessionWriterLease,
    SessionWriterProcessKind,
};
use crate::tool_effect_journal::find_unresolved_tool_effect_intents;
use crate::transcript::{
    PreparedTranscriptRecords, TranscriptRecordPreparationError, prepare_transcript_records,
};

/// Bound the aggregate transcript before `prepare_transcript_records` creates
/// its index and active-branch copies. The complete paged reader will replace
/// this cap for large session histories.
pub const MAX_SESSION_RESUME_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct SessionStore {
    paths: SessionPaths,
}

#[derive(Clone, Debug)]
pub struct SessionResumeOptions {
    pub process_kind: SessionWriterProcessKind,
    pub version: String,
    pub git_branch: Option<String>,
    pub allow_auto_continue: bool,
}

#[derive(Debug)]
pub struct ResumedSession {
    pub prepared_transcript: PreparedTranscriptRecords,
    pub recovery_plan: SessionRecoveryPlan,
    pub resume_token_counts: Option<ResumeTokenCounts>,
    pub recorder: SessionRecorder,
}

struct WriterLeaseGuard(Option<SessionWriterLease>);

impl WriterLeaseGuard {
    fn new(lease: SessionWriterLease) -> Self {
        Self(Some(lease))
    }

    fn lease_mut(&mut self) -> &mut SessionWriterLease {
        self.0.as_mut().expect("writer lease guard is populated")
    }

    fn into_inner(mut self) -> SessionWriterLease {
        self.0.take().expect("writer lease guard is populated")
    }
}

impl Drop for WriterLeaseGuard {
    fn drop(&mut self) {
        if let Some(lease) = &mut self.0 {
            let _ = lease.release();
        }
    }
}

#[derive(Debug, Error)]
pub enum SessionStoreError {
    #[error("invalid session id: {0}")]
    InvalidSessionId(String),
    #[error("session transcript exceeds the {limit}-byte in-memory resume limit (size: {size})")]
    TranscriptTooLarge { size: u64, limit: u64 },
    #[error("transcript session id does not match the requested session id")]
    SessionIdMismatch,
    #[error("session transcript has no valid conversation records")]
    EmptySession,
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Writer(#[from] SessionWriterError),
    #[error(transparent)]
    Projection(#[from] TranscriptRecordPreparationError),
    #[error(transparent)]
    ApiHistory(#[from] SessionApiHistoryError),
}

impl SessionStore {
    pub fn new(runtime_base_dir: impl Into<PathBuf>, project_root: impl Into<PathBuf>) -> Self {
        Self {
            paths: SessionPaths::new(runtime_base_dir, project_root),
        }
    }

    pub fn paths(&self) -> &SessionPaths {
        &self.paths
    }

    pub fn transcript_path(
        &self,
        session_id: &str,
        state: SessionArchiveState,
    ) -> Result<PathBuf, SessionStoreError> {
        self.paths
            .transcript_path(session_id, state, cfg!(windows))
            .ok_or_else(|| SessionStoreError::InvalidSessionId(session_id.to_owned()))
    }

    /// Read the raw tolerant JSONL record stream from a session file. Symlink
    /// following is refused on Unix and aggregate bytes are capped before
    /// record parsing.
    pub fn read_transcript(
        &self,
        session_id: &str,
        state: SessionArchiveState,
    ) -> Result<Vec<Value>, SessionStoreError> {
        let path = self.transcript_path(session_id, state)?;
        let file = match open_transcript_readonly(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error.into()),
        };
        let size = file.metadata()?.len();
        if size > MAX_SESSION_RESUME_BYTES {
            return Err(SessionStoreError::TranscriptTooLarge {
                size,
                limit: MAX_SESSION_RESUME_BYTES,
            });
        }
        jsonl::read_all_from(&mut BufReader::new(file)).map_err(Into::into)
    }

    pub fn prepare_active_transcript(
        &self,
        session_id: &str,
    ) -> Result<PreparedTranscriptRecords, SessionStoreError> {
        let records = self.read_transcript(session_id, SessionArchiveState::Active)?;
        let prepared = prepare_transcript_records(&Value::Array(records), None)?;
        if prepared.session_id.as_deref() != Some(session_id) {
            return Err(if prepared.session_id.is_none() {
                SessionStoreError::EmptySession
            } else {
                SessionStoreError::SessionIdMismatch
            });
        }
        Ok(prepared)
    }

    pub fn acquire_writer_lease(
        &self,
        session_id: &str,
        process_kind: SessionWriterProcessKind,
        version: Option<String>,
    ) -> Result<SessionWriterLease, SessionStoreError> {
        let transcript_path = self.transcript_path(session_id, SessionArchiveState::Active)?;
        let mut options = AcquireSessionWriterLeaseOptions::new(
            self.paths.runtime_base_dir(),
            session_id,
            transcript_path,
        );
        options.process_kind = process_kind;
        options.canopy_version = version;
        options.reclaim_stale_local_owner = true;
        Ok(SessionWriterLease::acquire(options)?)
    }

    /// Create a new session with a unique UUID and an exclusive writer lease.
    pub fn create_session(
        &self,
        process_kind: SessionWriterProcessKind,
        version: impl Into<String>,
        git_branch: Option<String>,
    ) -> Result<(String, SessionRecorder), SessionStoreError> {
        let version = version.into();
        let session_id = Uuid::new_v4().to_string();
        let lease = self.acquire_writer_lease(&session_id, process_kind, Some(version.clone()))?;
        let mut recorder_options =
            SessionRecorderOptions::new(self.paths.project_root().to_string_lossy(), version);
        recorder_options.git_branch = git_branch;
        Ok((session_id, SessionRecorder::new(lease, recorder_options)))
    }

    /// Acquire ownership before reading, rebuild active history and the
    /// recovery plan, then return a recorder positioned at the active leaf.
    pub fn resume_session(
        &self,
        session_id: &str,
        options: SessionResumeOptions,
    ) -> Result<ResumedSession, SessionStoreError> {
        let mut lease = WriterLeaseGuard::new(self.acquire_writer_lease(
            session_id,
            options.process_kind,
            Some(options.version.clone()),
        )?);
        let raw_records = self.read_transcript(session_id, SessionArchiveState::Active)?;
        lease.lease_mut().assert_owned_and_unchanged()?;
        let raw_value = Value::Array(raw_records);
        let prepared = prepare_transcript_records(&raw_value, None)?;
        if prepared.session_id.as_deref() != Some(session_id) {
            return Err(if prepared.session_id.is_none() {
                SessionStoreError::EmptySession
            } else {
                SessionStoreError::SessionIdMismatch
            });
        }

        let history_gaps: Vec<HistoryGap> = prepared
            .gaps
            .iter()
            .map(|gap| HistoryGap {
                child_uuid: gap.child_uuid.clone(),
                missing_parent_uuid: gap.missing_parent_uuid.clone(),
            })
            .collect();
        let api_records: Vec<Value> = prepared
            .records
            .iter()
            .map(serde_json::to_value)
            .collect::<Result<_, _>>()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        let resume_token_counts = get_resume_token_counts(&api_records);
        let unresolved_effects = find_unresolved_tool_effect_intents(&api_records);
        let mut recovery_plan = build_session_recovery_plan(
            session_id,
            api_records,
            &history_gaps,
            SessionRecoveryOptions {
                allow_auto_continue: options.allow_auto_continue,
            },
        )?;
        if !unresolved_effects.is_empty() {
            recovery_plan
                .repairs
                .extend(unresolved_effects.into_iter().map(|intent| {
                    crate::session_recovery::RecoveryRepair::UncertainToolEffect {
                        call_id: intent.call_id,
                        name: intent.name,
                    }
                }));
            recovery_plan.can_auto_continue = false;
            recovery_plan.requires_user_confirmation = true;
            recovery_plan.visible_notice = Some(format!(
                "Previous session stopped with {} tool side effect(s) whose result was not saved. Inspect the workspace before retrying those tools.",
                recovery_plan
                    .repairs
                    .iter()
                    .filter(|repair| matches!(
                        repair,
                        crate::session_recovery::RecoveryRepair::UncertainToolEffect { .. }
                    ))
                    .count()
            ));
        }
        let restored_last_record_uuid = prepared.records.last().map(|record| record.uuid.clone());
        let mut recorder_options = SessionRecorderOptions::new(
            self.paths.project_root().to_string_lossy(),
            options.version,
        );
        recorder_options.git_branch = options.git_branch;
        recorder_options.restored_last_record_uuid = restored_last_record_uuid;
        recorder_options.restored_artifact_records = select_active_session_artifact_records(
            raw_value.as_array().map_or(&[], Vec::as_slice),
            &prepared.records,
        );
        let recorder = SessionRecorder::new(lease.into_inner(), recorder_options);
        Ok(ResumedSession {
            prepared_transcript: prepared,
            recovery_plan,
            resume_token_counts,
            recorder,
        })
    }
}

fn open_transcript_readonly(path: &std::path::Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    options.open(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jsonl;
    use crate::session_paths::is_valid_session_id;
    use serde_json::json;
    use std::fs;
    use std::path::Path;

    fn temp_dir() -> PathBuf {
        std::env::temp_dir().join(format!("canopy-session-store-{}", Uuid::new_v4()))
    }

    fn sample_records(session_id: &str) -> Vec<Value> {
        vec![
            json!({"uuid":"u1","parentUuid":null,"sessionId":session_id,"timestamp":"2026-09-24T00:00:00Z","type":"user","cwd":"/work/project","version":"test","message":{"role":"user","parts":[{"text":"read file"}]}}),
            json!({"uuid":"a1","parentUuid":"u1","sessionId":session_id,"timestamp":"2026-09-24T00:00:01Z","type":"assistant","cwd":"/work/project","version":"test","message":{"role":"model","parts":[{"functionCall":{"id":"call-1","name":"read_file","args":{"path":"a.txt"}}}]}}),
        ]
    }

    fn store(root: &Path) -> SessionStore {
        SessionStore::new(root.join("state"), "/work/project")
    }

    fn write_sample(store: &SessionStore, session_id: &str) -> PathBuf {
        let path = store
            .transcript_path(session_id, SessionArchiveState::Active)
            .unwrap();
        jsonl::write(&path, &sample_records(session_id)).unwrap();
        path
    }

    #[test]
    fn resumes_a_session_into_a_repaired_plan_under_writer_ownership() {
        let root = temp_dir();
        let store = store(&root);
        let session_id = "00000000-0000-4000-8000-000000000001";
        let transcript = write_sample(&store, session_id);
        let resumed = store
            .resume_session(
                session_id,
                SessionResumeOptions {
                    process_kind: SessionWriterProcessKind::Interactive,
                    version: "test".to_owned(),
                    git_branch: None,
                    allow_auto_continue: false,
                },
            )
            .unwrap();
        assert_eq!(resumed.recovery_plan.session_id, session_id);
        assert_eq!(
            resumed.recovery_plan.kind,
            crate::session_recovery::SessionRecoveryKind::InterruptedTurn
        );
        assert_eq!(resumed.recorder.last_record_uuid(), Some("a1"));
        assert!(resumed.recorder.has_write_ownership());
        drop(resumed);
        fs::remove_dir_all(root).unwrap();
        assert!(!transcript.exists());
    }

    #[test]
    fn refuses_invalid_ids_and_transcripts_from_another_session() {
        let root = temp_dir();
        let store = store(&root);
        assert!(matches!(
            store.read_transcript("../../bad", SessionArchiveState::Active),
            Err(SessionStoreError::InvalidSessionId(_))
        ));
        let requested = "00000000-0000-4000-8000-000000000001";
        let transcript = store
            .transcript_path(requested, SessionArchiveState::Active)
            .unwrap();
        jsonl::write(
            &transcript,
            &sample_records("00000000-0000-4000-8000-000000000002"),
        )
        .unwrap();
        assert!(matches!(
            store.prepare_active_transcript(requested),
            Err(SessionStoreError::SessionIdMismatch)
        ));
        assert!(matches!(
            store.resume_session(
                requested,
                SessionResumeOptions {
                    process_kind: SessionWriterProcessKind::Interactive,
                    version: "test".to_owned(),
                    git_branch: None,
                    allow_auto_continue: false,
                }
            ),
            Err(SessionStoreError::SessionIdMismatch)
        ));
        jsonl::write(&transcript, &sample_records(requested)).unwrap();
        let mut resumed = store
            .resume_session(
                requested,
                SessionResumeOptions {
                    process_kind: SessionWriterProcessKind::Interactive,
                    version: "test".to_owned(),
                    git_branch: None,
                    allow_auto_continue: false,
                },
            )
            .unwrap();
        resumed.recorder.close().unwrap();
        fs::remove_dir_all(root).unwrap();
        assert!(!transcript.exists());
    }

    #[cfg(unix)]
    #[test]
    fn refuses_to_read_session_symlinks() {
        use std::os::unix::fs::symlink;

        let root = temp_dir();
        let store = store(&root);
        let session_id = "00000000-0000-4000-8000-000000000001";
        let path = store
            .transcript_path(session_id, SessionArchiveState::Active)
            .unwrap();
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let outside = root.join("outside.jsonl");
        jsonl::write(&outside, &sample_records(session_id)).unwrap();
        symlink(&outside, &path).unwrap();
        assert!(matches!(
            store.read_transcript(session_id, SessionArchiveState::Active),
            Err(SessionStoreError::Io(_))
        ));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn new_sessions_receive_uuid_ids_and_durable_writer_leases() {
        let root = temp_dir();
        let store = store(&root);
        let (session_id, recorder) = store
            .create_session(SessionWriterProcessKind::Interactive, "test", None)
            .unwrap();
        assert!(is_valid_session_id(&session_id));
        assert!(recorder.has_write_ownership());
        drop(recorder);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn unresolved_side_effect_intent_disables_automatic_resume() {
        let root = temp_dir();
        let store = store(&root);
        let session_id = "00000000-0000-4000-8000-000000000001";
        let mut records = sample_records(session_id);
        records.push(json!({
            "uuid":"intent-1",
            "parentUuid":"a1",
            "sessionId":session_id,
            "timestamp":"2026-09-24T00:00:02Z",
            "type":"system",
            "subtype":"tool_execution_intent",
            "cwd":"/work/project",
            "version":"test",
            "systemPayload":{"callId":"call-1","name":"write_file"}
        }));
        let path = store
            .transcript_path(session_id, SessionArchiveState::Active)
            .unwrap();
        jsonl::write(&path, &records).unwrap();
        let resumed = store
            .resume_session(
                session_id,
                SessionResumeOptions {
                    process_kind: SessionWriterProcessKind::Interactive,
                    version: "test".to_owned(),
                    git_branch: None,
                    allow_auto_continue: true,
                },
            )
            .unwrap();
        assert!(!resumed.recovery_plan.can_auto_continue);
        assert!(resumed.recovery_plan.requires_user_confirmation);
        assert!(
            resumed
                .recovery_plan
                .visible_notice
                .as_deref()
                .unwrap()
                .contains("Inspect the workspace")
        );
        assert!(resumed.recovery_plan.repairs.iter().any(|repair| matches!(
            repair,
            crate::session_recovery::RecoveryRepair::UncertainToolEffect { call_id, .. }
                if call_id == "call-1"
        )));
        drop(resumed);
        fs::remove_dir_all(root).unwrap();
    }
}
