//! Session artifact collection, identity, ownership, and JSONL persistence.
//!
//! The TS store has additional restore validation (workspace hashing, sticky
//! ephemeral ids, published upgrades, and adaptive snapshots). This module
//! ports the shared artifact contract, live-store path, and transcript replay
//! state needed to restore durable artifacts.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use chrono::{SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use thiserror::Error;

pub const SESSION_ARTIFACT_PERSISTENCE_VERSION: u8 = 2;
pub const DEFAULT_MAX_SESSION_ARTIFACTS: usize = 200;
const SOURCE_RESERVATIONS: [(ArtifactSource, usize); 3] = [
    (ArtifactSource::Tool, 100),
    (ArtifactSource::Client, 50),
    (ArtifactSource::Hook, 50),
];
const MAX_TITLE_CHARS: usize = 200;
const MAX_DESCRIPTION_CHARS: usize = 1_000;
const MAX_WORKSPACE_PATH_CHARS: usize = 500;
const MAX_URL_CHARS: usize = 2_048;
const MAX_METADATA_BYTES: usize = 16 * 1024;
const MAX_PERSISTED_ARTIFACTS: usize = 500;
const MAX_PERSISTED_EVENT_CHANGES: usize = 800;
const MAX_PERSISTED_IDS: usize = 500;
const MAX_PERSISTED_MARKER_ARTIFACTS: usize = MAX_PERSISTED_IDS * 2;
pub const SESSION_ARTIFACT_SNAPSHOT_INTERVAL: u64 = 50;
const MAX_SNAPSHOT_BACKOFF_MULTIPLIER: u64 = 4;

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactKind {
    File,
    Link,
    Html,
    Image,
    Video,
    Audio,
    Pdf,
    Notebook,
    Other,
}
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactStorage {
    Workspace,
    ExternalUrl,
    Managed,
    Published,
}
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactSource {
    Tool,
    Hook,
    Client,
}
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactStatus {
    Available,
    Missing,
    Changed,
}
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactRetention {
    Ephemeral,
    Restorable,
    /// Accepted when reading older persisted records, then downgraded during
    /// restore because the runtime does not implement pinned retention.
    Pinned,
}
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactRestoreState {
    Live,
    Restored,
    Unverified,
    Blocked,
}
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactPersistenceWarning {
    PersistenceUnavailable,
    MetadataOnlyRestore,
    RestoreValidationFailed,
    StickyOverrideActive,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionArtifact {
    pub id: String,
    pub kind: ArtifactKind,
    pub storage: ArtifactStorage,
    pub source: ArtifactSource,
    pub status: ArtifactStatus,
    pub title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub managed_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Map<String, Value>>,
    pub retention: ArtifactRetention,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub restore_state: Option<ArtifactRestoreState>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub persistence_warning: Option<ArtifactPersistenceWarning>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub persisted_at: Option<String>,
    pub client_retained: bool,
    pub created_at: String,
    pub updated_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hook_event_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionArtifactInput {
    pub kind: Option<ArtifactKind>,
    pub storage: Option<ArtifactStorage>,
    pub source: Option<ArtifactSource>,
    pub title: String,
    pub description: Option<String>,
    pub workspace_path: Option<String>,
    pub managed_id: Option<String>,
    pub url: Option<String>,
    pub mime_type: Option<String>,
    pub size_bytes: Option<u64>,
    pub metadata: Option<Map<String, Value>>,
    pub retention: Option<ArtifactRetention>,
    pub client_retained: Option<bool>,
    pub tool_call_id: Option<String>,
    pub tool_name: Option<String>,
    pub hook_event_name: Option<String>,
    pub client_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionArtifactChange {
    pub action: String,
    pub artifact_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artifact: Option<SessionArtifact>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionArtifactEventRecordPayload {
    pub v: u8,
    pub session_id: String,
    pub sequence: u64,
    pub recorded_at: String,
    pub changes: Vec<SessionArtifactChange>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionArtifactSnapshotRecordPayload {
    pub v: u8,
    pub session_id: String,
    pub sequence: u64,
    pub recorded_at: String,
    pub artifacts: Vec<SessionArtifact>,
    pub tombstoned_ids: Vec<String>,
    pub sticky_ephemeral_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub marker_artifacts: Vec<SessionArtifact>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RebuiltSessionArtifactSnapshot {
    pub snapshot: SessionArtifactSnapshotRecordPayload,
    pub warnings: Vec<String>,
}

#[derive(Debug, Error)]
pub enum SessionArtifactError {
    #[error("{0}")]
    Validation(String),
    #[error("artifact {artifact_id} is owned by a different client")]
    Forbidden {
        session_id: String,
        artifact_id: String,
        owner_client_id: String,
        requester_client_id: Option<String>,
    },
    #[error("Session artifact limit reached ({0})")]
    Limit(usize),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

pub trait SessionArtifactPersistence: Send + Sync {
    fn record_event(&self, payload: &SessionArtifactEventRecordPayload) -> std::io::Result<()>;
    fn record_snapshot(
        &self,
        payload: &SessionArtifactSnapshotRecordPayload,
    ) -> std::io::Result<()>;
}

/// Transcript JSONL adapter using Canopy's durable append helper.
pub struct JsonlArtifactPersistence {
    path: PathBuf,
}
impl JsonlArtifactPersistence {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}
impl SessionArtifactPersistence for JsonlArtifactPersistence {
    fn record_event(&self, payload: &SessionArtifactEventRecordPayload) -> std::io::Result<()> {
        crate::jsonl::write_line(
            &self.path,
            &json!({"type":"system","subtype":"session_artifact_event","systemPayload":payload}),
        )
    }
    fn record_snapshot(
        &self,
        payload: &SessionArtifactSnapshotRecordPayload,
    ) -> std::io::Result<()> {
        crate::jsonl::write_line(
            &self.path,
            &json!({"type":"system","subtype":"session_artifact_snapshot","systemPayload":payload}),
        )
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionArtifactsEnvelope {
    pub v: u8,
    pub session_id: String,
    pub artifacts: Vec<SessionArtifact>,
    pub generated_at: String,
    pub limits: Map<String, Value>,
}

#[derive(Clone)]
struct StoredArtifact {
    artifact: SessionArtifact,
    insert_seq: u64,
}
pub struct SessionArtifactStore {
    session_id: String,
    workspace_cwd: PathBuf,
    max_artifacts: usize,
    persistence: Option<Arc<dyn SessionArtifactPersistence>>,
    external_persistence: bool,
    artifacts: HashMap<String, StoredArtifact>,
    tombstoned_ids: HashSet<String>,
    tombstoned_order: VecDeque<String>,
    sticky_ephemeral_ids: HashSet<String>,
    sticky_ephemeral_order: VecDeque<String>,
    marker_artifacts: HashMap<String, SessionArtifact>,
    sequence: u64,
    insert_seq: u64,
    durable_events_since_snapshot: u64,
    consecutive_snapshot_failures: u32,
}

impl fmt::Debug for SessionArtifactStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionArtifactStore")
            .field("session_id", &self.session_id)
            .field("workspace_cwd", &self.workspace_cwd)
            .field("max_artifacts", &self.max_artifacts)
            .field("persistence_enabled", &self.persistence.is_some())
            .field("external_persistence", &self.external_persistence)
            .field("artifact_count", &self.artifacts.len())
            .field("sequence", &self.sequence)
            .finish()
    }
}

impl SessionArtifactStore {
    pub fn new(
        session_id: impl Into<String>,
        workspace_cwd: impl Into<PathBuf>,
        max_artifacts: Option<usize>,
        persistence: Option<Arc<dyn SessionArtifactPersistence>>,
    ) -> Self {
        Self {
            session_id: session_id.into(),
            workspace_cwd: workspace_cwd.into(),
            max_artifacts: max_artifacts.unwrap_or(DEFAULT_MAX_SESSION_ARTIFACTS),
            persistence,
            external_persistence: false,
            artifacts: HashMap::new(),
            tombstoned_ids: HashSet::new(),
            tombstoned_order: VecDeque::new(),
            sticky_ephemeral_ids: HashSet::new(),
            sticky_ephemeral_order: VecDeque::new(),
            marker_artifacts: HashMap::new(),
            sequence: 0,
            insert_seq: 0,
            durable_events_since_snapshot: 0,
            consecutive_snapshot_failures: 0,
        }
    }

    /// Rehydrate the live store from a validated transcript projection.
    ///
    /// This keeps subsequent tool artifacts on the same stable identities and
    /// sequence as durable events already present in the session.
    pub fn from_snapshot(
        snapshot: &SessionArtifactSnapshotRecordPayload,
        workspace_cwd: impl Into<PathBuf>,
        max_artifacts: Option<usize>,
    ) -> Self {
        let mut store = Self::new(
            snapshot.session_id.clone(),
            workspace_cwd,
            max_artifacts,
            None,
        );
        store.external_persistence = true;
        let rebuilt = RebuiltSessionArtifactSnapshot {
            snapshot: snapshot.clone(),
            warnings: Vec::new(),
        };
        let _ = store.restore_snapshot(&rebuilt, false);
        store
    }

    /// Revalidate replayed transcript artifacts against the current workspace
    /// before exposing them to live callers. Snapshot completeness failures
    /// preserve the previous live collection instead of replacing it with a
    /// partial restore.
    pub fn restore_snapshot(
        &mut self,
        rebuilt: &RebuiltSessionArtifactSnapshot,
        preserve_live_ephemeral: bool,
    ) -> Vec<String> {
        let snapshot = &rebuilt.snapshot;
        let mut warnings = rebuilt.warnings.clone();
        if snapshot.v != SESSION_ARTIFACT_PERSISTENCE_VERSION {
            warnings.push(format!(
                "skipped v{} artifact persistence snapshot (expected v{})",
                snapshot.v, SESSION_ARTIFACT_PERSISTENCE_VERSION
            ));
            return warnings;
        }
        if snapshot.session_id != self.session_id {
            warnings.push("skipped artifact snapshot for mismatched sessionId".into());
            return warnings;
        }
        if snapshot.artifacts.len() > MAX_PERSISTED_ARTIFACTS {
            warnings.push(format!(
                "snapshot artifact list truncated to {MAX_PERSISTED_ARTIFACTS}"
            ));
        }
        if snapshot.marker_artifacts.len() > MAX_PERSISTED_MARKER_ARTIFACTS {
            warnings.push(format!(
                "snapshot marker artifact list truncated to {MAX_PERSISTED_MARKER_ARTIFACTS}"
            ));
        }
        let baseline_warnings = warnings.clone();
        let previous_artifacts = self.artifacts.clone();
        let previous_tombstoned_ids = self.tombstoned_ids.clone();
        let previous_tombstoned_order = self.tombstoned_order.clone();
        let previous_sticky_ids = self.sticky_ephemeral_ids.clone();
        let previous_sticky_order = self.sticky_ephemeral_order.clone();
        let previous_markers = self.marker_artifacts.clone();
        let previous_sequence = self.sequence;
        let previous_insert_seq = self.insert_seq;
        let previous_events_since_snapshot = self.durable_events_since_snapshot;
        let previous_snapshot_failures = self.consecutive_snapshot_failures;
        let preserved_ephemeral: Vec<StoredArtifact> = if preserve_live_ephemeral {
            self.artifacts
                .values()
                .filter(|stored| stored.artifact.retention == ArtifactRetention::Ephemeral)
                .cloned()
                .collect()
        } else {
            Vec::new()
        };

        self.artifacts.clear();
        self.tombstoned_ids.clear();
        self.tombstoned_order.clear();
        self.sticky_ephemeral_ids.clear();
        self.sticky_ephemeral_order.clear();
        self.marker_artifacts.clear();
        self.sequence = snapshot.sequence;
        self.insert_seq = 0;
        self.durable_events_since_snapshot = 0;
        self.consecutive_snapshot_failures = 0;

        for id in snapshot
            .tombstoned_ids
            .iter()
            .filter(|id| id.chars().count() <= 200)
            .take(MAX_PERSISTED_IDS)
        {
            remember_ordered_id(
                &mut self.tombstoned_order,
                &mut self.tombstoned_ids,
                id,
                MAX_PERSISTED_IDS,
            );
        }
        for id in snapshot
            .sticky_ephemeral_ids
            .iter()
            .filter(|id| id.chars().count() <= 200)
        {
            remember_ordered_id(
                &mut self.sticky_ephemeral_order,
                &mut self.sticky_ephemeral_ids,
                id,
                MAX_PERSISTED_IDS,
            );
        }

        let marker_ids: HashSet<_> = self
            .tombstoned_ids
            .iter()
            .chain(self.sticky_ephemeral_ids.iter())
            .cloned()
            .collect();
        for artifact in snapshot
            .marker_artifacts
            .iter()
            .take(MAX_PERSISTED_MARKER_ARTIFACTS)
        {
            if !marker_ids.contains(&artifact.id) {
                continue;
            }
            if artifact.retention == ArtifactRetention::Pinned {
                warnings.push(format!(
                    "pinned marker artifact {} downgraded to restorable; runtime does not support pinned retention",
                    artifact.id
                ));
            }
            if persisted_metadata_exceeds_budget(artifact.metadata.as_ref()) {
                warnings.push(format!(
                    "skipped oversized metadata for artifact {}",
                    artifact.id
                ));
            }
            match self.normalize_restored_artifact(artifact) {
                Ok(normalized) if normalized.id == artifact.id => {
                    self.marker_artifacts
                        .insert(normalized.id.clone(), normalized);
                }
                Ok(_) => warnings.push(format!(
                    "skipped marker artifact with mismatched id {}",
                    artifact.id
                )),
                Err(error) => {
                    warnings.push(format!("skipped marker artifact {}: {error}", artifact.id))
                }
            }
        }

        let mut restored_count = 0usize;
        for artifact in snapshot.artifacts.iter().take(MAX_PERSISTED_ARTIFACTS) {
            if persisted_metadata_exceeds_budget(artifact.metadata.as_ref()) {
                warnings.push(format!(
                    "skipped oversized metadata for artifact {}",
                    artifact.id
                ));
            }
            let mut input_retention = artifact.retention;
            if input_retention == ArtifactRetention::Pinned {
                input_retention = ArtifactRetention::Restorable;
                warnings.push(format!(
                    "pinned artifact {} downgraded to restorable; runtime does not support pinned retention",
                    artifact.id
                ));
            }
            if input_retention == ArtifactRetention::Ephemeral {
                continue;
            }
            match self.normalize_restored_artifact_with_retention(artifact, input_retention) {
                Ok(mut normalized) => {
                    if normalized.id != artifact.id {
                        warnings.push(format!(
                            "skipped artifact with mismatched id {}",
                            artifact.id
                        ));
                        continue;
                    }
                    if self.sticky_ephemeral_ids.contains(&normalized.id) {
                        normalized.retention = ArtifactRetention::Ephemeral;
                        normalized.persistence_warning =
                            Some(ArtifactPersistenceWarning::StickyOverrideActive);
                    } else if normalized.status != ArtifactStatus::Available {
                        normalized.persistence_warning =
                            Some(ArtifactPersistenceWarning::MetadataOnlyRestore);
                    } else {
                        normalized.persistence_warning = None;
                    }
                    normalized.restore_state = Some(ArtifactRestoreState::Restored);
                    normalized.client_retained = artifact.client_retained;
                    normalized.client_id = artifact.client_id.clone();
                    normalized.created_at = artifact.created_at.clone();
                    normalized.updated_at = artifact.updated_at.clone();
                    normalized.persisted_at = artifact.persisted_at.clone();
                    self.insert_seq = self.insert_seq.saturating_add(1);
                    self.artifacts.insert(
                        normalized.id.clone(),
                        StoredArtifact {
                            artifact: normalized,
                            insert_seq: self.insert_seq,
                        },
                    );
                    restored_count += 1;
                }
                Err(error) => warnings.push(format!("skipped artifact restore: {error}")),
            }
        }

        let rollback = |store: &mut SessionArtifactStore| {
            store.artifacts = previous_artifacts.clone();
            store.tombstoned_ids = previous_tombstoned_ids.clone();
            store.tombstoned_order = previous_tombstoned_order.clone();
            store.sticky_ephemeral_ids = previous_sticky_ids.clone();
            store.sticky_ephemeral_order = previous_sticky_order.clone();
            store.marker_artifacts = previous_markers.clone();
            store.sequence = previous_sequence;
            store.insert_seq = previous_insert_seq;
            store.durable_events_since_snapshot = previous_events_since_snapshot;
            store.consecutive_snapshot_failures = previous_snapshot_failures;
        };
        if !snapshot.artifacts.is_empty() && restored_count == 0 {
            rollback(self);
            warnings.push("artifact snapshot restore failed; kept existing live artifacts".into());
            return warnings.clone();
        }
        if !previous_artifacts.is_empty()
            && baseline_warnings
                .iter()
                .any(|warning| is_artifact_snapshot_completeness_warning(warning))
        {
            rollback(self);
            let message = if restored_count == 0 {
                "artifact snapshot restore failed; kept existing live artifacts"
            } else {
                "artifact snapshot restore partially failed; kept existing live artifacts"
            };
            warnings.push(message.into());
            return warnings.clone();
        }

        for mut stored in preserved_ephemeral {
            if self.artifacts.contains_key(&stored.artifact.id)
                || self.tombstoned_ids.contains(&stored.artifact.id)
            {
                continue;
            }
            self.insert_seq = self.insert_seq.saturating_add(1);
            stored.insert_seq = self.insert_seq;
            self.artifacts.insert(stored.artifact.id.clone(), stored);
        }
        while self.artifacts.len() > self.max_artifacts {
            if self.evict_one().is_err() {
                break;
            }
            if !warnings
                .iter()
                .any(|warning| warning == "restored artifact list pruned to live limit")
            {
                warnings.push("restored artifact list pruned to live limit".into());
            }
        }
        warnings.clone()
    }

    fn normalize_restored_artifact(
        &self,
        artifact: &SessionArtifact,
    ) -> Result<SessionArtifact, SessionArtifactError> {
        self.normalize_restored_artifact_with_retention(artifact, artifact.retention)
    }

    fn normalize_restored_artifact_with_retention(
        &self,
        artifact: &SessionArtifact,
        mut retention: ArtifactRetention,
    ) -> Result<SessionArtifact, SessionArtifactError> {
        if retention == ArtifactRetention::Pinned {
            retention = ArtifactRetention::Restorable;
        }
        let input = SessionArtifactInput {
            kind: Some(artifact.kind),
            storage: Some(artifact.storage),
            source: Some(artifact.source),
            title: artifact.title.clone(),
            description: artifact.description.clone(),
            workspace_path: artifact.workspace_path.clone(),
            managed_id: artifact.managed_id.clone(),
            url: artifact.url.clone(),
            mime_type: artifact.mime_type.clone(),
            size_bytes: artifact.size_bytes,
            metadata: None,
            retention: Some(retention),
            client_retained: Some(artifact.client_retained),
            tool_call_id: artifact.tool_call_id.clone(),
            tool_name: artifact.tool_name.clone(),
            hook_event_name: artifact.hook_event_name.clone(),
            client_id: artifact.client_id.clone(),
        };
        let trusted_publisher = artifact.storage == ArtifactStorage::Published
            && artifact
                .url
                .as_deref()
                .is_some_and(|url| !url.to_ascii_lowercase().starts_with("file:"));
        let mut normalized = self.normalize(input, trusted_publisher)?;
        if normalized.id != artifact.id {
            return Ok(normalized);
        }
        normalized.metadata = sanitize_persisted_metadata(artifact.metadata.as_ref());
        if let Some(path) = normalized.workspace_path.as_deref() {
            let (status, size_bytes) = restored_workspace_status(
                &self.workspace_cwd,
                path,
                artifact.size_bytes,
                normalized.metadata.as_ref(),
            );
            normalized.status = status;
            normalized.size_bytes = size_bytes;
        }
        normalized.client_retained = artifact.client_retained;
        normalized.client_id = artifact.client_id.clone();
        normalized.created_at = artifact.created_at.clone();
        normalized.updated_at = artifact.updated_at.clone();
        normalized.persisted_at = artifact.persisted_at.clone();
        Ok(normalized)
    }

    /// Construct a live artifact store whose caller appends event and snapshot
    /// records to its own already-owned transcript writer.
    pub fn new_with_external_persistence(
        session_id: impl Into<String>,
        workspace_cwd: impl Into<PathBuf>,
        max_artifacts: Option<usize>,
    ) -> Self {
        let mut store = Self::new(session_id, workspace_cwd, max_artifacts, None);
        store.external_persistence = true;
        store
    }

    /// Build the durable snapshot payload corresponding to the current store.
    /// The caller supplies the transcript sequence to preserve recorder order.
    pub fn snapshot_payload(&self, sequence: u64) -> SessionArtifactSnapshotRecordPayload {
        let mut stored: Vec<_> = self
            .artifacts
            .values()
            .filter(|item| item.artifact.retention != ArtifactRetention::Ephemeral)
            .collect();
        stored.sort_by_key(|item| item.insert_seq);
        SessionArtifactSnapshotRecordPayload {
            v: SESSION_ARTIFACT_PERSISTENCE_VERSION,
            session_id: self.session_id.clone(),
            sequence,
            recorded_at: now_iso(),
            artifacts: stored
                .into_iter()
                .take(MAX_PERSISTED_ARTIFACTS)
                .map(|item| item.artifact.clone())
                .collect(),
            tombstoned_ids: self.tombstoned_order.iter().cloned().collect(),
            sticky_ephemeral_ids: self.sticky_ephemeral_order.iter().cloned().collect(),
            marker_artifacts: self
                .tombstoned_order
                .iter()
                .chain(self.sticky_ephemeral_order.iter())
                .filter_map(|id| self.marker_artifacts.get(id))
                .take(MAX_PERSISTED_MARKER_ARTIFACTS)
                .cloned()
                .collect(),
        }
    }
    pub fn input_batch_limit(&self) -> usize {
        self.max_artifacts.saturating_mul(2)
    }
    pub fn len(&self) -> usize {
        self.artifacts.len()
    }
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
    pub fn set_sequence(&mut self, sequence: u64) {
        self.sequence = sequence;
    }
    pub fn is_empty(&self) -> bool {
        self.artifacts.is_empty()
    }
    pub fn get(&self, id: &str) -> Option<&SessionArtifact> {
        self.artifacts.get(id).map(|s| &s.artifact)
    }
    pub fn list(&self) -> SessionArtifactsEnvelope {
        let mut artifacts: Vec<_> = self.artifacts.values().collect();
        artifacts.sort_by_key(|artifact| artifact.insert_seq);
        SessionArtifactsEnvelope {
            v: 1,
            session_id: self.session_id.clone(),
            artifacts: artifacts
                .into_iter()
                .map(|stored| stored.artifact.clone())
                .collect(),
            generated_at: now_iso(),
            limits: [("maxArtifacts".to_owned(), json!(self.max_artifacts))]
                .into_iter()
                .collect(),
        }
    }

    pub fn upsert(
        &mut self,
        input: SessionArtifactInput,
        trusted_publisher: bool,
    ) -> Result<SessionArtifactChange, SessionArtifactError> {
        let artifact = self.normalize(input, trusted_publisher)?;
        if self.tombstoned_ids.contains(&artifact.id) {
            return Ok(SessionArtifactChange {
                action: "suppressed".into(),
                artifact_id: artifact.id,
                artifact: None,
                reason: None,
            });
        }
        if let Some(existing) = self.artifacts.get(&artifact.id) {
            ensure_owner(
                &self.session_id,
                &existing.artifact,
                artifact.client_id.as_deref(),
            )?;
            let mut updated = artifact;
            updated.id = existing.artifact.id.clone();
            updated.created_at = existing.artifact.created_at.clone();
            updated.source = existing.artifact.source;
            updated.client_id = existing.artifact.client_id.clone();
            updated.client_retained |= existing.artifact.client_retained;
            if public_artifacts_equal(&existing.artifact, &updated) {
                return Ok(SessionArtifactChange {
                    action: "updated".into(),
                    artifact_id: updated.id,
                    artifact: Some(existing.artifact.clone()),
                    reason: None,
                });
            }
            let change = SessionArtifactChange {
                action: "updated".into(),
                artifact_id: updated.id.clone(),
                artifact: Some(updated.clone()),
                reason: None,
            };
            self.persist_changes(vec![change.clone()])?;
            if let Some(old) = self.artifacts.get_mut(&updated.id) {
                old.artifact = updated;
            }
            self.maybe_snapshot();
            return Ok(change);
        }
        if self.artifacts.len() >= self.max_artifacts {
            self.evict_one()?;
        }
        self.insert_seq += 1;
        let change = SessionArtifactChange {
            action: "created".into(),
            artifact_id: artifact.id.clone(),
            artifact: Some(artifact.clone()),
            reason: None,
        };
        self.persist_changes(vec![change.clone()])?;
        self.artifacts.insert(
            artifact.id.clone(),
            StoredArtifact {
                artifact,
                insert_seq: self.insert_seq,
            },
        );
        self.maybe_snapshot();
        Ok(change)
    }

    pub fn remove(
        &mut self,
        artifact_id: &str,
        client_id: Option<&str>,
    ) -> Result<Option<SessionArtifactChange>, SessionArtifactError> {
        let Some(stored) = self.artifacts.get(artifact_id) else {
            return Ok(None);
        };
        ensure_owner(&self.session_id, &stored.artifact, client_id)?;
        let artifact = stored.artifact.clone();
        let change = SessionArtifactChange {
            action: "removed".into(),
            artifact_id: artifact_id.into(),
            artifact: Some(artifact.clone()),
            reason: Some("explicit".into()),
        };
        self.persist_changes(vec![change.clone()])?;
        self.artifacts.remove(artifact_id);
        remember_ordered_id(
            &mut self.tombstoned_order,
            &mut self.tombstoned_ids,
            artifact_id,
            MAX_PERSISTED_IDS,
        );
        self.marker_artifacts
            .retain(|id, _| self.tombstoned_ids.contains(id));
        self.marker_artifacts
            .insert(artifact_id.into(), artifact.clone());
        self.maybe_snapshot();
        Ok(Some(change))
    }

    pub fn tombstoned_ids(&self) -> Vec<String> {
        self.tombstoned_order.iter().cloned().collect()
    }

    fn normalize(
        &self,
        input: SessionArtifactInput,
        trusted_publisher: bool,
    ) -> Result<SessionArtifact, SessionArtifactError> {
        let title = normalize_string(&input.title, "title", MAX_TITLE_CHARS, true)?;
        let description = input
            .description
            .as_deref()
            .map(|v| normalize_string(v, "description", MAX_DESCRIPTION_CHARS, false))
            .transpose()?;
        let source = input.source.unwrap_or(ArtifactSource::Tool);
        let workspace_path = input
            .workspace_path
            .as_deref()
            .map(|path| normalize_workspace_path(path, &self.workspace_cwd))
            .transpose()?;
        let managed_id = input
            .managed_id
            .as_deref()
            .map(normalize_managed_id)
            .transpose()?;
        let allow_file = trusted_publisher;
        let url = input
            .url
            .as_deref()
            .map(|url| normalize_url(url, allow_file))
            .transpose()?;
        let storage = input.storage.unwrap_or_else(|| {
            if workspace_path.is_some() {
                ArtifactStorage::Workspace
            } else if managed_id.is_some() {
                ArtifactStorage::Managed
            } else if trusted_publisher && url.is_some() {
                ArtifactStorage::Published
            } else {
                ArtifactStorage::ExternalUrl
            }
        });
        if storage == ArtifactStorage::Published {
            if !trusted_publisher || url.is_none() || workspace_path.is_some() {
                return Err(SessionArtifactError::Validation("published artifacts require trusted publisher and url, and cannot include workspacePath".into()));
            }
        } else if [
            workspace_path.is_some(),
            managed_id.is_some(),
            url.is_some(),
        ]
        .into_iter()
        .filter(|present| *present)
        .count()
            != 1
        {
            return Err(SessionArtifactError::Validation(
                "provide exactly one of workspacePath, managedId, or url".into(),
            ));
        }
        match storage {
            ArtifactStorage::Workspace if workspace_path.is_none() => {
                return Err(SessionArtifactError::Validation(
                    "workspace storage requires workspacePath".into(),
                ));
            }
            ArtifactStorage::Managed if managed_id.is_none() => {
                return Err(SessionArtifactError::Validation(
                    "managed storage requires managedId".into(),
                ));
            }
            ArtifactStorage::ExternalUrl if url.is_none() => {
                return Err(SessionArtifactError::Validation(
                    "external_url storage requires url".into(),
                ));
            }
            _ => {}
        }
        if let Some(metadata) = &input.metadata {
            let bytes = serde_json::to_vec(metadata).map_or(usize::MAX, |value| value.len());
            if bytes > MAX_METADATA_BYTES {
                return Err(SessionArtifactError::Validation(
                    "metadata exceeds user metadata budget".into(),
                ));
            }
            if metadata
                .keys()
                .any(|key| key.starts_with("__proto__") || key.starts_with("canopy."))
            {
                return Err(SessionArtifactError::Validation(
                    "metadata includes reserved key".into(),
                ));
            }
        }
        let identity = if let Some(path) = &workspace_path {
            format!("workspace:{path}")
        } else if let Some(id) = &managed_id {
            format!("managed:{id}")
        } else {
            format!("url:{}", url.as_deref().unwrap_or_default())
        };
        let id = stable_session_artifact_id(&self.session_id, &identity);
        let retention =
            input
                .retention
                .unwrap_or(if self.persistence.is_some() || self.external_persistence {
                    ArtifactRetention::Restorable
                } else {
                    ArtifactRetention::Ephemeral
                });
        if retention == ArtifactRetention::Pinned {
            return Err(SessionArtifactError::Validation(
                "pinned retention is not supported by session_artifacts_persistence".into(),
            ));
        }
        let now = now_iso();
        let kind = input
            .kind
            .unwrap_or_else(|| infer_kind(workspace_path.as_deref(), url.as_deref(), storage));
        let artifact = SessionArtifact {
            id,
            kind,
            storage,
            source,
            status: workspace_path
                .as_deref()
                .map(|path| workspace_file_status(&self.workspace_cwd.join(path)))
                .unwrap_or(ArtifactStatus::Available),
            title,
            description,
            workspace_path,
            managed_id,
            url,
            mime_type: input
                .mime_type
                .as_deref()
                .map(|v| normalize_string(v, "mimeType", 120, false))
                .transpose()?,
            size_bytes: input.size_bytes,
            metadata: input.metadata,
            retention,
            restore_state: Some(ArtifactRestoreState::Live),
            persistence_warning: if retention != ArtifactRetention::Ephemeral
                && self.persistence.is_none()
                && !self.external_persistence
            {
                Some(ArtifactPersistenceWarning::PersistenceUnavailable)
            } else {
                None
            },
            persisted_at: None,
            client_retained: source == ArtifactSource::Client
                && input.client_retained.unwrap_or(true),
            created_at: now.clone(),
            updated_at: now,
            tool_call_id: input.tool_call_id,
            tool_name: input.tool_name,
            hook_event_name: input.hook_event_name,
            client_id: input.client_id,
        };
        Ok(artifact)
    }

    fn evict_one(&mut self) -> Result<(), SessionArtifactError> {
        if self.artifacts.is_empty() {
            return Err(SessionArtifactError::Limit(self.max_artifacts));
        }
        let source_counts = self.artifacts.values().fold(
            HashMap::<ArtifactSource, usize>::new(),
            |mut counts, item| {
                *counts.entry(item.artifact.source).or_default() += 1;
                counts
            },
        );
        let mut ids: Vec<_> = self.artifacts.values().collect();
        ids.sort_by_key(|item| item.insert_seq);
        let candidate = ids
            .iter()
            .find(|item| {
                item.artifact.status == ArtifactStatus::Missing && !item.artifact.client_retained
            })
            .or_else(|| {
                ids.iter().find(|item| {
                    !item.artifact.client_retained
                        && source_counts
                            .get(&item.artifact.source)
                            .copied()
                            .unwrap_or(0)
                            > reservation(item.artifact.source)
                })
            })
            .or_else(|| ids.iter().find(|item| !item.artifact.client_retained))
            .or_else(|| ids.first())
            .ok_or(SessionArtifactError::Limit(self.max_artifacts))?;
        let id = candidate.artifact.id.clone();
        let change = SessionArtifactChange {
            action: "removed".into(),
            artifact_id: id.clone(),
            artifact: Some(candidate.artifact.clone()),
            reason: Some("eviction".into()),
        };
        self.persist_changes(vec![change])?;
        self.artifacts.remove(&id);
        self.maybe_snapshot();
        Ok(())
    }

    fn persist_changes(
        &mut self,
        changes: Vec<SessionArtifactChange>,
    ) -> Result<(), SessionArtifactError> {
        let durable: Vec<_> = changes
            .into_iter()
            .filter(|change| {
                change
                    .artifact
                    .as_ref()
                    .is_none_or(|artifact| artifact.retention != ArtifactRetention::Ephemeral)
                    || change.reason.as_deref() == Some("explicit")
            })
            .collect();
        if durable.is_empty() {
            return Ok(());
        }
        let Some(persistence) = &self.persistence else {
            return Ok(());
        };
        let next = self.sequence + 1;
        let payload = SessionArtifactEventRecordPayload {
            v: SESSION_ARTIFACT_PERSISTENCE_VERSION,
            session_id: self.session_id.clone(),
            sequence: next,
            recorded_at: now_iso(),
            changes: durable,
        };
        persistence.record_event(&payload)?;
        self.sequence = next;
        self.durable_events_since_snapshot = self.durable_events_since_snapshot.saturating_add(1);
        Ok(())
    }

    fn maybe_snapshot(&mut self) {
        let backoff = 2u64
            .saturating_pow(self.consecutive_snapshot_failures)
            .min(MAX_SNAPSHOT_BACKOFF_MULTIPLIER);
        let threshold = SESSION_ARTIFACT_SNAPSHOT_INTERVAL.saturating_mul(backoff);
        if self.durable_events_since_snapshot < threshold {
            return;
        }
        let Some(persistence) = &self.persistence else {
            return;
        };
        let next_sequence = self.sequence.saturating_add(1);
        let snapshot = self.snapshot_payload(next_sequence);
        match persistence.record_snapshot(&snapshot) {
            Ok(()) => {
                self.sequence = next_sequence;
                self.durable_events_since_snapshot = 0;
                self.consecutive_snapshot_failures = 0;
            }
            Err(_) => {
                self.consecutive_snapshot_failures =
                    self.consecutive_snapshot_failures.saturating_add(1).min(2);
            }
        }
    }
}

pub fn stable_session_artifact_id(session_id: &str, identity_key: &str) -> String {
    let digest = Sha256::digest(format!("{session_id}:{identity_key}").as_bytes());
    let mut output = String::with_capacity(16);
    for byte in &digest[..8] {
        output.push_str(&format!("{byte:02x}"));
    }
    output
}
pub fn session_artifact_identity_key(
    workspace_path: Option<&str>,
    managed_id: Option<&str>,
    url: Option<&str>,
) -> Option<String> {
    workspace_path
        .map(|v| format!("workspace:{v}"))
        .or_else(|| managed_id.map(|v| format!("managed:{v}")))
        .or_else(|| url.map(|v| format!("url:{v}")))
}
pub fn is_artifact_restore_failure_warning(warning: &str) -> bool {
    warning.starts_with("artifact snapshot restore failed")
        || warning.starts_with("artifact snapshot restore partially failed")
}
fn is_artifact_snapshot_completeness_warning(warning: &str) -> bool {
    if warning.starts_with("skipped stale event sequence ") {
        return false;
    }
    warning.starts_with("skipped ") || warning.contains(" list truncated to ")
}
pub fn public_artifacts_equal(a: &SessionArtifact, b: &SessionArtifact) -> bool {
    serde_json::to_value(a).ok() == serde_json::to_value(b).ok()
}

pub fn rebuild_session_artifact_snapshot(
    records: &[Value],
    fallback_session_id: Option<&str>,
) -> Option<SessionArtifactSnapshotRecordPayload> {
    rebuild_session_artifact_snapshot_with_warnings(records, fallback_session_id)
        .map(|rebuilt| rebuilt.snapshot)
}

pub fn rebuild_session_artifact_snapshot_with_warnings(
    records: &[Value],
    fallback_session_id: Option<&str>,
) -> Option<RebuiltSessionArtifactSnapshot> {
    let mut session_id = fallback_session_id.map(str::to_owned);
    let mut sequence = 0u64;
    let mut artifacts = HashMap::<String, SessionArtifact>::new();
    let mut tombstoned_ids = HashSet::<String>::new();
    let mut tombstoned_order = VecDeque::<String>::new();
    let mut sticky_ephemeral_ids = HashSet::<String>::new();
    let mut sticky_ephemeral_order = VecDeque::<String>::new();
    let mut marker_artifacts = HashMap::<String, SessionArtifact>::new();
    let mut last_snapshot_sequence = 0u64;
    let mut warnings = Vec::<String>::new();
    let mut saw = false;
    for record in records {
        if record.get("type").and_then(Value::as_str) != Some("system") {
            continue;
        }
        let subtype = record.get("subtype").and_then(Value::as_str);
        if subtype != Some("session_artifact_event") && subtype != Some("session_artifact_snapshot")
        {
            continue;
        }
        let Some(payload) = record.get("systemPayload") else {
            warnings.push("skipped malformed artifact persistence record".into());
            continue;
        };
        if payload.get("v").and_then(Value::as_u64)
            != Some(SESSION_ARTIFACT_PERSISTENCE_VERSION as u64)
        {
            warnings.push(format!(
                "skipped v{} artifact persistence record (expected v{})",
                payload
                    .get("v")
                    .map_or_else(|| "undefined".into(), Value::to_string),
                SESSION_ARTIFACT_PERSISTENCE_VERSION
            ));
            continue;
        }
        let Some(sid) = payload
            .get("sessionId")
            .and_then(Value::as_str)
            .filter(|sid| sid.chars().count() <= 200)
        else {
            warnings.push("skipped artifact persistence record without sessionId".into());
            continue;
        };
        let seq = payload.get("sequence").and_then(Value::as_u64).unwrap_or(0);
        if subtype == Some("session_artifact_snapshot") {
            if !payload.get("artifacts").is_some_and(Value::is_array) {
                warnings.push("skipped snapshot record without artifacts array".into());
                continue;
            }
            if payload
                .get("artifacts")
                .and_then(Value::as_array)
                .is_some_and(|items| items.len() > MAX_PERSISTED_ARTIFACTS)
            {
                warnings.push(format!(
                    "snapshot artifact list truncated to {MAX_PERSISTED_ARTIFACTS}"
                ));
            }
            if payload
                .get("markerArtifacts")
                .and_then(Value::as_array)
                .is_some_and(|items| items.len() > MAX_PERSISTED_MARKER_ARTIFACTS)
            {
                warnings.push(format!(
                    "snapshot marker artifact list truncated to {MAX_PERSISTED_MARKER_ARTIFACTS}"
                ));
            }
            saw = true;
            session_id = Some(sid.to_owned());
            sequence = sequence.max(seq);
            last_snapshot_sequence = seq;
            artifacts.clear();
            tombstoned_ids.clear();
            tombstoned_order.clear();
            sticky_ephemeral_ids.clear();
            sticky_ephemeral_order.clear();
            marker_artifacts.clear();
            for id in payload
                .get("tombstonedIds")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .filter(|id| id.chars().count() <= 200)
                .take(MAX_PERSISTED_IDS)
            {
                remember_ordered_id(
                    &mut tombstoned_order,
                    &mut tombstoned_ids,
                    id,
                    MAX_PERSISTED_IDS,
                );
            }
            for id in payload
                .get("stickyEphemeralIds")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .filter(|id| id.chars().count() <= 200)
                .take(MAX_PERSISTED_IDS)
            {
                remember_ordered_id(
                    &mut sticky_ephemeral_order,
                    &mut sticky_ephemeral_ids,
                    id,
                    MAX_PERSISTED_IDS,
                );
            }
            let marker_ids: HashSet<_> = tombstoned_ids
                .iter()
                .chain(sticky_ephemeral_ids.iter())
                .cloned()
                .collect();
            for value in payload
                .get("markerArtifacts")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .take(MAX_PERSISTED_MARKER_ARTIFACTS)
            {
                if let Some(artifact) = normalize_persisted_artifact(value, &mut warnings) {
                    if marker_ids.contains(&artifact.id) {
                        marker_artifacts.insert(artifact.id.clone(), artifact);
                    }
                }
            }
            for value in payload
                .get("artifacts")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .take(MAX_PERSISTED_ARTIFACTS)
            {
                if let Some(artifact) = normalize_persisted_artifact(value, &mut warnings) {
                    if artifact.retention != ArtifactRetention::Ephemeral {
                        artifacts.insert(artifact.id.clone(), artifact);
                    }
                }
            }
            continue;
        }
        let Some(changes) = payload.get("changes").and_then(Value::as_array) else {
            warnings.push("skipped event record without changes array".into());
            continue;
        };
        if changes.len() > MAX_PERSISTED_EVENT_CHANGES {
            warnings.push(format!(
                "event change list truncated to {MAX_PERSISTED_EVENT_CHANGES}"
            ));
        }
        saw = true;
        session_id = Some(sid.to_owned());
        if seq <= last_snapshot_sequence {
            warnings.push(format!(
                "skipped stale event sequence {seq} at or before snapshot sequence {last_snapshot_sequence}"
            ));
            continue;
        }
        sequence = sequence.max(seq);
        for change in changes.iter().take(MAX_PERSISTED_EVENT_CHANGES) {
            if !change.is_object() {
                warnings.push("skipped malformed artifact change".into());
                continue;
            }
            let action = change
                .get("action")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if !["created", "updated", "removed"].contains(&action) {
                warnings.push("skipped artifact change with invalid action".into());
                continue;
            }
            let id = change
                .get("artifactId")
                .and_then(Value::as_str)
                .filter(|id| id.chars().count() <= 200)
                .or_else(|| {
                    change
                        .get("artifact")
                        .and_then(Value::as_object)
                        .and_then(|artifact| artifact.get("id"))
                        .and_then(Value::as_str)
                })
                .unwrap_or_default();
            if id.is_empty() {
                warnings.push("skipped artifact change without artifactId".into());
                continue;
            }
            if action == "removed" {
                artifacts.remove(id);
                let marker = change
                    .get("artifact")
                    .and_then(|value| normalize_persisted_artifact(value, &mut warnings));
                match change.get("reason").and_then(Value::as_str) {
                    Some("explicit") => {
                        remember_ordered_id(
                            &mut tombstoned_order,
                            &mut tombstoned_ids,
                            id,
                            MAX_PERSISTED_IDS,
                        );
                        forget_ordered_id(
                            &mut sticky_ephemeral_order,
                            &mut sticky_ephemeral_ids,
                            id,
                        );
                        if let Some(artifact) = marker {
                            marker_artifacts.insert(id.into(), artifact);
                        }
                    }
                    Some("eviction") => {
                        forget_ordered_id(
                            &mut sticky_ephemeral_order,
                            &mut sticky_ephemeral_ids,
                            id,
                        );
                        marker_artifacts.remove(id);
                    }
                    Some("unpin_to_ephemeral") => {
                        remember_ordered_id(
                            &mut sticky_ephemeral_order,
                            &mut sticky_ephemeral_ids,
                            id,
                            MAX_PERSISTED_IDS,
                        );
                        if let Some(artifact) = marker {
                            marker_artifacts.insert(id.into(), artifact);
                        }
                    }
                    _ => {}
                }
            } else if let Some(value) = change.get("artifact") {
                if let Some(artifact) = normalize_persisted_artifact(value, &mut warnings) {
                    if artifact.retention != ArtifactRetention::Ephemeral {
                        forget_ordered_id(&mut tombstoned_order, &mut tombstoned_ids, &artifact.id);
                        forget_ordered_id(
                            &mut sticky_ephemeral_order,
                            &mut sticky_ephemeral_ids,
                            &artifact.id,
                        );
                        marker_artifacts.remove(&artifact.id);
                        artifacts.insert(artifact.id.clone(), artifact);
                    }
                }
            }
        }
    }
    if !saw {
        return None;
    }
    let marker_ids: HashSet<_> = tombstoned_ids
        .iter()
        .chain(sticky_ephemeral_ids.iter())
        .cloned()
        .collect();
    marker_artifacts.retain(|id, _| marker_ids.contains(id));
    Some(RebuiltSessionArtifactSnapshot {
        snapshot: SessionArtifactSnapshotRecordPayload {
            v: SESSION_ARTIFACT_PERSISTENCE_VERSION,
            session_id: session_id?,
            sequence,
            recorded_at: now_iso(),
            artifacts: sorted_artifacts(artifacts),
            tombstoned_ids: tombstoned_order.into_iter().collect(),
            sticky_ephemeral_ids: sticky_ephemeral_order.into_iter().collect(),
            marker_artifacts: sorted_artifacts(marker_artifacts),
        },
        warnings,
    })
}

fn normalize_persisted_artifact(
    value: &Value,
    warnings: &mut Vec<String>,
) -> Option<SessionArtifact> {
    let object = value.as_object()?;
    let id = object.get("id").and_then(Value::as_str);
    let title = object.get("title").and_then(Value::as_str);
    if id.is_none_or(|id| id.chars().count() > 200)
        || title.is_none_or(|title| title.chars().count() > MAX_TITLE_CHARS)
    {
        warnings.push("skipped artifact without id/title".into());
        return None;
    }
    match serde_json::from_value::<SessionArtifact>(value.clone()) {
        Ok(artifact) => Some(artifact),
        Err(_) => {
            warnings.push(format!(
                "skipped malformed artifact {}",
                id.unwrap_or_default()
            ));
            None
        }
    }
}

fn remember_ordered_id(
    order: &mut VecDeque<String>,
    ids: &mut HashSet<String>,
    id: &str,
    limit: usize,
) {
    forget_ordered_id(order, ids, id);
    order.push_back(id.to_owned());
    ids.insert(id.to_owned());
    while order.len() > limit {
        if let Some(expired) = order.pop_front() {
            ids.remove(&expired);
        }
    }
}

fn forget_ordered_id(order: &mut VecDeque<String>, ids: &mut HashSet<String>, id: &str) {
    order.retain(|candidate| candidate != id);
    ids.remove(id);
}

fn sorted_artifacts(artifacts: HashMap<String, SessionArtifact>) -> Vec<SessionArtifact> {
    let mut artifacts: Vec<_> = artifacts.into_values().collect();
    artifacts.sort_by(|a, b| a.id.cmp(&b.id));
    artifacts
}

fn ensure_owner(
    session_id: &str,
    artifact: &SessionArtifact,
    requester: Option<&str>,
) -> Result<(), SessionArtifactError> {
    if artifact.source == ArtifactSource::Client && artifact.client_id.as_deref() != requester {
        return Err(SessionArtifactError::Forbidden {
            session_id: session_id.into(),
            artifact_id: artifact.id.clone(),
            owner_client_id: artifact.client_id.clone().unwrap_or_default(),
            requester_client_id: requester.map(str::to_owned),
        });
    }
    Ok(())
}
fn normalize_string(
    value: &str,
    field: &str,
    max: usize,
    required: bool,
) -> Result<String, SessionArtifactError> {
    let trimmed = value.trim();
    if (required && trimmed.is_empty())
        || trimmed.chars().count() > max
        || trimmed.chars().any(char::is_control)
        || is_unsafe_display(trimmed)
    {
        return Err(SessionArtifactError::Validation(format!(
            "{field} is invalid"
        )));
    }
    Ok(trimmed.to_owned())
}
fn is_unsafe_display(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    lower.contains("<script")
        || lower.contains("javascript:")
        || lower.contains("data:text/html")
        || lower.contains("onerror=")
        || lower.contains("onclick=")
}
fn normalize_managed_id(value: &str) -> Result<String, SessionArtifactError> {
    let value = normalize_string(value, "managedId", 200, true)?;
    if value.contains('/')
        || value.contains('\\')
        || value.contains("..")
        || Path::new(&value).is_absolute()
    {
        return Err(SessionArtifactError::Validation(
            "managedId must be an opaque managed resource id".into(),
        ));
    }
    Ok(value)
}
fn normalize_workspace_path(value: &str, workspace: &Path) -> Result<String, SessionArtifactError> {
    let value = normalize_string(value, "workspacePath", MAX_WORKSPACE_PATH_CHARS, true)?;
    let path = Path::new(&value);
    if path.is_absolute() {
        return Err(SessionArtifactError::Validation(
            "workspacePath must be relative to the workspace".into(),
        ));
    }
    let mut parts = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => parts.push(part.to_string_lossy().to_string()),
            Component::CurDir => {}
            _ => {
                return Err(SessionArtifactError::Validation(
                    "workspacePath must stay inside the workspace".into(),
                ));
            }
        }
    }
    if parts.is_empty() {
        return Err(SessionArtifactError::Validation(
            "workspacePath must stay inside the workspace".into(),
        ));
    }
    let joined = workspace.join(&value);
    let workspace_real =
        std::fs::canonicalize(workspace).unwrap_or_else(|_| workspace.to_path_buf());
    if let Ok(real) = std::fs::canonicalize(&joined) {
        if !real.starts_with(&workspace_real) {
            return Err(SessionArtifactError::Validation(
                "workspacePath must stay inside the workspace".into(),
            ));
        }
        return Ok(real
            .strip_prefix(&workspace_real)
            .unwrap_or(&real)
            .components()
            .map(|c| c.as_os_str().to_string_lossy())
            .collect::<Vec<_>>()
            .join("/"));
    }
    Ok(parts.join("/"))
}
fn normalize_url(value: &str, allow_file: bool) -> Result<String, SessionArtifactError> {
    let value = normalize_string(value, "url", MAX_URL_CHARS, true)?;
    let parsed = reqwest::Url::parse(&value)
        .map_err(|_| SessionArtifactError::Validation("url must be valid".into()))?;
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(SessionArtifactError::Validation(
            "url must not include credentials".into(),
        ));
    }
    if !["http", "https"].contains(&parsed.scheme()) && !(allow_file && parsed.scheme() == "file") {
        return Err(SessionArtifactError::Validation(
            "url must use http or https".into(),
        ));
    }
    let lower = value.to_ascii_lowercase();
    if lower.contains("token=") || lower.contains("api_key=") || lower.contains("secret=") {
        return Err(SessionArtifactError::Validation(
            "url must not include secret-like components".into(),
        ));
    }
    Ok(parsed.to_string())
}
fn infer_kind(path: Option<&str>, url: Option<&str>, storage: ArtifactStorage) -> ArtifactKind {
    if storage == ArtifactStorage::Published {
        return ArtifactKind::Html;
    }
    if url.is_some() {
        return ArtifactKind::Link;
    }
    match Path::new(path.unwrap_or_default())
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "html" | "htm" => ArtifactKind::Html,
        "png" | "jpg" | "jpeg" | "gif" | "webp" | "svg" => ArtifactKind::Image,
        "mp4" | "mov" | "webm" => ArtifactKind::Video,
        "mp3" | "wav" | "m4a" | "ogg" => ArtifactKind::Audio,
        "pdf" => ArtifactKind::Pdf,
        "ipynb" => ArtifactKind::Notebook,
        _ if path.is_some() => ArtifactKind::File,
        _ => ArtifactKind::Other,
    }
}
fn workspace_file_status(path: &Path) -> ArtifactStatus {
    if path.exists() {
        ArtifactStatus::Available
    } else {
        ArtifactStatus::Missing
    }
}

fn sanitize_persisted_metadata(
    metadata: Option<&Map<String, Value>>,
) -> Option<Map<String, Value>> {
    let metadata = metadata?;
    let mut normalized = Map::new();
    for (key, value) in metadata {
        if key.len() > 120
            || matches!(key.as_str(), "__proto__" | "constructor" | "prototype")
            || contains_control_character(key)
            || is_unsafe_display(key)
        {
            continue;
        }
        let valid_workspace_metadata = match key.as_str() {
            "canopy.workspace.sha256" => value.as_str().is_some_and(|digest| {
                digest.len() == 64
                    && digest
                        .bytes()
                        .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
            }),
            "canopy.workspace.mtimeMs" => value.as_f64().is_some_and(f64::is_finite),
            _ => false,
        };
        if key.starts_with("canopy.") && !valid_workspace_metadata {
            continue;
        }
        let scalar =
            value.is_null() || value.is_boolean() || value.is_number() || value.is_string();
        if !scalar {
            continue;
        }
        if value
            .as_str()
            .is_some_and(|text| contains_control_character(text) || is_unsafe_display(text))
        {
            continue;
        }
        normalized.insert(key.clone(), value.clone());
    }
    if normalized.is_empty() {
        return None;
    }
    let user_metadata: Map<String, Value> = normalized
        .iter()
        .filter(|(key, _)| {
            !matches!(
                key.as_str(),
                "canopy.workspace.sha256" | "canopy.workspace.mtimeMs"
            )
        })
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    let encoded_len = serde_json::to_vec(&user_metadata).map_or(usize::MAX, |value| value.len());
    (encoded_len <= 4096).then_some(normalized)
}

fn persisted_metadata_exceeds_budget(metadata: Option<&Map<String, Value>>) -> bool {
    let Some(metadata) = metadata else {
        return false;
    };
    let mut user_metadata = Map::new();
    for (key, value) in metadata {
        if key.len() > 120
            || matches!(key.as_str(), "__proto__" | "constructor" | "prototype")
            || contains_control_character(key)
            || is_unsafe_display(key)
            || !(value.is_null() || value.is_boolean() || value.is_number() || value.is_string())
        {
            continue;
        }
        let valid_workspace_metadata = match key.as_str() {
            "canopy.workspace.sha256" => value.as_str().is_some_and(|digest| {
                digest.len() == 64
                    && digest
                        .bytes()
                        .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
            }),
            "canopy.workspace.mtimeMs" => value.as_f64().is_some_and(f64::is_finite),
            _ => false,
        };
        if key.starts_with("canopy.") && !valid_workspace_metadata {
            continue;
        }
        if matches!(
            key.as_str(),
            "canopy.workspace.sha256" | "canopy.workspace.mtimeMs"
        ) {
            continue;
        }
        if value
            .as_str()
            .is_some_and(|text| contains_control_character(text) || is_unsafe_display(text))
        {
            continue;
        }
        user_metadata.insert(key.clone(), value.clone());
    }
    serde_json::to_vec(&user_metadata).map_or(true, |value| value.len() > 4096)
}

fn contains_control_character(value: &str) -> bool {
    value.chars().any(|character| {
        let code = character as u32;
        code <= 0x1f
            || code == 0x7f
            || (0x200b..=0x200f).contains(&code)
            || matches!(code, 0x2028 | 0x2029)
            || (0x202a..=0x202e).contains(&code)
            || (0x2066..=0x2069).contains(&code)
            || code == 0xfeff
    })
}

fn restored_workspace_status(
    workspace: &Path,
    workspace_path: &str,
    expected_size: Option<u64>,
    metadata: Option<&Map<String, Value>>,
) -> (ArtifactStatus, Option<u64>) {
    let workspace_real =
        std::fs::canonicalize(workspace).unwrap_or_else(|_| workspace.to_path_buf());
    let Ok(real_path) = std::fs::canonicalize(workspace.join(workspace_path)) else {
        return (ArtifactStatus::Missing, None);
    };
    if !real_path.starts_with(&workspace_real) {
        return (ArtifactStatus::Missing, None);
    }
    let Ok(stat) = std::fs::metadata(&real_path) else {
        return (ArtifactStatus::Missing, None);
    };
    if !stat.is_file() {
        return (ArtifactStatus::Available, None);
    }
    let size = stat.len();
    if expected_size.is_some_and(|expected| expected != size) {
        return (ArtifactStatus::Changed, Some(size));
    }
    let expected_mtime = metadata
        .and_then(|items| items.get("canopy.workspace.mtimeMs"))
        .and_then(Value::as_f64);
    let actual_mtime = stat
        .modified()
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|duration| duration.as_secs_f64() * 1_000.0);
    let unchanged =
        expected_size == Some(size) && expected_mtime.is_some() && expected_mtime == actual_mtime;
    let has_expected_hash = metadata
        .and_then(|items| items.get("canopy.workspace.sha256"))
        .and_then(Value::as_str)
        .is_some();
    if has_expected_hash && !unchanged {
        (ArtifactStatus::Changed, Some(size))
    } else {
        (ArtifactStatus::Available, Some(size))
    }
}

fn reservation(source: ArtifactSource) -> usize {
    SOURCE_RESERVATIONS
        .iter()
        .find(|(candidate, _)| *candidate == source)
        .map_or(0, |(_, count)| *count)
}
fn now_iso() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn artifact_ids_are_stable_and_workspace_locators_are_relative() {
        let id = stable_session_artifact_id("s1", "workspace:src/main.rs");
        assert_eq!(
            id,
            stable_session_artifact_id("s1", "workspace:src/main.rs")
        );
        assert_eq!(id.len(), 16);
        assert!(normalize_workspace_path("../outside", Path::new("/tmp/project")).is_err());
    }

    #[test]
    fn store_deduplicates_identity_and_enforces_client_ownership() {
        let mut store = SessionArtifactStore::new("s1", "/tmp/project", Some(4), None);
        let input = SessionArtifactInput {
            title: "preview".into(),
            workspace_path: Some("output.html".into()),
            client_id: Some("client-a".into()),
            source: Some(ArtifactSource::Client),
            ..Default::default()
        };
        let created = store.upsert(input.clone(), false).unwrap();
        assert_eq!(created.action, "created");
        let wrong = store.remove(&created.artifact_id, Some("client-b"));
        assert!(matches!(wrong, Err(SessionArtifactError::Forbidden { .. })));
        assert_eq!(
            store
                .remove(&created.artifact_id, Some("client-a"))
                .unwrap()
                .unwrap()
                .reason
                .as_deref(),
            Some("explicit")
        );
        assert!(store.tombstoned_ids().contains(&created.artifact_id));
    }

    #[test]
    fn artifact_event_and_snapshot_records_rebuild_durable_state() {
        let artifact = SessionArtifact {
            id: "id".into(),
            kind: ArtifactKind::File,
            storage: ArtifactStorage::Managed,
            source: ArtifactSource::Tool,
            status: ArtifactStatus::Available,
            title: "x".into(),
            description: None,
            workspace_path: None,
            managed_id: Some("m1".into()),
            url: None,
            mime_type: None,
            size_bytes: None,
            metadata: None,
            retention: ArtifactRetention::Restorable,
            restore_state: None,
            persistence_warning: None,
            persisted_at: None,
            client_retained: false,
            created_at: "now".into(),
            updated_at: "now".into(),
            tool_call_id: None,
            tool_name: None,
            hook_event_name: None,
            client_id: None,
        };
        let record = json!({"type":"system","subtype":"session_artifact_event","systemPayload":{"v":2,"sessionId":"s1","sequence":1,"recordedAt":"now","changes":[{"action":"created","artifactId":"id","artifact":artifact}]}});
        let snapshot = rebuild_session_artifact_snapshot(&[record], None).unwrap();
        assert_eq!(snapshot.artifacts.len(), 1);
        assert_eq!(snapshot.session_id, "s1");
    }

    #[test]
    fn replay_preserves_marker_artifacts_and_reports_stale_events() {
        let marker = SessionArtifact {
            id: "marker-id".into(),
            kind: ArtifactKind::File,
            storage: ArtifactStorage::Managed,
            source: ArtifactSource::Tool,
            status: ArtifactStatus::Available,
            title: "removed artifact".into(),
            description: None,
            workspace_path: None,
            managed_id: Some("m1".into()),
            url: None,
            mime_type: None,
            size_bytes: None,
            metadata: None,
            retention: ArtifactRetention::Restorable,
            restore_state: None,
            persistence_warning: None,
            persisted_at: None,
            client_retained: false,
            created_at: "now".into(),
            updated_at: "now".into(),
            tool_call_id: None,
            tool_name: None,
            hook_event_name: None,
            client_id: None,
        };
        let snapshot = json!({
            "type": "system",
            "subtype": "session_artifact_snapshot",
            "systemPayload": {
                "v": 2,
                "sessionId": "s1",
                "sequence": 10,
                "recordedAt": "now",
                "artifacts": [],
                "tombstonedIds": ["marker-id"],
                "stickyEphemeralIds": [],
                "markerArtifacts": [marker]
            }
        });
        let stale_event = json!({
            "type": "system",
            "subtype": "session_artifact_event",
            "systemPayload": {
                "v": 2,
                "sessionId": "s1",
                "sequence": 10,
                "recordedAt": "now",
                "changes": []
            }
        });
        let rebuilt =
            rebuild_session_artifact_snapshot_with_warnings(&[snapshot, stale_event], None)
                .unwrap();

        assert_eq!(rebuilt.snapshot.sequence, 10);
        assert_eq!(rebuilt.snapshot.tombstoned_ids, vec!["marker-id"]);
        assert_eq!(rebuilt.snapshot.marker_artifacts[0].id, "marker-id");
        assert!(rebuilt.warnings[0].contains("skipped stale event sequence 10"));
    }

    #[test]
    fn artifact_replay_ignores_non_system_records() {
        let record = json!({
            "type": "assistant",
            "subtype": "session_artifact_event",
            "systemPayload": {
                "v": 2,
                "sessionId": "s1",
                "sequence": 1,
                "changes": []
            }
        });
        assert!(rebuild_session_artifact_snapshot(&[record], Some("s1")).is_none());
    }
}
