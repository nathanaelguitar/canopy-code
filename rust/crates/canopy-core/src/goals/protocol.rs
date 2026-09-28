use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

pub const GOAL_STATE_VERSION: u8 = 2;
pub const GOAL_PROPOSAL_REASON_MAX_CHARACTERS: usize = 8_000;
pub const GOAL_PROPOSAL_REASON_MAX_BYTES: usize = 16_000;
pub const GOAL_CHECKPOINT_CLAIM_LIMIT: usize = 32;
pub const GOAL_CHECKPOINT_CLAIM_MAX_CHARACTERS: usize = 2_000;
pub const GOAL_CHECKPOINT_CLAIM_MAX_BYTES: usize = 16_000;
pub const GOAL_CHECKPOINT_SOURCE_REFERENCE_LIMIT: usize = 32;
pub const GOAL_EVIDENCE_CATALOG_EXHAUSTED_REASON: &str = "The current Goal revision exceeded the bounded evidence catalog. Automatic retries cannot recover. Edit or replace the Goal before resuming it.";
pub const GOAL_CHECKPOINT_REQUEST_TOO_LARGE_REASON: &str = "The current Goal revision exceeded the checkpoint verifier request limit. Automatic retries cannot recover. Edit or replace the Goal before resuming it.";
pub const PAUSED_GOAL_SYSTEM_REMINDER: &str = "<system-reminder>\nThe Goal is paused. Do not continue its objective unless the user resumes it. Treat this message as ordinary conversation.\n</system-reminder>";

fn deserialize_non_null_optional<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: DeserializeOwned,
{
    let value = Value::deserialize(deserializer)?;
    if value.is_null() {
        return Err(serde::de::Error::custom(
            "optional Goal fields must be omitted instead of null",
        ));
    }
    T::deserialize(value)
        .map(Some)
        .map_err(serde::de::Error::custom)
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalLimitKind {
    EvidenceCatalog,
    CheckpointRequest,
}

impl GoalLimitKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::EvidenceCatalog => "evidence_catalog",
            Self::CheckpointRequest => "checkpoint_request",
        }
    }
}

pub fn is_goal_limit_kind(value: &Value) -> bool {
    matches!(
        value.as_str(),
        Some("evidence_catalog" | "checkpoint_request")
    )
}

pub fn goal_limit_kind_for_reason(reason: &str) -> Option<GoalLimitKind> {
    match reason {
        GOAL_EVIDENCE_CATALOG_EXHAUSTED_REASON => Some(GoalLimitKind::EvidenceCatalog),
        GOAL_CHECKPOINT_REQUEST_TOO_LARGE_REASON => Some(GoalLimitKind::CheckpointRequest),
        _ => None,
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalStatus {
    Active,
    Paused,
    Blocked,
    UsageLimited,
    Complete,
}

impl GoalStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Paused => "paused",
            Self::Blocked => "blocked",
            Self::UsageLimited => "usage_limited",
            Self::Complete => "complete",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalActivity {
    Idle,
    Running,
    Verifying,
}

impl GoalActivity {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Running => "running",
            Self::Verifying => "verifying",
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TranscriptCursor {
    pub record_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GoalExpectedVersion {
    pub goal_id: String,
    pub revision: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GoalTurnPermit {
    pub goal_id: String,
    pub revision: u64,
    pub turn_id: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalEvidenceProofKind {
    UserInput,
    DeliveredOutput,
    ExternalFact,
}

impl GoalEvidenceProofKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UserInput => "user_input",
            Self::DeliveredOutput => "delivered_output",
            Self::ExternalFact => "external_fact",
        }
    }
}

pub fn is_goal_evidence_proof_kind(value: &Value) -> bool {
    matches!(
        value.as_str(),
        Some("user_input" | "delivered_output" | "external_fact")
    )
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GoalEvidenceCheckpointClaim {
    pub id: String,
    pub proof_kind: GoalEvidenceProofKind,
    pub claim: String,
    pub source_refs: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GoalEvidenceCheckpoint {
    pub checkpoint_id: String,
    pub created_at: f64,
    pub claims: Vec<GoalEvidenceCheckpointClaim>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GoalRecord {
    pub goal_id: String,
    pub revision: u64,
    pub objective: String,
    pub status: GoalStatus,
    pub evidence_cursor: TranscriptCursor,
    pub turn_count: u64,
    pub active_time_ms: f64,
    pub created_at: f64,
    pub updated_at: f64,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub evidence_checkpoint: Option<GoalEvidenceCheckpoint>,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub last_reason: Option<String>,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub limit_kind: Option<GoalLimitKind>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GoalSnapshotV2 {
    pub v: u8,
    pub goal: Option<GoalRecord>,
    pub activity: GoalActivity,
}

pub fn empty_goal_snapshot() -> GoalSnapshotV2 {
    GoalSnapshotV2 {
        v: GOAL_STATE_VERSION,
        goal: None,
        activity: GoalActivity::Idle,
    }
}

pub fn goal_requires_exact_permit(snapshot: &GoalSnapshotV2) -> bool {
    snapshot.goal.as_ref().is_some_and(|goal| {
        goal.status == GoalStatus::Active || snapshot.activity == GoalActivity::Running
    })
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "action", deny_unknown_fields, rename_all = "lowercase")]
pub enum GoalControlRequest {
    Create {
        objective: String,
    },
    Replace {
        objective: String,
        #[serde(rename = "expectedGoalId")]
        expected_goal_id: String,
        #[serde(rename = "expectedRevision")]
        expected_revision: u64,
    },
    Edit {
        objective: String,
        #[serde(rename = "expectedGoalId")]
        expected_goal_id: String,
        #[serde(rename = "expectedRevision")]
        expected_revision: u64,
    },
    Pause {
        #[serde(rename = "expectedGoalId")]
        expected_goal_id: String,
        #[serde(rename = "expectedRevision")]
        expected_revision: u64,
    },
    Resume {
        #[serde(rename = "expectedGoalId")]
        expected_goal_id: String,
        #[serde(rename = "expectedRevision")]
        expected_revision: u64,
    },
    Clear {
        #[serde(rename = "expectedGoalId")]
        expected_goal_id: String,
        #[serde(rename = "expectedRevision")]
        expected_revision: u64,
    },
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GoalStateResponse {
    pub snapshot: GoalSnapshotV2,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalBlockerKind {
    Authority,
    External,
    Repeated,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GoalTerminalProposal {
    pub status: GoalTerminalProposalStatus,
    pub reason: String,
    pub evidence_refs: Vec<String>,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub blocker_kind: Option<GoalBlockerKind>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum GoalTerminalProposalStatus {
    Complete,
    Blocked,
}

pub fn is_repeated_blocker_proposal(proposal: &GoalTerminalProposal) -> bool {
    proposal.status == GoalTerminalProposalStatus::Blocked
        && !matches!(
            proposal.blocker_kind,
            Some(GoalBlockerKind::Authority | GoalBlockerKind::External)
        )
}

pub fn validate_goal_proposal_reason(reason: &str) -> Option<String> {
    if reason.trim().is_empty() {
        return Some("Goal proposal reason must not be empty".to_owned());
    }
    if reason.chars().count() > GOAL_PROPOSAL_REASON_MAX_CHARACTERS {
        return Some(format!(
            "Goal proposal reason exceeds {GOAL_PROPOSAL_REASON_MAX_CHARACTERS} characters"
        ));
    }
    if reason.len() > GOAL_PROPOSAL_REASON_MAX_BYTES {
        return Some(format!(
            "Goal proposal reason exceeds {GOAL_PROPOSAL_REASON_MAX_BYTES} UTF-8 bytes"
        ));
    }
    None
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalStateCause {
    Create,
    Replace,
    Edit,
    Pause,
    Resume,
    TurnFinished,
    Checkpoint,
    VerifierAccept,
    VerifierReject,
    Complete,
    Blocked,
    UsageLimited,
    Clear,
    Migrated,
}

impl GoalStateCause {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Replace => "replace",
            Self::Edit => "edit",
            Self::Pause => "pause",
            Self::Resume => "resume",
            Self::TurnFinished => "turn_finished",
            Self::Checkpoint => "checkpoint",
            Self::VerifierAccept => "verifier_accept",
            Self::VerifierReject => "verifier_reject",
            Self::Complete => "complete",
            Self::Blocked => "blocked",
            Self::UsageLimited => "usage_limited",
            Self::Clear => "clear",
            Self::Migrated => "migrated",
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GoalStateRecordPayloadV2 {
    pub v: u8,
    pub cause: GoalStateCause,
    pub snapshot: GoalSnapshotV2,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub checkpoint_pending: Option<GoalCheckpointPending>,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub blocked_audit: Option<GoalBlockedAudit>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GoalCheckpointPending {
    pub permit: GoalTurnPermit,
    pub record_uuid: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GoalBlockedAudit {
    pub fingerprint: String,
    pub count: u8,
    pub turn_ids: Vec<String>,
}
