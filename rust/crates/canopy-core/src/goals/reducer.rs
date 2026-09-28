use std::collections::HashSet;
use std::fmt;

use serde_json::{Map, Value};

use super::protocol::{
    GOAL_CHECKPOINT_CLAIM_LIMIT, GOAL_CHECKPOINT_CLAIM_MAX_BYTES,
    GOAL_CHECKPOINT_CLAIM_MAX_CHARACTERS, GOAL_CHECKPOINT_SOURCE_REFERENCE_LIMIT,
    GOAL_STATE_VERSION, GoalActivity, GoalBlockedAudit, GoalCheckpointPending, GoalControlRequest,
    GoalEvidenceCheckpoint, GoalEvidenceCheckpointClaim, GoalEvidenceProofKind, GoalLimitKind,
    GoalRecord, GoalSnapshotV2, GoalStateCause, GoalStateRecordPayloadV2, GoalStatus,
    GoalTurnPermit, TranscriptCursor, goal_limit_kind_for_reason, is_goal_evidence_proof_kind,
    is_goal_limit_kind,
};

const MAX_BLOCKED_AUDIT_COUNT: u64 = 3;

#[derive(Clone, Debug, PartialEq)]
pub struct GoalConflictError {
    pub current: Box<GoalSnapshotV2>,
}

impl fmt::Display for GoalConflictError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Goal version does not match the current session Goal")
    }
}

impl std::error::Error for GoalConflictError {}

#[derive(Clone, Debug, PartialEq)]
pub struct GoalInvalidTransitionError {
    pub message: String,
    pub current: Box<GoalSnapshotV2>,
}

impl fmt::Display for GoalInvalidTransitionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for GoalInvalidTransitionError {}

#[derive(Clone, Debug, PartialEq)]
pub enum GoalReducerError {
    Conflict(GoalConflictError),
    InvalidTransition(GoalInvalidTransitionError),
}

impl fmt::Display for GoalReducerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Conflict(error) => error.fmt(formatter),
            Self::InvalidTransition(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for GoalReducerError {}

impl From<GoalConflictError> for GoalReducerError {
    fn from(value: GoalConflictError) -> Self {
        Self::Conflict(value)
    }
}

impl From<GoalInvalidTransitionError> for GoalReducerError {
    fn from(value: GoalInvalidTransitionError) -> Self {
        Self::InvalidTransition(value)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct GoalControlTransition {
    pub request: GoalControlRequest,
    pub now: f64,
    pub next_goal_id: String,
    pub cursor: TranscriptCursor,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GoalTurnFinishedTransition {
    pub now: f64,
    pub last_reason: Option<String>,
}

pub fn elapsed_active_time(goal: &GoalRecord, now: f64) -> f64 {
    goal.active_time_ms
        + if goal.status == GoalStatus::Active {
            let elapsed = now - goal.updated_at;
            if elapsed < 0.0 { 0.0 } else { elapsed }
        } else {
            0.0
        }
}

pub fn reduce_goal_control(
    current: Option<&GoalRecord>,
    transition: &GoalControlTransition,
) -> Result<Option<GoalRecord>, GoalReducerError> {
    let request = &transition.request;
    if let GoalControlRequest::Create { objective } = request {
        if let Some(current) = current {
            return Err(GoalConflictError {
                current: Box::new(snapshot_of(Some(current))),
            }
            .into());
        }
        let objective = normalize_objective(objective, snapshot_of(None))?;
        return Ok(Some(create_goal(
            &transition.next_goal_id,
            objective,
            transition.now,
            &transition.cursor,
        )));
    }

    let (expected_goal_id, expected_revision) = expected_version(request);
    let current = assert_expected_version(current, expected_goal_id, expected_revision)?;
    if matches!(request, GoalControlRequest::Clear { .. }) {
        return Ok(None);
    }

    match request {
        GoalControlRequest::Replace { objective, .. } => {
            let objective = normalize_objective(objective, snapshot_of(Some(current)))?;
            Ok(Some(create_goal(
                &transition.next_goal_id,
                objective,
                transition.now,
                &transition.cursor,
            )))
        }
        GoalControlRequest::Edit { objective, .. } => {
            if current.status == GoalStatus::Complete {
                return Err(invalid_transition(
                    "A completed Goal cannot be edited",
                    current,
                ));
            }
            let objective = normalize_objective(objective, snapshot_of(Some(current)))?;
            let mut next = transition_goal(current, transition.now);
            next.revision = current.revision.saturating_add(1);
            next.objective = objective;
            next.evidence_cursor = copy_cursor(&transition.cursor);
            next.evidence_checkpoint = None;
            next.last_reason = None;
            next.limit_kind = None;
            Ok(Some(next))
        }
        GoalControlRequest::Pause { .. } => {
            if current.status != GoalStatus::Active {
                return Err(invalid_transition(
                    "Only an active Goal can be paused",
                    current,
                ));
            }
            let mut next = transition_goal(current, transition.now);
            next.status = GoalStatus::Paused;
            Ok(Some(next))
        }
        GoalControlRequest::Resume { .. } => {
            if current.status == GoalStatus::Complete {
                return Err(invalid_transition(
                    "A completed Goal cannot be resumed",
                    current,
                ));
            }
            if current.status == GoalStatus::Active {
                return Err(invalid_transition(
                    "An active Goal cannot be resumed",
                    current,
                ));
            }
            if current.status == GoalStatus::UsageLimited && is_evidence_limited(current) {
                return Err(invalid_transition(
                    "An evidence-limited Goal cannot be resumed; edit or replace the Goal first",
                    current,
                ));
            }
            let mut next = transition_goal(current, transition.now);
            next.status = GoalStatus::Active;
            Ok(Some(next))
        }
        GoalControlRequest::Create { .. } | GoalControlRequest::Clear { .. } => {
            unreachable!("create and clear transitions were handled above")
        }
    }
}

pub fn reduce_goal_turn_finished(
    current: &GoalRecord,
    transition: &GoalTurnFinishedTransition,
) -> Result<GoalRecord, GoalInvalidTransitionError> {
    if !matches!(current.status, GoalStatus::Active | GoalStatus::Paused) {
        return Err(GoalInvalidTransitionError {
            message: "Only an active or paused Goal can finish a turn".to_owned(),
            current: Box::new(snapshot_of(Some(current))),
        });
    }
    let mut next = transition_goal(current, transition.now);
    next.turn_count = current.turn_count.saturating_add(1);
    if let Some(reason) = transition.last_reason.as_ref() {
        next.last_reason = Some(reason.clone());
    }
    Ok(next)
}

pub fn parse_goal_control_request(value: &Value) -> Option<GoalControlRequest> {
    let object = value.as_object()?;
    let action = object.get("action")?.as_str()?;
    match action {
        "create" => {
            if !has_only_keys(object, &["action", "objective"]) {
                return None;
            }
            parse_objective(object.get("objective")?.as_str()?)
                .map(|objective| GoalControlRequest::Create { objective })
        }
        "replace" | "edit" => {
            if !has_only_keys(
                object,
                &["action", "objective", "expectedGoalId", "expectedRevision"],
            ) {
                return None;
            }
            let objective = parse_objective(object.get("objective")?.as_str()?)?;
            let (expected_goal_id, expected_revision) = parse_expected_version(object)?;
            if action == "replace" {
                Some(GoalControlRequest::Replace {
                    objective,
                    expected_goal_id,
                    expected_revision,
                })
            } else {
                Some(GoalControlRequest::Edit {
                    objective,
                    expected_goal_id,
                    expected_revision,
                })
            }
        }
        "pause" | "resume" | "clear" => {
            if !has_only_keys(object, &["action", "expectedGoalId", "expectedRevision"]) {
                return None;
            }
            let (expected_goal_id, expected_revision) = parse_expected_version(object)?;
            match action {
                "pause" => Some(GoalControlRequest::Pause {
                    expected_goal_id,
                    expected_revision,
                }),
                "resume" => Some(GoalControlRequest::Resume {
                    expected_goal_id,
                    expected_revision,
                }),
                "clear" => Some(GoalControlRequest::Clear {
                    expected_goal_id,
                    expected_revision,
                }),
                _ => None,
            }
        }
        _ => None,
    }
}

pub fn parse_goal_state_record_payload_v2(value: &Value) -> Option<GoalStateRecordPayloadV2> {
    let object = value.as_object()?;
    if !has_only_keys(
        object,
        &[
            "v",
            "cause",
            "snapshot",
            "checkpointPending",
            "blockedAudit",
        ],
    ) || parse_integer(object.get("v")?) != Some(u64::from(GOAL_STATE_VERSION))
    {
        return None;
    }
    let cause = parse_goal_state_cause(object.get("cause")?)?;
    let snapshot = parse_goal_snapshot_v2(object.get("snapshot")?)?;
    if snapshot.activity != GoalActivity::Idle {
        return None;
    }
    let checkpoint_pending = match object.get("checkpointPending") {
        None => None,
        Some(value) => Some(parse_checkpoint_pending(value)?),
    };
    let blocked_audit = match object.get("blockedAudit") {
        None => None,
        Some(value) => Some(parse_blocked_audit(value)?),
    };
    if let Some(pending) = checkpoint_pending.as_ref() {
        let goal = snapshot.goal.as_ref()?;
        if goal.status != GoalStatus::Active
            || pending.permit.goal_id != goal.goal_id
            || pending.permit.revision != goal.revision
            || goal.evidence_cursor.record_id.as_deref() == Some(pending.record_uuid.as_str())
            || !matches!(
                cause,
                GoalStateCause::TurnFinished | GoalStateCause::VerifierReject
            )
        {
            return None;
        }
    }
    Some(GoalStateRecordPayloadV2 {
        v: GOAL_STATE_VERSION,
        cause,
        snapshot,
        checkpoint_pending,
        blocked_audit,
    })
}

pub fn parse_goal_snapshot_v2(value: &Value) -> Option<GoalSnapshotV2> {
    let object = value.as_object()?;
    if !has_only_keys(object, &["v", "goal", "activity"])
        || parse_integer(object.get("v")?) != Some(u64::from(GOAL_STATE_VERSION))
    {
        return None;
    }
    let activity = parse_goal_activity(object.get("activity")?)?;
    let goal = match object.get("goal")? {
        Value::Null => None,
        goal => Some(parse_goal_record(goal)?),
    };
    Some(GoalSnapshotV2 {
        v: GOAL_STATE_VERSION,
        goal,
        activity,
    })
}

pub fn parse_goal_state_cause(value: &Value) -> Option<GoalStateCause> {
    Some(match value.as_str()? {
        "create" => GoalStateCause::Create,
        "replace" => GoalStateCause::Replace,
        "edit" => GoalStateCause::Edit,
        "pause" => GoalStateCause::Pause,
        "resume" => GoalStateCause::Resume,
        "turn_finished" => GoalStateCause::TurnFinished,
        "checkpoint" => GoalStateCause::Checkpoint,
        "verifier_accept" => GoalStateCause::VerifierAccept,
        "verifier_reject" => GoalStateCause::VerifierReject,
        "complete" => GoalStateCause::Complete,
        "blocked" => GoalStateCause::Blocked,
        "usage_limited" => GoalStateCause::UsageLimited,
        "clear" => GoalStateCause::Clear,
        "migrated" => GoalStateCause::Migrated,
        _ => return None,
    })
}

fn create_goal(
    goal_id: &str,
    objective: String,
    now: f64,
    cursor: &TranscriptCursor,
) -> GoalRecord {
    GoalRecord {
        goal_id: goal_id.to_owned(),
        revision: 1,
        objective,
        status: GoalStatus::Active,
        evidence_cursor: copy_cursor(cursor),
        turn_count: 0,
        active_time_ms: 0.0,
        created_at: now,
        updated_at: now,
        evidence_checkpoint: None,
        last_reason: None,
        limit_kind: None,
    }
}

fn expected_version(request: &GoalControlRequest) -> (&str, u64) {
    match request {
        GoalControlRequest::Replace {
            expected_goal_id,
            expected_revision,
            ..
        }
        | GoalControlRequest::Edit {
            expected_goal_id,
            expected_revision,
            ..
        }
        | GoalControlRequest::Pause {
            expected_goal_id,
            expected_revision,
        }
        | GoalControlRequest::Resume {
            expected_goal_id,
            expected_revision,
        }
        | GoalControlRequest::Clear {
            expected_goal_id,
            expected_revision,
        } => (expected_goal_id, *expected_revision),
        GoalControlRequest::Create { .. } => unreachable!("create has no expected version"),
    }
}

fn assert_expected_version<'a>(
    current: Option<&'a GoalRecord>,
    expected_goal_id: &str,
    expected_revision: u64,
) -> Result<&'a GoalRecord, GoalConflictError> {
    let Some(current) = current else {
        return Err(GoalConflictError {
            current: Box::new(snapshot_of(None)),
        });
    };
    if current.goal_id != expected_goal_id || current.revision != expected_revision {
        return Err(GoalConflictError {
            current: Box::new(snapshot_of(Some(current))),
        });
    }
    Ok(current)
}

fn normalize_objective(
    objective: &str,
    current: GoalSnapshotV2,
) -> Result<String, GoalInvalidTransitionError> {
    let normalized = objective.trim();
    if normalized.is_empty() {
        return Err(GoalInvalidTransitionError {
            message: "Goal objective must not be empty".to_owned(),
            current: Box::new(current),
        });
    }
    Ok(normalized.to_owned())
}

fn is_evidence_limited(goal: &GoalRecord) -> bool {
    goal.limit_kind.is_some()
        || goal
            .last_reason
            .as_deref()
            .is_some_and(|reason| goal_limit_kind_for_reason(reason).is_some())
}

fn transition_goal(goal: &GoalRecord, now: f64) -> GoalRecord {
    let mut next = goal.clone();
    next.active_time_ms = elapsed_active_time(goal, now);
    next.updated_at = now;
    next
}

fn snapshot_of(goal: Option<&GoalRecord>) -> GoalSnapshotV2 {
    GoalSnapshotV2 {
        v: GOAL_STATE_VERSION,
        goal: goal.cloned(),
        activity: GoalActivity::Idle,
    }
}

fn copy_cursor(cursor: &TranscriptCursor) -> TranscriptCursor {
    cursor.clone()
}

fn invalid_transition(message: &str, current: &GoalRecord) -> GoalReducerError {
    GoalInvalidTransitionError {
        message: message.to_owned(),
        current: Box::new(snapshot_of(Some(current))),
    }
    .into()
}

fn parse_objective(value: &str) -> Option<String> {
    let objective = value.trim();
    (!objective.is_empty()).then(|| objective.to_owned())
}

fn parse_expected_version(object: &Map<String, Value>) -> Option<(String, u64)> {
    let goal_id = object.get("expectedGoalId")?.as_str()?;
    let revision = parse_integer(object.get("expectedRevision")?)?;
    if goal_id.is_empty() || revision == 0 {
        return None;
    }
    Some((goal_id.to_owned(), revision))
}

fn parse_goal_record(value: &Value) -> Option<GoalRecord> {
    let object = value.as_object()?;
    if !has_only_keys(
        object,
        &[
            "goalId",
            "revision",
            "objective",
            "status",
            "evidenceCursor",
            "turnCount",
            "activeTimeMs",
            "createdAt",
            "updatedAt",
            "evidenceCheckpoint",
            "lastReason",
            "limitKind",
        ],
    ) {
        return None;
    }
    let goal_id = object.get("goalId")?.as_str()?;
    let revision = parse_integer(object.get("revision")?)?;
    let objective = object.get("objective")?.as_str()?;
    let status = parse_goal_status(object.get("status")?)?;
    let evidence_cursor = parse_transcript_cursor(object.get("evidenceCursor")?)?;
    let turn_count = parse_integer(object.get("turnCount")?)?;
    let active_time_ms = parse_nonnegative_number(object.get("activeTimeMs")?)?;
    let created_at = parse_finite_number(object.get("createdAt")?)?;
    let updated_at = parse_finite_number(object.get("updatedAt")?)?;
    if goal_id.is_empty() || revision == 0 || objective.trim().is_empty() {
        return None;
    }
    let evidence_checkpoint = match object.get("evidenceCheckpoint") {
        None => None,
        Some(value) => Some(parse_evidence_checkpoint(value)?),
    };
    let last_reason = match object.get("lastReason") {
        None => None,
        Some(Value::String(value)) => Some(value.clone()),
        Some(_) => return None,
    };
    let limit_kind = match object.get("limitKind") {
        None => None,
        Some(value) => Some(parse_goal_limit_kind(value)?),
    };
    if limit_kind.is_some() && status != GoalStatus::UsageLimited {
        return None;
    }
    if evidence_checkpoint.as_ref().is_some_and(|checkpoint| {
        evidence_cursor.record_id.as_deref() != Some(checkpoint.checkpoint_id.as_str())
    }) {
        return None;
    }
    Some(GoalRecord {
        goal_id: goal_id.to_owned(),
        revision,
        objective: objective.to_owned(),
        status,
        evidence_cursor,
        turn_count,
        active_time_ms,
        created_at,
        updated_at,
        evidence_checkpoint,
        last_reason,
        limit_kind,
    })
}

fn parse_transcript_cursor(value: &Value) -> Option<TranscriptCursor> {
    let object = value.as_object()?;
    if !has_only_keys(object, &["recordId"]) {
        return None;
    }
    let record_id = match object.get("recordId")? {
        Value::Null => None,
        Value::String(value) => Some(value.clone()),
        _ => return None,
    };
    Some(TranscriptCursor { record_id })
}

fn parse_evidence_checkpoint(value: &Value) -> Option<GoalEvidenceCheckpoint> {
    let object = value.as_object()?;
    if !has_only_keys(object, &["checkpointId", "createdAt", "claims"]) {
        return None;
    }
    let checkpoint_id = object.get("checkpointId")?.as_str()?;
    let created_at = parse_finite_number(object.get("createdAt")?)?;
    let claims_value = object.get("claims")?.as_array()?;
    if checkpoint_id.is_empty()
        || claims_value.is_empty()
        || claims_value.len() > GOAL_CHECKPOINT_CLAIM_LIMIT
    {
        return None;
    }
    let mut total_claim_bytes = 0usize;
    let mut claims = Vec::with_capacity(claims_value.len());
    for (index, value) in claims_value.iter().enumerate() {
        let claim_object = value.as_object()?;
        if !has_only_keys(claim_object, &["id", "proofKind", "claim", "sourceRefs"]) {
            return None;
        }
        let id = claim_object.get("id")?.as_str()?;
        let expected_id = format!("{checkpoint_id}:{}", index + 1);
        let proof_kind = parse_proof_kind(claim_object.get("proofKind")?)?;
        let claim = claim_object.get("claim")?.as_str()?;
        let source_refs_value = claim_object.get("sourceRefs")?.as_array()?;
        let source_refs = source_refs_value
            .iter()
            .map(|reference| reference.as_str().map(str::to_owned))
            .collect::<Option<Vec<_>>>()?;
        if id != expected_id
            || claim.trim().is_empty()
            || claim.chars().count() > GOAL_CHECKPOINT_CLAIM_MAX_CHARACTERS
            || source_refs.is_empty()
            || source_refs.len() > GOAL_CHECKPOINT_SOURCE_REFERENCE_LIMIT
            || source_refs.iter().any(String::is_empty)
        {
            return None;
        }
        let unique_refs = source_refs.iter().collect::<HashSet<_>>();
        if unique_refs.len() != source_refs.len() {
            return None;
        }
        total_claim_bytes = total_claim_bytes.checked_add(claim.len())?;
        if total_claim_bytes > GOAL_CHECKPOINT_CLAIM_MAX_BYTES {
            return None;
        }
        claims.push(GoalEvidenceCheckpointClaim {
            id: id.to_owned(),
            proof_kind,
            claim: claim.to_owned(),
            source_refs,
        });
    }
    Some(GoalEvidenceCheckpoint {
        checkpoint_id: checkpoint_id.to_owned(),
        created_at,
        claims,
    })
}

fn parse_checkpoint_pending(value: &Value) -> Option<GoalCheckpointPending> {
    let object = value.as_object()?;
    if !has_only_keys(object, &["permit", "recordUuid"]) {
        return None;
    }
    let permit = parse_turn_permit(object.get("permit")?)?;
    let record_uuid = object.get("recordUuid")?.as_str()?;
    if record_uuid.is_empty() {
        return None;
    }
    Some(GoalCheckpointPending {
        permit,
        record_uuid: record_uuid.to_owned(),
    })
}

fn parse_turn_permit(value: &Value) -> Option<GoalTurnPermit> {
    let object = value.as_object()?;
    if !has_only_keys(object, &["goalId", "revision", "turnId"]) {
        return None;
    }
    let goal_id = object.get("goalId")?.as_str()?;
    let revision = parse_integer(object.get("revision")?)?;
    let turn_id = object.get("turnId")?.as_str()?;
    if goal_id.is_empty() || revision == 0 || turn_id.is_empty() {
        return None;
    }
    Some(GoalTurnPermit {
        goal_id: goal_id.to_owned(),
        revision,
        turn_id: turn_id.to_owned(),
    })
}

fn parse_blocked_audit(value: &Value) -> Option<GoalBlockedAudit> {
    let object = value.as_object()?;
    if !has_only_keys(object, &["fingerprint", "count", "turnIds"]) {
        return None;
    }
    let fingerprint = object.get("fingerprint")?.as_str()?;
    let count = parse_integer(object.get("count")?)?;
    let turn_ids_value = object.get("turnIds")?.as_array()?;
    let turn_ids = turn_ids_value
        .iter()
        .map(|turn_id| turn_id.as_str().map(str::to_owned))
        .collect::<Option<Vec<_>>>()?;
    if fingerprint.is_empty()
        || count == 0
        || count > MAX_BLOCKED_AUDIT_COUNT
        || turn_ids.len() != count as usize
        || turn_ids.iter().any(String::is_empty)
    {
        return None;
    }
    Some(GoalBlockedAudit {
        fingerprint: fingerprint.to_owned(),
        count: count as u8,
        turn_ids,
    })
}

fn parse_goal_status(value: &Value) -> Option<GoalStatus> {
    Some(match value.as_str()? {
        "active" => GoalStatus::Active,
        "paused" => GoalStatus::Paused,
        "blocked" => GoalStatus::Blocked,
        "usage_limited" => GoalStatus::UsageLimited,
        "complete" => GoalStatus::Complete,
        _ => return None,
    })
}

fn parse_goal_activity(value: &Value) -> Option<GoalActivity> {
    Some(match value.as_str()? {
        "idle" => GoalActivity::Idle,
        "running" => GoalActivity::Running,
        "verifying" => GoalActivity::Verifying,
        _ => return None,
    })
}

fn parse_goal_limit_kind(value: &Value) -> Option<GoalLimitKind> {
    if !is_goal_limit_kind(value) {
        return None;
    }
    Some(match value.as_str()? {
        "evidence_catalog" => GoalLimitKind::EvidenceCatalog,
        "checkpoint_request" => GoalLimitKind::CheckpointRequest,
        _ => return None,
    })
}

fn parse_proof_kind(value: &Value) -> Option<GoalEvidenceProofKind> {
    if !is_goal_evidence_proof_kind(value) {
        return None;
    }
    Some(match value.as_str()? {
        "user_input" => GoalEvidenceProofKind::UserInput,
        "delivered_output" => GoalEvidenceProofKind::DeliveredOutput,
        "external_fact" => GoalEvidenceProofKind::ExternalFact,
        _ => return None,
    })
}

fn parse_finite_number(value: &Value) -> Option<f64> {
    let number = value.as_f64()?;
    number.is_finite().then_some(number)
}

fn parse_nonnegative_number(value: &Value) -> Option<f64> {
    let number = parse_finite_number(value)?;
    (number >= 0.0).then_some(number)
}

fn parse_integer(value: &Value) -> Option<u64> {
    if let Some(integer) = value.as_u64() {
        return Some(integer);
    }
    let number = parse_finite_number(value)?;
    (number >= 0.0 && number.fract() == 0.0 && number <= u64::MAX as f64).then_some(number as u64)
}

fn has_only_keys(object: &Map<String, Value>, allowed: &[&str]) -> bool {
    object.keys().all(|key| allowed.contains(&key.as_str()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn goal(status: GoalStatus) -> GoalRecord {
        GoalRecord {
            goal_id: "g-1".to_owned(),
            revision: 1,
            objective: "ship".to_owned(),
            status,
            evidence_cursor: TranscriptCursor {
                record_id: Some("r-100".to_owned()),
            },
            turn_count: 0,
            active_time_ms: 0.0,
            created_at: 100.0,
            updated_at: 100.0,
            evidence_checkpoint: None,
            last_reason: None,
            limit_kind: None,
        }
    }

    fn snapshot(goal: Option<GoalRecord>) -> GoalSnapshotV2 {
        GoalSnapshotV2 {
            v: GOAL_STATE_VERSION,
            goal,
            activity: GoalActivity::Idle,
        }
    }

    fn transition(request: GoalControlRequest, now: f64) -> GoalControlTransition {
        GoalControlTransition {
            request,
            now,
            next_goal_id: "g-next".to_owned(),
            cursor: TranscriptCursor {
                record_id: Some("r-next".to_owned()),
            },
        }
    }

    fn parse_error(error: GoalReducerError) -> (String, GoalSnapshotV2) {
        match error {
            GoalReducerError::Conflict(error) => (error.to_string(), *error.current),
            GoalReducerError::InvalidTransition(error) => (error.message, *error.current),
        }
    }

    #[test]
    fn control_requests_trim_objectives_and_reject_unknown_keys() {
        assert_eq!(
            parse_goal_control_request(&json!({"action":"create","objective":" ship "})),
            Some(GoalControlRequest::Create {
                objective: "ship".to_owned()
            })
        );
        assert!(
            parse_goal_control_request(&json!({
                "action":"pause",
                "expectedGoalId":"g-1",
                "expectedRevision":1,
                "extra":true
            }))
            .is_none()
        );
        assert!(
            parse_goal_control_request(&json!({
                "action":"edit",
                "objective":" ",
                "expectedGoalId":"g-1",
                "expectedRevision":1
            }))
            .is_none()
        );
    }

    #[test]
    fn creates_edits_pauses_and_resumes_with_source_transition_rules() {
        let created = reduce_goal_control(
            None,
            &transition(
                GoalControlRequest::Create {
                    objective: " ship ".to_owned(),
                },
                100.0,
            ),
        )
        .unwrap()
        .unwrap();
        assert_eq!(created.goal_id, "g-next");
        assert_eq!(created.revision, 1);
        assert_eq!(created.objective, "ship");
        assert_eq!(created.status, GoalStatus::Active);
        assert_eq!(created.evidence_cursor.record_id.as_deref(), Some("r-next"));
        assert_eq!(created.turn_count, 0);
        assert_eq!(created.active_time_ms, 0.0);
        assert_eq!(created.created_at, 100.0);
        assert_eq!(created.updated_at, 100.0);

        let paused = reduce_goal_control(
            Some(&created),
            &transition(
                GoalControlRequest::Pause {
                    expected_goal_id: "g-next".to_owned(),
                    expected_revision: 1,
                },
                160.0,
            ),
        )
        .unwrap()
        .unwrap();
        assert_eq!(paused.status, GoalStatus::Paused);
        assert_eq!(paused.active_time_ms, 60.0);
        assert_eq!(paused.evidence_cursor.record_id.as_deref(), Some("r-next"));

        let resumed = reduce_goal_control(
            Some(&paused),
            &transition(
                GoalControlRequest::Resume {
                    expected_goal_id: "g-next".to_owned(),
                    expected_revision: 1,
                },
                250.0,
            ),
        )
        .unwrap()
        .unwrap();
        assert_eq!(resumed.status, GoalStatus::Active);
        assert_eq!(resumed.active_time_ms, 60.0);

        let edited = reduce_goal_control(
            Some(&resumed),
            &transition(
                GoalControlRequest::Edit {
                    objective: " new objective ".to_owned(),
                    expected_goal_id: "g-next".to_owned(),
                    expected_revision: 1,
                },
                275.0,
            ),
        )
        .unwrap()
        .unwrap();
        assert_eq!(edited.objective, "new objective");
        assert_eq!(edited.revision, 2);
        assert_eq!(edited.active_time_ms, 85.0);
        assert_eq!(edited.evidence_cursor.record_id.as_deref(), Some("r-next"));
    }

    #[test]
    fn replace_starts_a_fresh_identity_and_clear_removes_only_the_matching_goal() {
        let mut previous = goal(GoalStatus::Paused);
        previous.revision = 7;
        previous.turn_count = 5;
        previous.active_time_ms = 80.0;
        previous.evidence_checkpoint = Some(GoalEvidenceCheckpoint {
            checkpoint_id: "cp-1".to_owned(),
            created_at: 150.0,
            claims: vec![GoalEvidenceCheckpointClaim {
                id: "cp-1:1".to_owned(),
                proof_kind: GoalEvidenceProofKind::ExternalFact,
                claim: "shipped".to_owned(),
                source_refs: vec!["record-1".to_owned()],
            }],
        });
        previous.last_reason = Some("old reason".to_owned());

        let replaced = reduce_goal_control(
            Some(&previous),
            &transition(
                GoalControlRequest::Replace {
                    objective: " ship again ".to_owned(),
                    expected_goal_id: "g-1".to_owned(),
                    expected_revision: 7,
                },
                300.0,
            ),
        )
        .unwrap()
        .unwrap();
        assert_eq!(replaced.goal_id, "g-next");
        assert_eq!(replaced.revision, 1);
        assert_eq!(replaced.objective, "ship again");
        assert_eq!(replaced.status, GoalStatus::Active);
        assert_eq!(replaced.turn_count, 0);
        assert_eq!(replaced.active_time_ms, 0.0);
        assert_eq!(
            replaced.evidence_cursor.record_id.as_deref(),
            Some("r-next")
        );
        assert!(replaced.evidence_checkpoint.is_none());
        assert!(replaced.last_reason.is_none());

        let cleared = reduce_goal_control(
            Some(&replaced),
            &transition(
                GoalControlRequest::Clear {
                    expected_goal_id: "g-next".to_owned(),
                    expected_revision: 1,
                },
                350.0,
            ),
        )
        .unwrap();
        assert!(cleared.is_none());
    }

    #[test]
    fn conflicts_and_invalid_transitions_include_the_current_idle_snapshot() {
        let current = goal(GoalStatus::Active);
        let error = reduce_goal_control(
            Some(&current),
            &transition(
                GoalControlRequest::Pause {
                    expected_goal_id: "stale".to_owned(),
                    expected_revision: 1,
                },
                200.0,
            ),
        )
        .unwrap_err();
        let (message, attached_snapshot) = parse_error(error);
        assert_eq!(
            message,
            "Goal version does not match the current session Goal"
        );
        assert_eq!(attached_snapshot, snapshot(Some(current.clone())));

        let error = reduce_goal_control(
            Some(&current),
            &transition(
                GoalControlRequest::Resume {
                    expected_goal_id: "g-1".to_owned(),
                    expected_revision: 1,
                },
                200.0,
            ),
        )
        .unwrap_err();
        let (message, attached_snapshot) = parse_error(error);
        assert_eq!(message, "An active Goal cannot be resumed");
        assert_eq!(attached_snapshot, snapshot(Some(current)));
    }

    #[test]
    fn evidence_limited_usage_goal_requires_edit_before_resume() {
        let mut limited = goal(GoalStatus::UsageLimited);
        limited.last_reason = Some("old persisted sentinel".to_owned());
        limited.limit_kind = Some(GoalLimitKind::EvidenceCatalog);
        let error = reduce_goal_control(
            Some(&limited),
            &transition(
                GoalControlRequest::Resume {
                    expected_goal_id: "g-1".to_owned(),
                    expected_revision: 1,
                },
                200.0,
            ),
        )
        .unwrap_err();
        assert!(error.to_string().contains("edit or replace the Goal first"));
    }

    #[test]
    fn turn_finish_counts_paused_turns_without_resuming_active_time() {
        let mut paused = goal(GoalStatus::Paused);
        paused.turn_count = 2;
        paused.active_time_ms = 60.0;
        paused.updated_at = 160.0;
        let finished = reduce_goal_turn_finished(
            &paused,
            &GoalTurnFinishedTransition {
                now: 225.0,
                last_reason: Some("still working".to_owned()),
            },
        )
        .unwrap();
        assert_eq!(finished.turn_count, 3);
        assert_eq!(finished.active_time_ms, 60.0);
        assert_eq!(finished.updated_at, 225.0);
        assert_eq!(finished.last_reason.as_deref(), Some("still working"));
    }

    #[test]
    fn turn_finish_without_a_reason_preserves_the_previous_reason_and_rejects_terminal_goals() {
        let mut paused = goal(GoalStatus::Paused);
        paused.last_reason = Some("existing".to_owned());
        let finished = reduce_goal_turn_finished(
            &paused,
            &GoalTurnFinishedTransition {
                now: 200.0,
                last_reason: None,
            },
        )
        .unwrap();
        assert_eq!(finished.last_reason.as_deref(), Some("existing"));

        let error = reduce_goal_turn_finished(
            &goal(GoalStatus::Complete),
            &GoalTurnFinishedTransition {
                now: 200.0,
                last_reason: None,
            },
        )
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "Only an active or paused Goal can finish a turn"
        );
        assert_eq!(error.current.goal.unwrap().status, GoalStatus::Complete);
    }

    #[test]
    fn snapshots_and_lifecycle_payloads_reject_unknown_keys_and_non_idle_activity() {
        let snapshot_value = json!({
            "v":2,
            "goal":{
                "goalId":"g-1","revision":1,"objective":"ship","status":"active",
                "evidenceCursor":{"recordId":"r-1"},"turnCount":0,"activeTimeMs":0,
                "createdAt":1,"updatedAt":1
            },
            "activity":"idle"
        });
        assert!(parse_goal_snapshot_v2(&snapshot_value).is_some());
        let mut unknown = snapshot_value.clone();
        unknown["other"] = json!(true);
        assert!(parse_goal_snapshot_v2(&unknown).is_none());
        let mut null_optional = snapshot_value.clone();
        null_optional["goal"]["lastReason"] = json!(null);
        assert!(parse_goal_snapshot_v2(&null_optional).is_none());
        assert!(serde_json::from_value::<GoalSnapshotV2>(null_optional).is_err());
        assert!(
            parse_goal_snapshot_v2(&json!({
                "v":2,"goal":snapshot_value["goal"],"activity":"running"
            }))
            .is_some()
        );

        let payload = json!({
            "v":2,"cause":"create","snapshot":snapshot_value
        });
        assert!(parse_goal_state_record_payload_v2(&payload).is_some());
        assert!(
            parse_goal_state_record_payload_v2(&json!({
                "v":2,"cause":"create","snapshot":{
                    "v":2,"goal":snapshot_value["goal"],"activity":"running"
                }
            }))
            .is_none()
        );
        let mut extra = payload;
        extra["unknown"] = json!(1);
        assert!(parse_goal_state_record_payload_v2(&extra).is_none());
    }

    #[test]
    fn pending_checkpoint_requires_matching_active_permit_and_a_new_cursor() {
        let valid = json!({
            "v":2,"cause":"turn_finished",
            "snapshot":{
                "v":2,"activity":"idle","goal":{
                    "goalId":"g-1","revision":2,"objective":"ship","status":"active",
                    "evidenceCursor":{"recordId":"r-1"},"turnCount":3,"activeTimeMs":10,
                    "createdAt":1,"updatedAt":2
                }
            },
            "checkpointPending":{
                "permit":{"goalId":"g-1","revision":2,"turnId":"turn-4"},
                "recordUuid":"checkpoint-1"
            }
        });
        assert!(parse_goal_state_record_payload_v2(&valid).is_some());
        let mut same_cursor = valid.clone();
        same_cursor["checkpointPending"]["recordUuid"] = json!("r-1");
        assert!(parse_goal_state_record_payload_v2(&same_cursor).is_none());
        let mut wrong_revision = valid;
        wrong_revision["checkpointPending"]["permit"]["revision"] = json!(1);
        assert!(parse_goal_state_record_payload_v2(&wrong_revision).is_none());
    }

    #[test]
    fn evidence_checkpoint_enforces_sequential_ids_and_utf8_byte_budget() {
        let goal_value = json!({
            "goalId":"g-1","revision":1,"objective":"ship","status":"active",
            "evidenceCursor":{"recordId":"cp-1"},"turnCount":0,"activeTimeMs":0,
            "createdAt":1,"updatedAt":1,
            "evidenceCheckpoint":{
                "checkpointId":"cp-1","createdAt":1,
                "claims":[{
                    "id":"cp-1:1","proofKind":"external_fact","claim":"done ✓",
                    "sourceRefs":["tool-1"]
                }]
            }
        });
        assert!(
            parse_goal_snapshot_v2(&json!({
                "v":2,"goal":goal_value,"activity":"idle"
            }))
            .is_some()
        );
        let mut bad_claim = goal_value;
        bad_claim["evidenceCheckpoint"]["claims"][0]["id"] = json!("cp-1:custom");
        assert!(
            parse_goal_snapshot_v2(&json!({
                "v":2,"goal":bad_claim,"activity":"idle"
            }))
            .is_none()
        );
    }

    #[test]
    fn blocked_audit_is_bounded_and_count_must_match_ids() {
        let payload = json!({
            "v":2,"cause":"turn_finished",
            "snapshot":{"v":2,"goal":null,"activity":"idle"},
            "blockedAudit":{"fingerprint":"same","count":2,"turnIds":["t-1","t-2"]}
        });
        assert!(parse_goal_state_record_payload_v2(&payload).is_some());
        let mut mismatch = payload.clone();
        mismatch["blockedAudit"]["count"] = json!(1);
        assert!(parse_goal_state_record_payload_v2(&mismatch).is_none());
        let mut extra = payload;
        extra["blockedAudit"]["extra"] = json!(true);
        assert!(parse_goal_state_record_payload_v2(&extra).is_none());
    }
}
