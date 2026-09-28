use serde::{Deserialize, Serialize};

use super::protocol::{
    GoalRecord, GoalSnapshotV2, GoalStateCause, GoalStateRecordPayloadV2, GoalStatus,
};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LegacyGoalStatusKind {
    Set,
    Achieved,
    Cleared,
    Failed,
    Aborted,
    Paused,
    Checking,
}

impl LegacyGoalStatusKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Set => "set",
            Self::Achieved => "achieved",
            Self::Cleared => "cleared",
            Self::Failed => "failed",
            Self::Aborted => "aborted",
            Self::Paused => "paused",
            Self::Checking => "checking",
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct LegacyGoalStatus {
    #[serde(rename = "type")]
    pub record_type: String,
    pub kind: LegacyGoalStatusKind,
    pub condition: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub iterations: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub set_at: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_reason: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct LegacyActiveGoal {
    pub condition: String,
    pub iterations: u64,
    pub set_at: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tokens_at_start: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hook_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_reason: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LegacyGoalTerminalKind {
    Achieved,
    Failed,
    Aborted,
}

impl LegacyGoalTerminalKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Achieved => "achieved",
            Self::Failed => "failed",
            Self::Aborted => "aborted",
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct LegacyGoalTerminal {
    pub kind: LegacyGoalTerminalKind,
    pub condition: String,
    pub iterations: u64,
    pub duration_ms: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_reason: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct LegacyGoalProjection {
    pub active_goal: Option<LegacyActiveGoal>,
    pub goal_status: LegacyGoalStatus,
    pub goal_terminal: Option<LegacyGoalTerminal>,
}

pub fn project_goal_state_to_legacy(
    payload: &GoalStateRecordPayloadV2,
    previous_goal: Option<&GoalRecord>,
) -> LegacyGoalProjection {
    let snapshot_goal = payload.snapshot.goal.as_ref();
    let display_goal = snapshot_goal.or(previous_goal);
    let kind = legacy_status_kind(payload);
    let goal_status = LegacyGoalStatus {
        record_type: "goal_status".to_owned(),
        kind,
        condition: display_goal.map_or_else(String::new, |goal| goal.objective.clone()),
        iterations: display_goal.map(|goal| goal.turn_count),
        set_at: display_goal.map(|goal| goal.created_at),
        duration_ms: display_goal.map(|goal| goal.active_time_ms),
        last_reason: display_goal.and_then(|goal| goal.last_reason.clone()),
    };
    let terminal_kind = match kind {
        LegacyGoalStatusKind::Achieved => Some(LegacyGoalTerminalKind::Achieved),
        LegacyGoalStatusKind::Failed => Some(LegacyGoalTerminalKind::Failed),
        LegacyGoalStatusKind::Aborted => Some(LegacyGoalTerminalKind::Aborted),
        _ => None,
    };
    let active_goal = snapshot_goal
        .filter(|goal| goal.status == GoalStatus::Active)
        .map(|goal| LegacyActiveGoal {
            condition: goal.objective.clone(),
            iterations: goal.turn_count,
            set_at: goal.created_at,
            tokens_at_start: None,
            hook_id: None,
            last_reason: goal.last_reason.clone(),
        });
    let goal_terminal = terminal_kind
        .zip(display_goal)
        .map(|(kind, goal)| LegacyGoalTerminal {
            kind,
            condition: goal.objective.clone(),
            iterations: goal.turn_count,
            duration_ms: goal.active_time_ms,
            last_reason: goal.last_reason.clone(),
        });
    LegacyGoalProjection {
        active_goal,
        goal_status,
        goal_terminal,
    }
}

#[derive(Clone, Debug)]
pub struct GoalCheckpointBookkeepingInput<'a> {
    pub cause: GoalStateCause,
    pub previous_cause: Option<GoalStateCause>,
    pub previous: Option<&'a GoalSnapshotV2>,
    pub next: &'a GoalSnapshotV2,
}

pub fn is_goal_checkpoint_bookkeeping_record(input: GoalCheckpointBookkeepingInput<'_>) -> bool {
    let Some(previous_goal) = input.previous.and_then(|snapshot| snapshot.goal.as_ref()) else {
        return false;
    };
    let Some(next_goal) = input.next.goal.as_ref() else {
        return false;
    };
    is_checkpoint_bookkeeping_transition(previous_goal, next_goal)
        && is_checkpoint_bookkeeping_cause(input.cause, input.previous_cause)
}

fn is_checkpoint_bookkeeping_transition(previous: &GoalRecord, next: &GoalRecord) -> bool {
    previous.goal_id == next.goal_id
        && previous.revision == next.revision
        && previous.objective == next.objective
        && previous.status == next.status
        && previous.turn_count == next.turn_count
        && previous.created_at == next.created_at
        && previous.last_reason == next.last_reason
}

fn is_checkpoint_bookkeeping_cause(
    cause: GoalStateCause,
    previous_cause: Option<GoalStateCause>,
) -> bool {
    cause == GoalStateCause::Checkpoint
        || (cause == GoalStateCause::VerifierReject
            && matches!(
                previous_cause,
                Some(GoalStateCause::VerifierReject | GoalStateCause::Checkpoint)
            ))
}

fn legacy_status_kind(payload: &GoalStateRecordPayloadV2) -> LegacyGoalStatusKind {
    match payload.cause {
        GoalStateCause::Create
        | GoalStateCause::Replace
        | GoalStateCause::Edit
        | GoalStateCause::Resume => LegacyGoalStatusKind::Set,
        GoalStateCause::Complete => LegacyGoalStatusKind::Achieved,
        GoalStateCause::Clear => LegacyGoalStatusKind::Cleared,
        GoalStateCause::Migrated | GoalStateCause::Pause => LegacyGoalStatusKind::Paused,
        GoalStateCause::Blocked | GoalStateCause::UsageLimited => LegacyGoalStatusKind::Aborted,
        GoalStateCause::TurnFinished
        | GoalStateCause::Checkpoint
        | GoalStateCause::VerifierAccept
        | GoalStateCause::VerifierReject => {
            match payload.snapshot.goal.as_ref().map(|g| g.status) {
                Some(GoalStatus::Complete) => LegacyGoalStatusKind::Achieved,
                Some(GoalStatus::Blocked | GoalStatus::UsageLimited) => {
                    LegacyGoalStatusKind::Aborted
                }
                _ => LegacyGoalStatusKind::Checking,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::goals::protocol::{
        GOAL_STATE_VERSION, GoalActivity, GoalStateRecordPayloadV2, TranscriptCursor,
    };

    fn goal(status: GoalStatus) -> GoalRecord {
        GoalRecord {
            goal_id: "g-1".to_owned(),
            revision: 2,
            objective: "ship it".to_owned(),
            status,
            evidence_cursor: TranscriptCursor {
                record_id: Some("state-1".to_owned()),
            },
            turn_count: 4,
            active_time_ms: 2000.0,
            created_at: 100.0,
            updated_at: 200.0,
            evidence_checkpoint: None,
            last_reason: Some("continuing".to_owned()),
            limit_kind: None,
        }
    }

    fn payload(cause: GoalStateCause, status: GoalStatus) -> GoalStateRecordPayloadV2 {
        GoalStateRecordPayloadV2 {
            v: GOAL_STATE_VERSION,
            cause,
            snapshot: GoalSnapshotV2 {
                v: GOAL_STATE_VERSION,
                activity: GoalActivity::Idle,
                goal: Some(goal(status)),
            },
            checkpoint_pending: None,
            blocked_audit: None,
        }
    }

    #[test]
    fn maps_lifecycle_causes_to_legacy_active_and_terminal_projections() {
        for cause in [
            GoalStateCause::Create,
            GoalStateCause::Replace,
            GoalStateCause::Edit,
            GoalStateCause::Resume,
        ] {
            let projected = project_goal_state_to_legacy(&payload(cause, GoalStatus::Active), None);
            assert_eq!(projected.goal_status.kind, LegacyGoalStatusKind::Set);
            assert!(projected.active_goal.is_some());
            assert!(projected.goal_terminal.is_none());
        }
        let completed = project_goal_state_to_legacy(
            &payload(GoalStateCause::Complete, GoalStatus::Complete),
            None,
        );
        assert_eq!(completed.goal_status.kind, LegacyGoalStatusKind::Achieved);
        assert_eq!(
            completed.goal_terminal.unwrap().kind,
            LegacyGoalTerminalKind::Achieved
        );
        assert!(completed.active_goal.is_none());
    }

    #[test]
    fn uses_previous_goal_for_clear_and_projects_migration_as_paused() {
        let previous = goal(GoalStatus::Active);
        let clear = GoalStateRecordPayloadV2 {
            v: GOAL_STATE_VERSION,
            cause: GoalStateCause::Clear,
            snapshot: GoalSnapshotV2 {
                v: GOAL_STATE_VERSION,
                activity: GoalActivity::Idle,
                goal: None,
            },
            checkpoint_pending: None,
            blocked_audit: None,
        };
        let projected = project_goal_state_to_legacy(&clear, Some(&previous));
        assert_eq!(projected.goal_status.kind, LegacyGoalStatusKind::Cleared);
        assert_eq!(projected.goal_status.condition, "ship it");
        assert!(projected.goal_terminal.is_none());

        let migrated = project_goal_state_to_legacy(
            &payload(GoalStateCause::Migrated, GoalStatus::Paused),
            None,
        );
        assert_eq!(migrated.goal_status.kind, LegacyGoalStatusKind::Paused);
        assert!(migrated.active_goal.is_none());
    }

    #[test]
    fn stopped_runtime_statuses_project_as_aborted() {
        for status in [GoalStatus::Blocked, GoalStatus::UsageLimited] {
            let projected =
                project_goal_state_to_legacy(&payload(GoalStateCause::Blocked, status), None);
            assert_eq!(projected.goal_status.kind, LegacyGoalStatusKind::Aborted);
            assert_eq!(
                projected.goal_terminal.unwrap().kind,
                LegacyGoalTerminalKind::Aborted
            );
        }
    }

    #[test]
    fn paused_turn_finish_projects_as_checking_without_repeating_a_terminal() {
        let projected = project_goal_state_to_legacy(
            &payload(GoalStateCause::TurnFinished, GoalStatus::Paused),
            None,
        );
        assert_eq!(projected.goal_status.kind, LegacyGoalStatusKind::Checking);
        assert!(projected.active_goal.is_none());
        assert!(projected.goal_terminal.is_none());
    }

    #[test]
    fn suppresses_only_shape_equal_checkpoint_followups() {
        let previous = GoalSnapshotV2 {
            v: GOAL_STATE_VERSION,
            activity: GoalActivity::Idle,
            goal: Some(goal(GoalStatus::Active)),
        };
        let mut next = previous.clone();
        next.goal.as_mut().unwrap().active_time_ms += 10.0;
        assert!(is_goal_checkpoint_bookkeeping_record(
            GoalCheckpointBookkeepingInput {
                cause: GoalStateCause::Checkpoint,
                previous_cause: Some(GoalStateCause::TurnFinished),
                previous: Some(&previous),
                next: &next,
            }
        ));
        assert!(is_goal_checkpoint_bookkeeping_record(
            GoalCheckpointBookkeepingInput {
                cause: GoalStateCause::VerifierReject,
                previous_cause: Some(GoalStateCause::Checkpoint),
                previous: Some(&previous),
                next: &previous,
            }
        ));
        assert!(!is_goal_checkpoint_bookkeeping_record(
            GoalCheckpointBookkeepingInput {
                cause: GoalStateCause::VerifierReject,
                previous_cause: Some(GoalStateCause::TurnFinished),
                previous: Some(&previous),
                next: &previous,
            }
        ));
    }
}
