pub mod active_store;
pub mod checkpoint;
pub mod checkpoint_verifier;
pub mod evidence;
pub mod legacy_projection;
pub mod persistence;
pub mod protocol;
pub mod reducer;
pub mod runtime;
pub mod turn_context;

pub use active_store::{
    __reset_active_goal_store_for_tests, ActiveGoal, GoalTerminalEvent, GoalTerminalKind,
    GoalTerminalObserver, active_goal_equals, clear_active_goal, clear_goal_terminal_observer,
    get_active_goal, get_last_goal_terminal, notify_goal_terminal, record_goal_deferral,
    record_goal_iteration, reset_goal_deferrals, set_active_goal, set_goal_terminal_observer,
    set_last_goal_terminal,
};
pub use checkpoint::*;
pub use checkpoint_verifier::*;
pub use evidence::*;
pub use legacy_projection::{
    GoalCheckpointBookkeepingInput, LegacyActiveGoal, LegacyGoalProjection, LegacyGoalStatus,
    LegacyGoalStatusKind, LegacyGoalTerminal, LegacyGoalTerminalKind,
    is_goal_checkpoint_bookkeeping_record, project_goal_state_to_legacy,
};
pub use persistence::{
    GoalRecovery, GoalRecoveryRecord, GoalRecoverySelection, MigratedGoalStateInput,
    create_migrated_goal_state, is_goal_recovery_candidate, normalize_goal_recovery_record,
    recover_goal_from_records, select_goal_recovery_from_records,
};
pub use protocol::{
    GOAL_CHECKPOINT_CLAIM_LIMIT, GOAL_CHECKPOINT_CLAIM_MAX_BYTES,
    GOAL_CHECKPOINT_CLAIM_MAX_CHARACTERS, GOAL_CHECKPOINT_REQUEST_TOO_LARGE_REASON,
    GOAL_CHECKPOINT_SOURCE_REFERENCE_LIMIT, GOAL_EVIDENCE_CATALOG_EXHAUSTED_REASON,
    GOAL_PROPOSAL_REASON_MAX_BYTES, GOAL_PROPOSAL_REASON_MAX_CHARACTERS, GOAL_STATE_VERSION,
    GoalActivity, GoalBlockedAudit, GoalBlockerKind, GoalCheckpointPending, GoalControlRequest,
    GoalEvidenceCheckpoint, GoalEvidenceCheckpointClaim, GoalEvidenceProofKind,
    GoalExpectedVersion, GoalLimitKind, GoalRecord, GoalSnapshotV2, GoalStateCause,
    GoalStateRecordPayloadV2, GoalStateResponse, GoalStatus, GoalTerminalProposal,
    GoalTerminalProposalStatus, GoalTurnPermit, TranscriptCursor, empty_goal_snapshot,
    goal_limit_kind_for_reason, goal_requires_exact_permit, is_goal_evidence_proof_kind,
    is_goal_limit_kind, is_repeated_blocker_proposal, validate_goal_proposal_reason,
};
pub use reducer::{
    GoalConflictError, GoalControlTransition, GoalInvalidTransitionError, GoalReducerError,
    GoalTurnFinishedTransition, elapsed_active_time, parse_goal_control_request,
    parse_goal_snapshot_v2, parse_goal_state_cause, parse_goal_state_record_payload_v2,
    reduce_goal_control, reduce_goal_turn_finished,
};
pub use runtime::*;
pub use turn_context::{
    GoalTurnContext, current_goal_turn_permit, run_with_goal_turn_context,
    run_without_goal_turn_context, spawn_with_current_goal_turn,
};
