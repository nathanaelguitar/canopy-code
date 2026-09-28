//! Serialized Goal runtime orchestration.
//!
//! This module owns runtime state, Goal-turn permits, continuation scheduling,
//! restoration, and terminal proposal verification. Storage and model access
//! stay behind narrow traits so the runtime does not depend on CLI, ACP, or a
//! particular model provider.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{SystemTime, UNIX_EPOCH};

use futures_util::FutureExt;
use thiserror::Error;
use tokio::sync::{Mutex as AsyncMutex, broadcast};
use uuid::Uuid;

use crate::goals::checkpoint::{
    GoalCheckpointVerificationResult, GoalCheckpointVerifierInput,
    materialize_goal_evidence_checkpoint,
};
use crate::goals::checkpoint_verifier::GoalCheckpointVerifierError;
use crate::goals::evidence::{
    EvidenceSourceUnavailableCode, EvidenceSourceUnavailableErrorOrReference, GoalEvidenceCatalog,
    GoalEvidenceCheckpointWindow, GoalEvidenceContext, GoalEvidenceValidationInput,
    InvalidGoalEvidenceReferenceCode, ValidatedGoalEvidenceRecord, build_goal_evidence_catalog,
    build_goal_evidence_checkpoint_window, validate_goal_evidence_references,
};
use crate::goals::persistence::{
    GoalRecovery, GoalRecoveryRecord, create_migrated_goal_state, recover_goal_from_records,
};
use crate::goals::protocol::{
    GOAL_CHECKPOINT_REQUEST_TOO_LARGE_REASON, GOAL_EVIDENCE_CATALOG_EXHAUSTED_REASON,
    GOAL_STATE_VERSION, GoalActivity, GoalBlockedAudit, GoalCheckpointPending, GoalControlRequest,
    GoalEvidenceCheckpoint, GoalLimitKind, GoalRecord, GoalSnapshotV2, GoalStateCause,
    GoalStateRecordPayloadV2, GoalStatus, GoalTerminalProposal, GoalTerminalProposalStatus,
    GoalTurnPermit, TranscriptCursor, empty_goal_snapshot, is_repeated_blocker_proposal,
    validate_goal_proposal_reason,
};
use crate::goals::reducer::{
    GoalControlTransition, GoalInvalidTransitionError, GoalReducerError,
    GoalTurnFinishedTransition, elapsed_active_time, reduce_goal_control,
    reduce_goal_turn_finished,
};
use crate::goals::turn_context::run_with_goal_turn_context;
use crate::transcript::TranscriptRecord;
use crate::utils::cancellation::CancellationToken;

pub const GOAL_RUNTIME_DISPOSED_MESSAGE: &str = "Goal runtime has been disposed";
pub const STALE_GOAL_TURN_MESSAGE: &str = "Goal turn permit is no longer valid";
pub const GOAL_VERIFIER_REASON_MAX_CHARACTERS: usize = 2_000;

pub type GoalRuntimeFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, String>> + Send + 'a>>;

/// Persistence boundary for Goal lifecycle records.
pub trait GoalJournal: Send + Sync {
    fn get_transcript_cursor(&self) -> TranscriptCursor;

    fn record_goal_state<'a>(
        &'a self,
        record_uuid: String,
        payload: GoalStateRecordPayloadV2,
    ) -> GoalRuntimeFuture<'a, ()>;
}

/// Evidence reader bound to the active transcript chain for one session.
pub trait GoalEvidenceSource: Send + Sync {
    fn flush<'a>(&'a self) -> GoalRuntimeFuture<'a, ()>;

    fn read_active_transcript_chain<'a>(&'a self) -> GoalRuntimeFuture<'a, Vec<TranscriptRecord>>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GoalVerificationGoal {
    pub goal_id: String,
    pub revision: u64,
    pub objective: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GoalVerificationProposal {
    pub status: GoalTerminalProposalStatus,
    pub reason: String,
    pub evidence_refs: Vec<String>,
    pub blocker_kind: Option<crate::goals::protocol::GoalBlockerKind>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GoalVerificationInput {
    pub goal: GoalVerificationGoal,
    pub current_turn_id: String,
    pub proposal: GoalVerificationProposal,
    pub evidence: Vec<ValidatedGoalEvidenceRecord>,
    pub current_delivered_output: Option<Vec<String>>,
    /// Present only for blocked proposals, matching the source verifier API.
    pub blocked_policy: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GoalVerificationDecision {
    Accept,
    Reject,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GoalVerificationResult {
    pub decision: GoalVerificationDecision,
    pub reason: String,
}

/// Independent terminal-proposal verifier. Implementations should perform
/// provider I/O and cancellation handling; the runtime validates its result.
pub trait GoalVerifier: Send + Sync {
    fn verify<'a>(
        &'a self,
        input: GoalVerificationInput,
        cancellation: &'a CancellationToken,
    ) -> GoalRuntimeFuture<'a, GoalVerificationResult>;
}

/// Checkpoint verifier boundary. Claim IDs and evidence provenance are still
/// validated and materialized by the runtime using the Goal core helpers.
pub trait GoalCheckpointVerifier: Send + Sync {
    fn verify_checkpoint<'a>(
        &'a self,
        input: GoalCheckpointVerifierInput,
        cancellation: &'a CancellationToken,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<GoalCheckpointVerificationResult, GoalCheckpointVerifierError>,
                > + Send
                + 'a,
        >,
    >;
}

/// Host that starts internal Goal turns and preempts invalidated work.
pub trait GoalTurnHost: Send + Sync {
    fn start_goal_turn<'a>(&'a self, input: GoalStartTurn) -> GoalRuntimeFuture<'a, ()>;

    fn preempt_goal_turn(&self, reason: &str);
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GoalStartTurn {
    pub permit: GoalTurnPermit,
    pub continuation_context: String,
    pub verifier_feedback: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GoalRuntimeEvent {
    pub snapshot: GoalSnapshotV2,
    pub cause: Option<GoalStateCause>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GoalProposalReceipt {
    pub recorded: bool,
    pub ready_for_verification: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GoalPendingProposal {
    pub permit: GoalTurnPermit,
    pub proposal: GoalTerminalProposal,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GoalWorkerView {
    pub goal_id: String,
    pub revision: u64,
    pub objective: String,
    pub evidence_cursor: TranscriptCursor,
    pub evidence_catalog: Option<GoalEvidenceCatalog>,
    pub verifier_feedback: Option<String>,
}

#[derive(Clone)]
pub struct CreateGoalRuntimeOptions {
    pub journal: Arc<dyn GoalJournal>,
    pub evidence_source: Option<Arc<dyn GoalEvidenceSource>>,
    pub verifier: Option<Arc<dyn GoalVerifier>>,
    pub checkpoint_verifier: Option<Arc<dyn GoalCheckpointVerifier>>,
}

#[derive(Clone, Debug, Error, PartialEq)]
pub enum GoalRuntimeError {
    #[error("{0}")]
    PersistenceUnavailable(String),
    #[error("Goal runtime has been disposed")]
    Disposed,
    #[error("Goal turn permit is no longer valid")]
    StalePermit,
    #[error(transparent)]
    Reducer(#[from] GoalReducerError),
    #[error("{0}")]
    InvalidOperation(String),
    #[error("Goal persistence failed: {0}")]
    Journal(String),
}

#[derive(Clone)]
struct VerificationAttempt {
    permit: GoalTurnPermit,
    proposal: GoalTerminalProposal,
    goal: GoalRecord,
    cancellation: CancellationToken,
}

#[derive(Clone)]
struct CheckpointAttempt {
    permit: GoalTurnPermit,
    goal: GoalRecord,
    record_uuid: String,
    cancellation: CancellationToken,
}

#[derive(Clone)]
struct CurrentProposal {
    proposal: GoalTerminalProposal,
    ready_for_verification: bool,
    blocked_audit_candidate: Option<GoalBlockedAudit>,
}

enum VerificationOutcome {
    Decision(GoalVerificationResult),
    UsageLimited {
        reason: String,
        limit_kind: Option<GoalLimitKind>,
    },
}

struct RuntimeState {
    snapshot: GoalSnapshotV2,
    host: Option<Arc<dyn GoalTurnHost>>,
    current_permit: Option<GoalTurnPermit>,
    current_permit_host: Option<Arc<dyn GoalTurnHost>>,
    current_turn_key: Option<String>,
    queued_turn_key: Option<String>,
    continuation_queued: bool,
    current_proposal: Option<CurrentProposal>,
    pending_proposal: Option<GoalPendingProposal>,
    verification_attempt: Option<Arc<VerificationAttempt>>,
    checkpoint_attempt: Option<Arc<CheckpointAttempt>>,
    blocked_audit: Option<GoalBlockedAudit>,
    next_verifier_feedback: Option<String>,
    current_turn_feedback: Option<String>,
    restored: bool,
    restore_activation_pending: bool,
    prepared_restore_cause: Option<GoalStateCause>,
    prepared_restore_has_snapshot: bool,
    prepared_checkpoint_window: Option<GoalEvidenceCheckpointWindow>,
    recovery_cause: Option<GoalStateCause>,
    recovery_error: Option<String>,
    disposed: bool,
    restore_activation_started: bool,
}

struct GoalRuntimeInner {
    options: CreateGoalRuntimeOptions,
    state: Mutex<RuntimeState>,
    operation_lock: AsyncMutex<()>,
    restore_lock: AsyncMutex<()>,
    activation_lock: AsyncMutex<()>,
    events: broadcast::Sender<GoalRuntimeEvent>,
}

/// Thread-safe Goal runtime. State transitions are protected by a synchronous
/// lock; async persistence transitions are serialized by `operation_lock`.
#[derive(Clone)]
pub struct GoalRuntime {
    inner: Arc<GoalRuntimeInner>,
}

/// RAII host registration. Dropping an older binding cannot clear a newer
/// host binding.
pub struct GoalHostBinding {
    runtime: std::sync::Weak<GoalRuntimeInner>,
    host: Arc<dyn GoalTurnHost>,
}

impl Drop for GoalHostBinding {
    fn drop(&mut self) {
        let Some(runtime) = self.runtime.upgrade() else {
            return;
        };
        let mut state = lock_state(&runtime.state);
        if state
            .host
            .as_ref()
            .is_some_and(|host| Arc::ptr_eq(host, &self.host))
        {
            state.host = None;
        }
    }
}

fn lock_state(mutex: &Mutex<RuntimeState>) -> MutexGuard<'_, RuntimeState> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn now_ms() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as f64)
        .unwrap_or_default()
}

fn new_id() -> String {
    Uuid::new_v4().to_string()
}

fn normalize_recovered_blocked_audit(mut audit: GoalBlockedAudit) -> GoalBlockedAudit {
    if audit.fingerprint.starts_with('\n') {
        audit.fingerprint = format!("repeated{}", audit.fingerprint);
    }
    audit
}

fn assert_available(state: &RuntimeState) -> Result<(), GoalRuntimeError> {
    if state.disposed {
        return Err(GoalRuntimeError::Disposed);
    }
    if let Some(error) = &state.recovery_error {
        return Err(GoalRuntimeError::PersistenceUnavailable(error.clone()));
    }
    Ok(())
}

fn is_current_permit(state: &RuntimeState, permit: &GoalTurnPermit) -> bool {
    state.snapshot.goal.as_ref().is_some_and(|goal| {
        goal.goal_id == permit.goal_id
            && goal.revision == permit.revision
            && state.current_permit.as_ref().is_some_and(|current| {
                current.goal_id == permit.goal_id
                    && current.revision == permit.revision
                    && current.turn_id == permit.turn_id
            })
    })
}

fn new_permit(goal: &GoalRecord) -> GoalTurnPermit {
    GoalTurnPermit {
        goal_id: goal.goal_id.clone(),
        revision: goal.revision,
        turn_id: new_id(),
    }
}

fn goal_state_payload(cause: GoalStateCause, snapshot: GoalSnapshotV2) -> GoalStateRecordPayloadV2 {
    GoalStateRecordPayloadV2 {
        v: GOAL_STATE_VERSION,
        cause,
        snapshot,
        checkpoint_pending: None,
        blocked_audit: None,
    }
}

impl GoalRuntime {
    pub fn create(options: CreateGoalRuntimeOptions) -> Result<Self, GoalRuntimeError> {
        if options.evidence_source.is_some() != options.verifier.is_some() {
            return Err(GoalRuntimeError::InvalidOperation(
                "Goal evidence source and verifier must be configured together".to_owned(),
            ));
        }
        if options.checkpoint_verifier.is_some() && options.evidence_source.is_none() {
            return Err(GoalRuntimeError::InvalidOperation(
                "Goal checkpoint verifier requires a Goal evidence source".to_owned(),
            ));
        }
        let (events, _) = broadcast::channel(32);
        Ok(Self {
            inner: Arc::new(GoalRuntimeInner {
                options,
                state: Mutex::new(RuntimeState {
                    snapshot: empty_goal_snapshot(),
                    host: None,
                    current_permit: None,
                    current_permit_host: None,
                    current_turn_key: None,
                    queued_turn_key: None,
                    continuation_queued: false,
                    current_proposal: None,
                    pending_proposal: None,
                    verification_attempt: None,
                    checkpoint_attempt: None,
                    blocked_audit: None,
                    next_verifier_feedback: None,
                    current_turn_feedback: None,
                    restored: false,
                    restore_activation_pending: false,
                    prepared_restore_cause: None,
                    prepared_restore_has_snapshot: false,
                    prepared_checkpoint_window: None,
                    recovery_cause: None,
                    recovery_error: None,
                    disposed: false,
                    restore_activation_started: false,
                }),
                operation_lock: AsyncMutex::new(()),
                restore_lock: AsyncMutex::new(()),
                activation_lock: AsyncMutex::new(()),
                events,
            }),
        })
    }

    pub fn get_snapshot(&self) -> GoalSnapshotV2 {
        lock_state(&self.inner.state).snapshot.clone()
    }

    pub fn get_snapshot_for_permit(
        &self,
        permit: &GoalTurnPermit,
    ) -> Result<GoalSnapshotV2, GoalRuntimeError> {
        let state = lock_state(&self.inner.state);
        assert_available(&state)?;
        if !is_current_permit(&state, permit) {
            return Err(GoalRuntimeError::StalePermit);
        }
        Ok(state.snapshot.clone())
    }

    pub fn get_recovery_cause(&self) -> Option<GoalStateCause> {
        lock_state(&self.inner.state).recovery_cause
    }

    pub fn subscribe(&self) -> broadcast::Receiver<GoalRuntimeEvent> {
        self.inner.events.subscribe()
    }

    fn broadcast(&self, cause: Option<GoalStateCause>) {
        let snapshot = self.get_snapshot();
        let _ = self.inner.events.send(GoalRuntimeEvent { snapshot, cause });
    }

    fn preempt_host(host: Option<Arc<dyn GoalTurnHost>>, reason: &str) {
        if let Some(host) = host {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                host.preempt_goal_turn(reason);
            }));
        }
    }

    fn invalidate_attempts(state: &mut RuntimeState, reason: &str) {
        let verification = state.verification_attempt.take();
        let checkpoint = state.checkpoint_attempt.take();
        state.pending_proposal = None;
        if let Some(attempt) = verification {
            attempt.cancellation.cancel_with_reason(reason.to_owned());
        }
        if let Some(attempt) = checkpoint {
            attempt.cancellation.cancel_with_reason(reason.to_owned());
        }
    }

    fn queue_continuation(&self, cause: Option<GoalStateCause>) -> bool {
        {
            let mut state = lock_state(&self.inner.state);
            if state.restore_activation_pending
                || state
                    .snapshot
                    .goal
                    .as_ref()
                    .is_none_or(|goal| goal.status != GoalStatus::Active)
                || state.current_permit.is_some()
                || state.pending_proposal.is_some()
                || state.verification_attempt.is_some()
                || state.checkpoint_attempt.is_some()
            {
                return false;
            }
            state.continuation_queued = true;
        }
        self.flush_continuation(cause)
    }

    fn flush_continuation(&self, cause: Option<GoalStateCause>) -> bool {
        let Ok(runtime_handle) = tokio::runtime::Handle::try_current() else {
            return false;
        };
        let start = {
            let mut state = lock_state(&self.inner.state);
            if !state.continuation_queued
                || state.host.is_none()
                || state.current_permit.is_some()
                || state.pending_proposal.is_some()
                || state.verification_attempt.is_some()
                || state.checkpoint_attempt.is_some()
                || state.snapshot.activity != GoalActivity::Idle
                || state
                    .snapshot
                    .goal
                    .as_ref()
                    .is_none_or(|goal| goal.status != GoalStatus::Active)
            {
                return false;
            }
            let host = state.host.clone().expect("checked host");
            let goal = state.snapshot.goal.as_ref().expect("checked goal");
            let permit = new_permit(goal);
            let input = GoalStartTurn {
                permit: permit.clone(),
                continuation_context: goal.objective.clone(),
                verifier_feedback: state.next_verifier_feedback.take(),
            };
            state.current_turn_feedback = input.verifier_feedback.clone();
            state.current_permit_host = Some(host.clone());
            state.current_turn_key = Some(format!("goal-runtime:{}", permit.turn_id));
            state.current_permit = Some(permit);
            state.continuation_queued = false;
            state.snapshot.activity = GoalActivity::Running;
            (host, input)
        };
        self.broadcast(cause);
        let runtime = self.clone();
        runtime_handle.spawn(async move {
            let host = start.0;
            let result = std::panic::AssertUnwindSafe(host.start_goal_turn(start.1.clone()))
                .catch_unwind()
                .await;
            if !matches!(result, Ok(Ok(()))) {
                runtime.handle_start_failure(host, start.1.permit).await;
            }
        });
        true
    }

    async fn handle_start_failure(
        &self,
        scheduled_host: Arc<dyn GoalTurnHost>,
        permit: GoalTurnPermit,
    ) {
        let _operation = self.inner.operation_lock.lock().await;
        let should_continue = {
            let mut state = lock_state(&self.inner.state);
            if state.disposed || !is_current_permit(&state, &permit) {
                return;
            }
            let next_turn_key = state.queued_turn_key.take();
            state.current_permit = None;
            state.current_permit_host = None;
            state.current_turn_key = None;
            state.current_proposal = None;
            if let Some(feedback) = state.current_turn_feedback.take() {
                state.next_verifier_feedback.get_or_insert(feedback);
            }
            if state
                .host
                .as_ref()
                .is_some_and(|host| Arc::ptr_eq(host, &scheduled_host))
            {
                state.host = None;
            }
            if let (Some(turn_key), Some(goal)) = (next_turn_key, state.snapshot.goal.as_ref())
                && goal.status == GoalStatus::Active
            {
                let permit = new_permit(goal);
                state.current_permit_host = state.host.clone();
                state.current_turn_key = Some(turn_key);
                state.current_permit = Some(permit);
                state.current_turn_feedback = state.next_verifier_feedback.take();
                state.continuation_queued = false;
                state.snapshot.activity = GoalActivity::Running;
                false
            } else {
                state.snapshot.activity = GoalActivity::Idle;
                true
            }
        };
        self.broadcast(None);
        if should_continue {
            self.queue_continuation(None);
        }
    }

    fn promote_queued_turn(state: &mut RuntimeState) -> bool {
        let Some(turn_key) = state.queued_turn_key.take() else {
            return false;
        };
        let Some(goal) = state.snapshot.goal.as_ref() else {
            state.queued_turn_key = Some(turn_key);
            return false;
        };
        if goal.status != GoalStatus::Active || state.current_permit.is_some() {
            state.queued_turn_key = Some(turn_key);
            return false;
        }
        let permit = new_permit(goal);
        state.continuation_queued = false;
        state.current_permit_host = state.host.clone();
        state.current_turn_key = Some(turn_key);
        state.current_turn_feedback = state.next_verifier_feedback.take();
        state.current_permit = Some(permit);
        state.snapshot.activity = GoalActivity::Running;
        true
    }

    fn create_checkpoint_attempt(
        &self,
        permit: &GoalTurnPermit,
        goal: &GoalRecord,
        record_uuid: Option<String>,
    ) -> Option<Arc<CheckpointAttempt>> {
        (self.inner.options.evidence_source.is_some()
            && self.inner.options.checkpoint_verifier.is_some())
        .then(|| {
            Arc::new(CheckpointAttempt {
                permit: permit.clone(),
                goal: goal.clone(),
                record_uuid: record_uuid.unwrap_or_else(new_id),
                cancellation: CancellationToken::new(),
            })
        })
    }

    fn is_current_verification_attempt(
        state: &RuntimeState,
        attempt: &Arc<VerificationAttempt>,
    ) -> bool {
        state
            .verification_attempt
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, attempt))
            && state.snapshot.goal.as_ref().is_some_and(|goal| {
                goal.goal_id == attempt.permit.goal_id
                    && goal.revision == attempt.permit.revision
                    && goal.status == GoalStatus::Active
            })
            && state.snapshot.activity == GoalActivity::Verifying
    }

    fn is_current_checkpoint_attempt(
        state: &RuntimeState,
        attempt: &Arc<CheckpointAttempt>,
    ) -> bool {
        state
            .checkpoint_attempt
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, attempt))
            && state.snapshot.goal.as_ref().is_some_and(|goal| {
                goal.goal_id == attempt.permit.goal_id
                    && goal.revision == attempt.permit.revision
                    && goal.status == GoalStatus::Active
            })
            && state.snapshot.activity == GoalActivity::Verifying
    }

    fn check_available(&self) -> Result<(), GoalRuntimeError> {
        assert_available(&lock_state(&self.inner.state))
    }

    pub async fn prepare_restore(
        &self,
        records: &[GoalRecoveryRecord],
        checkpoint_window: Option<GoalEvidenceCheckpointWindow>,
    ) -> Result<(), GoalRuntimeError> {
        let _restore = self.inner.restore_lock.lock().await;
        let _operation = self.inner.operation_lock.lock().await;
        {
            let mut state = lock_state(&self.inner.state);
            if state.disposed {
                return Err(GoalRuntimeError::Disposed);
            }
            if state.restored {
                return Ok(());
            }
            state.restore_activation_pending = true;
            state.prepared_checkpoint_window = checkpoint_window;
        }

        let recovery = recover_goal_from_records(records);
        if let GoalRecovery::Unsupported { reason } = &recovery {
            let mut state = lock_state(&self.inner.state);
            state.recovery_error = Some(reason.clone());
            state.restore_activation_pending = false;
            state.prepared_checkpoint_window = None;
            return Err(GoalRuntimeError::PersistenceUnavailable(reason.clone()));
        }

        let prepared = async move {
            let mut recovered_snapshot = None;
            let mut recovered_cause = None;
            let mut checkpoint = None;
            let mut blocked_audit = None;
            let mut next_feedback = None;
            match recovery {
                GoalRecovery::V2 { payload } => {
                    let mut snapshot = payload.snapshot;
                    snapshot.activity = GoalActivity::Idle;
                    blocked_audit = payload.blocked_audit.map(normalize_recovered_blocked_audit);
                    recovered_cause = Some(payload.cause);
                    if let Some(pending) = payload.checkpoint_pending
                        && let Some(goal) = snapshot.goal.as_ref()
                    {
                        checkpoint = self.create_checkpoint_attempt(
                            &pending.permit,
                            goal,
                            Some(pending.record_uuid),
                        );
                        if checkpoint.is_none() {
                            return Err(GoalRuntimeError::PersistenceUnavailable(
                                "Goal checkpoint recovery dependencies are unavailable".to_owned(),
                            ));
                        }
                        snapshot.activity = GoalActivity::Verifying;
                    }
                    if recovered_cause == Some(GoalStateCause::VerifierReject) {
                        next_feedback = snapshot
                            .goal
                            .as_ref()
                            .and_then(|goal| goal.last_reason.clone());
                    }
                    recovered_snapshot = Some(snapshot);
                }
                GoalRecovery::Legacy { objective } => {
                    let record_uuid = new_id();
                    let payload = create_migrated_goal_state(
                        &crate::goals::persistence::MigratedGoalStateInput {
                            objective,
                            goal_id: new_id(),
                            record_uuid: record_uuid.clone(),
                            now: now_ms(),
                        },
                    )
                    .map_err(GoalRuntimeError::PersistenceUnavailable)?;
                    self.inner
                        .options
                        .journal
                        .record_goal_state(record_uuid, payload.clone())
                        .await
                        .map_err(GoalRuntimeError::PersistenceUnavailable)?;
                    if lock_state(&self.inner.state).disposed {
                        return Err(GoalRuntimeError::Disposed);
                    }
                    recovered_snapshot = Some(payload.snapshot);
                    recovered_cause = Some(GoalStateCause::Migrated);
                }
                GoalRecovery::None | GoalRecovery::Unsupported { .. } => {}
            }
            if lock_state(&self.inner.state).disposed {
                return Err(GoalRuntimeError::Disposed);
            }
            let mut state = lock_state(&self.inner.state);
            if let Some(snapshot) = recovered_snapshot.clone() {
                state.snapshot = snapshot;
                state.recovery_cause = recovered_cause;
            }
            state.blocked_audit = blocked_audit;
            state.next_verifier_feedback = next_feedback;
            state.checkpoint_attempt = checkpoint;
            state.recovery_error = None;
            state.restored = true;
            state.prepared_restore_has_snapshot = recovered_snapshot.is_some();
            state.prepared_restore_cause = recovered_cause;
            Ok(())
        }
        .await;

        if let Err(error) = &prepared {
            let mut state = lock_state(&self.inner.state);
            state.restore_activation_pending = false;
            state.prepared_checkpoint_window = None;
            if !state.disposed {
                state.recovery_error = Some(error.to_string());
            }
        }
        prepared
    }

    pub async fn get_prepared_restore(&self) -> Result<(), GoalRuntimeError> {
        let _restore = self.inner.restore_lock.lock().await;
        let state = lock_state(&self.inner.state);
        assert_available(&state)?;
        if !state.restored {
            return Err(GoalRuntimeError::PersistenceUnavailable(
                "Goal restore preparation has not started".to_owned(),
            ));
        }
        Ok(())
    }

    pub async fn activate_restored_work(&self) -> Result<(), GoalRuntimeError> {
        {
            let _restore = self.inner.restore_lock.lock().await;
            let state = lock_state(&self.inner.state);
            assert_available(&state)?;
            if !state.restored {
                return Err(GoalRuntimeError::PersistenceUnavailable(
                    "Goal restore preparation has not started".to_owned(),
                ));
            }
        }
        let _activation = self.inner.activation_lock.lock().await;
        let (checkpoint, window, should_broadcast, cause) = {
            let mut state = lock_state(&self.inner.state);
            assert_available(&state)?;
            if !state.restored {
                return Err(GoalRuntimeError::PersistenceUnavailable(
                    "Goal restore preparation has not started".to_owned(),
                ));
            }
            if state.restore_activation_started {
                return Ok(());
            }
            state.restore_activation_started = true;
            state.restore_activation_pending = false;
            (
                state.checkpoint_attempt.clone(),
                state.prepared_checkpoint_window.take(),
                state.prepared_restore_has_snapshot,
                state.prepared_restore_cause,
            )
        };
        if should_broadcast {
            self.broadcast(cause);
        }
        let Some(attempt) = checkpoint else {
            self.queue_continuation(None);
            return Ok(());
        };
        if let Err(_error) = self.run_checkpoint(attempt.clone(), window).await {
            self.settle_dangling_attempt(&attempt.permit).await;
        }
        Ok(())
    }

    pub async fn restore(&self, records: &[GoalRecoveryRecord]) -> Result<(), GoalRuntimeError> {
        self.prepare_restore(records, None).await?;
        self.activate_restored_work().await
    }

    pub fn bind_host(
        &self,
        host: Arc<dyn GoalTurnHost>,
    ) -> Result<GoalHostBinding, GoalRuntimeError> {
        self.check_available()?;
        lock_state(&self.inner.state).host = Some(host.clone());
        self.queue_continuation(None);
        Ok(GoalHostBinding {
            runtime: Arc::downgrade(&self.inner),
            host,
        })
    }

    pub fn begin_turn(
        &self,
        turn_key: impl Into<String>,
    ) -> Result<Option<GoalTurnPermit>, GoalRuntimeError> {
        let turn_key = turn_key.into();
        let mut state = lock_state(&self.inner.state);
        assert_available(&state)?;
        if state
            .snapshot
            .goal
            .as_ref()
            .is_none_or(|goal| goal.status != GoalStatus::Active)
        {
            return Ok(None);
        }
        if state.snapshot.activity == GoalActivity::Verifying
            || state.pending_proposal.is_some()
            || state.verification_attempt.is_some()
            || state.checkpoint_attempt.is_some()
        {
            state.queued_turn_key.get_or_insert(turn_key);
            state.continuation_queued = false;
            return Ok(None);
        }
        if let Some(permit) = state.current_permit.as_ref() {
            if state.current_turn_key.as_deref() == Some(turn_key.as_str()) {
                return Ok(Some(permit.clone()));
            }
            state.queued_turn_key.get_or_insert(turn_key);
            state.continuation_queued = false;
            return Ok(None);
        }
        state.continuation_queued = false;
        let goal = state.snapshot.goal.as_ref().expect("checked active goal");
        let permit = new_permit(goal);
        state.current_permit_host = state.host.clone();
        state.current_turn_key = Some(turn_key);
        state.current_turn_feedback = state.next_verifier_feedback.take();
        state.current_permit = Some(permit.clone());
        state.snapshot.activity = GoalActivity::Running;
        drop(state);
        self.broadcast(None);
        Ok(Some(permit))
    }

    pub async fn release_turn(&self, turn_key: &str) -> Result<bool, GoalRuntimeError> {
        let _operation = self.inner.operation_lock.lock().await;
        let (released, snapshot_changed, no_current_permit) = {
            let mut state = lock_state(&self.inner.state);
            assert_available(&state)?;
            let mut released = false;
            let mut snapshot_changed = false;
            if state.queued_turn_key.as_deref() == Some(turn_key) {
                state.queued_turn_key = None;
                released = true;
            }
            if state.current_permit.is_some() && state.current_turn_key.as_deref() == Some(turn_key)
            {
                if let Some(feedback) = state.current_turn_feedback.take() {
                    state.next_verifier_feedback.get_or_insert(feedback);
                }
                state.current_permit = None;
                state.current_permit_host = None;
                state.current_turn_key = None;
                state.current_proposal = None;
                state.snapshot.activity = GoalActivity::Idle;
                snapshot_changed = true;
                if state.queued_turn_key.is_some()
                    && state
                        .snapshot
                        .goal
                        .as_ref()
                        .is_some_and(|goal| goal.status == GoalStatus::Active)
                    && state.pending_proposal.is_none()
                    && state.verification_attempt.is_none()
                {
                    Self::promote_queued_turn(&mut state);
                }
                released = true;
            }
            (released, snapshot_changed, state.current_permit.is_none())
        };
        if snapshot_changed {
            self.broadcast(None);
        }
        if released && no_current_permit {
            self.queue_continuation(None);
        }
        Ok(released)
    }

    pub fn permit_for_turn(
        &self,
        turn_key: &str,
    ) -> Result<Option<GoalTurnPermit>, GoalRuntimeError> {
        let state = lock_state(&self.inner.state);
        assert_available(&state)?;
        Ok(state
            .current_permit
            .as_ref()
            .filter(|_| state.current_turn_key.as_deref() == Some(turn_key))
            .cloned())
    }

    pub fn get_verifier_feedback(
        &self,
        permit: &GoalTurnPermit,
    ) -> Result<Option<String>, GoalRuntimeError> {
        let state = lock_state(&self.inner.state);
        assert_available(&state)?;
        if !is_current_permit(&state, permit) {
            return Err(GoalRuntimeError::StalePermit);
        }
        Ok(state.current_turn_feedback.clone())
    }

    /// Run executor work with this permit bound to the async task-local Goal
    /// context. Tool schedulers can leave ordinary turns unscoped by calling
    /// this only when their request carries a Goal permit.
    pub async fn scope_turn<F: Future>(
        &self,
        permit: &GoalTurnPermit,
        future: F,
    ) -> Result<F::Output, GoalRuntimeError> {
        self.get_snapshot_for_permit(permit)?;
        Ok(run_with_goal_turn_context(permit.clone(), future).await)
    }

    pub async fn dispatch(
        &self,
        request: GoalControlRequest,
    ) -> Result<GoalSnapshotV2, GoalRuntimeError> {
        let _operation = self.inner.operation_lock.lock().await;
        let record_uuid = new_id();
        let (next_snapshot, payload) = {
            let state = lock_state(&self.inner.state);
            assert_available(&state)?;
            let starts_revision = matches!(
                &request,
                GoalControlRequest::Create { .. }
                    | GoalControlRequest::Replace { .. }
                    | GoalControlRequest::Edit { .. }
            );
            let next_goal = reduce_goal_control(
                state.snapshot.goal.as_ref(),
                &GoalControlTransition {
                    request: request.clone(),
                    now: now_ms(),
                    next_goal_id: new_id(),
                    cursor: if starts_revision {
                        TranscriptCursor {
                            record_id: Some(record_uuid.clone()),
                        }
                    } else {
                        self.inner.options.journal.get_transcript_cursor()
                    },
                },
            )?;
            let snapshot = GoalSnapshotV2 {
                v: GOAL_STATE_VERSION,
                goal: next_goal,
                activity: GoalActivity::Idle,
            };
            let cause = match &request {
                GoalControlRequest::Create { .. } => GoalStateCause::Create,
                GoalControlRequest::Replace { .. } => GoalStateCause::Replace,
                GoalControlRequest::Edit { .. } => GoalStateCause::Edit,
                GoalControlRequest::Pause { .. } => GoalStateCause::Pause,
                GoalControlRequest::Resume { .. } => GoalStateCause::Resume,
                GoalControlRequest::Clear { .. } => GoalStateCause::Clear,
            };
            (snapshot.clone(), goal_state_payload(cause, snapshot))
        };
        self.inner
            .options
            .journal
            .record_goal_state(record_uuid, payload)
            .await
            .map_err(GoalRuntimeError::PersistenceUnavailable)?;
        let action = match &request {
            GoalControlRequest::Create { .. } => GoalStateCause::Create,
            GoalControlRequest::Replace { .. } => GoalStateCause::Replace,
            GoalControlRequest::Edit { .. } => GoalStateCause::Edit,
            GoalControlRequest::Pause { .. } => GoalStateCause::Pause,
            GoalControlRequest::Resume { .. } => GoalStateCause::Resume,
            GoalControlRequest::Clear { .. } => GoalStateCause::Clear,
        };
        let invalidates_permit = matches!(
            action,
            GoalStateCause::Create
                | GoalStateCause::Replace
                | GoalStateCause::Edit
                | GoalStateCause::Pause
                | GoalStateCause::Clear
        );
        let (snapshot, invalidated_host) = {
            let mut state = lock_state(&self.inner.state);
            if state.disposed {
                return Err(GoalRuntimeError::Disposed);
            }
            let invalidated_host = state
                .current_permit_host
                .clone()
                .or_else(|| state.host.clone());
            if invalidates_permit {
                Self::invalidate_attempts(&mut state, &format!("Goal {}", action.as_str()));
                state.current_permit = None;
                state.current_permit_host = None;
                state.current_turn_key = None;
                state.queued_turn_key = None;
                state.current_proposal = None;
                state.pending_proposal = None;
                state.blocked_audit = None;
                state.next_verifier_feedback = None;
                state.current_turn_feedback = None;
                state.continuation_queued = false;
            } else if action == GoalStateCause::Resume {
                state.blocked_audit = None;
            }
            state.snapshot = GoalSnapshotV2 {
                activity: if state.current_permit.is_some() && action == GoalStateCause::Resume {
                    GoalActivity::Running
                } else {
                    GoalActivity::Idle
                },
                ..next_snapshot
            };
            if action == GoalStateCause::Resume {
                Self::promote_queued_turn(&mut state);
            }
            (state.snapshot.clone(), invalidated_host)
        };
        self.broadcast(Some(action));
        if invalidates_permit {
            Self::preempt_host(invalidated_host, &format!("Goal {}", action.as_str()));
        }
        if action == GoalStateCause::Resume
            || (action != GoalStateCause::Clear
                && snapshot
                    .goal
                    .as_ref()
                    .is_some_and(|goal| goal.status == GoalStatus::Active))
        {
            self.queue_continuation(None);
        }
        Ok(self.get_snapshot())
    }

    pub async fn finish_turn(&self, permit: &GoalTurnPermit) -> Result<(), GoalRuntimeError> {
        let attempts = {
            let _operation = self.inner.operation_lock.lock().await;
            let (
                record_uuid,
                persisted_snapshot,
                pending_checkpoint,
                active_proposal,
                blocked_audit,
            ) = {
                let state = lock_state(&self.inner.state);
                assert_available(&state)?;
                if !is_current_permit(&state, permit) {
                    return Err(GoalRuntimeError::StalePermit);
                }
                let goal = state.snapshot.goal.as_ref().expect("permit requires goal");
                let finished_goal = reduce_goal_turn_finished(
                    goal,
                    &GoalTurnFinishedTransition {
                        now: now_ms(),
                        last_reason: None,
                    },
                )
                .map_err(|error: GoalInvalidTransitionError| {
                    GoalRuntimeError::Reducer(GoalReducerError::InvalidTransition(error))
                })?;
                let snapshot = GoalSnapshotV2 {
                    v: GOAL_STATE_VERSION,
                    goal: Some(finished_goal),
                    activity: GoalActivity::Idle,
                };
                let proposal = state.current_proposal.as_ref().filter(|_| {
                    snapshot
                        .goal
                        .as_ref()
                        .is_some_and(|goal| goal.status == GoalStatus::Active)
                });
                let checkpoint = if proposal.is_none() {
                    self.create_checkpoint_attempt(
                        permit,
                        snapshot.goal.as_ref().expect("finished goal"),
                        None,
                    )
                } else {
                    None
                };
                let active_proposal = proposal.cloned();
                let blocked_audit = active_proposal
                    .as_ref()
                    .and_then(|proposal| proposal.blocked_audit_candidate.clone());
                (
                    new_id(),
                    snapshot,
                    checkpoint,
                    active_proposal,
                    blocked_audit,
                )
            };

            let mut payload =
                goal_state_payload(GoalStateCause::TurnFinished, persisted_snapshot.clone());
            payload.checkpoint_pending =
                pending_checkpoint
                    .as_ref()
                    .map(|attempt| GoalCheckpointPending {
                        permit: attempt.permit.clone(),
                        record_uuid: attempt.record_uuid.clone(),
                    });
            payload.blocked_audit = blocked_audit.clone();
            self.inner
                .options
                .journal
                .record_goal_state(record_uuid, payload)
                .await
                .map_err(GoalRuntimeError::Journal)?;

            let (verification, checkpoint, verifying) =
                {
                    let mut state = lock_state(&self.inner.state);
                    assert_available(&state)?;
                    let next_turn_key = state.queued_turn_key.clone();
                    if active_proposal.is_some() {
                        state.blocked_audit = blocked_audit.clone();
                    } else if persisted_snapshot
                        .goal
                        .as_ref()
                        .is_some_and(|goal| goal.status == GoalStatus::Active)
                    {
                        state.blocked_audit = None;
                    }
                    state.pending_proposal = active_proposal.as_ref().and_then(|proposal| {
                        (proposal.ready_for_verification && self.inner.options.verifier.is_none())
                            .then(|| GoalPendingProposal {
                                permit: permit.clone(),
                                proposal: proposal.proposal.clone(),
                            })
                    });
                    state.verification_attempt = active_proposal.as_ref().and_then(|proposal| {
                        (proposal.ready_for_verification && self.inner.options.verifier.is_some())
                            .then(|| {
                                Arc::new(VerificationAttempt {
                                    permit: permit.clone(),
                                    proposal: proposal.proposal.clone(),
                                    goal: persisted_snapshot
                                        .goal
                                        .as_ref()
                                        .expect("finished goal")
                                        .clone(),
                                    cancellation: CancellationToken::new(),
                                })
                            })
                    });
                    state.checkpoint_attempt = pending_checkpoint.clone();
                    let verifying = state.pending_proposal.is_some()
                        || state.verification_attempt.is_some()
                        || state.checkpoint_attempt.is_some();
                    state.snapshot = GoalSnapshotV2 {
                        activity: if verifying {
                            GoalActivity::Verifying
                        } else {
                            GoalActivity::Idle
                        },
                        ..persisted_snapshot.clone()
                    };
                    state.current_permit = None;
                    state.current_permit_host = None;
                    state.current_turn_key = None;
                    state.current_turn_feedback = None;
                    state.queued_turn_key = if verifying {
                        next_turn_key.clone()
                    } else {
                        None
                    };
                    state.continuation_queued = false;
                    state.current_proposal = None;
                    if !verifying
                        && next_turn_key.is_some()
                        && state
                            .snapshot
                            .goal
                            .as_ref()
                            .is_some_and(|goal| goal.status == GoalStatus::Active)
                    {
                        Self::promote_queued_turn(&mut state);
                    }
                    (
                        state.verification_attempt.clone(),
                        state.checkpoint_attempt.clone(),
                        verifying,
                    )
                };
            self.broadcast(Some(GoalStateCause::TurnFinished));
            if !verifying && verification.is_none() && checkpoint.is_none() {
                let has_permit = lock_state(&self.inner.state).current_permit.is_some();
                if !has_permit {
                    self.queue_continuation(None);
                }
            }
            (verification, checkpoint)
        };

        if let Some(attempt) = attempts.0 {
            if let Err(error) = self.run_verification(attempt.clone()).await {
                self.settle_dangling_attempt(&attempt.permit).await;
                return Err(error);
            }
        } else if let Some(attempt) = attempts.1 {
            if self.run_checkpoint(attempt.clone(), None).await.is_err() {
                self.settle_dangling_attempt(&attempt.permit).await;
            }
        }
        Ok(())
    }

    pub async fn get_goal_for_worker(
        &self,
        permit: &GoalTurnPermit,
    ) -> Result<GoalWorkerView, GoalRuntimeError> {
        let (goal, feedback, evidence_source) = {
            let state = lock_state(&self.inner.state);
            assert_available(&state)?;
            if !is_current_permit(&state, permit) {
                return Err(GoalRuntimeError::StalePermit);
            }
            let goal = state
                .snapshot
                .goal
                .clone()
                .ok_or(GoalRuntimeError::StalePermit)?;
            (
                goal,
                state.current_turn_feedback.clone(),
                self.inner.options.evidence_source.clone(),
            )
        };
        let Some(evidence_source) = evidence_source else {
            return Ok(GoalWorkerView {
                goal_id: goal.goal_id,
                revision: goal.revision,
                objective: goal.objective,
                evidence_cursor: goal.evidence_cursor,
                evidence_catalog: None,
                verifier_feedback: feedback,
            });
        };
        evidence_source
            .flush()
            .await
            .map_err(GoalRuntimeError::InvalidOperation)?;
        let records = evidence_source
            .read_active_transcript_chain()
            .await
            .map_err(GoalRuntimeError::InvalidOperation)?;
        let evidence_catalog = build_goal_evidence_catalog(&GoalEvidenceContext {
            records: &records,
            goal: &goal,
            permit,
        })
        .map_err(|error| GoalRuntimeError::InvalidOperation(error.to_string()))?;
        let state = lock_state(&self.inner.state);
        assert_available(&state)?;
        if !is_current_permit(&state, permit) {
            return Err(GoalRuntimeError::StalePermit);
        }
        Ok(GoalWorkerView {
            goal_id: goal.goal_id,
            revision: goal.revision,
            objective: goal.objective,
            evidence_cursor: goal.evidence_cursor,
            evidence_catalog: Some(evidence_catalog),
            verifier_feedback: feedback,
        })
    }

    pub fn record_terminal_proposal(
        &self,
        permit: &GoalTurnPermit,
        proposal: GoalTerminalProposal,
    ) -> Result<GoalProposalReceipt, GoalRuntimeError> {
        let mut state = lock_state(&self.inner.state);
        assert_available(&state)?;
        if !is_current_permit(&state, permit) {
            return Err(GoalRuntimeError::StalePermit);
        }
        if let Some(reason) = validate_goal_proposal_reason(&proposal.reason) {
            return Err(GoalRuntimeError::InvalidOperation(reason));
        }
        if let Some(current) = &state.current_proposal {
            return Ok(GoalProposalReceipt {
                recorded: false,
                ready_for_verification: current.ready_for_verification,
            });
        }
        let blocked_audit_candidate = if is_repeated_blocker_proposal(&proposal) {
            let fingerprint = format!(
                "{}\n{}",
                proposal
                    .blocker_kind
                    .map(|kind| format!("{kind:?}").to_lowercase())
                    .unwrap_or_else(|| "repeated".to_owned()),
                proposal.reason
            );
            let previous = state
                .blocked_audit
                .as_ref()
                .filter(|audit| audit.fingerprint == fingerprint);
            let mut turn_ids = previous
                .map(|audit| audit.turn_ids.clone())
                .unwrap_or_default();
            turn_ids.push(permit.turn_id.clone());
            turn_ids = turn_ids.into_iter().rev().take(3).collect::<Vec<_>>();
            turn_ids.reverse();
            Some(GoalBlockedAudit {
                fingerprint,
                count: previous.map_or(1, |audit| audit.count.saturating_add(1).min(3)),
                turn_ids,
            })
        } else {
            None
        };
        let ready_for_verification = blocked_audit_candidate
            .as_ref()
            .is_none_or(|audit| audit.count >= 3);
        state.current_proposal = Some(CurrentProposal {
            proposal,
            ready_for_verification,
            blocked_audit_candidate,
        });
        Ok(GoalProposalReceipt {
            recorded: true,
            ready_for_verification,
        })
    }

    pub fn take_pending_terminal_proposal(
        &self,
    ) -> Result<Option<GoalPendingProposal>, GoalRuntimeError> {
        let mut state = lock_state(&self.inner.state);
        assert_available(&state)?;
        Ok(state.pending_proposal.take())
    }

    pub fn dispose(&self) {
        let (invalidated_host, verification, checkpoint) = {
            let mut state = lock_state(&self.inner.state);
            if state.disposed {
                return;
            }
            state.disposed = true;
            let invalidated_host = state
                .current_permit_host
                .clone()
                .or_else(|| state.host.clone());
            state.current_permit = None;
            state.current_permit_host = None;
            state.current_turn_key = None;
            state.queued_turn_key = None;
            state.continuation_queued = false;
            state.current_proposal = None;
            state.pending_proposal = None;
            let verification = state.verification_attempt.take();
            let checkpoint = state.checkpoint_attempt.take();
            state.blocked_audit = None;
            state.next_verifier_feedback = None;
            state.current_turn_feedback = None;
            state.host = None;
            (invalidated_host, verification, checkpoint)
        };
        if let Some(attempt) = verification {
            attempt
                .cancellation
                .cancel_with_reason("Goal runtime disposed");
        }
        if let Some(attempt) = checkpoint {
            attempt
                .cancellation
                .cancel_with_reason("Goal runtime disposed");
        }
        Self::preempt_host(invalidated_host, "Goal runtime disposed");
    }

    async fn run_verification(
        &self,
        attempt: Arc<VerificationAttempt>,
    ) -> Result<(), GoalRuntimeError> {
        let (Some(evidence_source), Some(verifier)) = (
            self.inner.options.evidence_source.clone(),
            self.inner.options.verifier.clone(),
        ) else {
            return Ok(());
        };

        if let Err(reason) = evidence_source.flush().await {
            if attempt.cancellation.is_cancelled() {
                return Ok(());
            }
            self.apply_verification_outcome(
                &attempt,
                VerificationOutcome::UsageLimited {
                    reason,
                    limit_kind: None,
                },
            )
            .await?;
            return Ok(());
        }
        if attempt.cancellation.is_cancelled() {
            return Ok(());
        }
        let records = match evidence_source.read_active_transcript_chain().await {
            Ok(records) => records,
            Err(reason) => {
                if attempt.cancellation.is_cancelled() {
                    return Ok(());
                }
                self.apply_verification_outcome(
                    &attempt,
                    VerificationOutcome::UsageLimited {
                        reason,
                        limit_kind: None,
                    },
                )
                .await?;
                return Ok(());
            }
        };
        if attempt.cancellation.is_cancelled() {
            return Ok(());
        }
        let validated = match validate_goal_evidence_references(&GoalEvidenceValidationInput {
            context: GoalEvidenceContext {
                records: &records,
                goal: &attempt.goal,
                permit: &attempt.permit,
            },
            proposal: &attempt.proposal,
        }) {
            Ok(validated) => validated,
            Err(EvidenceSourceUnavailableErrorOrReference::Reference(error)) => {
                let outcome = if error.code == InvalidGoalEvidenceReferenceCode::CatalogTruncated {
                    VerificationOutcome::UsageLimited {
                        reason: error.message,
                        limit_kind: Some(GoalLimitKind::EvidenceCatalog),
                    }
                } else {
                    VerificationOutcome::Decision(GoalVerificationResult {
                        decision: GoalVerificationDecision::Reject,
                        reason: error.message,
                    })
                };
                self.apply_verification_outcome(&attempt, outcome).await?;
                return Ok(());
            }
            Err(EvidenceSourceUnavailableErrorOrReference::Source(error)) => {
                self.apply_verification_outcome(
                    &attempt,
                    VerificationOutcome::UsageLimited {
                        reason: error.to_string(),
                        limit_kind: None,
                    },
                )
                .await?;
                return Ok(());
            }
        };
        let current_delivered_output = validated
            .cited_records
            .iter()
            .filter(|record| {
                record.proof_kind == crate::goals::protocol::GoalEvidenceProofKind::DeliveredOutput
                    && record.turn_id == attempt.permit.turn_id
            })
            .map(|record| record.content.clone())
            .collect::<Vec<_>>();
        let input = GoalVerificationInput {
            goal: GoalVerificationGoal {
                goal_id: attempt.goal.goal_id.clone(),
                revision: attempt.goal.revision,
                objective: attempt.goal.objective.clone(),
            },
            current_turn_id: attempt.permit.turn_id.clone(),
            proposal: GoalVerificationProposal {
                status: attempt.proposal.status,
                reason: attempt.proposal.reason.clone(),
                evidence_refs: attempt.proposal.evidence_refs.clone(),
                blocker_kind: attempt.proposal.blocker_kind,
            },
            evidence: validated.cited_records,
            current_delivered_output: (!current_delivered_output.is_empty())
                .then_some(current_delivered_output),
            blocked_policy: (attempt.proposal.status == GoalTerminalProposalStatus::Blocked)
                .then(|| BLOCKED_POLICY.to_owned()),
        };
        if attempt.cancellation.is_cancelled() {
            return Ok(());
        }
        let verification =
            std::panic::AssertUnwindSafe(verifier.verify(input, &attempt.cancellation))
                .catch_unwind()
                .await;
        let outcome = match verification {
            Ok(Ok(result)) if valid_verifier_result(&result) => {
                VerificationOutcome::Decision(result)
            }
            Ok(Ok(_)) => VerificationOutcome::UsageLimited {
                reason: "Goal verifier returned invalid output".to_owned(),
                limit_kind: None,
            },
            Ok(Err(reason)) if !attempt.cancellation.is_cancelled() => {
                VerificationOutcome::UsageLimited {
                    reason,
                    limit_kind: None,
                }
            }
            Ok(Err(_)) | Err(_) if attempt.cancellation.is_cancelled() => return Ok(()),
            Ok(Err(_)) | Err(_) => VerificationOutcome::UsageLimited {
                reason: "Goal verifier failed".to_owned(),
                limit_kind: None,
            },
        };
        self.apply_verification_outcome(&attempt, outcome).await?;
        Ok(())
    }

    async fn apply_verification_outcome(
        &self,
        attempt: &Arc<VerificationAttempt>,
        outcome: VerificationOutcome,
    ) -> Result<(), GoalRuntimeError> {
        let checkpoint = self.record_verification_outcome(attempt, outcome).await?;
        if let Some(checkpoint) = checkpoint {
            if self.run_checkpoint(checkpoint.clone(), None).await.is_err() {
                self.settle_dangling_attempt(&checkpoint.permit).await;
            }
        }
        Ok(())
    }

    async fn record_verification_outcome(
        &self,
        attempt: &Arc<VerificationAttempt>,
        outcome: VerificationOutcome,
    ) -> Result<Option<Arc<CheckpointAttempt>>, GoalRuntimeError> {
        let _operation = self.inner.operation_lock.lock().await;
        {
            let state = lock_state(&self.inner.state);
            if !Self::is_current_verification_attempt(&state, attempt) {
                return Ok(None);
            }
        }
        match outcome {
            VerificationOutcome::Decision(result)
                if result.decision == GoalVerificationDecision::Accept =>
            {
                let (accepted, terminal) = {
                    let state = lock_state(&self.inner.state);
                    let goal = state
                        .snapshot
                        .goal
                        .as_ref()
                        .ok_or(GoalRuntimeError::StalePermit)?;
                    let now = now_ms();
                    let accepted_goal = GoalRecord {
                        active_time_ms: elapsed_active_time(goal, now),
                        updated_at: now,
                        last_reason: Some(result.reason.clone()),
                        ..goal.clone()
                    };
                    let accepted = GoalSnapshotV2 {
                        v: GOAL_STATE_VERSION,
                        goal: Some(accepted_goal.clone()),
                        activity: GoalActivity::Idle,
                    };
                    let terminal_goal = GoalRecord {
                        status: match attempt.proposal.status {
                            GoalTerminalProposalStatus::Complete => GoalStatus::Complete,
                            GoalTerminalProposalStatus::Blocked => GoalStatus::Blocked,
                        },
                        ..accepted_goal
                    };
                    let terminal = GoalSnapshotV2 {
                        v: GOAL_STATE_VERSION,
                        goal: Some(terminal_goal),
                        activity: GoalActivity::Idle,
                    };
                    (accepted, terminal)
                };
                self.inner
                    .options
                    .journal
                    .record_goal_state(
                        new_id(),
                        goal_state_payload(GoalStateCause::VerifierAccept, accepted),
                    )
                    .await
                    .map_err(GoalRuntimeError::Journal)?;
                {
                    let state = lock_state(&self.inner.state);
                    if !Self::is_current_verification_attempt(&state, attempt) {
                        return Ok(None);
                    }
                }
                let terminal_cause = match attempt.proposal.status {
                    GoalTerminalProposalStatus::Complete => GoalStateCause::Complete,
                    GoalTerminalProposalStatus::Blocked => GoalStateCause::Blocked,
                };
                self.inner
                    .options
                    .journal
                    .record_goal_state(
                        new_id(),
                        goal_state_payload(terminal_cause, terminal.clone()),
                    )
                    .await
                    .map_err(GoalRuntimeError::Journal)?;
                {
                    let mut state = lock_state(&self.inner.state);
                    if !Self::is_current_verification_attempt(&state, attempt) {
                        return Ok(None);
                    }
                    state.verification_attempt = None;
                    state.pending_proposal = None;
                    if attempt.proposal.status == GoalTerminalProposalStatus::Complete {
                        state.queued_turn_key = None;
                    }
                    state.continuation_queued = false;
                    state.next_verifier_feedback = None;
                    state.current_turn_feedback = None;
                    state.snapshot = terminal;
                }
                self.broadcast(Some(terminal_cause));
                Ok(None)
            }
            VerificationOutcome::UsageLimited { reason, limit_kind } => {
                let limited = {
                    let state = lock_state(&self.inner.state);
                    let goal = state
                        .snapshot
                        .goal
                        .as_ref()
                        .ok_or(GoalRuntimeError::StalePermit)?;
                    let now = now_ms();
                    GoalSnapshotV2 {
                        v: GOAL_STATE_VERSION,
                        goal: Some(GoalRecord {
                            status: GoalStatus::UsageLimited,
                            active_time_ms: elapsed_active_time(goal, now),
                            updated_at: now,
                            last_reason: Some(reason),
                            limit_kind,
                            ..goal.clone()
                        }),
                        activity: GoalActivity::Idle,
                    }
                };
                self.inner
                    .options
                    .journal
                    .record_goal_state(
                        new_id(),
                        goal_state_payload(GoalStateCause::UsageLimited, limited.clone()),
                    )
                    .await
                    .map_err(GoalRuntimeError::Journal)?;
                {
                    let mut state = lock_state(&self.inner.state);
                    if !Self::is_current_verification_attempt(&state, attempt) {
                        return Ok(None);
                    }
                    state.verification_attempt = None;
                    state.pending_proposal = None;
                    state.continuation_queued = false;
                    state.next_verifier_feedback = None;
                    state.current_turn_feedback = None;
                    state.snapshot = limited;
                }
                self.broadcast(Some(GoalStateCause::UsageLimited));
                Ok(None)
            }
            VerificationOutcome::Decision(result) => {
                let checkpoint = if is_repeated_blocker_proposal(&attempt.proposal) {
                    None
                } else {
                    let state = lock_state(&self.inner.state);
                    state.snapshot.goal.as_ref().and_then(|goal| {
                        self.create_checkpoint_attempt(&attempt.permit, goal, None)
                    })
                };
                let rejected = {
                    let state = lock_state(&self.inner.state);
                    let goal = state
                        .snapshot
                        .goal
                        .as_ref()
                        .ok_or(GoalRuntimeError::StalePermit)?;
                    let now = now_ms();
                    GoalSnapshotV2 {
                        v: GOAL_STATE_VERSION,
                        goal: Some(GoalRecord {
                            active_time_ms: elapsed_active_time(goal, now),
                            updated_at: now,
                            last_reason: Some(result.reason.clone()),
                            ..goal.clone()
                        }),
                        activity: GoalActivity::Idle,
                    }
                };
                let mut payload =
                    goal_state_payload(GoalStateCause::VerifierReject, rejected.clone());
                payload.checkpoint_pending =
                    checkpoint.as_ref().map(|attempt| GoalCheckpointPending {
                        permit: attempt.permit.clone(),
                        record_uuid: attempt.record_uuid.clone(),
                    });
                payload.blocked_audit = lock_state(&self.inner.state).blocked_audit.clone();
                self.inner
                    .options
                    .journal
                    .record_goal_state(new_id(), payload)
                    .await
                    .map_err(GoalRuntimeError::Journal)?;
                {
                    let mut state = lock_state(&self.inner.state);
                    if !Self::is_current_verification_attempt(&state, attempt) {
                        return Ok(None);
                    }
                    state.verification_attempt = None;
                    state.pending_proposal = None;
                    state.checkpoint_attempt = checkpoint.clone();
                    state.snapshot = GoalSnapshotV2 {
                        activity: if checkpoint.is_some() {
                            GoalActivity::Verifying
                        } else {
                            GoalActivity::Idle
                        },
                        ..rejected
                    };
                    state.next_verifier_feedback = Some(result.reason);
                    state.current_turn_feedback = None;
                    if checkpoint.is_some() {
                        state.continuation_queued = false;
                    }
                }
                if let Some(checkpoint) = checkpoint {
                    self.broadcast(Some(GoalStateCause::VerifierReject));
                    Ok(Some(checkpoint))
                } else {
                    let promoted = {
                        let mut state = lock_state(&self.inner.state);
                        Self::promote_queued_turn(&mut state)
                    };
                    if promoted || !self.queue_continuation(Some(GoalStateCause::VerifierReject)) {
                        self.broadcast(Some(GoalStateCause::VerifierReject));
                    }
                    Ok(None)
                }
            }
        }
    }

    async fn run_checkpoint(
        &self,
        attempt: Arc<CheckpointAttempt>,
        prepared_window: Option<GoalEvidenceCheckpointWindow>,
    ) -> Result<(), GoalRuntimeError> {
        let evidence_source = self.inner.options.evidence_source.clone();
        let checkpoint_verifier = self.inner.options.checkpoint_verifier.clone();
        if evidence_source.is_none() || checkpoint_verifier.is_none() {
            self.record_checkpoint_failure(
                &attempt,
                "Goal checkpoint recovery dependencies are unavailable".to_owned(),
                None,
            )
            .await?;
            return Ok(());
        }
        let mut window = prepared_window;
        if window.is_none() {
            let source = evidence_source.expect("checked source");
            if let Err(reason) = source.flush().await {
                self.record_checkpoint_failure(&attempt, reason, None)
                    .await?;
                return Ok(());
            }
            if attempt.cancellation.is_cancelled() {
                return Ok(());
            }
            let records = match source.read_active_transcript_chain().await {
                Ok(records) => records,
                Err(reason) => {
                    self.record_checkpoint_failure(&attempt, reason, None)
                        .await?;
                    return Ok(());
                }
            };
            if attempt.cancellation.is_cancelled() {
                return Ok(());
            }
            window = match build_goal_evidence_checkpoint_window(&GoalEvidenceContext {
                records: &records,
                goal: &attempt.goal,
                permit: &attempt.permit,
            }) {
                Ok(window) => Some(window),
                Err(error) => {
                    if matches!(
                        &error,
                        EvidenceSourceUnavailableErrorOrReference::Source(source)
                            if source.code == EvidenceSourceUnavailableCode::CurrentTurnNotTail
                    ) {
                        self.finish_checkpoint_check(&attempt).await?;
                    } else {
                        self.record_checkpoint_failure(&attempt, error.to_string(), None)
                            .await?;
                    }
                    return Ok(());
                }
            };
        }
        let window = window.expect("prepared or read checkpoint window");
        if window.truncated {
            self.record_checkpoint_failure(
                &attempt,
                GOAL_EVIDENCE_CATALOG_EXHAUSTED_REASON.to_owned(),
                Some(GoalLimitKind::EvidenceCatalog),
            )
            .await?;
            return Ok(());
        }
        if !window.should_checkpoint {
            self.finish_checkpoint_check(&attempt).await?;
            return Ok(());
        }
        let checkpoint_verifier = checkpoint_verifier.expect("checked verifier");
        let input = GoalCheckpointVerifierInput {
            goal: crate::goals::checkpoint::GoalCheckpointVerifierGoal {
                goal_id: attempt.goal.goal_id.clone(),
                revision: attempt.goal.revision,
                objective: attempt.goal.objective.clone(),
            },
            previous_claims: window.previous_claims.clone(),
            evidence: window.evidence.clone(),
        };
        let verification = std::panic::AssertUnwindSafe(
            checkpoint_verifier.verify_checkpoint(input, &attempt.cancellation),
        )
        .catch_unwind()
        .await;
        if attempt.cancellation.is_cancelled() {
            return Ok(());
        }
        let result = match verification {
            Ok(Ok(result)) => result,
            Ok(Err(GoalCheckpointVerifierError::InputTooLarge { .. })) => {
                self.record_checkpoint_failure(
                    &attempt,
                    GOAL_CHECKPOINT_REQUEST_TOO_LARGE_REASON.to_owned(),
                    Some(GoalLimitKind::CheckpointRequest),
                )
                .await?;
                return Ok(());
            }
            Ok(Err(_)) | Err(_) => {
                // A transient or malformed verifier response is bookkeeping;
                // it must not stall a healthy Goal turn.
                self.finish_checkpoint_check(&attempt).await?;
                return Ok(());
            }
        };
        let checkpoint = match materialize_goal_evidence_checkpoint(
            attempt.record_uuid.clone(),
            now_ms(),
            &window.previous_claims,
            &window.evidence,
            &result,
        ) {
            Ok(checkpoint) => checkpoint,
            Err(error) => {
                self.finish_checkpoint_check(&attempt).await?;
                let _ = error;
                return Ok(());
            }
        };
        self.record_checkpoint(&attempt, checkpoint).await
    }

    async fn finish_checkpoint_check(
        &self,
        attempt: &Arc<CheckpointAttempt>,
    ) -> Result<(), GoalRuntimeError> {
        let _operation = self.inner.operation_lock.lock().await;
        let checked = {
            let state = lock_state(&self.inner.state);
            if !Self::is_current_checkpoint_attempt(&state, attempt) {
                return Ok(());
            }
            let goal = state
                .snapshot
                .goal
                .as_ref()
                .ok_or(GoalRuntimeError::StalePermit)?;
            let now = now_ms();
            GoalSnapshotV2 {
                v: GOAL_STATE_VERSION,
                goal: Some(GoalRecord {
                    active_time_ms: elapsed_active_time(goal, now),
                    updated_at: now,
                    ..goal.clone()
                }),
                activity: GoalActivity::Idle,
            }
        };
        let persisted_cause = if lock_state(&self.inner.state)
            .next_verifier_feedback
            .is_none()
        {
            GoalStateCause::Checkpoint
        } else {
            GoalStateCause::VerifierReject
        };
        let mut payload = goal_state_payload(persisted_cause, checked.clone());
        payload.blocked_audit = lock_state(&self.inner.state).blocked_audit.clone();
        self.inner
            .options
            .journal
            .record_goal_state(attempt.record_uuid.clone(), payload)
            .await
            .map_err(GoalRuntimeError::Journal)?;
        let promoted = {
            let mut state = lock_state(&self.inner.state);
            if !Self::is_current_checkpoint_attempt(&state, attempt) {
                return Ok(());
            }
            state.checkpoint_attempt = None;
            state.snapshot = checked;
            Self::promote_queued_turn(&mut state)
        };
        if promoted {
            self.broadcast(Some(GoalStateCause::Checkpoint));
        } else {
            self.queue_continuation(Some(GoalStateCause::Checkpoint));
        }
        Ok(())
    }

    async fn record_checkpoint_failure(
        &self,
        attempt: &Arc<CheckpointAttempt>,
        reason: String,
        limit_kind: Option<GoalLimitKind>,
    ) -> Result<(), GoalRuntimeError> {
        let _operation = self.inner.operation_lock.lock().await;
        let limited = {
            let state = lock_state(&self.inner.state);
            if !Self::is_current_checkpoint_attempt(&state, attempt) {
                return Ok(());
            }
            let goal = state
                .snapshot
                .goal
                .as_ref()
                .ok_or(GoalRuntimeError::StalePermit)?;
            let now = now_ms();
            GoalSnapshotV2 {
                v: GOAL_STATE_VERSION,
                goal: Some(GoalRecord {
                    status: GoalStatus::UsageLimited,
                    active_time_ms: elapsed_active_time(goal, now),
                    updated_at: now,
                    last_reason: Some(reason),
                    limit_kind,
                    ..goal.clone()
                }),
                activity: GoalActivity::Idle,
            }
        };
        self.inner
            .options
            .journal
            .record_goal_state(
                new_id(),
                goal_state_payload(GoalStateCause::UsageLimited, limited.clone()),
            )
            .await
            .map_err(GoalRuntimeError::Journal)?;
        {
            let mut state = lock_state(&self.inner.state);
            if !Self::is_current_checkpoint_attempt(&state, attempt) {
                return Ok(());
            }
            state.checkpoint_attempt = None;
            state.continuation_queued = false;
            state.current_turn_feedback = None;
            state.snapshot = limited;
        }
        self.broadcast(Some(GoalStateCause::UsageLimited));
        Ok(())
    }

    async fn record_checkpoint(
        &self,
        attempt: &Arc<CheckpointAttempt>,
        checkpoint: GoalEvidenceCheckpoint,
    ) -> Result<(), GoalRuntimeError> {
        let _operation = self.inner.operation_lock.lock().await;
        let persisted_cause = if lock_state(&self.inner.state)
            .next_verifier_feedback
            .is_none()
        {
            GoalStateCause::Checkpoint
        } else {
            GoalStateCause::VerifierReject
        };
        let snapshot = {
            let state = lock_state(&self.inner.state);
            if !Self::is_current_checkpoint_attempt(&state, attempt) {
                return Ok(());
            }
            let goal = state
                .snapshot
                .goal
                .as_ref()
                .ok_or(GoalRuntimeError::StalePermit)?;
            let now = now_ms();
            GoalSnapshotV2 {
                v: GOAL_STATE_VERSION,
                goal: Some(GoalRecord {
                    evidence_cursor: TranscriptCursor {
                        record_id: Some(attempt.record_uuid.clone()),
                    },
                    evidence_checkpoint: Some(checkpoint),
                    active_time_ms: elapsed_active_time(goal, now),
                    updated_at: now,
                    ..goal.clone()
                }),
                activity: GoalActivity::Idle,
            }
        };
        let mut payload = goal_state_payload(persisted_cause, snapshot.clone());
        payload.blocked_audit = lock_state(&self.inner.state).blocked_audit.clone();
        self.inner
            .options
            .journal
            .record_goal_state(attempt.record_uuid.clone(), payload)
            .await
            .map_err(GoalRuntimeError::Journal)?;
        let promoted = {
            let mut state = lock_state(&self.inner.state);
            if !Self::is_current_checkpoint_attempt(&state, attempt) {
                return Ok(());
            }
            state.checkpoint_attempt = None;
            state.snapshot = snapshot;
            Self::promote_queued_turn(&mut state)
        };
        if promoted {
            self.broadcast(Some(GoalStateCause::Checkpoint));
        } else {
            self.queue_continuation(Some(GoalStateCause::Checkpoint));
        }
        Ok(())
    }

    async fn settle_dangling_attempt(&self, permit: &GoalTurnPermit) {
        let _operation = self.inner.operation_lock.lock().await;
        let did_settle = {
            let mut state = lock_state(&self.inner.state);
            if state.disposed
                || state.snapshot.goal.as_ref().is_none_or(|goal| {
                    goal.goal_id != permit.goal_id || goal.revision != permit.revision
                })
                || (state.verification_attempt.is_none() && state.checkpoint_attempt.is_none())
            {
                return;
            }
            state.verification_attempt = None;
            state.checkpoint_attempt = None;
            state.pending_proposal = None;
            state.snapshot.activity = GoalActivity::Idle;
            Self::promote_queued_turn(&mut state)
        };
        self.broadcast(None);
        if !did_settle {
            self.queue_continuation(None);
        }
    }
}

pub fn create_goal_runtime(
    options: CreateGoalRuntimeOptions,
) -> Result<GoalRuntime, GoalRuntimeError> {
    GoalRuntime::create(options)
}

const BLOCKED_POLICY: &str = "A blocked Goal is resumable. It may be accepted immediately only when the evidence shows that new user authority or a material user choice is required, or that an external state change is required, and no meaningful in-scope work remains. An ordinary technical blocker requires evidence of the same cause from the current and two immediately preceding Goal turns. Difficulty, uncertainty, incomplete work, or a preference for clarification do not by themselves justify blocked.";

fn valid_verifier_result(result: &GoalVerificationResult) -> bool {
    let trimmed = result
        .reason
        .trim_matches(|character: char| character.is_whitespace() || character == '\u{feff}');
    !trimmed.is_empty()
        && result.reason.encode_utf16().count() <= GOAL_VERIFIER_REASON_MAX_CHARACTERS
}
