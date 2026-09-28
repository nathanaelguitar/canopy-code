//! In-memory lifecycle and usage state for workflow runs.
//!
//! This ports the state owned by `packages/core/src/agents/workflow-run-registry.ts`:
//! registrations, dispatch-state transitions, phases, dispatch/token counters,
//! recent logs, terminal settlement, queries, reset, and bounded terminal history.
//! Approval event bridges and callback channels remain integration work outside
//! this module; runner control is represented by a small trait at the registry
//! boundary.

use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{SystemTime, UNIX_EPOCH};
use std::{panic::AssertUnwindSafe, panic::catch_unwind};

use indexmap::IndexMap;
use serde_json::Value;
use thiserror::Error;

use crate::services::workflow_snapshot::{WorkflowMeta, WorkflowStatus};
use crate::utils::cancellation::CancellationToken;

/// At most this many terminal workflow rows are retained for the UI.
pub const MAX_RETAINED_TERMINAL_WORKFLOWS: usize = 10;

/// A workflow sandbox emits at most this many phase-start events.
pub const MAX_PHASE_ENTRIES: usize = 10_000;

/// Maximum number of recent workflow log lines retained for the UI.
pub const MAX_RECENT_LOG_LINES: usize = 100;

/// Non-terminal states emitted by the workflow dispatch scheduler.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkflowDispatchState {
    Running,
    Pausing,
    Paused,
}

impl WorkflowDispatchState {
    fn as_status(self) -> WorkflowStatus {
        match self {
            Self::Running => WorkflowStatus::Running,
            Self::Pausing => WorkflowStatus::Pausing,
            Self::Paused => WorkflowStatus::Paused,
        }
    }
}

/// Workflow statuses that still represent live work.
pub const fn is_active_workflow_status(status: WorkflowStatus) -> bool {
    matches!(
        status,
        WorkflowStatus::Running | WorkflowStatus::Pausing | WorkflowStatus::Paused
    )
}

/// Workflow statuses that represent a settled run.
///
/// Keep this as an explicit positive match so a future status addition does
/// not silently become terminal.
pub const fn is_terminal_workflow_status(status: WorkflowStatus) -> bool {
    matches!(
        status,
        WorkflowStatus::Completed | WorkflowStatus::Failed | WorkflowStatus::Cancelled
    )
}

/// Complete in-memory state for one workflow execution.
#[derive(Clone)]
pub struct WorkflowTask {
    pub id: String,
    pub kind: &'static str,
    pub run_id: String,
    pub description: String,
    pub meta: Option<WorkflowMeta>,
    pub status: WorkflowStatus,
    pub start_time: i64,
    pub end_time: Option<i64>,
    pub output_file: String,
    pub output_offset: u64,
    pub notified: bool,
    pub todo_work_chain_id: Option<String>,
    pub abort_controller: CancellationToken,
    pub is_backgrounded: bool,
    pub current_phase: Option<String>,
    pub phases: Vec<String>,
    pub agents_dispatched: u64,
    pub agents_completed: u64,
    pub recent_logs: Vec<String>,
    pub tokens_spent: u64,
    pub token_budget_total: Option<u64>,
    /// Insertion order matches the source JavaScript `Map`, including its
    /// `null` key for work dispatched before the first phase.
    pub per_phase_tokens: IndexMap<Option<String>, u64>,
    pub script: String,
    pub script_path: Option<String>,
    /// `None` is JavaScript `undefined`; `Some(Value::Null)` is explicit null.
    pub result: Option<Value>,
    pub error: Option<String>,
}

pub type SharedWorkflowTask = Arc<Mutex<WorkflowTask>>;

/// Control surface for a workflow runner handle.
///
/// The registry stores a shared handle while its run remains active. Pause and
/// resume request scheduler transitions; the scheduler reports the resulting
/// states separately through [`WorkflowRunRegistry::on_dispatch_state_change`].
pub trait WorkflowRunHandle: Send + Sync {
    fn run_id(&self) -> &str;
    fn abort(&self);
    fn pause(&self) -> bool;
    fn resume(&self) -> bool;
}

pub type SharedWorkflowRunHandle = Arc<dyn WorkflowRunHandle>;

/// Caller fields required to create a workflow task. Registry-owned values and
/// fields mirrored from the sandbox receive their source defaults in `register`.
pub struct WorkflowTaskRegistration {
    pub run_id: String,
    pub meta: Option<WorkflowMeta>,
    pub description: Option<String>,
    pub status: WorkflowStatus,
    pub start_time: i64,
    pub end_time: Option<i64>,
    pub output_file: String,
    pub abort_controller: CancellationToken,
    pub todo_work_chain_id: Option<String>,
    pub result: Option<Value>,
    pub error: Option<String>,
    pub is_backgrounded: Option<bool>,
    pub token_budget_total: Option<u64>,
    pub script: Option<String>,
    pub script_path: Option<String>,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum WorkflowRegistryError {
    #[error("Workflow run {0} is already active.")]
    AlreadyActive(String),
}

/// Mutable workflow registry. `IndexMap` preserves `Map` insertion order for
/// `list()` and retains the key's position when a terminal id is re-registered.
#[derive(Default)]
pub struct WorkflowRunRegistry {
    entries: IndexMap<String, SharedWorkflowTask>,
    handles: IndexMap<String, SharedWorkflowRunHandle>,
    usage_warning_shown: bool,
}

impl WorkflowRunRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Return true exactly once for this registry's lifetime. The source latch
    /// intentionally survives `reset()` because it is session-scoped.
    pub fn should_show_usage_warning(&mut self) -> bool {
        if self.usage_warning_shown {
            return false;
        }
        self.usage_warning_shown = true;
        true
    }

    /// Register a run, applying the same derived fields and defaults as the TS
    /// registry. Re-registering a terminal id replaces its row; an active id is
    /// rejected.
    pub fn register(
        &mut self,
        registration: WorkflowTaskRegistration,
    ) -> Result<SharedWorkflowTask, WorkflowRegistryError> {
        if self
            .entries
            .get(&registration.run_id)
            .is_some_and(|entry| is_active_workflow_status(lock(entry).status))
            || self.handles.contains_key(&registration.run_id)
        {
            return Err(WorkflowRegistryError::AlreadyActive(registration.run_id));
        }

        let description = registration
            .description
            .filter(|description| !description.is_empty())
            .or_else(|| registration.meta.as_ref().map(|meta| meta.name.clone()))
            .unwrap_or_else(|| registration.run_id.clone());
        let run_id = registration.run_id;
        let entry = Arc::new(Mutex::new(WorkflowTask {
            id: run_id.clone(),
            kind: "workflow",
            run_id: run_id.clone(),
            description,
            meta: registration.meta,
            status: registration.status,
            start_time: registration.start_time,
            end_time: registration.end_time,
            output_file: registration.output_file,
            output_offset: 0,
            notified: false,
            todo_work_chain_id: registration.todo_work_chain_id,
            abort_controller: registration.abort_controller,
            is_backgrounded: registration.is_backgrounded.unwrap_or(false),
            current_phase: None,
            phases: Vec::new(),
            agents_dispatched: 0,
            agents_completed: 0,
            recent_logs: Vec::new(),
            tokens_spent: 0,
            token_budget_total: registration.token_budget_total,
            per_phase_tokens: IndexMap::new(),
            script: registration.script.unwrap_or_default(),
            script_path: registration.script_path,
            result: registration.result,
            error: registration.error,
        }));
        self.entries.insert(run_id, Arc::clone(&entry));
        Ok(entry)
    }

    pub fn get(&self, run_id: &str) -> Option<SharedWorkflowTask> {
        self.entries.get(run_id).cloned()
    }

    /// All rows in registration order, including active and terminal runs.
    pub fn list(&self) -> Vec<SharedWorkflowTask> {
        self.entries.values().cloned().collect()
    }

    /// `paused` is active for duplicate-id guards, but does not block session
    /// switching because dispatches have drained while the run is paused.
    pub fn has_running_entries(&self) -> bool {
        self.entries.values().any(|entry| {
            matches!(
                lock(entry).status,
                WorkflowStatus::Running | WorkflowStatus::Pausing
            )
        })
    }

    /// Whether `pause()` can delegate to an attached handle now.
    pub fn can_pause(&self, run_id: &str) -> bool {
        self.handles.contains_key(run_id)
            && self.entries.get(run_id).is_some_and(|entry| {
                let task = lock(entry);
                task.is_backgrounded && task.status == WorkflowStatus::Running
            })
    }

    /// Whether `resume()` can delegate to an attached handle now.
    pub fn can_resume(&self, run_id: &str) -> bool {
        self.handles.contains_key(run_id)
            && self
                .entries
                .get(run_id)
                .is_some_and(|entry| lock(entry).status == WorkflowStatus::Paused)
    }

    /// Keep a runner handle only while its registered workflow is active.
    pub fn attach_handle(&mut self, handle: SharedWorkflowRunHandle) {
        let run_id = handle.run_id().to_owned();
        if self
            .entries
            .get(&run_id)
            .is_some_and(|entry| is_active_workflow_status(lock(entry).status))
        {
            self.handles.insert(run_id, handle);
        }
    }

    /// Request scheduler pause for a background workflow that is running and
    /// has an attached runner handle.
    pub fn pause(&self, run_id: &str) -> bool {
        let Some(entry) = self.entries.get(run_id) else {
            return false;
        };
        let eligible = {
            let task = lock(entry);
            task.is_backgrounded && task.status == WorkflowStatus::Running
        };
        if !eligible {
            return false;
        }
        self.handles
            .get(run_id)
            .is_some_and(|handle| handle.pause())
    }

    /// Request scheduler resume after the dispatch state has reached paused.
    pub fn resume(&self, run_id: &str) -> bool {
        let Some(entry) = self.entries.get(run_id) else {
            return false;
        };
        if lock(entry).status != WorkflowStatus::Paused {
            return false;
        }
        self.handles
            .get(run_id)
            .is_some_and(|handle| handle.resume())
    }

    pub fn get_handle(&self, run_id: &str) -> Option<SharedWorkflowRunHandle> {
        self.handles.get(run_id).cloned()
    }

    /// Release only if `handle` is still the exact registered handle. This
    /// protects a replacement handle installed under the same run id.
    pub fn release_handle(&mut self, run_id: &str, handle: &SharedWorkflowRunHandle) {
        if self
            .handles
            .get(run_id)
            .is_some_and(|installed| Arc::ptr_eq(installed, handle))
        {
            self.handles.shift_remove(run_id);
        }
    }

    /// Apply a dispatch scheduler state only when it follows the allowed
    /// `running -> pausing -> paused -> running` lifecycle edge.
    pub fn on_dispatch_state_change(&mut self, run_id: &str, state: WorkflowDispatchState) -> bool {
        let Some(entry) = self.entries.get(run_id) else {
            return false;
        };
        let mut task = lock(entry);
        if is_terminal_workflow_status(task.status) {
            return false;
        }
        let accepted = match state {
            WorkflowDispatchState::Pausing => task.status == WorkflowStatus::Running,
            WorkflowDispatchState::Paused => task.status == WorkflowStatus::Pausing,
            WorkflowDispatchState::Running => task.status == WorkflowStatus::Paused,
        };
        if !accepted {
            return false;
        }
        task.status = state.as_status();
        true
    }

    /// Append a phase title if it differs from the immediately preceding one.
    /// The sandbox emits no more than `MAX_PHASE_ENTRIES` phase events.
    pub fn on_phase_started(&mut self, run_id: &str, title: impl Into<String>) {
        let Some(entry) = self.entries.get(run_id) else {
            return;
        };
        let mut task = lock(entry);
        if !is_active_workflow_status(task.status) {
            return;
        }
        let title = title.into();
        task.current_phase = Some(title.clone());
        if task.phases.len() < MAX_PHASE_ENTRIES
            && task.phases.last().is_none_or(|previous| previous != &title)
        {
            task.phases.push(title);
        }
    }

    /// Count a new `agent()` dispatch. The source accepts these only while the
    /// run is active.
    pub fn on_agent_dispatched(&mut self, run_id: &str) {
        let Some(entry) = self.entries.get(run_id) else {
            return;
        };
        let mut task = lock(entry);
        if is_active_workflow_status(task.status) {
            task.agents_dispatched = task.agents_dispatched.saturating_add(1);
        }
    }

    /// Drain one settled dispatch. This deliberately also works after terminal
    /// settlement, and cannot increment beyond the dispatch count.
    pub fn on_agent_completed(&mut self, run_id: &str) {
        let Some(entry) = self.entries.get(run_id) else {
            return;
        };
        let mut task = lock(entry);
        if task.agents_completed < task.agents_dispatched {
            task.agents_completed += 1;
        }
    }

    /// Mirror the budget's cumulative usage and attribute only its positive
    /// delta to the phase current when the update arrives.
    pub fn on_budget_updated(&mut self, run_id: &str, spent: u64, total: Option<u64>) {
        let Some(entry) = self.entries.get(run_id) else {
            return;
        };
        let mut task = lock(entry);
        let delta = spent.saturating_sub(task.tokens_spent);
        let total_changed = task.token_budget_total != total;
        if delta == 0 && !total_changed {
            return;
        }
        if delta > 0 {
            let phase = task.current_phase.clone();
            let phase_total = task.per_phase_tokens.entry(phase).or_insert(0);
            *phase_total = phase_total.saturating_add(delta);
        }
        // Backward values produce no attribution delta. With an unchanged cap
        // the early return preserves the previous value; if the cap changed,
        // the source still mirrors the caller's cumulative spent value.
        task.tokens_spent = spent;
        task.token_budget_total = total;
    }

    /// Replace the UI log tail. Cancellation is allowed because its abort path
    /// can report logs after `cancel()`; completed and failed runs are final.
    pub fn set_recent_logs(&mut self, run_id: &str, logs: &[String]) {
        let Some(entry) = self.entries.get(run_id) else {
            return;
        };
        let mut task = lock(entry);
        if !is_active_workflow_status(task.status) && task.status != WorkflowStatus::Cancelled {
            return;
        }
        let start = logs.len().saturating_sub(MAX_RECENT_LOG_LINES);
        task.recent_logs.clear();
        task.recent_logs.extend_from_slice(&logs[start..]);
    }

    pub fn complete(&mut self, run_id: &str, result: Option<Value>, end_time: i64) {
        let Some(entry) = self.entries.get(run_id) else {
            return;
        };
        {
            let mut task = lock(entry);
            if !is_active_workflow_status(task.status) {
                return;
            }
            task.status = WorkflowStatus::Completed;
            task.end_time = Some(end_time);
            task.result = result;
            task.notified = true;
        }
        self.evict_terminal();
    }

    pub fn fail(&mut self, run_id: &str, message: impl Into<String>, end_time: i64) {
        let Some(entry) = self.entries.get(run_id) else {
            return;
        };
        {
            let mut task = lock(entry);
            if !is_active_workflow_status(task.status) {
                return;
            }
            task.status = WorkflowStatus::Failed;
            task.end_time = Some(end_time);
            task.error = Some(message.into());
            task.notified = true;
        }
        self.evict_terminal();
    }

    pub fn cancel(&mut self, run_id: &str, end_time: i64) {
        let Some(entry) = self.entries.get(run_id) else {
            return;
        };
        let abort_controller = {
            let mut task = lock(entry);
            if !is_active_workflow_status(task.status) {
                return;
            }
            task.status = WorkflowStatus::Cancelled;
            task.end_time = Some(end_time);
            task.notified = true;
            task.abort_controller.clone()
        };
        if let Some(handle) = self.handles.get(run_id).cloned() {
            // TypeScript catches a runner's abort exception so settlement is
            // still observable to callers.
            let _ = catch_unwind(AssertUnwindSafe(|| handle.abort()));
        } else {
            abort_controller.cancel();
        }
        self.evict_terminal();
    }

    /// Forget all rows without cancelling their controllers. The caller must
    /// first ensure no live work remains.
    pub fn reset(&mut self) {
        self.entries.clear();
        self.handles.clear();
    }

    /// Cancel all active rows at one timestamp. Callback batching and terminal
    /// notification delivery belong to the deferred callback adapter.
    pub fn abort_all(&mut self) {
        let end_time = current_time_millis();
        let mut changed = false;
        let entries = self.entries.values().cloned().collect::<Vec<_>>();
        for entry in entries {
            let (run_id, abort_controller) = {
                let mut task = lock(&entry);
                if !is_active_workflow_status(task.status) {
                    continue;
                }
                task.status = WorkflowStatus::Cancelled;
                task.end_time = Some(end_time);
                task.notified = true;
                (task.run_id.clone(), task.abort_controller.clone())
            };
            if let Some(handle) = self.handles.get(&run_id).cloned() {
                let _ = catch_unwind(AssertUnwindSafe(|| handle.abort()));
            } else {
                abort_controller.cancel();
            }
            changed = true;
        }
        if changed {
            self.evict_terminal();
        }
    }

    fn evict_terminal(&mut self) {
        let mut terminal = self
            .entries
            .iter()
            .enumerate()
            .filter_map(|(order, (id, entry))| {
                let task = lock(entry);
                is_terminal_workflow_status(task.status)
                    .then(|| (task.end_time.unwrap_or(0), order, id.clone()))
            })
            .collect::<Vec<_>>();
        terminal.sort_by_key(|(end_time, order, _)| (*end_time, *order));
        let excess = terminal
            .len()
            .saturating_sub(MAX_RETAINED_TERMINAL_WORKFLOWS);
        for (_, _, id) in terminal.into_iter().take(excess) {
            self.entries.shift_remove(&id);
        }
    }
}

fn current_time_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

fn lock(task: &SharedWorkflowTask) -> MutexGuard<'_, WorkflowTask> {
    task.lock().unwrap_or_else(|error| error.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct TestWorkflowHandle {
        run_id: String,
        abort_calls: AtomicUsize,
        pause_calls: AtomicUsize,
        resume_calls: AtomicUsize,
        pause_result: bool,
        resume_result: bool,
    }

    impl TestWorkflowHandle {
        fn new(run_id: &str, pause_result: bool, resume_result: bool) -> Arc<Self> {
            Arc::new(Self {
                run_id: run_id.to_owned(),
                abort_calls: AtomicUsize::new(0),
                pause_calls: AtomicUsize::new(0),
                resume_calls: AtomicUsize::new(0),
                pause_result,
                resume_result,
            })
        }
    }

    impl WorkflowRunHandle for TestWorkflowHandle {
        fn run_id(&self) -> &str {
            &self.run_id
        }

        fn abort(&self) {
            self.abort_calls.fetch_add(1, Ordering::SeqCst);
        }

        fn pause(&self) -> bool {
            self.pause_calls.fetch_add(1, Ordering::SeqCst);
            self.pause_result
        }

        fn resume(&self) -> bool {
            self.resume_calls.fetch_add(1, Ordering::SeqCst);
            self.resume_result
        }
    }

    fn registration(run_id: &str) -> WorkflowTaskRegistration {
        WorkflowTaskRegistration {
            run_id: run_id.to_owned(),
            meta: None,
            description: Some("workflow".to_owned()),
            status: WorkflowStatus::Running,
            start_time: 1_700_000_000_000,
            end_time: None,
            output_file: format!("/tmp/{run_id}.jsonl"),
            abort_controller: CancellationToken::new(),
            todo_work_chain_id: None,
            result: None,
            error: None,
            is_backgrounded: None,
            token_budget_total: None,
            script: None,
            script_path: None,
        }
    }

    fn task(entry: &SharedWorkflowTask) -> MutexGuard<'_, WorkflowTask> {
        lock(entry)
    }

    #[test]
    fn status_classifiers_use_explicit_active_and_terminal_sets() {
        for status in [
            WorkflowStatus::Running,
            WorkflowStatus::Pausing,
            WorkflowStatus::Paused,
        ] {
            assert!(is_active_workflow_status(status));
            assert!(!is_terminal_workflow_status(status));
        }
        for status in [
            WorkflowStatus::Completed,
            WorkflowStatus::Failed,
            WorkflowStatus::Cancelled,
        ] {
            assert!(!is_active_workflow_status(status));
            assert!(is_terminal_workflow_status(status));
        }
    }

    #[test]
    fn register_applies_task_defaults_description_and_optional_budget() {
        let mut registry = WorkflowRunRegistry::new();
        let mut input = registration("wf_named");
        input.description = None;
        input.meta = Some(WorkflowMeta {
            name: "capsules".to_owned(),
            description: "d".to_owned(),
            when_to_use: None,
            phases: None,
        });
        input.script = None;
        input.is_backgrounded = None;
        let entry = registry.register(input).unwrap();
        let state = task(&entry);
        assert_eq!(state.id, "wf_named");
        assert_eq!(state.run_id, "wf_named");
        assert_eq!(state.kind, "workflow");
        assert_eq!(state.description, "capsules");
        assert_eq!(state.current_phase, None);
        assert!(state.phases.is_empty());
        assert_eq!(state.agents_dispatched, 0);
        assert_eq!(state.agents_completed, 0);
        assert!(state.recent_logs.is_empty());
        assert_eq!(state.output_offset, 0);
        assert!(!state.notified);
        assert_eq!(state.tokens_spent, 0);
        assert_eq!(state.token_budget_total, None);
        assert!(state.per_phase_tokens.is_empty());
        assert_eq!(state.script, "");
        assert!(!state.is_backgrounded);
    }

    #[test]
    fn registration_defaults_empty_description_to_run_id_and_preserves_budget() {
        let mut registry = WorkflowRunRegistry::new();
        let mut input = registration("wf_anon");
        input.description = Some(String::new());
        input.token_budget_total = Some(50_000);
        let entry = registry.register(input).unwrap();
        assert_eq!(task(&entry).description, "wf_anon");
        assert_eq!(task(&entry).token_budget_total, Some(50_000));
    }

    #[test]
    fn register_preserves_optional_source_fields() {
        let mut registry = WorkflowRunRegistry::new();
        let mut input = registration("wf_seeded");
        input.end_time = Some(1_800);
        input.result = Some(Value::Null);
        input.error = Some("seed error".to_owned());
        input.script = Some("export default 1".to_owned());
        input.script_path = Some(".canopy/workflows/demo.js".to_owned());
        input.todo_work_chain_id = Some("chain-1".to_owned());
        let entry = registry.register(input).unwrap();
        let state = task(&entry);
        assert_eq!(state.end_time, Some(1_800));
        assert_eq!(state.result, Some(Value::Null));
        assert_eq!(state.error.as_deref(), Some("seed error"));
        assert_eq!(state.script, "export default 1");
        assert_eq!(
            state.script_path.as_deref(),
            Some(".canopy/workflows/demo.js")
        );
        assert_eq!(state.todo_work_chain_id.as_deref(), Some("chain-1"));
    }

    #[test]
    fn active_ids_are_rejected_but_terminal_ids_can_be_registered_again() {
        let mut registry = WorkflowRunRegistry::new();
        registry.register(registration("wf_id")).unwrap();
        assert!(matches!(
            registry.register(registration("wf_id")),
            Err(WorkflowRegistryError::AlreadyActive(run_id)) if run_id == "wf_id"
        ));
        registry.complete("wf_id", Some(Value::Null), 2_000);
        let next = registry.register(registration("wf_id")).unwrap();
        assert_eq!(task(&next).status, WorkflowStatus::Running);
        assert_eq!(registry.list().len(), 1);
    }

    #[test]
    fn attached_handle_blocks_id_reuse_until_released() {
        let mut registry = WorkflowRunRegistry::new();
        let entry = registry.register(registration("wf_handle_id")).unwrap();
        let concrete = TestWorkflowHandle::new("wf_handle_id", true, true);
        let handle: SharedWorkflowRunHandle = concrete.clone();
        registry.attach_handle(Arc::clone(&handle));
        registry.cancel("wf_handle_id", 2_000);

        assert!(!task(&entry).abort_controller.is_cancelled());
        assert!(matches!(
            registry.register(registration("wf_handle_id")),
            Err(WorkflowRegistryError::AlreadyActive(run_id)) if run_id == "wf_handle_id"
        ));
        assert_eq!(concrete.abort_calls.load(Ordering::SeqCst), 1);

        registry.release_handle("wf_handle_id", &handle);
        assert!(registry.get_handle("wf_handle_id").is_none());
        let replacement = registry.register(registration("wf_handle_id")).unwrap();
        assert_eq!(task(&replacement).status, WorkflowStatus::Running);
    }

    #[test]
    fn handle_release_uses_identity_and_preserves_a_replacement() {
        let mut registry = WorkflowRunRegistry::new();
        registry
            .register(registration("wf_handle_replace"))
            .unwrap();
        let first: SharedWorkflowRunHandle =
            TestWorkflowHandle::new("wf_handle_replace", true, true);
        let replacement: SharedWorkflowRunHandle =
            TestWorkflowHandle::new("wf_handle_replace", false, false);
        registry.attach_handle(Arc::clone(&first));
        registry.attach_handle(Arc::clone(&replacement));

        registry.release_handle("wf_handle_replace", &first);
        let stored = registry.get_handle("wf_handle_replace").unwrap();
        assert!(Arc::ptr_eq(&stored, &replacement));
        registry.release_handle("wf_handle_replace", &replacement);
        assert!(registry.get_handle("wf_handle_replace").is_none());
    }

    #[test]
    fn pause_and_resume_delegate_only_after_source_state_guards_pass() {
        let mut registry = WorkflowRunRegistry::new();
        let mut foreground = registration("wf_foreground_handle");
        foreground.is_backgrounded = Some(false);
        registry.register(foreground).unwrap();
        let foreground_handle = TestWorkflowHandle::new("wf_foreground_handle", true, true);
        let shared_foreground: SharedWorkflowRunHandle = foreground_handle.clone();
        registry.attach_handle(shared_foreground);
        assert!(!registry.pause("wf_foreground_handle"));
        assert_eq!(foreground_handle.pause_calls.load(Ordering::SeqCst), 0);

        let mut background = registration("wf_background_handle");
        background.is_backgrounded = Some(true);
        registry.register(background).unwrap();
        let background_handle = TestWorkflowHandle::new("wf_background_handle", true, true);
        let shared_background: SharedWorkflowRunHandle = background_handle.clone();
        registry.attach_handle(shared_background);
        assert!(registry.can_pause("wf_background_handle"));
        assert!(registry.pause("wf_background_handle"));
        assert_eq!(background_handle.pause_calls.load(Ordering::SeqCst), 1);

        registry.on_dispatch_state_change("wf_background_handle", WorkflowDispatchState::Pausing);
        assert!(!registry.resume("wf_background_handle"));
        registry.on_dispatch_state_change("wf_background_handle", WorkflowDispatchState::Paused);
        assert!(registry.can_resume("wf_background_handle"));
        assert!(registry.resume("wf_background_handle"));
        assert_eq!(background_handle.resume_calls.load(Ordering::SeqCst), 1);
        registry.on_dispatch_state_change("wf_background_handle", WorkflowDispatchState::Running);
        let resumed = registry.get("wf_background_handle").unwrap();
        assert_eq!(task(&resumed).status, WorkflowStatus::Running);
    }

    #[test]
    fn attach_handle_ignores_missing_and_terminal_entries() {
        let mut registry = WorkflowRunRegistry::new();
        let missing: SharedWorkflowRunHandle = TestWorkflowHandle::new("wf_missing", true, true);
        registry.attach_handle(missing);
        registry
            .register(registration("wf_terminal_handle"))
            .unwrap();
        registry.complete("wf_terminal_handle", None, 2_000);
        let terminal: SharedWorkflowRunHandle =
            TestWorkflowHandle::new("wf_terminal_handle", true, true);
        registry.attach_handle(terminal);
        assert!(registry.get_handle("wf_missing").is_none());
        assert!(registry.get_handle("wf_terminal_handle").is_none());
    }

    #[test]
    fn dispatch_state_guards_model_pause_resume_cycle_and_blocking_queries() {
        let mut registry = WorkflowRunRegistry::new();
        let entry = registry.register(registration("wf_state")).unwrap();
        assert!(registry.has_running_entries());
        assert!(!registry.can_pause("wf_state"));
        assert!(!registry.on_dispatch_state_change("wf_state", WorkflowDispatchState::Paused));
        assert!(registry.on_dispatch_state_change("wf_state", WorkflowDispatchState::Pausing));
        assert_eq!(task(&entry).status, WorkflowStatus::Pausing);
        assert!(registry.on_dispatch_state_change("wf_state", WorkflowDispatchState::Paused));
        assert_eq!(task(&entry).status, WorkflowStatus::Paused);
        assert!(!registry.has_running_entries());
        assert!(matches!(
            registry.register(registration("wf_state")),
            Err(WorkflowRegistryError::AlreadyActive(_))
        ));
        assert!(!registry.can_resume("wf_state"));
        assert!(!registry.on_dispatch_state_change("wf_state", WorkflowDispatchState::Pausing));
        assert!(registry.on_dispatch_state_change("wf_state", WorkflowDispatchState::Running));
        assert!(registry.has_running_entries());

        registry.cancel("wf_state", 3_000);
        assert!(!registry.on_dispatch_state_change("wf_state", WorkflowDispatchState::Paused));
        assert_eq!(task(&entry).status, WorkflowStatus::Cancelled);
    }

    #[test]
    fn pause_precondition_requires_backgrounded_running_entry() {
        let mut registry = WorkflowRunRegistry::new();
        let mut foreground = registration("wf_foreground");
        foreground.is_backgrounded = Some(false);
        registry.register(foreground).unwrap();
        let mut background = registration("wf_background");
        background.is_backgrounded = Some(true);
        registry.register(background).unwrap();
        assert!(!registry.can_pause("wf_foreground"));
        assert!(!registry.can_pause("wf_background"));
        registry.on_dispatch_state_change("wf_background", WorkflowDispatchState::Pausing);
        assert!(!registry.can_pause("wf_background"));
    }

    #[test]
    fn phase_history_deduplicates_consecutive_titles_and_is_bounded() {
        let mut registry = WorkflowRunRegistry::new();
        let entry = registry.register(registration("wf_phases")).unwrap();
        registry.on_phase_started("wf_phases", "Plan");
        registry.on_phase_started("wf_phases", "Plan");
        registry.on_phase_started("wf_phases", "Build");
        assert_eq!(
            task(&entry).phases,
            vec!["Plan".to_owned(), "Build".to_owned()]
        );
        assert_eq!(task(&entry).current_phase.as_deref(), Some("Build"));

        for index in 0..MAX_PHASE_ENTRIES {
            registry.on_phase_started("wf_phases", format!("phase-{index}"));
        }
        registry.on_phase_started("wf_phases", "beyond-cap");
        let state = task(&entry);
        assert_eq!(state.phases.len(), MAX_PHASE_ENTRIES);
        assert_eq!(state.current_phase.as_deref(), Some("beyond-cap"));
    }

    #[test]
    fn counters_increment_and_completion_drain_is_capped_even_after_terminal() {
        let mut registry = WorkflowRunRegistry::new();
        let entry = registry.register(registration("wf_counts")).unwrap();
        registry.on_agent_dispatched("wf_counts");
        registry.on_agent_dispatched("wf_counts");
        registry.on_agent_completed("wf_counts");
        registry.complete("wf_counts", None, 2_000);
        registry.on_agent_completed("wf_counts");
        registry.on_agent_completed("wf_counts");
        let state = task(&entry);
        assert_eq!(state.agents_dispatched, 2);
        assert_eq!(state.agents_completed, 2);
    }

    #[test]
    fn budget_updates_track_monotonic_spend_and_attribute_positive_deltas() {
        let mut registry = WorkflowRunRegistry::new();
        let entry = registry.register(registration("wf_budget")).unwrap();
        registry.on_budget_updated("wf_budget", 100, None);
        registry.on_phase_started("wf_budget", "Find");
        registry.on_budget_updated("wf_budget", 200, Some(1_000));
        registry.on_budget_updated("wf_budget", 350, Some(1_000));
        registry.on_phase_started("wf_budget", "Verify");
        registry.on_budget_updated("wf_budget", 500, Some(1_000));
        registry.on_budget_updated("wf_budget", 450, Some(1_000));
        let state = task(&entry);
        assert_eq!(state.tokens_spent, 500);
        assert_eq!(state.token_budget_total, Some(1_000));
        assert_eq!(state.per_phase_tokens.get(&None), Some(&100));
        assert_eq!(
            state.per_phase_tokens.get(&Some("Find".to_owned())),
            Some(&250)
        );
        assert_eq!(
            state.per_phase_tokens.get(&Some("Verify".to_owned())),
            Some(&150)
        );
    }

    #[test]
    fn budget_total_can_change_without_spend_and_unknown_runs_are_noops() {
        let mut registry = WorkflowRunRegistry::new();
        let entry = registry.register(registration("wf_budget_total")).unwrap();
        registry.on_budget_updated("wf_missing", 100, Some(1_000));
        registry.on_budget_updated("wf_budget_total", 0, Some(1_000));
        assert_eq!(task(&entry).token_budget_total, Some(1_000));
        assert_eq!(task(&entry).tokens_spent, 0);
        assert!(task(&entry).per_phase_tokens.is_empty());
    }

    #[test]
    fn logs_keep_the_last_hundred_and_accept_late_cancel_logs_only() {
        let mut registry = WorkflowRunRegistry::new();
        let entry = registry.register(registration("wf_logs")).unwrap();
        let logs = (0..250)
            .map(|index| format!("line {index}"))
            .collect::<Vec<_>>();
        registry.set_recent_logs("wf_logs", &logs);
        assert_eq!(task(&entry).recent_logs.len(), 100);
        assert_eq!(
            task(&entry).recent_logs.first().map(String::as_str),
            Some("line 150")
        );
        assert_eq!(
            task(&entry).recent_logs.last().map(String::as_str),
            Some("line 249")
        );

        registry.cancel("wf_logs", 2_000);
        registry.set_recent_logs("wf_logs", &["late line".to_owned()]);
        assert_eq!(task(&entry).recent_logs, vec!["late line".to_owned()]);

        registry.register(registration("wf_done_logs")).unwrap();
        registry.complete("wf_done_logs", None, 3_000);
        registry.set_recent_logs("wf_done_logs", &["too late".to_owned()]);
        let done = registry.get("wf_done_logs").unwrap();
        assert!(task(&done).recent_logs.is_empty());
    }

    #[test]
    fn terminal_transitions_are_idempotent_and_cancel_aborts() {
        let mut registry = WorkflowRunRegistry::new();
        let done = registry.register(registration("wf_done")).unwrap();
        registry.complete(
            "wf_done",
            Some(serde_json::json!({"answer":"Paris"})),
            2_000,
        );
        registry.fail("wf_done", "too late", 3_000);
        registry.cancel("wf_done", 4_000);
        assert_eq!(task(&done).status, WorkflowStatus::Completed);
        assert_eq!(task(&done).end_time, Some(2_000));
        assert_eq!(
            task(&done).result,
            Some(serde_json::json!({"answer":"Paris"}))
        );
        assert_eq!(task(&done).error, None);

        let cancelled = registry.register(registration("wf_cancel")).unwrap();
        registry.cancel("wf_cancel", 5_000);
        assert!(task(&cancelled).abort_controller.is_cancelled());
        assert_eq!(task(&cancelled).status, WorkflowStatus::Cancelled);

        registry.register(registration("wf_fail")).unwrap();
        registry.fail("wf_fail", "boom", 6_000);
        let failed = registry.get("wf_fail").unwrap();
        assert_eq!(task(&failed).status, WorkflowStatus::Failed);
        assert_eq!(task(&failed).error.as_deref(), Some("boom"));
    }

    #[test]
    fn reset_drops_rows_without_cancelling_and_warning_latch_survives() {
        let mut registry = WorkflowRunRegistry::new();
        assert!(registry.should_show_usage_warning());
        let entry = registry.register(registration("wf_reset")).unwrap();
        let concrete_handle = TestWorkflowHandle::new("wf_reset", true, true);
        let handle: SharedWorkflowRunHandle = concrete_handle.clone();
        registry.attach_handle(handle);
        registry.complete("wf_reset", None, 2_000);
        registry.reset();
        assert!(registry.list().is_empty());
        assert!(registry.get_handle("wf_reset").is_none());
        assert_eq!(concrete_handle.abort_calls.load(Ordering::SeqCst), 0);
        assert!(!task(&entry).abort_controller.is_cancelled());
        assert!(!registry.should_show_usage_warning());
    }

    #[test]
    fn abort_all_cancels_running_pausing_and_paused_but_leaves_terminal_rows() {
        let mut registry = WorkflowRunRegistry::new();
        let running_controller = CancellationToken::new();
        let pausing_controller = CancellationToken::new();
        let paused_controller = CancellationToken::new();
        let completed_controller = CancellationToken::new();

        let mut running = registration("wf_running");
        running.abort_controller = running_controller.clone();
        registry.register(running).unwrap();
        let mut pausing = registration("wf_pausing");
        pausing.abort_controller = pausing_controller.clone();
        registry.register(pausing).unwrap();
        registry.on_dispatch_state_change("wf_pausing", WorkflowDispatchState::Pausing);
        let mut paused = registration("wf_paused");
        paused.abort_controller = paused_controller.clone();
        registry.register(paused).unwrap();
        registry.on_dispatch_state_change("wf_paused", WorkflowDispatchState::Pausing);
        registry.on_dispatch_state_change("wf_paused", WorkflowDispatchState::Paused);
        let mut completed = registration("wf_completed");
        completed.abort_controller = completed_controller.clone();
        registry.register(completed).unwrap();
        registry.complete("wf_completed", None, 1_000);

        registry.abort_all();

        assert!(running_controller.is_cancelled());
        assert!(pausing_controller.is_cancelled());
        assert!(paused_controller.is_cancelled());
        assert!(!completed_controller.is_cancelled());
        for run_id in ["wf_running", "wf_pausing", "wf_paused"] {
            let entry = registry.get(run_id).unwrap();
            assert_eq!(task(&entry).status, WorkflowStatus::Cancelled);
        }
        let completed = registry.get("wf_completed").unwrap();
        assert_eq!(task(&completed).status, WorkflowStatus::Completed);
    }

    #[test]
    fn abort_all_prefers_attached_handle_and_uses_token_without_one() {
        let mut registry = WorkflowRunRegistry::new();
        let managed = registration("wf_managed");
        let managed_controller = managed.abort_controller.clone();
        registry.register(managed).unwrap();
        let handle = TestWorkflowHandle::new("wf_managed", true, true);
        let shared_handle: SharedWorkflowRunHandle = handle.clone();
        registry.attach_handle(shared_handle);

        let fallback_controller = CancellationToken::new();
        let mut fallback = registration("wf_fallback");
        fallback.abort_controller = fallback_controller.clone();
        registry.register(fallback).unwrap();

        registry.abort_all();

        assert_eq!(handle.abort_calls.load(Ordering::SeqCst), 1);
        assert!(!managed_controller.is_cancelled());
        assert!(fallback_controller.is_cancelled());
    }

    #[test]
    fn terminal_retention_evicts_oldest_end_time_and_preserves_active_rows() {
        let mut registry = WorkflowRunRegistry::new();
        let active = registry.register(registration("wf_active")).unwrap();
        for index in 0..MAX_RETAINED_TERMINAL_WORKFLOWS + 3 {
            let id = format!("wf_{index}");
            registry.register(registration(&id)).unwrap();
            registry.complete(&id, None, 1_000 + index as i64);
        }
        assert_eq!(registry.list().len(), MAX_RETAINED_TERMINAL_WORKFLOWS + 1);
        assert!(registry.get("wf_0").is_none());
        assert!(registry.get("wf_12").is_some());
        assert_eq!(task(&active).status, WorkflowStatus::Running);
    }

    #[test]
    fn list_preserves_registration_order_and_reset_can_be_reused() {
        let mut registry = WorkflowRunRegistry::new();
        registry.register(registration("wf_a")).unwrap();
        registry.register(registration("wf_b")).unwrap();
        registry.register(registration("wf_c")).unwrap();
        let ids = registry
            .list()
            .iter()
            .map(|entry| task(entry).run_id.clone())
            .collect::<Vec<_>>();
        assert_eq!(ids, ["wf_a", "wf_b", "wf_c"]);
        registry.reset();
        registry.register(registration("wf_new")).unwrap();
        assert_eq!(registry.list().len(), 1);
    }
}
