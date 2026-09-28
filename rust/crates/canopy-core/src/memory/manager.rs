//! Runtime-owned managed auto-memory task manager.
//!
//! Port of the stateful orchestration in packages/core/src/memory/manager.ts.
//! Model calls, telemetry, configuration, and process integration are injected
//! through MemoryManagerRuntime, keeping this module usable from native
//! runtimes and headless tests without importing the TypeScript Config graph.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, SecondsFormat, Utc};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::{oneshot, watch};
use uuid::Uuid;

use super::forget::{
    AutoMemoryForgetError, AutoMemoryForgetMatch, AutoMemoryForgetResult,
    AutoMemoryForgetSelectionResult, ForgetApplyOptions, ForgetOperationOptions,
    ForgetSelectionOptions, forget_managed_auto_memory_entries, forget_managed_auto_memory_matches,
    select_managed_auto_memory_forget_candidates,
};
use super::paths::AutoMemoryPaths;
use super::pending_skills::{
    PendingSkill, accept_pending_skill, reject_pending_skill, stage_skill_dirs,
};
use super::prompt::{
    BuildMemoryPromptOptions, TeamAutoMemorySection, UserAutoMemorySection,
    build_managed_auto_memory_prompt,
};
use super::status::{ManagedAutoMemoryStatus, MemoryTaskSource, get_managed_auto_memory_status};
use super::store::{
    AutoMemoryExtractCursor, AutoMemoryMetadata, AutoMemoryType, ensure_auto_memory_scaffold_at,
};

pub use super::status::{ManagedMemoryTaskType, MemoryTaskRecord, MemoryTaskStatus};
pub use super::status::{
    ManagedMemoryTaskType as TaskType, MemoryTaskRecord as TaskRecord,
    MemoryTaskStatus as TaskStatus,
};

pub type MemoryManagerFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, String>> + Send + 'a>>;
pub type MemoryTaskListener = Arc<dyn Fn() + Send + Sync + 'static>;

pub const EXTRACT_TASK_TYPE: &str = "managed-auto-memory-extraction";
pub const DREAM_TASK_TYPE: &str = "managed-auto-memory-dream";
pub const SKILL_REVIEW_TASK_TYPE: &str = "managed-skill-extractor";
pub const AUTO_SKILL_THRESHOLD: usize = 20;
pub const DEFAULT_AUTO_DREAM_MIN_HOURS: f64 = 24.0;
pub const DEFAULT_AUTO_DREAM_MIN_SESSIONS: usize = 5;
const DREAM_LOCK_STALE_MS: f64 = 60.0 * 60.0 * 1_000.0;
const SESSION_SCAN_INTERVAL_MS: f64 = 10.0 * 60.0 * 1_000.0;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct MemoryTurn {
    pub role: Option<String>,
    pub parts: Vec<Value>,
}

#[derive(Clone, Debug)]
pub struct ScheduleExtractParams {
    pub paths: AutoMemoryPaths,
    pub session_id: String,
    pub history: Vec<MemoryTurn>,
    pub now: Option<DateTime<Utc>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExtractSkipReason {
    AlreadyRunning,
    Queued,
    MemoryTool,
    MemoryPressure,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExtractResult {
    pub touched_topics: Vec<AutoMemoryType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skipped_reason: Option<ExtractSkipReason>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system_message: Option<String>,
    pub cursor: AutoMemoryExtractCursor,
}

#[derive(Clone, Debug)]
pub struct ScheduleDreamParams {
    pub paths: AutoMemoryPaths,
    pub session_id: String,
    pub enabled: bool,
    pub has_config: bool,
    pub now: Option<DateTime<Utc>>,
    pub min_hours_between_dreams: Option<f64>,
    pub min_sessions_between_dreams: Option<usize>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DreamSkipReason {
    Disabled,
    SameSession,
    MinHours,
    MinSessions,
    ScanThrottled,
    Locked,
    Running,
    MemoryPressure,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DreamScheduleResult {
    Scheduled {
        task_id: String,
    },
    Skipped {
        reason: DreamSkipReason,
        task_id: Option<String>,
    },
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct DreamResult {
    pub touched_topics: Vec<AutoMemoryType>,
    pub deduped_entries: usize,
    pub system_message: Option<String>,
}

#[derive(Clone, Debug)]
pub struct ScheduleSkillReviewParams {
    pub paths: AutoMemoryPaths,
    pub session_id: String,
    pub history: Vec<MemoryTurn>,
    pub tool_call_count: usize,
    pub skills_modified: bool,
    pub enabled: Option<bool>,
    pub has_config: bool,
    pub threshold: Option<usize>,
    pub max_turns: Option<usize>,
    pub timeout: Option<Duration>,
    pub confirm_before_persist: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SkillReviewSkipReason {
    BelowThreshold,
    SkillsModifiedInSession,
    Disabled,
    AlreadyRunning,
    MemoryPressure,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SkillReviewScheduleResult {
    Scheduled {
        task_id: String,
    },
    Skipped {
        reason: SkillReviewSkipReason,
        task_id: Option<String>,
    },
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct SkillReviewResult {
    pub touched_skill_files: Vec<PathBuf>,
    pub system_message: Option<String>,
}

/// Injected side effects for extraction, dream consolidation, and skill review.
/// Implementations normally adapt the native model client and memory-pressure
/// monitor. Futures return owned error strings for task records.
pub trait MemoryManagerRuntime: Send + Sync + 'static {
    fn is_under_memory_pressure(&self) -> bool {
        false
    }

    fn managed_auto_dream_enabled(&self) -> bool {
        false
    }

    fn extract<'a>(
        &'a self,
        params: ScheduleExtractParams,
    ) -> MemoryManagerFuture<'a, ExtractResult>;

    fn scan_sessions<'a>(
        &'a self,
        paths: &'a AutoMemoryPaths,
        since_ms: f64,
        exclude_session_id: &'a str,
    ) -> MemoryManagerFuture<'a, Vec<String>>;

    fn dream<'a>(
        &'a self,
        paths: AutoMemoryPaths,
        session_id: String,
        now: DateTime<Utc>,
        cancelled: Arc<AtomicBool>,
    ) -> MemoryManagerFuture<'a, DreamResult>;

    fn skill_review<'a>(
        &'a self,
        _params: ScheduleSkillReviewParams,
    ) -> MemoryManagerFuture<'a, SkillReviewResult> {
        Box::pin(async { Ok(SkillReviewResult::default()) })
    }
}

#[derive(Clone)]
struct QueuedExtract {
    task_id: String,
    params: ScheduleExtractParams,
}

#[derive(Default)]
struct ManagerState {
    tasks: IndexMap<String, MemoryTaskRecord>,
    extract_running: HashSet<PathBuf>,
    extract_current_task_id: HashMap<PathBuf, String>,
    extract_queued: HashMap<PathBuf, QueuedExtract>,
    skill_review_in_flight_by_project: HashMap<PathBuf, String>,
    dream_in_flight_by_key: HashMap<String, String>,
    dream_last_session_scan_at: HashMap<PathBuf, f64>,
    dream_abort_flags: HashMap<String, Arc<AtomicBool>>,
    dream_lock_release_failed: bool,
    active_tasks: HashSet<String>,
}

#[derive(Default)]
struct Subscribers {
    next_id: u64,
    all: HashMap<u64, MemoryTaskListener>,
    by_type: HashMap<&'static str, HashMap<u64, MemoryTaskListener>>,
}

struct ManagerInner {
    runtime: Arc<dyn MemoryManagerRuntime>,
    state: Mutex<ManagerState>,
    subscribers: Mutex<Subscribers>,
    active_count: watch::Sender<usize>,
}

/// Cloneable handle to one isolated set of memory-task state.
#[derive(Clone)]
pub struct MemoryManager {
    inner: Arc<ManagerInner>,
}

/// Unsubscribes synchronously when dropped.
pub struct MemorySubscription {
    manager: Weak<ManagerInner>,
    id: u64,
    task_type: Option<ManagedMemoryTaskType>,
}

impl Drop for MemorySubscription {
    fn drop(&mut self) {
        let Some(manager) = self.manager.upgrade() else {
            return;
        };
        let mut subscribers = lock(&manager.subscribers);
        if let Some(task_type) = &self.task_type {
            let key = task_type_key(task_type);
            let bucket_empty = if let Some(bucket) = subscribers.by_type.get_mut(key) {
                bucket.remove(&self.id);
                bucket.is_empty()
            } else {
                false
            };
            if bucket_empty {
                subscribers.by_type.remove(key);
            }
        } else {
            subscribers.all.remove(&self.id);
        }
    }
}

impl MemoryManager {
    pub fn new(runtime: Arc<dyn MemoryManagerRuntime>) -> Self {
        let (active_count, _) = watch::channel(0);
        Self {
            inner: Arc::new(ManagerInner {
                runtime,
                state: Mutex::new(ManagerState::default()),
                subscribers: Mutex::new(Subscribers::default()),
                active_count,
            }),
        }
    }

    pub fn subscribe(
        &self,
        listener: MemoryTaskListener,
        task_type: Option<ManagedMemoryTaskType>,
    ) -> MemorySubscription {
        let mut subscribers = lock(&self.inner.subscribers);
        subscribers.next_id = subscribers.next_id.wrapping_add(1);
        let id = subscribers.next_id;
        if let Some(task_type) = &task_type {
            subscribers
                .by_type
                .entry(task_type_key(task_type))
                .or_default()
                .insert(id, listener);
        } else {
            subscribers.all.insert(id, listener);
        }
        MemorySubscription {
            manager: Arc::downgrade(&self.inner),
            id,
            task_type,
        }
    }

    pub fn get_task(&self, task_id: &str) -> Option<MemoryTaskRecord> {
        lock(&self.inner.state).tasks.get(task_id).cloned()
    }

    pub async fn get_status(&self, paths: &AutoMemoryPaths) -> io::Result<ManagedAutoMemoryStatus> {
        get_managed_auto_memory_status(paths, self).await
    }

    pub async fn select_forget_candidates(
        &self,
        paths: &AutoMemoryPaths,
        query: &str,
        options: ForgetSelectionOptions<'_>,
    ) -> Result<AutoMemoryForgetSelectionResult, AutoMemoryForgetError> {
        select_managed_auto_memory_forget_candidates(paths, query, options).await
    }

    pub async fn forget_matches(
        &self,
        paths: &AutoMemoryPaths,
        matches: &[AutoMemoryForgetMatch],
        now: DateTime<Utc>,
        options: ForgetApplyOptions<'_>,
    ) -> Result<AutoMemoryForgetResult, AutoMemoryForgetError> {
        forget_managed_auto_memory_matches(paths, matches, now, options).await
    }

    pub async fn forget(
        &self,
        paths: &AutoMemoryPaths,
        query: &str,
        options: ForgetOperationOptions<'_>,
        now: DateTime<Utc>,
    ) -> Result<AutoMemoryForgetResult, AutoMemoryForgetError> {
        forget_managed_auto_memory_entries(paths, query, options, now).await
    }

    pub fn build_auto_memory_prompt(
        &self,
        memory_dir: &str,
        index_content: Option<&str>,
        user_section: Option<&UserAutoMemorySection<'_>>,
        team_section: Option<&TeamAutoMemorySection<'_>>,
        options: BuildMemoryPromptOptions,
    ) -> String {
        build_managed_auto_memory_prompt(
            memory_dir,
            index_content,
            user_section,
            team_section,
            options,
        )
    }

    pub fn list_tasks_by_type(
        &self,
        task_type: ManagedMemoryTaskType,
        project_root: Option<&Path>,
    ) -> Vec<MemoryTaskRecord> {
        let project_filter = project_root
            .filter(|path| !path.as_os_str().is_empty())
            .map(path_string);
        let mut records = lock(&self.inner.state)
            .tasks
            .values()
            .filter(|record| {
                record.task_type == task_type
                    && project_filter
                        .as_ref()
                        .is_none_or(|root| record.project_root == *root)
            })
            .cloned()
            .collect::<Vec<_>>();
        records.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));
        records
    }

    /// None or zero waits without a deadline. Drain waits until this manager
    /// instance has no active background work.
    pub async fn drain(&self, timeout: Option<Duration>) -> bool {
        let mut receiver = self.inner.active_count.subscribe();
        let wait_until_idle = async move {
            loop {
                if *receiver.borrow() == 0 {
                    return true;
                }
                if receiver.changed().await.is_err() {
                    return true;
                }
            }
        };
        match timeout.filter(|duration| !duration.is_zero()) {
            Some(timeout) => tokio::time::timeout(timeout, wait_until_idle)
                .await
                .unwrap_or(false),
            None => wait_until_idle.await,
        }
    }

    /// Schedule extraction. This future resolves with the active result;
    /// memory writes and queued requests return a synthetic result immediately.
    pub async fn schedule_extract(
        &self,
        params: ScheduleExtractParams,
    ) -> Result<ExtractResult, String> {
        if history_writes_to_memory(&params.history, &params.paths) {
            let now = params.now.unwrap_or_else(Utc::now);
            let record = make_task_record(
                ManagedMemoryTaskType::Extract,
                &params.paths,
                &params.session_id,
            );
            let result =
                skipped_extract_result(&params.session_id, now, ExtractSkipReason::MemoryTool);
            self.store_with(
                record,
                MemoryTaskStatus::Skipped,
                Some("Skipped: main agent wrote to memory files this turn."),
                json!({
                    "skippedReason": "memory_tool",
                    "historyLength": params.history.len(),
                }),
            );
            return Ok(result);
        }

        let project_root = params.paths.project_root().to_path_buf();
        let (record, sender, receiver) = {
            let mut state = lock(&self.inner.state);
            if state.extract_running.contains(&project_root) {
                let Some(current_task_id) =
                    state.extract_current_task_id.get(&project_root).cloned()
                else {
                    return Ok(skipped_extract_result(
                        &params.session_id,
                        params.now.unwrap_or_else(Utc::now),
                        ExtractSkipReason::AlreadyRunning,
                    ));
                };
                if let Some(queued_task_id) = state
                    .extract_queued
                    .get(&project_root)
                    .map(|queued| queued.task_id.clone())
                {
                    if let Some(queued) = state.extract_queued.get_mut(&project_root) {
                        queued.params = params.clone();
                    }
                    if let Some(record) = state.tasks.get_mut(&queued_task_id) {
                        update_record(
                            record,
                            Some(MemoryTaskStatus::Pending),
                            Some(
                                "Updated trailing managed auto-memory extraction request while another extraction is running.",
                            ),
                            json!({
                                "queuedBehindTaskId": current_task_id,
                                "historyLength": params.history.len(),
                                "supersededAt": timestamp(Utc::now()),
                            }),
                            None,
                        );
                    }
                } else {
                    let mut queued_record = make_task_record(
                        ManagedMemoryTaskType::Extract,
                        &params.paths,
                        &params.session_id,
                    );
                    update_record(
                        &mut queued_record,
                        Some(MemoryTaskStatus::Pending),
                        Some(
                            "Queued trailing managed auto-memory extraction until the active extraction completes.",
                        ),
                        json!({
                            "trailing": true,
                            "queuedBehindTaskId": current_task_id,
                            "historyLength": params.history.len(),
                        }),
                        None,
                    );
                    state.extract_queued.insert(
                        project_root,
                        QueuedExtract {
                            task_id: queued_record.id.clone(),
                            params: params.clone(),
                        },
                    );
                    state.tasks.insert(queued_record.id.clone(), queued_record);
                }
                drop(state);
                self.notify(Some(ManagedMemoryTaskType::Extract));
                return Ok(skipped_extract_result(
                    &params.session_id,
                    params.now.unwrap_or_else(Utc::now),
                    ExtractSkipReason::Queued,
                ));
            }

            let record = make_task_record(
                ManagedMemoryTaskType::Extract,
                &params.paths,
                &params.session_id,
            );
            let task_id = record.id.clone();
            state.tasks.insert(task_id.clone(), record.clone());
            state.extract_running.insert(project_root.clone());
            state
                .extract_current_task_id
                .insert(project_root, task_id.clone());
            state.active_tasks.insert(task_id);
            let (sender, receiver) = oneshot::channel();
            (record, sender, receiver)
        };
        self.publish_active_count();
        self.notify(Some(ManagedMemoryTaskType::Extract));

        let manager = self.clone();
        let task_id = record.id.clone();
        tokio::spawn(async move {
            manager
                .run_extract_chain(task_id, params, Some(sender))
                .await;
        });
        receiver
            .await
            .map_err(|_| "extraction task ended before returning a result".to_owned())?
    }

    async fn run_extract_chain(
        &self,
        mut task_id: String,
        mut params: ScheduleExtractParams,
        mut first_sender: Option<oneshot::Sender<Result<ExtractResult, String>>>,
    ) {
        loop {
            self.update_task(
                &task_id,
                Some(MemoryTaskStatus::Running),
                Some("Running managed auto-memory extraction."),
                json!({ "historyLength": params.history.len() }),
                None,
            );
            let result = if self.inner.runtime.is_under_memory_pressure() {
                Ok(skipped_extract_result(
                    &params.session_id,
                    params.now.unwrap_or_else(Utc::now),
                    ExtractSkipReason::MemoryPressure,
                ))
            } else {
                self.inner.runtime.extract(params.clone()).await
            };
            match &result {
                Ok(result) => {
                    let status = if result.skipped_reason.is_some() {
                        MemoryTaskStatus::Skipped
                    } else {
                        MemoryTaskStatus::Completed
                    };
                    let default_progress = if result.touched_topics.is_empty() {
                        Some("Managed auto-memory extraction completed without durable changes.")
                    } else {
                        None
                    };
                    self.update_task(
                        &task_id,
                        Some(status),
                        result.system_message.as_deref().or(default_progress),
                        json!({
                            "touchedTopics": result.touched_topics,
                            "processedOffset": result.cursor.processed_offset,
                            "skippedReason": result.skipped_reason,
                        }),
                        None,
                    );
                }
                Err(error) => {
                    self.update_task(
                        &task_id,
                        Some(MemoryTaskStatus::Failed),
                        None,
                        Value::Null,
                        Some(error.clone()),
                    );
                }
            }
            if let Some(sender) = first_sender.take() {
                let _ = sender.send(result.clone());
            }

            let project_root = params.paths.project_root().to_path_buf();
            let next = {
                let mut state = lock(&self.inner.state);
                state.active_tasks.remove(&task_id);
                state.extract_running.remove(&project_root);
                state.extract_current_task_id.remove(&project_root);
                if let Some(queued) = state.extract_queued.remove(&project_root) {
                    state.extract_running.insert(project_root.clone());
                    state
                        .extract_current_task_id
                        .insert(project_root, queued.task_id.clone());
                    state.active_tasks.insert(queued.task_id.clone());
                    if let Some(record) = state.tasks.get_mut(&queued.task_id) {
                        update_record(
                            record,
                            Some(MemoryTaskStatus::Running),
                            Some("Running managed auto-memory extraction."),
                            json!({ "historyLength": queued.params.history.len() }),
                            None,
                        );
                    }
                    Some(queued)
                } else {
                    None
                }
            };
            self.publish_active_count();
            if let Some(queued) = next {
                self.notify(Some(ManagedMemoryTaskType::Extract));
                task_id = queued.task_id;
                params = queued.params;
            } else {
                break;
            }
        }
    }

    /// Gate, scan, and asynchronously launch one dream task.
    pub async fn schedule_dream(
        &self,
        params: ScheduleDreamParams,
    ) -> Result<DreamScheduleResult, String> {
        if !params.has_config || !params.enabled || !self.inner.runtime.managed_auto_dream_enabled()
        {
            return Ok(skipped_dream(DreamSkipReason::Disabled));
        }
        if self.inner.runtime.is_under_memory_pressure() {
            return Ok(skipped_dream(DreamSkipReason::MemoryPressure));
        }

        let now = params.now.unwrap_or_else(Utc::now);
        let min_hours = params
            .min_hours_between_dreams
            .unwrap_or(DEFAULT_AUTO_DREAM_MIN_HOURS);
        let min_sessions = params
            .min_sessions_between_dreams
            .unwrap_or(DEFAULT_AUTO_DREAM_MIN_SESSIONS);
        ensure_auto_memory_scaffold_at(&params.paths, now)
            .await
            .map_err(|error| error.to_string())?;
        let metadata = read_dream_metadata(&params.paths).await?;
        if metadata.last_dream_session_id.as_deref() == Some(params.session_id.as_str()) {
            return Ok(skipped_dream(DreamSkipReason::SameSession));
        }
        if let Some(last_dream) = metadata.last_dream_at.as_deref()
            && let Some(elapsed) = hours_since(last_dream, now)
            && elapsed < min_hours
        {
            return Ok(skipped_dream(DreamSkipReason::MinHours));
        }

        let project_root = params.paths.project_root().to_path_buf();
        let now_ms = now.timestamp_millis() as f64;
        let last_scan = lock(&self.inner.state)
            .dream_last_session_scan_at
            .get(&project_root)
            .copied()
            .unwrap_or(0.0);
        if now_ms - last_scan < SESSION_SCAN_INTERVAL_MS {
            return Ok(skipped_dream(DreamSkipReason::ScanThrottled));
        }
        let last_dream_ms = metadata
            .last_dream_at
            .as_deref()
            .and_then(parse_timestamp)
            .map(|time| time.timestamp_millis() as f64)
            .unwrap_or(0.0);
        let sessions = self
            .inner
            .runtime
            .scan_sessions(&params.paths, last_dream_ms, &params.session_id)
            .await?;
        lock(&self.inner.state)
            .dream_last_session_scan_at
            .insert(project_root.clone(), now_ms);
        if sessions.len() < min_sessions {
            return Ok(skipped_dream(DreamSkipReason::MinSessions));
        }

        let release_failed = lock(&self.inner.state).dream_lock_release_failed;
        if release_failed {
            let _ =
                tokio::fs::remove_file(params.paths.auto_memory_consolidation_lock_path()).await;
            lock(&self.inner.state).dream_lock_release_failed = false;
        }
        if dream_lock_exists(&params.paths).await {
            return Ok(skipped_dream(DreamSkipReason::Locked));
        }

        let dedupe_key = format!("{DREAM_TASK_TYPE}:{}", project_root.display());
        let (record, abort) = {
            let mut state = lock(&self.inner.state);
            if let Some(task_id) = state.dream_in_flight_by_key.get(&dedupe_key) {
                return Ok(DreamScheduleResult::Skipped {
                    reason: DreamSkipReason::Running,
                    task_id: Some(task_id.clone()),
                });
            }
            let record = make_task_record(
                ManagedMemoryTaskType::Dream,
                &params.paths,
                &params.session_id,
            );
            let abort = Arc::new(AtomicBool::new(false));
            state
                .dream_abort_flags
                .insert(record.id.clone(), abort.clone());
            state
                .dream_in_flight_by_key
                .insert(dedupe_key.clone(), record.id.clone());
            state.active_tasks.insert(record.id.clone());
            let mut record = record;
            update_record(
                &mut record,
                Some(MemoryTaskStatus::Running),
                Some("Scheduled managed auto-memory dream."),
                json!({ "sessionCount": sessions.len() }),
                None,
            );
            state.tasks.insert(record.id.clone(), record.clone());
            (record, abort)
        };
        self.publish_active_count();
        self.notify(Some(ManagedMemoryTaskType::Dream));

        let manager = self.clone();
        let task_id = record.id.clone();
        tokio::spawn(async move {
            manager
                .run_dream(record.id, dedupe_key, params, now, abort)
                .await;
        });
        Ok(DreamScheduleResult::Scheduled { task_id })
    }

    async fn run_dream(
        &self,
        task_id: String,
        dedupe_key: String,
        params: ScheduleDreamParams,
        now: DateTime<Utc>,
        cancelled: Arc<AtomicBool>,
    ) {
        let mut lock_acquired = false;
        let result = match acquire_dream_lock(&params.paths).await {
            Ok(()) => {
                lock_acquired = true;
                self.inner
                    .runtime
                    .dream(
                        params.paths.clone(),
                        params.session_id.clone(),
                        now,
                        cancelled.clone(),
                    )
                    .await
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                self.update_task(
                    &task_id,
                    Some(MemoryTaskStatus::Skipped),
                    Some("Skipped managed auto-memory dream: consolidation lock already exists."),
                    json!({ "skippedReason": "locked" }),
                    None,
                );
                Err("dream lock already exists".to_owned())
            }
            Err(error) => Err(error.to_string()),
        };

        if let Ok(result) = &result {
            if !cancelled.load(Ordering::Acquire) {
                self.update_task(
                    &task_id,
                    Some(MemoryTaskStatus::Completed),
                    result
                        .system_message
                        .as_deref()
                        .or(Some("Managed auto-memory dream completed.")),
                    json!({
                        "touchedTopics": result.touched_topics,
                        "dedupedEntries": result.deduped_entries,
                        "lastDreamAt": timestamp(now),
                    }),
                    None,
                );
                if let Err(error) = update_dream_metadata(
                    &params.paths,
                    &params.session_id,
                    now,
                    &result.touched_topics,
                )
                .await
                {
                    self.update_task(
                        &task_id,
                        None,
                        None,
                        json!({ "metadataWriteError": error }),
                        None,
                    );
                }
            }
        } else if cancelled.load(Ordering::Acquire)
            && self
                .get_task(&task_id)
                .is_some_and(|record| record.status == MemoryTaskStatus::Cancelled)
        {
            // cancel_task already set terminal state; preserve it.
        } else if let Err(error) = &result {
            // Lock contention is represented as skipped above. Other errors
            // fail the background task record instead of panicking the caller.
            if self
                .get_task(&task_id)
                .is_some_and(|record| record.status != MemoryTaskStatus::Skipped)
            {
                self.update_task(
                    &task_id,
                    Some(MemoryTaskStatus::Failed),
                    None,
                    Value::Null,
                    Some(error.clone()),
                );
            }
        }

        if lock_acquired {
            if let Err(error) =
                tokio::fs::remove_file(params.paths.auto_memory_consolidation_lock_path()).await
            {
                if error.kind() != io::ErrorKind::NotFound {
                    lock(&self.inner.state).dream_lock_release_failed = true;
                    self.update_task(
                        &task_id,
                        None,
                        None,
                        json!({ "lockReleaseError": error.to_string() }),
                        None,
                    );
                }
            }
        }
        {
            let mut state = lock(&self.inner.state);
            state.dream_in_flight_by_key.remove(&dedupe_key);
            state.dream_abort_flags.remove(&task_id);
            state.active_tasks.remove(&task_id);
        }
        self.publish_active_count();
    }

    pub fn cancel_task(&self, task_id: &str) -> bool {
        let abort = {
            let mut state = lock(&self.inner.state);
            let Some(record) = state.tasks.get(task_id) else {
                return false;
            };
            if record.task_type != ManagedMemoryTaskType::Dream
                || record.status != MemoryTaskStatus::Running
            {
                return false;
            }
            let Some(abort) = state.dream_abort_flags.get(task_id).cloned() else {
                return false;
            };
            let record = state
                .tasks
                .get_mut(task_id)
                .expect("task record checked before cancellation");
            update_record(
                record,
                Some(MemoryTaskStatus::Cancelled),
                Some("Cancelled by user."),
                Value::Null,
                None,
            );
            abort
        };
        abort.store(true, Ordering::Release);
        self.notify(Some(ManagedMemoryTaskType::Dream));
        true
    }

    /// Apply the skill-review gates and launch the reviewer in the background.
    pub fn schedule_skill_review(
        &self,
        params: ScheduleSkillReviewParams,
    ) -> SkillReviewScheduleResult {
        if params.enabled == Some(false) || !params.has_config {
            return skipped_skill(SkillReviewSkipReason::Disabled);
        }
        if params.skills_modified {
            return skipped_skill(SkillReviewSkipReason::SkillsModifiedInSession);
        }
        let threshold = params.threshold.unwrap_or(AUTO_SKILL_THRESHOLD);
        if params.tool_call_count < threshold {
            return skipped_skill(SkillReviewSkipReason::BelowThreshold);
        }
        if self.inner.runtime.is_under_memory_pressure() {
            return skipped_skill(SkillReviewSkipReason::MemoryPressure);
        }
        let project_root = params.paths.project_root().to_path_buf();
        let record = {
            let mut state = lock(&self.inner.state);
            if let Some(task_id) = state.skill_review_in_flight_by_project.get(&project_root) {
                return SkillReviewScheduleResult::Skipped {
                    reason: SkillReviewSkipReason::AlreadyRunning,
                    task_id: Some(task_id.clone()),
                };
            }
            let record = make_task_record(
                ManagedMemoryTaskType::SkillReview,
                &params.paths,
                &params.session_id,
            );
            let mut record = record;
            update_record(
                &mut record,
                Some(MemoryTaskStatus::Running),
                Some("Running managed skill review."),
                json!({
                    "historyLength": params.history.len(),
                    "toolCallCount": params.tool_call_count,
                    "threshold": threshold,
                }),
                None,
            );
            state
                .skill_review_in_flight_by_project
                .insert(project_root, record.id.clone());
            state.active_tasks.insert(record.id.clone());
            state.tasks.insert(record.id.clone(), record.clone());
            record
        };
        self.publish_active_count();
        self.notify(Some(ManagedMemoryTaskType::SkillReview));
        let manager = self.clone();
        let task_id = record.id.clone();
        tokio::spawn(async move {
            manager.run_skill_review(record.id, params).await;
        });
        SkillReviewScheduleResult::Scheduled { task_id }
    }

    async fn run_skill_review(&self, task_id: String, params: ScheduleSkillReviewParams) {
        let existing_dirs = if params.confirm_before_persist {
            Some(
                super::skill_review_agent_planner::list_existing_skill_dir_names(
                    params.paths.project_root(),
                )
                .await
                .into_iter()
                .collect::<HashSet<_>>(),
            )
        } else {
            None
        };
        match self.inner.runtime.skill_review(params.clone()).await {
            Ok(result) => {
                let pending =
                    if params.confirm_before_persist && !result.touched_skill_files.is_empty() {
                        match stage_skill_dirs(
                            &result.touched_skill_files,
                            params.paths.project_root(),
                            existing_dirs.as_ref().expect("snapshot taken when staging"),
                            &task_id,
                        )
                        .await
                        {
                            Ok(pending) => pending,
                            Err(error) => {
                                self.fail_skill_review(&task_id, &error.to_string());
                                self.finish_skill_review(&task_id, params.paths.project_root());
                                return;
                            }
                        }
                    } else {
                        Vec::new()
                    };
                let default_progress = if pending.is_empty() {
                    result.system_message.as_deref().or(Some(
                        "Managed skill review completed without durable changes.",
                    ))
                } else {
                    None
                };
                let mut metadata = json!({ "touchedSkillFiles": result.touched_skill_files });
                if !pending.is_empty() {
                    metadata["pendingSkills"] =
                        serde_json::to_value(&pending).unwrap_or(Value::Null);
                    self.update_task(
                        &task_id,
                        Some(MemoryTaskStatus::Completed),
                        Some(&format!("{} skill(s) awaiting review.", pending.len())),
                        metadata,
                        None,
                    );
                } else {
                    self.update_task(
                        &task_id,
                        Some(MemoryTaskStatus::Completed),
                        default_progress,
                        metadata,
                        None,
                    );
                }
            }
            Err(error) => self.fail_skill_review(&task_id, &error),
        }
        self.finish_skill_review(&task_id, params.paths.project_root());
    }

    fn fail_skill_review(&self, task_id: &str, error: &str) {
        self.update_task(
            task_id,
            Some(MemoryTaskStatus::Failed),
            None,
            Value::Null,
            Some(error.to_owned()),
        );
    }

    fn finish_skill_review(&self, task_id: &str, project_root: &Path) {
        let mut state = lock(&self.inner.state);
        state.skill_review_in_flight_by_project.remove(project_root);
        state.active_tasks.remove(task_id);
        drop(state);
        self.publish_active_count();
    }

    pub async fn accept_pending_skill_from_task(
        &self,
        task_id: &str,
        skill_name: &str,
    ) -> Result<(), String> {
        self.resolve_pending_skill(task_id, skill_name, true).await
    }

    pub async fn reject_pending_skill_from_task(
        &self,
        task_id: &str,
        skill_name: &str,
    ) -> Result<(), String> {
        self.resolve_pending_skill(task_id, skill_name, false).await
    }

    async fn resolve_pending_skill(
        &self,
        task_id: &str,
        skill_name: &str,
        accept: bool,
    ) -> Result<(), String> {
        let record = self
            .get_task(task_id)
            .ok_or_else(|| "unknown task".to_owned())?;
        let pending = record
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.get("pendingSkills"))
            .and_then(|value| serde_json::from_value::<Vec<PendingSkill>>(value.clone()).ok())
            .unwrap_or_default();
        let target = pending
            .iter()
            .find(|item| item.name == skill_name)
            .cloned()
            .ok_or_else(|| "skill is not pending on this task".to_owned())?;
        if accept {
            accept_pending_skill(&target)
                .await
                .map_err(|error| error.to_string())?;
        } else {
            reject_pending_skill(&target)
                .await
                .map_err(|error| error.to_string())?;
        }
        // Re-read after filesystem I/O so concurrent accept/reject calls each
        // remove only their own entry, retaining the source race invariant.
        let mut state = lock(&self.inner.state);
        let Some(record) = state.tasks.get_mut(task_id) else {
            return Ok(());
        };
        let mut remaining = record
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.get("pendingSkills"))
            .and_then(|value| serde_json::from_value::<Vec<PendingSkill>>(value.clone()).ok())
            .unwrap_or_default();
        remaining.retain(|item| item.name != skill_name);
        update_record(
            record,
            None,
            None,
            json!({ "pendingSkills": remaining }),
            None,
        );
        drop(state);
        self.notify(Some(ManagedMemoryTaskType::SkillReview));
        Ok(())
    }

    pub fn reset_extract_state_for_tests(&self) {
        let mut state = lock(&self.inner.state);
        state.extract_running.clear();
        state.extract_current_task_id.clear();
        state.extract_queued.clear();
    }

    pub fn reset_dream_state_for_tests(&self) {
        let mut state = lock(&self.inner.state);
        state.dream_in_flight_by_key.clear();
        state.dream_last_session_scan_at.clear();
    }

    fn store_with(
        &self,
        mut record: MemoryTaskRecord,
        status: MemoryTaskStatus,
        progress: Option<&str>,
        metadata: Value,
    ) {
        update_record(&mut record, Some(status), progress, metadata, None);
        let task_type = record.task_type.clone();
        lock(&self.inner.state)
            .tasks
            .insert(record.id.clone(), record);
        self.notify(Some(task_type));
    }

    fn update_task(
        &self,
        task_id: &str,
        status: Option<MemoryTaskStatus>,
        progress: Option<&str>,
        metadata: Value,
        error: Option<String>,
    ) {
        let task_type = {
            let mut state = lock(&self.inner.state);
            let Some(record) = state.tasks.get_mut(task_id) else {
                return;
            };
            update_record(record, status, progress, metadata, error);
            record.task_type.clone()
        };
        self.notify(Some(task_type));
    }

    fn notify(&self, changed_type: Option<ManagedMemoryTaskType>) {
        let listeners = {
            let subscribers = lock(&self.inner.subscribers);
            let mut listeners = subscribers.all.values().cloned().collect::<Vec<_>>();
            if let Some(task_type) = changed_type
                && let Some(typed) = subscribers.by_type.get(task_type_key(&task_type))
            {
                listeners.extend(typed.values().cloned());
            }
            listeners
        };
        for listener in listeners {
            listener();
        }
    }

    fn publish_active_count(&self) {
        let count = lock(&self.inner.state).active_tasks.len();
        self.inner.active_count.send_replace(count);
    }
}

impl MemoryTaskSource for MemoryManager {
    fn list_tasks_by_type(
        &self,
        task_type: ManagedMemoryTaskType,
        project_root: &Path,
    ) -> Vec<MemoryTaskRecord> {
        MemoryManager::list_tasks_by_type(self, task_type, Some(project_root))
    }
}

fn history_writes_to_memory(history: &[MemoryTurn], paths: &AutoMemoryPaths) -> bool {
    const WRITE_TOOLS: [&str; 4] = ["write_file", "edit", "replace", "create_file"];
    history.iter().any(|turn| {
        turn.parts.iter().any(|part| {
            let Some(call) = part.get("functionCall") else {
                return false;
            };
            let Some(name) = call.get("name").and_then(Value::as_str) else {
                return false;
            };
            if !WRITE_TOOLS.contains(&name) {
                return false;
            }
            let Some(args) = call.get("args").and_then(Value::as_object) else {
                return false;
            };
            let file_path = ["file_path", "path", "target_file"]
                .into_iter()
                .filter_map(|key| args.get(key))
                .find(|value| !value.is_null())
                .and_then(Value::as_str);
            let Some(file_path) = file_path else {
                return false;
            };
            let file_path = Path::new(file_path);
            let absolute = if file_path.is_absolute() {
                file_path.to_path_buf()
            } else {
                paths.project_root().join(file_path)
            };
            paths.is_any_auto_memory_path(&absolute) || paths.is_team_auto_memory_path(&absolute)
        })
    })
}

fn skipped_extract_result(
    session_id: &str,
    now: DateTime<Utc>,
    reason: ExtractSkipReason,
) -> ExtractResult {
    ExtractResult {
        touched_topics: Vec::new(),
        skipped_reason: Some(reason),
        system_message: None,
        cursor: AutoMemoryExtractCursor {
            session_id: Some(session_id.to_owned()),
            processed_offset: None,
            updated_at: timestamp(now),
        },
    }
}

fn make_task_record(
    task_type: ManagedMemoryTaskType,
    paths: &AutoMemoryPaths,
    session_id: &str,
) -> MemoryTaskRecord {
    let now = timestamp(Utc::now());
    MemoryTaskRecord {
        id: Uuid::new_v4().to_string(),
        task_type,
        project_root: path_string(paths.project_root()),
        session_id: Some(session_id.to_owned()),
        status: MemoryTaskStatus::Pending,
        created_at: now.clone(),
        updated_at: now,
        progress_text: None,
        error: None,
        metadata: None,
    }
}

fn update_record(
    record: &mut MemoryTaskRecord,
    status: Option<MemoryTaskStatus>,
    progress: Option<&str>,
    metadata: Value,
    error: Option<String>,
) {
    if let Some(status) = status {
        record.status = status;
    }
    if let Some(progress) = progress {
        record.progress_text = Some(progress.to_owned());
    }
    if let Some(error) = error {
        record.error = Some(error);
    }
    if let Value::Object(patch) = metadata {
        let mut merged = record
            .metadata
            .take()
            .and_then(|value| value.as_object().cloned())
            .unwrap_or_default();
        for (key, value) in patch {
            merged.insert(key, value);
        }
        record.metadata = Some(Value::Object(merged));
    }
    record.updated_at = timestamp(Utc::now());
}

fn timestamp(time: DateTime<Utc>) -> String {
    time.to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn path_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn skipped_dream(reason: DreamSkipReason) -> DreamScheduleResult {
    DreamScheduleResult::Skipped {
        reason,
        task_id: None,
    }
}

fn skipped_skill(reason: SkillReviewSkipReason) -> SkillReviewScheduleResult {
    SkillReviewScheduleResult::Skipped {
        reason,
        task_id: None,
    }
}

fn task_type_key(task_type: &ManagedMemoryTaskType) -> &'static str {
    match task_type {
        ManagedMemoryTaskType::Extract => "extract",
        ManagedMemoryTaskType::Dream => "dream",
        ManagedMemoryTaskType::SkillReview => "skill-review",
    }
}

fn parse_timestamp(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|time| time.with_timezone(&Utc))
}

fn hours_since(last_dream_at: &str, now: DateTime<Utc>) -> Option<f64> {
    let parsed = parse_timestamp(last_dream_at)?;
    Some((now.timestamp_millis() - parsed.timestamp_millis()) as f64 / 3_600_000.0)
}

async fn read_dream_metadata(paths: &AutoMemoryPaths) -> Result<AutoMemoryMetadata, String> {
    let bytes = tokio::fs::read(paths.auto_memory_metadata_path())
        .await
        .map_err(|error| error.to_string())?;
    serde_json::from_slice(&bytes).map_err(|error| error.to_string())
}

async fn update_dream_metadata(
    paths: &AutoMemoryPaths,
    session_id: &str,
    now: DateTime<Utc>,
    touched_topics: &[AutoMemoryType],
) -> Result<(), String> {
    let mut metadata = read_dream_metadata(paths).await?;
    let now_text = timestamp(now);
    metadata.last_dream_at = Some(now_text.clone());
    metadata.last_dream_session_id = Some(session_id.to_owned());
    metadata.updated_at = now_text;
    metadata.last_dream_touched_topics = Some(touched_topics.to_vec());
    metadata.last_dream_status = Some(if touched_topics.is_empty() {
        super::store::AutoMemoryStatus::Noop
    } else {
        super::store::AutoMemoryStatus::Updated
    });
    metadata.recent_session_ids_since_dream = Some(Vec::new());
    let path = paths.auto_memory_metadata_path();
    let temp_path = path.with_extension(format!("json.{}.tmp", Uuid::new_v4()));
    let mut bytes = serde_json::to_vec_pretty(&metadata).map_err(|error| error.to_string())?;
    bytes.push(b'\n');
    tokio::fs::write(&temp_path, bytes)
        .await
        .map_err(|error| error.to_string())?;
    tokio::fs::rename(&temp_path, &path)
        .await
        .map_err(|error| error.to_string())
}

async fn dream_lock_exists(paths: &AutoMemoryPaths) -> bool {
    let lock_path = paths.auto_memory_consolidation_lock_path();
    let Ok(metadata) = tokio::fs::metadata(&lock_path).await else {
        return false;
    };
    let mtime = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_secs_f64() * 1_000.0)
        .unwrap_or(0.0);
    let age_ms = current_time_ms() - mtime;
    let holder_pid = tokio::fs::read_to_string(&lock_path)
        .await
        .ok()
        .and_then(|content| content.trim().parse::<u32>().ok())
        .filter(|pid| *pid > 0);
    if age_ms <= DREAM_LOCK_STALE_MS && holder_pid.is_some_and(is_process_running) {
        return true;
    }
    let _ = tokio::fs::remove_file(lock_path).await;
    false
}

async fn acquire_dream_lock(paths: &AutoMemoryPaths) -> io::Result<()> {
    let path = paths.auto_memory_consolidation_lock_path();
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .await?;
    use tokio::io::AsyncWriteExt;
    file.write_all(std::process::id().to_string().as_bytes())
        .await
}

fn current_time_ms() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
        * 1_000.0
}

fn is_process_running(pid: u32) -> bool {
    crate::services::session_registry::is_pid_alive(i64::from(pid))
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::atomic::AtomicBool;

    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::{mpsc, oneshot};

    use super::*;
    use crate::memory::paths::MemoryPathInputs;

    struct GateRuntime {
        pressure: AtomicBool,
        dream_enabled: bool,
        extracts: AtomicUsize,
        extract_started: mpsc::UnboundedSender<String>,
        extract_releases: Arc<tokio::sync::Mutex<VecDeque<oneshot::Receiver<()>>>>,
        scanned_sessions: Vec<String>,
        dream_started: mpsc::UnboundedSender<()>,
        dream_release: Arc<tokio::sync::Mutex<Option<oneshot::Receiver<()>>>>,
        observed_cancel: Arc<AtomicBool>,
        skill_reviews: AtomicUsize,
        review_files: Vec<(PathBuf, String)>,
    }

    fn gate_runtime(
        scanned_sessions: Vec<String>,
        extract_releases: Vec<oneshot::Receiver<()>>,
        dream_release: Option<oneshot::Receiver<()>>,
        review_files: Vec<(PathBuf, String)>,
    ) -> (
        Arc<GateRuntime>,
        mpsc::UnboundedReceiver<String>,
        mpsc::UnboundedReceiver<()>,
    ) {
        let (extract_started, extract_started_rx) = mpsc::unbounded_channel();
        let (dream_started, dream_started_rx) = mpsc::unbounded_channel();
        (
            Arc::new(GateRuntime {
                pressure: AtomicBool::new(false),
                dream_enabled: true,
                extracts: AtomicUsize::new(0),
                extract_started,
                extract_releases: Arc::new(tokio::sync::Mutex::new(extract_releases.into())),
                scanned_sessions,
                dream_started,
                dream_release: Arc::new(tokio::sync::Mutex::new(dream_release)),
                observed_cancel: Arc::new(AtomicBool::new(false)),
                skill_reviews: AtomicUsize::new(0),
                review_files,
            }),
            extract_started_rx,
            dream_started_rx,
        )
    }

    impl MemoryManagerRuntime for GateRuntime {
        fn is_under_memory_pressure(&self) -> bool {
            self.pressure.load(Ordering::Acquire)
        }

        fn managed_auto_dream_enabled(&self) -> bool {
            self.dream_enabled
        }

        fn extract<'a>(
            &'a self,
            params: ScheduleExtractParams,
        ) -> MemoryManagerFuture<'a, ExtractResult> {
            let extract_started = self.extract_started.clone();
            let extract_releases = self.extract_releases.clone();
            Box::pin(async move {
                self.extracts.fetch_add(1, Ordering::SeqCst);
                let release = extract_releases.lock().await.pop_front();
                let _ = extract_started.send(params.session_id.clone());
                if let Some(release) = release {
                    let _ = release.await;
                }
                Ok(ExtractResult {
                    touched_topics: vec![AutoMemoryType::User],
                    skipped_reason: None,
                    system_message: None,
                    cursor: AutoMemoryExtractCursor {
                        session_id: Some(params.session_id),
                        processed_offset: Some(1),
                        updated_at: timestamp(Utc::now()),
                    },
                })
            })
        }

        fn scan_sessions<'a>(
            &'a self,
            _paths: &'a AutoMemoryPaths,
            _since_ms: f64,
            _exclude_session_id: &'a str,
        ) -> MemoryManagerFuture<'a, Vec<String>> {
            let sessions = self.scanned_sessions.clone();
            Box::pin(async move { Ok(sessions) })
        }

        fn dream<'a>(
            &'a self,
            _paths: AutoMemoryPaths,
            _session_id: String,
            _now: DateTime<Utc>,
            cancelled: Arc<AtomicBool>,
        ) -> MemoryManagerFuture<'a, DreamResult> {
            let dream_started = self.dream_started.clone();
            let dream_release = self.dream_release.clone();
            let observed_cancel = self.observed_cancel.clone();
            Box::pin(async move {
                let _ = dream_started.send(());
                if let Some(release) = dream_release.lock().await.take() {
                    let _ = release.await;
                }
                observed_cancel.store(cancelled.load(Ordering::Acquire), Ordering::Release);
                Ok(DreamResult::default())
            })
        }

        fn skill_review<'a>(
            &'a self,
            _params: ScheduleSkillReviewParams,
        ) -> MemoryManagerFuture<'a, SkillReviewResult> {
            let review_files = self.review_files.clone();
            Box::pin(async move {
                self.skill_reviews.fetch_add(1, Ordering::SeqCst);
                for (path, contents) in &review_files {
                    if let Some(parent) = path.parent() {
                        tokio::fs::create_dir_all(parent)
                            .await
                            .map_err(|error| error.to_string())?;
                    }
                    tokio::fs::write(path, contents)
                        .await
                        .map_err(|error| error.to_string())?;
                }
                Ok(SkillReviewResult {
                    touched_skill_files: review_files.into_iter().map(|(path, _)| path).collect(),
                    system_message: None,
                })
            })
        }
    }

    fn unique_paths(label: &str) -> (PathBuf, AutoMemoryPaths) {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let temp_root = std::env::temp_dir().join(format!(
            "canopy-memory-manager-{label}-{}-{suffix}",
            std::process::id()
        ));
        let cwd = temp_root.clone();
        let paths = MemoryPathInputs {
            project_root: temp_root.join("project"),
            runtime_base_dir: temp_root.join("runtime"),
            memory_base_dir_override: None,
            memory_local: true,
            project_scope: Some("workspace".to_owned()),
            cwd,
            home_dir: None,
        }
        .paths();
        (temp_root, paths)
    }

    fn dream_params(
        paths: AutoMemoryPaths,
        session_id: &str,
        now: DateTime<Utc>,
    ) -> ScheduleDreamParams {
        ScheduleDreamParams {
            paths,
            session_id: session_id.to_owned(),
            enabled: true,
            has_config: true,
            now: Some(now),
            min_hours_between_dreams: Some(0.0),
            min_sessions_between_dreams: Some(1),
        }
    }

    fn review_params(paths: AutoMemoryPaths, tool_call_count: usize) -> ScheduleSkillReviewParams {
        ScheduleSkillReviewParams {
            paths,
            session_id: "review-session".to_owned(),
            history: vec![MemoryTurn {
                role: Some("user".to_owned()),
                parts: vec![json!({ "text": "remember this" })],
            }],
            tool_call_count,
            skills_modified: false,
            enabled: None,
            has_config: true,
            threshold: Some(2),
            max_turns: Some(3),
            timeout: Some(Duration::from_secs(5)),
            confirm_before_persist: true,
        }
    }

    fn extract_params(
        paths: AutoMemoryPaths,
        session_id: &str,
        history_length: usize,
    ) -> ScheduleExtractParams {
        ScheduleExtractParams {
            paths,
            session_id: session_id.to_owned(),
            history: (0..history_length)
                .map(|index| MemoryTurn {
                    role: Some("user".to_owned()),
                    parts: vec![json!({ "text": format!("turn {index}") })],
                })
                .collect(),
            now: Some(Utc::now()),
        }
    }

    struct TestRuntime {
        pressure: bool,
        extracts: AtomicUsize,
    }

    impl MemoryManagerRuntime for TestRuntime {
        fn is_under_memory_pressure(&self) -> bool {
            self.pressure
        }

        fn extract<'a>(
            &'a self,
            params: ScheduleExtractParams,
        ) -> MemoryManagerFuture<'a, ExtractResult> {
            self.extracts.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                Ok(ExtractResult {
                    touched_topics: vec![AutoMemoryType::User],
                    skipped_reason: None,
                    system_message: None,
                    cursor: AutoMemoryExtractCursor {
                        session_id: Some(params.session_id),
                        processed_offset: Some(3),
                        updated_at: timestamp(Utc::now()),
                    },
                })
            })
        }

        fn scan_sessions<'a>(
            &'a self,
            _paths: &'a AutoMemoryPaths,
            _since_ms: f64,
            _exclude_session_id: &'a str,
        ) -> MemoryManagerFuture<'a, Vec<String>> {
            Box::pin(async { Ok(Vec::new()) })
        }

        fn dream<'a>(
            &'a self,
            _paths: AutoMemoryPaths,
            _session_id: String,
            _now: DateTime<Utc>,
            _cancelled: Arc<AtomicBool>,
        ) -> MemoryManagerFuture<'a, DreamResult> {
            Box::pin(async { Ok(DreamResult::default()) })
        }
    }

    fn paths() -> AutoMemoryPaths {
        let cwd = std::env::current_dir().unwrap();
        MemoryPathInputs {
            project_root: cwd.join("project"),
            runtime_base_dir: cwd.join("runtime"),
            memory_base_dir_override: None,
            memory_local: true,
            project_scope: Some("workspace".to_owned()),
            cwd,
            home_dir: None,
        }
        .paths()
    }

    fn manager(pressure: bool) -> (MemoryManager, Arc<TestRuntime>) {
        let runtime = Arc::new(TestRuntime {
            pressure,
            extracts: AtomicUsize::new(0),
        });
        (MemoryManager::new(runtime.clone()), runtime)
    }

    fn params(history: Vec<MemoryTurn>) -> ScheduleExtractParams {
        ScheduleExtractParams {
            paths: paths(),
            session_id: "session-1".to_owned(),
            history,
            now: Some(Utc::now()),
        }
    }

    #[tokio::test]
    async fn extraction_records_completion_and_notifies_matching_subscribers() {
        let (manager, runtime) = manager(false);
        let all_calls = Arc::new(AtomicUsize::new(0));
        let typed_calls = Arc::new(AtomicUsize::new(0));
        let all_counter = all_calls.clone();
        let typed_counter = typed_calls.clone();
        let _all = manager.subscribe(
            Arc::new(move || {
                all_counter.fetch_add(1, Ordering::SeqCst);
            }),
            None,
        );
        let _typed = manager.subscribe(
            Arc::new(move || {
                typed_counter.fetch_add(1, Ordering::SeqCst);
            }),
            Some(ManagedMemoryTaskType::Extract),
        );

        let result = manager.schedule_extract(params(Vec::new())).await.unwrap();
        assert_eq!(result.touched_topics, [AutoMemoryType::User]);
        assert_eq!(runtime.extracts.load(Ordering::SeqCst), 1);
        let tasks = manager.list_tasks_by_type(ManagedMemoryTaskType::Extract, None);
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].status, MemoryTaskStatus::Completed);
        assert!(all_calls.load(Ordering::SeqCst) >= 3);
        assert_eq!(
            all_calls.load(Ordering::SeqCst),
            typed_calls.load(Ordering::SeqCst)
        );
        assert!(manager.drain(Some(Duration::from_millis(1))).await);
    }

    #[tokio::test]
    async fn memory_tool_write_skips_extract_and_records_reason() {
        let (manager, runtime) = manager(false);
        let root = paths().project_root().to_path_buf();
        let history = vec![MemoryTurn {
            role: Some("model".to_owned()),
            parts: vec![json!({
                "functionCall": {
                    "name": "write_file",
                    "args": { "file_path": root.join(".canopy/memory/user/a.md") }
                }
            })],
        }];
        let result = manager.schedule_extract(params(history)).await.unwrap();
        assert_eq!(result.skipped_reason, Some(ExtractSkipReason::MemoryTool));
        assert_eq!(runtime.extracts.load(Ordering::SeqCst), 0);
        assert_eq!(
            manager.list_tasks_by_type(ManagedMemoryTaskType::Extract, None)[0].status,
            MemoryTaskStatus::Skipped
        );
    }

    #[tokio::test]
    async fn pressure_gate_skips_extract_without_calling_the_runtime() {
        let (manager, runtime) = manager(true);
        let result = manager.schedule_extract(params(Vec::new())).await.unwrap();
        assert_eq!(
            result.skipped_reason,
            Some(ExtractSkipReason::MemoryPressure)
        );
        assert_eq!(runtime.extracts.load(Ordering::SeqCst), 0);
        assert_eq!(
            manager.list_tasks_by_type(ManagedMemoryTaskType::Extract, None)[0].status,
            MemoryTaskStatus::Skipped
        );
    }

    #[tokio::test]
    async fn dream_gates_live_lock_and_cancellation_are_enforced() {
        let (temp_root, paths) = unique_paths("dream-gates");
        tokio::fs::create_dir_all(paths.project_root())
            .await
            .unwrap();
        let (runtime, _, _) = gate_runtime(vec!["old-session".to_owned()], vec![], None, vec![]);
        let manager = MemoryManager::new(runtime.clone());
        let now = Utc::now();

        let mut disabled = dream_params(paths.clone(), "disabled-session", now);
        disabled.has_config = false;
        assert_eq!(
            manager.schedule_dream(disabled).await.unwrap(),
            DreamScheduleResult::Skipped {
                reason: DreamSkipReason::Disabled,
                task_id: None,
            }
        );

        runtime.pressure.store(true, Ordering::Release);
        assert_eq!(
            manager
                .schedule_dream(dream_params(paths.clone(), "pressure-session", now))
                .await
                .unwrap(),
            DreamScheduleResult::Skipped {
                reason: DreamSkipReason::MemoryPressure,
                task_id: None,
            }
        );
        runtime.pressure.store(false, Ordering::Release);

        let mut below_threshold = dream_params(paths.clone(), "few-sessions", now);
        below_threshold.min_sessions_between_dreams = Some(2);
        assert_eq!(
            manager.schedule_dream(below_threshold).await.unwrap(),
            DreamScheduleResult::Skipped {
                reason: DreamSkipReason::MinSessions,
                task_id: None,
            }
        );

        let lock_path = paths.auto_memory_consolidation_lock_path();
        tokio::fs::write(&lock_path, std::process::id().to_string())
            .await
            .unwrap();
        let after_scan_cooldown = now + chrono::Duration::minutes(11);
        assert_eq!(
            manager
                .schedule_dream(dream_params(
                    paths.clone(),
                    "locked-session",
                    after_scan_cooldown,
                ))
                .await
                .unwrap(),
            DreamScheduleResult::Skipped {
                reason: DreamSkipReason::Locked,
                task_id: None,
            }
        );
        assert!(tokio::fs::metadata(&lock_path).await.is_ok());
        tokio::fs::remove_dir_all(temp_root).await.unwrap();

        let (cancel_root, cancel_paths) = unique_paths("dream-cancel");
        tokio::fs::create_dir_all(cancel_paths.project_root())
            .await
            .unwrap();
        let (release_tx, release_rx) = oneshot::channel();
        let (cancel_runtime, _, mut dream_started) = gate_runtime(
            vec!["old-session".to_owned()],
            vec![],
            Some(release_rx),
            vec![],
        );
        let cancel_manager = MemoryManager::new(cancel_runtime.clone());
        let scheduled = cancel_manager
            .schedule_dream(dream_params(cancel_paths.clone(), "cancel-me", Utc::now()))
            .await
            .unwrap();
        let DreamScheduleResult::Scheduled { task_id } = scheduled else {
            panic!("expected dream task to schedule, got {scheduled:?}");
        };
        dream_started.recv().await.expect("dream runtime started");
        assert!(cancel_manager.cancel_task(&task_id));
        assert_eq!(
            cancel_manager.get_task(&task_id).unwrap().status,
            MemoryTaskStatus::Cancelled
        );
        release_tx.send(()).unwrap();
        assert!(cancel_manager.drain(None).await);
        assert!(cancel_runtime.observed_cancel.load(Ordering::Acquire));
        assert_eq!(
            cancel_manager.get_task(&task_id).unwrap().status,
            MemoryTaskStatus::Cancelled
        );
        assert!(
            tokio::fs::metadata(cancel_paths.auto_memory_consolidation_lock_path())
                .await
                .is_err()
        );
        tokio::fs::remove_dir_all(cancel_root).await.unwrap();
    }

    #[tokio::test]
    async fn skill_review_threshold_stages_and_resolves_pending_skills() {
        let (temp_root, paths) = unique_paths("skill-review");
        tokio::fs::create_dir_all(paths.project_root())
            .await
            .unwrap();
        let keep_manifest = paths
            .project_root()
            .join(".canopy/skills/auto-skill-keep/SKILL.md");
        let discard_manifest = paths
            .project_root()
            .join(".canopy/skills/auto-skill-discard/SKILL.md");
        let (runtime, _, _) = gate_runtime(
            vec![],
            vec![],
            None,
            vec![
                (
                    keep_manifest.clone(),
                    "---\ndescription: Keep this skill\n---\n# Keep\n".to_owned(),
                ),
                (
                    discard_manifest.clone(),
                    "---\ndescription: Discard this skill\n---\n# Discard\n".to_owned(),
                ),
            ],
        );
        let manager = MemoryManager::new(runtime.clone());

        assert_eq!(
            manager.schedule_skill_review(review_params(paths.clone(), 1)),
            SkillReviewScheduleResult::Skipped {
                reason: SkillReviewSkipReason::BelowThreshold,
                task_id: None,
            }
        );
        assert_eq!(runtime.skill_reviews.load(Ordering::SeqCst), 0);

        let scheduled = manager.schedule_skill_review(review_params(paths.clone(), 2));
        let SkillReviewScheduleResult::Scheduled { task_id } = scheduled else {
            panic!("expected skill review to schedule, got {scheduled:?}");
        };
        assert!(manager.drain(None).await);
        let record = manager.get_task(&task_id).unwrap();
        assert_eq!(record.status, MemoryTaskStatus::Completed);
        let pending: Vec<PendingSkill> = serde_json::from_value(
            record
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.get("pendingSkills"))
                .unwrap()
                .clone(),
        )
        .unwrap();
        assert_eq!(
            pending
                .iter()
                .map(|skill| skill.name.as_str())
                .collect::<Vec<_>>(),
            ["auto-skill-keep", "auto-skill-discard"]
        );
        assert!(tokio::fs::metadata(&keep_manifest).await.is_err());
        assert!(tokio::fs::metadata(&discard_manifest).await.is_err());

        manager
            .accept_pending_skill_from_task(&task_id, "auto-skill-keep")
            .await
            .unwrap();
        manager
            .reject_pending_skill_from_task(&task_id, "auto-skill-discard")
            .await
            .unwrap();
        assert!(tokio::fs::metadata(&keep_manifest).await.is_ok());
        assert!(tokio::fs::metadata(&discard_manifest).await.is_err());
        let updated = manager.get_task(&task_id).unwrap();
        let remaining: Vec<PendingSkill> = serde_json::from_value(
            updated
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.get("pendingSkills"))
                .unwrap()
                .clone(),
        )
        .unwrap();
        assert!(remaining.is_empty());
        tokio::fs::remove_dir_all(temp_root).await.unwrap();
    }

    #[tokio::test]
    async fn queued_extractions_coalesce_to_the_latest_trailing_request() {
        let (temp_root, paths) = unique_paths("extract-queue");
        tokio::fs::create_dir_all(paths.project_root())
            .await
            .unwrap();
        let (first_release_tx, first_release_rx) = oneshot::channel();
        let (trailing_release_tx, trailing_release_rx) = oneshot::channel();
        let (runtime, mut extract_started, _) = gate_runtime(
            vec![],
            vec![first_release_rx, trailing_release_rx],
            None,
            vec![],
        );
        let manager = MemoryManager::new(runtime.clone());

        let first_manager = manager.clone();
        let first_paths = paths.clone();
        let first = tokio::spawn(async move {
            first_manager
                .schedule_extract(extract_params(first_paths, "first", 1))
                .await
                .unwrap()
        });
        assert_eq!(extract_started.recv().await.as_deref(), Some("first"));

        let second = manager
            .schedule_extract(extract_params(paths.clone(), "superseded", 2))
            .await
            .unwrap();
        let trailing = manager
            .schedule_extract(extract_params(paths.clone(), "latest", 3))
            .await
            .unwrap();
        assert_eq!(second.skipped_reason, Some(ExtractSkipReason::Queued));
        assert_eq!(trailing.skipped_reason, Some(ExtractSkipReason::Queued));

        let queued_record = manager
            .list_tasks_by_type(ManagedMemoryTaskType::Extract, Some(paths.project_root()))
            .into_iter()
            .find(|record| record.status == MemoryTaskStatus::Pending)
            .expect("one trailing task should remain queued");
        // The task record keeps the session that created it, while the
        // execution params and metadata are replaced by the latest call.
        assert_eq!(queued_record.session_id.as_deref(), Some("superseded"));
        assert_eq!(
            queued_record
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.get("historyLength"))
                .and_then(Value::as_u64),
            Some(3)
        );

        first_release_tx.send(()).unwrap();
        let first_result = first.await.unwrap();
        assert_eq!(first_result.cursor.session_id.as_deref(), Some("first"));
        assert_eq!(extract_started.recv().await.as_deref(), Some("latest"));
        trailing_release_tx.send(()).unwrap();
        assert!(manager.drain(None).await);

        let records =
            manager.list_tasks_by_type(ManagedMemoryTaskType::Extract, Some(paths.project_root()));
        assert_eq!(records.len(), 2);
        assert!(
            records
                .iter()
                .all(|record| record.status == MemoryTaskStatus::Completed)
        );
        assert!(
            records
                .iter()
                .any(|record| record.session_id.as_deref() == Some("first"))
        );
        let final_record = records
            .iter()
            .find(|record| record.session_id.as_deref() == Some("superseded"))
            .unwrap();
        assert_eq!(runtime.extracts.load(Ordering::SeqCst), 2);
        assert_eq!(
            final_record
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.get("historyLength"))
                .and_then(Value::as_u64),
            Some(3)
        );
        tokio::fs::remove_dir_all(temp_root).await.unwrap();
    }
}
