//! Workspace-bound ACP session lifecycle and per-session prompt FIFO.
//!
//! The factory is the narrow seam around ACP initialize/new-session calls.
//! This module owns `spawnOrAttach` scope selection, single-scope coalescing,
//! session admission, client attachment accounting, per-session prompt
//! serialization, and shutdown/kill ordering.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::sync::{Mutex as AsyncMutex, Notify, mpsc, oneshot, watch};
use uuid::Uuid;

use crate::services::daemon_memory_budget::{DaemonMemoryBudget, serve_journal_growth_pool_bytes};

use super::compaction_engine::{
    JournalGrowthRegistry, SessionCompactionOptions, TurnBoundaryCompactionEngine,
};
use super::event_bus::{
    BridgeEvent, EventBusOptions, EventSubscription, SessionEventBus, SubscribeOptions,
    SubscriberLimitExceededError,
};
use super::session_artifacts::{
    JsonlArtifactPersistence, SessionArtifactPersistence, SessionArtifactStore,
};
use super::session_media::SessionMediaStore;
use super::session_source::{SessionSourceMetadata, parse_session_source};
use super::workspace_paths::canonicalize_workspace;

pub type SessionRuntimeFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BridgeSessionScope {
    Single,
    Thread,
}

#[derive(Clone, Debug, Default)]
pub struct BridgeSpawnRequest {
    pub workspace_cwd: String,
    pub session_scope: Option<String>,
    pub session_id: Option<String>,
    pub client_id: Option<String>,
    pub model_service_id: Option<String>,
    pub approval_mode: Option<String>,
    pub parent_session_id: Option<String>,
    pub source_type: Option<String>,
    pub source_id: Option<String>,
    /// Protocol-native MCP server entries supplied by this ACP session.
    pub session_mcp_servers: Vec<Value>,
    pub worktree: Option<Value>,
    pub branch: Option<Value>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BridgeRestoreAction {
    Load,
    Resume,
}

#[derive(Clone, Debug, Default)]
pub struct BridgeRestoreRequest {
    pub session_id: String,
    pub workspace_cwd: String,
    pub client_id: Option<String>,
    /// `stream` is the normal ACP replay path. `response` asks the child to
    /// return a bounded replay page in the `session/load` response.
    pub history_replay: Option<String>,
    pub history_page_size: Option<usize>,
    pub live_replay_mode: Option<String>,
    pub hide_inherited_history: bool,
    pub approval_mode: Option<String>,
    pub parent_session_id: Option<String>,
    pub source_type: Option<String>,
    pub source_id: Option<String>,
    /// Protocol-native MCP server entries supplied with this restored session.
    pub session_mcp_servers: Vec<Value>,
}

#[derive(Clone, Debug)]
pub struct BridgeSessionRuntimeOptions {
    pub bound_workspace: PathBuf,
    pub session_scope: BridgeSessionScope,
    /// `None` means unlimited, matching `0` / `Infinity` in the TS options.
    pub max_sessions: Option<usize>,
    /// `None` uses the source default (5); `Some(0)` disables the cap.
    pub max_pending_prompts_per_session: Option<usize>,
    pub event_bus: EventBusOptions,
    pub max_artifacts_per_session: Option<usize>,
}

impl BridgeSessionRuntimeOptions {
    /// Resolve the adaptive journal pool from the daemon memory budget.
    /// Explicit journal caps on `event_bus` suppress derived growth.
    pub fn with_memory_budget(mut self, budget: &DaemonMemoryBudget) -> Self {
        self.event_bus.journal_growth_pool_bytes = serve_journal_growth_pool_bytes(
            budget,
            self.event_bus.max_journal_events,
            self.event_bus.max_journal_bytes,
        );
        self
    }

    /// Share journal growth accounting with other workspace runtimes owned by
    /// the same daemon. The pool byte size is derived independently from each
    /// runtime's effective configuration; sharing is recommended only when
    /// those runtimes use the same daemon budget.
    pub fn with_shared_journal_growth_registry(
        mut self,
        registry: Arc<JournalGrowthRegistry>,
    ) -> Self {
        self.event_bus.journal_growth_registry = Some(registry);
        self
    }
}

impl Default for BridgeSessionRuntimeOptions {
    fn default() -> Self {
        Self {
            bound_workspace: PathBuf::new(),
            session_scope: BridgeSessionScope::Single,
            max_sessions: Some(32),
            max_pending_prompts_per_session: Some(5),
            event_bus: EventBusOptions::default(),
            max_artifacts_per_session: None,
        }
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum BridgeSessionRuntimeError {
    #[error("AcpSessionBridge is shutting down")]
    ShuttingDown,
    #[error(
        "Workspace mismatch: runtime is bound to \"{bound}\" but request asked for \"{requested}\""
    )]
    WorkspaceMismatch { bound: String, requested: String },
    #[error("Invalid session scope: {0}")]
    InvalidScope(String),
    #[error("Invalid session metadata: {0}")]
    InvalidMetadata(String),
    #[error("Invalid approval mode: {0}")]
    InvalidApprovalMode(String),
    #[error("Bridge bound workspace must be provided")]
    MissingWorkspace,
    #[error("Session limit reached ({0})")]
    SessionLimitExceeded(usize),
    #[error("A session with id \"{0}\" is already registered")]
    SessionIdCollision(String),
    #[error("Session \"{0}\" is already being restored")]
    RestoreInProgress(String),
    #[error("Invalid session history replay mode: {0}")]
    InvalidHistoryReplay(String),
    #[error("Invalid ACP session journal limits: {0}")]
    InvalidJournalLimits(String),
    #[error("Invalid live replay mode: {0}")]
    InvalidLiveReplayMode(String),
    #[error("Invalid historyPageSize; expected 1..500")]
    InvalidHistoryPageSize,
    #[error("Session {action:?} timed out for {session_id} after {timeout_ms}ms")]
    RestoreTimeout {
        session_id: String,
        action: BridgeRestoreAction,
        timeout_ms: u64,
    },
    #[error("No session with id \"{0}\"")]
    SessionNotFound(String),
    #[error("Session \"{0}\" is closing")]
    SessionClosing(String),
    #[error("Prompt aborted")]
    PromptAborted,
    #[error("Prompt queue full for session {session_id} (limit {limit}, pending {pending})")]
    PromptQueueFull {
        session_id: String,
        limit: usize,
        pending: usize,
    },
    #[error("ACP session operation failed: {0}")]
    Operation(String),
}

pub trait BridgeSessionAgent: Send + Sync + 'static {
    fn prompt<'a>(
        &'a self,
        prompt: Value,
        cancelled: watch::Receiver<bool>,
    ) -> SessionRuntimeFuture<'a, Result<Value, String>>;
    fn cancel<'a>(&'a self) -> SessionRuntimeFuture<'a, Result<(), String>>;
    fn shutdown<'a>(&'a self) -> SessionRuntimeFuture<'a, Result<(), String>>;
}

pub struct CreatedBridgeSession {
    pub session_id: String,
    pub effective_cwd: PathBuf,
    pub created_at: String,
    pub transcript_path: Option<PathBuf>,
    pub restore_state: Option<Value>,
    pub agent: Arc<dyn BridgeSessionAgent>,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum BridgeSessionFactoryError {
    #[error("{0}")]
    Operation(String),
    #[error("session restore timed out after {0}ms")]
    Timeout(u64),
}

pub trait BridgeSessionFactory: Send + Sync + 'static {
    fn spawn<'a>(
        &'a self,
        request: BridgeSpawnRequest,
        workspace_cwd: PathBuf,
        effective_scope: BridgeSessionScope,
    ) -> SessionRuntimeFuture<'a, Result<CreatedBridgeSession, String>>;

    fn restore<'a>(
        &'a self,
        _request: BridgeRestoreRequest,
        _action: BridgeRestoreAction,
        _workspace_cwd: PathBuf,
    ) -> SessionRuntimeFuture<'a, Result<CreatedBridgeSession, BridgeSessionFactoryError>> {
        Box::pin(async {
            Err(BridgeSessionFactoryError::Operation(
                "session restore is not supported by this factory".into(),
            ))
        })
    }

    fn shutdown<'a>(&'a self) -> SessionRuntimeFuture<'a, Result<(), String>> {
        Box::pin(async { Ok(()) })
    }

    fn kill_all_sync(&self) {}
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BridgeSessionInfo {
    pub session_id: String,
    pub workspace_cwd: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_cwd: Option<String>,
    pub attached: bool,
    pub client_id: String,
    pub created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_session_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BridgeRestoredSessionInfo {
    #[serde(flatten)]
    pub session: BridgeSessionInfo,
    pub state: Value,
}

struct PromptWork {
    prompt: Value,
    cancelled: watch::Receiver<bool>,
    result: oneshot::Sender<Result<Value, BridgeSessionRuntimeError>>,
}

pub struct BridgeSessionRuntimeEntry {
    pub session_id: String,
    pub workspace_cwd: PathBuf,
    effective_cwd: Mutex<PathBuf>,
    pub created_at: String,
    pub source: SessionSourceMetadata,
    pub parent_session_id: Option<String>,
    restore_state: Option<Value>,
    pub events: Arc<SessionEventBus>,
    pub artifacts: Arc<AsyncMutex<SessionArtifactStore>>,
    pub media: Arc<SessionMediaStore>,
    agent: Arc<dyn BridgeSessionAgent>,
    prompt_tx: mpsc::UnboundedSender<PromptWork>,
    worker_shutdown: watch::Sender<bool>,
    pending_prompts: Arc<AtomicUsize>,
    max_pending_prompts: usize,
    attach_count: AtomicUsize,
    spawn_owner_wanted_kill: AtomicBool,
    client_ids: Mutex<HashSet<String>>,
    attach_refs: Mutex<HashMap<String, usize>>,
    prompt_admission: AsyncMutex<()>,
    close_lock: AsyncMutex<()>,
    closing: AtomicBool,
}

impl BridgeSessionRuntimeEntry {
    fn new(
        created: CreatedBridgeSession,
        workspace_cwd: PathBuf,
        source: SessionSourceMetadata,
        parent_session_id: Option<String>,
        options: &BridgeSessionRuntimeOptions,
    ) -> Arc<Self> {
        let persistence: Option<Arc<dyn SessionArtifactPersistence>> = created
            .transcript_path
            .map(|path| Arc::new(JsonlArtifactPersistence::new(path)) as _);
        let artifacts = SessionArtifactStore::new(
            created.session_id.clone(),
            workspace_cwd.clone(),
            options.max_artifacts_per_session,
            persistence,
        );
        let (prompt_tx, mut prompt_rx) = mpsc::unbounded_channel::<PromptWork>();
        let (worker_shutdown, mut worker_shutdown_rx) = watch::channel(false);
        let pending_prompts = Arc::new(AtomicUsize::new(0));
        let worker_pending = pending_prompts.clone();
        let worker_agent = created.agent.clone();
        tokio::spawn(async move {
            loop {
                let work = tokio::select! {
                    shutdown_changed = worker_shutdown_rx.changed() => {
                        if shutdown_changed.is_err() || *worker_shutdown_rx.borrow() {
                            while let Ok(work) = prompt_rx.try_recv() {
                                let _pending = PendingPromptGuard(worker_pending.clone());
                                let _ = work.result.send(Err(BridgeSessionRuntimeError::SessionClosing(
                                    "session is shutting down".into(),
                                )));
                            }
                            break;
                        }
                        continue;
                    }
                    work = prompt_rx.recv() => match work {
                        Some(work) => work,
                        None => break,
                    },
                };
                let _pending = PendingPromptGuard(worker_pending.clone());
                if *work.cancelled.borrow() {
                    let _ = work
                        .result
                        .send(Err(BridgeSessionRuntimeError::PromptAborted));
                    continue;
                }
                let result = worker_agent
                    .prompt(work.prompt, work.cancelled)
                    .await
                    .map_err(BridgeSessionRuntimeError::Operation);
                let _ = work.result.send(result);
            }
        });
        Arc::new(Self {
            session_id: created.session_id,
            workspace_cwd,
            effective_cwd: Mutex::new(created.effective_cwd),
            created_at: created.created_at,
            source,
            parent_session_id,
            restore_state: created.restore_state,
            events: Arc::new(SessionEventBus::new(options.event_bus.clone())),
            artifacts: Arc::new(AsyncMutex::new(artifacts)),
            media: Arc::new(SessionMediaStore::new()),
            agent: created.agent,
            prompt_tx,
            worker_shutdown,
            pending_prompts,
            max_pending_prompts: options.max_pending_prompts_per_session.unwrap_or(5),
            attach_count: AtomicUsize::new(0),
            spawn_owner_wanted_kill: AtomicBool::new(false),
            client_ids: Mutex::new(HashSet::new()),
            attach_refs: Mutex::new(HashMap::new()),
            prompt_admission: AsyncMutex::new(()),
            close_lock: AsyncMutex::new(()),
            closing: AtomicBool::new(false),
        })
    }

    pub fn pending_prompt_count(&self) -> usize {
        self.pending_prompts.load(Ordering::Acquire)
    }

    pub fn attach_count(&self) -> usize {
        self.attach_count.load(Ordering::Acquire)
    }

    pub fn current_cwd(&self) -> PathBuf {
        lock(&self.effective_cwd).clone()
    }

    fn register_client(&self, requested: Option<&str>, attached: bool) -> String {
        self.register_client_with_counted_attach(requested, attached, attached)
    }

    fn register_reserved_client(&self, requested: Option<&str>) -> String {
        self.register_client_with_counted_attach(requested, true, false)
    }

    fn register_client_with_counted_attach(
        &self,
        requested: Option<&str>,
        attached: bool,
        count_attach: bool,
    ) -> String {
        let mut clients = lock(&self.client_ids);
        let id = requested
            .filter(|candidate| clients.contains(*candidate))
            .map(str::to_owned)
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        clients.insert(id.clone());
        if attached {
            if count_attach {
                self.attach_count.fetch_add(1, Ordering::AcqRel);
            }
            let mut refs = lock(&self.attach_refs);
            *refs.entry(id.clone()).or_default() += 1;
        }
        id
    }

    pub fn detach_client(&self, client_id: &str) -> bool {
        let mut refs = lock(&self.attach_refs);
        let Some(count) = refs.get_mut(client_id) else {
            return false;
        };
        if *count == 0 {
            return false;
        }
        *count -= 1;
        self.attach_count.fetch_sub(1, Ordering::AcqRel);
        true
    }

    pub async fn send_prompt(
        &self,
        prompt: Value,
        cancelled: watch::Receiver<bool>,
    ) -> Result<Value, BridgeSessionRuntimeError> {
        let admission = self.prompt_admission.lock().await;
        if self.closing.load(Ordering::Acquire) {
            return Err(BridgeSessionRuntimeError::SessionClosing(
                self.session_id.clone(),
            ));
        }
        if *cancelled.borrow() {
            return Err(BridgeSessionRuntimeError::PromptAborted);
        }
        let prompt = if let Some(content) = prompt.as_array() {
            self.media
                .assert_references(content)
                .await
                .map_err(|error| BridgeSessionRuntimeError::Operation(error.to_string()))?;
            Value::Array(
                self.media
                    .resolve_content(content)
                    .await
                    .map_err(|error| BridgeSessionRuntimeError::Operation(error.to_string()))?,
            )
        } else {
            prompt
        };
        if reserve_prompt_slot(&self.pending_prompts, self.max_pending_prompts).is_none() {
            return Err(BridgeSessionRuntimeError::PromptQueueFull {
                session_id: self.session_id.clone(),
                limit: self.max_pending_prompts,
                pending: self.pending_prompt_count(),
            });
        }
        let (result_tx, result_rx) = oneshot::channel();
        if self
            .prompt_tx
            .send(PromptWork {
                prompt,
                cancelled,
                result: result_tx,
            })
            .is_err()
        {
            self.pending_prompts.fetch_sub(1, Ordering::AcqRel);
            return Err(BridgeSessionRuntimeError::SessionClosing(
                self.session_id.clone(),
            ));
        }
        drop(admission);
        result_rx
            .await
            .map_err(|_| BridgeSessionRuntimeError::SessionClosing(self.session_id.clone()))?
    }

    pub async fn cancel(&self) -> Result<(), BridgeSessionRuntimeError> {
        self.agent
            .cancel()
            .await
            .map_err(BridgeSessionRuntimeError::Operation)
    }

    async fn close(&self) -> Result<(), BridgeSessionRuntimeError> {
        let _serial = self.close_lock.lock().await;
        if self.closing.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        if let Err(error) = self.agent.shutdown().await {
            self.closing.store(false, Ordering::Release);
            return Err(BridgeSessionRuntimeError::Operation(error));
        }
        let _ = self.events.publish(BridgeEvent::new(
            "session_died",
            serde_json::json!({"sessionId":self.session_id,"reason":"killed"}),
        ));
        self.events.close();
        let _ = self.worker_shutdown.send(true);
        let _ = self.media.close().await;
        Ok(())
    }
}

struct PendingPromptGuard(Arc<AtomicUsize>);
impl Drop for PendingPromptGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

fn reserve_prompt_slot(pending: &Arc<AtomicUsize>, limit: usize) -> Option<()> {
    loop {
        let current = pending.load(Ordering::Acquire);
        if limit > 0 && current >= limit {
            return None;
        }
        if pending
            .compare_exchange_weak(
                current,
                current.saturating_add(1),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            return Some(());
        }
    }
}

struct InFlightSpawn {
    receiver: watch::Receiver<InFlightSpawnResult>,
}

type InFlightSpawnResult =
    Option<Result<Arc<BridgeSessionRuntimeEntry>, Arc<BridgeSessionRuntimeError>>>;
type InFlightSpawns = HashMap<String, watch::Sender<InFlightSpawnResult>>;
type RestoreCompletion = Option<Result<Arc<BridgeSessionRuntimeEntry>, BridgeSessionRuntimeError>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RestoreRequestShape {
    action: BridgeRestoreAction,
    history_replay: &'static str,
    history_page_size: Option<usize>,
    live_replay_mode: &'static str,
    hide_inherited_history: bool,
}

impl RestoreRequestShape {
    fn can_join(&self, requested: &Self) -> bool {
        self.action == requested.action
            && self.history_replay == requested.history_replay
            && self.history_page_size == requested.history_page_size
            && self.live_replay_mode == requested.live_replay_mode
            && self.hide_inherited_history == requested.hide_inherited_history
    }
}

#[derive(Clone)]
struct InFlightRestore {
    shape: RestoreRequestShape,
    completion: watch::Sender<RestoreCompletion>,
    reservations: Arc<RestoreAttachReservations>,
}

enum RestoreAdmission {
    Owner {
        completion: watch::Sender<RestoreCompletion>,
        receiver: watch::Receiver<RestoreCompletion>,
    },
    Waiter {
        receiver: watch::Receiver<RestoreCompletion>,
        reservation: RestoreAttachReservation,
    },
}

#[derive(Default)]
struct RestoreAttachReservationState {
    count: usize,
    entry: Option<Weak<BridgeSessionRuntimeEntry>>,
}

#[derive(Default)]
struct RestoreAttachReservations {
    state: Mutex<RestoreAttachReservationState>,
}

impl RestoreAttachReservations {
    fn reserve(self: &Arc<Self>) -> RestoreAttachReservation {
        lock(&self.state).count += 1;
        RestoreAttachReservation {
            reservations: self.clone(),
            committed: false,
        }
    }

    fn register_entry(&self, entry: &Arc<BridgeSessionRuntimeEntry>) {
        let mut state = lock(&self.state);
        entry.attach_count.store(state.count, Ordering::Release);
        state.entry = Some(Arc::downgrade(entry));
    }

    fn release_one(&self) {
        let mut state = lock(&self.state);
        if let Some(entry) = state.entry.as_ref().and_then(Weak::upgrade) {
            let _ = entry
                .attach_count
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                    Some(count.saturating_sub(1))
                });
        } else {
            state.count = state.count.saturating_sub(1);
        }
    }
}

struct RestoreAttachReservation {
    reservations: Arc<RestoreAttachReservations>,
    committed: bool,
}

impl RestoreAttachReservation {
    fn commit(&mut self) {
        self.committed = true;
    }
}

impl Drop for RestoreAttachReservation {
    fn drop(&mut self) {
        if !self.committed {
            self.reservations.release_one();
        }
    }
}

struct RestoreOwnerGuard {
    state: Arc<Mutex<RuntimeState>>,
    session_id: String,
    completion: watch::Sender<RestoreCompletion>,
    finished: bool,
}

impl RestoreOwnerGuard {
    fn finish(
        &mut self,
        result: Result<Arc<BridgeSessionRuntimeEntry>, BridgeSessionRuntimeError>,
    ) {
        finish_restore_registration(&self.state, &self.session_id, &self.completion, result);
        self.finished = true;
    }
}

impl Drop for RestoreOwnerGuard {
    fn drop(&mut self) {
        if !self.finished {
            self.finish(Err(BridgeSessionRuntimeError::Operation(
                "session restore operation was cancelled".into(),
            )));
        }
    }
}

struct RuntimeState {
    sessions: HashMap<String, Arc<BridgeSessionRuntimeEntry>>,
    default_session_id: Option<String>,
    in_flight: InFlightSpawns,
    restoring: HashMap<String, InFlightRestore>,
    restore_notify: Arc<Notify>,
    shutting_down: bool,
}

#[derive(Clone)]
pub struct BridgeSessionRuntime {
    bound_workspace: PathBuf,
    options: BridgeSessionRuntimeOptions,
    factory: Arc<dyn BridgeSessionFactory>,
    state: Arc<Mutex<RuntimeState>>,
}

impl BridgeSessionRuntime {
    pub fn new(
        mut options: BridgeSessionRuntimeOptions,
        factory: Arc<dyn BridgeSessionFactory>,
    ) -> Result<Self, BridgeSessionRuntimeError> {
        if options.bound_workspace.as_os_str().is_empty() {
            return Err(BridgeSessionRuntimeError::MissingWorkspace);
        }
        let bound = canonicalize_workspace(&options.bound_workspace.to_string_lossy())
            .map_err(|error| BridgeSessionRuntimeError::Operation(error.to_string()))?;
        options.bound_workspace = bound.clone();
        if options.event_bus.journal_growth_pool_bytes.is_some()
            && options.event_bus.max_journal_events.is_none()
            && options.event_bus.max_journal_bytes.is_none()
            && options.event_bus.journal_growth_registry.is_none()
        {
            // One registry per runtime makes the derived pool aggregate over
            // every session in this workspace bridge. Callers that own several
            // workspace runtimes pass the same registry through the builder.
            options.event_bus.journal_growth_registry =
                Some(Arc::new(JournalGrowthRegistry::new()));
        }
        TurnBoundaryCompactionEngine::new(SessionCompactionOptions {
            max_replay_bytes: options.event_bus.compacted_replay_max_bytes,
            max_journal_events: options
                .event_bus
                .max_journal_events
                .unwrap_or(super::replay_window_limits::DEFAULT_MAX_JOURNAL_EVENTS),
            max_journal_bytes: options
                .event_bus
                .max_journal_bytes
                .unwrap_or(super::replay_window_limits::DEFAULT_MAX_JOURNAL_BYTES),
            journal_growth_pool_bytes: if options.event_bus.max_journal_events.is_some()
                || options.event_bus.max_journal_bytes.is_some()
            {
                None
            } else {
                options.event_bus.journal_growth_pool_bytes
            },
            growth_registry: options.event_bus.journal_growth_registry.clone(),
        })
        .map_err(|error| BridgeSessionRuntimeError::InvalidJournalLimits(error.to_string()))?;
        Ok(Self {
            bound_workspace: bound,
            options,
            factory,
            state: Arc::new(Mutex::new(RuntimeState {
                sessions: HashMap::new(),
                default_session_id: None,
                in_flight: HashMap::new(),
                restoring: HashMap::new(),
                restore_notify: Arc::new(Notify::new()),
                shutting_down: false,
            })),
        })
    }

    pub fn bound_workspace(&self) -> &PathBuf {
        &self.bound_workspace
    }

    pub fn session(&self, session_id: &str) -> Option<Arc<BridgeSessionRuntimeEntry>> {
        lock(&self.state).sessions.get(session_id).cloned()
    }

    pub fn sessions(&self) -> Vec<Arc<BridgeSessionRuntimeEntry>> {
        lock(&self.state).sessions.values().cloned().collect()
    }

    pub async fn send_prompt(
        &self,
        session_id: &str,
        prompt: Value,
        cancelled: watch::Receiver<bool>,
    ) -> Result<Value, BridgeSessionRuntimeError> {
        let entry = self
            .session(session_id)
            .ok_or_else(|| BridgeSessionRuntimeError::SessionNotFound(session_id.into()))?;
        entry.send_prompt(prompt, cancelled).await
    }

    pub async fn cancel(&self, session_id: &str) -> Result<(), BridgeSessionRuntimeError> {
        let entry = self
            .session(session_id)
            .ok_or_else(|| BridgeSessionRuntimeError::SessionNotFound(session_id.into()))?;
        entry.cancel().await
    }

    pub fn subscribe_events(
        &self,
        session_id: &str,
        options: SubscribeOptions,
    ) -> Result<EventSubscription, SessionEventAccessError> {
        let entry = self
            .session(session_id)
            .ok_or_else(|| SessionEventAccessError::SessionNotFound(session_id.into()))?;
        entry
            .events
            .subscribe(options)
            .map_err(SessionEventAccessError::SubscriberLimit)
    }

    pub fn publish_event(&self, session_id: &str, event: BridgeEvent) -> bool {
        self.session(session_id)
            .is_some_and(|entry| entry.events.publish(event).is_some())
    }

    pub fn detach_client(&self, session_id: &str, client_id: &str) -> bool {
        self.session(session_id)
            .is_some_and(|entry| entry.detach_client(client_id))
    }

    pub async fn detach_client_and_reap(&self, session_id: &str, client_id: &str) -> bool {
        let Some(entry) = self.session(session_id) else {
            return false;
        };
        if !entry.detach_client(client_id) {
            return false;
        }
        if entry.attach_count() == 0 && entry.spawn_owner_wanted_kill.swap(false, Ordering::AcqRel)
        {
            let _ = self.kill_session(session_id, false).await;
        }
        true
    }

    pub async fn spawn_or_attach(
        &self,
        request: BridgeSpawnRequest,
    ) -> Result<BridgeSessionInfo, BridgeSessionRuntimeError> {
        let requested_workspace = canonicalize_workspace(&request.workspace_cwd)
            .map_err(|error| BridgeSessionRuntimeError::Operation(error.to_string()))?;
        if requested_workspace != self.bound_workspace {
            return Err(BridgeSessionRuntimeError::WorkspaceMismatch {
                bound: self.bound_workspace.to_string_lossy().into_owned(),
                requested: requested_workspace.to_string_lossy().into_owned(),
            });
        }
        let source =
            parse_session_source(request.source_type.as_deref(), request.source_id.as_deref())
                .map_err(BridgeSessionRuntimeError::InvalidMetadata)?;
        if let Some(mode) = request.approval_mode.as_deref()
            && !matches!(mode, "plan" | "default" | "auto-edit" | "auto" | "yolo")
        {
            return Err(BridgeSessionRuntimeError::InvalidApprovalMode(
                serde_json::to_string(mode).unwrap_or_else(|_| mode.to_owned()),
            ));
        }
        let request_scope = match request.session_scope.as_deref() {
            None => None,
            Some("single") => Some(BridgeSessionScope::Single),
            Some("thread") => Some(BridgeSessionScope::Thread),
            Some(value) => return Err(BridgeSessionRuntimeError::InvalidScope(value.into())),
        };
        let scope = if request.session_id.is_some() {
            BridgeSessionScope::Thread
        } else {
            request_scope.unwrap_or(self.options.session_scope)
        };
        let single_key = self.bound_workspace.to_string_lossy().into_owned();
        let key = if scope == BridgeSessionScope::Single {
            single_key.clone()
        } else {
            format!("{}#{}", single_key, Uuid::new_v4())
        };

        let (in_flight, attached) = {
            let mut state = lock(&self.state);
            if state.shutting_down {
                return Err(BridgeSessionRuntimeError::ShuttingDown);
            }
            if scope == BridgeSessionScope::Single
                && let Some(session_id) = &state.default_session_id
                && let Some(entry) = state.sessions.get(session_id)
            {
                if entry.closing.load(Ordering::Acquire) {
                    return Err(BridgeSessionRuntimeError::SessionClosing(
                        entry.session_id.clone(),
                    ));
                }
                return Ok(make_session_info(entry, request.client_id.as_deref(), true));
            }
            if let Some(sender) = state.in_flight.get(&key) {
                (
                    InFlightSpawn {
                        receiver: sender.subscribe(),
                    },
                    true,
                )
            } else {
                if let Some(limit) = self.options.max_sessions
                    && limit > 0
                    && state.sessions.len() + state.in_flight.len() + state.restoring.len() >= limit
                {
                    return Err(BridgeSessionRuntimeError::SessionLimitExceeded(limit));
                }
                let (sender, receiver) = watch::channel(None);
                state.in_flight.insert(key.clone(), sender.clone());
                let factory = self.factory.clone();
                let request_for_spawn = request.clone();
                let workspace_for_spawn = requested_workspace.clone();
                let state_for_spawn = self.state.clone();
                let options = self.options.clone();
                let source_for_spawn = source.clone();
                let key_for_spawn = key.clone();
                let completion_sender = sender.clone();
                let single_scope = scope == BridgeSessionScope::Single;
                tokio::spawn(async move {
                    let created = factory
                        .spawn(
                            request_for_spawn.clone(),
                            workspace_for_spawn.clone(),
                            scope,
                        )
                        .await;
                    let result = match created {
                        Err(error) => Err(Arc::new(BridgeSessionRuntimeError::Operation(error))),
                        Ok(created) => {
                            if request_for_spawn
                                .session_id
                                .as_deref()
                                .is_some_and(|requested| requested != created.session_id)
                            {
                                let _ = created.agent.shutdown().await;
                                Err(Arc::new(BridgeSessionRuntimeError::Operation(
                                    "session factory did not honor the requested session id".into(),
                                )))
                            } else {
                                let entry = BridgeSessionRuntimeEntry::new(
                                    created,
                                    workspace_for_spawn,
                                    source_for_spawn,
                                    request_for_spawn.parent_session_id.clone(),
                                    &options,
                                );
                                let insert_result = {
                                    let mut state = lock(&state_for_spawn);
                                    if state.shutting_down {
                                        Err(Arc::new(BridgeSessionRuntimeError::ShuttingDown))
                                    } else if state.sessions.contains_key(&entry.session_id) {
                                        Err(Arc::new(
                                            BridgeSessionRuntimeError::SessionIdCollision(
                                                entry.session_id.clone(),
                                            ),
                                        ))
                                    } else {
                                        if single_scope {
                                            state.default_session_id =
                                                Some(entry.session_id.clone());
                                        }
                                        state
                                            .sessions
                                            .insert(entry.session_id.clone(), entry.clone());
                                        Ok(())
                                    }
                                };
                                match insert_result {
                                    Ok(()) => Ok(entry),
                                    Err(error) => {
                                        let _ = entry.close().await;
                                        Err(error)
                                    }
                                }
                            }
                        }
                    };
                    let mut state = lock(&state_for_spawn);
                    state.in_flight.remove(&key_for_spawn);
                    drop(state);
                    let _ = completion_sender.send(Some(result));
                });
                (InFlightSpawn { receiver }, false)
            }
        };
        let result = wait_inflight(in_flight).await?;
        Ok(make_session_info(
            &result,
            request.client_id.as_deref(),
            attached,
        ))
    }

    /// Restore an existing ACP session by id. Concurrent cold restores with
    /// matching request shapes share one factory operation.
    pub async fn restore_session(
        &self,
        action: BridgeRestoreAction,
        request: BridgeRestoreRequest,
    ) -> Result<BridgeRestoredSessionInfo, BridgeSessionRuntimeError> {
        let requested_workspace = canonicalize_workspace(&request.workspace_cwd)
            .map_err(|error| BridgeSessionRuntimeError::Operation(error.to_string()))?;
        if requested_workspace != self.bound_workspace {
            return Err(BridgeSessionRuntimeError::WorkspaceMismatch {
                bound: self.bound_workspace.to_string_lossy().into_owned(),
                requested: requested_workspace.to_string_lossy().into_owned(),
            });
        }
        if let Some(mode) = request.approval_mode.as_deref()
            && !matches!(mode, "plan" | "default" | "auto-edit" | "auto" | "yolo")
        {
            return Err(BridgeSessionRuntimeError::InvalidApprovalMode(
                serde_json::to_string(mode).unwrap_or_else(|_| mode.to_owned()),
            ));
        }
        let requested_replay_mode = request.live_replay_mode.as_deref().unwrap_or("full");
        if !matches!(requested_replay_mode, "full" | "summary") {
            return Err(BridgeSessionRuntimeError::InvalidLiveReplayMode(
                serde_json::to_string(requested_replay_mode)
                    .unwrap_or_else(|_| requested_replay_mode.to_owned()),
            ));
        }
        let requested_history_replay = if action == BridgeRestoreAction::Load {
            request.history_replay.as_deref().unwrap_or("stream")
        } else {
            "stream"
        };
        if !matches!(requested_history_replay, "stream" | "response") {
            return Err(BridgeSessionRuntimeError::InvalidHistoryReplay(
                serde_json::to_string(requested_history_replay)
                    .unwrap_or_else(|_| requested_history_replay.to_owned()),
            ));
        }
        let history_replay = match requested_history_replay {
            "response" => "response",
            _ => "stream",
        };
        let replay_mode = match requested_replay_mode {
            "summary" => "summary",
            _ => "full",
        };
        let history_page_size = (action == BridgeRestoreAction::Load
            && history_replay == "response")
            .then_some(request.history_page_size)
            .flatten();
        if history_page_size.is_some_and(|size| !(1..=500).contains(&size)) {
            return Err(BridgeSessionRuntimeError::InvalidHistoryPageSize);
        }
        let source =
            parse_session_source(request.source_type.as_deref(), request.source_id.as_deref())
                .map_err(BridgeSessionRuntimeError::InvalidMetadata)?;
        let shape = RestoreRequestShape {
            action,
            history_replay,
            history_page_size,
            live_replay_mode: if action == BridgeRestoreAction::Load {
                replay_mode
            } else {
                "full"
            },
            hide_inherited_history: action == BridgeRestoreAction::Load
                && request.hide_inherited_history,
        };

        let admission = {
            let mut state = lock(&self.state);
            if state.shutting_down {
                return Err(BridgeSessionRuntimeError::ShuttingDown);
            }
            if let Some(entry) = state.sessions.get(&request.session_id) {
                if entry.closing.load(Ordering::Acquire) {
                    return Err(BridgeSessionRuntimeError::SessionClosing(
                        request.session_id.clone(),
                    ));
                }
                let info = make_session_info(entry, request.client_id.as_deref(), true);
                return Ok(BridgeRestoredSessionInfo {
                    session: info,
                    state: entry
                        .restore_state
                        .clone()
                        .unwrap_or_else(|| serde_json::json!({})),
                });
            }
            if let Some(in_flight) = state.restoring.get(&request.session_id) {
                if !in_flight.shape.can_join(&shape) {
                    return Err(BridgeSessionRuntimeError::RestoreInProgress(
                        request.session_id,
                    ));
                }
                RestoreAdmission::Waiter {
                    receiver: in_flight.completion.subscribe(),
                    reservation: in_flight.reservations.reserve(),
                }
            } else {
                if let Some(limit) = self.options.max_sessions
                    && limit > 0
                    && state.sessions.len() + state.in_flight.len() + state.restoring.len() >= limit
                {
                    return Err(BridgeSessionRuntimeError::SessionLimitExceeded(limit));
                }
                let (completion, receiver) = watch::channel(None);
                let reservations = Arc::new(RestoreAttachReservations::default());
                state.restoring.insert(
                    request.session_id.clone(),
                    InFlightRestore {
                        shape,
                        completion: completion.clone(),
                        reservations,
                    },
                );
                RestoreAdmission::Owner {
                    completion,
                    receiver,
                }
            }
        };

        let (receiver, reservation) = match admission {
            RestoreAdmission::Owner {
                completion,
                receiver,
            } => {
                let runtime = self.clone();
                let worker_completion = completion.clone();
                let worker_session_id = request.session_id.clone();
                let worker_request = request.clone();
                tokio::spawn(async move {
                    let mut owner = RestoreOwnerGuard {
                        state: runtime.state.clone(),
                        session_id: worker_session_id,
                        completion: worker_completion,
                        finished: false,
                    };
                    let result = runtime
                        .perform_cold_restore(action, worker_request, requested_workspace, source)
                        .await;
                    owner.finish(result);
                });
                (receiver, None)
            }
            RestoreAdmission::Waiter {
                receiver,
                reservation,
            } => (receiver, Some(reservation)),
        };

        let entry = wait_for_restore(receiver).await?;
        let info = if let Some(mut reservation) = reservation {
            let info = {
                let state = lock(&self.state);
                if state.shutting_down {
                    return Err(BridgeSessionRuntimeError::ShuttingDown);
                }
                let Some(current) = state.sessions.get(&request.session_id) else {
                    return Err(BridgeSessionRuntimeError::SessionNotFound(
                        request.session_id,
                    ));
                };
                if !Arc::ptr_eq(current, &entry) {
                    return Err(BridgeSessionRuntimeError::SessionNotFound(
                        request.session_id,
                    ));
                }
                if entry.closing.load(Ordering::Acquire) {
                    return Err(BridgeSessionRuntimeError::SessionClosing(
                        request.session_id,
                    ));
                }
                make_reserved_attach_session_info(&entry, request.client_id.as_deref())
            };
            reservation.commit();
            info
        } else {
            make_session_info(&entry, request.client_id.as_deref(), false)
        };
        Ok(BridgeRestoredSessionInfo {
            session: info,
            state: entry
                .restore_state
                .clone()
                .unwrap_or_else(|| serde_json::json!({})),
        })
    }

    async fn perform_cold_restore(
        &self,
        action: BridgeRestoreAction,
        request: BridgeRestoreRequest,
        requested_workspace: PathBuf,
        source: SessionSourceMetadata,
    ) -> Result<Arc<BridgeSessionRuntimeEntry>, BridgeSessionRuntimeError> {
        let created = self
            .factory
            .restore(request.clone(), action, requested_workspace.clone())
            .await;
        let created = match created {
            Ok(created) => created,
            Err(BridgeSessionFactoryError::Timeout(timeout_ms)) => {
                return Err(BridgeSessionRuntimeError::RestoreTimeout {
                    session_id: request.session_id,
                    action,
                    timeout_ms,
                });
            }
            Err(BridgeSessionFactoryError::Operation(error)) => {
                return Err(BridgeSessionRuntimeError::Operation(error));
            }
        };
        if created.session_id != request.session_id {
            let _ = created.agent.shutdown().await;
            return Err(BridgeSessionRuntimeError::Operation(
                "session factory did not restore the requested session id".into(),
            ));
        }
        let is_shutting_down = { lock(&self.state).shutting_down };
        if is_shutting_down {
            let _ = created.agent.shutdown().await;
            return Err(BridgeSessionRuntimeError::ShuttingDown);
        }
        let entry = BridgeSessionRuntimeEntry::new(
            created,
            requested_workspace,
            source,
            request.parent_session_id,
            &self.options,
        );
        let insert_result = {
            let mut state = lock(&self.state);
            if state.shutting_down {
                Err(BridgeSessionRuntimeError::ShuttingDown)
            } else if state.sessions.contains_key(&entry.session_id) {
                Err(BridgeSessionRuntimeError::SessionIdCollision(
                    entry.session_id.clone(),
                ))
            } else if let Some(in_flight) = state.restoring.get(&entry.session_id).cloned() {
                in_flight.reservations.register_entry(&entry);
                state
                    .sessions
                    .insert(entry.session_id.clone(), entry.clone());
                Ok(())
            } else {
                Err(BridgeSessionRuntimeError::ShuttingDown)
            }
        };
        if let Err(error) = insert_result {
            let _ = entry.close().await;
            return Err(error);
        }
        Ok(entry)
    }

    pub async fn kill_session(
        &self,
        session_id: &str,
        require_zero_attaches: bool,
    ) -> Result<bool, BridgeSessionRuntimeError> {
        let entry = {
            let state = lock(&self.state);
            let Some(entry) = state.sessions.get(session_id).cloned() else {
                return Ok(false);
            };
            if require_zero_attaches && entry.attach_count() > 0 {
                entry.spawn_owner_wanted_kill.store(true, Ordering::Release);
                return Ok(false);
            }
            entry
        };
        entry.close().await?;
        let mut state = lock(&self.state);
        if state
            .sessions
            .get(session_id)
            .is_some_and(|current| Arc::ptr_eq(current, &entry))
        {
            state.sessions.remove(session_id);
            if state.default_session_id.as_deref() == Some(session_id) {
                state.default_session_id = None;
            }
        }
        Ok(true)
    }

    pub async fn shutdown(&self) -> Result<(), BridgeSessionRuntimeError> {
        let (pending, restore_notify) = {
            let mut state = lock(&self.state);
            state.shutting_down = true;
            (
                state
                    .in_flight
                    .values()
                    .map(watch::Sender::subscribe)
                    .collect::<Vec<_>>(),
                state.restore_notify.clone(),
            )
        };
        for receiver in pending {
            let _ = wait_inflight(InFlightSpawn { receiver }).await;
        }
        loop {
            let notified = restore_notify.notified();
            if lock(&self.state).restoring.is_empty() {
                break;
            }
            notified.await;
        }
        let entries = {
            let mut state = lock(&self.state);
            state.default_session_id = None;
            std::mem::take(&mut state.sessions)
                .into_values()
                .collect::<Vec<_>>()
        };
        let mut first_error = None;
        for entry in entries {
            if let Err(error) = entry.close().await
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        if let Err(error) = self.factory.shutdown().await
            && first_error.is_none()
        {
            first_error = Some(BridgeSessionRuntimeError::Operation(error));
        }
        first_error.map_or(Ok(()), Err)
    }

    pub fn kill_all_sync(&self) {
        let mut state = lock(&self.state);
        state.shutting_down = true;
        state.restoring.clear();
        state.restore_notify.notify_one();
        let entries = std::mem::take(&mut state.sessions);
        state.default_session_id = None;
        drop(state);
        for entry in entries.into_values() {
            entry.closing.store(true, Ordering::Release);
            entry.events.close();
            let _ = entry.worker_shutdown.send(true);
        }
        self.factory.kill_all_sync();
    }
}

#[derive(Debug, Error)]
pub enum SessionEventAccessError {
    #[error("No session with id \"{0}\"")]
    SessionNotFound(String),
    #[error(transparent)]
    SubscriberLimit(#[from] SubscriberLimitExceededError),
}

async fn wait_inflight(
    mut inflight: InFlightSpawn,
) -> Result<Arc<BridgeSessionRuntimeEntry>, BridgeSessionRuntimeError> {
    loop {
        if let Some(result) = inflight.receiver.borrow().clone() {
            return result.map_err(|error| (*error).clone());
        }
        if inflight.receiver.changed().await.is_err() {
            return Err(BridgeSessionRuntimeError::Operation(
                "session spawn result channel closed".into(),
            ));
        }
    }
}

async fn wait_for_restore(
    mut receiver: watch::Receiver<RestoreCompletion>,
) -> Result<Arc<BridgeSessionRuntimeEntry>, BridgeSessionRuntimeError> {
    loop {
        let result = { receiver.borrow_and_update().clone() };
        if let Some(result) = result {
            return result;
        }
        if receiver.changed().await.is_err() {
            return Err(BridgeSessionRuntimeError::Operation(
                "session restore result channel closed".into(),
            ));
        }
    }
}

fn finish_restore_registration(
    state: &Arc<Mutex<RuntimeState>>,
    session_id: &str,
    completion: &watch::Sender<RestoreCompletion>,
    result: Result<Arc<BridgeSessionRuntimeEntry>, BridgeSessionRuntimeError>,
) {
    let mut state = lock(state);
    state.restoring.remove(session_id);
    state.restore_notify.notify_one();
    drop(state);
    completion.send_replace(Some(result));
}

fn make_session_info(
    entry: &BridgeSessionRuntimeEntry,
    requested_client_id: Option<&str>,
    attached: bool,
) -> BridgeSessionInfo {
    make_session_info_inner(entry, requested_client_id, attached, false)
}

fn make_reserved_attach_session_info(
    entry: &BridgeSessionRuntimeEntry,
    requested_client_id: Option<&str>,
) -> BridgeSessionInfo {
    make_session_info_inner(entry, requested_client_id, true, true)
}

fn make_session_info_inner(
    entry: &BridgeSessionRuntimeEntry,
    requested_client_id: Option<&str>,
    attached: bool,
    attach_is_reserved: bool,
) -> BridgeSessionInfo {
    let current_cwd = entry.current_cwd();
    BridgeSessionInfo {
        session_id: entry.session_id.clone(),
        workspace_cwd: entry.workspace_cwd.to_string_lossy().into_owned(),
        current_cwd: (current_cwd != entry.workspace_cwd)
            .then(|| current_cwd.to_string_lossy().into_owned()),
        attached,
        client_id: if attach_is_reserved {
            entry.register_reserved_client(requested_client_id)
        } else {
            entry.register_client(requested_client_id, attached)
        },
        created_at: entry.created_at.clone(),
        source_type: entry.source.source_type.clone(),
        source_id: entry.source.source_id.clone(),
        parent_session_id: entry.parent_session_id.clone(),
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU64;
    use std::time::Duration;

    struct MockAgent {
        active: Arc<AtomicUsize>,
        maximum_active: Arc<AtomicUsize>,
        delay_ms: u64,
    }

    impl BridgeSessionAgent for MockAgent {
        fn prompt<'a>(
            &'a self,
            prompt: Value,
            _cancelled: watch::Receiver<bool>,
        ) -> SessionRuntimeFuture<'a, Result<Value, String>> {
            Box::pin(async move {
                let active = self.active.fetch_add(1, Ordering::AcqRel) + 1;
                self.maximum_active.fetch_max(active, Ordering::AcqRel);
                tokio::time::sleep(Duration::from_millis(self.delay_ms)).await;
                self.active.fetch_sub(1, Ordering::AcqRel);
                Ok(prompt)
            })
        }

        fn cancel<'a>(&'a self) -> SessionRuntimeFuture<'a, Result<(), String>> {
            Box::pin(async { Ok(()) })
        }

        fn shutdown<'a>(&'a self) -> SessionRuntimeFuture<'a, Result<(), String>> {
            Box::pin(async { Ok(()) })
        }
    }

    struct MockFactory {
        spawns: Arc<AtomicUsize>,
        next_id: AtomicU64,
        delay_ms: u64,
        active: Arc<AtomicUsize>,
        maximum_active: Arc<AtomicUsize>,
    }

    impl BridgeSessionFactory for MockFactory {
        fn spawn<'a>(
            &'a self,
            request: BridgeSpawnRequest,
            workspace_cwd: PathBuf,
            _effective_scope: BridgeSessionScope,
        ) -> SessionRuntimeFuture<'a, Result<CreatedBridgeSession, String>> {
            Box::pin(async move {
                self.spawns.fetch_add(1, Ordering::AcqRel);
                tokio::time::sleep(Duration::from_millis(self.delay_ms)).await;
                let session_id = request.session_id.unwrap_or_else(|| {
                    format!("session-{}", self.next_id.fetch_add(1, Ordering::AcqRel))
                });
                Ok(CreatedBridgeSession {
                    session_id,
                    effective_cwd: workspace_cwd,
                    created_at: "2026-09-24T00:00:00.000Z".into(),
                    transcript_path: None,
                    restore_state: None,
                    agent: Arc::new(MockAgent {
                        active: self.active.clone(),
                        maximum_active: self.maximum_active.clone(),
                        delay_ms: 10,
                    }),
                })
            })
        }

        fn restore<'a>(
            &'a self,
            request: BridgeRestoreRequest,
            _action: BridgeRestoreAction,
            workspace_cwd: PathBuf,
        ) -> SessionRuntimeFuture<'a, Result<CreatedBridgeSession, BridgeSessionFactoryError>>
        {
            Box::pin(async move {
                self.spawns.fetch_add(1, Ordering::AcqRel);
                tokio::time::sleep(Duration::from_millis(self.delay_ms)).await;
                Ok(CreatedBridgeSession {
                    session_id: request.session_id,
                    effective_cwd: workspace_cwd,
                    created_at: "2026-09-24T00:00:00.000Z".into(),
                    transcript_path: None,
                    restore_state: Some(serde_json::json!({"mode":"plan"})),
                    agent: Arc::new(MockAgent {
                        active: self.active.clone(),
                        maximum_active: self.maximum_active.clone(),
                        delay_ms: 0,
                    }),
                })
            })
        }
    }

    fn runtime(
        scope: BridgeSessionScope,
        max_sessions: Option<usize>,
        max_pending_prompts: Option<usize>,
        spawn_delay_ms: u64,
    ) -> (BridgeSessionRuntime, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let spawns = Arc::new(AtomicUsize::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let maximum_active = Arc::new(AtomicUsize::new(0));
        let factory = Arc::new(MockFactory {
            spawns: spawns.clone(),
            next_id: AtomicU64::new(1),
            delay_ms: spawn_delay_ms,
            active: active.clone(),
            maximum_active: maximum_active.clone(),
        });
        let options = BridgeSessionRuntimeOptions {
            bound_workspace: std::env::temp_dir(),
            session_scope: scope,
            max_sessions,
            max_pending_prompts_per_session: max_pending_prompts,
            ..BridgeSessionRuntimeOptions::default()
        };
        (
            BridgeSessionRuntime::new(options, factory).unwrap(),
            spawns,
            maximum_active,
        )
    }

    fn request(workspace: &std::path::Path) -> BridgeSpawnRequest {
        BridgeSpawnRequest {
            workspace_cwd: workspace.to_string_lossy().into_owned(),
            ..BridgeSpawnRequest::default()
        }
    }

    fn restore_request(workspace: &std::path::Path, session_id: &str) -> BridgeRestoreRequest {
        BridgeRestoreRequest {
            session_id: session_id.into(),
            workspace_cwd: workspace.to_string_lossy().into_owned(),
            ..BridgeRestoreRequest::default()
        }
    }

    #[tokio::test]
    async fn single_scope_coalesces_concurrent_spawns_and_counts_attach() {
        let (runtime, spawns, _) = runtime(BridgeSessionScope::Single, Some(4), Some(5), 20);
        let req = request(runtime.bound_workspace());
        let (first, second) = tokio::join!(
            runtime.spawn_or_attach(req.clone()),
            runtime.spawn_or_attach(req)
        );
        let first = first.unwrap();
        let second = second.unwrap();
        assert_eq!(first.session_id, second.session_id);
        assert_ne!(first.attached, second.attached);
        assert_eq!(spawns.load(Ordering::Acquire), 1);
        assert_eq!(
            runtime.session(&first.session_id).unwrap().attach_count(),
            1
        );
    }

    #[tokio::test]
    async fn thread_scope_creates_distinct_sessions_and_obeys_capacity() {
        let (runtime, spawns, _) = runtime(BridgeSessionScope::Thread, Some(1), Some(5), 0);
        let req = request(runtime.bound_workspace());
        let first = runtime.spawn_or_attach(req.clone()).await.unwrap();
        assert!(matches!(
            runtime.spawn_or_attach(req).await,
            Err(BridgeSessionRuntimeError::SessionLimitExceeded(1))
        ));
        assert_eq!(spawns.load(Ordering::Acquire), 1);
        assert_eq!(runtime.sessions().len(), 1);
        assert!(
            runtime
                .kill_session(&first.session_id, false)
                .await
                .unwrap()
        );
        assert_eq!(runtime.sessions().len(), 0);
    }

    #[tokio::test]
    async fn prompt_queue_serializes_calls_and_rejects_pre_aborted_work() {
        let (runtime, _, maximum_active) = runtime(BridgeSessionScope::Single, Some(4), Some(1), 0);
        let info = runtime
            .spawn_or_attach(request(runtime.bound_workspace()))
            .await
            .unwrap();
        let entry = runtime.session(&info.session_id).unwrap();
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let first_entry = entry.clone();
        let first = tokio::spawn(async move {
            first_entry
                .send_prompt(serde_json::json!("first"), cancel_rx)
                .await
        });
        while entry.pending_prompt_count() == 0 {
            tokio::task::yield_now().await;
        }
        let (_unused_tx, second_cancel) = watch::channel(false);
        assert!(matches!(
            entry
                .send_prompt(serde_json::json!("second"), second_cancel)
                .await,
            Err(BridgeSessionRuntimeError::PromptQueueFull { limit: 1, .. })
        ));
        assert_eq!(first.await.unwrap().unwrap(), serde_json::json!("first"));
        assert_eq!(maximum_active.load(Ordering::Acquire), 1);
        let (_cancel_tx, pre_aborted) = watch::channel(true);
        assert_eq!(
            entry
                .send_prompt(serde_json::json!("cancelled"), pre_aborted)
                .await,
            Err(BridgeSessionRuntimeError::PromptAborted)
        );
        drop(cancel_tx);
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn cold_restore_is_bounded_by_id_and_later_requests_attach() {
        let (runtime, spawns, _) = runtime(BridgeSessionScope::Single, Some(2), Some(5), 20);
        let workspace = runtime.bound_workspace().clone();
        let requested = restore_request(&workspace, "existing-session");
        let first_runtime = runtime.clone();
        let first = tokio::spawn(async move {
            first_runtime
                .restore_session(BridgeRestoreAction::Load, requested)
                .await
        });
        while !lock(&runtime.state)
            .restoring
            .contains_key("existing-session")
        {
            tokio::task::yield_now().await;
        }
        assert!(matches!(
            runtime
                .restore_session(
                    BridgeRestoreAction::Resume,
                    restore_request(&workspace, "existing-session"),
                )
                .await,
            Err(BridgeSessionRuntimeError::RestoreInProgress(_))
        ));
        let restored = first.await.unwrap().unwrap();
        assert!(!restored.session.attached);
        assert_eq!(restored.session.session_id, "existing-session");
        assert_eq!(restored.state, serde_json::json!({"mode":"plan"}));

        let attached = runtime
            .restore_session(
                BridgeRestoreAction::Resume,
                restore_request(&workspace, "existing-session"),
            )
            .await
            .unwrap();
        assert!(attached.session.attached);
        assert_eq!(attached.state, restored.state);
        assert_eq!(spawns.load(Ordering::Acquire), 1);
        assert_eq!(
            runtime.session("existing-session").unwrap().attach_count(),
            1
        );
    }

    #[tokio::test]
    async fn restore_validates_response_page_and_replay_modes() {
        let (runtime, _, _) = runtime(BridgeSessionScope::Single, Some(2), Some(5), 0);
        let workspace = runtime.bound_workspace().clone();
        let mut request = restore_request(&workspace, "s1");
        request.history_replay = Some("response".into());
        request.history_page_size = Some(501);
        assert_eq!(
            runtime
                .restore_session(BridgeRestoreAction::Load, request)
                .await,
            Err(BridgeSessionRuntimeError::InvalidHistoryPageSize)
        );

        let mut request = restore_request(&workspace, "s1");
        request.live_replay_mode = Some("unknown".into());
        assert!(matches!(
            runtime
                .restore_session(BridgeRestoreAction::Resume, request)
                .await,
            Err(BridgeSessionRuntimeError::InvalidLiveReplayMode(_))
        ));
    }
}
