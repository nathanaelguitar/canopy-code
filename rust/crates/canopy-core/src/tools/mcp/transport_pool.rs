//! Shared MCP transport ownership and lifecycle.
//!
//! The pool mirrors Canopy's workspace transport pool at the client boundary:
//! compatible transport configs share one connected `McpClientRuntime`, while
//! session references independently control its idle drain. Session-specific
//! registries are deliberately left to callers; handles expose a cloned
//! discovery snapshot and lifecycle events for each attached session.

use super::client_runtime::{
    McpClientError, McpClientRuntime, McpDiscoverySnapshot, McpRequestOptions,
    McpTransportBuildOptions, McpTransportFactory,
};
use super::discovery_timeout::discovery_timeout_for;
use super::pool_key::{
    McpTransportKind as PoolTransportKind, POOLED_TRANSPORTS_DEFAULT, connection_id_of,
    is_poolable, mcp_transport_of,
};
use super::session_config::{coerce_mcp_filter_entries, normalize_mcp_include_entry};
use super::status::{McpClientStatus, mcp_server_status_registry};
use crate::utils::cancellation::CancellationToken;
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::{Mutex as AsyncMutex, Notify, broadcast};

static NEXT_UNPOOLED_ID: AtomicU64 = AtomicU64::new(1);

const DEFAULT_DRAIN_DELAY: Duration = Duration::from_secs(30);
const DEFAULT_MAX_IDLE: Duration = Duration::from_secs(5 * 60);
const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
const EVENT_CHANNEL_CAPACITY: usize = 64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PoolEntryState {
    Spawning,
    Active,
    Draining,
    Closing,
    Closed,
    Failed,
}

#[derive(Clone, Debug, PartialEq)]
pub enum PoolEvent {
    ToolsChanged {
        server_name: String,
        snapshot: Vec<super::client_runtime::DiscoveredMcpTool>,
        generation: u64,
    },
    PromptsChanged {
        server_name: String,
        snapshot: Vec<super::client_runtime::McpPrompt>,
        generation: u64,
    },
    ResourcesChanged {
        server_name: String,
        snapshot: Vec<super::client_runtime::McpResource>,
        generation: u64,
    },
    Disconnected {
        server_name: String,
        generation: u64,
        reason: &'static str,
    },
    Reconnected {
        server_name: String,
        generation: u64,
    },
    Failed {
        server_name: String,
        generation: u64,
        last_error: String,
    },
}

#[derive(Clone, Debug)]
pub struct McpTransportPoolOptions {
    pub pooled_transports: Vec<PoolTransportKind>,
    pub drain_delay: Duration,
    pub max_idle: Duration,
    pub shutdown_timeout: Duration,
}

impl Default for McpTransportPoolOptions {
    fn default() -> Self {
        Self {
            pooled_transports: POOLED_TRANSPORTS_DEFAULT.to_vec(),
            drain_delay: DEFAULT_DRAIN_DELAY,
            max_idle: DEFAULT_MAX_IDLE,
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
        }
    }
}

#[derive(Debug, Error)]
pub enum McpTransportPoolError {
    #[error(transparent)]
    Client(#[from] McpClientError),
    #[error("MCP transport pool is draining")]
    Draining,
    #[error("MCP acquire was cancelled because session {0} was released")]
    SessionReleased(String),
    #[error("MCP pool startup failed: {0}")]
    Startup(String),
    #[error("MCP pool entry {0} is no longer active")]
    EntryUnavailable(String),
}

#[derive(Clone, Debug, Default)]
pub struct McpPoolSnapshot {
    pub total: usize,
    pub subprocess_count: usize,
    pub by_name: HashMap<String, Vec<McpPoolEntrySummary>>,
}

#[derive(Clone, Debug)]
pub struct McpPoolEntrySummary {
    pub entry_index: usize,
    pub refs: usize,
    pub status: McpClientStatus,
    pub state: PoolEntryState,
}

#[derive(Clone, Debug, Default)]
pub struct McpPoolDrainResult {
    pub drained: usize,
    pub forced: usize,
    pub errors: Vec<McpPoolDrainError>,
}

#[derive(Clone, Debug)]
pub struct McpPoolDrainError {
    pub server_name: String,
    pub entry_index: usize,
    pub error: String,
}

#[derive(Clone)]
pub struct McpTransportPool {
    inner: Arc<PoolInner>,
}

struct PoolInner {
    factory: Arc<dyn McpTransportFactory>,
    build_options: McpTransportBuildOptions,
    options: McpTransportPoolOptions,
    state: Mutex<PoolState>,
    acquire_locks: Mutex<HashMap<String, Arc<AsyncMutex<()>>>>,
    inflight_changed: Notify,
}

#[derive(Default)]
struct PoolState {
    entries: HashMap<String, Arc<PoolEntry>>,
    session_to_entries: HashMap<String, HashSet<String>>,
    session_epochs: HashMap<String, u64>,
    next_index_by_name: HashMap<String, usize>,
    inflight_spawns: HashSet<String>,
    draining: bool,
}

struct PoolEntry {
    id: String,
    transport_id: String,
    server_name: String,
    entry_index: usize,
    transport_kind: PoolTransportKind,
    config: Value,
    client: Arc<McpClientRuntime>,
    build_options: McpTransportBuildOptions,
    pool: Weak<PoolInner>,
    operation: AsyncMutex<()>,
    state: Mutex<EntryData>,
    events: broadcast::Sender<PoolEvent>,
    status_listener_id: Mutex<Option<u64>>,
    restart_in_progress: AtomicBool,
    shared: bool,
}

struct EntryData {
    state: PoolEntryState,
    refs: HashSet<String>,
    generation: u64,
    snapshot: McpDiscoverySnapshot,
    idle_epoch: u64,
    first_idle_at: Option<Instant>,
}

impl McpTransportPool {
    pub fn new(
        factory: Arc<dyn McpTransportFactory>,
        build_options: McpTransportBuildOptions,
        options: McpTransportPoolOptions,
    ) -> Self {
        Self {
            inner: Arc::new(PoolInner {
                factory,
                build_options,
                options,
                state: Mutex::new(PoolState::default()),
                acquire_locks: Mutex::new(HashMap::new()),
                inflight_changed: Notify::new(),
            }),
        }
    }

    /// Acquire a connection. Stdio and WebSocket entries share by the
    /// transport fingerprint by default; other families receive a distinct
    /// per-session client, matching the TypeScript pool's default policy.
    pub async fn acquire(
        &self,
        server_name: impl Into<String>,
        config: Value,
        session_id: impl Into<String>,
    ) -> Result<McpPooledConnection, McpTransportPoolError> {
        let server_name = server_name.into();
        let session_id = session_id.into();
        let transport_id = connection_id_of(&server_name, &config);
        let shared = is_poolable(&config, &self.inner.options.pooled_transports);
        let id = if shared {
            transport_id.clone()
        } else {
            format!(
                "{transport_id}::session:{}::{}",
                session_id,
                NEXT_UNPOOLED_ID.fetch_add(1, Ordering::Relaxed)
            )
        };
        let session_epoch = {
            let state = lock(&self.inner.state);
            if state.draining {
                return Err(McpTransportPoolError::Draining);
            }
            state.session_epochs.get(&session_id).copied().unwrap_or(0)
        };

        let acquire_lock = self.inner.acquire_lock(&id);
        let _acquire_guard = acquire_lock.lock().await;

        {
            let mut state = lock(&self.inner.state);
            self.inner
                .ensure_acquire_allowed(&state, &session_id, session_epoch)?;
            if shared {
                if let Some(entry) = state.entries.get(&id).cloned() {
                    let data = lock(&entry.state);
                    let reusable = matches!(
                        data.state,
                        PoolEntryState::Active | PoolEntryState::Draining
                    ) && entry.client.status() == McpClientStatus::Connected;
                    drop(data);
                    if reusable {
                        self.inner.attach_locked(&mut state, &entry, &session_id)?;
                        return Ok(McpPooledConnection::new(
                            Arc::clone(&entry),
                            Arc::downgrade(&self.inner),
                            session_id,
                            config,
                        ));
                    }
                    self.inner.remove_entry_locked(&mut state, &entry);
                    lock(&entry.state).state = PoolEntryState::Failed;
                    self.inner.start_background_close(entry, "transport_error");
                }
                state.inflight_spawns.insert(id.clone());
            } else {
                state.inflight_spawns.insert(id.clone());
            }
        }
        let _spawn_guard = SpawnReservation {
            pool: Arc::downgrade(&self.inner),
            id: id.clone(),
        };

        let client = Arc::new(McpClientRuntime::new(
            server_name.clone(),
            config.clone(),
            Arc::clone(&self.inner.factory),
        ));
        let cancellation = CancellationToken::new();
        let task_client = Arc::clone(&client);
        let task_options = self.inner.build_options.clone();
        let task_cancellation = cancellation.clone();
        let timeout = Duration::from_secs_f64(discovery_timeout_for(&config).max(0.0) / 1000.0);
        let task = tokio::spawn(async move {
            task_client
                .connect(&task_options, Some(task_cancellation.clone()))
                .await?;
            task_client.discover_and_return(false).await
        });
        let mut cleanup = AcquireCleanupGuard::new(
            Arc::clone(&client),
            Arc::downgrade(&self.inner),
            cancellation.clone(),
            task,
            self.inner.options.shutdown_timeout,
        );
        let snapshot = match tokio::time::timeout(timeout, cleanup.task_mut()).await {
            Ok(Ok(Ok(snapshot))) => snapshot,
            Ok(Ok(Err(error))) => {
                cleanup.disconnect().await;
                return Err(McpTransportPoolError::Client(error));
            }
            Ok(Err(error)) => {
                cleanup.disconnect().await;
                return Err(McpTransportPoolError::Startup(error.to_string()));
            }
            Err(_) => {
                cleanup.disconnect().await;
                return Err(McpTransportPoolError::Startup(format!(
                    "discovery timed out after {}ms for {server_name}",
                    timeout.as_millis()
                )));
            }
        };

        let entry_result = {
            let mut state = lock(&self.inner.state);
            if let Err(error) =
                self.inner
                    .ensure_acquire_allowed(&state, &session_id, session_epoch)
            {
                Err(error)
            } else {
                let entry_index = *state
                    .next_index_by_name
                    .entry(server_name.clone())
                    .and_modify(|next| *next += 1)
                    .or_insert(0);
                let entry = Arc::new(PoolEntry::new(
                    id.clone(),
                    transport_id,
                    server_name,
                    entry_index,
                    mcp_transport_of(&config),
                    config.clone(),
                    Arc::clone(&client),
                    self.inner.build_options.clone(),
                    Arc::downgrade(&self.inner),
                    snapshot,
                    shared,
                ));
                match self.inner.attach_locked(&mut state, &entry, &session_id) {
                    Ok(()) => {
                        state.entries.insert(id, Arc::clone(&entry));
                        Ok(entry)
                    }
                    Err(error) => Err(error),
                }
            }
        };
        let entry = match entry_result {
            Ok(entry) => entry,
            Err(error) => {
                cleanup.disconnect().await;
                return Err(error);
            }
        };
        cleanup.disarm();
        entry.install_status_listener();
        entry.emit_snapshot_events();
        Ok(McpPooledConnection::new(
            entry,
            Arc::downgrade(&self.inner),
            session_id,
            config,
        ))
    }

    /// Release one session's reference. Repeated releases are harmless.
    pub fn release(&self, id: &str, session_id: &str) {
        self.inner.release(id, session_id);
    }

    /// Release all entries held by a session in O(number of its references).
    /// Bumping the session epoch also cancels acquires which began before this
    /// call but have not completed their transport handshake yet.
    pub fn release_session(&self, session_id: &str) {
        self.inner.release_session(session_id);
    }

    pub fn get_snapshot(&self) -> McpPoolSnapshot {
        self.inner.snapshot()
    }

    /// Restart matching entries. Each entry serializes restart with discovery
    /// and calls; a reconnect failure terminally removes the entry so a later
    /// acquire can create a fresh client.
    pub async fn restart_by_name(&self, server_name: &str) -> Vec<McpPoolRestartResult> {
        let entries = {
            let state = lock(&self.inner.state);
            state
                .entries
                .values()
                .filter(|entry| entry.server_name == server_name)
                .cloned()
                .collect::<Vec<_>>()
        };
        let mut results = Vec::with_capacity(entries.len());
        for entry in entries {
            let started = Instant::now();
            let result = entry.restart().await;
            results.push(match result {
                Ok(()) => McpPoolRestartResult {
                    entry_index: entry.entry_index,
                    restarted: true,
                    duration: started.elapsed(),
                    reason: None,
                },
                Err(error) => McpPoolRestartResult {
                    entry_index: entry.entry_index,
                    restarted: false,
                    duration: started.elapsed(),
                    reason: Some(error.to_string()),
                },
            });
        }
        results
    }

    /// Stop accepting new acquires, wait briefly for in-flight handshakes,
    /// then disconnect every entry with a bounded cleanup window.
    pub async fn drain_all(&self, timeout: Duration) -> McpPoolDrainResult {
        {
            lock(&self.inner.state).draining = true;
        }
        let deadline = Instant::now() + timeout;
        loop {
            let notified = self.inner.inflight_changed.notified();
            if lock(&self.inner.state).inflight_spawns.is_empty() {
                break;
            }
            if tokio::time::timeout_at(deadline.into(), notified)
                .await
                .is_err()
            {
                break;
            }
        }
        let entries = {
            let mut state = lock(&self.inner.state);
            let entries = state.entries.values().cloned().collect::<Vec<_>>();
            for entry in &entries {
                let mut data = lock(&entry.state);
                data.state = PoolEntryState::Closing;
                data.idle_epoch = data.idle_epoch.saturating_add(1);
                data.refs.clear();
            }
            state.entries.clear();
            state.session_to_entries.clear();
            entries
        };
        let closes = futures_util::future::join_all(entries.into_iter().map(|entry| async move {
            let close =
                tokio::time::timeout_at(deadline.into(), entry.shutdown("transport_closed")).await;
            (entry, close)
        }))
        .await;
        let mut result = McpPoolDrainResult::default();
        for (entry, close_result) in closes {
            match close_result {
                Ok(Ok(())) => result.drained += 1,
                Ok(Err(error)) => result.errors.push(McpPoolDrainError {
                    server_name: entry.server_name.clone(),
                    entry_index: entry.entry_index,
                    error: error.to_string(),
                }),
                Err(_) => {
                    result.forced += 1;
                    self.inner.start_background_close(entry, "transport_closed");
                }
            }
        }
        result
    }
}

#[derive(Clone, Debug)]
pub struct McpPoolRestartResult {
    pub entry_index: usize,
    pub restarted: bool,
    pub duration: Duration,
    pub reason: Option<String>,
}

impl PoolInner {
    fn acquire_lock(&self, id: &str) -> Arc<AsyncMutex<()>> {
        Arc::clone(
            lock(&self.acquire_locks)
                .entry(id.to_owned())
                .or_insert_with(|| Arc::new(AsyncMutex::new(()))),
        )
    }

    fn ensure_acquire_allowed(
        &self,
        state: &PoolState,
        session_id: &str,
        expected_epoch: u64,
    ) -> Result<(), McpTransportPoolError> {
        if state.draining {
            return Err(McpTransportPoolError::Draining);
        }
        if state.session_epochs.get(session_id).copied().unwrap_or(0) != expected_epoch {
            return Err(McpTransportPoolError::SessionReleased(
                session_id.to_owned(),
            ));
        }
        Ok(())
    }

    fn attach_locked(
        &self,
        state: &mut PoolState,
        entry: &Arc<PoolEntry>,
        session_id: &str,
    ) -> Result<(), McpTransportPoolError> {
        let mut data = lock(&entry.state);
        if !matches!(
            data.state,
            PoolEntryState::Active | PoolEntryState::Draining
        ) {
            return Err(McpTransportPoolError::EntryUnavailable(entry.id.clone()));
        }
        data.refs.insert(session_id.to_owned());
        data.idle_epoch = data.idle_epoch.saturating_add(1);
        data.first_idle_at = None;
        data.state = PoolEntryState::Active;
        state
            .session_to_entries
            .entry(session_id.to_owned())
            .or_default()
            .insert(entry.id.clone());
        Ok(())
    }

    fn remove_entry_locked(&self, state: &mut PoolState, entry: &Arc<PoolEntry>) -> bool {
        if !state
            .entries
            .get(&entry.id)
            .is_some_and(|current| Arc::ptr_eq(current, entry))
        {
            return false;
        }
        state.entries.remove(&entry.id);
        let refs = {
            let mut data = lock(&entry.state);
            data.idle_epoch = data.idle_epoch.saturating_add(1);
            std::mem::take(&mut data.refs)
        };
        for session_id in refs {
            if let Some(ids) = state.session_to_entries.get_mut(&session_id) {
                ids.remove(&entry.id);
                if ids.is_empty() {
                    state.session_to_entries.remove(&session_id);
                }
            }
        }
        true
    }

    fn release(self: &Arc<Self>, id: &str, session_id: &str) {
        let entry = {
            let mut state = lock(&self.state);
            let Some(entry) = state.entries.get(id).cloned() else {
                return;
            };
            {
                let mut data = lock(&entry.state);
                data.refs.remove(session_id);
                if data.refs.is_empty() {
                    data.state = PoolEntryState::Draining;
                    if data.first_idle_at.is_none() {
                        data.first_idle_at = Some(Instant::now());
                    }
                    data.idle_epoch = data.idle_epoch.saturating_add(1);
                }
            }
            if let Some(ids) = state.session_to_entries.get_mut(session_id) {
                ids.remove(id);
                if ids.is_empty() {
                    state.session_to_entries.remove(session_id);
                }
            }
            entry
        };
        self.schedule_idle_close(entry);
    }

    fn schedule_idle_close(self: &Arc<Self>, entry: Arc<PoolEntry>) {
        let (epoch, delay) = {
            let mut data = lock(&entry.state);
            if !data.refs.is_empty() {
                return;
            }
            data.state = PoolEntryState::Draining;
            if data.first_idle_at.is_none() {
                data.first_idle_at = Some(Instant::now());
            }
            data.idle_epoch = data.idle_epoch.saturating_add(1);
            let idle_since = data.first_idle_at.unwrap_or_else(Instant::now);
            let max_idle_remaining = self.options.max_idle.saturating_sub(idle_since.elapsed());
            (
                data.idle_epoch,
                self.options.drain_delay.min(max_idle_remaining),
            )
        };
        if !entry.shared {
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                let pool = Arc::downgrade(self);
                handle.spawn(async move {
                    if let Some(pool) = pool.upgrade() {
                        pool.close_entry(entry, "transport_closed").await;
                    }
                });
            }
        } else if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let pool = Arc::downgrade(self);
            handle.spawn(async move {
                tokio::time::sleep(delay).await;
                let Some(pool) = pool.upgrade() else { return };
                let still_idle = {
                    let data = lock(&entry.state);
                    data.refs.is_empty()
                        && data.idle_epoch == epoch
                        && data.state == PoolEntryState::Draining
                };
                if still_idle {
                    pool.close_entry(entry, "transport_closed").await;
                }
            });
        }
    }

    fn release_session(self: &Arc<Self>, session_id: &str) {
        let entries = {
            let mut state = lock(&self.state);
            let epoch = state
                .session_epochs
                .entry(session_id.to_owned())
                .or_default();
            *epoch = epoch.saturating_add(1);
            let ids = state
                .session_to_entries
                .remove(session_id)
                .unwrap_or_default();
            let mut idle_entries = Vec::new();
            for id in ids {
                let Some(entry) = state.entries.get(&id).cloned() else {
                    continue;
                };
                let mut data = lock(&entry.state);
                data.refs.remove(session_id);
                if data.refs.is_empty() {
                    data.state = PoolEntryState::Draining;
                    if data.first_idle_at.is_none() {
                        data.first_idle_at = Some(Instant::now());
                    }
                    data.idle_epoch = data.idle_epoch.saturating_add(1);
                    idle_entries.push(entry.clone());
                }
            }
            idle_entries
        };
        for entry in entries {
            self.schedule_idle_close(entry);
        }
    }

    fn snapshot(&self) -> McpPoolSnapshot {
        let state = lock(&self.state);
        let mut snapshot = McpPoolSnapshot::default();
        for entry in state.entries.values() {
            let data = lock(&entry.state);
            let status = entry.client.status();
            if status == McpClientStatus::Connected {
                snapshot.total += 1;
                if entry.transport_kind == PoolTransportKind::Stdio {
                    snapshot.subprocess_count += 1;
                }
            }
            snapshot
                .by_name
                .entry(entry.server_name.clone())
                .or_default()
                .push(McpPoolEntrySummary {
                    entry_index: entry.entry_index,
                    refs: data.refs.len(),
                    status,
                    state: data.state,
                });
        }
        for entries in snapshot.by_name.values_mut() {
            entries.sort_by_key(|entry| entry.entry_index);
        }
        snapshot
    }

    async fn close_entry(self: &Arc<Self>, entry: Arc<PoolEntry>, reason: &'static str) {
        {
            let mut state = lock(&self.state);
            if !self.remove_entry_locked(&mut state, &entry) {
                return;
            }
            lock(&entry.state).state = PoolEntryState::Closing;
        }
        let timeout = self.options.shutdown_timeout;
        let close = entry.shutdown(reason);
        if tokio::time::timeout(timeout, close).await.is_err() {
            self.start_background_close(entry, reason);
        }
    }

    fn start_background_close(self: &Arc<Self>, entry: Arc<PoolEntry>, reason: &'static str) {
        let timeout = self.options.shutdown_timeout;
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let _ = tokio::time::timeout(timeout, entry.shutdown(reason)).await;
            });
        }
    }

    async fn disconnect_best_effort(&self, client: &Arc<McpClientRuntime>) {
        let timeout = self.options.shutdown_timeout;
        let disconnect = client.disconnect();
        let _ = tokio::time::timeout(timeout, disconnect).await;
    }
}

struct SpawnReservation {
    pool: Weak<PoolInner>,
    id: String,
}

impl Drop for SpawnReservation {
    fn drop(&mut self) {
        if let Some(pool) = self.pool.upgrade() {
            lock(&pool.state).inflight_spawns.remove(&self.id);
            pool.inflight_changed.notify_one();
        }
    }
}

/// Owns the spawned connection handshake until the pool entry is attached.
/// Dropping the outer acquire future aborts that task and schedules bounded
/// disconnect cleanup for any transport it managed to create.
struct AcquireCleanupGuard {
    client: Arc<McpClientRuntime>,
    pool: Weak<PoolInner>,
    cancellation: CancellationToken,
    task: Option<tokio::task::JoinHandle<Result<McpDiscoverySnapshot, McpClientError>>>,
    shutdown_timeout: Duration,
    armed: bool,
}

impl AcquireCleanupGuard {
    fn new(
        client: Arc<McpClientRuntime>,
        pool: Weak<PoolInner>,
        cancellation: CancellationToken,
        task: tokio::task::JoinHandle<Result<McpDiscoverySnapshot, McpClientError>>,
        shutdown_timeout: Duration,
    ) -> Self {
        Self {
            client,
            pool,
            cancellation,
            task: Some(task),
            shutdown_timeout,
            armed: true,
        }
    }

    fn task_mut(
        &mut self,
    ) -> &mut tokio::task::JoinHandle<Result<McpDiscoverySnapshot, McpClientError>> {
        self.task
            .as_mut()
            .expect("acquire cleanup task remains armed until entry attachment")
    }

    async fn disconnect(&mut self) {
        if let Some(cleanup) = self.start_cleanup() {
            let _ = cleanup.await;
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
        self.task.take();
    }

    fn start_cleanup(&mut self) -> Option<tokio::task::JoinHandle<()>> {
        if !self.armed {
            return None;
        }
        self.armed = false;
        self.cancellation.cancel();
        if let Some(task) = self.task.take() {
            task.abort();
        }
        schedule_client_disconnect(
            self.pool.clone(),
            Arc::clone(&self.client),
            self.shutdown_timeout,
        )
    }
}

impl Drop for AcquireCleanupGuard {
    fn drop(&mut self) {
        let _ = self.start_cleanup();
    }
}

fn schedule_client_disconnect(
    pool: Weak<PoolInner>,
    client: Arc<McpClientRuntime>,
    shutdown_timeout: Duration,
) -> Option<tokio::task::JoinHandle<()>> {
    let runtime = tokio::runtime::Handle::try_current().ok()?;
    Some(runtime.spawn(async move {
        if let Some(pool) = pool.upgrade() {
            pool.disconnect_best_effort(&client).await;
        } else {
            let _ = tokio::time::timeout(shutdown_timeout, client.disconnect()).await;
        }
    }))
}

impl PoolEntry {
    #[allow(clippy::too_many_arguments)]
    fn new(
        id: String,
        transport_id: String,
        server_name: String,
        entry_index: usize,
        transport_kind: PoolTransportKind,
        config: Value,
        client: Arc<McpClientRuntime>,
        build_options: McpTransportBuildOptions,
        pool: Weak<PoolInner>,
        snapshot: McpDiscoverySnapshot,
        shared: bool,
    ) -> Self {
        let (events, _) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        Self {
            id,
            transport_id,
            server_name,
            entry_index,
            transport_kind,
            config,
            client,
            build_options,
            pool,
            operation: AsyncMutex::new(()),
            state: Mutex::new(EntryData {
                state: PoolEntryState::Active,
                refs: HashSet::new(),
                generation: 0,
                snapshot,
                idle_epoch: 0,
                first_idle_at: None,
            }),
            events,
            status_listener_id: Mutex::new(None),
            restart_in_progress: AtomicBool::new(false),
            shared,
        }
    }

    fn install_status_listener(self: &Arc<Self>) {
        let entry = Arc::downgrade(self);
        let listener = Arc::new(move |name: &str, status: Option<McpClientStatus>| {
            let Some(entry) = entry.upgrade() else { return };
            if name != entry.server_name
                || status != Some(McpClientStatus::Disconnected)
                || entry.client.status() != McpClientStatus::Disconnected
                || entry.restart_in_progress.load(Ordering::Acquire)
            {
                return;
            }
            let generation = {
                let mut data = lock(&entry.state);
                // A callback may already have been copied out of the registry
                // before restart began. Recheck while holding the state lock
                // so it cannot turn the restarting entry into Failed.
                if entry.restart_in_progress.load(Ordering::Acquire) {
                    return;
                }
                if !matches!(
                    data.state,
                    PoolEntryState::Active | PoolEntryState::Draining
                ) {
                    return;
                }
                data.state = PoolEntryState::Failed;
                data.generation
            };
            let message = entry.client.get_last_transport_error().map_or_else(
                || "transport disconnected".to_owned(),
                |error| error.to_string(),
            );
            let _ = entry.events.send(PoolEvent::Failed {
                server_name: entry.server_name.clone(),
                generation,
                last_error: message,
            });
            if let Some(pool) = entry.pool.upgrade() {
                if let Ok(handle) = tokio::runtime::Handle::try_current() {
                    let pool = Arc::clone(&pool);
                    handle.spawn(async move { pool.close_entry(entry, "transport_error").await });
                }
            }
        });
        let id = mcp_server_status_registry().add_listener(listener);
        *lock(&self.status_listener_id) = Some(id);
    }

    fn emit_snapshot_events(&self) {
        let data = lock(&self.state);
        let generation = data.generation;
        let _ = self.events.send(PoolEvent::ToolsChanged {
            server_name: self.server_name.clone(),
            snapshot: data.snapshot.tools.clone(),
            generation,
        });
        let _ = self.events.send(PoolEvent::PromptsChanged {
            server_name: self.server_name.clone(),
            snapshot: data.snapshot.prompts.clone(),
            generation,
        });
        let _ = self.events.send(PoolEvent::ResourcesChanged {
            server_name: self.server_name.clone(),
            snapshot: data.snapshot.resources.clone(),
            generation,
        });
    }

    fn snapshot(&self) -> McpDiscoverySnapshot {
        lock(&self.state).snapshot.clone()
    }

    async fn call_tool(
        &self,
        name: &str,
        arguments: &Map<String, Value>,
        options: McpRequestOptions,
    ) -> Result<Value, McpClientError> {
        let _operation = self.operation.lock().await;
        self.ensure_active()?;
        let result = self.client.call_tool(name, arguments, options).await;
        self.observe_operation_result(&result);
        result
    }

    async fn read_resource(
        &self,
        uri: &str,
        options: McpRequestOptions,
    ) -> Result<Value, McpClientError> {
        let _operation = self.operation.lock().await;
        self.ensure_active()?;
        let result = self.client.read_resource(uri, options).await;
        self.observe_operation_result(&result);
        result
    }

    fn ensure_active(&self) -> Result<(), McpClientError> {
        let data = lock(&self.state);
        if data.state != PoolEntryState::Active
            || self.client.status() != McpClientStatus::Connected
        {
            return Err(McpClientError::NotConnected);
        }
        Ok(())
    }

    fn observe_operation_result(&self, result: &Result<Value, McpClientError>) {
        if self.client.status() != McpClientStatus::Disconnected {
            return;
        }
        let Ok(mut data) = self.state.lock() else {
            return;
        };
        if !matches!(
            data.state,
            PoolEntryState::Active | PoolEntryState::Draining
        ) {
            return;
        }
        data.state = PoolEntryState::Failed;
        let generation = data.generation;
        let last_error = result
            .as_ref()
            .err()
            .map(ToString::to_string)
            .or_else(|| {
                self.client
                    .get_last_transport_error()
                    .map(|error| error.to_string())
            })
            .unwrap_or_else(|| "transport disconnected".to_owned());
        let _ = self.events.send(PoolEvent::Failed {
            server_name: self.server_name.clone(),
            generation,
            last_error,
        });
    }

    async fn restart(self: &Arc<Self>) -> Result<(), McpClientError> {
        let _operation = self.operation.lock().await;
        {
            let mut data = lock(&self.state);
            if matches!(
                data.state,
                PoolEntryState::Closed | PoolEntryState::Closing | PoolEntryState::Failed
            ) {
                return Err(McpClientError::NotConnected);
            }
            self.restart_in_progress.store(true, Ordering::Release);
            data.idle_epoch = data.idle_epoch.saturating_add(1);
            data.first_idle_at = None;
            let old_generation = data.generation;
            data.generation = data.generation.saturating_add(1);
            data.state = PoolEntryState::Active;
            let _ = self.events.send(PoolEvent::Disconnected {
                server_name: self.server_name.clone(),
                generation: old_generation,
                reason: "restart",
            });
        }
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let client = Arc::clone(&self.client);
        let build_options = self.build_options.clone();
        let timeout_ms = discovery_timeout_for(&self.config);
        let timeout = Duration::from_secs_f64(timeout_ms / 1000.0);
        let mut task = tokio::spawn(async move {
            client.disconnect().await?;
            client
                .connect(&build_options, Some(task_cancellation.clone()))
                .await?;
            client.discover_and_return(false).await
        });
        let result = match tokio::time::timeout(timeout, &mut task).await {
            Ok(Ok(result)) => result,
            Ok(Err(error)) => Err(McpClientError::TransportSetup(error.to_string())),
            Err(_) => {
                cancellation.cancel();
                task.abort();
                Err(McpClientError::Timeout {
                    method: "pool restart".to_owned(),
                    timeout_ms: timeout_ms.ceil() as u64,
                })
            }
        };
        match result {
            Ok(snapshot) => {
                let idle = {
                    let mut data = lock(&self.state);
                    if matches!(data.state, PoolEntryState::Closing | PoolEntryState::Closed) {
                        self.restart_in_progress.store(false, Ordering::Release);
                        return Err(McpClientError::NotConnected);
                    }
                    let mut snapshot = snapshot;
                    if snapshot.resources.is_empty() {
                        snapshot.resources = data.snapshot.resources.clone();
                    }
                    data.snapshot = snapshot;
                    data.state = PoolEntryState::Active;
                    data.refs.is_empty()
                };
                self.restart_in_progress.store(false, Ordering::Release);
                self.emit_snapshot_events();
                let generation = lock(&self.state).generation;
                let _ = self.events.send(PoolEvent::Reconnected {
                    server_name: self.server_name.clone(),
                    generation,
                });
                if idle {
                    if let Some(pool) = self.pool.upgrade() {
                        pool.schedule_idle_close(Arc::clone(self));
                    }
                }
                Ok(())
            }
            Err(error) => {
                let generation = {
                    let mut data = lock(&self.state);
                    data.state = PoolEntryState::Failed;
                    data.generation
                };
                self.restart_in_progress.store(false, Ordering::Release);
                let _ = self.events.send(PoolEvent::Failed {
                    server_name: self.server_name.clone(),
                    generation,
                    last_error: error.to_string(),
                });
                if let Some(id) = lock(&self.status_listener_id).take() {
                    mcp_server_status_registry().remove_listener(id);
                }
                let cleanup_timeout = self
                    .pool
                    .upgrade()
                    .map(|pool| pool.options.shutdown_timeout)
                    .unwrap_or(DEFAULT_SHUTDOWN_TIMEOUT);
                let _ = tokio::time::timeout(cleanup_timeout, self.client.disconnect()).await;
                if let Some(pool) = self.pool.upgrade() {
                    let mut state = lock(&pool.state);
                    pool.remove_entry_locked(&mut state, self);
                }
                Err(error)
            }
        }
    }

    async fn shutdown(&self, reason: &'static str) -> Result<(), McpClientError> {
        let _operation = self.operation.lock().await;
        let (generation, first_close) = {
            let mut data = lock(&self.state);
            let first_close = data.state != PoolEntryState::Closed;
            if first_close {
                data.state = PoolEntryState::Closed;
                data.refs.clear();
            }
            (data.generation, first_close)
        };
        if first_close {
            if let Some(id) = lock(&self.status_listener_id).take() {
                mcp_server_status_registry().remove_listener(id);
            }
            let _ = self.events.send(PoolEvent::Disconnected {
                server_name: self.server_name.clone(),
                generation,
                reason,
            });
        }
        self.client.disconnect().await
    }
}

pub struct McpPooledConnection {
    entry: Arc<PoolEntry>,
    pool: Weak<PoolInner>,
    session_id: String,
    session_config: Arc<Mutex<Value>>,
    released: AtomicBool,
}

impl McpPooledConnection {
    fn new(
        entry: Arc<PoolEntry>,
        pool: Weak<PoolInner>,
        session_id: String,
        session_config: Value,
    ) -> Self {
        Self {
            entry,
            pool,
            session_id,
            session_config: Arc::new(Mutex::new(session_config)),
            released: AtomicBool::new(false),
        }
    }

    pub fn id(&self) -> &str {
        &self.entry.id
    }

    pub fn transport_id(&self) -> &str {
        &self.entry.transport_id
    }

    pub fn server_name(&self) -> &str {
        &self.entry.server_name
    }

    pub fn entry_index(&self) -> usize {
        self.entry.entry_index
    }

    pub fn generation(&self) -> u64 {
        lock(&self.entry.state).generation
    }

    pub fn state(&self) -> PoolEntryState {
        lock(&self.entry.state).state
    }

    pub fn client(&self) -> Arc<McpClientRuntime> {
        Arc::clone(&self.entry.client)
    }

    pub fn snapshot(&self) -> McpDiscoverySnapshot {
        project_snapshot(self.entry.snapshot(), &lock(&self.session_config))
    }

    pub fn subscribe(&self) -> McpPoolEventReceiver {
        McpPoolEventReceiver {
            receiver: self.entry.events.subscribe(),
            session_config: Arc::clone(&self.session_config),
        }
    }

    /// Refresh per-session filtering and tool metadata without changing the
    /// shared transport or its canonical discovery snapshot.
    pub fn update_config(&self, config: Value) {
        let changed = {
            let mut current = lock(&self.session_config);
            let old_key = super::session_config::mcp_session_metadata_key(&current).ok();
            let new_key = super::session_config::mcp_session_metadata_key(&config).ok();
            *current = config;
            old_key != new_key
        };
        if changed {
            // SessionMcpView reapplies current snapshots as soon as its
            // filters or tool metadata change. Broadcast the canonical
            // snapshot; each receiver projects it against its own config.
            self.entry.emit_snapshot_events();
        }
    }

    pub async fn call_tool(
        &self,
        name: &str,
        arguments: &Map<String, Value>,
        options: McpRequestOptions,
    ) -> Result<Value, McpClientError> {
        if self.released.load(Ordering::Acquire) {
            return Err(McpClientError::NotConnected);
        }
        self.entry.call_tool(name, arguments, options).await
    }

    pub async fn read_resource(
        &self,
        uri: &str,
        options: McpRequestOptions,
    ) -> Result<Value, McpClientError> {
        if self.released.load(Ordering::Acquire) {
            return Err(McpClientError::NotConnected);
        }
        self.entry.read_resource(uri, options).await
    }

    pub fn release(&self) {
        if self.released.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Some(pool) = self.pool.upgrade() {
            pool.release(&self.entry.id, &self.session_id);
        }
    }
}

pub struct McpPoolEventReceiver {
    receiver: broadcast::Receiver<PoolEvent>,
    session_config: Arc<Mutex<Value>>,
}

impl McpPoolEventReceiver {
    pub async fn recv(&mut self) -> Result<PoolEvent, broadcast::error::RecvError> {
        let event = self.receiver.recv().await?;
        let config = lock(&self.session_config);
        Ok(project_event(event, &config))
    }
}

fn project_event(event: PoolEvent, config: &Value) -> PoolEvent {
    match event {
        PoolEvent::ToolsChanged {
            server_name,
            snapshot,
            generation,
        } => PoolEvent::ToolsChanged {
            server_name,
            snapshot: project_tools(snapshot, config),
            generation,
        },
        PoolEvent::PromptsChanged {
            server_name,
            snapshot,
            generation,
        } => PoolEvent::PromptsChanged {
            server_name,
            snapshot: snapshot
                .into_iter()
                .filter(|prompt| session_name_is_enabled(&prompt.name, config))
                .collect(),
            generation,
        },
        other => other,
    }
}

fn project_snapshot(snapshot: McpDiscoverySnapshot, config: &Value) -> McpDiscoverySnapshot {
    McpDiscoverySnapshot {
        tools: project_tools(snapshot.tools, config),
        prompts: snapshot
            .prompts
            .into_iter()
            .filter(|prompt| session_name_is_enabled(&prompt.name, config))
            .collect(),
        resources: snapshot.resources,
    }
}

fn project_tools(
    tools: Vec<super::client_runtime::DiscoveredMcpTool>,
    config: &Value,
) -> Vec<super::client_runtime::DiscoveredMcpTool> {
    let trust = config.get("trust").and_then(Value::as_bool);
    let always_load = config.get("alwaysLoadTools").and_then(Value::as_bool) == Some(true);
    tools
        .into_iter()
        .filter(|tool| session_name_is_enabled(&tool.name, config))
        .map(|mut tool| {
            tool.trust = trust;
            tool.always_load = always_load;
            tool
        })
        .collect()
}

fn session_name_is_enabled(name: &str, config: &Value) -> bool {
    let include_value = config.get("includeTools").filter(|value| !value.is_null());
    let include = coerce_mcp_filter_entries(include_value);
    if config
        .get("excludeTools")
        .is_some_and(|value| coerce_mcp_filter_entries(Some(value)).contains(&name))
    {
        return false;
    }
    include_value.is_none()
        || include
            .into_iter()
            .any(|entry| normalize_mcp_include_entry(entry) == name)
}

impl Drop for McpPooledConnection {
    fn drop(&mut self) {
        self.release();
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poison| poison.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::mcp::client_manager::McpClientManager;
    use crate::tools::mcp::client_runtime::{McpTransport, McpTransportError, McpTransportSpec};
    use futures_util::future::BoxFuture;
    use serde_json::json;
    use std::sync::atomic::AtomicUsize;

    #[derive(Default)]
    struct FakeStats {
        creates: AtomicUsize,
        fail_on_create: AtomicUsize,
        closes: AtomicUsize,
        active_calls: AtomicUsize,
        max_active_calls: AtomicUsize,
    }

    struct FakeFactory(Arc<FakeStats>);

    impl McpTransportFactory for FakeFactory {
        fn create<'a>(
            &'a self,
            _spec: McpTransportSpec,
            _cancellation: Option<CancellationToken>,
        ) -> BoxFuture<
            'a,
            Result<Arc<dyn super::super::client_runtime::McpTransport>, McpTransportError>,
        > {
            let stats = Arc::clone(&self.0);
            Box::pin(async move {
                let create_index = stats.creates.fetch_add(1, Ordering::SeqCst) + 1;
                if stats.fail_on_create.load(Ordering::SeqCst) == create_index {
                    return Err(McpTransportError::Transport(
                        "injected factory failure".to_owned(),
                    ));
                }
                Ok(Arc::new(FakeTransport(stats)) as Arc<dyn McpTransport>)
            })
        }
    }

    struct FakeTransport(Arc<FakeStats>);

    impl McpTransport for FakeTransport {
        fn request<'a>(
            &'a self,
            request: Value,
            _cancellation: Option<CancellationToken>,
        ) -> BoxFuture<'a, Result<Value, McpTransportError>> {
            let stats = Arc::clone(&self.0);
            Box::pin(async move {
                let method = request
                    .get("method")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let id = request.get("id").cloned().unwrap_or(Value::Null);
                let result = match method {
                    "initialize" => json!({
                        "protocolVersion":"2025-06-18",
                        "capabilities":{},
                        "serverInfo":{"name":"test","version":"1"}
                    }),
                    "tools/list" => {
                        json!({"tools":[{"name":"echo","inputSchema":{"type":"object"}}]})
                    }
                    "prompts/list" => json!({"prompts":[
                        {"name":"echo","description":"Echo prompt"},
                        {"name":"ask","description":"Ask prompt"}
                    ]}),
                    "resources/list" => json!({"resources":[]}),
                    "tools/call" => {
                        let active = stats.active_calls.fetch_add(1, Ordering::SeqCst) + 1;
                        stats.max_active_calls.fetch_max(active, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        stats.active_calls.fetch_sub(1, Ordering::SeqCst);
                        json!({"content":[]})
                    }
                    "resources/read" => json!({"contents":[]}),
                    _ => json!({}),
                };
                Ok(json!({"jsonrpc":"2.0","id":id,"result":result}))
            })
        }

        fn notify<'a>(
            &'a self,
            _notification: Value,
            _cancellation: Option<CancellationToken>,
        ) -> BoxFuture<'a, Result<(), McpTransportError>> {
            Box::pin(async { Ok(()) })
        }

        fn close<'a>(&'a self) -> BoxFuture<'a, Result<(), McpTransportError>> {
            let stats = Arc::clone(&self.0);
            Box::pin(async move {
                stats.closes.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        }
    }

    fn make_pool(stats: Arc<FakeStats>, drain_delay: Duration) -> McpTransportPool {
        McpTransportPool::new(
            Arc::new(FakeFactory(stats)),
            McpTransportBuildOptions::default(),
            McpTransportPoolOptions {
                drain_delay,
                max_idle: Duration::from_secs(1),
                shutdown_timeout: Duration::from_millis(100),
                ..Default::default()
            },
        )
    }

    #[tokio::test]
    async fn same_transport_is_shared_and_last_session_release_drains_it() {
        let stats = Arc::new(FakeStats::default());
        let pool = make_pool(Arc::clone(&stats), Duration::from_millis(10));
        let config = json!({"command":"fake","timeout":1000,"discoveryTimeoutMs":1000});
        let first = pool.acquire("server", config.clone(), "s1").await.unwrap();
        let second = pool.acquire("server", config, "s2").await.unwrap();

        assert_eq!(stats.creates.load(Ordering::SeqCst), 1);
        assert_eq!(pool.get_snapshot().by_name["server"][0].refs, 2);
        first.release();
        assert_eq!(pool.get_snapshot().by_name["server"][0].refs, 1);
        assert_eq!(
            first
                .call_tool("echo", &Map::new(), McpRequestOptions::default())
                .await,
            Err(McpClientError::NotConnected)
        );
        assert!(
            second
                .call_tool("echo", &Map::new(), McpRequestOptions::default())
                .await
                .is_ok()
        );
        second.release();
        tokio::time::sleep(Duration::from_millis(30)).await;

        assert!(pool.get_snapshot().by_name.is_empty());
        assert_eq!(stats.closes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn reattach_resets_max_idle_age_before_last_release() {
        let stats = Arc::new(FakeStats::default());
        let pool = McpTransportPool::new(
            Arc::new(FakeFactory(Arc::clone(&stats))),
            McpTransportBuildOptions::default(),
            McpTransportPoolOptions {
                drain_delay: Duration::from_millis(100),
                max_idle: Duration::from_millis(120),
                shutdown_timeout: Duration::from_millis(100),
                ..Default::default()
            },
        );
        let config = json!({"command":"fake","timeout":1000,"discoveryTimeoutMs":1000});
        let first = pool.acquire("server", config.clone(), "s1").await.unwrap();
        first.release();
        tokio::time::sleep(Duration::from_millis(90)).await;

        let second = pool.acquire("server", config, "s2").await.unwrap();
        second.release();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(pool.get_snapshot().by_name["server"][0].refs, 0);
        assert_eq!(stats.closes.load(Ordering::SeqCst), 0);

        tokio::time::sleep(Duration::from_millis(70)).await;
        assert!(pool.get_snapshot().by_name.is_empty());
        assert_eq!(stats.closes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn restart_failure_commits_failed_state_before_status_listener_resumes() {
        let stats = Arc::new(FakeStats::default());
        let pool = make_pool(Arc::clone(&stats), Duration::from_secs(1));
        let connection = pool
            .acquire(
                "server",
                json!({"command":"fake","timeout":1000,"discoveryTimeoutMs":1000}),
                "session",
            )
            .await
            .unwrap();
        let mut events = connection.subscribe();
        stats.fail_on_create.store(2, Ordering::SeqCst);

        let results = pool.restart_by_name("server").await;

        assert_eq!(results.len(), 1);
        assert!(!results[0].restarted);
        assert_eq!(connection.state(), PoolEntryState::Failed);
        assert!(matches!(
            events.receiver.try_recv(),
            Ok(PoolEvent::Disconnected {
                reason: "restart",
                ..
            })
        ));
        assert!(matches!(
            events.receiver.try_recv(),
            Ok(PoolEvent::Failed { last_error, .. }) if last_error.contains("injected factory failure")
        ));
        assert!(events.receiver.try_recv().is_err());
        connection.release();
    }

    #[tokio::test]
    async fn shared_entry_keeps_session_filters_and_metadata_isolated() {
        let stats = Arc::new(FakeStats::default());
        let pool = make_pool(Arc::clone(&stats), Duration::from_millis(10));
        let first = pool
            .acquire(
                "server",
                json!({
                    "command":"fake",
                    "timeout":1000,
                    "discoveryTimeoutMs":1000,
                    "includeTools":["echo(args)"],
                    "trust":true,
                    "alwaysLoadTools":true
                }),
                "s1",
            )
            .await
            .unwrap();
        let second = pool
            .acquire(
                "server",
                json!({
                    "command":"fake",
                    "timeout":1000,
                    "discoveryTimeoutMs":1000,
                    "includeTools":["different"],
                    "trust":false
                }),
                "s2",
            )
            .await
            .unwrap();

        assert_eq!(first.id(), second.id());
        assert_eq!(first.snapshot().tools.len(), 1);
        assert_eq!(first.snapshot().tools[0].trust, Some(true));
        assert!(first.snapshot().tools[0].always_load);
        assert_eq!(first.snapshot().prompts.len(), 1);
        assert_eq!(first.snapshot().prompts[0].name, "echo");
        assert!(second.snapshot().tools.is_empty());
        assert!(second.snapshot().prompts.is_empty());
        second.update_config(json!({
            "excludeTools":["echo", "ask"],
            "trust":true
        }));
        assert!(second.snapshot().tools.is_empty());
        assert!(second.snapshot().prompts.is_empty());

        let mut events = second.subscribe();
        pool.restart_by_name("server").await;
        loop {
            if let PoolEvent::ToolsChanged { snapshot, .. } = events.recv().await.unwrap() {
                assert!(snapshot.is_empty());
                break;
            }
        }

        first.release();
        second.release();
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(stats.creates.load(Ordering::SeqCst), 2);
        assert!(pool.get_snapshot().by_name.is_empty());
    }

    #[tokio::test]
    async fn http_connections_remain_session_scoped_by_default() {
        let stats = Arc::new(FakeStats::default());
        let pool = make_pool(Arc::clone(&stats), Duration::from_millis(10));
        let config = json!({"httpUrl":"https://example.test/mcp","timeout":1000});
        let first = pool.acquire("remote", config.clone(), "s1").await.unwrap();
        let second = pool.acquire("remote", config, "s2").await.unwrap();

        assert_ne!(first.id(), second.id());
        assert_eq!(stats.creates.load(Ordering::SeqCst), 2);
        assert_eq!(pool.get_snapshot().by_name["remote"].len(), 2);

        first.release();
        second.release();
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(pool.get_snapshot().by_name.is_empty());
        assert_eq!(stats.closes.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn manager_serializes_discovery_and_calls_per_entry() {
        let stats = Arc::new(FakeStats::default());
        let pool = make_pool(Arc::clone(&stats), Duration::from_millis(10));
        let manager = Arc::new(McpClientManager::new(pool.clone(), "session"));
        let servers: Map<String, Value> = serde_json::from_value(json!({
            "server":{"command":"fake","timeout":1000,"discoveryTimeoutMs":1000}
        }))
        .unwrap();
        let (first, second) = tokio::join!(
            manager.discover_all(&servers),
            manager.discover_all(&servers),
        );
        assert!(first.errors.is_empty());
        assert!(second.errors.is_empty());
        assert_eq!(stats.creates.load(Ordering::SeqCst), 1);
        assert_eq!(first.snapshots["server"].tools.len(), 1);
        let connection = manager.connection("server").unwrap();
        let mut events = connection.subscribe();

        let filtered_servers: Map<String, Value> = serde_json::from_value(json!({
            "server": {
                "command":"fake",
                "timeout":1000,
                "discoveryTimeoutMs":1000,
                "includeTools":[]
            }
        }))
        .unwrap();
        let filtered = manager.discover_all(&filtered_servers).await;
        assert!(filtered.snapshots["server"].tools.is_empty());
        assert_eq!(stats.creates.load(Ordering::SeqCst), 1);
        assert!(matches!(
            events.recv().await.unwrap(),
            PoolEvent::ToolsChanged { snapshot, .. } if snapshot.is_empty()
        ));
        let args = Map::new();
        let (a, b) = tokio::join!(
            connection.call_tool("echo", &args, McpRequestOptions::default()),
            connection.call_tool("echo", &args, McpRequestOptions::default()),
        );
        assert!(a.is_ok() && b.is_ok());

        let other_session = pool
            .acquire(
                "server",
                json!({"command":"fake","timeout":1000,"discoveryTimeoutMs":1000}),
                "other-session",
            )
            .await
            .unwrap();
        assert_eq!(stats.max_active_calls.load(Ordering::SeqCst), 1);
        manager.stop();
        assert_eq!(
            connection
                .call_tool("echo", &args, McpRequestOptions::default())
                .await,
            Err(McpClientError::NotConnected)
        );
        assert!(
            other_session
                .call_tool("echo", &args, McpRequestOptions::default())
                .await
                .is_ok()
        );
        other_session.release();
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(pool.get_snapshot().by_name.is_empty());
    }
}
