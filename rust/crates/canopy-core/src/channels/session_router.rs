//! Channel session routing and crash recovery.
//!
//! This ports the stateful core of `packages/channels/base/src/SessionRouter.ts`.
//! The bridge trait keeps daemon creation/loading outside this module while
//! preserving per-route reservations and binding-token invalidation.

use super::{SessionTarget, sanitize::sanitize_log_text};
use indexmap::IndexMap;
use serde::Serialize;
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::watch;

pub type BridgeFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, String>> + Send + 'a>>;

/// Options passed to a channel bridge when a route creates or reloads a session.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SessionBridgeOptions {
    pub approval_mode: Option<String>,
    /// New sessions are attributed to their creating channel. Loads never
    /// re-stamp attribution on an existing session.
    pub source_id: Option<String>,
}

/// The narrow asynchronous seam between routing and an ACP/daemon bridge.
/// `binding_token` identifies the route operation that owns a prospective
/// bridge binding; stale results are discarded with that same token.
pub trait ChannelSessionBridge: Send + Sync + 'static {
    fn new_session<'a>(
        &'a self,
        cwd: &'a str,
        options: SessionBridgeOptions,
        binding_token: u64,
    ) -> BridgeFuture<'a, String>;

    fn load_session<'a>(
        &'a self,
        session_id: &'a str,
        cwd: &'a str,
        options: SessionBridgeOptions,
        binding_token: u64,
    ) -> BridgeFuture<'a, String>;

    fn discard_session<'a>(
        &'a self,
        _session_id: &'a str,
        _binding_token: u64,
    ) -> BridgeFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SessionScope {
    #[default]
    User,
    Thread,
    ChatThread,
    Single,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SessionRecoveryMode {
    #[default]
    Eager,
    Lazy,
}

#[derive(Clone, Debug, Default)]
pub struct SessionRouterOptions {
    pub persist_path: Option<PathBuf>,
    pub recovery_mode: SessionRecoveryMode,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionRouterError {
    message: Arc<str>,
    invalidated: bool,
}

impl SessionRouterError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: Arc::from(message.into()),
            invalidated: false,
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    fn is_invalidated(&self) -> bool {
        self.invalidated
    }

    fn invalidated() -> Self {
        Self {
            message: Arc::from("Session route operation was invalidated"),
            invalidated: true,
        }
    }
}

impl fmt::Display for SessionRouterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message())
    }
}

impl std::error::Error for SessionRouterError {}

pub type RouterResult<T> = Result<T, SessionRouterError>;
type OperationResult = Option<RouterResult<String>>;

#[derive(Clone, Debug)]
struct PersistedEntry {
    session_id: String,
    target: SessionTarget,
    cwd: String,
}

impl Serialize for PersistedEntry {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Entry<'a> {
            session_id: &'a str,
            target: &'a SessionTarget,
            cwd: &'a str,
        }
        Entry {
            session_id: &self.session_id,
            target: &self.target,
            cwd: &self.cwd,
        }
        .serialize(serializer)
    }
}

#[derive(Clone, Debug)]
struct SessionInput {
    channel_name: String,
    sender_id: String,
    chat_id: String,
    thread_id: Option<String>,
    cwd: String,
    is_group: Option<bool>,
}

impl SessionInput {
    fn target(&self) -> SessionTarget {
        SessionTarget {
            channel_name: self.channel_name.clone(),
            sender_id: self.sender_id.clone(),
            chat_id: self.chat_id.clone(),
            thread_id: self.thread_id.clone(),
            is_group: self.is_group,
            extra: Map::new(),
        }
    }
}

#[derive(Debug)]
struct SessionOperation {
    target: SessionTarget,
    lifecycle_generation: u64,
    route_token: u64,
    invalidated: AtomicBool,
    result_tx: watch::Sender<OperationResult>,
}

impl SessionOperation {
    fn new(target: SessionTarget, lifecycle_generation: u64, route_token: u64) -> Self {
        let (result_tx, _) = watch::channel(None);
        Self {
            target,
            lifecycle_generation,
            route_token,
            invalidated: AtomicBool::new(false),
            result_tx,
        }
    }

    fn invalidate(&self) {
        self.invalidated.store(true, Ordering::Release);
    }

    async fn wait(&self) -> RouterResult<String> {
        let mut result_rx = self.result_tx.subscribe();
        loop {
            if let Some(result) = result_rx.borrow().clone() {
                return result;
            }
            if result_rx.changed().await.is_err() {
                return Err(SessionRouterError::new(
                    "Session route operation ended without a result",
                ));
            }
        }
    }

    fn finish(&self, result: RouterResult<String>) {
        self.result_tx.send_replace(Some(result));
    }
}

#[derive(Default)]
struct RouterState {
    // TypeScript's Map preserves route insertion order in crash-state JSON.
    to_session: IndexMap<String, String>,
    to_target: HashMap<String, SessionTarget>,
    to_cwd: HashMap<String, String>,
    creating: HashMap<String, Arc<SessionOperation>>,
    session_load_windows: HashMap<u64, HashSet<String>>,
    live_session_ids: HashSet<String>,
    route_tokens: HashMap<String, u64>,
    lifecycle_generation: u64,
    channel_scopes: HashMap<String, SessionScope>,
    channel_approval_modes: HashMap<String, String>,
}

struct RouterInner {
    state: Mutex<RouterState>,
    bridge: RwLock<Arc<dyn ChannelSessionBridge>>,
    default_cwd: String,
    default_scope: SessionScope,
    persist_path: Option<PathBuf>,
    recovery_mode: SessionRecoveryMode,
    next_token: AtomicU64,
    next_load_window: AtomicU64,
}

/// Routes inbound channel identities to daemon sessions.
///
/// All synchronous state is protected by a short-held standard mutex; bridge
/// futures are always awaited after releasing it. A router clone shares the
/// same maps and in-flight reservations.
#[derive(Clone)]
pub struct SessionRouter {
    inner: Arc<RouterInner>,
}

impl SessionRouter {
    pub fn new(
        bridge: Arc<dyn ChannelSessionBridge>,
        default_cwd: impl Into<String>,
        default_scope: SessionScope,
        options: SessionRouterOptions,
    ) -> Self {
        Self {
            inner: Arc::new(RouterInner {
                state: Mutex::new(RouterState::default()),
                bridge: RwLock::new(bridge),
                default_cwd: default_cwd.into(),
                default_scope,
                persist_path: options.persist_path,
                recovery_mode: options.recovery_mode,
                next_token: AtomicU64::new(1),
                next_load_window: AtomicU64::new(1),
            }),
        }
    }

    pub fn set_bridge(&self, bridge: Arc<dyn ChannelSessionBridge>) {
        *self.inner.bridge.write().expect("bridge lock poisoned") = bridge;
    }

    pub fn set_channel_scope(&self, channel_name: impl Into<String>, scope: SessionScope) {
        self.lock_state()
            .channel_scopes
            .insert(channel_name.into(), scope);
    }

    pub fn set_channel_approval_mode(
        &self,
        channel_name: impl Into<String>,
        approval_mode: Option<String>,
    ) {
        let channel_name = channel_name.into();
        let mut state = self.lock_state();
        if let Some(approval_mode) = approval_mode.filter(|mode| !mode.is_empty()) {
            state
                .channel_approval_modes
                .insert(channel_name, approval_mode);
        } else {
            state.channel_approval_modes.remove(&channel_name);
        }
    }

    /// Resolve or create the session for one channel target.
    // Keep the source-compatible argument list: callers provide the complete
    // routing target as independent values.
    #[allow(clippy::too_many_arguments)]
    pub async fn resolve(
        &self,
        channel_name: impl Into<String>,
        sender_id: impl Into<String>,
        chat_id: impl Into<String>,
        thread_id: Option<String>,
        cwd: Option<String>,
        is_group: Option<bool>,
        routing_thread_id: Option<String>,
    ) -> RouterResult<String> {
        let input = SessionInput {
            channel_name: channel_name.into(),
            sender_id: sender_id.into(),
            chat_id: chat_id.into(),
            thread_id: thread_id.clone(),
            cwd: cwd
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| self.inner.default_cwd.clone()),
            is_group,
        };
        let key = self.routing_key(
            &input.channel_name,
            &input.sender_id,
            &input.chat_id,
            routing_thread_id.as_deref().or(thread_id.as_deref()),
        );
        let mut failed_waits = 0;
        loop {
            enum Choice {
                Return(String),
                Wait(Arc<SessionOperation>),
                Start(Arc<SessionOperation>, Option<String>),
            }
            let choice = {
                let mut state = self.lock_state();
                if let Some(session_id) = state.to_session.get(&key).cloned() {
                    if self.is_live(&state, &session_id) {
                        let promoted = promote_target_locked(&mut state, &session_id, is_group);
                        drop(state);
                        if promoted {
                            self.persist();
                        }
                        Choice::Return(session_id)
                    } else if let Some(operation) = state.creating.get(&key).cloned() {
                        Choice::Wait(operation)
                    } else {
                        let operation =
                            self.create_operation_locked(&mut state, &key, input.target());
                        state.creating.insert(key.clone(), operation.clone());
                        Choice::Start(operation, Some(session_id))
                    }
                } else if let Some(operation) = state.creating.get(&key).cloned() {
                    Choice::Wait(operation)
                } else {
                    let operation = self.create_operation_locked(&mut state, &key, input.target());
                    state.creating.insert(key.clone(), operation.clone());
                    Choice::Start(operation, None)
                }
            };
            match choice {
                Choice::Return(session_id) => return Ok(session_id),
                Choice::Start(operation, saved) => {
                    let inner = self.inner.clone();
                    let key_for_task = key.clone();
                    let input_for_task = input.clone();
                    let operation_for_task = operation.clone();
                    tokio::spawn(async move {
                        run_route_operation(
                            inner,
                            key_for_task,
                            input_for_task,
                            saved,
                            operation_for_task,
                        )
                        .await;
                    });
                    let session_id = operation.wait().await?;
                    self.assert_operation_result_current(&key, &session_id, &operation)?;
                    self.promote_target_to_group(&session_id, is_group);
                    return Ok(session_id);
                }
                Choice::Wait(operation) => match operation.wait().await {
                    Ok(session_id) => {
                        self.assert_operation_result_current(&key, &session_id, &operation)?;
                        self.promote_target_to_group(&session_id, is_group);
                        return Ok(session_id);
                    }
                    Err(error) if operation.invalidated.load(Ordering::Acquire) => {
                        return Err(error);
                    }
                    Err(error) => {
                        failed_waits += 1;
                        if failed_waits > 3 {
                            return Err(error);
                        }
                    }
                },
            }
        }
    }

    pub fn get_target(&self, session_id: &str) -> Option<SessionTarget> {
        self.lock_state().to_target.get(session_id).cloned()
    }

    pub fn get_session(
        &self,
        channel_name: &str,
        sender_id: &str,
        chat_id: &str,
        thread_id: Option<&str>,
    ) -> Option<String> {
        let key = self.routing_key(channel_name, sender_id, chat_id, thread_id);
        self.lock_state().to_session.get(&key).cloned()
    }

    pub fn has_session(
        &self,
        channel_name: &str,
        sender_id: &str,
        chat_id: Option<&str>,
        thread_id: Option<&str>,
    ) -> bool {
        let state = self.lock_state();
        if let Some(chat_id) = chat_id.filter(|value| !value.is_empty()) {
            let key = routing_key(
                &state,
                self.inner.default_scope,
                channel_name,
                sender_id,
                chat_id,
                thread_id,
            );
            return state.to_session.contains_key(&key);
        }
        if scope_for(&state, self.inner.default_scope, channel_name) == SessionScope::Single {
            return false;
        }
        state
            .to_target
            .values()
            .any(|target| target.channel_name == channel_name && target.sender_id == sender_id)
    }

    /// Remove one route or every route owned by a sender on a channel.
    pub fn remove_session(
        &self,
        channel_name: &str,
        sender_id: &str,
        chat_id: Option<&str>,
        thread_id: Option<&str>,
    ) -> Vec<String> {
        let removed_ids = {
            let mut state = self.lock_state();
            let scope = scope_for(&state, self.inner.default_scope, channel_name);
            let mut removed_ids = Vec::new();
            if let Some(chat_id) = chat_id.filter(|value| !value.is_empty()) {
                let key = routing_key(
                    &state,
                    self.inner.default_scope,
                    channel_name,
                    sender_id,
                    chat_id,
                    thread_id,
                );
                invalidate_route_locked(&mut state, &key);
                if let Some(session_id) = delete_by_key(&mut state, &key) {
                    removed_ids.push(session_id);
                }
            } else if scope != SessionScope::Single {
                let matching: Vec<(String, String)> = state
                    .to_session
                    .iter()
                    .filter_map(|(key, session_id)| {
                        let target = state.to_target.get(session_id)?;
                        (target.channel_name == channel_name && target.sender_id == sender_id)
                            .then(|| (key.clone(), session_id.clone()))
                    })
                    .collect();
                for (key, _) in matching {
                    invalidate_route_locked(&mut state, &key);
                    if let Some(session_id) = delete_by_key(&mut state, &key) {
                        removed_ids.push(session_id);
                    }
                }
                let inflight: Vec<String> = state
                    .creating
                    .iter()
                    .filter(|&(_, operation)| {
                        operation.target.channel_name == channel_name
                            && operation.target.sender_id == sender_id
                    })
                    .map(|(key, _)| key.clone())
                    .collect();
                for key in inflight {
                    invalidate_route_locked(&mut state, &key);
                }
            }
            removed_ids
        };
        if !removed_ids.is_empty() {
            self.persist();
        }
        removed_ids
    }

    pub fn remove_session_id(&self, session_id: &str) -> bool {
        let removed = {
            let mut state = self.lock_state();
            let matching_keys: Vec<String> = state
                .to_session
                .iter()
                .filter(|&(_, mapped)| mapped == session_id)
                .map(|(key, _)| key.clone())
                .collect();
            let mut removed = false;
            for key in matching_keys {
                invalidate_route_locked(&mut state, &key);
                state.to_session.shift_remove(&key);
                removed = true;
            }
            removed |= state.to_target.remove(session_id).is_some();
            removed |= state.to_cwd.remove(session_id).is_some();
            state.live_session_ids.remove(session_id);
            if !removed {
                for window in state.session_load_windows.values_mut() {
                    window.insert(session_id.to_owned());
                }
            }
            removed
        };
        if removed {
            self.persist();
        }
        removed
    }

    pub fn handle_session_died(&self, session_id: &str) -> bool {
        if self.inner.recovery_mode == SessionRecoveryMode::Eager {
            return self.remove_session_id(session_id);
        }
        let mut state = self.lock_state();
        let known = state.to_target.contains_key(session_id);
        state.live_session_ids.remove(session_id);
        for window in state.session_load_windows.values_mut() {
            window.insert(session_id.to_owned());
        }
        known
    }

    pub fn get_all(&self) -> Vec<(String, String, SessionTarget)> {
        let state = self.lock_state();
        state
            .to_session
            .iter()
            .filter_map(|(key, session_id)| {
                state
                    .to_target
                    .get(session_id)
                    .cloned()
                    .map(|target| (key.clone(), session_id.clone(), target))
            })
            .collect()
    }

    /// Restore route metadata without loading bridge sessions. This is valid
    /// only when `recovery_mode` is `Lazy`.
    pub fn restore_routes(&self) -> RouterResult<(usize, usize)> {
        if self.inner.recovery_mode != SessionRecoveryMode::Lazy {
            return Err(SessionRouterError::new(
                "restoreRoutes requires lazy recovery mode",
            ));
        }
        let Some((entries, dropped_keys)) = self.read_persisted_entries() else {
            return Ok((0, 0));
        };
        {
            let mut state = self.lock_state();
            dispose_locked(&mut state);
            for (key, entry) in &entries {
                state
                    .to_session
                    .insert(key.clone(), entry.session_id.clone());
                state
                    .to_target
                    .insert(entry.session_id.clone(), entry.target.clone());
                state
                    .to_cwd
                    .insert(entry.session_id.clone(), entry.cwd.clone());
            }
        }
        if !dropped_keys.is_empty() {
            self.persist();
        }
        Ok((entries.len(), dropped_keys.len()))
    }

    /// Eagerly ask the bridge to load each persisted session after restart.
    /// All route reservations are installed before the first bridge call so
    /// inbound messages wait for recovery instead of creating duplicates.
    pub async fn restore_sessions(&self) -> (usize, usize) {
        let Some((entries, dropped_keys)) = self.read_persisted_entries() else {
            return (0, 0);
        };
        let restore_generation = self.lock_state().lifecycle_generation;
        let reservations: Vec<(String, PersistedEntry, Arc<SessionOperation>)> = {
            let mut state = self.lock_state();
            for key in &dropped_keys {
                invalidate_route_locked(&mut state, key);
                delete_by_key(&mut state, key);
            }
            let mut reservations = Vec::with_capacity(entries.len());
            for (key, entry) in &entries {
                invalidate_route_locked(&mut state, key);
                delete_by_key(&mut state, key);
                let operation = self.create_operation_locked(&mut state, key, entry.target.clone());
                state.creating.insert(key.clone(), operation.clone());
                reservations.push((key.clone(), entry.clone(), operation));
            }
            reservations
        };
        let window_id = self.begin_load_window();
        let mut restored = 0;
        let mut failed = 0;
        let mut changed = !dropped_keys.is_empty();
        for (key, entry, operation) in reservations {
            let result = self.restore_one(&key, &entry, &operation, window_id).await;
            match &result {
                Ok(session_id) => {
                    restored += 1;
                    changed |= *session_id != entry.session_id;
                }
                Err(error) => {
                    failed += 1;
                    changed = true;
                    eprintln!(
                        "[SessionRouter] Failed to restore session {} for key {}: {}",
                        sanitize_log_text(&entry.session_id, 128),
                        sanitize_log_text(&key, 256),
                        sanitize_log_text(error.message(), 512),
                    );
                }
            }
            self.finish_operation(&key, &operation, result, false);
        }
        self.end_load_window(window_id);
        if changed && self.lock_state().lifecycle_generation == restore_generation {
            self.persist();
        }
        (restored, failed)
    }

    pub fn dispose(&self) {
        let mut state = self.lock_state();
        dispose_locked(&mut state);
    }

    /// Clear in-memory state and best-effort delete the persisted mapping.
    pub fn clear_all(&self) {
        self.dispose();
        if let Some(path) = &self.inner.persist_path {
            let _ = std::fs::remove_file(path);
        }
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, RouterState> {
        self.inner
            .state
            .lock()
            .expect("session router state poisoned")
    }

    fn routing_key(
        &self,
        channel_name: &str,
        sender_id: &str,
        chat_id: &str,
        thread_id: Option<&str>,
    ) -> String {
        let state = self.lock_state();
        routing_key(
            &state,
            self.inner.default_scope,
            channel_name,
            sender_id,
            chat_id,
            thread_id,
        )
    }

    fn is_live(&self, state: &RouterState, session_id: &str) -> bool {
        self.inner.recovery_mode == SessionRecoveryMode::Eager
            || state.live_session_ids.contains(session_id)
    }

    fn create_operation_locked(
        &self,
        state: &mut RouterState,
        key: &str,
        target: SessionTarget,
    ) -> Arc<SessionOperation> {
        let route_token = *state
            .route_tokens
            .entry(key.to_owned())
            .or_insert_with(|| self.inner.next_token.fetch_add(1, Ordering::Relaxed));
        Arc::new(SessionOperation::new(
            target,
            state.lifecycle_generation,
            route_token,
        ))
    }

    fn options_for(&self, channel_name: &str, source_id: Option<String>) -> SessionBridgeOptions {
        let approval_mode = self
            .lock_state()
            .channel_approval_modes
            .get(channel_name)
            .cloned();
        SessionBridgeOptions {
            approval_mode,
            source_id,
        }
    }

    fn assert_operation_result_current(
        &self,
        key: &str,
        session_id: &str,
        operation: &Arc<SessionOperation>,
    ) -> RouterResult<()> {
        let state = self.lock_state();
        if !operation_is_current(&state, key, operation)
            || state
                .to_session
                .get(key)
                .is_none_or(|stored| stored != session_id)
        {
            operation.invalidate();
            return Err(SessionRouterError::invalidated());
        }
        Ok(())
    }

    fn promote_target_to_group(&self, session_id: &str, is_group: Option<bool>) {
        let promoted = promote_target_locked(&mut self.lock_state(), session_id, is_group);
        if promoted {
            self.persist();
        }
    }

    fn begin_load_window(&self) -> u64 {
        let id = self.inner.next_load_window.fetch_add(1, Ordering::Relaxed);
        self.lock_state()
            .session_load_windows
            .insert(id, HashSet::new());
        id
    }

    fn end_load_window(&self, id: u64) {
        self.lock_state().session_load_windows.remove(&id);
    }

    async fn restore_one(
        &self,
        key: &str,
        entry: &PersistedEntry,
        operation: &Arc<SessionOperation>,
        window_id: u64,
    ) -> RouterResult<String> {
        self.assert_operation_current(key, operation)?;
        let bridge = self
            .inner
            .bridge
            .read()
            .expect("bridge lock poisoned")
            .clone();
        let options = self.options_for(&entry.target.channel_name, None);
        let session_id = bridge
            .load_session(
                &entry.session_id,
                &entry.cwd,
                options,
                operation.route_token,
            )
            .await
            .map_err(SessionRouterError::new)?;
        if session_id.is_empty() {
            return Err(SessionRouterError::new("Invalid restored session ID"));
        }
        let result = {
            let mut state = self.lock_state();
            if !operation_is_current(&state, key, operation) {
                Err(SessionRouterError::invalidated())
            } else if state
                .session_load_windows
                .get_mut(&window_id)
                .is_some_and(|window| window.remove(&session_id))
            {
                Err(SessionRouterError::new(
                    "Restored session died before routing completed",
                ))
            } else {
                state.to_session.insert(key.to_owned(), session_id.clone());
                state
                    .to_target
                    .insert(session_id.clone(), entry.target.clone());
                state.to_cwd.insert(session_id.clone(), entry.cwd.clone());
                state.live_session_ids.insert(session_id.clone());
                Ok(session_id.clone())
            }
        };
        if result.is_err() {
            self.schedule_discard(&session_id, operation);
        }
        result
    }

    fn assert_operation_current(
        &self,
        key: &str,
        operation: &Arc<SessionOperation>,
    ) -> RouterResult<()> {
        if operation_is_current(&self.lock_state(), key, operation) {
            Ok(())
        } else {
            operation.invalidate();
            Err(SessionRouterError::invalidated())
        }
    }

    fn finish_operation(
        &self,
        key: &str,
        operation: &Arc<SessionOperation>,
        mut result: RouterResult<String>,
        discard_invalid_result: bool,
    ) {
        let mut discard = None;
        {
            let mut state = self.lock_state();
            if let Ok(session_id) = &result {
                if !operation_is_current(&state, key, operation)
                    || state
                        .to_session
                        .get(key)
                        .is_none_or(|stored| stored != session_id)
                {
                    operation.invalidate();
                    if discard_invalid_result {
                        discard = Some(session_id.clone());
                    }
                    result = Err(SessionRouterError::invalidated());
                }
            }
            if state
                .creating
                .get(key)
                .is_some_and(|current| Arc::ptr_eq(current, operation))
            {
                state.creating.remove(key);
            }
            if state.route_tokens.get(key) == Some(&operation.route_token)
                && !state.to_session.contains_key(key)
                && !state.creating.contains_key(key)
            {
                state.route_tokens.remove(key);
            }
        }
        if let Some(session_id) = discard {
            self.schedule_discard(&session_id, operation);
        }
        operation.finish(result);
    }

    fn schedule_discard(&self, session_id: &str, operation: &Arc<SessionOperation>) {
        let shared_elsewhere = self
            .lock_state()
            .to_session
            .values()
            .any(|mapped| mapped == session_id);
        if shared_elsewhere {
            return;
        }
        let bridge = self
            .inner
            .bridge
            .read()
            .expect("bridge lock poisoned")
            .clone();
        let session_id = session_id.to_owned();
        let token = operation.route_token;
        tokio::spawn(async move {
            let _ = bridge.discard_session(&session_id, token).await;
        });
    }

    fn read_persisted_entries(&self) -> Option<(IndexMap<String, PersistedEntry>, Vec<String>)> {
        let path = self.inner.persist_path.as_ref()?;
        if !path.exists() {
            return None;
        }
        let parsed = match std::fs::read_to_string(path).and_then(|text| {
            serde_json::from_str::<Value>(&text)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
        }) {
            Ok(parsed) => parsed,
            Err(error) => {
                self.quarantine(path);
                eprintln!(
                    "[SessionRouter] Corrupted persist file at {}: {}",
                    sanitize_log_text(&path.to_string_lossy(), 1024),
                    sanitize_log_text(&error.to_string(), 512),
                );
                return None;
            }
        };
        let Some(object) = parsed.as_object() else {
            self.quarantine(path);
            eprintln!(
                "[SessionRouter] Invalid route store at {}: expected an object",
                sanitize_log_text(&path.to_string_lossy(), 1024),
            );
            return None;
        };
        let mut entries = IndexMap::new();
        let mut dropped_keys = Vec::new();
        for (key, value) in object {
            if let Some(entry) = parse_persisted_entry(value) {
                entries.insert(key.clone(), entry);
            } else {
                dropped_keys.push(key.clone());
            }
        }
        Some((entries, dropped_keys))
    }

    fn quarantine(&self, path: &Path) {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let quarantine = PathBuf::from(format!(
            "{}.corrupt-{timestamp}-{}",
            path.to_string_lossy(),
            uuid::Uuid::new_v4().simple()
        ));
        let _ = std::fs::rename(path, quarantine);
    }

    fn persist(&self) {
        let Some(path) = self.inner.persist_path.as_ref() else {
            return;
        };
        let entries: IndexMap<String, PersistedEntry> = {
            let state = self.lock_state();
            state
                .to_session
                .iter()
                .filter_map(|(key, session_id)| {
                    let target = state.to_target.get(session_id)?.clone();
                    Some((
                        key.clone(),
                        PersistedEntry {
                            session_id: session_id.clone(),
                            target,
                            cwd: state
                                .to_cwd
                                .get(session_id)
                                .cloned()
                                .unwrap_or_else(|| self.inner.default_cwd.clone()),
                        },
                    ))
                })
                .collect()
        };
        let result = (|| -> io::Result<()> {
            if let Some(parent) = path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
            {
                std::fs::create_dir_all(parent)?;
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))?;
                }
            }
            let bytes = serde_json::to_vec_pretty(&entries)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            let options = crate::utils::atomic_file_write::AtomicWriteOptions {
                mode: Some(0o600),
                force_mode: true,
                symlink_policy: crate::utils::atomic_file_write::SymlinkPolicy::NoFollow,
                ..Default::default()
            };
            crate::utils::atomic_file_write::atomic_write_file(path, &bytes, &options)
        })();
        if let Err(error) = result {
            eprintln!(
                "[SessionRouter] Failed to persist routes at {}: {}",
                sanitize_log_text(&path.to_string_lossy(), 1024),
                sanitize_log_text(&error.to_string(), 512),
            );
        }
    }
}

async fn run_route_operation(
    inner: Arc<RouterInner>,
    key: String,
    input: SessionInput,
    saved_session_id: Option<String>,
    operation: Arc<SessionOperation>,
) {
    let router = SessionRouter { inner };
    let result = if let Some(saved_session_id) = saved_session_id {
        router
            .load_or_replace(&key, &saved_session_id, &input, &operation)
            .await
    } else {
        router.create_and_store(&key, &input, &operation).await
    };
    router.finish_operation(&key, &operation, result, true);
}

impl SessionRouter {
    async fn create_and_store(
        &self,
        key: &str,
        input: &SessionInput,
        operation: &Arc<SessionOperation>,
    ) -> RouterResult<String> {
        let window_id = self.begin_load_window();
        let result = self
            .create_live_route(key, input, operation, window_id)
            .await;
        self.end_load_window(window_id);
        result
    }

    async fn load_or_replace(
        &self,
        key: &str,
        saved_session_id: &str,
        input: &SessionInput,
        operation: &Arc<SessionOperation>,
    ) -> RouterResult<String> {
        let saved_cwd = {
            let state = self.lock_state();
            state
                .to_cwd
                .get(saved_session_id)
                .cloned()
                .unwrap_or_else(|| input.cwd.clone())
        };
        let window_id = self.begin_load_window();
        let load_result = async {
            self.assert_operation_current(key, operation)?;
            let bridge = self
                .inner
                .bridge
                .read()
                .expect("bridge lock poisoned")
                .clone();
            let options = self.options_for(&input.channel_name, None);
            let loaded_id = bridge
                .load_session(saved_session_id, &saved_cwd, options, operation.route_token)
                .await
                .map_err(SessionRouterError::new)?;
            if loaded_id.is_empty() {
                return Err(SessionRouterError::new(
                    "Invalid or dead restored session ID",
                ));
            }
            let installed = {
                let mut state = self.lock_state();
                if !operation_is_current(&state, key, operation)
                    || state
                        .to_session
                        .get(key)
                        .is_none_or(|value| value != saved_session_id)
                {
                    operation.invalidate();
                    Err(SessionRouterError::invalidated())
                } else if state
                    .session_load_windows
                    .get_mut(&window_id)
                    .is_some_and(|window| window.remove(&loaded_id))
                {
                    Err(SessionRouterError::new(
                        "Invalid or dead restored session ID",
                    ))
                } else {
                    if loaded_id != saved_session_id {
                        let target = state.to_target.get(saved_session_id).cloned();
                        delete_by_key(&mut state, key);
                        state.to_session.insert(key.to_owned(), loaded_id.clone());
                        if let Some(target) = target {
                            state.to_target.insert(loaded_id.clone(), target);
                        }
                        state.to_cwd.insert(loaded_id.clone(), saved_cwd.clone());
                    }
                    state.live_session_ids.insert(loaded_id.clone());
                    Ok(loaded_id.clone())
                }
            };
            if installed.is_ok() {
                self.persist();
            } else if installed
                .as_ref()
                .is_err_and(SessionRouterError::is_invalidated)
            {
                self.schedule_discard(&loaded_id, operation);
            }
            installed
        }
        .await;
        match load_result {
            Ok(session_id) => {
                self.end_load_window(window_id);
                Ok(session_id)
            }
            Err(load_error) if load_error.is_invalidated() => {
                self.end_load_window(window_id);
                Err(load_error)
            }
            Err(load_error) => {
                let replacement = self
                    .create_live_route(key, input, operation, window_id)
                    .await;
                self.end_load_window(window_id);
                match replacement {
                    Ok(replacement) => {
                        eprintln!(
                            "[SessionRouter] Replaced unavailable session {} for key {} after load failed: {}",
                            sanitize_log_text(saved_session_id, 128),
                            sanitize_log_text(key, 256),
                            sanitize_log_text(load_error.message(), 512),
                        );
                        Ok(replacement)
                    }
                    Err(create_error) => {
                        self.assert_operation_current(key, operation)?;
                        eprintln!(
                            "[SessionRouter] Failed to load session {} for key {} ({}) and failed to create a replacement ({})",
                            sanitize_log_text(saved_session_id, 128),
                            sanitize_log_text(key, 256),
                            sanitize_log_text(load_error.message(), 512),
                            sanitize_log_text(create_error.message(), 512),
                        );
                        Err(create_error)
                    }
                }
            }
        }
    }

    async fn create_live_route(
        &self,
        key: &str,
        input: &SessionInput,
        operation: &Arc<SessionOperation>,
        window_id: u64,
    ) -> RouterResult<String> {
        let max_attempts = 2;
        let mut last_dead_session_id = None;
        for _attempt in 0..max_attempts {
            self.assert_operation_current(key, operation)?;
            let bridge = self
                .inner
                .bridge
                .read()
                .expect("bridge lock poisoned")
                .clone();
            let options = self.options_for(&input.channel_name, Some(input.channel_name.clone()));
            let session_id = bridge
                .new_session(&input.cwd, options, operation.route_token)
                .await
                .map_err(SessionRouterError::new)?;
            if session_id.is_empty() {
                return Err(SessionRouterError::new("Invalid session ID from bridge"));
            }
            let installed = {
                let mut state = self.lock_state();
                if !operation_is_current(&state, key, operation) {
                    operation.invalidate();
                    Err(SessionRouterError::invalidated())
                } else if state
                    .session_load_windows
                    .get_mut(&window_id)
                    .is_some_and(|window| window.remove(&session_id))
                {
                    last_dead_session_id = Some(session_id.clone());
                    Ok(None)
                } else {
                    if state.to_session.contains_key(key) {
                        delete_by_key(&mut state, key);
                    }
                    state.to_session.insert(key.to_owned(), session_id.clone());
                    state.to_target.insert(session_id.clone(), input.target());
                    state.to_cwd.insert(session_id.clone(), input.cwd.clone());
                    state.live_session_ids.insert(session_id.clone());
                    Ok(Some(session_id.clone()))
                }
            };
            match installed {
                Err(error) => {
                    self.schedule_discard(&session_id, operation);
                    return Err(error);
                }
                Ok(Some(session_id)) => {
                    self.persist();
                    return Ok(session_id);
                }
                Ok(None) => continue,
            }
        }
        Err(SessionRouterError::new(format!(
            "Session {} died before routing completed (2/2 attempts, key {})",
            last_dead_session_id.as_deref().unwrap_or("unknown"),
            key
        )))
    }
}

fn scope_for(state: &RouterState, default_scope: SessionScope, channel_name: &str) -> SessionScope {
    state
        .channel_scopes
        .get(channel_name)
        .copied()
        .unwrap_or(default_scope)
}

fn routing_key(
    state: &RouterState,
    default_scope: SessionScope,
    channel_name: &str,
    sender_id: &str,
    chat_id: &str,
    thread_id: Option<&str>,
) -> String {
    match scope_for(state, default_scope, channel_name) {
        SessionScope::Thread => format!(
            "{channel_name}:{}",
            thread_id.filter(|id| !id.is_empty()).unwrap_or(chat_id)
        ),
        SessionScope::ChatThread => match thread_id.filter(|id| !id.is_empty()) {
            Some(thread_id) => format!("{channel_name}:{chat_id}:{thread_id}"),
            None => format!("{channel_name}:{chat_id}"),
        },
        SessionScope::Single => format!("{channel_name}:__single__"),
        SessionScope::User => format!("{channel_name}:{sender_id}:{chat_id}"),
    }
}

fn operation_is_current(state: &RouterState, key: &str, operation: &SessionOperation) -> bool {
    !operation.invalidated.load(Ordering::Acquire)
        && operation.lifecycle_generation == state.lifecycle_generation
        && state.route_tokens.get(key) == Some(&operation.route_token)
}

fn invalidate_route_locked(state: &mut RouterState, key: &str) {
    state.route_tokens.remove(key);
    if let Some(operation) = state.creating.remove(key) {
        operation.invalidate();
    }
}

fn delete_by_key(state: &mut RouterState, key: &str) -> Option<String> {
    let session_id = state.to_session.shift_remove(key)?;
    state.to_target.remove(&session_id);
    state.to_cwd.remove(&session_id);
    state.live_session_ids.remove(&session_id);
    Some(session_id)
}

fn promote_target_locked(
    state: &mut RouterState,
    session_id: &str,
    is_group: Option<bool>,
) -> bool {
    if is_group != Some(true) {
        return false;
    }
    let Some(target) = state.to_target.get_mut(session_id) else {
        return false;
    };
    if target.is_group == Some(true) {
        return false;
    }
    target.is_group = Some(true);
    true
}

fn dispose_locked(state: &mut RouterState) {
    state.lifecycle_generation = state.lifecycle_generation.wrapping_add(1);
    for operation in state.creating.values() {
        operation.invalidate();
    }
    state.to_session.clear();
    state.to_target.clear();
    state.to_cwd.clear();
    state.creating.clear();
    state.session_load_windows.clear();
    state.live_session_ids.clear();
    state.route_tokens.clear();
}

fn parse_persisted_entry(value: &Value) -> Option<PersistedEntry> {
    let object = value.as_object()?;
    let session_id = object.get("sessionId")?.as_str()?;
    if session_id.is_empty() {
        return None;
    }
    let cwd = object.get("cwd")?.as_str()?;
    if cwd.is_empty() {
        return None;
    }
    let target_object = object.get("target")?.as_object()?;
    let channel_name = target_object.get("channelName")?.as_str()?.to_owned();
    let sender_id = target_object.get("senderId")?.as_str()?.to_owned();
    let chat_id = target_object.get("chatId")?.as_str()?.to_owned();
    let thread_id = match target_object.get("threadId") {
        None => None,
        Some(Value::String(value)) => Some(value.clone()),
        Some(_) => return None,
    };
    let is_group = match target_object.get("isGroup") {
        None => None,
        Some(Value::Bool(value)) => Some(*value),
        Some(_) => return None,
    };
    let known = ["channelName", "senderId", "chatId", "threadId", "isGroup"];
    let extra = target_object
        .iter()
        .filter(|(key, _)| !known.contains(&key.as_str()))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    Some(PersistedEntry {
        session_id: session_id.to_owned(),
        target: SessionTarget {
            channel_name,
            sender_id,
            chat_id,
            thread_id,
            is_group,
            extra,
        },
        cwd: cwd.to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Notify;

    type NewSessionCallback = Arc<dyn Fn(&str) + Send + Sync>;

    #[derive(Default)]
    struct FakeState {
        next_id: usize,
        new_results: VecDeque<Result<String, String>>,
        load_results: VecDeque<Result<String, String>>,
        new_calls: Vec<(String, SessionBridgeOptions, u64)>,
        load_calls: Vec<(String, String, SessionBridgeOptions, u64)>,
        discarded: Vec<(String, u64)>,
    }

    #[derive(Default)]
    struct FakeBridge {
        state: Mutex<FakeState>,
        new_started: Option<Arc<Notify>>,
        new_continue: Option<Arc<Notify>>,
        load_started: Option<Arc<Notify>>,
        load_continue: Option<Arc<Notify>>,
        on_new_return: Mutex<Option<NewSessionCallback>>,
        call_count: AtomicUsize,
    }

    impl FakeBridge {
        fn boxed() -> Arc<dyn ChannelSessionBridge> {
            Arc::new(Self::default())
        }

        fn with_new_gate() -> (Arc<Self>, Arc<Notify>, Arc<Notify>) {
            let started = Arc::new(Notify::new());
            let continue_gate = Arc::new(Notify::new());
            (
                Arc::new(Self {
                    new_started: Some(started.clone()),
                    new_continue: Some(continue_gate.clone()),
                    ..Self::default()
                }),
                started,
                continue_gate,
            )
        }

        fn with_load_gate() -> (Arc<Self>, Arc<Notify>, Arc<Notify>) {
            let started = Arc::new(Notify::new());
            let continue_gate = Arc::new(Notify::new());
            (
                Arc::new(Self {
                    load_started: Some(started.clone()),
                    load_continue: Some(continue_gate.clone()),
                    ..Self::default()
                }),
                started,
                continue_gate,
            )
        }

        fn push_new(&self, value: Result<&str, &str>) {
            self.state
                .lock()
                .unwrap()
                .new_results
                .push_back(value.map(str::to_owned).map_err(str::to_owned));
        }

        fn push_load(&self, value: Result<&str, &str>) {
            self.state
                .lock()
                .unwrap()
                .load_results
                .push_back(value.map(str::to_owned).map_err(str::to_owned));
        }
    }

    impl ChannelSessionBridge for FakeBridge {
        fn new_session<'a>(
            &'a self,
            cwd: &'a str,
            options: SessionBridgeOptions,
            binding_token: u64,
        ) -> BridgeFuture<'a, String> {
            Box::pin(async move {
                self.call_count.fetch_add(1, Ordering::Relaxed);
                let result = {
                    let mut state = self.state.lock().unwrap();
                    state
                        .new_calls
                        .push((cwd.to_owned(), options, binding_token));
                    state.new_results.pop_front().unwrap_or_else(|| {
                        state.next_id += 1;
                        Ok(format!("session-{}", state.next_id))
                    })
                };
                if let (Some(started), Some(continue_gate)) =
                    (&self.new_started, &self.new_continue)
                {
                    started.notify_one();
                    continue_gate.notified().await;
                }
                if let (Ok(session_id), Some(on_new_return)) =
                    (&result, self.on_new_return.lock().unwrap().clone())
                {
                    on_new_return(session_id);
                }
                result
            })
        }

        fn load_session<'a>(
            &'a self,
            session_id: &'a str,
            cwd: &'a str,
            options: SessionBridgeOptions,
            binding_token: u64,
        ) -> BridgeFuture<'a, String> {
            Box::pin(async move {
                let result = {
                    let mut state = self.state.lock().unwrap();
                    state.load_calls.push((
                        session_id.to_owned(),
                        cwd.to_owned(),
                        options,
                        binding_token,
                    ));
                    state
                        .load_results
                        .pop_front()
                        .unwrap_or_else(|| Ok(session_id.to_owned()))
                };
                if let (Some(started), Some(continue_gate)) =
                    (&self.load_started, &self.load_continue)
                {
                    started.notify_one();
                    continue_gate.notified().await;
                }
                result
            })
        }

        fn discard_session<'a>(
            &'a self,
            session_id: &'a str,
            binding_token: u64,
        ) -> BridgeFuture<'a, ()> {
            Box::pin(async move {
                self.state
                    .lock()
                    .unwrap()
                    .discarded
                    .push((session_id.to_owned(), binding_token));
                Ok(())
            })
        }
    }

    fn router(bridge: Arc<dyn ChannelSessionBridge>) -> SessionRouter {
        SessionRouter::new(
            bridge,
            "/tmp",
            SessionScope::User,
            SessionRouterOptions::default(),
        )
    }

    fn route_entry(session_id: &str, key_target: SessionTarget) -> PersistedEntry {
        PersistedEntry {
            session_id: session_id.to_owned(),
            target: key_target,
            cwd: "/tmp".to_owned(),
        }
    }

    fn target(channel: &str, sender: &str, chat: &str) -> SessionTarget {
        SessionTarget {
            channel_name: channel.to_owned(),
            sender_id: sender.to_owned(),
            chat_id: chat.to_owned(),
            thread_id: None,
            is_group: None,
            extra: Map::new(),
        }
    }

    #[tokio::test]
    async fn routing_scopes_and_group_promotion_match_channel_targets() {
        let bridge = FakeBridge::boxed();
        let router = SessionRouter::new(
            bridge,
            "/tmp",
            SessionScope::Thread,
            SessionRouterOptions::default(),
        );
        let first = router
            .resolve(
                "ch",
                "alice",
                "chat",
                Some("thread".into()),
                None,
                Some(false),
                None,
            )
            .await
            .unwrap();
        let second = router
            .resolve(
                "ch",
                "bob",
                "elsewhere",
                Some("thread".into()),
                None,
                Some(true),
                None,
            )
            .await
            .unwrap();
        assert_eq!(first, second);
        assert_eq!(router.get_target(&first).unwrap().sender_id, "alice");
        assert_eq!(router.get_target(&first).unwrap().is_group, Some(true));
        assert!(router.has_session("ch", "alice", None, None));
        assert!(!router.has_session("ch", "bob", None, None));
    }

    #[tokio::test]
    async fn concurrent_resolves_share_one_creation_reservation() {
        let (bridge, started, continue_gate) = FakeBridge::with_new_gate();
        let router = router(bridge.clone());
        let first_router = router.clone();
        let first = tokio::spawn(async move {
            first_router
                .resolve("ch", "alice", "chat", None, None, None, None)
                .await
        });
        started.notified().await;
        let second_router = router.clone();
        let second = tokio::spawn(async move {
            second_router
                .resolve("ch", "alice", "chat", None, None, None, None)
                .await
        });
        tokio::task::yield_now().await;
        continue_gate.notify_one();
        let first = first.await.unwrap().unwrap();
        let second = second.await.unwrap().unwrap();
        assert_eq!(first, second);
        assert_eq!(bridge.call_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn removing_a_route_invalidates_and_discards_late_bridge_creation() {
        let (bridge, started, continue_gate) = FakeBridge::with_new_gate();
        let router = router(bridge.clone());
        let pending_router = router.clone();
        let pending = tokio::spawn(async move {
            pending_router
                .resolve("ch", "alice", "chat", None, None, None, None)
                .await
        });
        started.notified().await;
        assert!(
            router
                .remove_session("ch", "alice", Some("chat"), None)
                .is_empty()
        );
        continue_gate.notify_one();
        let error = pending.await.unwrap().unwrap_err();
        assert_eq!(error.message(), "Session route operation was invalidated");
        assert!(router.get_all().is_empty());
        tokio::task::yield_now().await;
        assert_eq!(bridge.state.lock().unwrap().discarded.len(), 1);
    }

    #[tokio::test]
    async fn retries_creation_when_death_arrives_during_the_load_window() {
        let concrete = Arc::new(FakeBridge::default());
        concrete.push_new(Ok("died"));
        concrete.push_new(Ok("live"));
        let bridge: Arc<dyn ChannelSessionBridge> = concrete.clone();
        let router = router(bridge);
        let death_router = router.clone();
        *concrete.on_new_return.lock().unwrap() = Some(Arc::new(move |session_id| {
            if session_id == "died" {
                death_router.handle_session_died(session_id);
            }
        }));
        assert_eq!(
            router
                .resolve("ch", "a", "c", None, None, None, None)
                .await
                .unwrap(),
            "live"
        );
        assert_eq!(concrete.state.lock().unwrap().new_calls.len(), 2);
    }

    #[tokio::test]
    async fn lazy_recovery_loads_dormant_routes_once_and_replaces_unavailable_sessions() {
        let dir = std::env::temp_dir().join(format!("router-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let persist_path = dir.join("routes.json");
        let entries = IndexMap::from([(
            "ch:alice:chat".to_owned(),
            route_entry("old", target("ch", "alice", "chat")),
        )]);
        std::fs::write(&persist_path, serde_json::to_vec(&entries).unwrap()).unwrap();
        let concrete = Arc::new(FakeBridge::default());
        let bridge: Arc<dyn ChannelSessionBridge> = concrete.clone();
        concrete.push_load(Ok("replacement"));
        let router = SessionRouter::new(
            bridge,
            "/tmp",
            SessionScope::User,
            SessionRouterOptions {
                persist_path: Some(persist_path.clone()),
                recovery_mode: SessionRecoveryMode::Lazy,
            },
        );
        assert_eq!(router.restore_routes().unwrap(), (1, 0));
        assert_eq!(
            router.get_session("ch", "alice", "chat", None).as_deref(),
            Some("old")
        );
        assert_eq!(
            router
                .resolve("ch", "alice", "chat", None, None, None, None)
                .await
                .unwrap(),
            "replacement"
        );
        assert_eq!(
            router
                .resolve("ch", "alice", "chat", None, None, None, None)
                .await
                .unwrap(),
            "replacement"
        );
        assert_eq!(concrete.state.lock().unwrap().load_calls.len(), 1);
        let persisted: Value =
            serde_json::from_slice(&std::fs::read(&persist_path).unwrap()).unwrap();
        assert_eq!(persisted["ch:alice:chat"]["sessionId"], "replacement");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn concurrent_resolves_share_one_dormant_load() {
        let dir = std::env::temp_dir().join(format!("router-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let persist_path = dir.join("routes.json");
        let entries = IndexMap::from([(
            "ch:alice:chat".to_owned(),
            route_entry("old", target("ch", "alice", "chat")),
        )]);
        std::fs::write(&persist_path, serde_json::to_vec(&entries).unwrap()).unwrap();
        let (concrete, started, continue_gate) = FakeBridge::with_load_gate();
        let bridge: Arc<dyn ChannelSessionBridge> = concrete.clone();
        let router = SessionRouter::new(
            bridge,
            "/tmp",
            SessionScope::User,
            SessionRouterOptions {
                persist_path: Some(persist_path),
                recovery_mode: SessionRecoveryMode::Lazy,
            },
        );
        router.restore_routes().unwrap();
        let first_router = router.clone();
        let first = tokio::spawn(async move {
            first_router
                .resolve("ch", "alice", "chat", None, None, None, None)
                .await
        });
        started.notified().await;
        let second_router = router.clone();
        let second = tokio::spawn(async move {
            second_router
                .resolve("ch", "alice", "chat", None, None, None, None)
                .await
        });
        tokio::task::yield_now().await;
        continue_gate.notify_one();
        assert_eq!(first.await.unwrap().unwrap(), "old");
        assert_eq!(second.await.unwrap().unwrap(), "old");
        assert_eq!(concrete.state.lock().unwrap().load_calls.len(), 1);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn lazy_load_failure_keeps_the_dormant_mapping_if_fallback_fails() {
        let dir = std::env::temp_dir().join(format!("router-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let persist_path = dir.join("routes.json");
        let entries = IndexMap::from([(
            "ch:alice:chat".to_owned(),
            route_entry("old", target("ch", "alice", "chat")),
        )]);
        std::fs::write(&persist_path, serde_json::to_vec(&entries).unwrap()).unwrap();
        let concrete = Arc::new(FakeBridge::default());
        concrete.push_load(Err("temporarily gone"));
        concrete.push_new(Err("at capacity"));
        let bridge: Arc<dyn ChannelSessionBridge> = concrete.clone();
        let router = SessionRouter::new(
            bridge,
            "/tmp",
            SessionScope::User,
            SessionRouterOptions {
                persist_path: Some(persist_path.clone()),
                recovery_mode: SessionRecoveryMode::Lazy,
            },
        );
        router.restore_routes().unwrap();
        assert_eq!(
            router
                .resolve("ch", "alice", "chat", None, None, None, None)
                .await
                .unwrap_err()
                .message(),
            "at capacity"
        );
        assert_eq!(
            router.get_session("ch", "alice", "chat", None).as_deref(),
            Some("old")
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn unavailable_dormant_route_is_replaced_only_after_fallback_succeeds() {
        let dir = std::env::temp_dir().join(format!("router-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let persist_path = dir.join("routes.json");
        let entries = IndexMap::from([(
            "ch:alice:chat".to_owned(),
            route_entry("old", target("ch", "alice", "chat")),
        )]);
        std::fs::write(&persist_path, serde_json::to_vec(&entries).unwrap()).unwrap();
        let concrete = Arc::new(FakeBridge::default());
        concrete.push_load(Err("gone"));
        concrete.push_new(Ok("replacement"));
        let bridge: Arc<dyn ChannelSessionBridge> = concrete.clone();
        let router = SessionRouter::new(
            bridge,
            "/tmp",
            SessionScope::User,
            SessionRouterOptions {
                persist_path: Some(persist_path.clone()),
                recovery_mode: SessionRecoveryMode::Lazy,
            },
        );
        router.restore_routes().unwrap();
        assert_eq!(
            router
                .resolve("ch", "alice", "chat", None, None, None, None)
                .await
                .unwrap(),
            "replacement"
        );
        assert_eq!(
            concrete.state.lock().unwrap().new_calls[0]
                .1
                .source_id
                .as_deref(),
            Some("ch")
        );
        let persisted: Value =
            serde_json::from_slice(&std::fs::read(&persist_path).unwrap()).unwrap();
        assert_eq!(persisted["ch:alice:chat"]["sessionId"], "replacement");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn restore_reserves_all_routes_before_loading_and_drops_failed_routes() {
        let dir = std::env::temp_dir().join(format!("router-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let persist_path = dir.join("routes.json");
        let entries = IndexMap::from([
            (
                "ch:alice:one".to_owned(),
                route_entry("old-a", target("ch", "alice", "one")),
            ),
            (
                "ch:bob:two".to_owned(),
                route_entry("old-b", target("ch", "bob", "two")),
            ),
        ]);
        std::fs::write(&persist_path, serde_json::to_vec(&entries).unwrap()).unwrap();
        let concrete = Arc::new(FakeBridge::default());
        concrete.push_load(Ok("new-a"));
        concrete.push_load(Err("gone"));
        let bridge: Arc<dyn ChannelSessionBridge> = concrete.clone();
        let router = SessionRouter::new(
            bridge,
            "/tmp",
            SessionScope::User,
            SessionRouterOptions {
                persist_path: Some(persist_path.clone()),
                recovery_mode: SessionRecoveryMode::Eager,
            },
        );
        assert_eq!(router.restore_sessions().await, (1, 1));
        assert_eq!(
            router.get_session("ch", "alice", "one", None).as_deref(),
            Some("new-a")
        );
        assert_eq!(router.get_session("ch", "bob", "two", None), None);
        let persisted: Value =
            serde_json::from_slice(&std::fs::read(&persist_path).unwrap()).unwrap();
        assert!(persisted.get("ch:bob:two").is_none());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn resolve_waits_on_restore_reservation_instead_of_creating_a_duplicate() {
        let dir = std::env::temp_dir().join(format!("router-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let persist_path = dir.join("routes.json");
        let entries = IndexMap::from([(
            "ch:alice:chat".to_owned(),
            route_entry("old", target("ch", "alice", "chat")),
        )]);
        std::fs::write(&persist_path, serde_json::to_vec(&entries).unwrap()).unwrap();
        let (concrete, started, continue_gate) = FakeBridge::with_load_gate();
        let bridge: Arc<dyn ChannelSessionBridge> = concrete.clone();
        let router = SessionRouter::new(
            bridge,
            "/tmp",
            SessionScope::User,
            SessionRouterOptions {
                persist_path: Some(persist_path),
                recovery_mode: SessionRecoveryMode::Eager,
            },
        );
        let restore_router = router.clone();
        let restore = tokio::spawn(async move { restore_router.restore_sessions().await });
        started.notified().await;
        let resolve_router = router.clone();
        let resolving = tokio::spawn(async move {
            resolve_router
                .resolve("ch", "alice", "chat", None, None, None, None)
                .await
        });
        tokio::task::yield_now().await;
        continue_gate.notify_one();
        assert_eq!(restore.await.unwrap(), (1, 0));
        assert_eq!(resolving.await.unwrap().unwrap(), "old");
        assert!(concrete.state.lock().unwrap().new_calls.is_empty());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn restore_reserves_every_persisted_route_before_the_first_bridge_load() {
        let dir = std::env::temp_dir().join(format!("router-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let persist_path = dir.join("routes.json");
        let entries = IndexMap::from([
            (
                "ch:alice:one".to_owned(),
                route_entry("old-a", target("ch", "alice", "one")),
            ),
            (
                "ch:bob:two".to_owned(),
                route_entry("old-b", target("ch", "bob", "two")),
            ),
        ]);
        std::fs::write(&persist_path, serde_json::to_vec(&entries).unwrap()).unwrap();
        let (concrete, started, continue_gate) = FakeBridge::with_load_gate();
        let bridge: Arc<dyn ChannelSessionBridge> = concrete.clone();
        let router = SessionRouter::new(
            bridge,
            "/tmp",
            SessionScope::User,
            SessionRouterOptions {
                persist_path: Some(persist_path),
                recovery_mode: SessionRecoveryMode::Eager,
            },
        );
        let restore_router = router.clone();
        let restore = tokio::spawn(async move { restore_router.restore_sessions().await });
        started.notified().await;
        let resolve_router = router.clone();
        let bob = tokio::spawn(async move {
            resolve_router
                .resolve("ch", "bob", "two", None, None, None, None)
                .await
        });
        tokio::task::yield_now().await;
        assert!(concrete.state.lock().unwrap().new_calls.is_empty());
        continue_gate.notify_one();
        started.notified().await;
        continue_gate.notify_one();
        assert_eq!(restore.await.unwrap(), (2, 0));
        assert_eq!(bob.await.unwrap().unwrap(), "old-b");
        let calls = concrete.state.lock().unwrap();
        assert_eq!(calls.load_calls.len(), 2);
        assert!(calls.new_calls.is_empty());
        drop(calls);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn bridge_replacement_and_channel_approval_apply_to_new_routes() {
        let first = Arc::new(FakeBridge::default());
        let second = Arc::new(FakeBridge::default());
        second.push_new(Ok("from-replacement"));
        let first_bridge: Arc<dyn ChannelSessionBridge> = first.clone();
        let second_bridge: Arc<dyn ChannelSessionBridge> = second.clone();
        let router = router(first_bridge);
        router.set_bridge(second_bridge);
        router.set_channel_approval_mode("telegram", Some("yolo".to_owned()));
        assert_eq!(
            router
                .resolve("telegram", "alice", "chat", None, None, None, None)
                .await
                .unwrap(),
            "from-replacement"
        );
        assert!(first.state.lock().unwrap().new_calls.is_empty());
        assert_eq!(
            second.state.lock().unwrap().new_calls[0].1,
            SessionBridgeOptions {
                approval_mode: Some("yolo".to_owned()),
                source_id: Some("telegram".to_owned()),
            }
        );
    }

    #[tokio::test]
    async fn persistence_uses_private_atomic_replacement_and_clear_removes_the_file() {
        let dir = std::env::temp_dir().join(format!("router-test-{}", uuid::Uuid::new_v4()));
        let persist_path = dir.join("nested").join("routes.json");
        let router = SessionRouter::new(
            FakeBridge::boxed(),
            "/tmp",
            SessionScope::User,
            SessionRouterOptions {
                persist_path: Some(persist_path.clone()),
                recovery_mode: SessionRecoveryMode::Eager,
            },
        );
        router
            .resolve("ch", "alice", "chat", None, None, None, None)
            .await
            .unwrap();
        router
            .resolve("ch", "bob", "chat", None, None, None, None)
            .await
            .unwrap();
        let persisted: Value =
            serde_json::from_slice(&std::fs::read(&persist_path).unwrap()).unwrap();
        assert_eq!(persisted["ch:alice:chat"]["sessionId"], "session-1");
        assert_eq!(
            persisted
                .as_object()
                .unwrap()
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            ["ch:alice:chat", "ch:bob:chat"]
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&persist_path)
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
            assert_eq!(
                std::fs::metadata(persist_path.parent().unwrap())
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o700
            );
        }
        router.clear_all();
        assert!(!persist_path.exists());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn malformed_persisted_route_file_is_quarantined_without_failing_startup() {
        let dir = std::env::temp_dir().join(format!("router-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let persist_path = dir.join("routes.json");
        std::fs::write(&persist_path, "{bad").unwrap();
        let router = SessionRouter::new(
            FakeBridge::boxed(),
            "/tmp",
            SessionScope::User,
            SessionRouterOptions {
                persist_path: Some(persist_path.clone()),
                recovery_mode: SessionRecoveryMode::Lazy,
            },
        );
        assert_eq!(router.restore_routes().unwrap(), (0, 0));
        assert!(!persist_path.exists());
        assert!(
            std::fs::read_dir(&dir)
                .unwrap()
                .filter_map(Result::ok)
                .any(|entry| entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with("routes.json.corrupt-"))
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn persisted_target_validation_drops_invalid_entries_and_preserves_extra_fields() {
        let valid = serde_json::json!({
            "sessionId": "s",
            "cwd": "/tmp",
            "target": {"channelName":"ch", "senderId":"u", "chatId":"c", "futureField": 7}
        });
        let entry = parse_persisted_entry(&valid).unwrap();
        assert_eq!(entry.target.extra["futureField"], 7);
        assert!(
            parse_persisted_entry(&serde_json::json!({"sessionId":"", "cwd":"/tmp", "target":{}}))
                .is_none()
        );
        assert!(
            parse_persisted_entry(&serde_json::json!({
                "sessionId":"s", "cwd":"/tmp",
                "target":{"channelName":"ch", "senderId":"u", "chatId":"c", "isGroup":null}
            }))
            .is_none()
        );
    }

    #[tokio::test]
    async fn lazy_session_death_keeps_route_metadata_and_forces_a_reload() {
        let dir = std::env::temp_dir().join(format!("router-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let persist_path = dir.join("routes.json");
        let entries = IndexMap::from([(
            "ch:alice:chat".to_owned(),
            route_entry("old", target("ch", "alice", "chat")),
        )]);
        std::fs::write(&persist_path, serde_json::to_vec(&entries).unwrap()).unwrap();
        let concrete = Arc::new(FakeBridge::default());
        let bridge: Arc<dyn ChannelSessionBridge> = concrete.clone();
        let router = SessionRouter::new(
            bridge,
            "/tmp",
            SessionScope::User,
            SessionRouterOptions {
                persist_path: Some(persist_path),
                recovery_mode: SessionRecoveryMode::Lazy,
            },
        );
        router.restore_routes().unwrap();
        assert!(router.handle_session_died("old"));
        assert_eq!(
            router.get_session("ch", "alice", "chat", None).as_deref(),
            Some("old")
        );
        assert_eq!(
            router
                .resolve("ch", "alice", "chat", None, None, None, None)
                .await
                .unwrap(),
            "old"
        );
        assert_eq!(concrete.state.lock().unwrap().load_calls.len(), 1);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn invalidation_tokens_are_released_after_removed_and_failed_routes() {
        let concrete = Arc::new(FakeBridge::default());
        let bridge: Arc<dyn ChannelSessionBridge> = concrete.clone();
        let router = router(bridge);
        for index in 0..20 {
            router.remove_session("ch", &format!("missing-{index}"), Some("chat"), None);
        }
        for index in 0..20 {
            let sender = format!("complete-{index}");
            router
                .resolve("ch", &sender, "chat", None, None, None, None)
                .await
                .unwrap();
            router.remove_session("ch", &sender, Some("chat"), None);
        }
        for index in 0..20 {
            concrete.push_new(Err("unavailable"));
            assert!(
                router
                    .resolve(
                        "ch",
                        &format!("failed-{index}"),
                        "chat",
                        None,
                        None,
                        None,
                        None
                    )
                    .await
                    .is_err()
            );
        }
        let state = router.lock_state();
        assert!(state.route_tokens.is_empty());
        assert!(state.to_session.is_empty());
    }
}
