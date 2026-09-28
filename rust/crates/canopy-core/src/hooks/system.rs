//! Native hook-system façade.
//!
//! Connects host-built [`HookEventPayload`] values to the configured registry,
//! event dispatcher, session hooks, and native runner adapter. Configuration
//! loading and application integration remain outside this crate module.

use std::convert::Infallible;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use serde_json::{Map, Value};

use super::aggregator::AggregatedHookResult;
use super::event_dispatch::HookEventDispatcher;
use super::event_inputs::HookEventPayload;
use super::native_dispatch_executor::NativeDispatchExecutor;
use super::planner::HookPlannerEntry;
use super::prompt_runner::PromptModelExecutor;
use super::registry::{HookRegistry, HookRegistryEntry};
use super::session_manager::SessionHooksManager;
use crate::utils::cancellation::CancellationToken;

#[derive(Clone, Debug, Default)]
struct HookSystemState {
    registry: HookRegistry,
    session_hooks: SessionHooksManager,
}

/// Coordinates event payloads and the initialized hook registries.
///
/// The host owns trusted-folder decisions, schema validation, provider setup,
/// event input construction, and telemetry. This façade snapshots registry
/// and session state before the async dispatch, so its lock is never held over
/// runner execution.
pub struct HookSystem<P: PromptModelExecutor + Send + Sync + 'static> {
    state: Arc<RwLock<HookSystemState>>,
    dispatcher: HookEventDispatcher,
    executor: Arc<NativeDispatchExecutor<P>>,
}

impl<P: PromptModelExecutor + Send + Sync + 'static> Clone for HookSystem<P> {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
            dispatcher: self.dispatcher,
            executor: Arc::clone(&self.executor),
        }
    }
}

impl<P: PromptModelExecutor + Send + Sync + 'static> HookSystem<P> {
    /// Create the façade around host-initialized registry state, session hooks,
    /// and runner adapters.
    pub fn new(
        registry: HookRegistry,
        session_hooks: SessionHooksManager,
        executor: Arc<NativeDispatchExecutor<P>>,
    ) -> Self {
        Self {
            state: Arc::new(RwLock::new(HookSystemState {
                registry,
                session_hooks,
            })),
            dispatcher: HookEventDispatcher::default(),
            executor,
        }
    }

    /// Dispatch one host-built event payload.
    ///
    /// `messages` is a per-event conversation snapshot supplied by the host;
    /// cancellation is forwarded to every runner through the event dispatcher.
    pub async fn execute_event(
        &self,
        payload: &HookEventPayload,
        messages: Option<Vec<Map<String, Value>>>,
        cancellation: Option<&CancellationToken>,
    ) -> AggregatedHookResult {
        let (registry_entries, session_hooks) = {
            let state = read_lock(&self.state);
            let registry_entries = state
                .registry
                .get_hooks_for_event(payload.event_name)
                .into_iter()
                .map(|entry| HookPlannerEntry {
                    config: entry.config,
                    matcher: entry.matcher,
                    sequential: entry.sequential,
                })
                .collect::<Vec<_>>();
            (registry_entries, state.session_hooks.clone())
        };

        self.dispatcher
            .execute(
                self.executor.as_ref(),
                &session_hooks,
                &registry_entries,
                payload.event_name,
                &payload.input,
                payload.matcher_context.as_ref(),
                messages,
                cancellation,
            )
            .await
    }

    /// Toggle all configured/ephemeral entries with this display name and
    /// return how many entries changed, matching `HookRegistry` semantics.
    pub fn set_hook_enabled(&self, hook_name: &str, enabled: bool) -> usize {
        let mut state = write_lock(&self.state);
        state.registry.set_hook_enabled(hook_name, enabled)
    }

    /// Replace configured entries after the host has loaded, trusted, and
    /// validated them. Existing enabled overrides and ephemeral agent entries
    /// are retained by the registry's transactional reload operation.
    pub fn reload_configured_hooks(&self, entries: Vec<HookRegistryEntry>) {
        let mut state = write_lock(&self.state);
        let result = state
            .registry
            .reload_configured_hooks(|| Ok::<Vec<HookRegistryEntry>, Infallible>(entries));
        if let Err(never) = result {
            match never {}
        }
    }

    /// Return a cloned registry snapshot for host inspection.
    pub fn registry_snapshot(&self) -> HookRegistry {
        read_lock(&self.state).registry.clone()
    }

    /// Return a cloned session-manager snapshot for host inspection.
    pub fn session_manager_snapshot(&self) -> SessionHooksManager {
        read_lock(&self.state).session_hooks.clone()
    }

    /// Run a short synchronous mutation against the session manager.
    ///
    /// The closure executes while the façade's write lock is held and must not
    /// block, re-enter this `HookSystem`, or carry references across an await.
    pub fn with_session_manager_mut<R>(
        &self,
        update: impl FnOnce(&mut SessionHooksManager) -> R,
    ) -> R {
        let mut state = write_lock(&self.state);
        update(&mut state.session_hooks)
    }
}

fn read_lock<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn write_lock<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    lock.write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
