//! Informational InstructionsLoaded callback for memory discovery.
//!
//! Port of `packages/core/src/hooks/instructionsLoadedCallback.ts`. The
//! callback checks configured and session hooks before dispatch, and discards
//! event output because loaded-instruction notifications do not gate discovery.

use std::marker::PhantomData;

use serde::{Deserialize, Serialize};

use super::event_inputs::HookBaseInput;
use super::planner::HookEventName;
use super::prompt_runner::PromptModelExecutor;
use super::system::HookSystem;
use super::system_events::HookEventExecutionOptions;

/// Instruction-file notification emitted by memory discovery.
///
/// `memory_type` values are `user`, `project`, `local`, or `extension`;
/// `load_reason` values are `session_start`, `include`, or `refresh`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InstructionsLoadedNotification {
    pub file_path: String,
    pub memory_type: String,
    pub load_reason: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trigger_file_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_file_path: Option<String>,
}

/// Callback factory that resolves the current hook system for each
/// notification, matching the TypeScript `getHookSystem` closure.
pub fn create_instructions_loaded_callback<
    P: PromptModelExecutor + Send + Sync + 'static,
    F: Fn() -> Option<HookSystem<P>> + Send + Sync,
>(
    get_hook_system: F,
) -> InstructionsLoadedCallback<P, F> {
    InstructionsLoadedCallback {
        get_hook_system,
        marker: PhantomData,
    }
}

/// Created callback for informational InstructionsLoaded notifications.
pub struct InstructionsLoadedCallback<P, F> {
    get_hook_system: F,
    marker: PhantomData<fn() -> P>,
}

impl<P: PromptModelExecutor + Send + Sync + 'static, F: Fn() -> Option<HookSystem<P>> + Send + Sync>
    InstructionsLoadedCallback<P, F>
{
    /// Notify the current hook system when an InstructionsLoaded hook exists.
    ///
    /// The host passes base event data and function-hook messages/cancellation
    /// for this notification. If neither configured nor session hooks are
    /// enabled, this returns before building or dispatching the event. Any
    /// hook output is intentionally discarded.
    pub async fn notify(
        &self,
        base: &HookBaseInput,
        notification: &InstructionsLoadedNotification,
        options: HookEventExecutionOptions<'_>,
    ) {
        let Some(hook_system) = (self.get_hook_system)() else {
            return;
        };

        let event = HookEventName::InstructionsLoaded;
        let has_configured_hooks = !hook_system
            .registry_snapshot()
            .get_hooks_for_event(event)
            .is_empty();
        let has_session_hooks = hook_system
            .session_manager_snapshot()
            .has_hooks_for_event(event, None);
        if !has_configured_hooks && !has_session_hooks {
            return;
        }

        let _ = hook_system
            .fire_instructions_loaded_event(
                base,
                &notification.file_path,
                &notification.memory_type,
                &notification.load_reason,
                notification.trigger_file_path.as_deref(),
                notification.parent_file_path.as_deref(),
                options,
            )
            .await;
    }
}
