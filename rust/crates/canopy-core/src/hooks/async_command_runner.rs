//! Admission and background execution for asynchronous command hooks.
//!
//! Port of `HookRunner.executeAsyncHook` and
//! `executeCommandHookInBackground` from `packages/core/src/hooks/hookRunner.ts`.
//! The host owns an `Arc<CommandHookRunner>` and a shared async registry; this
//! wrapper admits a hook, starts its command in a Tokio task, and records the
//! terminal result without delaying the caller.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};
use tokio::sync::Mutex;
use tokio::task::JoinError;

use super::async_registry::{AsyncHookRegistration, AsyncHookRegistry, generate_hook_id};
use super::command_runner::{CommandHookConfig, CommandHookRunner};
use super::planner::HookEventName;
use crate::utils::cancellation::CancellationToken;

/// The default timeout used by async-hook registry admission, matching the
/// source's `hookConfig.timeout || DEFAULT_HOOK_TIMEOUT` behavior.
pub const DEFAULT_ASYNC_COMMAND_HOOK_TIMEOUT_MS: f64 = 60_000.0;

/// Outcome set on the outer `executeHook` result when it was cancelled before
/// async command admission began. Other immediate async results omit outcome.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AsyncCommandStartOutcome {
    Cancelled,
}

/// The immediate result returned after async admission, not the background
/// command's eventual `CommandHookExecutionResult`.
#[derive(Clone, Debug, PartialEq)]
pub struct AsyncCommandStartResult {
    pub hook_config: Value,
    pub event_name: HookEventName,
    pub success: bool,
    pub outcome: Option<AsyncCommandStartOutcome>,
    pub output: Option<Value>,
    pub error: Option<String>,
    pub duration_ms: u64,
    pub is_async: Option<bool>,
}

/// Starts async command hooks and updates the shared pending-hook registry.
#[derive(Clone)]
pub struct AsyncCommandHookRunner {
    command_runner: Arc<CommandHookRunner>,
    registry: Arc<Mutex<AsyncHookRegistry>>,
}

impl AsyncCommandHookRunner {
    pub fn new(
        command_runner: Arc<CommandHookRunner>,
        registry: Arc<Mutex<AsyncHookRegistry>>,
    ) -> Self {
        Self {
            command_runner,
            registry,
        }
    }

    /// Expose the same registry handle to host code that lists or drains async
    /// hook results.
    pub fn registry(&self) -> Arc<Mutex<AsyncHookRegistry>> {
        Arc::clone(&self.registry)
    }

    /// Admit an async command hook, spawn it in the background, and return an
    /// immediate allow-to-continue result. `hook_config` is retained as its
    /// original JSON value for result projection and command execution.
    pub async fn execute(
        &self,
        hook_config: &Value,
        event_name: HookEventName,
        input: &Value,
        signal: Option<&CancellationToken>,
    ) -> AsyncCommandStartResult {
        let started = std::time::Instant::now();
        let hook_id_for_error = get_hook_id(hook_config);

        if signal.is_some_and(CancellationToken::is_cancelled) {
            return AsyncCommandStartResult {
                hook_config: hook_config.clone(),
                event_name,
                success: false,
                outcome: Some(AsyncCommandStartOutcome::Cancelled),
                output: None,
                error: Some(format!(
                    "Hook execution cancelled (aborted): {hook_id_for_error}"
                )),
                duration_ms: 0,
                is_async: None,
            };
        }

        let command_config = match CommandHookConfig::from_value(hook_config.clone()) {
            Ok(config) if config.hook_type == "command" && config.is_async => config,
            Ok(_) => {
                return execution_error(
                    hook_config,
                    event_name,
                    started,
                    &hook_id_for_error,
                    "Async command runner requires a command hook with async=true",
                );
            }
            Err(error) => {
                return execution_error(
                    hook_config,
                    event_name,
                    started,
                    &hook_id_for_error,
                    &error.to_string(),
                );
            }
        };

        let hook_id = generate_hook_id();
        let hook_name = command_config
            .name
            .as_deref()
            .filter(|name| !name.is_empty())
            .or_else(|| command_config.command.as_deref().filter(|s| !s.is_empty()))
            .unwrap_or("async-hook")
            .to_owned();

        // Match the source's fast admission check. Registration checks again
        // after this lock is released, covering another task taking the final
        // slot between the check and register calls.
        if !self.registry.lock().await.can_accept_more() {
            return admission_rejected(hook_config, event_name);
        }

        let registration = AsyncHookRegistration {
            hook_id: hook_id.clone(),
            hook_name: hook_name.clone(),
            hook_event: event_name_name(event_name),
            session_id: input
                .get("session_id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            start_time_ms: current_timestamp_ms(),
            timeout_ms: registry_timeout_ms(command_config.timeout),
        };
        let registered_id = self.registry.lock().await.register(registration);
        if registered_id.is_none() {
            return admission_rejected(hook_config, event_name);
        }

        let runner = Arc::clone(&self.command_runner);
        let registry = Arc::clone(&self.registry);
        let config = command_config.clone();
        let event = event_name;
        let input = input.clone();
        let cancellation = signal.cloned();
        let background_hook_id = hook_id.clone();

        let task = tokio::spawn(async move {
            let result = runner
                .execute(&config, event, &input, cancellation.as_ref())
                .await;
            let mut registry = registry.lock().await;
            if result.success {
                registry.update_output(
                    &background_hook_id,
                    result.stdout.as_deref(),
                    result.stderr.as_deref(),
                );
                registry.complete(&background_hook_id, result.output);
            } else {
                registry.fail(
                    &background_hook_id,
                    result.error.as_deref().unwrap_or("Unknown error"),
                );
            }
        });

        // The source attaches a detached `.catch()` to ensure an unexpected
        // background rejection is recorded. A second Tokio task observes the
        // JoinHandle and marks panics/cancellation as failures too.
        let registry = Arc::clone(&self.registry);
        let background_hook_id = hook_id.clone();
        tokio::spawn(async move {
            if let Err(error) = task.await {
                let message = join_error_message(error);
                registry.lock().await.fail(&background_hook_id, &message);
            }
        });

        AsyncCommandStartResult {
            hook_config: hook_config.clone(),
            event_name,
            success: true,
            outcome: None,
            output: Some(json!({ "continue": true })),
            error: None,
            duration_ms: 0,
            is_async: Some(true),
        }
    }
}

fn admission_rejected(hook_config: &Value, event_name: HookEventName) -> AsyncCommandStartResult {
    AsyncCommandStartResult {
        hook_config: hook_config.clone(),
        event_name,
        success: false,
        outcome: None,
        output: Some(json!({ "continue": true })),
        error: Some("Async hook rejected: too many concurrent async hooks running".to_owned()),
        duration_ms: 0,
        is_async: Some(true),
    }
}

fn execution_error(
    hook_config: &Value,
    event_name: HookEventName,
    started: std::time::Instant,
    hook_id: &str,
    error: &str,
) -> AsyncCommandStartResult {
    AsyncCommandStartResult {
        hook_config: hook_config.clone(),
        event_name,
        success: false,
        outcome: None,
        output: None,
        error: Some(format!(
            "Hook execution failed for event '{event_name:?}' (hook: {hook_id}): {error}"
        )),
        duration_ms: started.elapsed().as_millis().min(u64::MAX as u128) as u64,
        is_async: None,
    }
}

fn get_hook_id(hook_config: &Value) -> String {
    hook_config
        .get("name")
        .and_then(Value::as_str)
        .filter(|name| !name.is_empty())
        .or_else(|| {
            hook_config
                .get("command")
                .and_then(Value::as_str)
                .filter(|command| !command.is_empty())
        })
        .unwrap_or("unknown-command")
        .to_owned()
}

fn event_name_name(event_name: HookEventName) -> String {
    serde_json::to_value(event_name)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_else(|| format!("{event_name:?}"))
}

fn current_timestamp_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or_default()
}

fn registry_timeout_ms(timeout: Option<f64>) -> i64 {
    match timeout {
        Some(value) if value != 0.0 && !value.is_nan() => {
            if value >= i64::MAX as f64 {
                i64::MAX
            } else if value <= i64::MIN as f64 {
                i64::MIN
            } else {
                value.trunc() as i64
            }
        }
        _ => DEFAULT_ASYNC_COMMAND_HOOK_TIMEOUT_MS as i64,
    }
}

fn join_error_message(error: JoinError) -> String {
    if error.is_panic() {
        let panic = error.into_panic();
        if let Some(message) = panic.downcast_ref::<String>() {
            return message.clone();
        }
        if let Some(message) = panic.downcast_ref::<&'static str>() {
            return (*message).to_owned();
        }
        "Unexpected error in async hook background execution".to_owned()
    } else {
        error.to_string()
    }
}
