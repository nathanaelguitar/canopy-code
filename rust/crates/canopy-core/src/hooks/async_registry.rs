//! Registry for asynchronously executing hooks.
//!
//! Source: `packages/core/src/hooks/asyncHookRegistry.ts`. The registry owns
//! admission, pending execution metadata, output accumulation, terminal
//! messages, and timeout bookkeeping. It does not create a timer or own child
//! processes: the host should periodically call [`AsyncHookRegistry::check_timeouts`]
//! (the TypeScript default interval is 5 seconds), then terminate processes
//! for the returned hook IDs as appropriate. This explicit seam keeps timer
//! and process ownership with the executor.
//!
//! The source emits debug and warning logs. This module has no logger and
//! therefore silently ignores unknown IDs for update/completion/failure/
//! timeout operations; `register` still reports admission rejection with
//! `None`.

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/// Default maximum number of concurrently running async hooks.
pub const DEFAULT_MAX_CONCURRENT_HOOKS: usize = 10;
/// Suggested host polling interval matching the TypeScript registry default.
pub const DEFAULT_TIMEOUT_CHECK_INTERVAL_MS: u64 = 5_000;

/// Generate a unique ID for an async hook execution.
///
/// IDs keep the source's `hook_<epoch-ms>_<random>` shape. The random suffix
/// uses seven hexadecimal UUID characters rather than JavaScript's seven
/// base-36 random characters.
pub fn generate_hook_id() -> String {
    let random = Uuid::new_v4().simple().to_string();
    format!("hook_{}_{}", current_timestamp_ms(), &random[..7])
}

/// Registry configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AsyncHookRegistryOptions {
    pub max_concurrent_hooks: usize,
}

impl Default for AsyncHookRegistryOptions {
    fn default() -> Self {
        Self {
            max_concurrent_hooks: DEFAULT_MAX_CONCURRENT_HOOKS,
        }
    }
}

/// Input used to register one async hook execution.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AsyncHookRegistration {
    pub hook_id: String,
    pub hook_name: String,
    pub hook_event: String,
    pub session_id: String,
    /// Epoch time in milliseconds when execution began.
    #[serde(rename = "startTime")]
    pub start_time_ms: i64,
    /// Execution timeout in milliseconds.
    #[serde(rename = "timeout")]
    pub timeout_ms: i64,
}

/// Current state of an async hook.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum AsyncHookStatus {
    Running,
    Completed,
    Failed,
    Timeout,
}

/// A pending hook's execution metadata and accumulated output.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PendingAsyncHook {
    pub hook_id: String,
    pub hook_name: String,
    pub hook_event: String,
    pub session_id: String,
    #[serde(rename = "startTime")]
    pub start_time_ms: i64,
    #[serde(rename = "timeout")]
    pub timeout_ms: i64,
    pub stdout: String,
    pub stderr: String,
    pub status: AsyncHookStatus,
    /// Hook result represented as JSON because the shared HookOutput type is
    /// not currently available in the Rust hook modules.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<Value>,
    /// Error message from a failed or timed-out hook.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Kind of user-facing message produced when an async hook finishes.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum AsyncHookOutputType {
    System,
    Info,
    Warning,
    Error,
}

/// One message queued for delivery after an async hook finishes.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AsyncHookOutputMessage {
    #[serde(rename = "type")]
    pub output_type: AsyncHookOutputType,
    pub message: String,
    pub hook_name: String,
    pub hook_id: String,
    /// Epoch time in milliseconds.
    #[serde(rename = "timestamp")]
    pub timestamp_ms: i64,
}

/// Output queued for delivery to the next turn.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct PendingAsyncOutput {
    pub messages: Vec<AsyncHookOutputMessage>,
    pub contexts: Vec<String>,
}

/// Tracks pending hooks and queues their completed outputs for later delivery.
#[derive(Debug)]
pub struct AsyncHookRegistry {
    pending_hooks: IndexMap<String, PendingAsyncHook>,
    completed_outputs: Vec<AsyncHookOutputMessage>,
    completed_contexts: Vec<String>,
    max_concurrent_hooks: usize,
}

impl Default for AsyncHookRegistry {
    fn default() -> Self {
        Self::with_options(AsyncHookRegistryOptions::default())
    }
}

impl AsyncHookRegistry {
    /// Create a registry with the default concurrency limit.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a registry with an explicit concurrency limit.
    pub fn with_options(options: AsyncHookRegistryOptions) -> Self {
        Self {
            pending_hooks: IndexMap::new(),
            completed_outputs: Vec::new(),
            completed_contexts: Vec::new(),
            max_concurrent_hooks: options.max_concurrent_hooks,
        }
    }

    /// Current number of pending hooks whose status is running.
    pub fn get_running_count(&self) -> usize {
        self.pending_hooks
            .values()
            .filter(|hook| hook.status == AsyncHookStatus::Running)
            .count()
    }

    /// Whether the registry can admit another hook under its concurrency cap.
    pub fn can_accept_more(&self) -> bool {
        self.get_running_count() < self.max_concurrent_hooks
    }

    /// Register a hook, returning `None` when the concurrency cap is reached.
    ///
    /// A repeated hook ID replaces the previous entry when admission succeeds,
    /// matching the source `Map.set` behavior.
    pub fn register(&mut self, registration: AsyncHookRegistration) -> Option<String> {
        if !self.can_accept_more() {
            return None;
        }

        let hook_id = registration.hook_id;
        let hook = PendingAsyncHook {
            hook_id: hook_id.clone(),
            hook_name: registration.hook_name,
            hook_event: registration.hook_event,
            session_id: registration.session_id,
            start_time_ms: registration.start_time_ms,
            timeout_ms: registration.timeout_ms,
            stdout: String::new(),
            stderr: String::new(),
            status: AsyncHookStatus::Running,
            output: None,
            error: None,
        };
        self.pending_hooks.insert(hook_id.clone(), hook);
        Some(hook_id)
    }

    /// Append new stdout and/or stderr bytes to a pending hook.
    pub fn update_output(&mut self, hook_id: &str, stdout: Option<&str>, stderr: Option<&str>) {
        if let Some(hook) = self.pending_hooks.get_mut(hook_id) {
            if let Some(stdout) = stdout {
                hook.stdout.push_str(stdout);
            }
            if let Some(stderr) = stderr {
                hook.stderr.push_str(stderr);
            }
        }
    }

    /// Mark a hook complete, process its captured output, and remove it.
    pub fn complete(&mut self, hook_id: &str, output: Option<Value>) {
        let Some(hook) = self.pending_hooks.get_mut(hook_id) else {
            return;
        };
        hook.status = AsyncHookStatus::Completed;
        hook.output = output;
        let completed = hook.clone();
        self.process_completed_output(&completed, current_timestamp_ms());
        self.pending_hooks.shift_remove(hook_id);
    }

    /// Mark a hook failed, queue its error message, and remove it.
    pub fn fail(&mut self, hook_id: &str, error_message: &str) {
        let Some(hook) = self.pending_hooks.get_mut(hook_id) else {
            return;
        };
        hook.status = AsyncHookStatus::Failed;
        hook.error = Some(error_message.to_owned());
        self.completed_outputs.push(AsyncHookOutputMessage {
            output_type: AsyncHookOutputType::Error,
            message: format!("Async hook {} failed: {error_message}", hook.hook_name),
            hook_name: hook.hook_name.clone(),
            hook_id: hook_id.to_owned(),
            timestamp_ms: current_timestamp_ms(),
        });
        self.pending_hooks.shift_remove(hook_id);
    }

    /// Mark a hook timed out, queue its warning, and remove it.
    pub fn timeout(&mut self, hook_id: &str) {
        self.timeout_at(hook_id, current_timestamp_ms());
    }

    /// Return all currently pending hooks in registration order.
    pub fn get_pending_hooks(&self) -> Vec<PendingAsyncHook> {
        self.pending_hooks.values().cloned().collect()
    }

    /// Return hooks for one session in registration order.
    pub fn get_pending_hooks_for_session(&self, session_id: &str) -> Vec<PendingAsyncHook> {
        self.pending_hooks
            .values()
            .filter(|hook| hook.session_id.as_str() == session_id)
            .cloned()
            .collect()
    }

    /// Drain completed messages and contexts for delivery to the next turn.
    pub fn get_pending_output(&mut self) -> PendingAsyncOutput {
        PendingAsyncOutput {
            messages: std::mem::take(&mut self.completed_outputs),
            contexts: std::mem::take(&mut self.completed_contexts),
        }
    }

    /// Whether any messages or additional contexts are queued.
    pub fn has_pending_output(&self) -> bool {
        !self.completed_outputs.is_empty() || !self.completed_contexts.is_empty()
    }

    /// Whether any hook executions remain pending.
    pub fn has_running_hooks(&self) -> bool {
        !self.pending_hooks.is_empty()
    }

    /// Mark hooks whose elapsed time is strictly greater than their timeout.
    ///
    /// The caller supplies epoch milliseconds so a host scheduler can drive
    /// timeout checks without this registry creating its own timer. The
    /// returned IDs identify timed-out processes for host-side termination.
    pub fn check_timeouts(&mut self, now_ms: i64) -> Vec<String> {
        let expired_hook_ids = self
            .pending_hooks
            .values()
            .filter(|hook| {
                hook.status == AsyncHookStatus::Running
                    && now_ms.saturating_sub(hook.start_time_ms) > hook.timeout_ms
            })
            .map(|hook| hook.hook_id.clone())
            .collect::<Vec<_>>();

        for hook_id in &expired_hook_ids {
            self.timeout_at(hook_id, now_ms);
        }
        expired_hook_ids
    }

    /// Remove pending hooks for a session without adding output messages.
    pub fn clear_session(&mut self, session_id: &str) {
        self.pending_hooks
            .retain(|_, hook| hook.session_id != session_id);
    }

    /// Number of pending hooks.
    pub fn len(&self) -> usize {
        self.pending_hooks.len()
    }

    /// Whether no hooks are pending.
    pub fn is_empty(&self) -> bool {
        self.pending_hooks.is_empty()
    }

    fn timeout_at(&mut self, hook_id: &str, timestamp_ms: i64) {
        let Some(hook) = self.pending_hooks.get_mut(hook_id) else {
            return;
        };
        hook.status = AsyncHookStatus::Timeout;
        hook.error = Some(format!("Hook timed out after {}ms", hook.timeout_ms));
        self.completed_outputs.push(AsyncHookOutputMessage {
            output_type: AsyncHookOutputType::Warning,
            message: format!(
                "Async hook {} timed out after {}ms",
                hook.hook_name, hook.timeout_ms
            ),
            hook_name: hook.hook_name.clone(),
            hook_id: hook_id.to_owned(),
            timestamp_ms,
        });
        self.pending_hooks.shift_remove(hook_id);
    }

    fn process_completed_output(&mut self, hook: &PendingAsyncHook, timestamp_ms: i64) {
        if !hook.stdout.is_empty() {
            let trimmed = hook.stdout.trim();
            match serde_json::from_str::<Value>(trimmed) {
                Ok(Value::Null) => {
                    self.completed_outputs.push(AsyncHookOutputMessage {
                        output_type: AsyncHookOutputType::Info,
                        message: trimmed.to_owned(),
                        hook_name: hook.hook_name.clone(),
                        hook_id: hook.hook_id.clone(),
                        timestamp_ms,
                    });
                }
                Ok(parsed) => {
                    if let Some(system_message) = parsed
                        .get("systemMessage")
                        .and_then(Value::as_str)
                        .filter(|message| !message.is_empty())
                    {
                        self.completed_outputs.push(AsyncHookOutputMessage {
                            output_type: AsyncHookOutputType::System,
                            message: system_message.to_owned(),
                            hook_name: hook.hook_name.clone(),
                            hook_id: hook.hook_id.clone(),
                            timestamp_ms,
                        });
                    }

                    if let Some(additional_context) = parsed
                        .get("hookSpecificOutput")
                        .and_then(|value| value.get("additionalContext"))
                        .and_then(Value::as_str)
                        .filter(|context| !context.is_empty())
                    {
                        self.completed_contexts.push(additional_context.to_owned());
                    }
                }
                Err(_) if !trimmed.is_empty() => {
                    self.completed_outputs.push(AsyncHookOutputMessage {
                        output_type: AsyncHookOutputType::Info,
                        message: trimmed.to_owned(),
                        hook_name: hook.hook_name.clone(),
                        hook_id: hook.hook_id.clone(),
                        timestamp_ms,
                    });
                }
                Err(_) => {}
            }
        }

        let stderr = hook.stderr.trim();
        if !stderr.is_empty() {
            self.completed_outputs.push(AsyncHookOutputMessage {
                output_type: AsyncHookOutputType::Warning,
                message: stderr.to_owned(),
                hook_name: hook.hook_name.clone(),
                hook_id: hook.hook_id.clone(),
                timestamp_ms,
            });
        }
    }
}

fn current_timestamp_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or_default()
}
