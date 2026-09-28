//! Execution of SDK-registered function hooks.
//!
//! Port of `packages/core/src/hooks/functionHookRunner.ts`. The callback is a
//! host-provided async function; this module owns callback timeout/abort
//! handling, result classification, error-message projection, and the
//! optional success callback.

use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::FutureExt;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use super::planner::HookEventName;
use crate::utils::cancellation::CancellationToken;

/// Default function-hook timeout, measured in milliseconds.
pub const DEFAULT_FUNCTION_TIMEOUT_MS: f64 = 5_000.0;

pub type FunctionHookFuture =
    Pin<Box<dyn Future<Output = Result<FunctionHookValue, String>> + Send + 'static>>;

/// Function-hook return forms. `Undefined` has the same default-output
/// behavior as a JavaScript callback returning `undefined`.
#[derive(Clone, Debug, PartialEq)]
pub enum FunctionHookValue {
    Boolean(bool),
    Output(Value),
    Undefined,
}

/// Callback receives owned JSON values so timed-out callbacks can continue in
/// a detached task, matching `Promise.race` (which does not cancel the losing
/// promise).
pub type FunctionHookCallback =
    Arc<dyn Fn(Value, Option<FunctionHookContext>) -> FunctionHookFuture + Send + Sync + 'static>;

/// Rust context passed through to a function hook.
#[derive(Clone, Default)]
pub struct FunctionHookContext {
    pub messages: Option<Vec<Map<String, Value>>>,
    pub tool_use_id: Option<String>,
    pub signal: Option<CancellationToken>,
}

impl std::fmt::Debug for FunctionHookContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FunctionHookContext")
            .field("messages", &self.messages)
            .field("tool_use_id", &self.tool_use_id)
            .field("has_signal", &self.signal.is_some())
            .finish()
    }
}

/// Configuration for a function hook. `extra` holds descriptive/source fields
/// that execution does not interpret but that callers may need to retain.
#[derive(Clone)]
pub struct FunctionHookConfig {
    pub id: Option<String>,
    pub name: Option<String>,
    pub description: Option<String>,
    /// Milliseconds, as used by the source runner (unlike command/HTTP hooks).
    pub timeout_ms: Option<f64>,
    pub callback: Option<FunctionHookCallback>,
    pub error_message: String,
    pub status_message: Option<String>,
    pub extra: IndexMap<String, Value>,
    pub on_hook_success: Option<FunctionHookSuccessCallback>,
}

pub type FunctionHookSuccessCallback =
    Arc<dyn Fn(&FunctionHookExecutionResult) -> Result<(), String> + Send + Sync + 'static>;

impl FunctionHookConfig {
    /// Construct a config around a typed asynchronous Rust callback.
    pub fn new<F, Fut>(error_message: impl Into<String>, callback: F) -> Self
    where
        F: Fn(Value, Option<FunctionHookContext>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<FunctionHookValue, String>> + Send + 'static,
    {
        Self {
            id: None,
            name: None,
            description: None,
            timeout_ms: None,
            callback: Some(Arc::new(move |input, context| {
                Box::pin(callback(input, context))
            })),
            error_message: error_message.into(),
            status_message: None,
            extra: IndexMap::new(),
            on_hook_success: None,
        }
    }

    pub fn set_on_hook_success<F>(&mut self, callback: F)
    where
        F: Fn(&FunctionHookExecutionResult) -> Result<(), String> + Send + Sync + 'static,
    {
        self.on_hook_success = Some(Arc::new(callback));
    }
}

impl std::fmt::Debug for FunctionHookConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FunctionHookConfig")
            .field("id", &self.id)
            .field("name", &self.name)
            .field("description", &self.description)
            .field("timeout_ms", &self.timeout_ms)
            .field("has_callback", &self.callback.is_some())
            .field("error_message", &self.error_message)
            .field("status_message", &self.status_message)
            .field("extra", &self.extra)
            .field("has_on_hook_success", &self.on_hook_success.is_some())
            .finish()
    }
}

/// Execution outcomes in the source `HookExecutionOutcome` union.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FunctionHookOutcome {
    Success,
    Blocking,
    NonBlockingError,
    Cancelled,
}

/// Result of one function-hook invocation.
#[derive(Clone, Debug)]
pub struct FunctionHookExecutionResult {
    pub hook_config: FunctionHookConfig,
    pub event_name: HookEventName,
    pub success: bool,
    pub outcome: FunctionHookOutcome,
    pub error: Option<String>,
    pub output: Option<Value>,
    /// Elapsed milliseconds, matching the TypeScript result's `duration`.
    pub duration_ms: u64,
}

/// Executes SDK-registered function callbacks.
#[derive(Clone, Copy, Debug, Default)]
pub struct FunctionHookRunner;

impl FunctionHookRunner {
    /// Execute a function hook. Aborting before entry yields `cancelled`; an
    /// abort after callback execution starts is surfaced as a
    /// `non_blocking_error`, matching the source catch path.
    pub async fn execute(
        &self,
        hook_config: &FunctionHookConfig,
        event_name: HookEventName,
        input: &Value,
        context: Option<FunctionHookContext>,
    ) -> FunctionHookExecutionResult {
        let started = Instant::now();
        let hook_id = hook_config
            .id
            .as_deref()
            .filter(|value| !value.is_empty())
            .or_else(|| {
                hook_config
                    .name
                    .as_deref()
                    .filter(|value| !value.is_empty())
            })
            .unwrap_or("anonymous-function")
            .to_owned();
        let signal = context.as_ref().and_then(|context| context.signal.clone());

        if signal.as_ref().is_some_and(CancellationToken::is_cancelled) {
            return FunctionHookExecutionResult {
                hook_config: hook_config.clone(),
                event_name,
                success: false,
                outcome: FunctionHookOutcome::Cancelled,
                error: Some(format!(
                    "Function hook execution cancelled (aborted): {hook_id}"
                )),
                output: None,
                duration_ms: 0,
            };
        }

        let timeout_ms = hook_config
            .timeout_ms
            .unwrap_or(DEFAULT_FUNCTION_TIMEOUT_MS);
        let callback_result = execute_with_timeout(
            hook_config.callback.clone(),
            input.clone(),
            context,
            timeout_ms,
            signal,
        )
        .await;

        match callback_result {
            Ok(value) => {
                // The source captures duration before result processing and
                // before it invokes `onHookSuccess`.
                let duration_ms = elapsed_ms(started);
                let result = process_callback_result(hook_config, event_name, value, duration_ms);
                if result.success {
                    if let Some(callback) = &hook_config.on_hook_success {
                        // TypeScript catches synchronous callback errors and
                        // leaves the successful execution result unchanged.
                        // Rust callbacks can report the same failure by
                        // returning Err; panic is the counterpart to throw.
                        let callback_result = catch_unwind(AssertUnwindSafe(|| callback(&result)));
                        if callback_result.is_err() || callback_result.is_ok_and(|r| r.is_err()) {
                            // No logger is coupled to this core module.
                        }
                    }
                }
                result
            }
            Err(error) => {
                let duration_ms = elapsed_ms(started);
                let display_error = if hook_config.error_message.is_empty() {
                    error
                } else {
                    format!("{}: {error}", hook_config.error_message)
                };
                FunctionHookExecutionResult {
                    hook_config: hook_config.clone(),
                    event_name,
                    success: false,
                    outcome: FunctionHookOutcome::NonBlockingError,
                    error: Some(display_error),
                    output: None,
                    duration_ms,
                }
            }
        }
    }
}

fn process_callback_result(
    hook_config: &FunctionHookConfig,
    event_name: HookEventName,
    result: FunctionHookValue,
    duration_ms: u64,
) -> FunctionHookExecutionResult {
    match result {
        FunctionHookValue::Boolean(true) => FunctionHookExecutionResult {
            hook_config: hook_config.clone(),
            event_name,
            success: true,
            outcome: FunctionHookOutcome::Success,
            error: None,
            output: Some(json!({ "continue": true })),
            duration_ms,
        },
        FunctionHookValue::Boolean(false) => {
            let reason = if hook_config.error_message.is_empty() {
                "Blocked by function hook"
            } else {
                hook_config.error_message.as_str()
            };
            FunctionHookExecutionResult {
                hook_config: hook_config.clone(),
                event_name,
                success: false,
                outcome: FunctionHookOutcome::Blocking,
                error: None,
                output: Some(json!({
                    "continue": false,
                    "stopReason": reason,
                    "decision": "block",
                    "reason": reason,
                })),
                duration_ms,
            }
        }
        FunctionHookValue::Undefined => success_with_output(
            hook_config,
            event_name,
            json!({ "continue": true }),
            duration_ms,
        ),
        FunctionHookValue::Output(output) if !js_truthy(&output) => success_with_output(
            hook_config,
            event_name,
            json!({ "continue": true }),
            duration_ms,
        ),
        FunctionHookValue::Output(output) => {
            let blocking = output
                .get("decision")
                .and_then(Value::as_str)
                .is_some_and(|decision| matches!(decision, "block" | "deny"))
                || output.get("continue").and_then(Value::as_bool) == Some(false);
            FunctionHookExecutionResult {
                hook_config: hook_config.clone(),
                event_name,
                success: !blocking,
                outcome: if blocking {
                    FunctionHookOutcome::Blocking
                } else {
                    FunctionHookOutcome::Success
                },
                error: None,
                output: Some(output),
                duration_ms,
            }
        }
    }
}

fn success_with_output(
    hook_config: &FunctionHookConfig,
    event_name: HookEventName,
    output: Value,
    duration_ms: u64,
) -> FunctionHookExecutionResult {
    FunctionHookExecutionResult {
        hook_config: hook_config.clone(),
        event_name,
        success: true,
        outcome: FunctionHookOutcome::Success,
        error: None,
        output: Some(output),
        duration_ms,
    }
}

async fn execute_with_timeout(
    callback: Option<FunctionHookCallback>,
    input: Value,
    context: Option<FunctionHookContext>,
    timeout_ms: f64,
    signal: Option<CancellationToken>,
) -> Result<FunctionHookValue, String> {
    let Some(callback) = callback else {
        return Err("Invalid callback: expected a function".to_owned());
    };

    let callback_future =
        catch_unwind(AssertUnwindSafe(|| callback(input, context))).map_err(panic_message)?;
    let callback_task = tokio::spawn(async move {
        AssertUnwindSafe(callback_future)
            .catch_unwind()
            .await
            .unwrap_or_else(|panic| Err(panic_message(panic)))
    });
    tokio::pin!(callback_task);
    let timeout_error = format!(
        "Function hook timed out after {}ms",
        js_number_string(timeout_ms)
    );
    let delay = js_timer_delay(timeout_ms);
    let abort_wait = wait_for_abort(signal);
    tokio::pin!(abort_wait);

    tokio::select! {
        biased;
        result = &mut callback_task => result
            .map_err(|error| format!("Function hook callback task failed: {error}"))?,
        _ = tokio::time::sleep(delay) => Err(timeout_error),
        _ = &mut abort_wait => Err("Function hook execution aborted".to_owned()),
    }
}

async fn wait_for_abort(signal: Option<CancellationToken>) {
    if let Some(signal) = signal {
        let _ = signal.cancelled().await;
    } else {
        std::future::pending::<()>().await;
    }
}

fn js_timer_delay(timeout_ms: f64) -> Duration {
    if !timeout_ms.is_finite() || timeout_ms < 1.0 || timeout_ms > f64::from(i32::MAX) {
        return Duration::from_millis(1);
    }
    Duration::from_millis((timeout_ms.floor() as u64).max(1))
}

fn js_number_string(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_owned()
    } else if value == f64::INFINITY {
        "Infinity".to_owned()
    } else if value == f64::NEG_INFINITY {
        "-Infinity".to_owned()
    } else {
        value.to_string()
    }
}

fn js_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_f64().is_some_and(|value| value != 0.0),
        Value::String(value) => !value.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}

fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else {
        "function hook callback panicked".to_owned()
    }
}
