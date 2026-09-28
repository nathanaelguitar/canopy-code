//! Adapter from event dispatch to the native hook runners.
//!
//! This module connects `HookEventDispatcher`'s injected executor seam to the
//! command, async-command, HTTP, function, and prompt runner implementations.
//! Runner-specific result metadata is reduced to the fields consumed by the
//! shared aggregator.

use std::sync::Arc;

use indexmap::IndexMap;
use serde_json::Value;

use super::aggregator::HookExecutionResult;
use super::async_command_runner::AsyncCommandHookRunner;
use super::async_registry::AsyncHookRegistry;
use super::command_runner::{CommandHookConfig, CommandHookRunner};
use super::event_dispatch::{HookDispatchConfig, HookDispatchExecutor, HookDispatchFuture};
use super::function_runner::{FunctionHookConfig, FunctionHookContext, FunctionHookRunner};
use super::http_runner::{HttpHookConfig, HttpHookRunner};
use super::planner::HookEventName;
use super::prompt_runner::{PromptHookRunner, PromptModelExecutor};

/// Synchronous resolver for JSON function-hook configs registered by a host.
/// Session function hooks already carry a typed callback and bypass this
/// resolver. Return `None` when the host has no callback for the config.
pub trait JsonFunctionHookResolver: Send + Sync {
    fn resolve(&self, hook_config: &Value) -> Option<FunctionHookConfig>;
}

/// Native adapter for the distinct Rust hook runner APIs.
pub struct NativeDispatchExecutor<P: PromptModelExecutor + Send + Sync + 'static> {
    command_runner: Arc<CommandHookRunner>,
    async_command_runner: AsyncCommandHookRunner,
    http_runner: Arc<HttpHookRunner>,
    function_runner: FunctionHookRunner,
    prompt_runner: Arc<PromptHookRunner<P>>,
    json_function_resolver: Option<Arc<dyn JsonFunctionHookResolver>>,
}

impl<P: PromptModelExecutor + Send + Sync + 'static> NativeDispatchExecutor<P> {
    /// Construct the adapter from already-configured runners and a shared
    /// async-hook registry used by the host to observe/drain completions.
    pub fn new(
        command_runner: Arc<CommandHookRunner>,
        async_registry: Arc<tokio::sync::Mutex<AsyncHookRegistry>>,
        http_runner: HttpHookRunner,
        function_runner: FunctionHookRunner,
        prompt_runner: PromptHookRunner<P>,
        json_function_resolver: Option<Arc<dyn JsonFunctionHookResolver>>,
    ) -> Self {
        Self {
            command_runner: Arc::clone(&command_runner),
            async_command_runner: AsyncCommandHookRunner::new(command_runner, async_registry),
            http_runner: Arc::new(http_runner),
            function_runner,
            prompt_runner: Arc::new(prompt_runner),
            json_function_resolver,
        }
    }

    /// Return a clone of the shared registry handle so the host can poll
    /// pending hooks, drain queued output, and drive timeout checks.
    pub fn async_registry(&self) -> Arc<tokio::sync::Mutex<AsyncHookRegistry>> {
        self.async_command_runner.registry()
    }

    async fn execute_json_config(
        &self,
        hook_config: &Value,
        event_name: HookEventName,
        input: &Value,
        function_context: &FunctionHookContext,
    ) -> Result<HookExecutionResult, String> {
        match hook_config.get("type").and_then(Value::as_str) {
            Some("command") => {
                if hook_config.get("async").and_then(Value::as_bool) == Some(true) {
                    let result = self
                        .async_command_runner
                        .execute(
                            hook_config,
                            event_name,
                            input,
                            function_context.signal.as_ref(),
                        )
                        .await;
                    return Ok(HookExecutionResult {
                        success: result.success,
                        output: result.output,
                        error: result.error,
                        duration: result.duration_ms as f64,
                    });
                }
                let command_config = match CommandHookConfig::from_value(hook_config.clone()) {
                    Ok(config) => config,
                    Err(error) => {
                        return Ok(failed_result(format!(
                            "Invalid command hook configuration: {error}"
                        )));
                    }
                };
                let result = self
                    .command_runner
                    .execute(
                        &command_config,
                        event_name,
                        input,
                        function_context.signal.as_ref(),
                    )
                    .await;
                Ok(HookExecutionResult {
                    success: result.success,
                    output: result.output,
                    error: result.error,
                    duration: result.duration_ms as f64,
                })
            }
            Some("http") => {
                let http_config = match HttpHookConfig::from_value(hook_config.clone()) {
                    Ok(config) => config,
                    Err(error) => {
                        return Ok(failed_result(format!(
                            "Invalid HTTP hook configuration: {error}"
                        )));
                    }
                };
                let result = self
                    .http_runner
                    .execute(
                        &http_config,
                        event_name,
                        input,
                        function_context.signal.as_ref(),
                    )
                    .await;
                Ok(HookExecutionResult {
                    success: result.success,
                    output: result.output,
                    error: result.error,
                    duration: result.duration_ms as f64,
                })
            }
            Some("function") => {
                let function_config = self
                    .json_function_resolver
                    .as_ref()
                    .and_then(|resolver| resolver.resolve(hook_config))
                    .unwrap_or_else(|| unresolved_function_config(hook_config));
                let result = self
                    .function_runner
                    .execute(
                        &function_config,
                        event_name,
                        input,
                        Some(function_context.clone()),
                    )
                    .await;
                Ok(HookExecutionResult {
                    success: result.success,
                    output: result.output,
                    error: result.error,
                    duration: result.duration_ms as f64,
                })
            }
            Some("prompt") => {
                let result = self
                    .prompt_runner
                    .execute(
                        hook_config,
                        event_name,
                        input,
                        function_context.signal.as_ref(),
                    )
                    .await;
                Ok(HookExecutionResult {
                    success: result.success,
                    output: result.output,
                    error: result.error,
                    duration: result.duration_ms,
                })
            }
            Some(other) => Ok(failed_result(format!("Unknown hook type: {other}"))),
            None => Ok(failed_result("Unknown hook type: undefined".to_owned())),
        }
    }
}

impl<P: PromptModelExecutor + Send + Sync + 'static> HookDispatchExecutor
    for NativeDispatchExecutor<P>
{
    fn execute<'a>(
        &'a self,
        config: &'a HookDispatchConfig,
        event_name: HookEventName,
        input: &'a Value,
        function_context: &'a FunctionHookContext,
    ) -> HookDispatchFuture<'a> {
        Box::pin(async move {
            match config {
                HookDispatchConfig::Json(config) => {
                    self.execute_json_config(config, event_name, input, function_context)
                        .await
                }
                HookDispatchConfig::Function(function_config) => {
                    let result = self
                        .function_runner
                        .execute(
                            function_config,
                            event_name,
                            input,
                            Some(function_context.clone()),
                        )
                        .await;
                    Ok(HookExecutionResult {
                        success: result.success,
                        output: result.output,
                        error: result.error,
                        duration: result.duration_ms as f64,
                    })
                }
            }
        })
    }
}

fn unresolved_function_config(config: &Value) -> FunctionHookConfig {
    FunctionHookConfig {
        id: config.get("id").and_then(Value::as_str).map(str::to_owned),
        name: config
            .get("name")
            .and_then(Value::as_str)
            .map(str::to_owned),
        description: config
            .get("description")
            .and_then(Value::as_str)
            .map(str::to_owned),
        timeout_ms: config.get("timeout").and_then(Value::as_f64),
        callback: None,
        error_message: config
            .get("errorMessage")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        status_message: config
            .get("statusMessage")
            .and_then(Value::as_str)
            .map(str::to_owned),
        extra: IndexMap::new(),
        on_hook_success: None,
    }
}

fn failed_result(error: String) -> HookExecutionResult {
    HookExecutionResult {
        success: false,
        output: None,
        error: Some(error),
        duration: 0.0,
    }
}
