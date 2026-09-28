//! Hook event dispatch and result aggregation.
//!
//! Port of the private `executeHooks` path in
//! `packages/core/src/hooks/hookEventHandler.ts`. Event-specific input builders
//! and host telemetry/logging remain outside this module.

use std::future::Future;
use std::pin::Pin;

use futures_util::future::join_all;
use serde_json::{Map, Value, json};

use super::aggregator::{
    AggregatedHookResult, HookAggregator, HookExecutionResult, SpecificHookOutput,
    SpecificHookOutputKind,
};
use super::function_runner::{FunctionHookConfig, FunctionHookContext};
use super::planner::{
    HookEventContext, HookEventName, HookPlannerEntry, create_execution_plan,
    get_hook_matcher_target,
};
use super::session_manager::{SessionHookConfig, SessionHooksManager};
use crate::utils::cancellation::CancellationToken;

/// Owned hook config variants that the host executor dispatches to the
/// command, HTTP, prompt, or function runner.
#[derive(Clone, Debug)]
pub enum HookDispatchConfig {
    /// Config from the configured registry or a JSON-backed session hook.
    Json(Value),
    /// SDK-registered session function hook.
    Function(FunctionHookConfig),
}

pub type HookDispatchFuture<'a> =
    Pin<Box<dyn Future<Output = Result<HookExecutionResult, String>> + Send + 'a>>;

/// Host adapter for the runner types, which are not yet unified behind one
/// execution API. Return `Err` only for a dispatcher-level failure; ordinary
/// hook failure belongs in `HookExecutionResult`.
pub trait HookDispatchExecutor: Send + Sync {
    fn execute<'a>(
        &'a self,
        config: &'a HookDispatchConfig,
        event_name: HookEventName,
        input: &'a Value,
        function_context: &'a FunctionHookContext,
    ) -> HookDispatchFuture<'a>;
}

/// Coordinates registry planning, session hook matching, runner dispatch, and
/// aggregation for one event.
#[derive(Clone, Copy, Debug, Default)]
pub struct HookEventDispatcher {
    aggregator: HookAggregator,
}

impl HookEventDispatcher {
    /// Execute one event using registry entries already filtered for enabled
    /// state and sorted by source priority. The host maps those entries to
    /// `HookPlannerEntry` and supplies event-specific context.
    pub async fn execute<E: HookDispatchExecutor>(
        &self,
        executor: &E,
        session_hooks_manager: &SessionHooksManager,
        registry_entries: &[HookPlannerEntry],
        event_name: HookEventName,
        input: &Value,
        event_context: Option<&HookEventContext>,
        messages: Option<Vec<Map<String, Value>>>,
        cancellation: Option<&CancellationToken>,
    ) -> AggregatedHookResult {
        let plan = create_execution_plan(event_name, registry_entries, event_context);
        let session_id = input.get("session_id").and_then(Value::as_str);
        let matcher_target =
            get_hook_matcher_target(event_name, event_context).map(|target| target.target);
        let session_entries = match (session_id, matcher_target.as_deref()) {
            (Some(session_id), Some(target)) => {
                session_hooks_manager.get_matching_hooks(session_id, event_name, target)
            }
            (Some(session_id), None) => {
                session_hooks_manager.get_hooks_for_event(session_id, event_name)
            }
            (None, _) => Vec::new(),
        };

        let mut hook_configs = plan
            .as_ref()
            .map(|plan| {
                plan.hook_configs
                    .iter()
                    .cloned()
                    .map(HookDispatchConfig::Json)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        hook_configs.extend(session_entries.iter().map(|entry| match &entry.config {
            SessionHookConfig::Function(config) => HookDispatchConfig::Function(config.clone()),
            SessionHookConfig::Json(config) => HookDispatchConfig::Json(config.clone()),
        }));

        if hook_configs.is_empty() {
            return self.aggregator.aggregate_results(&[], event_name);
        }

        let sequential = plan.as_ref().is_some_and(|plan| plan.sequential)
            || session_entries
                .iter()
                .any(|entry| entry.sequential == Some(true));
        let function_context = FunctionHookContext {
            messages,
            tool_use_id: input
                .get("tool_use_id")
                .and_then(Value::as_str)
                .map(str::to_owned),
            signal: cancellation.cloned(),
        };

        let execution_results = if sequential {
            execute_sequential(
                executor,
                &hook_configs,
                event_name,
                input,
                &function_context,
            )
            .await
        } else {
            execute_parallel(
                executor,
                &hook_configs,
                event_name,
                input,
                &function_context,
            )
            .await
        };

        match execution_results {
            Ok(results) => self.aggregator.aggregate_results(&results, event_name),
            Err(error) => fail_closed_result(event_name, Some(error)),
        }
    }
}

async fn execute_parallel<E: HookDispatchExecutor>(
    executor: &E,
    configs: &[HookDispatchConfig],
    event_name: HookEventName,
    input: &Value,
    function_context: &FunctionHookContext,
) -> Result<Vec<HookExecutionResult>, String> {
    if function_context
        .signal
        .as_ref()
        .is_some_and(CancellationToken::is_cancelled)
    {
        return Ok(configs.iter().map(cancelled_result).collect());
    }

    let results = join_all(
        configs
            .iter()
            .map(|config| executor.execute(config, event_name, input, function_context)),
    )
    .await;
    results.into_iter().collect()
}

async fn execute_sequential<E: HookDispatchExecutor>(
    executor: &E,
    configs: &[HookDispatchConfig],
    event_name: HookEventName,
    input: &Value,
    function_context: &FunctionHookContext,
) -> Result<Vec<HookExecutionResult>, String> {
    let mut current_input = input.clone();
    let mut results = Vec::with_capacity(configs.len());

    for config in configs {
        if function_context
            .signal
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
        {
            break;
        }
        let result = executor
            .execute(config, event_name, &current_input, function_context)
            .await?;
        if result.success {
            if let Some(output) = result.output.as_ref().filter(|output| js_truthy(output)) {
                current_input = apply_hook_output_to_input(&current_input, output, event_name);
            }
        }
        results.push(result);
    }

    Ok(results)
}

fn cancelled_result(config: &HookDispatchConfig) -> HookExecutionResult {
    HookExecutionResult {
        success: false,
        output: None,
        error: Some(format!(
            "Hook execution cancelled (aborted): {}",
            hook_id(config)
        )),
        duration: 0.0,
    }
}

fn hook_id(config: &HookDispatchConfig) -> String {
    match config {
        HookDispatchConfig::Function(config) => config
            .name
            .as_deref()
            .filter(|name| !name.is_empty())
            .or_else(|| config.id.as_deref().filter(|id| !id.is_empty()))
            .unwrap_or("unknown-function")
            .to_owned(),
        HookDispatchConfig::Json(config) => {
            if let Some(name) = config
                .get("name")
                .and_then(Value::as_str)
                .filter(|name| !name.is_empty())
            {
                return name.to_owned();
            }
            match config.get("type").and_then(Value::as_str) {
                Some("command") => config
                    .get("command")
                    .and_then(Value::as_str)
                    .filter(|value| !value.is_empty())
                    .unwrap_or("unknown-command")
                    .to_owned(),
                Some("http") => config
                    .get("url")
                    .and_then(Value::as_str)
                    .filter(|value| !value.is_empty())
                    .unwrap_or("unknown-url")
                    .to_owned(),
                Some("function") => config
                    .get("id")
                    .and_then(Value::as_str)
                    .filter(|value| !value.is_empty())
                    .unwrap_or("unknown-function")
                    .to_owned(),
                Some("prompt") => "prompt-hook".to_owned(),
                _ => "unknown".to_owned(),
            }
        }
    }
}

fn fail_closed_result(event_name: HookEventName, error: Option<String>) -> AggregatedHookResult {
    let todo_event = matches!(
        event_name,
        HookEventName::TodoCreated | HookEventName::TodoCompleted
    );
    let final_output = todo_event.then(|| {
        let event_label = serde_json::to_value(event_name)
            .ok()
            .and_then(|value| value.as_str().map(str::to_owned))
            .unwrap_or_else(|| "unknown event".to_owned());
        let base_reason = format!("Hook system failed while processing {event_label}");
        let reason = error
            .as_ref()
            .map(|error| format!("{base_reason}: {error}"))
            .unwrap_or(base_reason);
        SpecificHookOutput {
            kind: SpecificHookOutputKind::Default,
            value: json!({ "decision": "block", "reason": reason }),
        }
    });
    AggregatedHookResult {
        success: false,
        all_outputs: Vec::new(),
        errors: error.into_iter().collect(),
        total_duration: 0.0,
        final_output,
    }
}

fn apply_hook_output_to_input(
    original_input: &Value,
    output: &Value,
    event_name: HookEventName,
) -> Value {
    let Some(specific) = output.get("hookSpecificOutput").and_then(Value::as_object) else {
        return original_input.clone();
    };
    let mut input = original_input.clone();
    let Some(input_object) = input.as_object_mut() else {
        return input;
    };

    match event_name {
        HookEventName::UserPromptSubmit => {
            if let (Some(Value::String(additional_context)), Some(Value::String(prompt))) = (
                specific.get("additionalContext"),
                input_object.get_mut("prompt"),
            ) && !additional_context.is_empty()
            {
                prompt.push_str("\n\n");
                prompt.push_str(additional_context);
            }
        }
        HookEventName::UserPromptExpansion => {
            if let Some(Value::String(raw_context)) = specific.get("additionalContext") {
                let additional_context = sanitize_prompt_expansion_context(raw_context);
                if !additional_context.is_empty()
                    && let Some(Value::String(prompt)) = input_object.get_mut("prompt")
                {
                    prompt.push_str("\n\n");
                    prompt.push_str(&additional_context);
                }
            }
        }
        HookEventName::PreToolUse => {
            if let Some(Value::Object(new_tool_input)) = specific.get("tool_input")
                && let Some(Value::Object(tool_input)) = input_object.get_mut("tool_input")
            {
                for (key, value) in new_tool_input {
                    tool_input.insert(key.clone(), value.clone());
                }
            }
        }
        _ => {}
    }

    input
}

fn sanitize_prompt_expansion_context(raw: &str) -> String {
    const MAX_UTF16_UNITS: usize = 10_000;
    let escaped = raw
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;");
    let mut result = String::new();
    let mut units = 0;
    for character in escaped.chars() {
        let next_units = character.len_utf16();
        if units + next_units > MAX_UTF16_UNITS {
            break;
        }
        result.push(character);
        units += next_units;
    }

    if let Some(entity_start) = result.rfind('&') {
        let suffix = &result[entity_start..];
        if matches!(
            suffix,
            "&" | "&a" | "&am" | "&amp" | "&l" | "&lt" | "&g" | "&gt"
        ) {
            result.truncate(entity_start);
        }
    }
    result
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
