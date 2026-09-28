//! Host-neutral execution policy for prompt hooks.
//!
//! Port of `packages/core/src/hooks/promptHookRunner.ts`. A host supplies a
//! model executor that resolves provider/model handles and performs one
//! generation request; this module owns prompt construction, request shaping,
//! timeout and cancellation policy, response validation, and hook outcomes.

use std::future::{Future, pending};
use std::pin::Pin;
use std::time::{Duration, Instant};

use serde_json::{Map, Value};

use crate::utils::cancellation::{CancellationToken, create_child_cancellation_token};

use super::planner::HookEventName;

/// System instruction used for each prompt-hook evaluation.
pub const PROMPT_HOOK_SYSTEM_PROMPT: &str = r#"You are evaluating a hook in Canopy Code.
Your task is to analyze the provided context and make a decision.

You MUST respond with valid JSON in one of these formats:
- {"ok": true} - Allow the operation to proceed
- {"ok": true, "additionalContext": "..."} - Allow and provide context
- {"ok": false, "reason": "..."} - Block the operation with a reason
- {"ok": false, "reason": "...", "additionalContext": "..."} - Block with reason and context

The "reason" field is required when blocking and will be shown to the user.
The "additionalContext" field is optional and can provide useful information to the main conversation.

Do NOT output anything other than the JSON response. No explanations, no markdown formatting."#;

/// A model/provider handle selected by the host.
#[derive(Clone, Debug)]
pub struct ResolvedPromptModel<M> {
    /// Provider-facing model identifier after any aliases have been resolved.
    pub model: String,
    /// Whether the resolved generator configuration enables reasoning.
    pub reasoning_configured: bool,
    /// Opaque provider-specific generator/model handle.
    pub handle: M,
}

/// One user message sent to a prompt-hook model.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PromptHookMessage {
    pub role: String,
    pub text: String,
}

/// Provider-neutral request options required by prompt hooks.
#[derive(Clone, Debug, PartialEq)]
pub struct PromptGenerationRequest {
    pub model: String,
    pub messages: Vec<PromptHookMessage>,
    pub system_instruction: String,
    /// `Some(0.0)` for ordinary models; omitted for reasoning models.
    pub temperature: Option<f32>,
    pub max_output_tokens: u32,
    /// Prompt hooks explicitly disable inherited reasoning.
    pub reasoning: bool,
    /// Prompt hooks do not request provider thought parts.
    pub include_thoughts: bool,
    /// Source request purpose passed to the content generator.
    pub purpose: String,
}

/// A provider response part. `thought` is kept as JSON so source truthiness,
/// rather than only the boolean `true`, controls filtering.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PromptResponsePart {
    pub text: Option<String>,
    pub thought: Option<Value>,
}

/// The first candidate's projected generation result.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PromptResponseCandidate {
    pub finish_reason: Option<String>,
    pub parts: Vec<PromptResponsePart>,
}

/// Provider-neutral subset of a model response used by prompt hooks.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PromptModelResponse {
    pub candidates: Vec<PromptResponseCandidate>,
    /// Usage is exposed for host telemetry but the TypeScript runner does not
    /// inspect it when deciding a hook result.
    pub usage: Option<Value>,
}

/// Injected host seam for resolving and executing prompt-hook model requests.
///
/// `current_model()` returns `None` when the host has no authenticated current
/// generator. The source runner checks that generator before resolving even a
/// model override. `resolve_for_model` must honor the supplied cancellation
/// token while doing potentially slow model resolution.
pub trait PromptModelExecutor: Sync {
    type ModelHandle: Send + Sync;

    /// Main configured model ID, used when the hook has no model override.
    fn main_model(&self) -> String;

    /// Current generator, or `None` when authentication/generation is absent.
    fn current_model(&self) -> Option<ResolvedPromptModel<Self::ModelHandle>>;

    /// Resolve an override with fail-closed model-selection policy.
    fn resolve_for_model<'a>(
        &'a self,
        model: &'a str,
        cancellation: &'a CancellationToken,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<ResolvedPromptModel<Self::ModelHandle>, String>> + Send + 'a,
        >,
    >;

    /// Generate one response using the shaped request and cancellation token.
    fn generate_content<'a>(
        &'a self,
        model: &'a ResolvedPromptModel<Self::ModelHandle>,
        request: PromptGenerationRequest,
        cancellation: &'a CancellationToken,
    ) -> Pin<Box<dyn Future<Output = Result<PromptModelResponse, String>> + Send + 'a>>;
}

/// Hook result policy matching the TypeScript prompt runner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PromptHookOutcome {
    Success,
    Blocking,
    NonBlockingError,
    Cancelled,
}

/// Prompt-hook result retaining the original config and event projection.
#[derive(Clone, Debug, PartialEq)]
pub struct PromptHookExecutionResult {
    pub hook_config: Value,
    pub event_name: HookEventName,
    pub success: bool,
    pub outcome: PromptHookOutcome,
    pub output: Option<Value>,
    pub error: Option<String>,
    /// Wall duration in milliseconds, matching the source result field.
    pub duration_ms: f64,
}

/// Executes prompt hooks using a host-provided model adapter.
pub struct PromptHookRunner<E> {
    executor: E,
}

impl<E: PromptModelExecutor> PromptHookRunner<E> {
    pub fn new(executor: E) -> Self {
        Self { executor }
    }

    /// Execute one prompt hook. Hook configs and inputs use JSON values so the
    /// module stays independent of the TypeScript config and provider types.
    pub async fn execute(
        &self,
        hook_config: &Value,
        event_name: HookEventName,
        input: &Value,
        signal: Option<&CancellationToken>,
    ) -> PromptHookExecutionResult {
        let started = Instant::now();
        let hook_name = hook_config
            .get("name")
            .and_then(Value::as_str)
            .filter(|name| !name.is_empty())
            .unwrap_or("prompt-hook");

        if signal.is_some_and(CancellationToken::is_cancelled) {
            return PromptHookExecutionResult {
                hook_config: hook_config.clone(),
                event_name,
                success: false,
                outcome: PromptHookOutcome::Cancelled,
                output: None,
                error: Some(format!(
                    "Prompt hook execution cancelled (aborted): {hook_name}"
                )),
                duration_ms: 0.0,
            };
        }

        let result = self.execute_inner(hook_config, input, signal).await;
        let duration_ms = started.elapsed().as_secs_f64() * 1000.0;
        match result {
            Ok(response) => process_response(hook_config, event_name, response, duration_ms),
            Err(error) if is_cancel_error(&error) => PromptHookExecutionResult {
                hook_config: hook_config.clone(),
                event_name,
                success: false,
                outcome: PromptHookOutcome::Cancelled,
                output: None,
                error: Some(error),
                duration_ms,
            },
            Err(error) => PromptHookExecutionResult {
                hook_config: hook_config.clone(),
                event_name,
                success: false,
                outcome: PromptHookOutcome::NonBlockingError,
                output: Some(continue_output()),
                error: Some(error),
                duration_ms,
            },
        }
    }

    async fn execute_inner(
        &self,
        hook_config: &Value,
        input: &Value,
        signal: Option<&CancellationToken>,
    ) -> Result<PromptHookResponse, String> {
        let prompt = hook_config
            .get("prompt")
            .and_then(Value::as_str)
            .ok_or_else(|| "Prompt hook prompt must be a string".to_owned())?;
        let json_input = serde_json::to_string_pretty(input)
            .map_err(|error| format!("Could not serialize prompt hook input: {error}"))?;
        let processed_prompt = prompt.replace("$ARGUMENTS", &json_input);

        let main_model = self.executor.main_model();
        let configured_model = hook_config
            .get("model")
            .and_then(Value::as_str)
            .unwrap_or(&main_model);
        let timeout_ms = hook_config
            .get("timeout")
            .filter(|value| !value.is_null())
            .and_then(Value::as_f64)
            .map(|seconds| seconds * 1000.0)
            .unwrap_or(30_000.0);
        let timeout = timeout_duration(timeout_ms);

        // Child cancellation reaches the provider, while the outer select
        // distinguishes caller abort from the runner's own timeout.
        let request_cancellation = create_child_cancellation_token(signal);
        let operation = self.execute_model_request(
            configured_model,
            &main_model,
            processed_prompt,
            &request_cancellation,
        );
        tokio::pin!(operation);
        let timeout_future = tokio::time::sleep(timeout);
        tokio::pin!(timeout_future);
        let abort_future = async move {
            match signal {
                Some(signal) => signal.cancelled().await,
                None => pending().await,
            }
        };
        tokio::pin!(abort_future);

        let result = tokio::select! {
            result = &mut operation => result,
            _ = &mut abort_future => {
                request_cancellation.cancel();
                Err("Prompt hook execution aborted".to_owned())
            },
            _ = &mut timeout_future => {
                request_cancellation.cancel_with_reason(format!(
                    "Prompt hook timed out after {}ms",
                    format_js_number(timeout_ms),
                ));
                Err(format!(
                    "Prompt hook timed out after {}ms",
                    format_js_number(timeout_ms),
                ))
            },
        };
        // The TypeScript child controller aborts in `finally` to detach its
        // parent listener even after success. Rust child cancellation removes
        // that weak parent registration through the same cleanup path.
        request_cancellation.cancel();
        result
    }

    async fn execute_model_request(
        &self,
        configured_model: &str,
        main_model: &str,
        prompt: String,
        cancellation: &CancellationToken,
    ) -> Result<PromptHookResponse, String> {
        let Some(current_model) = self.executor.current_model() else {
            return Err(
                "ContentGenerator not available - make sure you are authenticated".to_owned(),
            );
        };

        let resolved_model = if configured_model == main_model {
            current_model
        } else {
            self.executor
                .resolve_for_model(configured_model, cancellation)
                .await?
        };
        if cancellation.is_cancelled() {
            return Err("Prompt hook execution aborted".to_owned());
        }

        let request_model = if configured_model == main_model {
            configured_model
        } else {
            &resolved_model.model
        };
        let reasoning_model =
            resolved_model.reasoning_configured || is_reasoning_model(request_model);
        let request = PromptGenerationRequest {
            model: request_model.to_owned(),
            messages: vec![PromptHookMessage {
                role: "user".to_owned(),
                text: prompt,
            }],
            system_instruction: PROMPT_HOOK_SYSTEM_PROMPT.to_owned(),
            temperature: (!reasoning_model).then_some(0.0),
            max_output_tokens: 500,
            reasoning: false,
            include_thoughts: false,
            purpose: "prompt_hook".to_owned(),
        };
        let response = self
            .executor
            .generate_content(&resolved_model, request, cancellation)
            .await?;
        project_model_response(response)
    }
}

#[derive(Clone, Debug, PartialEq)]
struct PromptHookResponse {
    ok: bool,
    reason: Option<String>,
    additional_context: Option<String>,
}

fn project_model_response(response: PromptModelResponse) -> Result<PromptHookResponse, String> {
    let candidate = response.candidates.first();
    if candidate.is_some_and(|candidate| candidate.finish_reason.as_deref() == Some("MAX_TOKENS")) {
        return Err("Response truncated due to token limit".to_owned());
    }
    let text = candidate
        .into_iter()
        .flat_map(|candidate| candidate.parts.iter())
        .filter(|part| !part.thought.as_ref().is_some_and(js_truthy))
        .map(|part| part.text.as_deref().unwrap_or_default())
        .fold(String::new(), |mut text, part| {
            text.push_str(part);
            text
        });
    let text = trim_ecmascript_whitespace(&text);
    if text.is_empty() {
        return Err("Empty response from LLM".to_owned());
    }
    Ok(parse_response(text))
}

fn parse_response(text: &str) -> PromptHookResponse {
    let trimmed = trim_ecmascript_whitespace(text);
    let json_text = code_block_content(trimmed).unwrap_or(trimmed);
    let parsed = match serde_json::from_str::<Value>(json_text) {
        Ok(value) => value,
        Err(_) => {
            return PromptHookResponse {
                ok: true,
                reason: Some("Failed to parse LLM response, defaulting to allow".to_owned()),
                additional_context: None,
            };
        }
    };
    let Some(object) = parsed.as_object() else {
        return invalid_response_allow();
    };
    let Some(ok) = object.get("ok").and_then(Value::as_bool) else {
        return invalid_response_allow();
    };
    let reason = match object.get("reason") {
        None => None,
        Some(Value::String(reason)) => Some(reason.clone()),
        Some(_) => return invalid_response_allow(),
    };
    let additional_context = match object.get("additionalContext") {
        None => None,
        Some(Value::String(context)) => Some(context.clone()),
        Some(_) => return invalid_response_allow(),
    };
    PromptHookResponse {
        ok,
        reason,
        additional_context,
    }
}

fn invalid_response_allow() -> PromptHookResponse {
    PromptHookResponse {
        ok: true,
        reason: Some("Response validation failed, defaulting to allow".to_owned()),
        additional_context: None,
    }
}

fn code_block_content(text: &str) -> Option<&str> {
    let opening = text.find("```")? + 3;
    let after_opening = &text[opening..];
    let after_language = after_opening.strip_prefix("json").unwrap_or(after_opening);
    let body = after_language.trim_start_matches(is_ecmascript_whitespace);
    let closing = body.find("```")?;
    Some(trim_ecmascript_whitespace(&body[..closing]))
}

fn process_response(
    hook_config: &Value,
    event_name: HookEventName,
    response: PromptHookResponse,
    duration_ms: f64,
) -> PromptHookExecutionResult {
    let mut output = Map::new();
    if response.ok {
        output.insert("continue".to_owned(), Value::Bool(true));
        output.insert("decision".to_owned(), Value::String("allow".to_owned()));
        if let Some(reason) = response.reason {
            output.insert("reason".to_owned(), Value::String(reason));
        }
        if let Some(context) = response
            .additional_context
            .filter(|context| !context.is_empty())
        {
            output.insert("hookSpecificOutput".to_owned(), hook_context(context));
        }
        PromptHookExecutionResult {
            hook_config: hook_config.clone(),
            event_name,
            success: true,
            outcome: PromptHookOutcome::Success,
            output: Some(Value::Object(output)),
            error: None,
            duration_ms,
        }
    } else {
        let reason = response
            .reason
            .filter(|reason| !reason.is_empty())
            .unwrap_or_else(|| "Blocked by prompt hook".to_owned());
        output.insert("continue".to_owned(), Value::Bool(false));
        output.insert("stopReason".to_owned(), Value::String(reason.clone()));
        output.insert("decision".to_owned(), Value::String("block".to_owned()));
        output.insert("reason".to_owned(), Value::String(reason));
        if let Some(context) = response
            .additional_context
            .filter(|context| !context.is_empty())
        {
            output.insert("hookSpecificOutput".to_owned(), hook_context(context));
        }
        PromptHookExecutionResult {
            hook_config: hook_config.clone(),
            event_name,
            success: false,
            outcome: PromptHookOutcome::Blocking,
            output: Some(Value::Object(output)),
            error: None,
            duration_ms,
        }
    }
}

fn hook_context(context: String) -> Value {
    let mut specific = Map::new();
    specific.insert("additionalContext".to_owned(), Value::String(context));
    Value::Object(specific)
}

fn continue_output() -> Value {
    let mut output = Map::new();
    output.insert("continue".to_owned(), Value::Bool(true));
    Value::Object(output)
}

fn is_reasoning_model(model: &str) -> bool {
    let normalized = model.to_lowercase();
    normalized.starts_with("o1") || normalized.starts_with("o3") || normalized.contains("reasoner")
}

fn is_cancel_error(message: &str) -> bool {
    message.contains("timed out") || message.contains("aborted")
}

fn timeout_duration(timeout_ms: f64) -> Duration {
    // Node timers normalize non-positive, fractional-submillisecond, and
    // out-of-range delays to a one-millisecond timer.
    let milliseconds =
        if !timeout_ms.is_finite() || timeout_ms < 1.0 || timeout_ms > 2_147_483_647.0 {
            1
        } else {
            timeout_ms.trunc() as u64
        };
    Duration::from_millis(milliseconds)
}

fn format_js_number(number: f64) -> String {
    if number == 0.0 {
        return "0".to_owned();
    }
    if number.is_finite() && number.fract() == 0.0 {
        format!("{number:.0}")
    } else {
        number.to_string()
    }
}

fn trim_ecmascript_whitespace(value: &str) -> &str {
    value.trim_matches(is_ecmascript_whitespace)
}

fn is_ecmascript_whitespace(character: char) -> bool {
    matches!(
        character,
        '\u{0009}'..='\u{000d}'
            | '\u{0020}'
            | '\u{00a0}'
            | '\u{1680}'
            | '\u{2000}'..='\u{200a}'
            | '\u{2028}'..='\u{2029}'
            | '\u{202f}'
            | '\u{205f}'
            | '\u{3000}'
            | '\u{feff}'
    )
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
