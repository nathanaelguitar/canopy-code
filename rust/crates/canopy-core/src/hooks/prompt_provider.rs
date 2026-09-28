//! Provider transport adapter for prompt-hook model requests.
//!
//! Model aliases, provider selection, and credentials stay with the host. The
//! host resolves a model to one of Canopy's native provider clients; this
//! adapter maps the prompt-hook request to that provider and projects its
//! response back to the provider-neutral hook contract.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde_json::{Value, json};

use crate::hooks::prompt_runner::{
    PromptGenerationRequest, PromptHookMessage, PromptModelExecutor, PromptModelResponse,
    PromptResponseCandidate, PromptResponsePart, ResolvedPromptModel,
};
use crate::modalities::{
    is_canopy_family_wire_model, is_glm_wire_model, is_tiered_effort_wire_model,
};
use crate::providers::anthropic::AnthropicMessagesClient;
use crate::providers::gemini::GeminiNativeClient;
use crate::providers::openai_compatible::OpenAiCompatibleClient;
use crate::providers::openai_pipeline::{OpenAiPipelineConfig, build_openai_request};
use crate::providers::openai_profiles::OpenAiProviderProfile;
use crate::providers::openai_response::{
    OpenAiResponseContext, ResponseParsingOptions, convert_openai_response_to_gemini,
};
use crate::utils::cancellation::CancellationToken;

/// Provider client selected for a prompt-hook model.
#[derive(Clone)]
pub enum PromptProviderBackend {
    OpenAiCompatible {
        client: Arc<OpenAiCompatibleClient>,
        pipeline: OpenAiPipelineConfig,
    },
    Anthropic(Arc<AnthropicMessagesClient>),
    Gemini {
        client: Arc<GeminiNativeClient>,
        sampling_params: Option<Value>,
    },
}

/// Provider-backed model handle returned by the host's model selector.
pub type ProviderPromptModel = ResolvedPromptModel<PromptProviderBackend>;

/// Host-owned source of the primary generator and fail-closed model overrides.
///
/// Implementations must apply the same model alias, provider install, and
/// credential selection rules used for ordinary agent generation. The
/// current-model lookup intentionally happens before an override is resolved,
/// matching the TypeScript prompt hook's authentication gate.
pub trait PromptProviderModelResolver: Send + Sync {
    fn main_model(&self) -> String;

    fn current_model(&self) -> Option<ProviderPromptModel>;

    fn resolve_for_model<'a>(
        &'a self,
        model: &'a str,
        cancellation: &'a CancellationToken,
    ) -> PromptProviderResolveFuture<'a>;
}

pub type PromptProviderResolveFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ProviderPromptModel, String>> + Send + 'a>>;

/// Concrete [`PromptModelExecutor`] using Canopy's native provider transports.
pub struct ProviderPromptModelExecutor<R> {
    resolver: R,
}

impl<R> ProviderPromptModelExecutor<R> {
    pub fn new(resolver: R) -> Self {
        Self { resolver }
    }
}

impl<R: PromptProviderModelResolver> PromptModelExecutor for ProviderPromptModelExecutor<R> {
    type ModelHandle = PromptProviderBackend;

    fn main_model(&self) -> String {
        self.resolver.main_model()
    }

    fn current_model(&self) -> Option<ProviderPromptModel> {
        self.resolver.current_model()
    }

    fn resolve_for_model<'a>(
        &'a self,
        model: &'a str,
        cancellation: &'a CancellationToken,
    ) -> Pin<Box<dyn Future<Output = Result<ProviderPromptModel, String>> + Send + 'a>> {
        Box::pin(async move {
            let resolve = self.resolver.resolve_for_model(model, cancellation);
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => Err("Prompt hook execution aborted".to_owned()),
                result = resolve => result,
            }
        })
    }

    fn generate_content<'a>(
        &'a self,
        model: &'a ProviderPromptModel,
        request: PromptGenerationRequest,
        cancellation: &'a CancellationToken,
    ) -> Pin<Box<dyn Future<Output = Result<PromptModelResponse, String>> + Send + 'a>> {
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err("Prompt hook execution aborted".to_owned());
            }
            let request = build_provider_neutral_request(&request);
            let response = match &model.handle {
                PromptProviderBackend::OpenAiCompatible { client, pipeline } => {
                    let mut pipeline = pipeline.clone();
                    pipeline.request_context.model = model.model.clone();
                    pipeline.reasoning = Some(Value::Bool(false));
                    if let Some(temperature) = request
                        .pointer("/config/temperature")
                        .and_then(Value::as_f64)
                    {
                        let sampling = pipeline.sampling_params.get_or_insert_with(|| json!({}));
                        if let Some(sampling) = sampling.as_object_mut() {
                            sampling.insert("temperature".to_owned(), json!(temperature));
                        }
                    }
                    let mut wire_request = build_openai_request(
                        &request,
                        &pipeline,
                        request
                            .get("purpose")
                            .and_then(Value::as_str)
                            .unwrap_or("prompt_hook"),
                        false,
                        |request, _| request,
                    );
                    disable_openai_prompt_reasoning(&mut wire_request, &model.model, &pipeline);
                    let raw_response = cancellable_provider_request(cancellation, async {
                        client.complete(&wire_request).await
                    })
                    .await?;
                    let mut context = OpenAiResponseContext {
                        model: request
                            .get("model")
                            .and_then(Value::as_str)
                            .unwrap_or(&model.model)
                            .to_owned(),
                        response_parsing_options: ResponseParsingOptions {
                            tagged_thinking_tags: matches!(
                                pipeline.provider_profile,
                                OpenAiProviderProfile::MiniMax
                            ),
                            content_only_thinking_tag_leaks: !matches!(
                                pipeline.provider_profile,
                                OpenAiProviderProfile::MiniMax
                            ),
                        },
                        ..OpenAiResponseContext::default()
                    };
                    convert_openai_response_to_gemini(&raw_response, &mut context).response
                }
                PromptProviderBackend::Anthropic(client) => {
                    client
                        .complete_gemini_with_cancellation(&request, cancellation)
                        .await
                        .map_err(|error| provider_error(error, cancellation))?
                        .response
                }
                PromptProviderBackend::Gemini {
                    client,
                    sampling_params,
                } => {
                    let mut sampling_params = sampling_params.clone();
                    if let Some(temperature) = request
                        .pointer("/config/temperature")
                        .and_then(Value::as_f64)
                    {
                        let sampling = sampling_params.get_or_insert_with(|| json!({}));
                        if let Some(sampling) = sampling.as_object_mut() {
                            sampling.insert("temperature".to_owned(), json!(temperature));
                        }
                    }
                    cancellable_provider_request(cancellation, async {
                        client
                            .complete(
                                &request,
                                sampling_params.as_ref(),
                                Some(&Value::Bool(false)),
                                Some(cancellation),
                            )
                            .await
                    })
                    .await?
                }
            };
            Ok(project_provider_response(response))
        })
    }
}

fn build_provider_neutral_request(request: &PromptGenerationRequest) -> Value {
    let contents = request
        .messages
        .iter()
        .map(build_content)
        .collect::<Vec<_>>();
    let mut config = serde_json::Map::new();
    config.insert(
        "systemInstruction".to_owned(),
        json!({"parts":[{"text":request.system_instruction}]}),
    );
    config.insert(
        "maxOutputTokens".to_owned(),
        json!(request.max_output_tokens),
    );
    config.insert(
        "thinkingConfig".to_owned(),
        json!({"includeThoughts":request.include_thoughts}),
    );
    if let Some(temperature) = request.temperature {
        config.insert("temperature".to_owned(), json!(temperature));
    }
    json!({
        "model": request.model,
        "contents": contents,
        "config": config,
        "purpose": request.purpose,
    })
}

fn build_content(message: &PromptHookMessage) -> Value {
    let role = if matches!(message.role.as_str(), "assistant" | "model") {
        "model"
    } else {
        "user"
    };
    json!({"role": role, "parts": [{"text": message.text}]})
}

fn disable_openai_prompt_reasoning(
    request: &mut Value,
    resolved_model: &str,
    pipeline: &OpenAiPipelineConfig,
) {
    let Some(request) = request.as_object_mut() else {
        return;
    };
    let model = request
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or(resolved_model);
    let normalized_model = model.to_ascii_lowercase();
    let zai_endpoint = matches!(
        pipeline.provider_profile,
        OpenAiProviderProfile::Zai {
            official_hostname: true
        }
    );
    let ollama_endpoint = pipeline
        .prefix_cache_config
        .base_url
        .as_deref()
        .and_then(|url| reqwest::Url::parse(url).ok())
        .is_some_and(|url| {
            url.host_str()
                .is_some_and(|host| host == "ollama.com" || host.ends_with(".ollama.com"))
                || url.port() == Some(11434)
        });
    if pipeline.thinking_mandatory
        || (zai_endpoint && matches!(normalized_model.as_str(), "glm-5.3" | "glm-5.3-flash"))
    {
        return;
    }

    if is_glm_wire_model(Some(&normalized_model)) {
        if zai_endpoint {
            let existing = request
                .get("thinking")
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default();
            let mut thinking = existing;
            thinking.remove("enabled");
            thinking.insert("type".to_owned(), Value::String("disabled".to_owned()));
            request.insert("thinking".to_owned(), Value::Object(thinking));
        } else if ollama_endpoint {
            request.remove("thinking");
            request.insert(
                "reasoning_effort".to_owned(),
                Value::String("none".to_owned()),
            );
        }
    }

    if is_canopy_family_wire_model(Some(&normalized_model)) {
        match pipeline.provider_profile {
            OpenAiProviderProfile::DashScope
                if is_tiered_effort_wire_model(Some(&normalized_model)) =>
            {
                request.remove("enable_thinking");
                request.remove("thinking_budget");
                request.insert(
                    "reasoning_effort".to_owned(),
                    Value::String("none".to_owned()),
                );
            }
            OpenAiProviderProfile::DashScope => {
                request.insert("enable_thinking".to_owned(), Value::Bool(false));
            }
            _ => {
                request.remove("enable_thinking");
                let mut template = request
                    .get("chat_template_kwargs")
                    .and_then(Value::as_object)
                    .cloned()
                    .unwrap_or_default();
                template.insert("enable_thinking".to_owned(), Value::Bool(false));
                request.insert("chat_template_kwargs".to_owned(), Value::Object(template));
            }
        }
    }

    request.remove("reasoning");
    if request.get("reasoning_effort").and_then(Value::as_str) != Some("none") {
        request.remove("reasoning_effort");
    }
}

fn project_provider_response(response: Value) -> PromptModelResponse {
    let candidates = response
        .get("candidates")
        .and_then(Value::as_array)
        .map(|candidates| {
            candidates
                .iter()
                .map(|candidate| {
                    let parts = candidate
                        .pointer("/content/parts")
                        .and_then(Value::as_array)
                        .map(|parts| {
                            parts
                                .iter()
                                .map(|part| PromptResponsePart {
                                    text: part
                                        .get("text")
                                        .and_then(Value::as_str)
                                        .map(str::to_owned),
                                    thought: part.get("thought").cloned(),
                                })
                                .collect()
                        })
                        .unwrap_or_default();
                    PromptResponseCandidate {
                        finish_reason: candidate
                            .get("finishReason")
                            .and_then(Value::as_str)
                            .map(str::to_owned),
                        parts,
                    }
                })
                .collect()
        })
        .unwrap_or_default();
    PromptModelResponse {
        candidates,
        usage: response.get("usageMetadata").cloned(),
    }
}

async fn cancellable_provider_request<T, F>(
    cancellation: &CancellationToken,
    request: F,
) -> Result<T, String>
where
    T: Send,
    F: Future<Output = Result<T, crate::providers::openai_compatible::ProviderError>> + Send,
{
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => Err("Prompt hook execution aborted".to_owned()),
        result = request => result.map_err(|error| provider_error(error, cancellation)),
    }
}

fn provider_error(
    error: crate::providers::openai_compatible::ProviderError,
    cancellation: &CancellationToken,
) -> String {
    if cancellation.is_cancelled()
        || matches!(
            &error,
            crate::providers::openai_compatible::ProviderError::Cancelled
        )
    {
        "Prompt hook execution aborted".to_owned()
    } else {
        error.to_string()
    }
}
