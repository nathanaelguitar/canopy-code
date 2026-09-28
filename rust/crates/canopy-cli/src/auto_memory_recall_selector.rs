//! OpenAI-compatible host adapter for model-based auto-memory recall.
//!
//! The core resolver owns manifest construction, the 30-second deadline,
//! response validation, and deterministic fallback. This module supplies its
//! configured fast/main model choice and runs the JSON side query through the
//! CLI's regular OpenAI-compatible request pipeline.

use std::future::Future;
use std::pin::Pin;

use canopy_core::memory::{AutoMemoryRecallRequest, AutoMemoryRecallSelector};
use canopy_core::providers::openai_compatible::{OpenAiCompatibleClient, OpenAiCompatibleConfig};
use canopy_core::providers::openai_pipeline::{OpenAiPipelineConfig, build_openai_request};
use canopy_core::providers::prefix_caching::{OpenAiAuthMode, OpenAiPrefixCacheConfig};
use canopy_core::utils::cancellation::CancellationToken;
use serde_json::{Value, json};

pub struct OpenAiAutoMemoryRecallSelector {
    client: OpenAiCompatibleClient,
    pipeline: OpenAiPipelineConfig,
    fast_model: Option<String>,
    configured_model: String,
}

impl OpenAiAutoMemoryRecallSelector {
    pub fn new(
        provider: OpenAiCompatibleConfig,
        mut pipeline: OpenAiPipelineConfig,
        fast_model: Option<String>,
    ) -> Result<Self, String> {
        let configured_model = pipeline.request_context.model.clone();
        pipeline.prefix_cache_config = OpenAiPrefixCacheConfig {
            auth_mode: OpenAiAuthMode::OpenAi,
            base_url: Some(provider.base_url.clone()),
        };
        let client = OpenAiCompatibleClient::new(provider).map_err(|error| {
            format!("could not initialize auto-memory recall provider: {error}")
        })?;
        Ok(Self {
            client,
            pipeline,
            fast_model,
            configured_model,
        })
    }

    fn provider_request(&self, request: &AutoMemoryRecallRequest) -> Value {
        let model = self
            .fast_model
            .as_deref()
            .filter(|model| !model.trim().is_empty())
            .unwrap_or(&self.configured_model);
        let contents = request
            .contents
            .iter()
            .map(|content| {
                json!({
                    "role": content.role,
                    "parts": [{"text": content.text}],
                })
            })
            .collect::<Vec<_>>();
        let source_request = json!({
            "model": model,
            "contents": contents,
            "config": {
                "temperature": request.temperature,
                "responseMimeType": "application/json",
                "responseJsonSchema": request.schema,
                "systemInstruction": {"parts": [{"text": request.system_instruction}]},
            },
        });
        let mut pipeline = self.pipeline.clone();
        pipeline.request_context.model = model.to_owned();
        let mut provider_request = build_openai_request(
            &source_request,
            &pipeline,
            &format!("side-query:{}", request.purpose),
            false,
            |provider_request, _prompt_id| provider_request,
        );
        // The shared pipeline emits strict JSON Schema for the official
        // endpoint. Other OpenAI-compatible endpoints still get JSON mode;
        // the core selector validates the returned paths after parsing.
        if provider_request.get("response_format").is_none() {
            provider_request["response_format"] = json!({"type": "json_object"});
        }
        provider_request
    }
}

impl AutoMemoryRecallSelector for OpenAiAutoMemoryRecallSelector {
    fn select_json<'a>(
        &'a self,
        request: &'a AutoMemoryRecallRequest,
        cancellation: &'a CancellationToken,
    ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            let provider_request = self.provider_request(request);
            let response = tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Err("auto-memory recall side query cancelled".to_owned()),
                response = self.client.complete(&provider_request) => {
                    response.map_err(|error| error.to_string())?
                }
            };
            response_json(&response)
        })
    }
}

fn response_json(response: &Value) -> Result<Value, String> {
    let message = response.pointer("/choices/0/message").ok_or_else(|| {
        "auto-memory recall response did not contain assistant content".to_owned()
    })?;
    if let Some(parsed) = message.get("parsed") {
        return Ok(parsed.clone());
    }
    let content = message.get("content").ok_or_else(|| {
        "auto-memory recall response did not contain assistant content".to_owned()
    })?;
    match content {
        Value::String(text) => serde_json::from_str(text)
            .map_err(|error| format!("auto-memory recall response was not valid JSON: {error}")),
        Value::Array(parts) => {
            let text = parts
                .iter()
                .filter_map(|part| part.get("text").and_then(Value::as_str))
                .collect::<String>();
            serde_json::from_str(&text)
                .map_err(|error| format!("auto-memory recall response was not valid JSON: {error}"))
        }
        Value::Object(_) => Ok(content.clone()),
        _ => Err("auto-memory recall response content was not JSON text".to_owned()),
    }
}
