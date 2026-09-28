//! CLI host adapter for WebFetch side queries.
//!
//! This connects the core's provider-neutral side-query policy to Canopy's
//! bounded OpenAI-compatible transport. The module is kept separate from CLI
//! dispatch so hosts can wire it where the fetch invocation is assembled.

use canopy_core::providers::openai_compatible::{MAX_RESPONSE_BODY_BYTES, OpenAiCompatibleClient};
use canopy_core::providers::openai_pipeline::{OpenAiPipelineConfig, build_openai_request};
use canopy_core::providers::openai_profiles::response_parsing_options;
use canopy_core::providers::openai_stream::{OpenAiGeminiEventStream, OpenAiGeminiStreamError};
use canopy_core::providers::streaming_converter::OpenAiStreamContext;
use canopy_core::utils::cancellation::CancellationToken;
use canopy_core::utils::side_query::{
    SideQueryExecutor, SideQueryFuture, SideQueryMode, SideQueryRequest, SideQueryResponse,
    SideQueryTextResult,
};
use serde_json::{Map, Value, json};

/// Side-query executor backed by the CLI's configured OpenAI-compatible
/// client and the same provider request pipeline used for regular turns.
pub struct OpenAiSideQueryExecutor<'a> {
    client: &'a OpenAiCompatibleClient,
    pipeline: &'a OpenAiPipelineConfig,
}

impl<'a> OpenAiSideQueryExecutor<'a> {
    pub fn new(client: &'a OpenAiCompatibleClient, pipeline: &'a OpenAiPipelineConfig) -> Self {
        Self { client, pipeline }
    }

    fn provider_request(
        &self,
        request: &SideQueryRequest,
        streaming: bool,
    ) -> Result<Value, String> {
        let mut config = match &request.generation_config {
            Value::Null => Map::new(),
            Value::Object(config) => config.clone(),
            _ => return Err("side-query generation config must be an object".to_owned()),
        };
        if let Some(system_instruction) = request
            .system_instruction
            .as_ref()
            .filter(|instruction| !instruction.is_null())
        {
            config.insert("systemInstruction".to_owned(), system_instruction.clone());
        }

        let mut pipeline = self.pipeline.clone();
        pipeline.request_context.model.clone_from(&request.model);
        let source_request = json!({
            "model": request.model,
            "contents": request.contents,
            "config": Value::Object(config),
        });
        Ok(build_openai_request(
            &source_request,
            &pipeline,
            &request.prompt_id,
            streaming,
            |request, _prompt_id| request,
        ))
    }

    async fn complete(&self, provider_request: &Value) -> Result<SideQueryResponse, String> {
        let response = self
            .client
            .complete(provider_request)
            .await
            .map_err(|error| error.to_string())?;
        let content = response
            .pointer("/choices/0/message/content")
            .ok_or_else(|| "provider response did not contain assistant content".to_owned())?;
        let text = response_content_text(content)?;
        check_text_size(&text)?;
        let usage = response.get("usage").cloned();
        Ok(SideQueryResponse::Text(SideQueryTextResult { text, usage }))
    }

    async fn stream(
        &self,
        request: &SideQueryRequest,
        provider_request: &Value,
    ) -> Result<SideQueryResponse, String> {
        let mut context = OpenAiStreamContext::new(request.model.clone());
        context.response_parsing_options = response_parsing_options(self.pipeline.provider_profile);
        let mut stream = OpenAiGeminiEventStream::start(self.client, provider_request, context)
            .await
            .map_err(|error| error.to_string())?;
        let mut text = String::new();
        let mut usage = None;

        while let Some(chunk) = stream.next_chunk().await.map_err(stream_error)? {
            if let Some(parts) = chunk
                .response
                .pointer("/candidates/0/content/parts")
                .and_then(Value::as_array)
            {
                for part in parts {
                    if part.get("thought").and_then(Value::as_bool) == Some(true) {
                        continue;
                    }
                    if let Some(piece) = part.get("text").and_then(Value::as_str) {
                        append_bounded_text(&mut text, piece)?;
                    }
                }
            }
            if let Some(chunk_usage) = chunk.response.get("usageMetadata") {
                usage = Some(chunk_usage.clone());
            }
        }

        Ok(SideQueryResponse::Text(SideQueryTextResult { text, usage }))
    }
}

impl SideQueryExecutor for OpenAiSideQueryExecutor<'_> {
    fn execute<'a>(
        &'a self,
        request: SideQueryRequest,
        cancellation: CancellationToken,
    ) -> SideQueryFuture<'a> {
        Box::pin(async move {
            let streaming = match &request.mode {
                SideQueryMode::Text {
                    stream,
                    fail_closed,
                } => {
                    if fail_closed.is_some() {
                        return Err(
                            "OpenAI side-query adapter does not support fail_closed".to_owned()
                        );
                    }
                    *stream == Some(true)
                }
                SideQueryMode::Json { .. } => {
                    return Err("OpenAI side-query adapter only supports text mode".to_owned());
                }
            };
            if request.max_attempts.is_some_and(|attempts| attempts != 1) {
                return Err(
                    "OpenAI side-query adapter performs exactly one provider attempt".to_owned(),
                );
            }

            let provider_request = self.provider_request(&request, streaming)?;
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => Err("side query cancelled".to_owned()),
                result = async {
                    if streaming {
                        self.stream(&request, &provider_request).await
                    } else {
                        self.complete(&provider_request).await
                    }
                } => result,
            }
        })
    }
}

fn response_content_text(content: &Value) -> Result<String, String> {
    match content {
        Value::String(text) => Ok(text.clone()),
        Value::Array(parts) => {
            let mut text = String::new();
            for part in parts {
                if let Some(piece) = part
                    .get("text")
                    .and_then(Value::as_str)
                    .or_else(|| part.get("content").and_then(Value::as_str))
                {
                    append_bounded_text(&mut text, piece)?;
                }
            }
            Ok(text)
        }
        Value::Null => Ok(String::new()),
        _ => Err("provider response assistant content was not text".to_owned()),
    }
}

fn append_bounded_text(output: &mut String, text: &str) -> Result<(), String> {
    let next_size = output.len().saturating_add(text.len());
    if next_size > MAX_RESPONSE_BODY_BYTES {
        return Err(format!(
            "side-query text exceeds the {MAX_RESPONSE_BODY_BYTES}-byte response limit"
        ));
    }
    output.push_str(text);
    Ok(())
}

fn check_text_size(text: &str) -> Result<(), String> {
    if text.len() > MAX_RESPONSE_BODY_BYTES {
        Err(format!(
            "side-query text exceeds the {MAX_RESPONSE_BODY_BYTES}-byte response limit"
        ))
    } else {
        Ok(())
    }
}

fn stream_error(error: OpenAiGeminiStreamError) -> String {
    error.to_string()
}
