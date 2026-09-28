//! Connects the bounded OpenAI-compatible SSE transport to the stateful
//! Gemini-shaped stream converter.

use serde_json::Value;
use thiserror::Error;

use crate::providers::openai_compatible::{
    OpenAiCompatibleClient, OpenAiEventStream, ProviderError,
};
use crate::providers::streaming_converter::{
    ConvertedStreamChunk, OpenAiStreamContext, StreamConversionError,
    convert_openai_chunk_to_gemini,
};
use crate::turn::{Turn, TurnEvent, TurnResponseError};

#[derive(Debug, Error)]
pub enum OpenAiGeminiStreamError {
    #[error(transparent)]
    Provider(#[from] ProviderError),
    #[error(transparent)]
    Conversion(#[from] StreamConversionError),
    #[error("provider returned a stream error: {message}")]
    StreamContent { message: String },
}

#[derive(Debug, Error)]
pub enum OpenAiGeminiTurnError {
    #[error(transparent)]
    Stream(#[from] OpenAiGeminiStreamError),
    #[error(transparent)]
    Turn(#[from] TurnResponseError),
}

/// One converted provider stream. Keep this value local to a single request;
/// its parser and delta-normalization state are intentionally not shareable.
pub struct OpenAiGeminiEventStream {
    source: Option<OpenAiEventStream>,
    context: OpenAiStreamContext,
    done: bool,
    source_finished: bool,
    pending_finish: Option<ConvertedStreamChunk>,
    finish_yielded: bool,
    pending_finish_protocol_tag_sanitized: bool,
}

impl OpenAiGeminiEventStream {
    pub async fn start(
        client: &OpenAiCompatibleClient,
        request: &Value,
        context: OpenAiStreamContext,
    ) -> Result<Self, ProviderError> {
        let source = client.stream(request).await?;
        Ok(Self {
            source: Some(source),
            context,
            done: false,
            source_finished: false,
            pending_finish: None,
            finish_yielded: false,
            pending_finish_protocol_tag_sanitized: false,
        })
    }

    /// Decode and convert the next SSE event. `None` means the provider sent
    /// `[DONE]` or closed the stream cleanly.
    pub async fn next_chunk(
        &mut self,
    ) -> Result<Option<ConvertedStreamChunk>, OpenAiGeminiStreamError> {
        if self.done {
            return Ok(None);
        }
        loop {
            if self.source_finished {
                if !self.finish_yielded {
                    if let Some(pending) = self.pending_finish.as_ref() {
                        self.finish_yielded = true;
                        return Ok(Some(pending.clone()));
                    }
                }
                self.finish();
                return Ok(None);
            }

            let event = match self.source.as_mut() {
                Some(source) => match source.next_event().await {
                    Ok(event) => event,
                    Err(error) => {
                        self.finish();
                        return Err(error.into());
                    }
                },
                None => None,
            };
            let Some(event) = event else {
                self.source = None;
                self.source_finished = true;
                match self.context.finish_stream() {
                    Ok(Some(tail)) => return Ok(Some(tail)),
                    Ok(None) => continue,
                    Err(error) => {
                        self.finish();
                        return Err(error.into());
                    }
                }
            };

            if let Some(error) = stream_content_error(&event.data) {
                self.finish();
                return Err(OpenAiGeminiStreamError::StreamContent { message: error });
            }
            let mut current = match convert_openai_chunk_to_gemini(&event.data, &mut self.context) {
                Ok(chunk) => chunk,
                Err(error) => {
                    self.finish();
                    return Err(error.into());
                }
            };
            let sanitized = self.context.protocol_tag_sanitized.take().is_some();

            if is_empty_response(&current) {
                continue;
            }
            if self.pending_finish_protocol_tag_sanitized
                && self.pending_finish.is_some()
                && !has_finish_reason(&current.response)
                && has_any_parts(&current.response)
            {
                self.finish();
                return Err(StreamConversionError::ProtocolTagLeak.into());
            }

            if self.finish_yielded {
                if let (Some(pending), Some(usage)) = (
                    self.pending_finish.as_mut(),
                    current
                        .response
                        .get("usageMetadata")
                        .filter(|usage| is_truthy(usage)),
                ) {
                    pending.response["usageMetadata"] = usage.clone();
                    if current.usage_provenance.is_some() {
                        pending.usage_provenance = current.usage_provenance.take();
                    }
                }
                continue;
            }

            if self.pending_finish.is_none() && has_finish_reason(&current.response) && sanitized {
                self.pending_finish_protocol_tag_sanitized = true;
            }

            if has_finish_reason(&current.response) {
                if let Some(pending) = self.pending_finish.as_mut() {
                    merge_duplicate_finish(pending, current);
                } else {
                    self.pending_finish = Some(current);
                }
                continue;
            }

            if let Some(pending) = self.pending_finish.as_mut() {
                merge_following_chunk(pending, current);
                self.finish_yielded = true;
                return Ok(self.pending_finish.clone());
            }
            return Ok(Some(current));
        }
    }

    /// Pull converted chunks through Canopy's turn event adapter until one
    /// produces events or the provider stream ends.
    pub async fn next_turn_events(
        &mut self,
        turn: &mut Turn,
    ) -> Result<Option<Vec<TurnEvent>>, OpenAiGeminiTurnError> {
        loop {
            let Some(chunk) = self.next_chunk().await? else {
                return Ok(None);
            };
            let events = turn.accept_response(&chunk.response, chunk.openai_reasoning_thought)?;
            if !events.is_empty() {
                return Ok(Some(events));
            }
        }
    }

    pub fn context(&self) -> &OpenAiStreamContext {
        &self.context
    }

    fn finish(&mut self) {
        self.source = None;
        self.context = OpenAiStreamContext::default();
        self.pending_finish = None;
        self.done = true;
    }
}

fn stream_content_error(chunk: &Value) -> Option<String> {
    let choice = chunk.get("choices")?.as_array()?.first()?;
    if choice.get("finish_reason").and_then(Value::as_str) != Some("error_finish") {
        return None;
    }
    let content = choice
        .pointer("/delta/content")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim();
    Some(if content.is_empty() {
        "Unknown stream error".to_owned()
    } else {
        content.to_owned()
    })
}

fn is_empty_response(chunk: &ConvertedStreamChunk) -> bool {
    let candidate = chunk
        .response
        .get("candidates")
        .and_then(Value::as_array)
        .and_then(|candidates| candidates.first());
    let parts_empty = candidate
        .and_then(|candidate| candidate.pointer("/content/parts"))
        .and_then(Value::as_array)
        .is_none_or(Vec::is_empty);
    let no_finish = candidate
        .and_then(|candidate| candidate.get("finishReason"))
        .is_none_or(|reason| !is_truthy(reason));
    parts_empty
        && no_finish
        && chunk
            .response
            .get("usageMetadata")
            .is_none_or(|usage| !is_truthy(usage))
        && chunk.tool_call_preparations.is_empty()
}

fn has_finish_reason(response: &Value) -> bool {
    response
        .pointer("/candidates/0/finishReason")
        .is_some_and(is_truthy)
}

fn has_any_parts(response: &Value) -> bool {
    response
        .get("candidates")
        .and_then(Value::as_array)
        .is_some_and(|candidates| {
            candidates.iter().any(|candidate| {
                candidate
                    .pointer("/content/parts")
                    .and_then(Value::as_array)
                    .is_some_and(|parts| !parts.is_empty())
            })
        })
}

fn merge_duplicate_finish(pending: &mut ConvertedStreamChunk, current: ConvertedStreamChunk) {
    pending.openai_reasoning_thought |= current.openai_reasoning_thought;
    if let Some(usage) = current
        .response
        .get("usageMetadata")
        .filter(|usage| is_truthy(usage))
    {
        pending.response["usageMetadata"] = usage.clone();
        pending.usage_provenance = current.usage_provenance;
    }
    if let Some(model) = current
        .response
        .get("modelVersion")
        .filter(|model| is_truthy(model))
    {
        pending.response["modelVersion"] = model.clone();
    }
}

fn merge_following_chunk(pending: &mut ConvertedStreamChunk, current: ConvertedStreamChunk) {
    let response_id = current
        .response
        .get("responseId")
        .filter(|value| is_truthy(value))
        .or_else(|| pending.response.get("responseId"))
        .cloned();
    let create_time = current
        .response
        .get("createTime")
        .filter(|value| is_truthy(value))
        .or_else(|| pending.response.get("createTime"))
        .cloned();
    let model = current
        .response
        .get("modelVersion")
        .filter(|value| is_truthy(value))
        .or_else(|| pending.response.get("modelVersion"))
        .cloned();
    let prompt_feedback = current
        .response
        .get("promptFeedback")
        .filter(|value| is_truthy(value))
        .or_else(|| pending.response.get("promptFeedback"))
        .cloned();
    if let Some(usage) = current
        .response
        .get("usageMetadata")
        .filter(|usage| is_truthy(usage))
    {
        pending.response["usageMetadata"] = usage.clone();
        pending.usage_provenance = current.usage_provenance;
    }
    for (key, value) in [
        ("responseId", response_id),
        ("createTime", create_time),
        ("modelVersion", model),
        ("promptFeedback", prompt_feedback),
    ] {
        if let Some(value) = value {
            pending.response[key] = value;
        }
    }
}

fn is_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_f64().is_some_and(|number| number != 0.0),
        Value::String(value) => !value.is_empty(),
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn bounded_sse_events_flow_through_stream_conversion() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind local provider");
        let address = listener.local_addr().expect("provider address");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept request");
            let mut request = Vec::new();
            let mut buffer = [0u8; 2048];
            let mut expected = None;
            loop {
                let count = socket.read(&mut buffer).await.expect("read request");
                if count == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..count]);
                if expected.is_none() {
                    if let Some(header_end) =
                        request.windows(4).position(|bytes| bytes == b"\r\n\r\n")
                    {
                        let headers = String::from_utf8_lossy(&request[..header_end]);
                        let body_len = headers
                            .lines()
                            .filter_map(|line| line.split_once(':'))
                            .find_map(|(name, value)| {
                                name.eq_ignore_ascii_case("content-length")
                                    .then(|| value.trim().parse::<usize>().ok())
                                    .flatten()
                            })
                            .unwrap_or_default();
                        expected = Some(header_end + 4 + body_len);
                    }
                }
                if expected.is_some_and(|length| request.len() >= length) {
                    break;
                }
            }

            let body = concat!(
                "data: {\"id\":\"chunk-1\",\"created\":1,\"choices\":[{\"delta\":{\"content\":\"hi\",\"reasoning_content\":\"**thinking**\"},\"finish_reason\":null}]}\n\n",
                "data: {\"id\":\"chunk-2\",\"created\":1,\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
                "data: {\"id\":\"chunk-3\",\"choices\":[],\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":1,\"total_tokens\":3}}\n\n",
                "data: [DONE]\n\n"
            );
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body,
            );
            socket
                .write_all(response.as_bytes())
                .await
                .expect("write SSE response");
        });

        let client = OpenAiCompatibleClient::new(
            crate::providers::openai_compatible::OpenAiCompatibleConfig {
                base_url: format!("http://{address}/v1"),
                stream_idle_timeout: None,
                stream_max_lifetime: None,
                ..Default::default()
            },
        )
        .expect("create provider client");
        let mut stream = OpenAiGeminiEventStream::start(
            &client,
            &serde_json::json!({"model":"test","messages":[]}),
            OpenAiStreamContext::new("test"),
        )
        .await
        .expect("start stream");

        let mut turn = Turn::new("prompt-1", None);
        let first = stream
            .next_turn_events(&mut turn)
            .await
            .expect("first turn events")
            .expect("events present");
        assert_eq!(
            first,
            vec![
                TurnEvent::Thought {
                    value: crate::turn::ThoughtSummary {
                        subject: String::new(),
                        description: "**thinking**".to_owned(),
                    }
                },
                TurnEvent::Content {
                    value: "hi".to_owned(),
                    parts: None,
                }
            ]
        );
        let finished = stream
            .next_turn_events(&mut turn)
            .await
            .expect("finish event")
            .expect("events present");
        assert_eq!(
            finished,
            vec![TurnEvent::Finished {
                value: crate::turn::FinishedEventValue {
                    reason: Some("STOP".to_owned()),
                    usage_metadata: Some(serde_json::json!({
                        "thoughtsTokenCount": 1,
                        "totalTokenCount": 3,
                        "cachedContentTokenCount": 0,
                        "promptTokenCount": 2,
                        "candidatesTokenCount": 1
                    })),
                }
            }]
        );
        assert!(
            stream
                .next_turn_events(&mut turn)
                .await
                .expect("done event")
                .is_none()
        );
        server.await.expect("provider server task");
    }

    #[test]
    fn embedded_stream_error_is_preserved_as_a_readable_message() {
        assert_eq!(
            stream_content_error(&serde_json::json!({
                "choices":[{"delta":{"content":" provider failed "},"finish_reason":"error_finish"}]
            })),
            Some("provider failed".to_owned())
        );
        assert_eq!(
            stream_content_error(&serde_json::json!({
                "choices":[{"delta":{},"finish_reason":"error_finish"}]
            })),
            Some("Unknown stream error".to_owned())
        );
        assert_eq!(
            stream_content_error(&serde_json::json!({
                "choices":[{"delta":{"content":"ok"},"finish_reason":"stop"}]
            })),
            None
        );
    }
}
