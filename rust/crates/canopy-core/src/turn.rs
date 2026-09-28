//! Canopy's response-to-server-event state for one model turn.
//!
//! This is the provider-independent part of `packages/core/src/core/turn.ts`.
//! Transport, retry policy, and tool execution are driven by the caller.

use std::collections::BTreeSet;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

const MAX_PENDING_TOOL_CALLS: usize = 4096;
const MAX_PENDING_CITATIONS: usize = 4096;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ThoughtSummary {
    pub subject: String,
    pub description: String,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ToolCallRequestInfo {
    #[serde(rename = "callId")]
    pub call_id: String,
    #[serde(rename = "providerCallId", skip_serializing_if = "Option::is_none")]
    pub provider_call_id: Option<String>,
    pub name: String,
    pub args: Value,
    #[serde(rename = "isClientInitiated")]
    pub is_client_initiated: bool,
    pub prompt_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_id: Option<String>,
    #[serde(rename = "wasOutputTruncated", skip_serializing_if = "Option::is_none")]
    pub was_output_truncated: Option<bool>,
    #[serde(rename = "goalContext", skip_serializing_if = "Option::is_none")]
    pub goal_context: Option<Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct FinishedEventValue {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(rename = "usageMetadata", skip_serializing_if = "Option::is_none")]
    pub usage_metadata: Option<Value>,
}

/// Server event shape emitted by the Canopy turn adapter.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(tag = "type")]
pub enum TurnEvent {
    #[serde(rename = "content")]
    Content {
        value: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        parts: Option<Vec<Value>>,
    },
    #[serde(rename = "thought")]
    Thought { value: ThoughtSummary },
    #[serde(rename = "tool_call_request")]
    ToolCallRequest { value: ToolCallRequestInfo },
    #[serde(rename = "citation")]
    Citation { value: String },
    #[serde(rename = "finished")]
    Finished { value: FinishedEventValue },
    #[serde(rename = "retry")]
    Retry {
        #[serde(rename = "retryInfo", skip_serializing_if = "Option::is_none")]
        retry_info: Option<Value>,
        #[serde(rename = "isContinuation", skip_serializing_if = "Option::is_none")]
        is_continuation: Option<bool>,
    },
    #[serde(rename = "model_fallback")]
    ModelFallback {
        #[serde(rename = "fromModel")]
        from_model: String,
        #[serde(rename = "toModel")]
        to_model: String,
        #[serde(rename = "statusCode", skip_serializing_if = "Option::is_none")]
        status_code: Option<u16>,
        #[serde(rename = "fallbackIndex")]
        fallback_index: u32,
    },
    #[serde(rename = "chat_compressed")]
    ChatCompressed {
        #[serde(skip_serializing_if = "Option::is_none")]
        value: Option<Value>,
    },
    #[serde(rename = "user_cancelled")]
    UserCancelled,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum TurnResponseError {
    #[error("turn accumulated more than {MAX_PENDING_TOOL_CALLS} pending tool calls")]
    TooManyPendingToolCalls,
    #[error("turn accumulated more than {MAX_PENDING_CITATIONS} pending citations")]
    TooManyPendingCitations,
}

/// State retained across provider chunks for one Canopy turn.
#[derive(Clone, Debug)]
pub struct Turn {
    pub pending_tool_calls: Vec<ToolCallRequestInfo>,
    pub finish_reason: Option<String>,
    prompt_id: String,
    goal_context: Option<Value>,
    pending_citations: BTreeSet<String>,
    current_response_id: Option<String>,
}

impl Turn {
    pub fn new(prompt_id: impl Into<String>, goal_context: Option<Value>) -> Self {
        Self {
            pending_tool_calls: Vec::new(),
            finish_reason: None,
            prompt_id: prompt_id.into(),
            goal_context,
            pending_citations: BTreeSet::new(),
            current_response_id: None,
        }
    }

    /// Convert one Gemini-shaped response chunk into ordered Canopy events.
    ///
    /// `openai_reasoning_thought` carries the non-serialized marker used by
    /// Canopy's TypeScript adapter for OpenAI reasoning parts. When set, the
    /// thought text is kept as a description instead of parsing `**subject**`.
    pub fn accept_response(
        &mut self,
        response: &Value,
        openai_reasoning_thought: bool,
    ) -> Result<Vec<TurnEvent>, TurnResponseError> {
        let calls = function_calls(response)?;
        if self.pending_tool_calls.len().saturating_add(calls.len()) > MAX_PENDING_TOOL_CALLS {
            return Err(TurnResponseError::TooManyPendingToolCalls);
        }
        let citation_list = citations(response);
        let new_citations: BTreeSet<&str> = citation_list
            .iter()
            .map(String::as_str)
            .filter(|citation| !self.pending_citations.contains(*citation))
            .collect();
        if self
            .pending_citations
            .len()
            .saturating_add(new_citations.len())
            > MAX_PENDING_CITATIONS
        {
            return Err(TurnResponseError::TooManyPendingCitations);
        }

        let mut events = Vec::new();

        if let Some(response_id) = response
            .get("responseId")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
        {
            self.current_response_id = Some(response_id.to_owned());
        }

        if let Some(summary) = thought_summary(response, openai_reasoning_thought) {
            events.push(TurnEvent::Thought { value: summary });
        }

        let text = response_text(response);
        let display_parts = display_content_parts(response);
        let has_image = display_parts
            .iter()
            .any(|part| part.get("inlineData").is_some());
        if !text.is_empty() || has_image {
            events.push(TurnEvent::Content {
                value: text,
                parts: has_image.then_some(display_parts),
            });
        }

        for call in calls {
            let request = self.make_tool_call_request(call);
            self.pending_tool_calls.push(request.clone());
            events.push(TurnEvent::ToolCallRequest { value: request });
        }

        for citation in citation_list {
            self.pending_citations.insert(citation);
        }

        let finish_reason = first_candidate(response)
            .and_then(|candidate| candidate.get("finishReason"))
            .and_then(Value::as_str)
            .filter(|reason| !reason.is_empty());
        if let Some(reason) = finish_reason {
            if reason == "MAX_TOKENS" {
                for call in &mut self.pending_tool_calls {
                    call.was_output_truncated = Some(true);
                }
            }

            if !self.pending_citations.is_empty() {
                events.push(TurnEvent::Citation {
                    value: format!(
                        "Citations:\n{}",
                        self.pending_citations
                            .iter()
                            .cloned()
                            .collect::<Vec<_>>()
                            .join("\n")
                    ),
                });
                self.pending_citations.clear();
            }

            self.finish_reason = Some(reason.to_owned());
            events.push(TurnEvent::Finished {
                value: FinishedEventValue {
                    reason: Some(reason.to_owned()),
                    usage_metadata: response
                        .get("usageMetadata")
                        .filter(|value| !value.is_null())
                        .cloned(),
                },
            });
        }

        Ok(events)
    }

    /// Retry clears output associated with the failed attempt but retains the
    /// current response ID, matching `Turn.run`.
    pub fn accept_retry(
        &mut self,
        retry_info: Option<Value>,
        is_continuation: Option<bool>,
    ) -> TurnEvent {
        self.pending_tool_calls.clear();
        self.pending_citations.clear();
        self.finish_reason = None;
        TurnEvent::Retry {
            retry_info,
            is_continuation,
        }
    }

    pub fn accept_model_fallback(
        &mut self,
        from_model: impl Into<String>,
        to_model: impl Into<String>,
        status_code: Option<u16>,
        fallback_index: u32,
    ) -> TurnEvent {
        self.pending_tool_calls.clear();
        self.pending_citations.clear();
        self.finish_reason = None;
        self.current_response_id = None;
        TurnEvent::ModelFallback {
            from_model: from_model.into(),
            to_model: to_model.into(),
            status_code,
            fallback_index,
        }
    }

    pub fn accept_compressed(&self, info: Option<Value>) -> TurnEvent {
        TurnEvent::ChatCompressed { value: info }
    }

    pub fn accept_cancelled(&self) -> TurnEvent {
        TurnEvent::UserCancelled
    }

    fn make_tool_call_request(&self, call: &Value) -> ToolCallRequestInfo {
        let raw_name = call.get("name").and_then(Value::as_str);
        let name = raw_name
            .filter(|name| !name.is_empty())
            .unwrap_or("undefined_tool_name")
            .to_owned();
        let call_id = call
            .get("id")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .unwrap_or_else(|| generated_call_id(raw_name.unwrap_or("undefined")));
        let provider_call_id = call
            .get("providerCallId")
            .and_then(Value::as_str)
            .or_else(|| call.get("id").and_then(Value::as_str))
            .filter(|id| !id.is_empty())
            .map(str::to_owned);
        let args = call
            .get("args")
            .filter(|args| js_truthy(args))
            .cloned()
            .unwrap_or_else(|| Value::Object(Default::default()));

        ToolCallRequestInfo {
            call_id,
            provider_call_id,
            name,
            args,
            is_client_initiated: false,
            prompt_id: self.prompt_id.clone(),
            response_id: self.current_response_id.clone(),
            was_output_truncated: None,
            goal_context: self.goal_context.clone(),
        }
    }
}

fn first_candidate(response: &Value) -> Option<&Value> {
    response.get("candidates")?.as_array()?.first()
}

fn candidate_parts(response: &Value) -> &[Value] {
    first_candidate(response)
        .and_then(|candidate| candidate.pointer("/content/parts"))
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default()
}

fn response_text(response: &Value) -> String {
    let mut text = String::new();
    for part in candidate_parts(response) {
        if js_truthy(part.get("thought").unwrap_or(&Value::Null)) {
            continue;
        }
        if let Some(part_text) = part.get("text").and_then(Value::as_str)
            && !part_text.is_empty()
        {
            text.push_str(part_text);
        }
    }
    text
}

fn display_content_parts(response: &Value) -> Vec<Value> {
    let mut display_parts = Vec::new();
    for part in candidate_parts(response) {
        if js_truthy(part.get("thought").unwrap_or(&Value::Null)) {
            continue;
        }
        if let Some(text) = part.get("text").and_then(Value::as_str)
            && !text.is_empty()
        {
            display_parts.push(serde_json::json!({"text": text}));
        }
        if let Some(inline_data) = part.get("inlineData") {
            let mime_type = inline_data.get("mimeType").and_then(Value::as_str);
            let data = inline_data.get("data").and_then(Value::as_str);
            if mime_type.is_some_and(|mime_type| {
                mime_type.trim().to_ascii_lowercase().starts_with("image/")
            }) && data.is_some_and(|data| !data.is_empty())
            {
                let mut display = serde_json::Map::new();
                display.insert("data".to_owned(), Value::String(data.unwrap().to_owned()));
                display.insert(
                    "mimeType".to_owned(),
                    Value::String(mime_type.unwrap().to_owned()),
                );
                if let Some(display_name) = inline_data.get("displayName").and_then(Value::as_str) {
                    display.insert(
                        "displayName".to_owned(),
                        Value::String(display_name.to_owned()),
                    );
                }
                display_parts.push(serde_json::json!({"inlineData": display}));
            }
        }
    }
    display_parts
}

fn thought_summary(response: &Value, openai_reasoning_thought: bool) -> Option<ThoughtSummary> {
    let mut thoughts = String::new();
    let mut found_thought = false;
    for part in candidate_parts(response) {
        if js_truthy(part.get("thought").unwrap_or(&Value::Null)) {
            found_thought = true;
            if let Some(text) = part.get("text").and_then(Value::as_str) {
                thoughts.push_str(text);
            }
        }
    }
    if !found_thought || thoughts.is_empty() {
        return None;
    }
    if openai_reasoning_thought {
        return Some(ThoughtSummary {
            subject: String::new(),
            description: thoughts,
        });
    }

    Some(parse_thought(&thoughts))
}

fn parse_thought(raw_text: &str) -> ThoughtSummary {
    let Some(start) = raw_text.find("**") else {
        return ThoughtSummary {
            subject: String::new(),
            description: raw_text.to_owned(),
        };
    };
    let Some(relative_end) = raw_text[start + 2..].find("**") else {
        return ThoughtSummary {
            subject: String::new(),
            description: raw_text.to_owned(),
        };
    };
    let end = start + 2 + relative_end;
    let subject = raw_text[start + 2..end].trim().to_owned();
    let description = format!("{}{}", &raw_text[..start], &raw_text[end + 2..])
        .trim()
        .to_owned();
    ThoughtSummary {
        subject,
        description,
    }
}

fn function_calls(response: &Value) -> Result<Vec<&Value>, TurnResponseError> {
    if let Some(value) = response.get("functionCalls") {
        let calls = value.as_array().map(Vec::as_slice).unwrap_or_default();
        if calls.len() > MAX_PENDING_TOOL_CALLS {
            return Err(TurnResponseError::TooManyPendingToolCalls);
        }
        return Ok(calls.iter().collect());
    }

    let mut calls = Vec::new();
    for part in candidate_parts(response) {
        if let Some(call) = part.get("functionCall") {
            if calls.len() == MAX_PENDING_TOOL_CALLS {
                return Err(TurnResponseError::TooManyPendingToolCalls);
            }
            calls.push(call);
        }
    }
    Ok(calls)
}

fn citations(response: &Value) -> Vec<String> {
    first_candidate(response)
        .and_then(|candidate| candidate.pointer("/citationMetadata/citations"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|citation| {
            let uri = citation.get("uri").and_then(Value::as_str)?;
            let title = citation
                .get("title")
                .and_then(Value::as_str)
                .filter(|title| !title.is_empty());
            Some(match title {
                Some(title) => format!("({title}) {uri}"),
                None => uri.to_owned(),
            })
        })
        .collect()
}

fn generated_call_id(name: &str) -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default();
    format!("{name}-{millis:013}-{}", Uuid::new_v4().simple())
}

fn js_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_f64().is_some_and(|number| number != 0.0),
        Value::String(value) => !value.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn emits_thought_content_and_ordered_image_display_parts() {
        let mut turn = Turn::new("prompt-1", None);
        let events = turn
            .accept_response(
                &json!({
                    "candidates": [{"content": {"parts": [
                        {"thought": true, "text": "before **subject** after"},
                        {"text": "hello "},
                        {"inlineData": {"mimeType": " Image/PNG ", "data": "aGVsbG8=", "displayName": "image"}},
                        {"text": "world"},
                        {"inlineData": {"mimeType": "application/pdf", "data": "bm8="}}
                    ]}}]
                }),
                false,
            )
            .unwrap();

        assert_eq!(
            events,
            vec![
                TurnEvent::Thought {
                    value: ThoughtSummary {
                        subject: "subject".to_owned(),
                        description: "before  after".to_owned(),
                    }
                },
                TurnEvent::Content {
                    value: "hello world".to_owned(),
                    parts: Some(vec![
                        json!({"text":"hello "}),
                        json!({"inlineData":{"data":"aGVsbG8=","mimeType":" Image/PNG ","displayName":"image"}}),
                        json!({"text":"world"}),
                    ]),
                },
            ]
        );
    }

    #[test]
    fn emits_tool_requests_with_response_and_goal_context_then_marks_truncation() {
        let mut turn = Turn::new("prompt-1", Some(json!({"goalId":"goal-1"})));
        let request_events = turn
            .accept_response(
                &json!({
                    "responseId":"response-1",
                    "functionCalls":[
                        {"id":"provider-call-1","name":"read","args":{"path":"a"}},
                        {"name":"write","args":false}
                    ]
                }),
                false,
            )
            .unwrap();

        assert_eq!(request_events.len(), 2);
        assert_eq!(turn.pending_tool_calls[0].call_id, "provider-call-1");
        assert_eq!(
            turn.pending_tool_calls[0].provider_call_id.as_deref(),
            Some("provider-call-1")
        );
        assert_eq!(
            turn.pending_tool_calls[0].response_id.as_deref(),
            Some("response-1")
        );
        assert_eq!(
            turn.pending_tool_calls[0].goal_context,
            Some(json!({"goalId":"goal-1"}))
        );
        assert_eq!(turn.pending_tool_calls[1].args, json!({}));
        assert!(turn.pending_tool_calls[1].call_id.starts_with("write-"));

        let finished = turn
            .accept_response(
                &json!({"candidates":[{"finishReason":"MAX_TOKENS"}]}),
                false,
            )
            .unwrap();
        assert_eq!(
            finished,
            vec![TurnEvent::Finished {
                value: FinishedEventValue {
                    reason: Some("MAX_TOKENS".to_owned()),
                    usage_metadata: None,
                }
            }]
        );
        assert!(
            turn.pending_tool_calls
                .iter()
                .all(|call| call.was_output_truncated == Some(true))
        );
    }

    #[test]
    fn citations_are_unique_sorted_and_wait_for_finish_reason() {
        let mut turn = Turn::new("prompt-1", None);
        assert!(
            turn.accept_response(
                &json!({
                    "candidates":[{"citationMetadata":{"citations":[
                        {"uri":"https://b.example","title":"B"},
                        {"uri":"https://a.example","title":"A"},
                        {"title":"missing uri"}
                    ]}}]
                }),
                false,
            )
            .unwrap()
            .is_empty()
        );

        let events = turn
            .accept_response(
                &json!({
                    "candidates":[{"finishReason":"STOP","citationMetadata":{"citations":[
                        {"uri":"https://b.example","title":"B"}
                    ]}}],
                    "usageMetadata":{"promptTokenCount":12}
                }),
                false,
            )
            .unwrap();
        assert_eq!(
            events,
            vec![
                TurnEvent::Citation {
                    value: "Citations:\n(A) https://a.example\n(B) https://b.example".to_owned()
                },
                TurnEvent::Finished {
                    value: FinishedEventValue {
                        reason: Some("STOP".to_owned()),
                        usage_metadata: Some(json!({"promptTokenCount":12})),
                    }
                }
            ]
        );
    }

    #[test]
    fn retry_retains_response_id_and_fallback_clears_it() {
        let mut turn = Turn::new("prompt-1", None);
        turn.accept_response(&json!({"responseId":"response-1"}), false)
            .unwrap();
        let _ = turn.accept_retry(Some(json!({"attempt":2})), Some(true));
        turn.accept_response(&json!({"functionCalls":[{"name":"read"}]}), false)
            .unwrap();
        assert_eq!(
            turn.pending_tool_calls[0].response_id.as_deref(),
            Some("response-1")
        );

        turn.accept_model_fallback("model-a", "model-b", Some(503), 1);
        turn.accept_response(&json!({"functionCalls":[{"name":"write"}]}), false)
            .unwrap();
        assert_eq!(turn.pending_tool_calls[0].response_id, None);
        assert_eq!(turn.finish_reason, None);
    }

    #[test]
    fn openai_reasoning_marker_disables_subject_parsing() {
        let mut turn = Turn::new("prompt-1", None);
        let events = turn
            .accept_response(
                &json!({"candidates":[{"content":{"parts":[
                    {"thought":true,"text":"**Analyze** the request"}
                ]}}]}),
                true,
            )
            .unwrap();
        assert_eq!(
            events,
            vec![TurnEvent::Thought {
                value: ThoughtSummary {
                    subject: String::new(),
                    description: "**Analyze** the request".to_owned(),
                }
            }]
        );
    }

    #[test]
    fn serializes_server_event_field_names() {
        let event = TurnEvent::ToolCallRequest {
            value: ToolCallRequestInfo {
                call_id: "call-1".to_owned(),
                provider_call_id: Some("provider-1".to_owned()),
                name: "read".to_owned(),
                args: json!({"path":"a"}),
                is_client_initiated: false,
                prompt_id: "prompt-1".to_owned(),
                response_id: Some("response-1".to_owned()),
                was_output_truncated: None,
                goal_context: None,
            },
        };
        assert_eq!(
            serde_json::to_value(event).unwrap(),
            json!({"type":"tool_call_request","value":{
                "callId":"call-1",
                "providerCallId":"provider-1",
                "name":"read",
                "args":{"path":"a"},
                "isClientInitiated":false,
                "prompt_id":"prompt-1",
                "response_id":"response-1"
            }})
        );
    }

    #[test]
    fn oversized_tool_batches_are_rejected_without_partial_turn_state() {
        let mut turn = Turn::new("prompt-1", None);
        let calls: Vec<Value> = (0..=MAX_PENDING_TOOL_CALLS)
            .map(|index| json!({"id":format!("call-{index}"),"name":"read"}))
            .collect();
        let response = json!({
            "responseId":"must-not-be-committed",
            "functionCalls":calls
        });

        assert_eq!(
            turn.accept_response(&response, false),
            Err(TurnResponseError::TooManyPendingToolCalls)
        );
        assert!(turn.pending_tool_calls.is_empty());
        let followup = turn
            .accept_response(&json!({"functionCalls":[{"name":"read"}]}), false)
            .unwrap();
        let TurnEvent::ToolCallRequest { value } = &followup[0] else {
            panic!("expected a tool call request")
        };
        assert_eq!(value.response_id, None);
    }
}
