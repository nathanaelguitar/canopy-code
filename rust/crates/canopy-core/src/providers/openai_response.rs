//! Response conversion between Canopy's Gemini-shaped API and OpenAI Chat
//! Completions.

use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{Map, Value, json};

use crate::utils::request_tokenizer::estimate_text_tokens;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ResponseParsingOptions {
    pub tagged_thinking_tags: bool,
    pub content_only_thinking_tag_leaks: bool,
}

#[derive(Clone, Debug, Default)]
pub struct OpenAiResponseContext {
    pub model: String,
    pub response_parsing_options: ResponseParsingOptions,
    pub tagged_thinking_parser: Option<TaggedThinkingParser>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GenAiUsageProvenance {
    pub cached_input_tokens_reported: bool,
    pub cache_creation_input_tokens: Option<u64>,
}

/// The JSON response plus metadata that Canopy keeps outside the wire object.
#[derive(Clone, Debug, PartialEq)]
pub struct ConvertedGeminiResponse {
    pub response: Value,
    pub usage_provenance: Option<GenAiUsageProvenance>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
enum ParserMode {
    #[default]
    Text,
    Thought,
}

/// Stateful parser used by both complete responses and future stream chunks.
/// Only an incomplete tag suffix is retained between calls.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TaggedThinkingParser {
    mode: ParserMode,
    buffer: String,
}

impl TaggedThinkingParser {
    pub fn parse(&mut self, chunk: &str, final_chunk: bool) -> Vec<Value> {
        self.buffer.push_str(chunk);
        let lower = self.buffer.to_ascii_lowercase();
        let bytes = self.buffer.as_bytes();
        let lower_bytes = lower.as_bytes();
        let mut parts = Vec::new();
        let mut segment_start = 0;
        let mut index = 0;

        while index < bytes.len() {
            let tags: &[&str] = match self.mode {
                ParserMode::Text => &["<think>", "<thinking>"],
                ParserMode::Thought => &["</think>", "</thinking>"],
            };
            if let Some(tag) = find_tag(lower_bytes, index, tags) {
                append_text_part(&mut parts, &self.buffer[segment_start..index], &self.mode);
                self.mode = match self.mode {
                    ParserMode::Text => ParserMode::Thought,
                    ParserMode::Thought => ParserMode::Text,
                };
                index += tag.len();
                segment_start = index;
                continue;
            }

            if !final_chunk && is_tag_prefix(lower_bytes, index, tags) {
                break;
            }

            index += self.buffer[index..]
                .chars()
                .next()
                .map(char::len_utf8)
                .unwrap_or(1);
        }

        if index < bytes.len() {
            append_text_part(&mut parts, &self.buffer[segment_start..index], &self.mode);
            self.buffer = self.buffer[index..].to_owned();
            return parts;
        }

        append_text_part(&mut parts, &self.buffer[segment_start..], &self.mode);
        self.buffer.clear();
        parts
    }
}

/// Convert the first Gemini candidate into an OpenAI completion-shaped value.
pub fn convert_gemini_response_to_openai(
    response: &Value,
    context: &OpenAiResponseContext,
) -> Value {
    let candidate = response
        .get("candidates")
        .and_then(Value::as_array)
        .and_then(|candidates| candidates.first());
    let parts = candidate
        .and_then(|candidate| candidate.get("content"))
        .and_then(|content| content.get("parts"))
        .and_then(Value::as_array);

    let mut thought_parts = Vec::new();
    let mut content_parts = Vec::new();
    let mut tool_calls = Vec::new();
    if let Some(parts) = parts {
        for part in parts {
            if let Some(text) = part.as_str() {
                content_parts.push(text.to_owned());
            } else if let Some(part) = part.as_object() {
                if let Some(text) = part
                    .get("text")
                    .and_then(Value::as_str)
                    .filter(|text| !text.is_empty())
                {
                    if part.get("thought").is_some_and(is_truthy) {
                        thought_parts.push(text.to_owned());
                    } else {
                        content_parts.push(text.to_owned());
                    }
                } else if let Some(function_call) =
                    part.get("functionCall").and_then(Value::as_object)
                {
                    let call_index = tool_calls.len();
                    let call_id = function_call
                        .get("id")
                        .and_then(Value::as_str)
                        .filter(|id| !id.is_empty())
                        .map(str::to_owned)
                        .unwrap_or_else(|| format!("call_{call_index}"));
                    let name = function_call
                        .get("name")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    let arguments = function_call
                        .get("args")
                        .filter(|args| is_truthy(args))
                        .unwrap_or(&Value::Null);
                    let arguments = if is_truthy(arguments) {
                        serde_json::to_string(arguments).unwrap_or_else(|_| "{}".to_owned())
                    } else {
                        "{}".to_owned()
                    };
                    tool_calls.push(json!({
                        "id": call_id,
                        "type": "function",
                        "function": {"name": name, "arguments": arguments}
                    }));
                }
            }
        }
    }

    let content = content_parts.concat();
    let reasoning_content = thought_parts.concat();
    let mut message = Map::new();
    message.insert("role".to_owned(), Value::String("assistant".to_owned()));
    message.insert(
        "content".to_owned(),
        if content.is_empty() {
            Value::Null
        } else {
            Value::String(content)
        },
    );
    message.insert("refusal".to_owned(), Value::Null);
    if !reasoning_content.is_empty() {
        message.insert(
            "reasoning_content".to_owned(),
            Value::String(reasoning_content),
        );
    }
    if !tool_calls.is_empty() {
        message.insert("tool_calls".to_owned(), Value::Array(tool_calls));
    }

    let usage_metadata = response.get("usageMetadata");
    let mut usage = Map::new();
    usage.insert(
        "prompt_tokens".to_owned(),
        truthy_or_zero(usage_metadata.and_then(|usage| usage.get("promptTokenCount"))),
    );
    usage.insert(
        "completion_tokens".to_owned(),
        truthy_or_zero(usage_metadata.and_then(|usage| usage.get("candidatesTokenCount"))),
    );
    usage.insert(
        "total_tokens".to_owned(),
        truthy_or_zero(usage_metadata.and_then(|usage| usage.get("totalTokenCount"))),
    );
    if let Some(cached_tokens) =
        usage_metadata.and_then(|usage| usage.get("cachedContentTokenCount"))
    {
        usage.insert(
            "prompt_tokens_details".to_owned(),
            json!({"cached_tokens": cached_tokens}),
        );
    }

    let now = now_millis();
    let created_ms = response
        .get("createTime")
        .filter(|value| is_truthy(value))
        .and_then(js_number)
        .filter(|value| value.is_finite())
        .unwrap_or(now as f64);
    let created_seconds = (created_ms / 1000.0).floor() as i64;
    let response_id = response
        .get("responseId")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| format!("gemini-{now}"));
    let model = response
        .get("modelVersion")
        .and_then(Value::as_str)
        .filter(|model| !model.is_empty())
        .unwrap_or(&context.model);

    json!({
        "id": response_id,
        "object": "chat.completion",
        "created": created_seconds,
        "model": model,
        "choices": [{
            "index": 0,
            "message": Value::Object(message),
            "finish_reason": map_gemini_finish_reason_to_openai(
                candidate.and_then(|candidate| candidate.get("finishReason")).and_then(Value::as_str)
            ),
            "logprobs": null
        }],
        "usage": Value::Object(usage)
    })
}

/// Convert an OpenAI completion response into Gemini's response JSON shape.
pub fn convert_openai_response_to_gemini(
    response: &Value,
    context: &mut OpenAiResponseContext,
) -> ConvertedGeminiResponse {
    let choice = response
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|choices| choices.first());
    let message = choice.and_then(|choice| choice.get("message"));
    let reasoning_text = message
        .and_then(|message| message.get("reasoning_content"))
        .filter(|value| !value.is_null())
        .or_else(|| message.and_then(|message| message.get("reasoning")))
        .and_then(Value::as_str)
        .unwrap_or("");

    let (candidates, thought_parts) = if let Some(choice) = choice {
        let mut parts = Vec::new();
        let content = message
            .and_then(|message| message.get("content"))
            .and_then(Value::as_str)
            .unwrap_or("");
        let text_parts = parse_openai_text_to_parts(content, context);
        let content_has_thought = text_parts
            .iter()
            .any(|part| part.get("thought").and_then(Value::as_bool) == Some(true));
        if !reasoning_text.is_empty() && !content_has_thought {
            parts.push(json!({"text": reasoning_text, "thought": true}));
        }
        parts.extend(text_parts);

        if let Some(tool_calls) = message
            .and_then(|message| message.get("tool_calls"))
            .and_then(Value::as_array)
        {
            for tool_call in tool_calls {
                let Some(function) = tool_call.get("function").and_then(Value::as_object) else {
                    continue;
                };
                let arguments = function
                    .get("arguments")
                    .and_then(Value::as_str)
                    .filter(|arguments| !arguments.is_empty())
                    .and_then(|arguments| serde_json::from_str::<Value>(arguments).ok())
                    .unwrap_or_else(|| json!({}));
                parts.push(json!({
                    "functionCall": {
                        "id": tool_call.get("id").and_then(Value::as_str).unwrap_or(""),
                        "name": function.get("name").and_then(Value::as_str).unwrap_or(""),
                        "args": arguments
                    }
                }));
            }
        }

        let finish_reason = choice
            .get("finish_reason")
            .and_then(Value::as_str)
            .filter(|reason| !reason.is_empty())
            .unwrap_or("stop");
        (
            vec![json!({
                "content": {"parts": parts, "role": "model"},
                "finishReason": map_openai_finish_reason_to_gemini(finish_reason),
                "index": 0,
                "safetyRatings": []
            })],
            true,
        )
    } else {
        (Vec::new(), false)
    };

    let now = now_millis();
    let created = response
        .get("created")
        .filter(|value| is_truthy(value))
        .map(js_string)
        .unwrap_or_else(|| now.to_string());
    let response_id = response.get("id").and_then(Value::as_str).unwrap_or("");
    let model = response
        .get("model")
        .and_then(Value::as_str)
        .filter(|model| !model.is_empty());
    let mut converted = Map::new();
    converted.insert("candidates".to_owned(), json!(candidates));
    converted.insert(
        "responseId".to_owned(),
        Value::String(response_id.to_owned()),
    );
    converted.insert("createTime".to_owned(), Value::String(created));
    if let Some(model) = model {
        converted.insert("modelVersion".to_owned(), Value::String(model.to_owned()));
    }
    converted.insert("promptFeedback".to_owned(), json!({"safetyRatings": []}));

    let (usage_metadata, usage_provenance) = convert_usage_to_gemini(
        response.get("usage"),
        if thought_parts { reasoning_text } else { "" },
    );
    if let Some(usage_metadata) = usage_metadata {
        converted.insert("usageMetadata".to_owned(), usage_metadata);
    }

    ConvertedGeminiResponse {
        response: Value::Object(converted),
        usage_provenance,
    }
}

fn convert_usage_to_gemini(
    usage: Option<&Value>,
    reasoning_text: &str,
) -> (Option<Value>, Option<GenAiUsageProvenance>) {
    let Some(usage) = usage else {
        return (None, None);
    };

    let prompt_tokens = usage
        .get("prompt_tokens")
        .and_then(js_number)
        .unwrap_or(0.0);
    let completion_tokens = usage
        .get("completion_tokens")
        .and_then(js_number)
        .unwrap_or(0.0);
    let total_tokens = usage.get("total_tokens").and_then(js_number).unwrap_or(0.0);
    let prompt_cached = usage
        .get("prompt_tokens_details")
        .and_then(|details| details.get("cached_tokens"));
    let top_level_cached = usage.get("cached_tokens");
    let cached_tokens = prompt_cached
        .filter(|value| !value.is_null())
        .or_else(|| top_level_cached.filter(|value| !value.is_null()))
        .cloned()
        .unwrap_or_else(|| json!(0));
    let cached_input_tokens_reported = prompt_cached.is_some_and(Value::is_number)
        || top_level_cached.is_some_and(Value::is_number);

    let provider_thought_tokens = usage
        .get("completion_tokens_details")
        .and_then(|details| details.get("reasoning_tokens"))
        .filter(|value| value.is_number());
    let thought_tokens = provider_thought_tokens.cloned().unwrap_or_else(|| {
        let estimate = estimate_text_tokens(reasoning_text);
        let bounded = if completion_tokens > 0.0 {
            estimate.min(completion_tokens as u64)
        } else {
            estimate
        };
        json!(bounded)
    });
    let has_token_breakdown =
        total_tokens == 0.0 || prompt_tokens != 0.0 || completion_tokens != 0.0;
    let mut metadata = Map::new();
    if has_token_breakdown {
        metadata.insert("promptTokenCount".to_owned(), number_value(prompt_tokens));
        metadata.insert(
            "candidatesTokenCount".to_owned(),
            number_value(completion_tokens),
        );
    }
    metadata.insert("totalTokenCount".to_owned(), number_value(total_tokens));
    metadata.insert("cachedContentTokenCount".to_owned(), cached_tokens);
    metadata.insert("thoughtsTokenCount".to_owned(), thought_tokens);

    (
        Some(Value::Object(metadata)),
        Some(GenAiUsageProvenance {
            cached_input_tokens_reported,
            cache_creation_input_tokens: None,
        }),
    )
}

fn parse_openai_text_to_parts(text: &str, context: &mut OpenAiResponseContext) -> Vec<Value> {
    if !context.response_parsing_options.tagged_thinking_tags {
        return if text.is_empty() {
            Vec::new()
        } else {
            vec![json!({"text": text})]
        };
    }
    if let Some(parser) = context.tagged_thinking_parser.as_mut() {
        parser.parse(text, true)
    } else {
        TaggedThinkingParser::default().parse(text, true)
    }
}

fn append_text_part(parts: &mut Vec<Value>, text: &str, mode: &ParserMode) {
    if text.is_empty() {
        return;
    }
    if *mode == ParserMode::Thought {
        parts.push(json!({"text": text, "thought": true}));
    } else {
        parts.push(json!({"text": text}));
    }
}

fn find_tag<'a>(lower: &[u8], offset: usize, tags: &'a [&'static str]) -> Option<&'a str> {
    tags.iter().copied().find(|tag| {
        lower
            .get(offset..offset + tag.len())
            .is_some_and(|candidate| candidate == tag.as_bytes())
    })
}

fn is_tag_prefix(lower: &[u8], offset: usize, tags: &[&str]) -> bool {
    let remaining = &lower[offset..];
    !remaining.is_empty()
        && remaining.len() <= "</thinking>".len()
        && tags.iter().any(|tag| tag.as_bytes().starts_with(remaining))
}

fn map_gemini_finish_reason_to_openai(reason: Option<&str>) -> &'static str {
    match reason {
        None | Some("") => "stop",
        Some("STOP") => "stop",
        Some("MAX_TOKENS") => "length",
        Some(
            "SAFETY"
            | "RECITATION"
            | "BLOCKLIST"
            | "PROHIBITED_CONTENT"
            | "SPII"
            | "IMAGE_SAFETY"
            | "IMAGE_RECITATION"
            | "IMAGE_PROHIBITED_CONTENT"
            | "IMAGE_OTHER",
        ) => "content_filter",
        Some("NO_IMAGE") => "stop",
        _ => "stop",
    }
}

fn map_openai_finish_reason_to_gemini(reason: &str) -> &'static str {
    match reason {
        "stop" | "function_call" | "tool_calls" => "STOP",
        "length" => "MAX_TOKENS",
        "content_filter" => "SAFETY",
        _ => "FINISH_REASON_UNSPECIFIED",
    }
}

fn truthy_or_zero(value: Option<&Value>) -> Value {
    value
        .filter(|value| is_truthy(value))
        .cloned()
        .unwrap_or_else(|| json!(0))
}

fn number_value(value: f64) -> Value {
    if value.is_finite() && value.fract() == 0.0 {
        if value >= 0.0 && value <= u64::MAX as f64 {
            return json!(value as u64);
        }
        if value >= i64::MIN as f64 && value < 0.0 {
            return json!(value as i64);
        }
    }
    serde_json::Number::from_f64(value)
        .map(Value::Number)
        .unwrap_or_else(|| json!(0))
}

fn js_number(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(string) if string.trim().is_empty() => Some(0.0),
        Value::String(string) => string.parse().ok(),
        Value::Bool(value) => Some(if *value { 1.0 } else { 0.0 }),
        Value::Null => Some(0.0),
        Value::Array(_) | Value::Object(_) => None,
    }
}

fn js_string(value: &Value) -> String {
    match value {
        Value::Null => "null".to_owned(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => value.clone(),
        Value::Array(values) => values
            .iter()
            .map(|value| {
                if value.is_null() {
                    String::new()
                } else {
                    js_string(value)
                }
            })
            .collect::<Vec<_>>()
            .join(","),
        Value::Object(_) => "[object Object]".to_owned(),
    }
}

fn is_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_f64().is_some_and(|value| value != 0.0),
        Value::String(value) => !value.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}

fn now_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_gemini_candidate_content_tools_reasoning_and_usage() {
        let response = json!({
            "responseId": "response-1",
            "createTime": "123000",
            "modelVersion": "gemini-test",
            "candidates": [{
                "content": {"role": "model", "parts": [
                    "Answer",
                    {"text": "Reasoning", "thought": true},
                    {"functionCall": {"id": "call-1", "name": "read_file", "args": {"path": "a"}}}
                ]},
                "finishReason": "MAX_TOKENS"
            }],
            "usageMetadata": {
                "promptTokenCount": 5,
                "candidatesTokenCount": 6,
                "totalTokenCount": 11,
                "cachedContentTokenCount": 2
            }
        });

        let converted = convert_gemini_response_to_openai(
            &response,
            &OpenAiResponseContext {
                model: "fallback-model".to_owned(),
                ..OpenAiResponseContext::default()
            },
        );

        assert_eq!(converted["id"], "response-1");
        assert_eq!(converted["created"], 123);
        assert_eq!(converted["model"], "gemini-test");
        assert_eq!(converted["choices"][0]["finish_reason"], "length");
        assert_eq!(converted["choices"][0]["message"]["content"], "Answer");
        assert_eq!(
            converted["choices"][0]["message"]["reasoning_content"],
            "Reasoning"
        );
        assert_eq!(
            converted["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"],
            r#"{"path":"a"}"#
        );
        assert_eq!(
            converted["usage"]["prompt_tokens_details"]["cached_tokens"],
            2
        );
    }

    #[test]
    fn converts_openai_content_tools_usage_and_cache_provenance() {
        let response = json!({
            "id": "chatcmpl-1",
            "created": 123,
            "model": "provider-model",
            "choices": [{
                "message": {
                    "role": "assistant",
                    "content": "answer",
                    "reasoning_content": "先仔细想",
                    "tool_calls": [{
                        "id": "call-1",
                        "function": {"name": "read_file", "arguments": "{\"path\":\"a\"}"}
                    }]
                },
                "finish_reason": "tool_calls"
            }],
            "usage": {
                "prompt_tokens": 1,
                "completion_tokens": 10,
                "total_tokens": 11,
                "prompt_tokens_details": {"cached_tokens": 0}
            }
        });
        let mut context = OpenAiResponseContext::default();

        let converted = convert_openai_response_to_gemini(&response, &mut context);

        assert_eq!(converted.response["responseId"], "chatcmpl-1");
        assert_eq!(converted.response["createTime"], "123");
        assert_eq!(converted.response["modelVersion"], "provider-model");
        assert_eq!(converted.response["candidates"][0]["finishReason"], "STOP");
        assert_eq!(
            converted.response["candidates"][0]["content"]["parts"][0]["thought"],
            true
        );
        assert_eq!(
            converted.response["candidates"][0]["content"]["parts"][1]["text"],
            "answer"
        );
        assert_eq!(
            converted.response["candidates"][0]["content"]["parts"][2]["functionCall"]["args"]["path"],
            "a"
        );
        assert_eq!(converted.response["usageMetadata"]["thoughtsTokenCount"], 5);
        assert_eq!(
            converted.response["usageMetadata"]["cachedContentTokenCount"],
            0
        );
        assert_eq!(
            converted.usage_provenance,
            Some(GenAiUsageProvenance {
                cached_input_tokens_reported: true,
                cache_creation_input_tokens: None
            })
        );
    }

    #[test]
    fn tagged_thinking_parser_handles_split_tags_and_unicode_text() {
        let mut parser = TaggedThinkingParser::default();
        assert_eq!(parser.parse("🙂<think", false), vec![json!({"text": "🙂"})]);
        let parts = parser.parse("ing>reason</thinking>answer", true);

        assert_eq!(
            parts,
            vec![
                json!({"text": "reason", "thought": true}),
                json!({"text": "answer"})
            ]
        );
    }
}
