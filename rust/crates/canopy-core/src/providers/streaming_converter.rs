//! Stateful conversion of OpenAI-compatible stream chunks into Canopy's
//! Gemini-shaped response values.
//!
//! All mutable state is scoped to one provider stream. Retained buffers are
//! capped so a long or malformed stream cannot grow memory without bound.

use std::collections::HashSet;

use serde_json::{Value, json};
use thiserror::Error;

use crate::providers::openai_compatible::MAX_RESPONSE_BODY_BYTES;
use crate::providers::openai_response::{GenAiUsageProvenance, TaggedThinkingParser};
use crate::providers::streaming_tool_call_parser::StreamingToolCallParser;

const CUMULATIVE_DELTA_EXACT_REPEAT_MIN_LENGTH: usize = 64;
const CUMULATIVE_DETECTION_WINDOW_UNITS: usize = 1024;
const MAX_PENDING_STREAM_BYTES: usize = MAX_RESPONSE_BODY_BYTES;
const MAX_PENDING_STREAM_PARTS: usize = 4096;
const TOKEN_ESTIMATE_UNITS_PER_TOKEN: u64 = 20;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StreamingTextDeltaState {
    pub emitted_text: String,
    /// JavaScript string length is UTF-16 code units; retain that contract.
    pub emitted_length: usize,
    pub emitted_token_units: u64,
    pub cumulative_mode: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StreamResponseParsingOptions {
    pub tagged_thinking_tags: bool,
    pub content_only_thinking_tag_leaks: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ToolCallPreparation {
    pub call_id: String,
    pub tool_name: String,
}

#[derive(Clone, Debug, Default)]
struct ThinkingTagCandidate {
    text: String,
    closing_tag_name: Option<String>,
}

/// Per-request state. Construct a fresh value for every stream.
#[derive(Clone, Debug, Default)]
pub struct OpenAiStreamContext {
    pub model: String,
    pub response_parsing_options: StreamResponseParsingOptions,
    pub tool_call_parser: StreamingToolCallParser,
    pub tagged_thinking_parser: Option<TaggedThinkingParser>,
    pub text_delta_state: StreamingTextDeltaState,
    pub reasoning_delta_state: StreamingTextDeltaState,
    pub has_tagged_thinking_thought: bool,
    pending_reasoning_text: String,
    pending_content_parts: Vec<Value>,
    prepared_tool_call_ids: HashSet<String>,
    pending_untrusted_response_parts: Vec<Value>,
    pending_untrusted_openai_reasoning_thought: bool,
    has_structured_reasoning_content: bool,
    has_thinking_tag_in_reasoning: bool,
    has_visible_content: bool,
    at_visible_line_start: bool,
    pending_thinking_tag_candidate: Option<ThinkingTagCandidate>,
    pub protocol_tag_sanitized: Option<(String, usize)>,
}

impl OpenAiStreamContext {
    pub fn new(model: impl Into<String>) -> Self {
        Self {
            model: model.into(),
            ..Self::default()
        }
    }

    pub fn with_tagged_thinking(mut self, enabled: bool) -> Self {
        self.response_parsing_options.tagged_thinking_tags = enabled;
        self
    }

    pub fn with_content_only_tag_leak_detection(mut self, enabled: bool) -> Self {
        self.response_parsing_options
            .content_only_thinking_tag_leaks = enabled;
        self
    }

    /// Flush or reject withheld stream output when the provider closes without
    /// a final chunk, matching the pipeline's end-of-stream protocol check.
    pub fn finish_stream(&mut self) -> Result<Option<ConvertedStreamChunk>, StreamConversionError> {
        let Some(candidate) = self.pending_thinking_tag_candidate.as_ref() else {
            return Ok(None);
        };
        if candidate.closing_tag_name.is_none() && candidate.text.trim().is_empty() {
            self.pending_thinking_tag_candidate = None;
            let parts = std::mem::take(&mut self.pending_untrusted_response_parts);
            if parts.is_empty() {
                return Ok(None);
            }
            return Ok(Some(ConvertedStreamChunk {
                response: json!({"candidates":[{"content":{"parts":parts,"role":"model"},"index":0}]}),
                openai_reasoning_thought: std::mem::take(
                    &mut self.pending_untrusted_openai_reasoning_thought,
                ),
                ..ConvertedStreamChunk::default()
            }));
        }
        self.pending_thinking_tag_candidate = None;
        self.pending_untrusted_response_parts.clear();
        Err(StreamConversionError::ProtocolTagLeak)
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ConvertedStreamChunk {
    pub response: Value,
    pub tool_call_preparations: Vec<ToolCallPreparation>,
    pub usage_provenance: Option<GenAiUsageProvenance>,
    /// Mirrors the TypeScript hidden symbol on OpenAI reasoning thought parts.
    pub openai_reasoning_thought: bool,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum StreamConversionError {
    #[error("convert_openai_chunk_to_gemini requires a fresh stream context")]
    MissingStreamContext,
    #[error("Model response leaked thinking tags.")]
    ProtocolTagLeak,
    #[error("Model response contained a malformed tool call.")]
    MalformedToolCall,
    #[error("provider stream exceeded its retained-data limit")]
    ResourceLimit,
}

/// Normalize providers that replay accumulated text instead of sending only
/// the newly generated suffix. Cumulative retention is capped at 16 MiB.
pub fn normalize_streaming_text_delta(
    raw_delta: &str,
    state: &mut StreamingTextDeltaState,
) -> Result<String, StreamConversionError> {
    if raw_delta.is_empty() {
        return Ok(String::new());
    }
    if raw_delta.len() > MAX_PENDING_STREAM_BYTES {
        return Err(StreamConversionError::ResourceLimit);
    }

    if state.emitted_text.is_empty() {
        state.emitted_text = raw_delta.to_owned();
        state.emitted_length = utf16_len(raw_delta);
        return Ok(raw_delta.to_owned());
    }

    if state.cumulative_mode {
        if let Some(suffix) = strip_prefix_utf16(raw_delta, &state.emitted_text) {
            state.emitted_text.clear();
            state.emitted_text.push_str(raw_delta);
            state.emitted_length = utf16_len(raw_delta);
            return Ok(suffix.to_owned());
        }
        if starts_with_utf16(&state.emitted_text, raw_delta) {
            return Ok(String::new());
        }
        state.cumulative_mode = false;
        state.emitted_text.clear();
        state.emitted_text.push_str(raw_delta);
        state.emitted_length = state.emitted_length.saturating_add(utf16_len(raw_delta));
        return Ok(raw_delta.to_owned());
    }

    let baseline_len = utf16_len(&state.emitted_text);
    if utf16_len(raw_delta) > baseline_len && starts_with_utf16(raw_delta, &state.emitted_text) {
        let slice_from = if baseline_len >= CUMULATIVE_DETECTION_WINDOW_UNITS
            && state.emitted_length > baseline_len
        {
            state.emitted_length
        } else {
            baseline_len
        };
        let raw_len = utf16_len(raw_delta);
        if raw_len > slice_from {
            let suffix =
                slice_utf16(raw_delta, slice_from).ok_or(StreamConversionError::ResourceLimit)?;
            state.emitted_text.clear();
            state.emitted_text.push_str(raw_delta);
            state.emitted_length = raw_len;
            state.cumulative_mode = true;
            return Ok(suffix.to_owned());
        }
    }

    if raw_delta == state.emitted_text {
        let raw_len = utf16_len(raw_delta);
        if raw_len >= CUMULATIVE_DELTA_EXACT_REPEAT_MIN_LENGTH {
            state.cumulative_mode = true;
            return Ok(String::new());
        }
        state.emitted_length = state.emitted_length.saturating_add(raw_len);
        return Ok(raw_delta.to_owned());
    }

    let current_units = utf16_len(&state.emitted_text);
    if current_units < CUMULATIVE_DETECTION_WINDOW_UNITS {
        // The source detector keeps the whole chunk that crosses this window,
        // then stops extending the baseline on later incremental chunks.
        if state
            .emitted_text
            .len()
            .checked_add(raw_delta.len())
            .is_none_or(|length| length > MAX_PENDING_STREAM_BYTES)
        {
            return Err(StreamConversionError::ResourceLimit);
        }
        state.emitted_text.push_str(raw_delta);
    }
    state.emitted_length = state.emitted_length.saturating_add(utf16_len(raw_delta));
    Ok(raw_delta.to_owned())
}

/// Convert one OpenAI Chat Completions stream chunk.
pub fn convert_openai_chunk_to_gemini(
    chunk: &Value,
    context: &mut OpenAiStreamContext,
) -> Result<ConvertedStreamChunk, StreamConversionError> {
    let mut parts = Vec::new();
    let mut preparations = Vec::new();
    let mut openai_reasoning_thought = false;
    let choice = chunk
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|choices| choices.first());
    let finish_reason = choice
        .and_then(|choice| choice.get("finish_reason"))
        .and_then(Value::as_str)
        .filter(|reason| !reason.is_empty());

    if let Some(choice) = choice {
        let delta = choice.get("delta");
        let mut content_parts = Vec::new();
        if let Some(text) = delta
            .and_then(|delta| delta.get("content"))
            .and_then(Value::as_str)
        {
            let normalized = normalize_streaming_text_delta(text, &mut context.text_delta_state)?;
            if !normalized.is_empty() || finish_reason.is_some() {
                content_parts =
                    convert_text_to_parts(&normalized, context, finish_reason.is_some());
            }
        } else if finish_reason.is_some() {
            content_parts = convert_text_to_parts("", context, true);
        }

        if content_parts.iter().any(is_thought_part) {
            context.has_tagged_thinking_thought = true;
            context.pending_reasoning_text.clear();
            parts.append(&mut context.pending_content_parts);
        }

        let reasoning_text = delta
            .and_then(|delta| delta.get("reasoning_content"))
            .or_else(|| delta.and_then(|delta| delta.get("reasoning")))
            .and_then(Value::as_str)
            .filter(|text| !text.is_empty());
        if let Some(reasoning_text) = reasoning_text.filter(|_| {
            !context.response_parsing_options.tagged_thinking_tags
                || !context.has_tagged_thinking_thought
        }) {
            let normalized =
                normalize_streaming_text_delta(reasoning_text, &mut context.reasoning_delta_state)?;
            if !normalized.is_empty() {
                context.reasoning_delta_state.emitted_token_units = context
                    .reasoning_delta_state
                    .emitted_token_units
                    .saturating_add(estimate_text_token_units(&normalized));
                context.has_structured_reasoning_content = true;
                context.has_thinking_tag_in_reasoning |= contains_thinking_tag(&normalized);
            }
            if !normalized.is_empty() && !context.response_parsing_options.tagged_thinking_tags {
                parts.push(json!({"text": normalized, "thought": true}));
                openai_reasoning_thought = true;
            } else if !normalized.is_empty() && !context.has_tagged_thinking_thought {
                append_bounded(&mut context.pending_reasoning_text, &normalized)?;
            }
        }

        if context.response_parsing_options.tagged_thinking_tags
            && !context.has_tagged_thinking_thought
            && !context.pending_reasoning_text.is_empty()
            && !content_parts.is_empty()
        {
            append_parts_bounded(&mut context.pending_content_parts, content_parts)?;
            content_parts = Vec::new();
        }

        if finish_reason.is_some()
            && context.response_parsing_options.tagged_thinking_tags
            && !context.has_tagged_thinking_thought
            && !context.pending_reasoning_text.is_empty()
        {
            parts.push(json!({"text": std::mem::take(&mut context.pending_reasoning_text), "thought": true}));
            openai_reasoning_thought = true;
        }
        if finish_reason.is_some() && !context.pending_content_parts.is_empty() {
            parts.append(&mut context.pending_content_parts);
        }
        parts.append(&mut content_parts);

        if let Some(tool_calls) = delta
            .and_then(|delta| delta.get("tool_calls"))
            .and_then(Value::as_array)
        {
            for call in tool_calls {
                let index = call.get("index").and_then(js_number).unwrap_or(0.0);
                let id = call.get("id").and_then(Value::as_str);
                let function = call.get("function");
                let name = function
                    .and_then(|function| function.get("name"))
                    .and_then(Value::as_str);
                let arguments = function
                    .and_then(|function| function.get("arguments"))
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let parsed = context
                    .tool_call_parser
                    .add_chunk(index, arguments, id, name);
                if context.tool_call_parser.has_resource_limit_error() {
                    return Err(StreamConversionError::ResourceLimit);
                }
                let meta = context
                    .tool_call_parser
                    .get_tool_call_meta(parsed.actual_index.unwrap_or(index.max(0.0) as u64));
                if let (Some(call_id), Some(tool_name)) = (meta.id, meta.name) {
                    if context.prepared_tool_call_ids.insert(call_id.clone()) {
                        preparations.push(ToolCallPreparation { call_id, tool_name });
                    }
                }
            }
        }

        process_thinking_tag_leaks(context, &mut parts, finish_reason.is_some())?;

        let nameless = context.tool_call_parser.has_nameless_tool_call();
        let completed = if finish_reason.is_some() {
            context.tool_call_parser.get_completed_tool_calls()
        } else {
            Vec::new()
        };
        let truncated =
            finish_reason.is_some() && context.tool_call_parser.has_incomplete_tool_calls();

        if finish_reason.is_some()
            && context
                .pending_thinking_tag_candidate
                .as_ref()
                .is_some_and(|candidate| candidate.closing_tag_name.is_some())
        {
            let Some(candidate) = context.pending_thinking_tag_candidate.as_ref() else {
                return Err(StreamConversionError::ProtocolTagLeak);
            };
            if context.has_thinking_tag_in_reasoning
                || finish_reason != Some("tool_calls")
                || completed.is_empty()
                || nameless
                || context
                    .tool_call_parser
                    .has_conflicting_tool_call_identity()
                || truncated
                || context.tool_call_parser.has_invalid_tool_call_arguments()
            {
                return Err(protocol_tag_leak(context));
            }
            let Some(tag_name) = candidate.closing_tag_name.clone() else {
                return Err(StreamConversionError::ProtocolTagLeak);
            };
            context.protocol_tag_sanitized = Some((tag_name, completed.len()));
            context.pending_thinking_tag_candidate = None;
        }

        if finish_reason.is_some()
            && (context.tool_call_parser.has_invalid_tool_call_index()
                || nameless
                || (finish_reason == Some("tool_calls") && completed.is_empty()))
        {
            context.pending_untrusted_response_parts.clear();
            return Err(StreamConversionError::MalformedToolCall);
        }

        let should_hold = finish_reason.is_none()
            && (nameless
                || context.has_thinking_tag_in_reasoning
                || context.pending_thinking_tag_candidate.is_some());
        if should_hold {
            context.pending_untrusted_openai_reasoning_thought |= openai_reasoning_thought;
            append_parts_bounded(&mut context.pending_untrusted_response_parts, parts)?;
            parts = Vec::new();
        } else if !context.pending_untrusted_response_parts.is_empty() {
            let mut pending = std::mem::take(&mut context.pending_untrusted_response_parts);
            pending.append(&mut parts);
            parts = pending;
            openai_reasoning_thought |=
                std::mem::take(&mut context.pending_untrusted_openai_reasoning_thought);
        }

        if finish_reason.is_some() {
            for call in completed {
                parts.push(json!({"functionCall": {
                    "id": call.id.unwrap_or_else(|| {
                        let random = uuid::Uuid::new_v4().simple().to_string();
                        format!("call_{}_{}", now_millis_string(), &random[..7])
                    }),
                    "name": call.name,
                    "args": Value::Object(call.args),
                }}));
            }
        }

        let effective_finish = if truncated && finish_reason != Some("length") {
            Some("length")
        } else {
            finish_reason
        };
        let mut candidate = json!({
            "content": {"parts": parts, "role": "model"},
            "index": 0,
            "safetyRatings": [],
        });
        if let Some(reason) = effective_finish {
            candidate["finishReason"] = Value::String(map_finish_reason(reason).to_owned());
        }
        let mut response = json!({
            "candidates": [candidate],
            "responseId": chunk.get("id").cloned().unwrap_or(Value::Null),
            "createTime": chunk.get("created").filter(|value| is_truthy(value)).map_or_else(now_millis_string, |value| value.as_str().map(str::to_owned).unwrap_or_else(|| value.to_string())),
            "modelVersion": chunk.get("model").filter(|value| is_truthy(value)).cloned().unwrap_or(Value::Null),
            "promptFeedback": {"safetyRatings": []},
        });
        if let Some(usage) = chunk.get("usage").filter(|value| value.is_object()) {
            let prompt = truthy_number(usage.get("prompt_tokens"));
            let completion = truthy_number(usage.get("completion_tokens"));
            let total = truthy_number(usage.get("total_tokens"));
            let provider_reasoning = usage
                .pointer("/completion_tokens_details/reasoning_tokens")
                .and_then(Value::as_u64);
            let estimated = context
                .reasoning_delta_state
                .emitted_token_units
                .div_ceil(TOKEN_ESTIMATE_UNITS_PER_TOKEN);
            let thoughts = provider_reasoning.unwrap_or_else(|| {
                if completion > 0 {
                    estimated.min(completion)
                } else {
                    estimated
                }
            });
            let cached_value = usage
                .pointer("/prompt_tokens_details/cached_tokens")
                .or_else(|| usage.get("cached_tokens"));
            let cached = cached_value.and_then(Value::as_u64).unwrap_or(0);
            let cached_reported = cached_value.is_some_and(Value::is_number);
            let has_breakdown = total == 0 || prompt != 0 || completion != 0;
            let mut metadata = json!({"thoughtsTokenCount": thoughts, "totalTokenCount": total, "cachedContentTokenCount": cached});
            if has_breakdown {
                metadata["promptTokenCount"] = json!(prompt);
                metadata["candidatesTokenCount"] = json!(completion);
            }
            response["usageMetadata"] = metadata;
            return Ok(ConvertedStreamChunk {
                response,
                tool_call_preparations: preparations,
                usage_provenance: Some(GenAiUsageProvenance {
                    cached_input_tokens_reported: cached_reported,
                    cache_creation_input_tokens: None,
                }),
                openai_reasoning_thought,
            });
        }
        return Ok(ConvertedStreamChunk {
            response,
            tool_call_preparations: preparations,
            usage_provenance: None,
            openai_reasoning_thought,
        });
    }

    let mut response = json!({
        "candidates": [],
        "responseId": chunk.get("id").cloned().unwrap_or(Value::Null),
        "createTime": chunk.get("created").filter(|value| is_truthy(value)).map_or_else(now_millis_string, |value| value.as_str().map(str::to_owned).unwrap_or_else(|| value.to_string())),
        "modelVersion": chunk.get("model").filter(|value| is_truthy(value)).cloned().unwrap_or(Value::Null),
        "promptFeedback": {"safetyRatings": []},
    });
    let usage_provenance = if let Some(usage) = chunk.get("usage").filter(|value| value.is_object())
    {
        let prompt = truthy_number(usage.get("prompt_tokens"));
        let completion = truthy_number(usage.get("completion_tokens"));
        let total = truthy_number(usage.get("total_tokens"));
        let provider_reasoning = usage
            .pointer("/completion_tokens_details/reasoning_tokens")
            .and_then(Value::as_u64);
        let estimated = context
            .reasoning_delta_state
            .emitted_token_units
            .div_ceil(TOKEN_ESTIMATE_UNITS_PER_TOKEN);
        let thoughts = provider_reasoning.unwrap_or_else(|| {
            if completion > 0 {
                estimated.min(completion)
            } else {
                estimated
            }
        });
        let cached_value = usage
            .pointer("/prompt_tokens_details/cached_tokens")
            .or_else(|| usage.get("cached_tokens"));
        let cached = cached_value.and_then(Value::as_u64).unwrap_or(0);
        let cached_reported = cached_value.is_some_and(Value::is_number);
        let mut metadata = json!({
            "thoughtsTokenCount": thoughts,
            "totalTokenCount": total,
            "cachedContentTokenCount": cached,
        });
        if total == 0 || prompt != 0 || completion != 0 {
            metadata["promptTokenCount"] = json!(prompt);
            metadata["candidatesTokenCount"] = json!(completion);
        }
        response["usageMetadata"] = metadata;
        Some(GenAiUsageProvenance {
            cached_input_tokens_reported: cached_reported,
            cache_creation_input_tokens: None,
        })
    } else {
        None
    };
    Ok(ConvertedStreamChunk {
        response,
        tool_call_preparations: preparations,
        usage_provenance,
        openai_reasoning_thought,
    })
}

fn convert_text_to_parts(
    text: &str,
    context: &mut OpenAiStreamContext,
    final_chunk: bool,
) -> Vec<Value> {
    if !context.response_parsing_options.tagged_thinking_tags {
        return if text.is_empty() {
            Vec::new()
        } else {
            vec![json!({"text": text})]
        };
    }
    let parser = context
        .tagged_thinking_parser
        .get_or_insert_with(TaggedThinkingParser::default);
    parser.parse(text, final_chunk)
}

fn process_thinking_tag_leaks(
    context: &mut OpenAiStreamContext,
    parts: &mut Vec<Value>,
    finished: bool,
) -> Result<(), StreamConversionError> {
    let mut visible_text = parts.iter().map(visible_part_text).collect::<String>();
    let prior = context.pending_thinking_tag_candidate.clone();
    let replayed_prefix = prior.as_ref().is_some_and(|candidate| {
        candidate.closing_tag_name.is_none()
            && !candidate.text.trim().is_empty()
            && candidate.text == visible_text
    });
    let replayed_closing = standalone_closing_tag(&visible_text);
    if replayed_prefix
        || prior
            .as_ref()
            .and_then(|candidate| candidate.closing_tag_name.as_deref())
            .is_some_and(|name| Some(name) == replayed_closing.as_deref())
    {
        parts.retain(|part| visible_part_text(part).is_empty());
        visible_text.clear();
    }
    if prior
        .as_ref()
        .map_or(0, |candidate| candidate.text.len())
        .checked_add(visible_text.len())
        .is_none_or(|length| length > MAX_PENDING_STREAM_BYTES)
    {
        return Err(StreamConversionError::ResourceLimit);
    }
    let combined = format!(
        "{}{}",
        prior
            .as_ref()
            .map_or("", |candidate| candidate.text.as_str()),
        visible_text
    );
    let has_structured = context.has_structured_reasoning_content;
    let content_only_state = if has_structured
        || context.has_visible_content
        || !context
            .response_parsing_options
            .content_only_thinking_tag_leaks
    {
        ContentOnlyState::Clean
    } else {
        classify_content_only_prefix(&combined, finished)
    };
    let can_start = !context.has_visible_content
        && !visible_text.is_empty()
        && ((has_structured && can_be_standalone_prefix(&combined))
            || content_only_state != ContentOnlyState::Clean);

    if prior.is_some() || can_start {
        let closing = standalone_closing_tag(&combined);
        let opening = standalone_opening_tag(&combined).is_some();
        let possible = can_be_standalone_prefix(&combined)
            || matches!(
                content_only_state,
                ContentOnlyState::Pending | ContentOnlyState::Suspicious
            );
        let confirmed_open = leading_thinking_tag(&combined).is_some_and(|(_, closing)| !closing);
        let finished_whitespace = finished && closing.is_none() && combined.trim().is_empty();
        let release_content_only = content_only_state == ContentOnlyState::Pending
            && !confirmed_open
            && (finished || utf16_len(combined.trim_start()) > 128);
        if content_only_state == ContentOnlyState::Leaked {
            return Err(protocol_tag_leak(context));
        }
        if opening && has_structured {
            return Err(protocol_tag_leak(context));
        }
        if prior
            .as_ref()
            .and_then(|candidate| candidate.closing_tag_name.as_ref())
            .is_some()
            && closing.is_none()
        {
            return Err(protocol_tag_leak(context));
        }
        if finished_whitespace || release_content_only {
            parts.retain(|part| visible_part_text(part).is_empty());
            if !combined.is_empty() {
                parts.push(json!({"text": combined}));
            }
            visible_text = combined;
            context.pending_thinking_tag_candidate = None;
        } else if possible {
            if !confirmed_open && closing.is_none() && utf16_len(combined.trim_start()) > 128 {
                return Err(protocol_tag_leak(context));
            }
            context.pending_thinking_tag_candidate = Some(if let Some(name) = closing {
                ThinkingTagCandidate {
                    text: format!("</{name}>"),
                    closing_tag_name: Some(name),
                }
            } else {
                ThinkingTagCandidate {
                    text: combined,
                    closing_tag_name: None,
                }
            });
            parts.retain(|part| visible_part_text(part).is_empty());
            visible_text.clear();
            if finished
                && context
                    .pending_thinking_tag_candidate
                    .as_ref()
                    .is_some_and(|candidate| candidate.closing_tag_name.is_none())
            {
                return Err(protocol_tag_leak(context));
            }
        } else if prior.is_some() {
            parts.retain(|part| visible_part_text(part).is_empty());
            parts.push(json!({"text": combined}));
            visible_text = combined;
            context.pending_thinking_tag_candidate = None;
        }
    }

    let leaked = context.has_structured_reasoning_content
        && ((!context.has_visible_content && leading_thinking_tag(&visible_text).is_some())
            || (context.has_thinking_tag_in_reasoning
                && (contains_closing_tag_after_newline(&visible_text)
                    || (context.at_visible_line_start
                        && leading_closing_tag(&visible_text).is_some()))));
    if !visible_text.trim().is_empty() {
        context.has_visible_content = true;
    }
    if !visible_text.is_empty() && context.has_thinking_tag_in_reasoning {
        let suffix = visible_text
            .rsplit_once('\n')
            .map_or(visible_text.as_str(), |(_, suffix)| suffix);
        context.at_visible_line_start = (visible_text.contains('\n')
            || context.at_visible_line_start)
            && suffix.chars().all(|character| {
                character.is_whitespace() && character != '\n' && character != '\r'
            });
    }
    if leaked {
        return Err(protocol_tag_leak(context));
    }
    Ok(())
}

fn visible_part_text(part: &Value) -> &str {
    if part.get("thought").and_then(Value::as_bool) == Some(true) {
        ""
    } else {
        part.get("text").and_then(Value::as_str).unwrap_or("")
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ContentOnlyState {
    Clean,
    Pending,
    Suspicious,
    Leaked,
}

fn classify_content_only_prefix(text: &str, finished: bool) -> ContentOnlyState {
    let mut rest = text.trim_start().to_ascii_lowercase();
    if rest.is_empty() {
        return ContentOnlyState::Clean;
    }
    for closing in [false, true, false] {
        match consume_leading_tag(&rest, closing) {
            Some(Some(length)) => {
                rest = rest[length..].trim_start().to_owned();
                if closing && rest.is_empty() {
                    return ContentOnlyState::Clean;
                }
                if !closing && !rest.trim().is_empty() && find_closing_tag(&rest).is_none() {
                    return if finished {
                        ContentOnlyState::Leaked
                    } else {
                        ContentOnlyState::Pending
                    };
                }
            }
            Some(None) => return ContentOnlyState::Pending,
            None => {
                if !closing {
                    return ContentOnlyState::Clean;
                }
                break;
            }
        }
    }
    let mut depth = 1i32;
    let mut nested_open = false;
    let mut cursor = 0;
    while let Some((offset, length, closing)) = find_any_tag(&rest, cursor) {
        depth += if closing { -1 } else { 1 };
        if depth == 0 {
            return ContentOnlyState::Clean;
        }
        nested_open |= !closing;
        cursor = offset + length;
    }
    if !nested_open {
        ContentOnlyState::Pending
    } else if finished {
        ContentOnlyState::Leaked
    } else {
        ContentOnlyState::Suspicious
    }
}

// `Some(Some(n))` means a complete leading tag; `Some(None)` means a prefix
// that may become the requested opening/closing tag; `None` means no match.
fn consume_leading_tag(value: &str, closing: bool) -> Option<Option<usize>> {
    if let Some((length, actual_closing)) = leading_thinking_tag(value) {
        return if actual_closing == closing {
            Some(Some(length))
        } else {
            None
        };
    }
    if !can_be_standalone_prefix(value) {
        return None;
    }
    let lower = value.trim_start();
    if lower.is_empty() || lower == "<" {
        return Some(None);
    }
    if lower.starts_with("</") == closing {
        Some(None)
    } else {
        None
    }
}

fn can_be_standalone_prefix(text: &str) -> bool {
    let candidate = text.trim_start().to_ascii_lowercase();
    if candidate.is_empty() {
        return true;
    }
    ["<think", "<thinking", "</think", "</thinking"]
        .iter()
        .any(|tag| {
            if tag.starts_with(&candidate) {
                return true;
            }
            if !candidate.starts_with(tag) {
                return false;
            }
            let rest = &candidate[tag.len()..];
            rest.trim().is_empty() || (rest.starts_with('>') && rest[1..].trim().is_empty())
        })
}

fn leading_thinking_tag(text: &str) -> Option<(usize, bool)> {
    let trimmed = text.trim_start();
    let leading = text.len() - trimmed.len();
    let (length, closing) = parse_tag(trimmed, 0)?;
    Some((leading + length, closing))
}

fn standalone_closing_tag(text: &str) -> Option<String> {
    let trimmed = text.trim();
    let (length, closing) = parse_tag(trimmed, 0)?;
    if !closing || length != trimmed.len() {
        return None;
    }
    tag_name(trimmed)
}

fn standalone_opening_tag(text: &str) -> Option<String> {
    let trimmed = text.trim();
    let (length, closing) = parse_tag(trimmed, 0)?;
    if closing || length != trimmed.len() {
        return None;
    }
    tag_name(trimmed)
}

fn tag_name(tag: &str) -> Option<String> {
    let lower = tag.to_ascii_lowercase();
    if lower.starts_with("</thinking") {
        Some("thinking".to_owned())
    } else if lower.starts_with("</think") || lower.starts_with("<think") {
        Some("think".to_owned())
    } else {
        None
    }
}

fn parse_tag(text: &str, start: usize) -> Option<(usize, bool)> {
    let bytes = text.as_bytes();
    if bytes.get(start) != Some(&b'<') {
        return None;
    }
    let mut cursor = start + 1;
    let closing = bytes.get(cursor) == Some(&b'/');
    if closing {
        cursor += 1;
    }
    let remaining = text.get(cursor..)?;
    let lower = remaining.to_ascii_lowercase();
    if lower.starts_with("thinking") {
        cursor += 8;
    } else if lower.starts_with("think") {
        cursor += 5;
    } else {
        return None;
    }
    while let Some(character) = text.get(cursor..)?.chars().next() {
        if character == '>' {
            return Some((cursor + 1, closing));
        }
        if !character.is_whitespace() {
            return None;
        }
        cursor += character.len_utf8();
    }
    None
}

fn find_any_tag(text: &str, from: usize) -> Option<(usize, usize, bool)> {
    let bytes = text.as_bytes();
    let mut index = from;
    while index < bytes.len() {
        if bytes[index] == b'<' {
            if let Some((length, closing)) = parse_tag(text, index) {
                return Some((index, length, closing));
            }
        }
        index += text[index..].chars().next()?.len_utf8();
    }
    None
}

fn find_closing_tag(text: &str) -> Option<usize> {
    let mut cursor = 0;
    while let Some((offset, length, closing)) = find_any_tag(text, cursor) {
        if closing {
            return Some(offset);
        }
        cursor = offset + length;
    }
    None
}

fn contains_thinking_tag(text: &str) -> bool {
    find_any_tag(text, 0).is_some()
}

fn contains_closing_tag_after_newline(text: &str) -> bool {
    text.match_indices('\n')
        .any(|(index, _)| leading_closing_tag(&text[index + 1..]).is_some())
}

fn leading_closing_tag(text: &str) -> Option<String> {
    let trimmed = text.trim_start_matches(|character: char| {
        character.is_whitespace() && character != '\n' && character != '\r'
    });
    let (length, closing) = parse_tag(trimmed, 0)?;
    if !closing {
        return None;
    }
    tag_name(trimmed.get(..length)?)
}

fn protocol_tag_leak(context: &mut OpenAiStreamContext) -> StreamConversionError {
    context.pending_thinking_tag_candidate = None;
    context.pending_untrusted_response_parts.clear();
    StreamConversionError::ProtocolTagLeak
}

fn append_bounded(target: &mut String, text: &str) -> Result<(), StreamConversionError> {
    if target
        .len()
        .checked_add(text.len())
        .is_none_or(|length| length > MAX_PENDING_STREAM_BYTES)
    {
        return Err(StreamConversionError::ResourceLimit);
    }
    target.push_str(text);
    Ok(())
}

fn append_parts_bounded(
    target: &mut Vec<Value>,
    parts: Vec<Value>,
) -> Result<(), StreamConversionError> {
    if target
        .len()
        .checked_add(parts.len())
        .is_none_or(|count| count > MAX_PENDING_STREAM_PARTS)
    {
        return Err(StreamConversionError::ResourceLimit);
    }
    let mut bytes = target
        .iter()
        .map(Value::to_string)
        .map(|value| value.len())
        .sum::<usize>();
    for part in &parts {
        bytes = bytes
            .checked_add(part.to_string().len())
            .ok_or(StreamConversionError::ResourceLimit)?;
        if bytes > MAX_PENDING_STREAM_BYTES {
            return Err(StreamConversionError::ResourceLimit);
        }
    }
    target.extend(parts);
    Ok(())
}

fn is_thought_part(part: &Value) -> bool {
    part.get("thought").and_then(Value::as_bool) == Some(true)
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
fn truthy_number(value: Option<&Value>) -> u64 {
    value
        .filter(|value| is_truthy(value))
        .and_then(Value::as_u64)
        .unwrap_or(0)
}
fn js_number(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
}
fn map_finish_reason(reason: &str) -> &'static str {
    match reason {
        "stop" | "function_call" | "tool_calls" => "STOP",
        "length" => "MAX_TOKENS",
        "content_filter" => "SAFETY",
        _ => "FINISH_REASON_UNSPECIFIED",
    }
}
fn now_millis_string() -> String {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis().to_string())
        .unwrap_or_else(|_| "0".to_owned())
}

fn estimate_text_token_units(text: &str) -> u64 {
    text.encode_utf16().fold(0u64, |units, unit| {
        units.saturating_add(if unit < 128 { 5 } else { 22 })
    })
}
fn utf16_len(text: &str) -> usize {
    text.encode_utf16().count()
}
fn starts_with_utf16(text: &str, prefix: &str) -> bool {
    strip_prefix_utf16(text, prefix).is_some()
}
fn strip_prefix_utf16<'a>(text: &'a str, prefix: &str) -> Option<&'a str> {
    let byte_end = prefix.len();
    let candidate = text.get(..byte_end)?;
    (candidate == prefix).then(|| &text[byte_end..])
}
fn slice_utf16(text: &str, offset: usize) -> Option<&str> {
    let mut units = 0;
    if offset == 0 {
        return Some(text);
    }
    for (byte, character) in text.char_indices() {
        let next = units + character.len_utf16();
        if next == offset {
            return text.get(byte + character.len_utf8()..);
        }
        if next > offset {
            return None;
        }
        units = next;
    }
    (units == offset).then_some("")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_cumulative_content_rewinds_and_mode_exit() {
        let mut state = StreamingTextDeltaState::default();
        assert_eq!(
            normalize_streaming_text_delta("Hello ", &mut state).unwrap(),
            "Hello "
        );
        assert_eq!(
            normalize_streaming_text_delta("Hello world", &mut state).unwrap(),
            "world"
        );
        assert!(state.cumulative_mode);
        assert_eq!(
            normalize_streaming_text_delta("Hello", &mut state).unwrap(),
            ""
        );
        assert_eq!(
            normalize_streaming_text_delta("next", &mut state).unwrap(),
            "next"
        );
        assert!(!state.cumulative_mode);
        assert_eq!(state.emitted_length, 15);
    }

    #[test]
    fn normalizer_matches_short_repeat_and_utf16_slice_behavior() {
        let mut short = StreamingTextDeltaState::default();
        assert_eq!(
            normalize_streaming_text_delta("x", &mut short).unwrap(),
            "x"
        );
        assert_eq!(
            normalize_streaming_text_delta("x", &mut short).unwrap(),
            "x"
        );
        assert_eq!(
            normalize_streaming_text_delta("xy", &mut short).unwrap(),
            "y"
        );
        let mut unicode = StreamingTextDeltaState::default();
        assert_eq!(
            normalize_streaming_text_delta("😀", &mut unicode).unwrap(),
            "😀"
        );
        assert_eq!(
            normalize_streaming_text_delta("😀ok", &mut unicode).unwrap(),
            "ok"
        );
    }

    #[test]
    fn normalizer_slices_late_cumulative_switch_after_detection_window() {
        let prefix = "a".repeat(CUMULATIVE_DETECTION_WINDOW_UNITS);
        let extension = "b".repeat(12);
        let mut state = StreamingTextDeltaState::default();
        assert_eq!(
            normalize_streaming_text_delta(&prefix, &mut state).unwrap(),
            prefix
        );
        assert_eq!(
            normalize_streaming_text_delta(&extension, &mut state).unwrap(),
            extension
        );
        let cumulative = format!("{prefix}{extension}!");
        assert_eq!(
            normalize_streaming_text_delta(&cumulative, &mut state).unwrap(),
            "!"
        );
        assert!(state.cumulative_mode);
    }

    #[test]
    fn converts_text_and_reasoning_parts_with_usage_metadata() {
        let mut context = OpenAiStreamContext::new("model-x");
        let converted = convert_openai_chunk_to_gemini(
            &json!({
                "id": "chunk-1", "created": 123, "model": "model-x",
                "choices": [{"delta": {"content": "answer", "reasoning_content": "thinking"}, "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 10, "completion_tokens": 6, "total_tokens": 16, "prompt_tokens_details": {"cached_tokens": 2}}
            }),
            &mut context,
        ).unwrap();
        assert_eq!(converted.response["candidates"][0]["finishReason"], "STOP");
        assert_eq!(
            converted.response["candidates"][0]["content"]["parts"][0],
            json!({"text":"thinking", "thought":true})
        );
        assert_eq!(
            converted.response["candidates"][0]["content"]["parts"][1],
            json!({"text":"answer"})
        );
        assert_eq!(
            converted.response["usageMetadata"]["cachedContentTokenCount"],
            2
        );
        assert!(converted.openai_reasoning_thought);
        assert!(
            converted
                .usage_provenance
                .unwrap()
                .cached_input_tokens_reported
        );
    }

    #[test]
    fn converts_tool_calls_only_at_finish_and_prepares_once() {
        let mut context = OpenAiStreamContext::new("model-x");
        let first = convert_openai_chunk_to_gemini(
            &json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call-1","type":"function","function":{"name":"lookup","arguments":"{\"key\":"}}]},"finish_reason":null}]}),
            &mut context,
        ).unwrap();
        assert_eq!(
            first.tool_call_preparations,
            vec![ToolCallPreparation {
                call_id: "call-1".into(),
                tool_name: "lookup".into()
            }]
        );
        assert!(
            first.response["candidates"][0]["content"]["parts"]
                .as_array()
                .unwrap()
                .is_empty()
        );

        let second = convert_openai_chunk_to_gemini(
            &json!({"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"value\"}"}}]},"finish_reason":null}]}),
            &mut context,
        ).unwrap();
        assert!(second.tool_call_preparations.is_empty());
        let final_chunk = convert_openai_chunk_to_gemini(
            &json!({"choices":[{"delta":{},"finish_reason":"tool_calls"}]}),
            &mut context,
        )
        .unwrap();
        assert_eq!(
            final_chunk.response["candidates"][0]["content"]["parts"][0]["functionCall"]["name"],
            "lookup"
        );
        assert_eq!(
            final_chunk.response["candidates"][0]["content"]["parts"][0]["functionCall"]["args"]["key"],
            "value"
        );
    }

    #[test]
    fn tagged_thinking_parser_emits_thoughts_and_holds_content_until_resolved() {
        let mut context = OpenAiStreamContext::new("model-x").with_tagged_thinking(true);
        let first = convert_openai_chunk_to_gemini(
            &json!({"choices":[{"delta":{"reasoning_content":"reasoning"},"finish_reason":null}]}),
            &mut context,
        )
        .unwrap();
        assert!(
            first.response["candidates"][0]["content"]["parts"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        let final_chunk = convert_openai_chunk_to_gemini(
            &json!({"choices":[{"delta":{"content":"<think>tagged thought</think>answer"},"finish_reason":"stop"}]}),
            &mut context,
        ).unwrap();
        let parts = final_chunk.response["candidates"][0]["content"]["parts"]
            .as_array()
            .unwrap();
        assert_eq!(parts[0], json!({"text":"tagged thought", "thought":true}));
        assert_eq!(parts[1], json!({"text":"answer"}));
        assert!(context.pending_reasoning_text.is_empty());
    }

    #[test]
    fn content_only_open_thinking_tag_is_rejected_when_stream_finishes_unclosed() {
        let mut context =
            OpenAiStreamContext::new("model-x").with_content_only_tag_leak_detection(true);
        let partial = convert_openai_chunk_to_gemini(
            &json!({"choices":[{"delta":{"content":"<think>private thought"},"finish_reason":null}]}),
            &mut context,
        ).unwrap();
        assert!(
            partial.response["candidates"][0]["content"]["parts"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            convert_openai_chunk_to_gemini(
                &json!({"choices":[{"delta":{"content":" continues"},"finish_reason":"stop"}]}),
                &mut context,
            )
            .unwrap_err(),
            StreamConversionError::ProtocolTagLeak
        );
    }

    #[test]
    fn truncated_tool_arguments_override_finish_reason_to_max_tokens() {
        let mut context = OpenAiStreamContext::new("model-x");
        let converted = convert_openai_chunk_to_gemini(
            &json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call-1","function":{"name":"lookup","arguments":"{\"key\":"}}]},"finish_reason":"stop"}]}),
            &mut context,
        ).unwrap();
        assert_eq!(
            converted.response["candidates"][0]["finishReason"],
            "MAX_TOKENS"
        );
    }

    #[test]
    fn usage_only_final_chunk_keeps_usage_metadata_without_candidate() {
        let mut context = OpenAiStreamContext::new("model-x");
        let converted = convert_openai_chunk_to_gemini(
            &json!({"id":"final","choices":[],"usage":{"prompt_tokens":2,"completion_tokens":3,"total_tokens":5}}),
            &mut context,
        ).unwrap();
        assert_eq!(converted.response["candidates"], json!([]));
        assert_eq!(converted.response["usageMetadata"]["totalTokenCount"], 5);
    }

    #[test]
    fn end_of_stream_releases_held_whitespace_candidate() {
        let mut context = OpenAiStreamContext::new("model-x");
        context.pending_thinking_tag_candidate = Some(ThinkingTagCandidate {
            text: "  \n".to_owned(),
            closing_tag_name: None,
        });
        context
            .pending_untrusted_response_parts
            .push(json!({"text":"answer"}));

        let tail = context.finish_stream().unwrap().unwrap();
        assert_eq!(
            tail.response["candidates"][0]["content"]["parts"],
            json!([{"text":"answer"}])
        );
        assert!(context.finish_stream().unwrap().is_none());
    }
}
