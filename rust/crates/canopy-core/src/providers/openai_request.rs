//! Conversion from Canopy's Gemini-shaped generation requests to OpenAI Chat
//! Completions messages.
//!
//! This ports the request-side content, media, tool-result, merge, and orphan
//! cleanup logic from `core/openaiContentGenerator/converter.ts`.

use std::collections::{HashMap, HashSet};

use serde_json::{Map, Value, json};

const SPLIT_TOOL_MEDIA_TEXT: &str = "(attached media from previous tool call)";
const MAX_TOOL_NAME_LENGTH: usize = 63;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ToolResultContentFormat {
    #[default]
    Parts,
    String,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct InputModalities {
    pub image: bool,
    pub pdf: bool,
    pub audio: bool,
    pub video: bool,
}

#[derive(Clone, Debug)]
pub struct OpenAiRequestContext {
    pub model: String,
    pub modalities: InputModalities,
    /// Canopy defaults this compatibility mode on for OpenAI tool results.
    pub split_tool_media: bool,
    pub tool_result_content_format: ToolResultContentFormat,
}

impl Default for OpenAiRequestContext {
    fn default() -> Self {
        Self {
            model: String::new(),
            modalities: InputModalities::default(),
            split_tool_media: true,
            tool_result_content_format: ToolResultContentFormat::Parts,
        }
    }
}

/// Convert a Gemini-shaped request to the OpenAI Chat Completions `messages`
/// array. Consecutive assistant turns are merged and orphaned tool calls and
/// responses are removed, matching the source converter's default behavior.
pub fn convert_gemini_request_to_openai(
    request: &Value,
    context: &OpenAiRequestContext,
) -> Vec<Value> {
    convert_gemini_request_to_openai_with_cleanup(request, context, true)
}

/// Convert request messages and optionally remove tool calls which do not
/// have a corresponding immediately-following tool result.
pub fn convert_gemini_request_to_openai_with_cleanup(
    request: &Value,
    context: &OpenAiRequestContext,
    clean_orphan_tool_calls: bool,
) -> Vec<Value> {
    let mut messages = Vec::new();
    if let Some(system_instruction) = request
        .get("config")
        .and_then(|config| config.get("systemInstruction"))
    {
        let text = extract_text_from_content_union(system_instruction);
        if !text.is_empty() {
            messages.push(json!({"role": "system", "content": text}));
        }
    }

    if let Some(contents) = request.get("contents") {
        process_contents(contents, &mut messages, context);
    }

    messages = merge_consecutive_assistant_messages(messages);
    if clean_orphan_tool_calls {
        messages = clean_orphaned_tool_calls(messages);
        messages = merge_consecutive_assistant_messages(messages);
    }
    messages
}

fn process_contents(contents: &Value, messages: &mut Vec<Value>, context: &OpenAiRequestContext) {
    if let Some(contents) = contents.as_array() {
        for content in contents {
            process_content(content, messages, context);
        }
    } else if !contents.is_null() {
        process_content(contents, messages, context);
    }
}

fn process_content(content: &Value, messages: &mut Vec<Value>, context: &OpenAiRequestContext) {
    if let Some(text) = content.as_str() {
        messages.push(json!({"role": "user", "content": text}));
        return;
    }

    let Some(content_object) = content.as_object() else {
        return;
    };
    let (Some(role), Some(parts)) = (
        content_object.get("role").and_then(Value::as_str),
        content_object.get("parts").and_then(Value::as_array),
    ) else {
        return;
    };
    let role = if role == "model" { "assistant" } else { "user" };

    let mut content_parts = Vec::new();
    let mut reasoning_parts = Vec::new();
    let mut tool_calls = Vec::new();
    let mut emitted_function_call_ids = HashSet::new();
    let mut emitted_function_response_ids = HashSet::new();
    let mut accumulated_split_media = Vec::new();

    for part in parts {
        if let Some(text) = part.as_str() {
            content_parts.push(json!({"type": "text", "text": text}));
            continue;
        }
        let Some(part) = part.as_object() else {
            continue;
        };

        let thought = part.get("thought").is_some_and(is_truthy);
        if role == "assistant" && thought {
            if let Some(text) = part.get("text").and_then(Value::as_str) {
                if !text.is_empty() {
                    reasoning_parts.push(text.to_owned());
                }
            }
        }
        if !thought {
            if let Some(text) = part.get("text").and_then(Value::as_str) {
                if !text.is_empty() {
                    content_parts.push(json!({"type": "text", "text": text}));
                }
            }
        }

        if let Some(media) = create_media_content_part(part, context) {
            if role == "user" {
                content_parts.push(media);
            }
        }

        if role == "assistant" {
            if let Some(function_call) = part.get("functionCall").and_then(Value::as_object) {
                let call_id = function_call
                    .get("id")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                if !call_id.is_empty() && !emitted_function_call_ids.insert(call_id.to_owned()) {
                    continue;
                }
                let call_name = function_call
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let arguments = function_call
                    .get("args")
                    .filter(|value| is_truthy(value))
                    .unwrap_or(&Value::Null);
                let arguments = if is_truthy(arguments) {
                    serde_json::to_string(arguments).unwrap_or_else(|_| "{}".to_owned())
                } else {
                    "{}".to_owned()
                };
                tool_calls.push(json!({
                    "id": if call_id.is_empty() {
                        format!("call_{}", tool_calls.len())
                    } else {
                        call_id.to_owned()
                    },
                    "type": "function",
                    "function": {
                        "name": normalize_mcp_tool_name(call_name),
                        "arguments": arguments,
                    }
                }));
            }
        }

        if role == "user" {
            if let Some(function_response) = part.get("functionResponse").and_then(Value::as_object)
            {
                let response_id = function_response
                    .get("id")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                if !response_id.is_empty()
                    && !emitted_function_response_ids.insert(response_id.to_owned())
                {
                    continue;
                }

                if let Some(mut tool_message) = create_tool_message(function_response, context) {
                    if context.split_tool_media {
                        if let Some(tool_parts) =
                            tool_message.get("content").and_then(Value::as_array)
                        {
                            let mut media_parts = Vec::new();
                            let mut text_parts = Vec::new();
                            for content_part in tool_parts {
                                match content_part.get("type").and_then(Value::as_str) {
                                    Some("image_url" | "input_audio" | "video_url" | "file") => {
                                        media_parts.push(content_part.clone());
                                    }
                                    Some("text") => text_parts.push(content_part.clone()),
                                    _ => {}
                                }
                            }
                            if !media_parts.is_empty() {
                                let text_only = text_parts
                                    .iter()
                                    .filter_map(|part| part.get("text").and_then(Value::as_str))
                                    .collect::<Vec<_>>()
                                    .join("\n");
                                if let Some(message) = tool_message.as_object_mut() {
                                    message.insert(
                                        "content".to_owned(),
                                        Value::String(if text_only.is_empty() {
                                            "[media attached in following user message]".to_owned()
                                        } else {
                                            text_only
                                        }),
                                    );
                                }
                                accumulated_split_media.extend(media_parts);
                            }
                        }
                    }

                    if context.tool_result_content_format == ToolResultContentFormat::String {
                        if let Some(tool_parts) =
                            tool_message.get("content").and_then(Value::as_array)
                        {
                            if tool_parts.iter().all(|part| {
                                part.get("type").and_then(Value::as_str) == Some("text")
                            }) {
                                let content = tool_parts
                                    .iter()
                                    .filter_map(|part| part.get("text").and_then(Value::as_str))
                                    .collect::<Vec<_>>()
                                    .join("\n");
                                if let Some(message) = tool_message.as_object_mut() {
                                    message.insert("content".to_owned(), Value::String(content));
                                }
                            }
                        }
                    }
                    messages.push(tool_message);
                }
            }
        }
    }

    if !accumulated_split_media.is_empty() {
        let mut following_content = vec![json!({
            "type": "text",
            "text": SPLIT_TOOL_MEDIA_TEXT
        })];
        following_content.extend(accumulated_split_media);
        messages.push(json!({"role": "user", "content": following_content}));
    }

    if role == "assistant" {
        if content_parts.is_empty() && tool_calls.is_empty() && reasoning_parts.is_empty() {
            return;
        }
        let assistant_text = content_parts
            .iter()
            .filter(|part| part.get("type").and_then(Value::as_str) == Some("text"))
            .filter_map(|part| part.get("text").and_then(Value::as_str))
            .collect::<String>();
        let content = if !assistant_text.is_empty() || !reasoning_parts.is_empty() {
            Value::String(assistant_text)
        } else {
            Value::Null
        };
        let mut assistant = Map::new();
        assistant.insert("role".to_owned(), Value::String("assistant".to_owned()));
        assistant.insert("content".to_owned(), content);
        if !tool_calls.is_empty() {
            assistant.insert("tool_calls".to_owned(), Value::Array(tool_calls));
        }
        let reasoning = reasoning_parts.concat();
        if !reasoning.is_empty() {
            assistant.insert("reasoning_content".to_owned(), Value::String(reasoning));
        }
        messages.push(Value::Object(assistant));
    } else if !content_parts.is_empty() {
        messages.push(json!({"role": "user", "content": content_parts}));
    }
}

fn create_tool_message(
    response: &Map<String, Value>,
    context: &OpenAiRequestContext,
) -> Option<Value> {
    let text_content = extract_function_response_content(response.get("response"));
    let mut content_parts = Vec::new();
    if !text_content.is_empty() {
        content_parts.push(json!({"type": "text", "text": text_content}));
    }

    if let Some(parts) = response.get("parts").and_then(Value::as_array) {
        for part in parts {
            if let Some(text) = part.as_object().and_then(|part| part.get("text")) {
                if let Some(text) = text.as_str().filter(|text| !text.is_empty()) {
                    content_parts.push(json!({"type": "text", "text": text}));
                }
                continue;
            }
            if let Some(part) = part.as_object() {
                if let Some(media) = create_media_content_part(part, context) {
                    content_parts.push(media);
                }
            }
        }
    }

    let response_id = response.get("id").and_then(Value::as_str).unwrap_or("");
    if content_parts.is_empty() {
        Some(json!({
            "role": "tool",
            "tool_call_id": response_id,
            "content": ""
        }))
    } else {
        Some(json!({
            "role": "tool",
            "tool_call_id": response_id,
            "content": content_parts
        }))
    }
}

fn extract_function_response_content(response: Option<&Value>) -> String {
    let Some(response) = response else {
        return String::new();
    };
    if response.is_null() {
        return String::new();
    }
    if let Some(text) = response.as_str() {
        return text.to_owned();
    }
    if let Some(response) = response.as_object() {
        if let Some(output) = response.get("output").and_then(Value::as_str) {
            return output.to_owned();
        }
        if let Some(error) = response.get("error").and_then(Value::as_str) {
            return error.to_owned();
        }
    }
    serde_json::to_string(response).unwrap_or_else(|_| js_string(response))
}

fn create_media_content_part(
    part: &Map<String, Value>,
    context: &OpenAiRequestContext,
) -> Option<Value> {
    if let Some(inline_data) = part.get("inlineData").and_then(Value::as_object) {
        let mime_type = inline_data.get("mimeType").and_then(Value::as_str)?;
        let data = inline_data.get("data").and_then(Value::as_str)?;
        if data.is_empty() {
            return None;
        }
        let display_name =
            non_empty(inline_data.get("displayName").and_then(Value::as_str)).unwrap_or(mime_type);

        if mime_type.starts_with("image/") {
            return Some(if context.modalities.image {
                json!({"type": "image_url", "image_url": {"url": format!("data:{mime_type};base64,{data}")}})
            } else {
                unsupported_modality_placeholder("image", display_name)
            });
        }
        if mime_type == "application/pdf" {
            return Some(if context.modalities.pdf {
                json!({
                    "type": "file",
                    "file": {
                        "filename": non_empty(inline_data.get("displayName").and_then(Value::as_str)).unwrap_or("document.pdf"),
                        "file_data": format!("data:{mime_type};base64,{data}")
                    }
                })
            } else {
                unsupported_modality_placeholder("pdf", display_name)
            });
        }
        if mime_type.starts_with("audio/") {
            return Some(if !context.modalities.audio {
                unsupported_modality_placeholder("audio", display_name)
            } else if mime_type.contains("wav") {
                json!({"type": "input_audio", "input_audio": {"data": format!("data:{mime_type};base64,{data}"), "format": "wav"}})
            } else if mime_type.contains("mp3") || mime_type.contains("mpeg") {
                json!({"type": "input_audio", "input_audio": {"data": format!("data:{mime_type};base64,{data}"), "format": "mp3"}})
            } else {
                json!({"type": "text", "text": format!("Unsupported inline media type: {mime_type} ({display_name}).")})
            });
        }
        if mime_type.starts_with("video/") {
            return Some(if context.modalities.video {
                json!({"type": "video_url", "video_url": {"url": format!("data:{mime_type};base64,{data}")}})
            } else {
                unsupported_modality_placeholder("video", display_name)
            });
        }
        return Some(
            json!({"type": "text", "text": format!("Unsupported inline media type: {mime_type} ({display_name}).")}),
        );
    }

    if let Some(file_data) = part.get("fileData").and_then(Value::as_object) {
        let mime_type = file_data.get("mimeType").and_then(Value::as_str)?;
        let file_uri = file_data.get("fileUri").and_then(Value::as_str)?;
        let filename =
            non_empty(file_data.get("displayName").and_then(Value::as_str)).unwrap_or("file");

        if mime_type.starts_with("image/") {
            return Some(if context.modalities.image {
                json!({"type": "image_url", "image_url": {"url": file_uri}})
            } else {
                unsupported_modality_placeholder("image", filename)
            });
        }
        if mime_type == "application/pdf" {
            return Some(if context.modalities.pdf {
                json!({"type": "file", "file": {"filename": filename, "file_data": file_uri}})
            } else {
                unsupported_modality_placeholder("pdf", filename)
            });
        }
        if mime_type.starts_with("video/") {
            return Some(if context.modalities.video {
                json!({"type": "video_url", "video_url": {"url": file_uri}})
            } else {
                unsupported_modality_placeholder("video", filename)
            });
        }
        let display_name = file_data
            .get("displayName")
            .and_then(Value::as_str)
            .filter(|name| !name.is_empty())
            .map(|name| format!(" ({name})"))
            .unwrap_or_default();
        return Some(
            json!({"type": "text", "text": format!("Unsupported file media type: {mime_type}{display_name}.")}),
        );
    }
    None
}

fn unsupported_modality_placeholder(modality: &str, display_name: &str) -> Value {
    let hint = if modality == "pdf" {
        "This model does not support PDF input directly. The read_file tool cannot extract PDF content either. To extract text from the PDF file, try using skills if applicable, or guide user to install pdf skill by running this slash command:\n/extensions install https://github.com/anthropics/skills:document-skills"
    } else {
        return json!({
            "type": "text",
            "text": format!(
                "[Unsupported {modality} file: \"{display_name}\". This model does not support {modality} input. The read_file tool cannot process this type of file either. To handle this file, try using skills if applicable, or any tools installed at system wide, or let the user know you cannot process this type of file.]"
            )
        });
    };
    json!({
        "type": "text",
        "text": format!("[Unsupported {modality} file: \"{display_name}\". {hint}]")
    })
}

fn extract_text_from_content_union(value: &Value) -> String {
    if let Some(value) = value.as_str() {
        return value.to_owned();
    }
    if let Some(values) = value.as_array() {
        return values
            .iter()
            .map(extract_text_from_content_union)
            .filter(|text| !text.is_empty())
            .collect::<Vec<_>>()
            .join("\n");
    }
    if let Some(parts) = value.get("parts").and_then(Value::as_array) {
        return parts
            .iter()
            .filter_map(|part| {
                part.as_str()
                    .or_else(|| part.get("text").and_then(Value::as_str))
            })
            .filter(|text| !text.is_empty())
            .collect::<Vec<_>>()
            .join("\n");
    }
    String::new()
}

fn non_empty(value: Option<&str>) -> Option<&str> {
    value.filter(|value| !value.is_empty())
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

pub(super) fn normalize_mcp_tool_name(name: &str) -> String {
    if !name.starts_with("mcp__") {
        return name.to_owned();
    }
    let units = name.encode_utf16().collect::<Vec<_>>();
    let provider_safe = units.len() <= MAX_TOOL_NAME_LENGTH
        && units.iter().all(|unit| is_safe_ascii_tool_name_unit(*unit))
        && units.first().is_some_and(|unit| is_ascii_alpha_unit(*unit));
    if provider_safe {
        return name.to_owned();
    }

    let sanitized = units
        .iter()
        .map(|unit| {
            if is_safe_ascii_tool_name_unit(*unit) {
                char::from_u32(*unit as u32).unwrap_or('_')
            } else {
                '_'
            }
        })
        .collect::<String>();
    let sanitized = if sanitized
        .chars()
        .next()
        .is_some_and(|character| character.is_ascii_alphabetic())
    {
        sanitized
    } else {
        format!("tool_{sanitized}")
    };
    let suffix = format!("_{}", stable_tool_name_hash(&units));
    let keep = MAX_TOOL_NAME_LENGTH.saturating_sub(suffix.len());
    format!(
        "{}{suffix}",
        sanitized.chars().take(keep).collect::<String>()
    )
}

fn is_safe_ascii_tool_name_unit(unit: u16) -> bool {
    matches!(unit, 45 | 48..=57 | 65..=90 | 95 | 97..=122)
}

fn is_ascii_alpha_unit(unit: u16) -> bool {
    matches!(unit, 65..=90 | 97..=122)
}

fn stable_tool_name_hash(units: &[u16]) -> String {
    let mut hash = 2_166_136_261_u32;
    for unit in units {
        hash = (hash ^ u32::from(*unit)).wrapping_mul(16_777_619);
    }
    let mut value = hash;
    let mut encoded = Vec::new();
    while value > 0 {
        let digit = (value % 36) as u8;
        encoded.push(if digit < 10 {
            char::from(b'0' + digit)
        } else {
            char::from(b'a' + digit - 10)
        });
        value /= 36;
    }
    while encoded.len() < 7 {
        encoded.push('0');
    }
    encoded.iter().rev().collect()
}

fn merge_consecutive_assistant_messages(messages: Vec<Value>) -> Vec<Value> {
    let mut merged: Vec<Value> = Vec::new();
    for message in messages {
        if message.get("role").and_then(Value::as_str) == Some("assistant")
            && merged
                .last()
                .is_some_and(|last| last.get("role").and_then(Value::as_str) == Some("assistant"))
        {
            let last = merged
                .last_mut()
                .expect("assistant message was checked above");
            let last_content = last.get("content").cloned().unwrap_or(Value::Null);
            let current_content = message.get("content").cloned().unwrap_or(Value::Null);
            let array_format = last_content.is_array() || current_content.is_array();
            let combined_content = if array_format {
                let mut parts = as_content_parts(&last_content);
                parts.extend(as_content_parts(&current_content));
                Value::Array(parts)
            } else {
                let last_text = last_content.as_str().unwrap_or("");
                let current_text = current_content.as_str().unwrap_or("");
                let combined = format!("{last_text}{current_text}");
                if combined.is_empty() {
                    Value::Null
                } else {
                    Value::String(combined)
                }
            };
            if let Some(last) = last.as_object_mut() {
                last.insert("content".to_owned(), combined_content);

                let mut calls = last
                    .get("tool_calls")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                calls.extend(
                    message
                        .get("tool_calls")
                        .and_then(Value::as_array)
                        .cloned()
                        .unwrap_or_default(),
                );
                if !calls.is_empty() {
                    last.insert("tool_calls".to_owned(), Value::Array(calls));
                }

                let reasoning = format!(
                    "{}{}",
                    last.get("reasoning_content")
                        .and_then(Value::as_str)
                        .unwrap_or(""),
                    message
                        .get("reasoning_content")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                );
                if !reasoning.is_empty() {
                    last.insert("reasoning_content".to_owned(), Value::String(reasoning));
                }
            }
            continue;
        }
        merged.push(message);
    }
    merged
}

fn as_content_parts(content: &Value) -> Vec<Value> {
    if let Some(parts) = content.as_array() {
        return parts.clone();
    }
    if let Some(text) = content.as_str().filter(|text| !text.is_empty()) {
        return vec![json!({"type": "text", "text": text})];
    }
    Vec::new()
}

#[derive(Default)]
struct AssistantToolLinks {
    tool_calls: Vec<Value>,
    tool_response_indexes: Vec<usize>,
    split_media_indexes: Vec<usize>,
}

fn clean_orphaned_tool_calls(messages: Vec<Value>) -> Vec<Value> {
    let mut links_by_assistant = HashMap::<usize, AssistantToolLinks>::new();
    let mut surviving_call_ids = HashSet::<String>::new();

    for index in 0..messages.len() {
        let Some(calls) = tool_calls_at(&messages[index]) else {
            continue;
        };
        let mut candidate_calls = Vec::new();
        let mut candidate_ids = HashSet::new();
        for call in calls {
            let Some(id) = call.get("id").and_then(Value::as_str) else {
                continue;
            };
            if id.is_empty()
                || surviving_call_ids.contains(id)
                || !candidate_ids.insert(id.to_owned())
            {
                continue;
            }
            candidate_calls.push(call.clone());
        }

        let mut adjacent_response_ids = HashSet::new();
        let mut tool_response_indexes = Vec::new();
        let mut split_media_indexes = Vec::new();
        let mut last_tool_response_matches = false;

        for (next_index, next) in messages.iter().enumerate().skip(index + 1) {
            if next.get("role").and_then(Value::as_str) == Some("tool")
                && next
                    .as_object()
                    .is_some_and(|object| object.contains_key("tool_call_id"))
            {
                let response_id = next
                    .get("tool_call_id")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                if response_id.is_empty() {
                    last_tool_response_matches = false;
                    continue;
                }
                if candidate_ids.contains(response_id)
                    && adjacent_response_ids.insert(response_id.to_owned())
                {
                    tool_response_indexes.push(next_index);
                    last_tool_response_matches = true;
                } else {
                    last_tool_response_matches = false;
                }
                continue;
            }
            if is_split_tool_media_message(next) {
                if last_tool_response_matches {
                    split_media_indexes.push(next_index);
                }
                continue;
            }
            if next.get("role").and_then(Value::as_str) == Some("assistant")
                && tool_calls_at(next).is_none()
            {
                continue;
            }
            break;
        }

        let valid_calls = candidate_calls
            .into_iter()
            .filter(|call| {
                call.get("id")
                    .and_then(Value::as_str)
                    .is_some_and(|id| adjacent_response_ids.contains(id))
            })
            .collect::<Vec<_>>();
        for call in &valid_calls {
            if let Some(id) = call.get("id").and_then(Value::as_str) {
                surviving_call_ids.insert(id.to_owned());
            }
        }
        links_by_assistant.insert(
            index,
            AssistantToolLinks {
                tool_calls: valid_calls,
                tool_response_indexes,
                split_media_indexes,
            },
        );
    }

    let mut cleaned = Vec::new();
    let mut emitted_indexes = HashSet::new();
    for (index, message) in messages.iter().enumerate() {
        if emitted_indexes.contains(&index) {
            continue;
        }
        if tool_calls_at(message).is_some() {
            let links = links_by_assistant.remove(&index).unwrap_or_default();
            if !links.tool_calls.is_empty() {
                let mut cleaned_message = message.clone();
                if let Some(object) = cleaned_message.as_object_mut() {
                    object.insert("tool_calls".to_owned(), Value::Array(links.tool_calls));
                }
                cleaned.push(cleaned_message);
                for response_index in links
                    .tool_response_indexes
                    .into_iter()
                    .chain(links.split_media_indexes)
                {
                    if let Some(response) = messages.get(response_index) {
                        cleaned.push(response.clone());
                        emitted_indexes.insert(response_index);
                    }
                }
            } else if message
                .get("content")
                .and_then(Value::as_str)
                .is_some_and(|content| !content.trim().is_empty())
                || message.get("reasoning_content").is_some_and(is_truthy)
            {
                let mut cleaned_message = message.clone();
                if let Some(object) = cleaned_message.as_object_mut() {
                    object.remove("tool_calls");
                }
                cleaned.push(cleaned_message);
            }
            continue;
        }
        if message.get("role").and_then(Value::as_str) == Some("tool")
            && message
                .as_object()
                .is_some_and(|object| object.contains_key("tool_call_id"))
        {
            continue;
        }
        if is_split_tool_media_message(message) {
            continue;
        }
        cleaned.push(message.clone());
    }
    cleaned
}

fn tool_calls_at(message: &Value) -> Option<&Vec<Value>> {
    if message.get("role").and_then(Value::as_str) != Some("assistant") {
        return None;
    }
    message
        .get("tool_calls")
        .and_then(Value::as_array)
        .filter(|calls| !calls.is_empty())
}

fn is_split_tool_media_message(message: &Value) -> bool {
    if message.get("role").and_then(Value::as_str) != Some("user") {
        return false;
    }
    let Some(first) = message
        .get("content")
        .and_then(Value::as_array)
        .and_then(|parts| parts.first())
    else {
        return false;
    };
    first.get("type").and_then(Value::as_str) == Some("text")
        && first.get("text").and_then(Value::as_str) == Some(SPLIT_TOOL_MEDIA_TEXT)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_system_user_and_assistant_messages_with_reasoning_and_tools() {
        let request = json!({
            "config": {"systemInstruction": {"parts": [{"text": "system"}, "more"]}},
            "contents": [
                {"role": "user", "parts": ["hello", {"text": "there"}]},
                {"role": "model", "parts": [
                    {"text": "thinking", "thought": true},
                    {"text": "answer"},
                    {"functionCall": {"id": "call-1", "name": "mcp__server__read-file", "args": {"path": "a"}}}
                ]},
                {"role": "user", "parts": [{"functionResponse": {"id": "call-1", "response": "done"}}]}
            ]
        });
        let messages = convert_gemini_request_to_openai(&request, &OpenAiRequestContext::default());
        assert_eq!(messages.len(), 4);
        assert_eq!(
            messages[0],
            json!({"role": "system", "content": "system\nmore"})
        );
        assert_eq!(messages[1]["role"], "user");
        assert_eq!(messages[1]["content"][1]["text"], "there");
        assert_eq!(messages[2]["content"], "answer");
        assert_eq!(messages[2]["reasoning_content"], "thinking");
        assert_eq!(
            messages[2]["tool_calls"][0]["function"]["name"],
            "mcp__server__read-file"
        );
        assert_eq!(messages[3]["tool_call_id"], "call-1");
    }

    #[test]
    fn retains_only_tool_calls_with_adjacent_results_and_removes_duplicate_ids() {
        let request = json!({
            "contents": [
                {"role": "model", "parts": [
                    {"functionCall": {"id": "a", "name": "good", "args": {}}},
                    {"functionCall": {"id": "a", "name": "duplicate", "args": {}}},
                    {"functionCall": {"id": "orphan", "name": "bad", "args": {}}}
                ]},
                {"role": "user", "parts": [
                    {"functionResponse": {"id": "a", "response": {"output": "ok"}}},
                    {"functionResponse": {"id": "a", "response": "duplicate"}},
                    {"functionResponse": {"id": "unowned", "response": "drop"}}
                ]}
            ]
        });
        let messages = convert_gemini_request_to_openai(&request, &OpenAiRequestContext::default());
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0]["tool_calls"].as_array().map(Vec::len), Some(1));
        assert_eq!(messages[1]["tool_call_id"], "a");
        assert_eq!(messages[1]["content"][0]["text"], "ok");
    }

    #[test]
    fn splits_unsupported_media_from_tool_messages_after_parallel_results() {
        let request = json!({
            "contents": [
                {"role": "model", "parts": [
                    {"functionCall": {"id": "a", "name": "read", "args": {}}},
                    {"functionCall": {"id": "b", "name": "read", "args": {}}}
                ]},
                {"role": "user", "parts": [
                {"functionResponse": {"id": "a", "response": "text", "parts": [
                    {"inlineData": {"mimeType": "image/png", "data": "AQ=="}}
                ]}},
                {"functionResponse": {"id": "b", "response": "second", "parts": [
                    {"inlineData": {"mimeType": "image/png", "data": "Ag=="}}
                ]}}
            ]}
            ]
        });
        let context = OpenAiRequestContext {
            modalities: InputModalities {
                image: true,
                ..InputModalities::default()
            },
            ..OpenAiRequestContext::default()
        };
        let messages = convert_gemini_request_to_openai(&request, &context);
        assert_eq!(messages.len(), 4);
        assert_eq!(messages[0]["role"], "assistant");
        assert_eq!(messages[1]["role"], "tool");
        assert_eq!(messages[2]["role"], "tool");
        assert_eq!(messages[3]["role"], "user");
        assert_eq!(messages[3]["content"][0]["text"], SPLIT_TOOL_MEDIA_TEXT);
        assert_eq!(messages[3]["content"].as_array().map(Vec::len), Some(3));
    }

    #[test]
    fn substitutes_unsupported_modality_and_merges_assistant_turns() {
        let request = json!({
            "contents": [
                {"role": "user", "parts": [{"inlineData": {"mimeType": "image/png", "data": "AQ==", "displayName": "pic.png"}}]},
                {"role": "model", "parts": [{"text": "first"}]},
                {"role": "model", "parts": [{"text": " second"}]}
            ]
        });
        let context = OpenAiRequestContext {
            modalities: InputModalities::default(),
            ..OpenAiRequestContext::default()
        };
        let messages = convert_gemini_request_to_openai(&request, &context);
        assert_eq!(messages.len(), 2);
        assert!(
            messages[0]["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("pic.png")
        );
        assert_eq!(messages[1]["content"], "first second");
    }

    #[test]
    fn provider_tool_name_normalization_matches_stable_ascii_alias() {
        assert_eq!(
            normalize_mcp_tool_name("mcp__server__read-file"),
            "mcp__server__read-file"
        );
        assert_eq!(normalize_mcp_tool_name("local_tool"), "local_tool");
    }
}
