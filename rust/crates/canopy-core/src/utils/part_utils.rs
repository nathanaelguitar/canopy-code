//! Helpers for Google GenAI-style `PartListUnion` values.
//!
//! Provider content is kept as [`serde_json::Value`] because the TypeScript
//! SDK's `Part` is an open object and carries provider-specific fields. This
//! preserves unknown fields and the distinction between an absent property
//! and an explicitly-null property when parts are copied or edited.

use std::future::Future;

use serde_json::{Map, Value};

fn js_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_f64().is_some_and(|value| value != 0.0),
        Value::String(value) => !value.is_empty(),
        // JavaScript arrays and objects are truthy, including empty ones.
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
            .map(|value| match value {
                Value::Null => String::new(),
                value => js_string(value),
            })
            .collect::<Vec<_>>()
            .join(","),
        Value::Object(_) => "[object Object]".to_owned(),
    }
}

fn text_part(text: impl Into<String>) -> Value {
    let mut part = Map::new();
    part.insert("text".to_owned(), Value::String(text.into()));
    Value::Object(part)
}

/// Convert a part list into display text.
///
/// `None` corresponds to JavaScript `undefined`; both it and JSON `null`
/// follow the source helper's falsy-input behavior and produce an empty
/// string. Unknown fields remain available on the original values.
pub fn part_to_string(value: Option<&Value>, verbose: bool) -> String {
    let Some(value) = value else {
        return String::new();
    };

    match value {
        Value::Null | Value::Bool(false) => return String::new(),
        Value::Number(number) if number.as_f64() == Some(0.0) => return String::new(),
        Value::String(text) => return text.clone(),
        Value::Array(parts) => {
            return parts
                .iter()
                .map(|part| part_to_string(Some(part), verbose))
                .collect();
        }
        _ => {}
    }

    let Some(part) = value.as_object() else {
        // The public SDK type excludes other primitive values. Keep malformed
        // runtime values harmless, as the source's final nullish fallback does.
        return String::new();
    };

    if verbose {
        // These source checks use `!== undefined`, so a present null value
        // still selects the corresponding marker.
        if part.contains_key("videoMetadata") {
            return "[Video Metadata]".to_owned();
        }
        if part.get("thought").is_some_and(js_truthy) {
            return match part.get("text").filter(|text| js_truthy(text)) {
                Some(text) => format!("[Thought: {}]", js_string(text)),
                None => "[Thought]".to_owned(),
            };
        }
        if part.contains_key("codeExecutionResult") {
            return "[Code Execution Result]".to_owned();
        }
        if part.contains_key("executableCode") {
            return "[Executable Code]".to_owned();
        }
        if part.contains_key("fileData") {
            return "[File Data]".to_owned();
        }
        if let Some(call) = part.get("functionCall") {
            let name = call
                .as_object()
                .and_then(|call| call.get("name"))
                .map(js_string)
                .unwrap_or_else(|| "undefined".to_owned());
            return format!("[Function Call: {name}]");
        }
        if let Some(response) = part.get("functionResponse") {
            let name = response
                .as_object()
                .and_then(|response| response.get("name"))
                .map(js_string)
                .unwrap_or_else(|| "undefined".to_owned());
            return format!("[Function Response: {name}]");
        }
        if let Some(inline_data) = part.get("inlineData") {
            let mime_type = inline_data
                .as_object()
                .and_then(|inline_data| inline_data.get("mimeType"))
                .map(js_string)
                .unwrap_or_else(|| "undefined".to_owned());
            return format!("<{mime_type}>");
        }
    }

    part.get("text")
        .filter(|text| !text.is_null())
        .map(js_string)
        .unwrap_or_default()
}

/// Read visible text from the first candidate, excluding thought parts.
/// `None` distinguishes the source helper's null result (no first-candidate
/// parts) from `Some("")` (parts exist but none contribute visible text).
pub fn get_response_text(response: Option<&Value>) -> Option<String> {
    let candidates = response?.get("candidates")?.as_array()?;
    let candidate = candidates.first()?;
    let parts = candidate.get("content")?.get("parts")?.as_array()?;
    if parts.is_empty() {
        return None;
    }

    let mut text = String::new();
    for part in parts {
        let Some(part) = part.as_object() else {
            continue;
        };
        let Some(part_text) = part.get("text").filter(|text| js_truthy(text)) else {
            continue;
        };
        if part.get("thought").is_some_and(js_truthy) {
            continue;
        }
        text.push_str(&js_string(part_text));
    }
    Some(text)
}

/// Transform each text entry in a part list in order, passing other parts
/// through unchanged. The async callback mirrors the source utility's awaited
/// sequential loop, and accepting JSON text values preserves explicit nulls.
pub async fn flat_map_text_parts<F, Fut>(parts: &Value, mut transform: F) -> Vec<Value>
where
    F: FnMut(Value) -> Fut,
    Fut: Future<Output = Vec<Value>>,
{
    let normalized;
    let part_array = match parts {
        Value::Array(parts) => parts.as_slice(),
        Value::String(text) => {
            normalized = vec![text_part(text.clone())];
            normalized.as_slice()
        }
        value => {
            normalized = vec![value.clone()];
            normalized.as_slice()
        }
    };

    let mut result = Vec::new();
    for part in part_array {
        let text_to_process = match part {
            Value::String(text) => Some(Value::String(text.clone())),
            Value::Object(part) => part.get("text").cloned(),
            _ => None,
        };
        if let Some(text) = text_to_process {
            result.extend(transform(text).await);
        } else {
            result.push(part.clone());
        }
    }
    result
}

/// Append text to the final text part, copying the prompt and retaining all
/// unknown part fields. Explicit-null `text` is stringified like JavaScript's
/// template interpolation; an absent `text` field causes a new part instead.
pub fn append_to_last_text_part(
    prompt: &[Value],
    text_to_append: &str,
    separator: &str,
) -> Vec<Value> {
    if text_to_append.is_empty() {
        return prompt.to_vec();
    }
    if prompt.is_empty() {
        return vec![text_part(text_to_append)];
    }

    let mut new_prompt = prompt.to_vec();
    let last_index = new_prompt.len() - 1;
    match &mut new_prompt[last_index] {
        Value::String(text) => {
            text.push_str(separator);
            text.push_str(text_to_append);
        }
        Value::Object(part) if part.contains_key("text") => {
            let current = js_string(&part["text"]);
            part.insert(
                "text".to_owned(),
                Value::String(format!("{current}{separator}{text_to_append}")),
            );
        }
        _ => new_prompt.push(text_part(format!("{separator}{text_to_append}"))),
    }
    new_prompt
}

/// Prepend text to the first text part, inserting a new text part if needed.
/// For an existing `text: null`, the source helper's nullish coalescing treats
/// its current text as empty while preserving the rest of the object.
pub fn prepend_to_first_text_part(
    prompt: &[Value],
    text_to_prepend: &str,
    separator: &str,
) -> Vec<Value> {
    if text_to_prepend.is_empty() {
        return prompt.to_vec();
    }
    if prompt.is_empty() {
        return vec![text_part(text_to_prepend)];
    }

    let Some(text_index) = prompt.iter().position(|part| {
        part.is_string()
            || part
                .as_object()
                .is_some_and(|part| part.contains_key("text"))
    }) else {
        let mut result = Vec::with_capacity(prompt.len() + 1);
        result.push(text_part(text_to_prepend));
        result.extend_from_slice(prompt);
        return result;
    };

    let mut new_prompt = prompt.to_vec();
    match &mut new_prompt[text_index] {
        Value::String(text) => {
            *text = format!("{text_to_prepend}{separator}{text}");
        }
        Value::Object(part) => {
            let current = part
                .get("text")
                .filter(|text| !text.is_null())
                .map(js_string)
                .unwrap_or_default();
            part.insert(
                "text".to_owned(),
                Value::String(format!("{text_to_prepend}{separator}{current}")),
            );
        }
        _ => unreachable!("selected text part must be a string or object"),
    }
    new_prompt
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn part_to_string_handles_text_thought_markers_and_null_presence() {
        assert_eq!(part_to_string(None, false), "");
        assert_eq!(part_to_string(Some(&Value::Null), true), "");
        assert_eq!(
            part_to_string(Some(&json!(["a", {"text":"b"}])), false),
            "ab"
        );
        assert_eq!(
            part_to_string(Some(&json!({"thought":true,"text":"think"})), true),
            "[Thought: think]"
        );
        assert_eq!(
            part_to_string(Some(&json!({"thought":false,"text":"ordinary"})), true),
            "ordinary"
        );
        assert_eq!(
            part_to_string(Some(&json!({"videoMetadata":null,"text":"fallback"})), true),
            "[Video Metadata]"
        );
        assert_eq!(
            part_to_string(Some(&json!({"inlineData":{"mimeType":"image/png"}})), true),
            "<image/png>"
        );
    }

    #[test]
    fn response_text_distinguishes_missing_parts_from_empty_visible_text() {
        assert_eq!(get_response_text(None), None);
        assert_eq!(get_response_text(Some(&json!({"candidates":[]}))), None);
        assert_eq!(
            get_response_text(Some(&json!({"candidates":[{"content":{"parts":[]}}]}))),
            None
        );
        assert_eq!(
            get_response_text(Some(
                &json!({"candidates":[{"content":{"parts":[{"thought":true,"text":"hidden"},{"functionCall":{"name":"f"}}]}}]})
            )),
            Some(String::new())
        );
        assert_eq!(
            get_response_text(Some(
                &json!({"candidates":[{"content":{"parts":[{"text":"a"},{"text":"hidden","thought":true},{"text":"b"}]}}]})
            )),
            Some("ab".to_owned())
        );
    }

    #[tokio::test]
    async fn flat_map_text_parts_transforms_sequentially_and_passes_through_nontext() {
        let source = json!([{"text":"ab","thought":true},{"functionCall":{"name":"go"}},"cd",{"text":null},{"other":true}]);
        let mut seen = Vec::new();
        let output = flat_map_text_parts(&source, |text| {
            seen.push(text.clone());
            async move { vec![json!({"mapped":text})] }
        })
        .await;
        assert_eq!(seen, vec![json!("ab"), json!("cd"), Value::Null]);
        assert_eq!(
            output,
            vec![
                json!({"mapped":"ab"}),
                json!({"functionCall":{"name":"go"}}),
                json!({"mapped":"cd"}),
                json!({"mapped":null}),
                json!({"other":true})
            ]
        );
    }

    #[test]
    fn append_and_prepend_preserve_other_fields_and_null_semantics() {
        assert_eq!(
            append_to_last_text_part(&[json!({"text":null,"thought":true})], "tail", "--"),
            vec![json!({"text":"null--tail","thought":true})]
        );
        assert_eq!(
            append_to_last_text_part(&[json!({"thought":true})], "tail", "--"),
            vec![json!({"thought":true}), json!({"text":"--tail"})]
        );
        assert_eq!(
            prepend_to_first_text_part(&[json!({"text":null,"thought":true})], "head", "--"),
            vec![json!({"text":"head--","thought":true})]
        );
        assert_eq!(
            prepend_to_first_text_part(&[json!({"functionCall":{"name":"f"}})], "head", "\n\n"),
            vec![json!({"text":"head"}), json!({"functionCall":{"name":"f"}})]
        );
    }
}
