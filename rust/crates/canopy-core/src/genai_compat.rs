//! Conversion helpers matching Canopy's `core/genai-compat.ts` contract.

use serde_json::{Map, Value, json};
use thiserror::Error;

const PART_MARKERS: &[&str] = &[
    "fileData",
    "text",
    "functionCall",
    "functionResponse",
    "inlineData",
    "videoMetadata",
    "codeExecutionResult",
    "executableCode",
];

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ContentConversionError {
    #[error("partOrString must be a Part object, string, or array")]
    InvalidInput,
    #[error("partOrString cannot be an empty array")]
    EmptyArray,
    #[error("element in PartUnion must be a Part object or string")]
    InvalidElement,
}

fn is_part(value: &Value) -> bool {
    value
        .as_object()
        .is_some_and(|object| PART_MARKERS.iter().any(|key| object.contains_key(*key)))
}

fn text_part(text: String) -> Value {
    let mut object = Map::new();
    object.insert("text".to_owned(), Value::String(text));
    Value::Object(object)
}

/// Convert a Google GenAI `PartListUnion` JSON value into its `parts` array.
/// A string becomes one text part; an empty array and malformed elements are
/// rejected just like the TypeScript compatibility helper.
pub fn to_parts(value: Value) -> Result<Vec<Value>, ContentConversionError> {
    match value {
        Value::String(text) => Ok(vec![text_part(text)]),
        Value::Object(_) if is_part(&value) => Ok(vec![value]),
        Value::Array(values) => {
            if values.is_empty() {
                return Err(ContentConversionError::EmptyArray);
            }
            values
                .into_iter()
                .map(|part| match part {
                    Value::String(text) => Ok(text_part(text)),
                    part if is_part(&part) => Ok(part),
                    _ => Err(ContentConversionError::InvalidElement),
                })
                .collect()
        }
        _ => Err(ContentConversionError::InvalidInput),
    }
}

pub fn create_content(role: &str, value: Value) -> Result<Value, ContentConversionError> {
    Ok(json!({"role": role, "parts": to_parts(value)?}))
}

pub fn create_user_content(value: Value) -> Result<Value, ContentConversionError> {
    create_content("user", value)
}

pub fn create_model_content(value: Value) -> Result<Value, ContentConversionError> {
    create_content("model", value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn converts_strings_and_single_parts() {
        assert_eq!(
            create_user_content(json!("hello")).unwrap(),
            json!({
                "role":"user",
                "parts":[{"text":"hello"}]
            })
        );
        assert_eq!(
            create_model_content(json!({"functionCall":{"name":"read"}})).unwrap(),
            json!({
                "role":"model",
                "parts":[{"functionCall":{"name":"read"}}]
            })
        );
    }

    #[test]
    fn converts_arrays_and_rejects_invalid_values() {
        assert_eq!(
            to_parts(json!(["one", {"text":"two"}])).unwrap(),
            vec![json!({"text":"one"}), json!({"text":"two"})]
        );
        assert_eq!(to_parts(json!([])), Err(ContentConversionError::EmptyArray));
        assert_eq!(
            to_parts(json!([null])),
            Err(ContentConversionError::InvalidElement)
        );
        assert_eq!(
            to_parts(json!(null)),
            Err(ContentConversionError::InvalidInput)
        );
    }
}
