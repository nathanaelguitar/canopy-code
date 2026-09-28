//! Safe serialization of JSON-compatible values.
//!
//! This ports the JSON formatting boundary of
//! `packages/core/src/utils/safeJsonStringify.ts`. `serde_json::Value`
//! cannot contain reference cycles, so the Rust API cannot encounter the
//! source helper's `[Circular]` replacement path.

use serde::Serialize;
use serde_json::Value;
use serde_json::ser::{PrettyFormatter, Serializer};

/// JavaScript-compatible indentation input for JSON.stringify's `space`
/// parameter.
#[derive(Clone, Debug, PartialEq)]
pub enum JsonIndent {
    /// Positive values add that many spaces, capped at ten. Fractions are
    /// truncated toward zero; non-finite and non-positive values add none.
    Number(f64),
    /// Use the first ten UTF-16 code units, without splitting a Rust scalar.
    Text(String),
}

/// Serialize a JSON value, optionally pretty-printed.
///
/// `None` represents JavaScript `undefined` and returns no serialized
/// string; `Some(Value::Null)` returns `"null"`.
pub fn safe_json_stringify(value: Option<&Value>, space: Option<JsonIndent>) -> Option<String> {
    let value = value?;
    let Some(indent) = space.map(indent_bytes).filter(|indent| !indent.is_empty()) else {
        return serde_json::to_string(value).ok();
    };

    let mut bytes = Vec::new();
    let formatter = PrettyFormatter::with_indent(&indent);
    let mut serializer = Serializer::with_formatter(&mut bytes, formatter);
    value.serialize(&mut serializer).ok()?;
    String::from_utf8(bytes).ok()
}

fn indent_bytes(indent: JsonIndent) -> Vec<u8> {
    match indent {
        JsonIndent::Number(number) => {
            let count = if number.is_nan() || number <= 0.0 {
                0
            } else if number.is_infinite() {
                10
            } else {
                number.floor().min(10.0) as usize
            };
            vec![b' '; count]
        }
        JsonIndent::Text(text) => {
            let mut output = String::new();
            let mut units = 0;
            for character in text.chars() {
                let next_units = character.len_utf16();
                if units + next_units > 10 {
                    break;
                }
                units += next_units;
                output.push(character);
            }
            output.into_bytes()
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{JsonIndent, safe_json_stringify};

    #[test]
    fn serializes_objects_arrays_and_primitive_values() {
        assert_eq!(
            safe_json_stringify(Some(&json!({"name":"test","value":42})), None),
            Some(r#"{"name":"test","value":42}"#.to_owned())
        );
        assert_eq!(
            safe_json_stringify(Some(&json!([{"id":1}, "text", true])), None),
            Some(r#"[{"id":1},"text",true]"#.to_owned())
        );
        assert_eq!(
            safe_json_stringify(Some(&json!("test")), None),
            Some(r#""test""#.to_owned())
        );
    }

    #[test]
    fn distinguishes_missing_values_from_null() {
        assert_eq!(safe_json_stringify(None, None), None);
        assert_eq!(
            safe_json_stringify(Some(&Value::Null), None),
            Some("null".to_owned())
        );
    }

    #[test]
    fn formats_with_numeric_and_text_indentation() {
        let value = json!({"name":"test","value":42});
        assert_eq!(
            safe_json_stringify(Some(&value), Some(JsonIndent::Number(2.0))),
            Some("{\n  \"name\": \"test\",\n  \"value\": 42\n}".to_owned())
        );
        assert_eq!(
            safe_json_stringify(Some(&value), Some(JsonIndent::Text("--".to_owned()))),
            Some("{\n--\"name\": \"test\",\n--\"value\": 42\n}".to_owned())
        );
    }

    #[test]
    fn numeric_indentation_matches_json_stringify_clamping() {
        let value = json!({"x":1});
        for (space, expected_prefix) in [
            (-2.0, "{\"x\""),
            (0.9, "{\"x\""),
            (2.9, "{\n  \"x\""),
            (99.0, "{\n          \"x\""),
            (f64::INFINITY, "{\n          \"x\""),
            (f64::NEG_INFINITY, "{\"x\""),
            (f64::NAN, "{\"x\""),
        ] {
            let actual =
                safe_json_stringify(Some(&value), Some(JsonIndent::Number(space))).unwrap();
            assert!(actual.starts_with(expected_prefix), "{space}: {actual}");
        }
    }

    #[test]
    fn text_indentation_is_capped_at_ten_utf16_units() {
        let value = json!({"x":1});
        let actual = safe_json_stringify(
            Some(&value),
            Some(JsonIndent::Text("abcdefghijklmnop".to_owned())),
        )
        .unwrap();
        assert!(actual.starts_with("{\nabcdefghij\"x\""), "{actual}");
    }

    #[test]
    fn repeated_sibling_values_are_serialized_in_full() {
        // serde_json values are owned trees, so shared object references are
        // materialized independently and can never be misclassified as cycles.
        let shared = json!({"name":"shared"});
        let value = json!({"a":shared.clone(), "b":shared});
        assert_eq!(
            safe_json_stringify(Some(&value), None),
            Some(r#"{"a":{"name":"shared"},"b":{"name":"shared"}}"#.to_owned())
        );
    }

    use serde_json::Value;
}
