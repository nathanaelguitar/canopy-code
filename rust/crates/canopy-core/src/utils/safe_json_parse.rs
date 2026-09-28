//! Parse JSON, falling back to `jsonrepair-rs` for common model-output errors.
//!
//! This mirrors `packages/core/src/utils/safeJsonParse.ts`. `None` represents a
//! nullish JavaScript input; Rust's `&str` type prevents other non-string inputs.

use serde_json::{Value, json};

/// Parse a JSON string, repair common formatting errors, then return `fallback`
/// if neither parse succeeds. An empty string or absent input returns fallback
/// immediately, matching the source helper's falsy-input branch.
pub fn safe_json_parse(input: Option<&str>, fallback: Value) -> Value {
    let Some(input) = input.filter(|input| !input.is_empty()) else {
        return fallback;
    };

    if let Ok(value) = serde_json::from_str(input) {
        return value;
    }

    jsonrepair_rs::jsonrepair(input)
        .ok()
        .and_then(|repaired| serde_json::from_str(&repaired).ok())
        .unwrap_or(fallback)
}

/// Parse a JSON string with the source helper's default empty-object fallback.
pub fn safe_json_parse_default(input: Option<&str>) -> Value {
    safe_json_parse(input, json!({}))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{safe_json_parse, safe_json_parse_default};

    #[test]
    fn parses_valid_objects_arrays_and_nested_values() {
        assert_eq!(
            safe_json_parse_default(Some(r#"{"name":"test","value":123}"#)),
            json!({"name": "test", "value": 123})
        );
        assert_eq!(
            safe_json_parse_default(Some(r#"["item1","item2","item3"]"#)),
            json!(["item1", "item2", "item3"])
        );
        assert_eq!(
            safe_json_parse_default(Some(
                r#"{"config":{"paths":["testlogs/*.py"],"options":{"recursive":true}}}"#
            )),
            json!({"config": {"paths": ["testlogs/*.py"], "options": {"recursive": true}}})
        );
    }

    #[test]
    fn preserves_json_primitive_values() {
        for (input, expected) in [
            ("null", json!(null)),
            ("true", json!(true)),
            ("42", json!(42)),
            (r#""text""#, json!("text")),
        ] {
            assert_eq!(safe_json_parse_default(Some(input)), expected, "{input}");
        }
    }

    #[test]
    fn repairs_single_quotes_unquoted_keys_trailing_commas_and_comments() {
        assert_eq!(
            safe_json_parse_default(Some("{'name': 'test', 'value': 123}")),
            json!({"name": "test", "value": 123})
        );
        assert_eq!(
            safe_json_parse_default(Some(r#"{name: "test", value: 123}"#)),
            json!({"name": "test", "value": 123})
        );
        assert_eq!(
            safe_json_parse_default(Some(r#"{"name":"test","value":123,}"#)),
            json!({"name": "test", "value": 123})
        );
        assert_eq!(
            safe_json_parse_default(Some("{\"name\": \"test\", // comment\n \"value\": 123}")),
            json!({"name": "test", "value": 123})
        );
    }

    #[test]
    fn returns_custom_fallback_for_absent_empty_or_unrepairable_input() {
        let fallback = json!({"default": "value"});
        assert_eq!(safe_json_parse(None, fallback.clone()), fallback);
        assert_eq!(safe_json_parse(Some(""), fallback.clone()), fallback);
        assert_eq!(safe_json_parse(Some("{\"a\","), fallback.clone()), fallback);
        assert_eq!(safe_json_parse_default(Some("{\"a\",")), json!({}));

        let fallback = json!(["fallback"]);
        assert_eq!(safe_json_parse(Some("   "), fallback.clone()), fallback);
    }

    #[test]
    fn invalid_unquoted_text_is_repaired_to_a_json_string_like_the_source() {
        assert_eq!(
            safe_json_parse_default(Some("invalid json")),
            json!("invalid json")
        );

        // A successful repair wins even when a custom fallback has another shape.
        assert_eq!(
            safe_json_parse(Some("invalid json"), json!({"error": "fallback"})),
            json!("invalid json")
        );
    }

    #[test]
    fn only_empty_string_is_short_circuited() {
        let fallback = json!({"fallback": true});
        assert_eq!(safe_json_parse(Some(""), fallback.clone()), fallback);
        assert_eq!(safe_json_parse(Some("  {}  "), json!(null)), json!({}));
    }
}
