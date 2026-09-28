//! Predicates for provider messages that contain only function parts.

use serde_json::Value;

fn js_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_f64().is_some_and(|value| value != 0.0),
        Value::String(value) => !value.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}

fn all_parts_have_truthy_field(content: &Value, role: &str, field: &str) -> bool {
    if content.get("role").and_then(Value::as_str) != Some(role) {
        return false;
    }
    let Some(parts) = content.get("parts").and_then(Value::as_array) else {
        return false;
    };
    !parts.is_empty()
        && parts.iter().all(|part| {
            part.as_object()
                .and_then(|part| part.get(field))
                .is_some_and(js_truthy)
        })
}

/// Whether a non-empty user content consists entirely of truthy function
/// response values. Missing and explicit-null `functionResponse` are distinct
/// in the input, and both are false as in JavaScript's `!!part.functionResponse`.
pub fn is_function_response(content: &Value) -> bool {
    all_parts_have_truthy_field(content, "user", "functionResponse")
}

/// Whether a non-empty model content consists entirely of truthy function
/// call values.
pub fn is_function_call(content: &Value) -> bool {
    all_parts_have_truthy_field(content, "model", "functionCall")
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn function_response_requires_nonempty_user_turn_and_truthy_every_part() {
        assert!(is_function_response(
            &json!({"role":"user","parts":[{"functionResponse":{"name":"f","response":{}}}]})
        ));
        assert!(!is_function_response(&json!({"role":"user","parts":[]})));
        assert!(!is_function_response(
            &json!({"role":"user","parts":[{"functionResponse":null}]})
        ));
        assert!(!is_function_response(
            &json!({"role":"user","parts":[{"functionResponse":{}},{"text":"x"}]})
        ));
        assert!(!is_function_response(
            &json!({"role":"model","parts":[{"functionResponse":{}}]})
        ));
    }

    #[test]
    fn function_call_requires_nonempty_model_turn_and_truthy_every_part() {
        assert!(is_function_call(
            &json!({"role":"model","parts":[{"functionCall":{"name":"f","args":{}}}]})
        ));
        assert!(!is_function_call(&json!({"role":"model","parts":[]})));
        assert!(!is_function_call(
            &json!({"role":"model","parts":[{"functionCall":false}]})
        ));
        assert!(!is_function_call(
            &json!({"role":"model","parts":[{"functionCall":{}},{"text":"x"}]})
        ));
        assert!(!is_function_call(
            &json!({"role":"user","parts":[{"functionCall":{}}]})
        ));
    }
}
