//! Resolve environment variable placeholders in strings and JSON values.
//!
//! Port of `packages/cli/src/utils/envVarResolver.ts`.

use std::collections::HashMap;

use serde_json::Value;

/// Resolve `$NAME` and `${NAME}` placeholders in a string.
///
/// Values in `custom_env` take precedence over the process environment.
/// Placeholders with no matching string value are left unchanged.
pub fn resolve_env_vars_in_string(
    value: &str,
    custom_env: Option<&HashMap<String, String>>,
) -> String {
    let mut output = String::with_capacity(value.len());
    let mut cursor = 0;

    while let Some(relative_dollar) = value[cursor..].find('$') {
        let dollar = cursor + relative_dollar;
        output.push_str(&value[cursor..dollar]);

        let after_dollar = dollar + 1;
        let Some(&next) = value.as_bytes().get(after_dollar) else {
            output.push('$');
            cursor = after_dollar;
            continue;
        };

        let (name_end, placeholder_end) = if is_word_byte(next) {
            let mut end = after_dollar + 1;
            while value
                .as_bytes()
                .get(end)
                .is_some_and(|byte| is_word_byte(*byte))
            {
                end += 1;
            }
            (end, end)
        } else if next == b'{' {
            let name_start = after_dollar + 1;
            let Some(relative_close) = value[name_start..].find('}') else {
                output.push('$');
                cursor = after_dollar;
                continue;
            };
            let close = name_start + relative_close;
            if close == name_start {
                output.push('$');
                cursor = after_dollar;
                continue;
            }
            (close, close + 1)
        } else {
            output.push('$');
            cursor = after_dollar;
            continue;
        };

        let name_start = if next == b'{' {
            after_dollar + 1
        } else {
            after_dollar
        };
        let name = &value[name_start..name_end];
        if let Some(replacement) = custom_env
            .and_then(|env| env.get(name).cloned())
            .or_else(|| std::env::var(name).ok())
        {
            output.push_str(&replacement);
        } else {
            output.push_str(&value[dollar..placeholder_end]);
        }
        cursor = placeholder_end;
    }

    output.push_str(&value[cursor..]);
    output
}

/// Recursively resolve environment placeholders in JSON strings.
///
/// Arrays and objects are rebuilt without mutating `value`; object keys and
/// non-string JSON primitives are preserved. JSON values cannot represent
/// JavaScript object identity or cycles, so this function accepts JSON trees
/// and has no circular-reference handling.
pub fn resolve_env_vars_in_object(
    value: &Value,
    custom_env: Option<&HashMap<String, String>>,
) -> Value {
    match value {
        Value::String(string) => Value::String(resolve_env_vars_in_string(string, custom_env)),
        Value::Array(values) => Value::Array(
            values
                .iter()
                .map(|value| resolve_env_vars_in_object(value, custom_env))
                .collect(),
        ),
        Value::Object(object) => Value::Object(
            object
                .iter()
                .map(|(key, value)| (key.clone(), resolve_env_vars_in_object(value, custom_env)))
                .collect(),
        ),
        primitive => primitive.clone(),
    }
}

fn is_word_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde_json::{Value, json};

    use super::{resolve_env_vars_in_object, resolve_env_vars_in_string};

    fn custom_env(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect()
    }

    #[test]
    fn resolves_both_placeholder_forms_and_multiple_variables() {
        let env = custom_env(&[
            ("TEST_VAR", "test-value"),
            ("HOST", "localhost"),
            ("PORT", "3000"),
        ]);
        assert_eq!(
            resolve_env_vars_in_string("Value is $TEST_VAR", Some(&env)),
            "Value is test-value"
        );
        assert_eq!(
            resolve_env_vars_in_string("Value is ${TEST_VAR}", Some(&env)),
            "Value is test-value"
        );
        assert_eq!(
            resolve_env_vars_in_string("URL: http://$HOST:${PORT}/api", Some(&env)),
            "URL: http://localhost:3000/api"
        );
    }

    #[test]
    fn leaves_missing_variables_and_strings_without_variables_unchanged() {
        assert_eq!(
            resolve_env_vars_in_string("Value is $UNDEFINED_VAR", None),
            "Value is $UNDEFINED_VAR"
        );
        assert_eq!(
            resolve_env_vars_in_string("Value is ${UNDEFINED_VAR}", None),
            "Value is ${UNDEFINED_VAR}"
        );
        assert_eq!(resolve_env_vars_in_string("", None), "");
        assert_eq!(
            resolve_env_vars_in_string("No variables here", None),
            "No variables here"
        );
    }

    #[test]
    fn resolves_defined_variables_and_preserves_missing_ones_in_mixed_strings() {
        let env = custom_env(&[("DEFINED", "value")]);
        assert_eq!(
            resolve_env_vars_in_string("$DEFINED and $UNDEFINED mixed", Some(&env)),
            "value and $UNDEFINED mixed"
        );
    }

    #[test]
    fn custom_environment_overrides_process_environment() {
        let Some((key, process_value)) = std::env::vars_os()
            .find_map(|(key, value)| key.into_string().ok().zip(value.into_string().ok()))
        else {
            return;
        };
        let env = custom_env(&[(&key, "custom-value")]);
        assert_eq!(
            resolve_env_vars_in_string(&format!("${{{key}}}"), Some(&env)),
            "custom-value"
        );
        assert_eq!(
            resolve_env_vars_in_string(&format!("${{{key}}}"), None),
            process_value
        );
    }

    #[test]
    fn recursively_resolves_nested_objects_and_arrays_without_mutating_input() {
        let env = custom_env(&[
            ("API_KEY", "secret-123"),
            ("DB_URL", "postgresql://localhost/test"),
            ("ENV", "production"),
            ("VERSION", "1.0.0"),
            ("SERVER_PORT", "8080"),
            ("API_TOKEN", "token-123"),
        ]);
        let config = json!({
            "server": {
                "auth": {"key": "$API_KEY"},
                "database": "${DB_URL}"
            },
            "port": 3000,
            "tags": ["$ENV", "app", "${VERSION}"],
            "mcpServers": {
                "test-server": {
                    "command": "node",
                    "args": ["server.js", "--port", "${SERVER_PORT}"],
                    "env": {"API_KEY": "$API_TOKEN", "STATIC_VALUE": "unchanged"},
                    "timeout": 5000
                }
            }
        });
        let original = config.clone();

        let result = resolve_env_vars_in_object(&config, Some(&env));

        assert_eq!(config, original);
        assert_eq!(
            result,
            json!({
                "server": {
                    "auth": {"key": "secret-123"},
                    "database": "postgresql://localhost/test"
                },
                "port": 3000,
                "tags": ["production", "app", "1.0.0"],
                "mcpServers": {
                    "test-server": {
                        "command": "node",
                        "args": ["server.js", "--port", "8080"],
                        "env": {"API_KEY": "token-123", "STATIC_VALUE": "unchanged"},
                        "timeout": 5000
                    }
                }
            })
        );
    }

    #[test]
    fn preserves_non_string_json_types_and_empty_values() {
        let config = json!({
            "enabled": true,
            "count": 42,
            "value": null,
            "tags": ["item1", "item2"],
            "empty": "",
            "zero": 0,
            "false": false
        });
        assert_eq!(resolve_env_vars_in_object(&config, None), config);
    }

    #[test]
    fn resolves_top_level_strings_and_preserves_top_level_primitives() {
        let env = custom_env(&[("VALUE", "resolved")]);
        assert_eq!(
            resolve_env_vars_in_object(&Value::String("$VALUE".into()), Some(&env)),
            json!("resolved")
        );
        for value in [json!(null), json!(true), json!(42)] {
            assert_eq!(resolve_env_vars_in_object(&value, Some(&env)), value);
        }
    }
}
