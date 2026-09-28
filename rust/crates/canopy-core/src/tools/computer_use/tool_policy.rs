//! Pure policy and schema-directed coercion helpers for computer-use calls.
//!
//! These mirror the source helpers in `packages/core/src/tools/computer-use/tool.ts`.
//! They do not execute tools or decide whether a user has granted permission.

use std::sync::LazyLock;

use regex::Regex;
use serde_json::{Map, Number, Value};

const HIGH_RISK_TOOLS: &[&str] = &[
    "kill_app",
    "launch_app",
    "start_recording",
    "set_config",
    "replay_trajectory",
];

const HIGH_RISK_PAGE_ACTIONS: &[&str] = &["execute_javascript", "enable_javascript_apple_events"];

static INTEGER_STRING: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\A-?[0-9]+\z").expect("integer string regex is valid"));
static NUMBER_STRING: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\A-?[0-9]+(\.[0-9]+)?\z").expect("number string regex is valid"));

/// Whether a computer-use call is gated as destructive or sensitive.
///
/// Tool names and page actions intentionally match the upstream TypeScript
/// sets exactly. Only a string-valued `action` parameter can match a page gate.
pub fn is_high_risk_call(upstream_name: &str, params: &Map<String, Value>) -> bool {
    if HIGH_RISK_TOOLS.contains(&upstream_name) {
        return true;
    }

    upstream_name == "page"
        && params
            .get("action")
            .and_then(Value::as_str)
            .is_some_and(|action| HIGH_RISK_PAGE_ACTIONS.contains(&action))
}

/// Coerce scalar parameter values in the directions declared by JSON Schema.
///
/// Numeric strings are accepted only when they match the source's exact
/// decimal grammar after ECMAScript-style trimming. Values that do not parse
/// to a finite binary64 number are left untouched, as are all fields without a
/// matching `properties.<name>.type` declaration.
pub fn coerce_types(params: &Map<String, Value>, schema: &Value) -> Map<String, Value> {
    let Some(properties) = schema.get("properties").and_then(Value::as_object) else {
        return params.clone();
    };

    params
        .iter()
        .map(|(key, value)| {
            let field_type = properties
                .get(key)
                .and_then(Value::as_object)
                .and_then(|field| field.get("type"))
                .and_then(Value::as_str);

            let coerced = match (field_type, value) {
                (Some("integer"), Value::String(text)) => coerce_numeric_string(text, true),
                (Some("number"), Value::String(text)) => coerce_numeric_string(text, false),
                (Some("string"), Value::Number(number)) => {
                    js_number_to_string(number).map(Value::String)
                }
                _ => None,
            };

            (key.clone(), coerced.unwrap_or_else(|| value.clone()))
        })
        .collect()
}

fn coerce_numeric_string(value: &str, integer: bool) -> Option<Value> {
    let trimmed = ecmascript_trim(value);
    let matches_type = if integer {
        INTEGER_STRING.is_match(trimmed)
    } else {
        NUMBER_STRING.is_match(trimmed)
    };
    if !matches_type {
        return None;
    }

    // Parsing a decimal string as f64 has the same binary64 rounding model as
    // JavaScript Number/parseInt for these grammars. `parseInt` and `parseFloat`
    // cannot diverge here because the regex excludes prefixes and exponents.
    let parsed = trimmed.parse::<f64>().ok()?;
    Number::from_f64(parsed).map(Value::Number)
}

/// Trim exactly the ECMAScript WhiteSpace and LineTerminator code points used
/// by `String.prototype.trim`; Rust's `char::is_whitespace` has a few different
/// members (notably U+0085) and omits U+FEFF.
fn ecmascript_trim(value: &str) -> &str {
    fn is_ecmascript_whitespace(ch: char) -> bool {
        matches!(
            ch,
            '\u{0009}'
                | '\u{000A}'
                | '\u{000B}'
                | '\u{000C}'
                | '\u{000D}'
                | '\u{0020}'
                | '\u{00A0}'
                | '\u{1680}'
                | '\u{2000}'
                ..='\u{200A}'
                    | '\u{2028}'
                    | '\u{2029}'
                    | '\u{202F}'
                    | '\u{205F}'
                    | '\u{3000}'
                    | '\u{FEFF}'
        )
    }

    value.trim_matches(is_ecmascript_whitespace)
}

/// Convert a JSON number to the string JavaScript `String(number)` would use.
///
/// Rust's shortest-round-trip float formatting supplies the significant digits;
/// this function applies ECMAScript's decimal/exponent thresholds and spelling.
fn js_number_to_string(number: &Number) -> Option<String> {
    let value = number.as_f64()?;
    if !value.is_finite() {
        return None;
    }
    if value == 0.0 {
        // JavaScript String(-0) is "0".
        return Some("0".to_owned());
    }

    let negative = value.is_sign_negative();
    let magnitude = value.abs();
    let shortest = format!("{magnitude:?}");
    let exponent_split = shortest
        .split_once('e')
        .or_else(|| shortest.split_once('E'));
    let (mantissa, exponent) = match exponent_split {
        Some((mantissa, exponent)) => (mantissa, exponent.parse::<i32>().ok()?),
        None => (shortest.as_str(), 0),
    };

    let decimal_position = mantissa.find('.').unwrap_or(mantissa.len()) as i32 + exponent;
    let mut digits: String = mantissa.chars().filter(|ch| *ch != '.').collect();
    while digits.len() > 1 && digits.ends_with('0') {
        digits.pop();
    }

    let body = if (1e-6..1e21).contains(&magnitude) {
        if decimal_position <= 0 {
            format!("0.{}{}", "0".repeat((-decimal_position) as usize), digits)
        } else if decimal_position as usize >= digits.len() {
            format!(
                "{}{}",
                digits,
                "0".repeat(decimal_position as usize - digits.len())
            )
        } else {
            let split = decimal_position as usize;
            format!("{}.{}", &digits[..split], &digits[split..])
        }
    } else {
        let scientific_exponent = decimal_position - 1;
        if digits.len() == 1 {
            format!("{}e{:+}", digits, scientific_exponent)
        } else {
            format!(
                "{}.{}e{:+}",
                &digits[..1],
                &digits[1..],
                scientific_exponent
            )
        }
    };

    Some(if negative { format!("-{body}") } else { body })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn params(value: Value) -> Map<String, Value> {
        value
            .as_object()
            .expect("test params are an object")
            .clone()
    }

    #[test]
    fn flags_all_high_risk_tool_names_and_both_sensitive_page_actions() {
        for name in [
            "kill_app",
            "launch_app",
            "start_recording",
            "set_config",
            "replay_trajectory",
        ] {
            assert!(is_high_risk_call(name, &Map::new()), "{name}");
        }

        assert!(is_high_risk_call(
            "page",
            &params(json!({"action":"execute_javascript"}))
        ));
        assert!(is_high_risk_call(
            "page",
            &params(json!({"action":"enable_javascript_apple_events"}))
        ));
    }

    #[test]
    fn allows_read_and_visible_interaction_page_actions() {
        for action in ["get_text", "query_dom", "click_element"] {
            assert!(
                !is_high_risk_call("page", &params(json!({"action":action}))),
                "{action}"
            );
        }
        assert!(!is_high_risk_call("page", &params(json!({"action":42}))));
        assert!(!is_high_risk_call("click", &Map::new()));
        assert!(!is_high_risk_call("not_a_tool", &Map::new()));
    }

    #[test]
    fn coerces_integer_and_number_strings_using_the_declared_schema_type() {
        let schema = json!({"properties":{
            "integer":{"type":"integer"},
            "number":{"type":"number"},
        }});
        let result = coerce_types(
            &params(json!({"integer":"  -11\n", "number":" 11.5 "})),
            &schema,
        );
        assert_eq!(result["integer"].as_f64(), Some(-11.0));
        assert_eq!(result["number"].as_f64(), Some(11.5));

        // ECMAScript trim includes BOM and excludes NEL.
        let bom_wrapped = params(json!({"integer":"\u{FEFF}7\u{FEFF}"}));
        assert_eq!(
            coerce_types(&bom_wrapped, &schema)["integer"].as_f64(),
            Some(7.0)
        );
        let nel_wrapped = params(json!({"integer":"\u{0085}7\u{0085}"}));
        assert!(coerce_types(&nel_wrapped, &schema)["integer"].is_string());
    }

    #[test]
    fn leaves_fractional_integer_strings_and_nonmatching_numeric_strings_untouched() {
        let schema = json!({"properties":{
            "integer":{"type":"integer"},
            "number":{"type":"number"},
        }});
        for bad in ["11.5", "+11", "1e3", ".5", "5.", "--1", "abc"] {
            let input = params(json!({"integer":bad}));
            assert_eq!(coerce_types(&input, &schema)["integer"], json!(bad));
        }
        for bad in ["+1", "1e3", ".5", "5.", "Infinity", "abc"] {
            let input = params(json!({"number":bad}));
            assert_eq!(coerce_types(&input, &schema)["number"], json!(bad));
        }

        let huge_integer = "9".repeat(400);
        let input = params(json!({"integer":huge_integer}));
        assert!(coerce_types(&input, &schema)["integer"].is_string());
    }

    #[test]
    fn coerces_numbers_to_javascript_strings_and_keeps_other_fields_unchanged() {
        let schema = json!({"properties":{
            "label":{"type":"string"},
            "small":{"type":"string"},
            "large":{"type":"string"},
            "negative_zero":{"type":"string"},
            "app":{"type":"string"},
            "coordinate":{"type":"number"},
        }});
        let input = params(json!({
            "label":11,
            "small":0.000001,
            "large":1e21,
            "negative_zero":-0.0,
            "app":"com.apple.stocks",
            "coordinate":100,
            "unknown":{"preserved":true},
        }));
        let result = coerce_types(&input, &schema);
        assert_eq!(result["label"], json!("11"));
        assert_eq!(result["small"], json!("0.000001"));
        assert_eq!(result["large"], json!("1e+21"));
        assert_eq!(result["negative_zero"], json!("0"));
        assert_eq!(result["app"], json!("com.apple.stocks"));
        assert_eq!(result["coordinate"], json!(100));
        assert_eq!(result["unknown"], json!({"preserved":true}));

        let no_properties = coerce_types(&input, &json!({"type":"object"}));
        assert_eq!(no_properties, input);
    }

    #[test]
    fn number_to_string_formats_small_exponents_and_rounded_binary64_values() {
        assert_eq!(
            js_number_to_string(json!(1e-7).as_number().unwrap()).as_deref(),
            Some("1e-7")
        );
        assert_eq!(
            js_number_to_string(json!(1.2345e-7).as_number().unwrap()).as_deref(),
            Some("1.2345e-7")
        );
        assert_eq!(
            js_number_to_string(json!(9007199254740993_u64).as_number().unwrap()).as_deref(),
            Some("9007199254740992")
        );
    }
}
