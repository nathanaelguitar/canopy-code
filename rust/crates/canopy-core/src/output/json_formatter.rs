// Copyright 2025 Google LLC
// SPDX-License-Identifier: Apache-2.0

//! JSON output formatting port of `packages/core/src/output/json-formatter.ts`.
//!
//! Statistics are accepted as a JSON value so the formatter preserves the
//! complete TypeScript `SessionMetrics` shape, including optional fields that
//! are not represented by the Rust usage-history summary type.

use regex::Regex;
use serde::Serialize;
use serde::ser::Serializer;
use serde_json::Value;
use std::sync::OnceLock;

/// A string or numeric error code, matching the TypeScript `string | number`.
#[derive(Clone, Debug, PartialEq)]
pub enum JsonErrorCode {
    String(String),
    Number(f64),
}

impl JsonErrorCode {
    fn is_js_truthy(&self) -> bool {
        match self {
            Self::String(value) => !value.is_empty(),
            Self::Number(value) => *value != 0.0 && !value.is_nan(),
        }
    }
}

impl Serialize for JsonErrorCode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::String(value) => serializer.serialize_str(value),
            Self::Number(value) if !value.is_finite() => serializer.serialize_none(),
            Self::Number(value) if *value == 0.0 => serializer.serialize_i64(0),
            // JavaScript's JSON.stringify writes integral numbers below 1e21
            // without a decimal or exponent. Keeping that behavior also
            // normalizes integral f64 error codes such as 500.0 to `500`.
            Self::Number(value) if value.fract() == 0.0 && value.abs() < 1e21 => {
                if *value < 0.0 {
                    serializer.serialize_i128(*value as i128)
                } else {
                    serializer.serialize_u128(*value as u128)
                }
            }
            Self::Number(value) => serializer.serialize_f64(*value),
        }
    }
}

/// Error object included by [`JsonFormatter::format`].
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct JsonError {
    /// JavaScript `Error.constructor.name`, or the equivalent custom name.
    #[serde(rename = "type")]
    pub error_type: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<JsonErrorCode>,
}

impl JsonError {
    pub fn new(
        error_type: impl Into<String>,
        message: impl Into<String>,
        code: Option<JsonErrorCode>,
    ) -> Self {
        Self {
            error_type: error_type.into(),
            message: message.into(),
            code,
        }
    }
}

/// Accepted input modes from `packages/core/src/output/types.ts`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum InputFormat {
    Text,
    StreamJson,
}

/// Supported output modes from `packages/core/src/output/types.ts`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum OutputFormat {
    Text,
    Json,
    StreamJson,
}

/// Optional fields returned by the JSON formatter.
#[derive(Serialize)]
pub struct JsonOutput<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stats: Option<&'a Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<&'a JsonError>,
}

/// Formats response, usage statistics, and error data in the TypeScript
/// formatter's field order and two-space pretty-printed JSON layout.
#[derive(Clone, Copy, Debug, Default)]
pub struct JsonFormatter;

impl JsonFormatter {
    /// Format whichever values are present. `stats` is a JSON value so callers
    /// can supply the full UI telemetry object without losing optional fields.
    pub fn format(
        &self,
        response: Option<&str>,
        stats: Option<&Value>,
        error: Option<&JsonError>,
    ) -> String {
        let output = JsonOutput {
            response: response.map(strip_ansi),
            // JavaScript's `if (stats)` omits null, false, zero, and empty
            // strings. Arrays and objects remain truthy, including empty ones.
            stats: stats.filter(|stats| js_value_is_truthy(stats)),
            error,
        };
        serde_json::to_string_pretty(&output)
            .expect("JSON formatter only serializes JSON-compatible values")
    }

    /// Format an error like TypeScript `formatError`.
    ///
    /// The source uses `...(code && { code })`, so an empty string, positive
    /// or negative zero, and NaN are omitted here. A falsey code passed
    /// directly inside [`JsonError`] to [`Self::format`] is retained, matching
    /// the source method's separate behavior.
    pub fn format_error(
        &self,
        error_type: &str,
        message: &str,
        code: Option<JsonErrorCode>,
    ) -> String {
        let error = JsonError {
            error_type: error_type.to_owned(),
            message: strip_ansi(message),
            code: code.filter(JsonErrorCode::is_js_truthy),
        };
        self.format(None, None, Some(&error))
    }
}

fn js_value_is_truthy(value: &Value) -> bool {
    match value {
        Value::Null | Value::Bool(false) => false,
        Value::Number(number) => number.as_f64().is_none_or(|value| value != 0.0),
        Value::String(value) => !value.is_empty(),
        // JavaScript arrays and objects are truthy even when empty.
        Value::Array(_) | Value::Object(_) | Value::Bool(true) => true,
    }
}

/// Strip the ANSI CSI and OSC sequences handled by `strip-ansi` 7.1.
pub fn strip_ansi(input: &str) -> String {
    static ANSI: OnceLock<Regex> = OnceLock::new();
    ANSI.get_or_init(|| {
        Regex::new(
            r"(?:\x1B\][\s\S]*?(?:\x07|\x1B\\|\u{009C})|[\x1B\u{009B}](?:\[|\]|\(|\)|#|;|\?)*(?:\d{1,4}(?:[;:]\d{0,4})*)?[\dA-PR-TZcf-nq-uy=><~])",
        )
        .expect("ANSI escape regex is valid")
    })
    .replace_all(input, "")
    .into_owned()
}

#[cfg(test)]
mod tests {
    use super::{InputFormat, JsonError, JsonErrorCode, JsonFormatter, OutputFormat, strip_ansi};
    use serde_json::{Value, json};

    #[test]
    fn formats_present_fields_as_pretty_json_in_source_order() {
        let formatter = JsonFormatter;
        assert_eq!(
            formatter.format(Some("hello"), None, None),
            "{\n  \"response\": \"hello\"\n}"
        );
        assert_eq!(formatter.format(None, None, None), "{}");

        let stats = json!({"models": {}, "skills": {"totalCalls": 1}});
        let error = JsonError::new(
            "TimeoutError",
            "timed out",
            Some(JsonErrorCode::String("TIMEOUT".to_owned())),
        );
        let parsed: Value =
            serde_json::from_str(&formatter.format(Some("partial"), Some(&stats), Some(&error)))
                .unwrap();
        assert_eq!(
            parsed,
            json!({
                "response": "partial",
                "stats": stats,
                "error": {"type": "TimeoutError", "message": "timed out", "code": "TIMEOUT"}
            })
        );
    }

    #[test]
    fn input_and_output_format_names_match_the_typescript_enums() {
        assert_eq!(
            serde_json::to_string(&InputFormat::StreamJson).unwrap(),
            "\"stream-json\""
        );
        assert_eq!(
            serde_json::to_string(&OutputFormat::Json).unwrap(),
            "\"json\""
        );
        assert_eq!(
            serde_json::to_string(&OutputFormat::StreamJson).unwrap(),
            "\"stream-json\""
        );
    }

    #[test]
    fn strips_csi_and_osc_sequences_including_c1_forms() {
        let formatted = JsonFormatter.format(
            Some("\x1b[31mred\x1b[0m \x1b]0;title\x07green \x1b]8;;url\x1b\\link\x1b]8;;\x1b\\ \u{009b}1;2Hblue"),
            None,
            None,
        );
        let parsed: Value = serde_json::from_str(&formatted).unwrap();
        assert_eq!(parsed["response"], "red green link blue");
    }

    #[test]
    fn preserves_non_ansi_controls_and_matches_incomplete_ansi_stripping() {
        let input = "bell\x07 backspace\x08 vertical\x0b\n\t \x1b[31";
        // strip-ansi 7.1 removes an incomplete CSI tail while preserving the
        // unrelated C0 controls and whitespace.
        let expected = "bell\x07 backspace\x08 vertical\x0b\n\t ";
        assert_eq!(strip_ansi(input), expected);

        let formatter = JsonFormatter;
        let parsed: Value =
            serde_json::from_str(&formatter.format(Some(input), None, None)).unwrap();
        assert_eq!(parsed["response"], expected);
    }

    #[test]
    fn follows_javascript_truthiness_for_optional_stats() {
        let formatter = JsonFormatter;
        for stats in [Value::Null, Value::Bool(false), json!(0), json!("")] {
            assert_eq!(formatter.format(None, Some(&stats), None), "{}");
        }
        assert_eq!(
            formatter.format(None, Some(&json!({})), None),
            "{\n  \"stats\": {}\n}"
        );
        assert_eq!(
            formatter.format(None, Some(&json!([])), None),
            "{\n  \"stats\": []\n}"
        );
    }

    #[test]
    fn format_error_strips_ansi_and_omits_falsey_codes() {
        let formatter = JsonFormatter;
        for code in [
            JsonErrorCode::String(String::new()),
            JsonErrorCode::Number(0.0),
            JsonErrorCode::Number(-0.0),
            JsonErrorCode::Number(f64::NAN),
        ] {
            let parsed: Value = serde_json::from_str(&formatter.format_error(
                "CustomError",
                "\x1b[31mfailed\x1b[0m",
                Some(code),
            ))
            .unwrap();
            assert_eq!(
                parsed,
                json!({"error": {"type": "CustomError", "message": "failed"}})
            );
        }

        assert_eq!(
            formatter.format_error("Error", "failed", Some(JsonErrorCode::Number(500.0))),
            "{\n  \"error\": {\n    \"type\": \"Error\",\n    \"message\": \"failed\",\n    \"code\": 500\n  }\n}"
        );
    }

    #[test]
    fn format_retains_a_falsey_code_supplied_in_an_error_object() {
        let formatter = JsonFormatter;
        for (code, expected) in [
            (JsonErrorCode::String(String::new()), json!("")),
            (JsonErrorCode::Number(0.0), json!(0)),
        ] {
            let error = JsonError::new("Error", "failed", Some(code));
            let parsed: Value =
                serde_json::from_str(&formatter.format(None, None, Some(&error))).unwrap();
            assert_eq!(parsed["error"]["code"], expected);
        }
    }

    #[test]
    fn format_does_not_rewrite_error_messages_passed_directly() {
        let formatter = JsonFormatter;
        let error = JsonError::new("Error", "\x1b[31mraw\x1b[0m", None);
        let parsed: Value =
            serde_json::from_str(&formatter.format(None, None, Some(&error))).unwrap();
        assert_eq!(parsed["error"]["message"], "\x1b[31mraw\x1b[0m");
    }
}
