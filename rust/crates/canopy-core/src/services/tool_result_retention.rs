//! Aggregate size diagnostics for function responses retained in history.
//!
//! Port of `packages/core/src/utils/tool-result-retention.ts`. The report
//! contains counts and sizes only, so it is safe to attach to diagnostics.
//! Sizing delegates to `compaction_input_slimming::estimate_part_chars` so
//! nested media and the wrapper floor use the same accounting as compaction.

use serde::Serialize;
use serde_json::Value;

use super::compaction_input_slimming::{DEFAULT_IMAGE_TOKEN_ESTIMATE, estimate_part_chars};

/// Fallback for tool results whose producer does not declare an output budget.
/// Production callers should pass the configured global truncation threshold.
pub const OVERSIZED_TOOL_RESULT_THRESHOLD_CHARS: f64 = 30_000.0;

/// Stable truncation marker emitted by the shared tool-output truncator.
pub const TOOL_OUTPUT_TRUNCATED_PREFIX: &str = "Tool output was too large and has been truncated";

/// The combined scheduler pass tolerates retained content up to twice budget.
pub const COMBINED_PASS_TOLERANCE_FACTOR: f64 = 2.0;

/// Tolerance for sentinel-less originals returned when wrapping would not save
/// enough characters in the token-aware truncation fallback.
pub const TRUNCATION_FALLBACK_ENVELOPE_SLACK: f64 = 500.0;

/// Aggregate retained function-response sizes and oversized-result signals.
/// Field names serialize to the TypeScript diagnostics schema.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolResultRetentionStats {
    /// Number of function-response parts retained in history.
    pub tool_result_count: usize,
    /// Total estimated characters retained across all tool results.
    pub total_chars: f64,
    /// Estimated character size of the largest retained tool result.
    pub largest_result_chars: f64,
    /// Untruncated results measured above twice their producer's budget.
    pub oversized_result_count: usize,
    /// Fallback threshold; zero represents a non-finite threshold in JSON.
    pub oversized_threshold_chars: f64,
}

pub type ToolBudgetResolver<'a> = &'a dyn Fn(&str) -> Option<f64>;

/// Inputs corresponding to the optional TypeScript analyzer settings.
pub struct AnalyzeToolResultRetentionOptions<'a> {
    /// Fallback budget for tools that declare no `maxOutputChars`.
    pub threshold_chars: f64,
    /// Resolves a producing tool's output budget by function-response name.
    /// Returning `None` applies `threshold_chars`.
    pub resolve_tool_budget_chars: Option<ToolBudgetResolver<'a>>,
    /// Token estimate for nested inline media.
    pub image_token_estimate: f64,
}

impl<'a> Default for AnalyzeToolResultRetentionOptions<'a> {
    fn default() -> Self {
        Self {
            threshold_chars: OVERSIZED_TOOL_RESULT_THRESHOLD_CHARS,
            resolve_tool_budget_chars: None,
            image_token_estimate: DEFAULT_IMAGE_TOKEN_ESTIMATE,
        }
    }
}

/// Compute retained-result counts, size totals, largest result, and oversized
/// untruncated result count for Gemini-shaped conversation contents.
pub fn analyze_tool_result_retention(
    history: &[Value],
    options: AnalyzeToolResultRetentionOptions<'_>,
) -> ToolResultRetentionStats {
    let mut stats = ToolResultRetentionStats {
        oversized_threshold_chars: if options.threshold_chars.is_finite() {
            options.threshold_chars
        } else {
            0.0
        },
        ..ToolResultRetentionStats::default()
    };

    for content in history {
        let Some(parts) = content.get("parts").and_then(Value::as_array) else {
            continue;
        };
        for part in parts {
            let Some(function_response) = part.get("functionResponse") else {
                continue;
            };
            if !js_truthy(function_response) {
                continue;
            }

            let chars = estimate_part_chars(part, options.image_token_estimate);
            stats.tool_result_count += 1;
            stats.total_chars += chars;
            stats.largest_result_chars = stats.largest_result_chars.max(chars);

            let tool_name = function_response
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let budget = options
                .resolve_tool_budget_chars
                .and_then(|resolve| resolve(tool_name))
                .unwrap_or(options.threshold_chars);

            let response = function_response.get("response");
            let output = response
                .and_then(|response| response.get("output"))
                .filter(|output| !output.is_null())
                .or_else(|| {
                    response
                        .and_then(|response| response.get("error"))
                        .filter(|error| !error.is_null())
                });

            let output_text = output.and_then(Value::as_str);
            let already_truncated = output_text.is_some_and(|text| {
                text.starts_with(TOOL_OUTPUT_TRUNCATED_PREFIX)
                    || text.starts_with("<persisted-output>")
            });
            // The scheduler bounds raw JS string length, not the wrapper-aware
            // part estimate used in aggregate reporting.
            let raw_chars = output_text.map(js_string_len).unwrap_or_default() as f64;
            if !already_truncated
                && budget.is_finite()
                && raw_chars
                    > budget * COMBINED_PASS_TOLERANCE_FACTOR + TRUNCATION_FALLBACK_ENVELOPE_SLACK
            {
                stats.oversized_result_count += 1;
            }
        }
    }

    stats
}

fn js_string_len(text: &str) -> usize {
    text.encode_utf16().count()
}

fn js_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_f64().is_some_and(|number| number != 0.0),
        Value::String(value) => !value.is_empty(),
        // JavaScript arrays and objects are truthy, including empty ones.
        Value::Array(_) | Value::Object(_) => true,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::{
        AnalyzeToolResultRetentionOptions, DEFAULT_IMAGE_TOKEN_ESTIMATE,
        OVERSIZED_TOOL_RESULT_THRESHOLD_CHARS, TOOL_OUTPUT_TRUNCATED_PREFIX,
        analyze_tool_result_retention,
    };
    use crate::services::compaction_input_slimming::TOKEN_TO_CHAR_RATIO;

    const WRAPPER_FLOOR_CHARS: f64 = 64.0;

    fn tool_result(output: &str, name: &str) -> Value {
        json!({
            "role": "user",
            "parts": [{
                "functionResponse": { "name": name, "response": { "output": output } }
            }]
        })
    }

    #[test]
    fn empty_history_returns_the_serializable_zero_schema() {
        let stats = analyze_tool_result_retention(&[], Default::default());
        assert_eq!(stats.tool_result_count, 0);
        assert_eq!(stats.total_chars, 0.0);
        assert_eq!(stats.largest_result_chars, 0.0);
        assert_eq!(stats.oversized_result_count, 0);
        assert_eq!(
            stats.oversized_threshold_chars,
            OVERSIZED_TOOL_RESULT_THRESHOLD_CHARS
        );
        assert_eq!(
            serde_json::to_value(stats).unwrap(),
            json!({
                "toolResultCount": 0,
                "totalChars": 0.0,
                "largestResultChars": 0.0,
                "oversizedResultCount": 0,
                "oversizedThresholdChars": 30_000.0
            })
        );
    }

    #[test]
    fn ignores_non_tool_parts_and_missing_parts() {
        let history = vec![
            json!({ "role": "model", "parts": [{ "text": "hello" }] }),
            json!({ "role": "user" }),
            json!({ "role": "user", "parts": [{ "functionResponse": null }] }),
        ];
        let stats = analyze_tool_result_retention(&history, Default::default());
        assert_eq!(stats.tool_result_count, 0);
        assert_eq!(stats.total_chars, 0.0);
    }

    #[test]
    fn aggregates_sizes_using_the_shared_part_estimator() {
        let history = vec![
            tool_result(&"x".repeat(100), "shell"),
            tool_result(&"y".repeat(500), "shell"),
        ];
        let stats = analyze_tool_result_retention(&history, Default::default());
        assert_eq!(stats.tool_result_count, 2);
        assert_eq!(stats.total_chars, 600.0 + 2.0 * WRAPPER_FLOOR_CHARS);
        assert_eq!(stats.largest_result_chars, 500.0 + WRAPPER_FLOOR_CHARS);
        assert_eq!(stats.oversized_result_count, 0);
    }

    #[test]
    fn newline_dense_output_is_measured_by_raw_string_length() {
        let dense = "line\n".repeat(5_980);
        let stats =
            analyze_tool_result_retention(&[tool_result(&dense, "shell")], Default::default());

        assert_eq!(stats.largest_result_chars, 29_900.0 + WRAPPER_FLOOR_CHARS);
        assert_eq!(stats.oversized_result_count, 0);
    }

    #[test]
    fn raw_output_comparison_uses_utf16_length_and_strict_threshold() {
        let exact = "z".repeat(60_000);
        let mut history = vec![tool_result(&exact, "shell")];
        let stats = analyze_tool_result_retention(&history, Default::default());
        assert_eq!(stats.oversized_result_count, 0);

        // Each emoji has two UTF-16 code units, matching JavaScript `.length`.
        let astral = "😀".repeat(30_501);
        history[0] = tool_result(&astral, "shell");
        let stats = analyze_tool_result_retention(&history, Default::default());
        assert_eq!(stats.oversized_result_count, 1);
        assert_eq!(stats.largest_result_chars, 61_002.0 + WRAPPER_FLOOR_CHARS);
    }

    #[test]
    fn honors_custom_thresholds_and_per_tool_budget_fallback() {
        let history = vec![
            tool_result(&"m".repeat(60_000), "big"),
            tool_result(&"s".repeat(801), "small"),
            tool_result(&"u".repeat(500), "unknown"),
        ];
        let budgets = |name: &str| match name {
            "big" => Some(500_000.0),
            "small" => Some(100.0),
            _ => None,
        };
        let stats = analyze_tool_result_retention(
            &history,
            AnalyzeToolResultRetentionOptions {
                resolve_tool_budget_chars: Some(&budgets),
                ..Default::default()
            },
        );
        assert_eq!(stats.oversized_result_count, 1);

        let custom = analyze_tool_result_retention(
            &[tool_result(&"z".repeat(511), "shell")],
            AnalyzeToolResultRetentionOptions {
                threshold_chars: 5.0,
                ..Default::default()
            },
        );
        assert_eq!(custom.oversized_threshold_chars, 5.0);
        assert_eq!(custom.oversized_result_count, 1);
    }

    #[test]
    fn skips_both_truncation_sentinels_but_counts_the_retained_content() {
        let spilled = format!("{TOOL_OUTPUT_TRUNCATED_PREFIX}.\n{}", "x".repeat(100_000));
        let persisted = format!(
            "<persisted-output>job-123</persisted-output>{}",
            "x".repeat(100_000)
        );
        let history = vec![
            tool_result(&spilled, "shell"),
            json!({
                "role": "user",
                "parts": [{ "functionResponse": { "name": "shell", "response": { "error": persisted } } }]
            }),
        ];
        let stats = analyze_tool_result_retention(&history, Default::default());
        assert_eq!(stats.oversized_result_count, 0);
        assert_eq!(stats.tool_result_count, 2);
        assert!(stats.total_chars > 200_000.0);
    }

    #[test]
    fn honors_infinite_tool_budget_and_sanitizes_infinite_fallback_for_json() {
        let huge = tool_result(&"r".repeat(100_000), "unbounded");
        let infinite = |_: &str| Some(f64::INFINITY);
        let stats = analyze_tool_result_retention(
            std::slice::from_ref(&huge),
            AnalyzeToolResultRetentionOptions {
                resolve_tool_budget_chars: Some(&infinite),
                ..Default::default()
            },
        );
        assert_eq!(stats.oversized_result_count, 0);

        let stats = analyze_tool_result_retention(
            &[tool_result("small", "shell")],
            AnalyzeToolResultRetentionOptions {
                threshold_chars: f64::INFINITY,
                ..Default::default()
            },
        );
        assert_eq!(stats.oversized_threshold_chars, 0.0);
        assert_eq!(
            serde_json::to_value(stats).unwrap()["oversizedThresholdChars"],
            json!(0.0)
        );
    }

    #[test]
    fn handles_missing_payload_and_bills_nested_media_by_image_estimate() {
        let missing = json!({
            "role": "user",
            "parts": [{ "functionResponse": { "name": "shell" } }]
        });
        let media = json!({
            "role": "user",
            "parts": [{
                "functionResponse": {
                    "name": "read_file",
                    "response": { "output": "img" },
                    "parts": [{ "inlineData": {} }]
                }
            }]
        });
        let stats = analyze_tool_result_retention(&[missing, media.clone()], Default::default());
        let image_chars = DEFAULT_IMAGE_TOKEN_ESTIMATE * TOKEN_TO_CHAR_RATIO;
        assert_eq!(stats.tool_result_count, 2);
        assert_eq!(
            stats.total_chars,
            WRAPPER_FLOOR_CHARS + 3.0 + image_chars + WRAPPER_FLOOR_CHARS
        );

        let custom = analyze_tool_result_retention(
            &[media],
            AnalyzeToolResultRetentionOptions {
                image_token_estimate: 800.0,
                ..Default::default()
            },
        );
        assert_eq!(
            custom.total_chars,
            3.0 + 800.0 * TOKEN_TO_CHAR_RATIO + WRAPPER_FLOOR_CHARS
        );
    }

    #[test]
    fn uses_output_before_error_and_counts_multiple_function_responses() {
        let history = vec![json!({
            "role": "user",
            "parts": [
                { "functionResponse": { "name": "shell", "response": { "output": "x".repeat(100), "error": "ignored" } } },
                { "functionResponse": { "name": "shell", "response": { "output": "y".repeat(200) } } }
            ]
        })];
        let stats = analyze_tool_result_retention(&history, Default::default());
        assert_eq!(stats.tool_result_count, 2);
        assert_eq!(stats.total_chars, 300.0 + 2.0 * WRAPPER_FLOOR_CHARS);
        assert_eq!(stats.largest_result_chars, 200.0 + WRAPPER_FLOOR_CHARS);
    }

    #[test]
    fn oversized_error_fallback_uses_nullish_output_precedence() {
        let error = "e".repeat(60_501);
        let history = vec![
            json!({
                "parts": [{
                    "functionResponse": {
                        "name": "shell",
                        "response": { "output": null, "error": error }
                    }
                }]
            }),
            json!({
                "parts": [{
                    "functionResponse": {
                        "name": "shell",
                        "response": { "output": "", "error": "e".repeat(60_501) }
                    }
                }]
            }),
            json!({
                "parts": [{
                    "functionResponse": {
                        "name": "shell",
                        "response": { "output": 0, "error": "e".repeat(60_501) }
                    }
                }]
            }),
        ];

        let stats = analyze_tool_result_retention(&history, Default::default());

        // Null falls through to `error`; empty string and numeric zero are
        // non-null values and therefore shadow `error` for the raw-size test.
        assert_eq!(stats.tool_result_count, 3);
        assert_eq!(stats.oversized_result_count, 1);
        assert_eq!(
            stats.total_chars,
            2.0 * (60_501.0 + WRAPPER_FLOOR_CHARS) + WRAPPER_FLOOR_CHARS
        );
        assert_eq!(stats.largest_result_chars, 60_501.0 + WRAPPER_FLOOR_CHARS);
    }
}
