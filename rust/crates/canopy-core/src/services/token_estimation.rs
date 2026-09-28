//! Character-based token estimates used by the prompt auto-compaction gate.
//!
//! Port of `packages/core/src/services/tokenEstimation.ts`. This intentionally
//! reuses the content character estimator from `compaction_input_slimming` so
//! text, inline media, function calls, and nested tool responses use the same
//! size model across both code paths.

use serde_json::Value;

use super::compaction_input_slimming::{
    DEFAULT_IMAGE_TOKEN_ESTIMATE, TOKEN_TO_CHAR_RATIO, estimate_content_chars,
};

/// Character divisor shared with the compaction input size estimator.
pub const CHARS_PER_TOKEN: f64 = TOKEN_TO_CHAR_RATIO;

/// Extra multiplier for newly added content when a prompt estimate must err
/// on the high side. The API-reported running prompt total is not scaled.
pub const CONSERVATIVE_NEW_CONTENT_SAFETY_FACTOR: f64 = 1.5;

/// Options corresponding to the optional arguments on the TypeScript helper.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PromptTokenEstimateOptions {
    pub last_output_token_count: f64,
    pub image_token_estimate: f64,
    pub conservative: bool,
}

impl Default for PromptTokenEstimateOptions {
    fn default() -> Self {
        Self {
            last_output_token_count: 0.0,
            image_token_estimate: DEFAULT_IMAGE_TOKEN_ESTIMATE,
            conservative: false,
        }
    }
}

/// Estimate a collection of Gemini-shaped contents as `ceil(chars / 4)`.
pub fn estimate_content_tokens(contents: &[Value]) -> f64 {
    estimate_content_tokens_with_image_estimate(contents, DEFAULT_IMAGE_TOKEN_ESTIMATE)
}

/// Estimate contents while using a caller-provided token budget per image.
pub fn estimate_content_tokens_with_image_estimate(
    contents: &[Value],
    image_token_estimate: f64,
) -> f64 {
    ceil_tokens(
        contents
            .iter()
            .map(|content| estimate_content_chars(content, image_token_estimate))
            .sum(),
    )
}

/// Estimate a prompt using defaults for output tokens, image budgeting, and
/// conservative scaling.
pub fn estimate_prompt_tokens(
    history: &[Value],
    user_message: &Value,
    last_prompt_token_count: f64,
) -> f64 {
    estimate_prompt_tokens_with_options(
        history,
        user_message,
        last_prompt_token_count,
        PromptTokenEstimateOptions::default(),
    )
}

/// Compute the effective prompt-token count used by the auto-compaction gate.
///
/// When an API prompt count is available, only the current user message is
/// estimated and added to it, along with the previous model output count. In
/// conservative mode only that newly estimated portion is multiplied by 1.5.
/// Before any API count is available, the complete local history and current
/// message are estimated together, preserving a single final ceiling.
pub fn estimate_prompt_tokens_with_options(
    history: &[Value],
    user_message: &Value,
    last_prompt_token_count: f64,
    options: PromptTokenEstimateOptions,
) -> f64 {
    if last_prompt_token_count > 0.0 {
        let new_content_tokens = estimate_content_tokens_with_image_estimate(
            std::slice::from_ref(user_message),
            options.image_token_estimate,
        );
        let incremental_tokens = if options.conservative {
            (new_content_tokens * CONSERVATIVE_NEW_CONTENT_SAFETY_FACTOR).ceil()
        } else {
            new_content_tokens
        };
        return last_prompt_token_count + options.last_output_token_count + incremental_tokens;
    }

    let total_chars = history
        .iter()
        .chain(std::iter::once(user_message))
        .map(|content| estimate_content_chars(content, options.image_token_estimate))
        .sum();
    ceil_tokens(total_chars)
}

/// Return output-token usage suitable for advancing a prompt estimate.
///
/// The JSON value uses Gemini usage-metadata field names. A missing
/// `promptTokenCount` means the usage object cannot anchor a prompt estimate.
/// When `totalTokenCount` is missing, candidate and thoughts counts follow the
/// source overlap rule: thoughts are treated as overlapping only when the
/// candidate count strictly exceeds the thoughts count.
pub fn get_usage_output_token_count_for_prompt_estimate(usage: Option<&Value>) -> f64 {
    let Some(usage) = usage else {
        return 0.0;
    };
    let Some(object) = usage.as_object() else {
        return 0.0;
    };
    let Some(prompt_value) = object.get("promptTokenCount") else {
        return 0.0;
    };
    let prompt_tokens = js_number(prompt_value);

    if let Some(total_value) = object.get("totalTokenCount") {
        return js_max_zero(js_number(total_value) - prompt_tokens);
    }

    let candidates = js_max_zero(
        object
            .get("candidatesTokenCount")
            .map(js_number)
            .unwrap_or(0.0),
    );
    let thoughts = js_max_zero(
        object
            .get("thoughtsTokenCount")
            .map(js_number)
            .unwrap_or(0.0),
    );

    if candidates > thoughts {
        candidates
    } else {
        candidates + thoughts
    }
}

fn ceil_tokens(total_chars: f64) -> f64 {
    (total_chars / CHARS_PER_TOKEN).ceil()
}

/// JSON's number coercion for usage fields, which are numeric in the declared
/// TypeScript API but can still arrive as null or other JSON values at runtime.
fn js_number(value: &Value) -> f64 {
    match value {
        Value::Null => 0.0,
        Value::Bool(value) => f64::from(u8::from(*value)),
        Value::Number(value) => value.as_f64().unwrap_or(f64::NAN),
        Value::String(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                0.0
            } else {
                trimmed.parse::<f64>().unwrap_or(f64::NAN)
            }
        }
        Value::Array(_) | Value::Object(_) => f64::NAN,
    }
}

/// Match JavaScript `Math.max(0, value)`, including its NaN propagation.
fn js_max_zero(value: f64) -> f64 {
    if value.is_nan() {
        f64::NAN
    } else {
        value.max(0.0)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::{
        CHARS_PER_TOKEN, CONSERVATIVE_NEW_CONTENT_SAFETY_FACTOR, PromptTokenEstimateOptions,
        estimate_content_tokens, estimate_content_tokens_with_image_estimate,
        estimate_prompt_tokens, estimate_prompt_tokens_with_options,
        get_usage_output_token_count_for_prompt_estimate,
    };

    fn text_content(text: &str) -> Value {
        json!({"role":"user", "parts":[{"text":text}]})
    }

    #[test]
    fn empty_contents_estimate_to_zero() {
        assert_eq!(estimate_content_tokens(&[]), 0.0);
    }

    #[test]
    fn estimates_plain_text_by_character_count_divided_by_four() {
        assert_eq!(CHARS_PER_TOKEN, 4.0);
        assert_eq!(estimate_content_tokens(&[text_content("hello world")]), 3.0);
    }

    #[test]
    fn sums_characters_across_messages_before_rounding() {
        assert_eq!(
            estimate_content_tokens(&[text_content("aaaa"), text_content("bbbbbbbb")]),
            3.0
        );
        // The shared estimator uses JavaScript string.length semantics, so a
        // supplementary Unicode character contributes two UTF-16 code units.
        assert_eq!(estimate_content_tokens(&[text_content("😀")]), 1.0);
    }

    #[test]
    fn estimates_inline_data_from_the_configured_image_budget() {
        let image = json!({
            "role":"user",
            "parts":[{"inlineData":{"mimeType":"image/png", "data":"xxx"}}]
        });
        assert_eq!(
            estimate_content_tokens_with_image_estimate(&[image], 1_600.0),
            1_600.0
        );
    }

    #[test]
    fn estimates_function_calls_and_nested_function_responses() {
        let function_call = json!({
            "role":"model",
            "parts":[{"functionCall":{"name":"foo", "args":{"a":1,"b":2}}}]
        });
        let function_response = json!({
            "role":"user",
            "parts":[{"functionResponse":{"name":"tool", "response":{"result":"data".repeat(100)}}}]
        });
        assert!(estimate_content_tokens(&[function_call]) > 0.0);
        assert!(estimate_content_tokens(&[function_response]) > 0.0);
    }

    #[test]
    fn adds_the_current_message_and_previous_output_to_api_prompt_count() {
        let history = vec![
            text_content("older message a"),
            text_content("older message b"),
        ];
        let user = text_content("current user message");
        let user_estimate = estimate_content_tokens(std::slice::from_ref(&user));
        assert_eq!(
            estimate_prompt_tokens(&history, &user, 5_000.0),
            5_000.0 + user_estimate
        );
        assert_eq!(
            estimate_prompt_tokens_with_options(
                &history,
                &user,
                5_000.0,
                PromptTokenEstimateOptions {
                    last_output_token_count: 1_200.0,
                    ..PromptTokenEstimateOptions::default()
                }
            ),
            5_000.0 + 1_200.0 + user_estimate
        );
    }

    #[test]
    fn supports_custom_image_budget_for_prompt_increment() {
        let history = vec![text_content("history")];
        let image_user = json!({
            "role":"user",
            "parts":[{"inlineData":{"mimeType":"image/png", "data":"xxx"}}]
        });
        assert_eq!(
            estimate_prompt_tokens_with_options(
                &history,
                &image_user,
                5_000.0,
                PromptTokenEstimateOptions {
                    last_output_token_count: 1_200.0,
                    image_token_estimate: 1_600.0,
                    conservative: false,
                }
            ),
            7_800.0
        );
    }

    #[test]
    fn falls_back_to_estimation_of_history_and_current_message() {
        let history = vec![
            text_content("older message a"),
            text_content("older message b"),
        ];
        let user = text_content("current user message");
        assert_eq!(
            estimate_prompt_tokens(&history, &user, 0.0),
            estimate_content_tokens(&[history[0].clone(), history[1].clone(), user,])
        );
        // Output tokens cannot become an API anchor on the first-send path.
        let options = PromptTokenEstimateOptions {
            last_output_token_count: 9_999.0,
            ..PromptTokenEstimateOptions::default()
        };
        assert_eq!(
            estimate_prompt_tokens_with_options(
                &history,
                &text_content("current user message"),
                0.0,
                options
            ),
            estimate_prompt_tokens(&history, &text_content("current user message"), 0.0)
        );
    }

    #[test]
    fn conservative_mode_scales_only_new_content_and_rounds_up() {
        let history = vec![text_content("irrelevant history")];
        let user = text_content("current user message");
        let user_estimate = estimate_content_tokens(std::slice::from_ref(&user));
        let conservative = estimate_prompt_tokens_with_options(
            &history,
            &user,
            5_000.0,
            PromptTokenEstimateOptions {
                last_output_token_count: 1_200.0,
                conservative: true,
                ..PromptTokenEstimateOptions::default()
            },
        );
        assert_eq!(
            conservative,
            5_000.0 + 1_200.0 + (user_estimate * 1.5).ceil()
        );
        assert_eq!(CONSERVATIVE_NEW_CONTENT_SAFETY_FACTOR, 1.5);
    }

    #[test]
    fn conservative_mode_catches_cjk_dense_new_tool_content() {
        let cjk_tool_result = json!({
            "role":"user",
            "parts":[{"functionResponse":{"name":"read_file", "response":{"output":"设计文档章节内容。".repeat(2_000)}}}]
        });
        let last_prompt = 25_000.0;
        let ordinary = estimate_prompt_tokens(&[], &cjk_tool_result, last_prompt);
        let safe = estimate_prompt_tokens_with_options(
            &[],
            &cjk_tool_result,
            last_prompt,
            PromptTokenEstimateOptions {
                conservative: true,
                ..PromptTokenEstimateOptions::default()
            },
        );
        assert!(safe > ordinary);
        assert_eq!(
            safe,
            last_prompt
                + (estimate_content_tokens(std::slice::from_ref(&cjk_tool_result))
                    * CONSERVATIVE_NEW_CONTENT_SAFETY_FACTOR)
                    .ceil()
        );
    }

    #[test]
    fn usage_total_count_resolves_candidate_thought_overlap() {
        assert_eq!(
            get_usage_output_token_count_for_prompt_estimate(Some(&json!({
                "promptTokenCount":100,
                "totalTokenCount":180,
                "candidatesTokenCount":70,
                "thoughtsTokenCount":50
            }))),
            80.0
        );
    }

    #[test]
    fn usage_omits_thoughts_only_when_candidates_strictly_dominate() {
        assert_eq!(
            get_usage_output_token_count_for_prompt_estimate(Some(&json!({
                "promptTokenCount":100,
                "candidatesTokenCount":150,
                "thoughtsTokenCount":120
            }))),
            150.0
        );
        assert_eq!(
            get_usage_output_token_count_for_prompt_estimate(Some(&json!({
                "promptTokenCount":100,
                "candidatesTokenCount":50,
                "thoughtsTokenCount":120
            }))),
            170.0
        );
        // Equal counts do not establish overlap.
        assert_eq!(
            get_usage_output_token_count_for_prompt_estimate(Some(&json!({
                "promptTokenCount":100,
                "candidatesTokenCount":80,
                "thoughtsTokenCount":80
            }))),
            160.0
        );
    }

    #[test]
    fn usage_counts_clamp_negative_values_and_missing_prompt_anchor() {
        assert_eq!(get_usage_output_token_count_for_prompt_estimate(None), 0.0);
        assert_eq!(
            get_usage_output_token_count_for_prompt_estimate(Some(&json!({
                "candidatesTokenCount":99
            }))),
            0.0
        );
        assert_eq!(
            get_usage_output_token_count_for_prompt_estimate(Some(&json!({
                "promptTokenCount":100,
                "candidatesTokenCount":-10,
                "thoughtsTokenCount":-5
            }))),
            0.0
        );
        assert_eq!(
            get_usage_output_token_count_for_prompt_estimate(Some(&json!({
                "promptTokenCount":100,
                "totalTokenCount":80
            }))),
            0.0
        );
    }
}
