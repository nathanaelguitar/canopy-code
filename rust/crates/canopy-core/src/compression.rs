use serde_json::{Map, Value, json};

use crate::services::compaction_input_slimming::estimate_content_chars;

pub const COMPACT_MAX_OUTPUT_TOKENS: f64 = 20_000.0;
pub const DEFAULT_PCT: f64 = 0.85;
pub const SUMMARY_RESERVE: f64 = COMPACT_MAX_OUTPUT_TOKENS;
pub const COMPRESSION_CONTEXT_MARGIN_TOKENS: f64 = 1.0;
pub const COMPRESSION_INPUT_SAFETY_MARGIN_TOKENS: f64 = 2_048.0;
pub const COMPRESSION_EDGE_OUTPUT_TOKENS: f64 = 2_048.0;
pub const COMPRESSION_LARGE_CONTEXT_HISTORY_MARGIN_TOKENS: f64 = 131_072.0;
pub const COMPRESSION_LARGE_CONTEXT_HISTORY_TOKEN_BUDGET: f64 = 64_000.0;
pub const AUTOCOMPACT_BUFFER: f64 = 13_000.0;
pub const WARN_BUFFER: f64 = 20_000.0;
pub const HARD_BUFFER: f64 = 3_000.0;
pub const MAX_CONSECUTIVE_FAILURES: u32 = 3;
pub const CHARS_PER_TOKEN: f64 = 4.0;
pub const DEFAULT_IMAGE_TOKEN_ESTIMATE: f64 = 1_600.0;

const COMPRESSION_REQUEST_DIRECTIVE: &str =
    "First, reason in your <analysis> block. Then, produce the <state_snapshot> XML.";
const COMPRESSION_HISTORY_TRUNCATION_MARKER: &str = "[Earlier conversation omitted from this emergency compression pass because the context window was full.]";

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CompactionThresholds {
    pub warn: f64,
    pub auto: f64,
    pub hard: f64,
    pub effective_window: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FittedCompressionHistory {
    pub history: Vec<Value>,
    pub estimated_input_tokens: f64,
    pub omitted_token_estimate: f64,
    pub was_trimmed: bool,
}

pub fn compute_thresholds(window: f64, pct: Option<f64>) -> CompactionThresholds {
    let requested_pct = pct.filter(|value| value.is_finite()).unwrap_or(DEFAULT_PCT);
    let effective_pct = requested_pct.clamp(0.0, 1.0);
    let effective_window = (window - SUMMARY_RESERVE).max(0.0);
    let proportional = effective_pct * window;
    let absolute_ceiling = effective_window - AUTOCOMPACT_BUFFER;
    let auto = if absolute_ceiling > 0.0 {
        proportional.min(absolute_ceiling)
    } else {
        proportional
    };
    let warn = (auto - WARN_BUFFER).max(0.0);
    let hard_edge = effective_window - HARD_BUFFER;
    let hard = window.min(hard_edge.max(auto + HARD_BUFFER));
    CompactionThresholds {
        warn,
        auto,
        hard,
        effective_window,
    }
}

pub fn compute_compression_output_tokens(context_window: f64, input_token_estimate: f64) -> f64 {
    let available = context_window - input_token_estimate.max(0.0).ceil();
    let safe_available = if available >= COMPACT_MAX_OUTPUT_TOKENS {
        available
    } else {
        available - COMPRESSION_INPUT_SAFETY_MARGIN_TOKENS
    };
    1.0_f64.max(COMPACT_MAX_OUTPUT_TOKENS.min(safe_available))
}

fn js_string_len(value: &str) -> usize {
    value.encode_utf16().count()
}

pub fn estimate_content_tokens(contents: &[Value], image_token_estimate: f64) -> f64 {
    let total_chars = contents
        .iter()
        .map(|content| estimate_content_chars(content, image_token_estimate))
        .sum::<f64>();
    (total_chars / CHARS_PER_TOKEN).ceil()
}

/// Fits the history sent to a compaction side-query before that request is
/// made. The stored transcript remains untouched; only the provider request
/// gets the bounded suffix and explicit omission marker.
pub fn fit_compression_history_to_context(
    history: Vec<Value>,
    context_window: f64,
    system_instruction: &str,
    image_token_estimate: f64,
    authoritative_input_token_estimate: Option<f64>,
    output_reserve_tokens: f64,
) -> FittedCompressionHistory {
    let directive = json!({
        "role":"user",
        "parts":[{"text":COMPRESSION_REQUEST_DIRECTIVE}]
    });
    let fixed_tokens = estimate_content_tokens(&[directive], image_token_estimate)
        + (js_string_len(system_instruction) as f64 / CHARS_PER_TOKEN).ceil();
    let marker_tokens =
        (js_string_len(COMPRESSION_HISTORY_TRUNCATION_MARKER) as f64 / CHARS_PER_TOKEN).ceil();
    let original_history_tokens = estimate_content_tokens(&history, image_token_estimate);
    let full_input_tokens = original_history_tokens + fixed_tokens;
    let unbuffered_input_budget =
        context_window - output_reserve_tokens - COMPRESSION_CONTEXT_MARGIN_TOKENS;
    let authoritative = authoritative_input_token_estimate.unwrap_or_default();
    let calibrated_full_input_tokens = full_input_tokens.max(authoritative);
    let is_large_context_emergency = context_window >= 250_000.0 && authoritative > context_window;
    let emergency_input_margin = if is_large_context_emergency {
        COMPRESSION_LARGE_CONTEXT_HISTORY_MARGIN_TOKENS
    } else {
        COMPRESSION_INPUT_SAFETY_MARGIN_TOKENS
    };
    let needs_emergency_trim = if is_large_context_emergency {
        calibrated_full_input_tokens > unbuffered_input_budget
    } else {
        calibrated_full_input_tokens > unbuffered_input_budget + emergency_input_margin
    };
    let full_input_budget = if needs_emergency_trim {
        unbuffered_input_budget - emergency_input_margin
    } else {
        unbuffered_input_budget
    };
    let calibration_ratio = if full_input_tokens > 0.0 {
        calibrated_full_input_tokens / full_input_tokens
    } else {
        1.0
    };
    let proportional_history_budget =
        (full_input_budget / calibration_ratio).floor() - fixed_tokens - marker_tokens;
    let calibrated_history_budget = if is_large_context_emergency {
        proportional_history_budget.clamp(0.0, COMPRESSION_LARGE_CONTEXT_HISTORY_TOKEN_BUDGET)
    } else {
        proportional_history_budget.max(0.0)
    };

    if !needs_emergency_trim {
        return FittedCompressionHistory {
            history,
            estimated_input_tokens: full_input_tokens,
            omitted_token_estimate: 0.0,
            was_trimmed: false,
        };
    }

    let content_costs = history
        .iter()
        .map(|content| estimate_content_tokens(std::slice::from_ref(content), image_token_estimate))
        .collect::<Vec<_>>();
    let mut prefix_costs = Vec::with_capacity(content_costs.len() + 1);
    prefix_costs.push(0.0);
    for cost in content_costs {
        prefix_costs.push(prefix_costs.last().copied().unwrap_or_default() + cost);
    }

    let mut start = history
        .iter()
        .position(|content| content.get("role").and_then(Value::as_str) == Some("user"))
        .unwrap_or_else(|| history.len().saturating_sub(1));
    // The loop range is fixed at the first user message; `start` tracks the
    // latest candidate independently, matching the source loop.
    #[allow(clippy::mut_range_bound)]
    for index in start..history.len() {
        if history[index].get("role").and_then(Value::as_str) != Some("user") {
            continue;
        }
        let suffix_tokens = original_history_tokens - prefix_costs[index];
        if suffix_tokens <= calibrated_history_budget {
            start = index;
            break;
        }
        start = index;
    }

    let mut fitted_history = history[start..].to_vec();
    if fitted_history
        .first()
        .and_then(|content| content.get("role"))
        .and_then(Value::as_str)
        == Some("user")
    {
        let mut first = fitted_history[0]
            .as_object()
            .cloned()
            .unwrap_or_else(Map::new);
        let old_parts = first
            .get("parts")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let mut parts = Vec::with_capacity(old_parts.len() + 1);
        parts.push(json!({"text":COMPRESSION_HISTORY_TRUNCATION_MARKER}));
        parts.extend(old_parts);
        first.insert("parts".to_owned(), Value::Array(parts));
        fitted_history[0] = Value::Object(first);
    }

    let fitted_history_tokens = estimate_content_tokens(&fitted_history, image_token_estimate);
    FittedCompressionHistory {
        history: fitted_history,
        estimated_input_tokens: fitted_history_tokens + fixed_tokens,
        omitted_token_estimate: (original_history_tokens - fitted_history_tokens).max(0.0),
        was_trimmed: start > 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn content(role: &str, text: &str) -> Value {
        json!({"role":role,"parts":[{"text":text}]})
    }

    #[test]
    fn threshold_ladder_matches_current_windows() {
        for (window, warn, auto, hard, effective) in [
            (32_000.0, 7_200.0, 27_200.0, 30_200.0, 12_000.0),
            (60_000.0, 7_000.0, 27_000.0, 37_000.0, 40_000.0),
            (128_000.0, 75_000.0, 95_000.0, 105_000.0, 108_000.0),
            (200_000.0, 147_000.0, 167_000.0, 177_000.0, 180_000.0),
            (1_000_000.0, 830_000.0, 850_000.0, 977_000.0, 980_000.0),
        ] {
            let result = compute_thresholds(window, None);
            assert_eq!(result.warn, warn);
            assert_eq!(result.auto, auto);
            assert_eq!(result.hard, hard);
            assert_eq!(result.effective_window, effective);
        }
    }

    #[test]
    fn threshold_custom_percentages_clamp_and_fall_back_on_nan() {
        assert_eq!(compute_thresholds(32_000.0, Some(0.5)).auto, 16_000.0);
        assert_eq!(compute_thresholds(1_000_000.0, Some(0.5)).auto, 500_000.0);
        assert_eq!(
            compute_thresholds(32_000.0, Some(-0.5)),
            compute_thresholds(32_000.0, Some(0.0))
        );
        assert_eq!(
            compute_thresholds(32_000.0, Some(1.5)),
            compute_thresholds(32_000.0, Some(1.0))
        );
        assert_eq!(
            compute_thresholds(32_000.0, Some(f64::NAN)),
            compute_thresholds(32_000.0, None)
        );
        assert_eq!(
            compute_thresholds(0.0, None),
            CompactionThresholds {
                warn: 0.0,
                auto: 0.0,
                hard: 0.0,
                effective_window: 0.0
            }
        );
    }

    #[test]
    fn compression_output_budget_keeps_safety_margin_at_window_edge() {
        assert_eq!(
            compute_compression_output_tokens(262_144.0, 253_953.0),
            6_143.0
        );
        assert_eq!(
            compute_compression_output_tokens(262_144.0, 100_000.0),
            20_000.0
        );
        assert_eq!(compute_compression_output_tokens(262_144.0, 262_144.0), 1.0);
    }

    #[test]
    fn token_estimate_counts_utf16_text_and_fixed_image_budget() {
        assert_eq!(
            estimate_content_tokens(&[content("user", "hello world")], 1_600.0),
            3.0
        );
        assert_eq!(
            estimate_content_tokens(&[json!({"role":"user","parts":[{"text":"😀"}]})], 1_600.0),
            1.0
        );
        assert_eq!(
            estimate_content_tokens(
                &[
                    json!({"role":"user","parts":[{"inlineData":{"data":"base64","mimeType":"image/png"}}]})
                ],
                1_600.0
            ),
            1_600.0
        );
    }

    #[test]
    fn function_result_estimate_counts_text_without_base64_payloads() {
        let result = json!({
            "role":"user",
            "parts":[{
                "functionResponse":{
                    "name":"read_file",
                    "response":{"output":"visible"},
                    "parts":[{"inlineData":{"data":"large-base64","mimeType":"image/png"}}]
                }
            }]
        });
        assert_eq!(estimate_content_tokens(&[result], 1_600.0), 1_618.0);
    }

    #[test]
    fn compression_history_is_returned_unchanged_when_it_fits() {
        let history = vec![content("user", "a small prompt")];
        let result = fit_compression_history_to_context(
            history.clone(),
            262_144.0,
            "system",
            1_600.0,
            None,
            COMPACT_MAX_OUTPUT_TOKENS,
        );
        assert_eq!(result.history, history);
        assert!(!result.was_trimmed);
        assert_eq!(result.omitted_token_estimate, 0.0);
    }

    #[test]
    fn emergency_compaction_keeps_a_complete_user_suffix_and_marks_omission() {
        let history = vec![
            content("user", &"old ".repeat(10_000)),
            content("model", &"tool exchange ".repeat(2_000)),
            content("user", "latest question"),
            content("model", "latest answer"),
        ];
        let result = fit_compression_history_to_context(
            history,
            32_000.0,
            "",
            1_600.0,
            Some(80_000.0),
            1_000.0,
        );
        assert!(result.was_trimmed);
        assert_eq!(result.history.len(), 2);
        assert_eq!(
            result.history[0]["parts"][0]["text"],
            COMPRESSION_HISTORY_TRUNCATION_MARKER
        );
        assert_eq!(result.history[0]["parts"][1]["text"], "latest question");
        assert!(result.omitted_token_estimate > 0.0);
    }

    #[test]
    fn large_context_rescue_caps_the_fitted_history() {
        let history = vec![
            content("user", &"earlier ".repeat(100_000)),
            content("model", "tool result"),
            content("user", "last request"),
        ];
        let result = fit_compression_history_to_context(
            history,
            262_144.0,
            "system",
            1_600.0,
            Some(300_000.0),
            COMPACT_MAX_OUTPUT_TOKENS,
        );
        assert!(result.was_trimmed);
        assert!(!result.history.is_empty());
        assert_eq!(
            result.history[0]["parts"][0]["text"],
            COMPRESSION_HISTORY_TRUNCATION_MARKER
        );
        assert!(result.omitted_token_estimate > 0.0);
    }
}
