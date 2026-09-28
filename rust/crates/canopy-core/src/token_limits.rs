//! Shared model-window and output-budget policy, ported from `core/tokenLimits.ts`.

use std::sync::OnceLock;

use regex::Regex;

pub type TokenCount = u64;

pub const DEFAULT_TOKEN_LIMIT: TokenCount = 200_000;
pub const DEFAULT_OUTPUT_TOKEN_LIMIT: TokenCount = 32_000;
pub const ESCALATED_MAX_TOKENS: TokenCount = 64_000;
pub const OUTPUT_TOKEN_CEILING: TokenCount = ESCALATED_MAX_TOKENS;
pub const MIN_CLAMPED_OUTPUT_TOKENS: TokenCount = 4_000;
pub const DEFAULT_CONTEXT_WINDOW_SIZE: TokenCount = 200_000;

const SAFE_INTEGER_MAX: u64 = 9_007_199_254_740_991;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum TokenLimitType {
    #[default]
    Input,
    Output,
}

#[derive(Clone, Debug)]
struct NormalizeRegexes {
    whitespace: Regex,
    right_side_variant: Regex,
    claude_dotted_minor: Regex,
    qwen_plus_latest: Regex,
    qwen_flash_latest: Regex,
    qwen_vl_max_latest: Regex,
    kimi_k2_date: Regex,
    suffix_build: Regex,
    dotted_suffix: Regex,
    quantization: Regex,
    claude_opus_extended: Regex,
}

fn normalization_regexes() -> &'static NormalizeRegexes {
    static REGEXES: OnceLock<NormalizeRegexes> = OnceLock::new();
    REGEXES.get_or_init(|| NormalizeRegexes {
        whitespace: Regex::new(r"\s+").expect("valid whitespace regex"),
        right_side_variant: Regex::new(
            r":(?:free|beta|extended|thinking|online|nitro|floor|latest|\d+(?:\.\d+)?(?:x\d+)?b(?:-[A-Za-z0-9_.]+)*)$",
        ).expect("valid model-variant regex"),
        claude_dotted_minor: Regex::new(r"^(claude-[a-z]+-\d+(?:-\d+)?)\.(\d+)(?:\.\d+)*")
            .expect("valid Claude alias regex"),
        qwen_plus_latest: Regex::new(r"^qwen-plus-latest$").expect("valid Qwen alias regex"),
        qwen_flash_latest: Regex::new(r"^qwen-flash-latest$").expect("valid Qwen alias regex"),
        qwen_vl_max_latest: Regex::new(r"^qwen-vl-max-latest$").expect("valid Qwen alias regex"),
        kimi_k2_date: Regex::new(r"^kimi-k2-\d{4}$").expect("valid Kimi alias regex"),
        suffix_build: Regex::new(r"-(?:\d{4,}|\d+x\d+b|v\d+(?:\.\d+)*|latest|exp)$")
            .expect("valid model suffix regex"),
        dotted_suffix: Regex::new(r"^(.+-[^-]+)-\d+(?:\.\d+)+$")
            .expect("valid dotted-version suffix regex"),
        quantization: Regex::new(r"-(?:\d?bit|int[48]|bf16|fp16|q[45]|quantized)$")
            .expect("valid quantization suffix regex"),
        claude_opus_extended: Regex::new(r"^claude-opus-(?:4-(?:6|7|8)|5)")
            .expect("valid Opus tier regex"),
    })
}

const INPUT_RULES: &[(&str, TokenCount)] = &[
    (r"^gemini-3", 1_000_000),
    (r"^gemini-", 1_000_000),
    (r"^gpt-5", 272_000),
    (r"^gpt-", 131_072),
    (r"^o\d", 200_000),
    (r"^claude-opus-(?:4-(?:6|7|8)|5)", 1_000_000),
    (r"^claude-", 200_000),
    (r"^qwen3-coder-plus", 1_000_000),
    (r"^qwen3-coder-flash", 1_000_000),
    (r"^qwen3\.8-flash-next", 262_144),
    (r"^qwen3\.\d", 1_000_000),
    (r"^qwen-plus-latest$", 1_000_000),
    (r"^qwen-flash-latest$", 1_000_000),
    (r"^coder-model$", 1_000_000),
    (r"^qwen3-max", 262_144),
    (r"^qwen3-coder-", 262_144),
    (r"^qwen", 262_144),
    (r"^deepseek-v4", 1_000_000),
    (r"^deepseek", 131_072),
    (r"^glm-5(\.[01])?(-|$)", 202_752),
    (r"^glm-(?:[5-9]|\d{2,})", 1_000_000),
    (r"^glm-", 202_752),
    (r"^minimax-m3", 1_000_000),
    (r"^minimax-m2\.5", 196_608),
    (r"^minimax-", 200_000),
    (r"^kimi-k3", 1_000_000),
    (r"^kimi-", 262_144),
    (r"^seed-oss", 524_288),
];

const OUTPUT_RULES: &[(&str, TokenCount)] = &[
    (r"^gemini-3", 65_536),
    (r"^gemini-", 8_192),
    (r"^gpt-5", 131_072),
    (r"^gpt-", 16_384),
    (r"^o\d", 131_072),
    (r"^claude-opus-(?:4-(?:6|7|8)|5)", 128_000),
    (r"^claude-sonnet-4-6", 65_536),
    (r"^claude-", 65_536),
    (r"^qwen3\.\d", 65_536),
    (r"^coder-model$", 65_536),
    (r"^qwen", 32_768),
    (r"^deepseek-v4", 384_000),
    (r"^deepseek-reasoner", 65_536),
    (r"^deepseek-r1", 65_536),
    (r"^deepseek-chat", 8_192),
    (r"^glm-5(?:\.\d+)?(?:-|$)", 131_072),
    (r"^glm-4\.7", 16_384),
    (r"^minimax-m2\.5", 65_536),
    (r"^kimi-k3", 131_072),
    (r"^kimi-k2\.5", 32_768),
];

fn input_rules() -> &'static [(Regex, TokenCount)] {
    static RULES: OnceLock<Vec<(Regex, TokenCount)>> = OnceLock::new();
    RULES.get_or_init(|| compile_rules(INPUT_RULES))
}

fn output_rules() -> &'static [(Regex, TokenCount)] {
    static RULES: OnceLock<Vec<(Regex, TokenCount)>> = OnceLock::new();
    RULES.get_or_init(|| compile_rules(OUTPUT_RULES))
}

fn compile_rules(source: &[(&str, TokenCount)]) -> Vec<(Regex, TokenCount)> {
    source
        .iter()
        .map(|(pattern, limit)| {
            (
                Regex::new(pattern).expect("valid model token-limit regex"),
                *limit,
            )
        })
        .collect()
}

/// Normalize provider-qualified model IDs and common version/quantization tags.
pub fn normalize(model: &str) -> String {
    let regexes = normalization_regexes();
    let mut normalized = model.to_lowercase().trim().to_owned();
    if let Some((_, provider_model)) = normalized.rsplit_once('/') {
        normalized = provider_model.to_owned();
    }
    if let Some((_, final_model)) = normalized.rsplit_once('|') {
        normalized = final_model.to_owned();
    }
    if regexes.right_side_variant.is_match(&normalized) {
        if let Some((model, _)) = normalized.rsplit_once(':') {
            normalized = model.to_owned();
        }
    }
    if let Some((_, right)) = normalized.rsplit_once(':') {
        normalized = right.to_owned();
    }
    normalized = regexes
        .whitespace
        .replace_all(&normalized, "-")
        .into_owned();
    normalized = regexes
        .claude_dotted_minor
        .replace(&normalized, "$1-$2")
        .into_owned();
    normalized = normalized.replace("-preview", "");

    let preserve_latest = regexes.qwen_plus_latest.is_match(&normalized)
        || regexes.qwen_flash_latest.is_match(&normalized)
        || regexes.qwen_vl_max_latest.is_match(&normalized);
    let preserve_kimi_date = regexes.kimi_k2_date.is_match(&normalized);
    if !preserve_latest && !preserve_kimi_date {
        normalized = regexes.suffix_build.replace(&normalized, "").into_owned();
        if let Some(captures) = regexes.dotted_suffix.captures(&normalized) {
            normalized = captures
                .get(1)
                .map_or_else(|| normalized.clone(), |prefix| prefix.as_str().to_owned());
        }
    }
    regexes.quantization.replace(&normalized, "").into_owned()
}

pub fn output_clamp_margin(context_window_size: f64) -> f64 {
    if context_window_size.is_nan() {
        return f64::NAN;
    }
    10_000.0_f64.max((0.05 * context_window_size).round())
}

/// Return `min(ceiling, max(4000, window - prompt - margin))`.
pub fn clamp_output_tokens_to_window(
    output_ceiling: f64,
    context_window_size: f64,
    prompt_tokens: f64,
) -> f64 {
    let margin = output_clamp_margin(context_window_size);
    let room = context_window_size - prompt_tokens - margin;
    if output_ceiling.is_nan() || room.is_nan() {
        return f64::NAN;
    }
    output_ceiling.min(room.max(MIN_CLAMPED_OUTPUT_TOKENS as f64))
}

pub fn parse_positive_integer_env_value(raw: Option<&str>) -> Option<u64> {
    let raw = raw?;
    let trimmed = raw.trim();
    if trimmed.is_empty() || !trimmed.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let parsed = trimmed.parse::<u64>().ok()?;
    (parsed > 0 && parsed <= SAFE_INTEGER_MAX).then_some(parsed)
}

fn find_token_limit(model: &str, limit_type: TokenLimitType) -> Option<u64> {
    let normalized = normalize(model);
    let rules = match limit_type {
        TokenLimitType::Input => input_rules(),
        TokenLimitType::Output => output_rules(),
    };
    rules
        .iter()
        .find(|(pattern, _)| pattern.is_match(&normalized))
        .map(|(_, limit)| *limit)
}

pub fn has_explicit_output_limit(model: &str) -> bool {
    let normalized = normalize(model);
    output_rules()
        .iter()
        .any(|(pattern, _)| pattern.is_match(&normalized))
}

pub fn known_token_limit(model: &str, limit_type: TokenLimitType) -> Option<u64> {
    find_token_limit(model, limit_type)
}

pub fn token_limit(model: &str, limit_type: TokenLimitType) -> u64 {
    known_token_limit(model, limit_type).unwrap_or(match limit_type {
        TokenLimitType::Input => DEFAULT_TOKEN_LIMIT,
        TokenLimitType::Output => DEFAULT_OUTPUT_TOKEN_LIMIT,
    })
}

pub fn default_output_ceiling(model: &str) -> u64 {
    let output_limit = token_limit(model, TokenLimitType::Output);
    if normalization_regexes()
        .claude_opus_extended
        .is_match(&normalize(model))
    {
        output_limit
    } else {
        output_limit.min(OUTPUT_TOKEN_CEILING)
    }
}

pub fn reconcile_max_tokens(
    config_max_tokens: Option<f64>,
    request_max_tokens: Option<f64>,
) -> Option<f64> {
    let config_max_tokens = config_max_tokens?;
    let request_max_tokens = request_max_tokens?;
    if config_max_tokens.is_nan() || request_max_tokens.is_nan() {
        Some(f64::NAN)
    } else {
        Some(config_max_tokens.min(request_max_tokens))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalization_handles_prefixes_variants_and_version_suffixes() {
        let cases = [
            ("  GEMINI-1.5-PRO  ", "gemini-1.5-pro"),
            ("google/gemini-1.5-pro", "gemini-1.5-pro"),
            ("qwen|qwen2.5:qwen2.5-1m", "qwen2.5-1m"),
            ("qwen/qwen3-coder:free", "qwen3-coder"),
            ("google/gemini-2.5-pro:online", "gemini-2.5-pro"),
            ("qwen2.5-coder:32b", "qwen2.5-coder"),
            ("llama3.1:8b-instruct-q4_k_m", "llama3.1"),
            ("claude 3.5 sonnet", "claude-3.5-sonnet"),
            ("claude-opus-4.8", "claude-opus-4-8"),
            ("claude-opus-4.8.0", "claude-opus-4-8"),
            ("claude-opus-4-8.0", "claude-opus-4-8-0"),
            ("Claude Opus 4.8", "claude-opus-4-8"),
            ("claude-newfam-5.1", "claude-newfam-5-1"),
            ("gemini-1.5-pro-20250219", "gemini-1.5-pro"),
            ("gpt-4o-mini-v1", "gpt-4o-mini"),
            ("gpt-4.1-latest", "gpt-4.1"),
            ("gemini-2.0-flash-preview-20250520", "gemini-2.0-flash"),
            ("qwen3-coder-7b-4bit", "qwen3-coder-7b"),
            ("llama-4-scout-int8", "llama-4-scout"),
            ("mistral-large-2-bf16", "mistral-large-2"),
            ("deepseek-v3.1-q4", "deepseek-v3.1"),
            ("qwen2.5-quantized", "qwen2.5"),
            ("qwen-plus-latest", "qwen-plus-latest"),
            ("qwen-vl-max-latest", "qwen-vl-max-latest"),
            ("kimi-k2-0905-preview", "kimi-k2-0905"),
            ("model-test-v1.1", "model-test"),
            ("model-test-1.1", "model-test"),
            ("gpt-4.1", "gpt-4.1"),
        ];
        for (input, expected) in cases {
            assert_eq!(normalize(input), expected, "{input}");
        }
        assert_eq!(normalize(""), "");
    }

    #[test]
    fn model_limit_tables_match_provider_families() {
        let cases = [
            ("gemini-3-pro-preview", 1_000_000, 65_536),
            ("gemini-2.5-pro", 1_000_000, 8_192),
            ("gpt-5.2-pro", 272_000, 131_072),
            ("gpt-4.1", 131_072, 16_384),
            ("o4-mini", 200_000, 131_072),
            ("claude-opus-4.8", 1_000_000, 128_000),
            ("Claude Opus 5.1", 1_000_000, 128_000),
            ("claude-sonnet-4-6", 200_000, 65_536),
            ("qwen3-coder-plus", 1_000_000, 32_768),
            ("qwen3.5-plus", 1_000_000, 65_536),
            ("qwen3.8-flash-next", 262_144, 65_536),
            ("qwen3-max", 262_144, 32_768),
            ("coder-model", 1_000_000, 65_536),
            ("deepseek-v4-pro", 1_000_000, 384_000),
            ("deepseek-r1-0528", 131_072, 65_536),
            ("deepseek-chat", 131_072, 8_192),
            ("glm-5.1", 202_752, 131_072),
            ("glm-5.2", 1_000_000, 131_072),
            ("glm-10", 1_000_000, 32_000),
            ("glm-4.7", 202_752, 16_384),
            ("MiniMax-M3", 1_000_000, 32_000),
            ("MiniMax-M2.5", 196_608, 65_536),
            ("MiniMax-M2.1", 200_000, 32_000),
            ("kimi-k3", 1_000_000, 131_072),
            ("kimi-k2.5", 262_144, 32_768),
            ("seed-oss", 524_288, 32_000),
            ("unknown-model", 200_000, 32_000),
        ];
        for (model, input, output) in cases {
            assert_eq!(
                token_limit(model, TokenLimitType::Input),
                input,
                "{model} input"
            );
            assert_eq!(
                token_limit(model, TokenLimitType::Output),
                output,
                "{model} output"
            );
        }
    }

    #[test]
    fn normalization_preserves_variant_tagged_model_limits() {
        let cases = [
            ("qwen/qwen3-coder:free", "qwen/qwen3-coder", 262_144),
            (
                "google/gemini-2.5-pro:online",
                "google/gemini-2.5-pro",
                1_000_000,
            ),
            ("openai/gpt-5:free", "openai/gpt-5", 272_000),
            ("qwen2.5-coder:32b", "qwen2.5-coder", 262_144),
            ("qwen3-coder-plus:nitro", "qwen3-coder-plus", 1_000_000),
        ];
        for (tagged, bare, expected) in cases {
            assert_eq!(token_limit(tagged, TokenLimitType::Input), expected);
            assert_eq!(
                token_limit(tagged, TokenLimitType::Input),
                token_limit(bare, TokenLimitType::Input)
            );
            assert_eq!(
                token_limit(tagged, TokenLimitType::Output),
                token_limit(bare, TokenLimitType::Output)
            );
        }
    }

    #[test]
    fn clamps_output_budgets_with_margin_and_floor() {
        assert_eq!(output_clamp_margin(200_000.0), 10_000.0);
        assert_eq!(output_clamp_margin(1_000_000.0), 50_000.0);
        assert_eq!(
            clamp_output_tokens_to_window(32_000.0, 200_000.0, 50_000.0),
            32_000.0
        );
        assert_eq!(
            clamp_output_tokens_to_window(32_000.0, 200_000.0, 170_000.0),
            20_000.0
        );
        assert_eq!(
            clamp_output_tokens_to_window(32_000.0, 40_000.0, 60_000.0),
            4_000.0
        );
        assert_eq!(
            clamp_output_tokens_to_window(2_000.0, 40_000.0, 39_000.0),
            2_000.0
        );
        assert_eq!(
            clamp_output_tokens_to_window(64_000.0, 1_000_000.0, 500_000.0),
            64_000.0
        );
        assert!(clamp_output_tokens_to_window(f64::NAN, 10_000.0, 0.0).is_nan());
    }

    #[test]
    fn output_ceiling_exempts_extended_opus_and_caps_other_models() {
        assert_eq!(
            default_output_ceiling("deepseek-v4-pro"),
            OUTPUT_TOKEN_CEILING
        );
        assert_eq!(default_output_ceiling("kimi-k2.5"), 32_768);
        for model in ["claude-opus-4-6", "claude-opus-4.8", "claude-opus-5-1"] {
            assert_eq!(default_output_ceiling(model), 128_000, "{model}");
        }
        assert_eq!(
            default_output_ceiling("unknown-model"),
            DEFAULT_OUTPUT_TOKEN_LIMIT
        );
    }

    #[test]
    fn output_limit_presence_and_config_reconciliation_are_explicit() {
        assert!(has_explicit_output_limit("qwen3-max"));
        assert!(!has_explicit_output_limit("unknown-model"));
        assert_eq!(
            known_token_limit("qwen3-max", TokenLimitType::Output),
            Some(32_768)
        );
        assert_eq!(
            known_token_limit("unknown-model", TokenLimitType::Input),
            None
        );
        assert_eq!(
            reconcile_max_tokens(Some(8_000.0), Some(5_000.0)),
            Some(5_000.0)
        );
        assert_eq!(
            reconcile_max_tokens(Some(5_000.0), Some(8_000.0)),
            Some(5_000.0)
        );
        assert_eq!(reconcile_max_tokens(Some(8_000.0), None), None);
    }

    #[test]
    fn parses_only_positive_safe_decimal_integers() {
        assert_eq!(parse_positive_integer_env_value(Some(" 42 ")), Some(42));
        for value in [
            None,
            Some(""),
            Some("0"),
            Some("-2"),
            Some("1.0"),
            Some("1e3"),
            Some("0x10"),
            Some("9007199254740992"),
        ] {
            assert_eq!(parse_positive_integer_env_value(value), None, "{value:?}");
        }
    }
}
