//! Provider-specific request mutations for the OpenAI-compatible adapters.
//!
//! These transforms remain independent from the selected agent runtime so
//! request compatibility can be reused if Canopy adopts a different harness.

use serde_json::{Map, Value};

pub use crate::modalities::{
    is_canopy_family_wire_model, is_glm_wire_model, is_tiered_effort_wire_model,
};
use crate::token_limits::{
    TokenLimitType, default_output_ceiling, has_explicit_output_limit,
    parse_positive_integer_env_value, token_limit,
};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum OpenAiProviderProfile {
    #[default]
    Default,
    DashScope,
    DeepSeek {
        official_hostname: bool,
    },
    Mistral,
    MiniMax,
    MiMo,
    ModelScope,
    Zai {
        official_hostname: bool,
    },
}

/// Full provider selection, including DashScope's auth/default-URL rules.
pub fn detect_openai_provider_profile(
    auth_type: Option<&str>,
    base_url: Option<&str>,
    model: Option<&str>,
    dashscope_proxy_base_url: Option<&str>,
) -> OpenAiProviderProfile {
    if crate::providers::dashscope::is_dashscope_provider(
        auth_type,
        base_url,
        dashscope_proxy_base_url,
    ) {
        OpenAiProviderProfile::DashScope
    } else {
        detect_non_dashscope_profile(base_url, model)
    }
}

/// Select the non-DashScope provider profile using the same dispatch order as
/// `openaiContentGenerator/index.ts`. The caller must test DashScope first;
/// it has auth and default-endpoint rules that need the full content config.
pub fn detect_non_dashscope_profile(
    base_url: Option<&str>,
    model: Option<&str>,
) -> OpenAiProviderProfile {
    let hostname = base_url.and_then(parse_hostname);
    let model = model.unwrap_or_default().to_ascii_lowercase();
    let deepseek_hostname = hostname.as_deref().is_some_and(|hostname| {
        hostname == "api.deepseek.com" || hostname.ends_with(".api.deepseek.com")
    });
    if deepseek_hostname || model.contains("deepseek") {
        return OpenAiProviderProfile::DeepSeek {
            official_hostname: deepseek_hostname,
        };
    }

    let zai_hostname = hostname.as_deref().is_some_and(|hostname| {
        hostname == "z.ai"
            || hostname.ends_with(".z.ai")
            || hostname == "bigmodel.cn"
            || hostname.ends_with(".bigmodel.cn")
    });
    if zai_hostname || model.starts_with("glm-") {
        return OpenAiProviderProfile::Zai {
            official_hostname: zai_hostname,
        };
    }

    if hostname.as_deref().is_some_and(|hostname| {
        hostname == "xiaomimimo.com" || hostname.ends_with(".xiaomimimo.com")
    }) || model.starts_with("mimo-")
    {
        return OpenAiProviderProfile::MiMo;
    }
    if hostname
        .as_deref()
        .is_some_and(|hostname| hostname == "modelscope.cn" || hostname.ends_with(".modelscope.cn"))
    {
        return OpenAiProviderProfile::ModelScope;
    }
    if hostname.as_deref().is_some_and(|hostname| {
        matches!(hostname, "api.minimaxi.com" | "api.minimax.io")
            || hostname.ends_with(".minimaxi.com")
            || hostname.ends_with(".minimax.io")
    }) {
        return OpenAiProviderProfile::MiniMax;
    }
    if hostname.as_deref().is_some_and(|hostname| {
        hostname == "api.mistral.ai" || hostname.ends_with(".api.mistral.ai")
    }) || [
        "mistral",
        "mixtral",
        "codestral",
        "ministral",
        "pixtral",
        "magistral",
        "devstral",
    ]
    .iter()
    .any(|marker| model.contains(marker))
    {
        return OpenAiProviderProfile::Mistral;
    }
    OpenAiProviderProfile::Default
}

fn parse_hostname(base_url: &str) -> Option<String> {
    reqwest::Url::parse(base_url)
        .ok()?
        .host_str()
        .map(str::to_ascii_lowercase)
}

/// Provider generation defaults; explicit request/config values still win.
pub fn provider_default_generation_config(profile: OpenAiProviderProfile) -> Value {
    match profile {
        OpenAiProviderProfile::DashScope => Value::Object(Map::new()),
        OpenAiProviderProfile::DeepSeek { .. } => serde_json::json!({"temperature":0}),
        _ => Value::Object(Map::new()),
    }
}

/// Parsing behavior returned by the selected adapter. MiniMax uses tagged
/// `<think>` content; other current profiles inherit the default leak check.
pub fn response_parsing_options(
    profile: OpenAiProviderProfile,
) -> crate::providers::streaming_converter::StreamResponseParsingOptions {
    crate::providers::streaming_converter::StreamResponseParsingOptions {
        tagged_thinking_tags: matches!(profile, OpenAiProviderProfile::MiniMax),
        content_only_thinking_tag_leaks: !matches!(profile, OpenAiProviderProfile::MiniMax),
    }
}

/// Apply a provider adapter's request changes after the shared request builder.
/// `extra_body` has the same last-write precedence as the TypeScript adapter.
pub fn apply_openai_provider_profile(
    request: Value,
    profile: OpenAiProviderProfile,
    sampling_params_configured: bool,
    extra_body: Option<&Value>,
) -> Value {
    if profile == OpenAiProviderProfile::DashScope {
        return request;
    }
    let mut request = match request {
        Value::Object(request) => request,
        value => return value,
    };

    if !sampling_params_configured {
        apply_default_output_limit(&mut request);
    }

    let model = request
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    if model.contains("qwen3") {
        if let Some(messages) = request.get_mut("messages").and_then(Value::as_array_mut) {
            for message in messages {
                mirror_reasoning_content(message);
            }
        }
    }

    if let Some(Value::Object(extra_body)) = extra_body {
        request.extend(extra_body.clone());
    }

    if let Some(messages) = request.get_mut("messages").and_then(Value::as_array_mut) {
        match profile {
            OpenAiProviderProfile::Default
            | OpenAiProviderProfile::DashScope
            | OpenAiProviderProfile::MiniMax => {}
            OpenAiProviderProfile::DeepSeek { .. } => {
                for message in messages {
                    flatten_content_parts(message);
                    ensure_reasoning_content(message);
                }
            }
            OpenAiProviderProfile::Mistral => {
                for message in messages {
                    if let Some(message) = message.as_object_mut() {
                        message.remove("reasoning_content");
                    }
                }
            }
            OpenAiProviderProfile::MiMo => {
                for message in messages {
                    ensure_reasoning_content(message);
                }
            }
            OpenAiProviderProfile::ModelScope | OpenAiProviderProfile::Zai { .. } => {}
        }
    }

    match profile {
        OpenAiProviderProfile::DeepSeek {
            official_hostname: true,
        } => translate_reasoning_effort(&mut request, true),
        OpenAiProviderProfile::Zai {
            official_hostname: true,
        } => translate_reasoning_effort(&mut request, false),
        OpenAiProviderProfile::ModelScope
            if request.get("stream").and_then(Value::as_bool) != Some(true) =>
        {
            request.remove("stream_options");
        }
        _ => {}
    }

    Value::Object(request)
}

pub(crate) fn apply_default_output_limit(request: &mut Map<String, Value>) {
    let model = request
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let model_limit = token_limit(model, TokenLimitType::Output);
    let is_known_model = has_explicit_output_limit(model);
    let configured = request.get("max_tokens");

    let effective = match configured {
        Some(Value::Number(number)) => number.as_f64().map(|value| {
            if is_known_model {
                value.min(model_limit as f64)
            } else {
                value
            }
        }),
        Some(Value::Null) | None => {
            let env = std::env::var("CANOPY_CODE_MAX_OUTPUT_TOKENS").ok();
            match parse_positive_integer_env_value(env.as_deref()) {
                Some(limit) => Some(if is_known_model {
                    limit.min(model_limit)
                } else {
                    limit
                } as f64),
                None => Some(default_output_ceiling(model) as f64),
            }
        }
        Some(_) => None,
    };
    if let Some(effective) = effective.filter(|value| value.is_finite()) {
        let number = if effective.fract() == 0.0
            && effective >= i64::MIN as f64
            && effective < -(i64::MIN as f64)
        {
            Some(serde_json::Number::from(effective as i64))
        } else if effective.fract() == 0.0
            && (0.0..18_446_744_073_709_551_616.0).contains(&effective)
        {
            Some(serde_json::Number::from(effective as u64))
        } else {
            serde_json::Number::from_f64(effective)
        };
        if let Some(number) = number {
            request.insert("max_tokens".to_owned(), Value::Number(number));
        }
    }
}

fn mirror_reasoning_content(message: &mut Value) {
    let Some(message) = message.as_object_mut() else {
        return;
    };
    if message.get("role").and_then(Value::as_str) != Some("assistant") {
        return;
    }
    let Some(reasoning_content) = message
        .get("reasoning_content")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
    else {
        return;
    };
    if message.get("reasoning").and_then(Value::as_str).is_none() {
        message.insert(
            "reasoning".to_owned(),
            Value::String(reasoning_content.to_owned()),
        );
    }
}

fn ensure_reasoning_content(message: &mut Value) {
    let Some(message) = message.as_object_mut() else {
        return;
    };
    if message.get("role").and_then(Value::as_str) == Some("assistant")
        && !message
            .get("reasoning_content")
            .is_some_and(Value::is_string)
    {
        message.insert("reasoning_content".to_owned(), Value::String(String::new()));
    }
}

fn flatten_content_parts(message: &mut Value) {
    let Some(message) = message.as_object_mut() else {
        return;
    };
    let Some(parts) = message.get("content").and_then(Value::as_array) else {
        return;
    };
    let text = parts
        .iter()
        .map(|part| match part {
            Value::String(text) => text.clone(),
            Value::Object(part) if part.get("type").and_then(Value::as_str) == Some("text") => part
                .get("text")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            Value::Object(part) => format!(
                "[Unsupported content type: {}]",
                part.get("type")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
            ),
            _ => "[Unsupported content type: unknown]".to_owned(),
        })
        .collect::<Vec<_>>()
        .join("\n\n");
    message.insert("content".to_owned(), Value::String(text));
}

fn translate_reasoning_effort(request: &mut Map<String, Value>, map_effort: bool) {
    let nested = request.get("reasoning").and_then(Value::as_object).cloned();
    let Some(mut nested) = nested else {
        return;
    };
    let Some(effort) = nested.get("effort").and_then(Value::as_str) else {
        return;
    };
    if effort.is_empty() {
        return;
    }
    if !request
        .get("reasoning_effort")
        .and_then(Value::as_str)
        .is_some_and(|value| !value.is_empty())
    {
        let value = if map_effort {
            match effort {
                "low" | "medium" => "high",
                "xhigh" => "max",
                other => other,
            }
        } else {
            effort
        };
        request.insert(
            "reasoning_effort".to_owned(),
            Value::String(value.to_owned()),
        );
    }
    nested.remove("effort");
    if nested.is_empty() {
        request.remove("reasoning");
    } else {
        request.insert("reasoning".to_owned(), Value::Object(nested));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn default_profile_applies_output_ceiling_and_mirrors_qwen_reasoning() {
        let result = apply_openai_provider_profile(
            json!({
                "model":"qwen3-coder-plus",
                "messages":[{"role":"assistant","reasoning_content":"thought"}]
            }),
            OpenAiProviderProfile::Default,
            false,
            None,
        );
        assert_eq!(result["max_tokens"].as_u64(), Some(32_768));
        assert_eq!(result["messages"][0]["reasoning"], "thought");
    }

    #[test]
    fn explicit_known_output_is_capped_but_unknown_output_is_preserved() {
        let known = apply_openai_provider_profile(
            json!({"model":"gpt-4o","max_tokens":100_000}),
            OpenAiProviderProfile::Default,
            false,
            None,
        );
        assert_eq!(known["max_tokens"].as_u64(), Some(16_384));
        let unknown = apply_openai_provider_profile(
            json!({"model":"deployment-alias","max_tokens":100_000}),
            OpenAiProviderProfile::Default,
            false,
            None,
        );
        assert_eq!(unknown["max_tokens"].as_u64(), Some(100_000));
    }

    #[test]
    fn sampling_params_suppress_default_output_injection_and_extra_body_wins() {
        let result = apply_openai_provider_profile(
            json!({"model":"gpt-4o","max_tokens":8000}),
            OpenAiProviderProfile::Default,
            true,
            Some(&json!({"max_tokens":1234})),
        );
        assert_eq!(result["max_tokens"].as_u64(), Some(1234));
        let absent = apply_openai_provider_profile(
            json!({"model":"gpt-4o"}),
            OpenAiProviderProfile::Default,
            true,
            None,
        );
        assert!(absent.get("max_tokens").is_none());
    }

    #[test]
    fn mistral_and_modelscope_remove_unsupported_request_fields() {
        let mistral = apply_openai_provider_profile(
            json!({"model":"mistral-large","messages":[{
                "role":"assistant","reasoning_content":"private"
            }]}),
            OpenAiProviderProfile::Mistral,
            true,
            None,
        );
        assert!(mistral["messages"][0].get("reasoning_content").is_none());

        let modelscope = apply_openai_provider_profile(
            json!({"stream":false,"stream_options":{"include_usage":true}}),
            OpenAiProviderProfile::ModelScope,
            true,
            None,
        );
        assert!(modelscope.get("stream_options").is_none());
    }

    #[test]
    fn deepseek_flattens_parts_adds_reasoning_and_translates_effort_only_for_official_host() {
        let input = json!({
            "reasoning":{"effort":"xhigh","budget_tokens":100},
            "messages":[
                {"role":"user","content":[{"type":"text","text":"a"},{"type":"image_url"}]},
                {"role":"assistant","content":"answer"}
            ]
        });
        let official = apply_openai_provider_profile(
            input.clone(),
            OpenAiProviderProfile::DeepSeek {
                official_hostname: true,
            },
            true,
            None,
        );
        assert_eq!(official["reasoning_effort"], "max");
        assert_eq!(official["reasoning"]["budget_tokens"], 100);
        assert_eq!(
            official["messages"][0]["content"],
            "a\n\n[Unsupported content type: image_url]"
        );
        assert_eq!(official["messages"][1]["reasoning_content"], "");

        let self_hosted = apply_openai_provider_profile(
            input,
            OpenAiProviderProfile::DeepSeek {
                official_hostname: false,
            },
            true,
            None,
        );
        assert!(self_hosted.get("reasoning_effort").is_none());
        assert_eq!(self_hosted["reasoning"]["effort"], "xhigh");
    }

    #[test]
    fn zai_maps_effort_verbatim_and_keeps_user_top_level_override() {
        let mapped = apply_openai_provider_profile(
            json!({"reasoning":{"effort":"xhigh","budget_tokens":20}}),
            OpenAiProviderProfile::Zai {
                official_hostname: true,
            },
            true,
            None,
        );
        assert_eq!(mapped["reasoning_effort"], "xhigh");
        assert_eq!(mapped["reasoning"]["budget_tokens"], 20);

        let overridden = apply_openai_provider_profile(
            json!({"reasoning":{"effort":"low"},"reasoning_effort":"high"}),
            OpenAiProviderProfile::Zai {
                official_hostname: true,
            },
            true,
            None,
        );
        assert_eq!(overridden["reasoning_effort"], "high");
        assert!(overridden.get("reasoning").is_none());
    }

    #[test]
    fn mimo_adds_empty_reasoning_content_only_to_assistant_messages() {
        let result = apply_openai_provider_profile(
            json!({"messages":[{"role":"user","content":"hi"},{"role":"assistant","content":"ok"}]}),
            OpenAiProviderProfile::MiMo,
            true,
            None,
        );
        assert!(result["messages"][0].get("reasoning_content").is_none());
        assert_eq!(result["messages"][1]["reasoning_content"], "");
    }

    #[test]
    fn detects_non_dashscope_profiles_with_hostname_and_model_gates() {
        assert_eq!(
            detect_non_dashscope_profile(Some("https://api.deepseek.com/v1"), Some("alias")),
            OpenAiProviderProfile::DeepSeek {
                official_hostname: true
            }
        );
        assert_eq!(
            detect_non_dashscope_profile(Some("https://evil.example"), Some("deepseek-chat")),
            OpenAiProviderProfile::DeepSeek {
                official_hostname: false
            }
        );
        assert_eq!(
            detect_non_dashscope_profile(Some("https://z.ai.evil.example/v1"), Some("glm-5.2")),
            OpenAiProviderProfile::Zai {
                official_hostname: false
            }
        );
        assert_eq!(
            detect_non_dashscope_profile(Some("https://api.modelscope.cn/v1"), None),
            OpenAiProviderProfile::ModelScope
        );
        assert_eq!(
            detect_non_dashscope_profile(Some("not a URL"), Some("gpt-4o")),
            OpenAiProviderProfile::Default
        );
        assert_eq!(
            detect_openai_provider_profile(
                Some("canopy-oauth"),
                Some("https://api.openai.com/v1"),
                Some("gpt-5"),
                None,
            ),
            OpenAiProviderProfile::DashScope
        );
        assert_eq!(
            detect_openai_provider_profile(
                None,
                Some("https://dashscope.aliyuncs.com/compatible-mode/v1"),
                Some("qwen3.8-max"),
                None,
            ),
            OpenAiProviderProfile::DashScope
        );
    }

    #[test]
    fn deepseek_default_and_minimax_stream_parsing_options_match_profiles() {
        assert_eq!(
            provider_default_generation_config(OpenAiProviderProfile::DeepSeek {
                official_hostname: false
            }),
            json!({"temperature":0})
        );
        let minimax = response_parsing_options(OpenAiProviderProfile::MiniMax);
        assert!(minimax.tagged_thinking_tags);
        assert!(!minimax.content_only_thinking_tag_leaks);
        let default = response_parsing_options(OpenAiProviderProfile::Default);
        assert!(!default.tagged_thinking_tags);
        assert!(default.content_only_thinking_tag_leaks);
    }
}
