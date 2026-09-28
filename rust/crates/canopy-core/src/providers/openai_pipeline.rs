//! Base request construction for Canopy's OpenAI-compatible generation path.
//!
//! Provider-specific mutations are supplied through a hook, matching the
//! upstream provider adapter boundary. Retry and error recovery remain in the
//! outer generation loop.

use serde_json::{Map, Value, json};

use crate::providers::dashscope::{DashScopeRequestConfig, build_dashscope_request};
use crate::providers::openai_profiles::{
    OpenAiProviderProfile, apply_openai_provider_profile, provider_default_generation_config,
};
use crate::providers::openai_request::{OpenAiRequestContext, convert_gemini_request_to_openai};
use crate::providers::prefix_caching::{
    OpenAiPrefixCacheConfig, apply_official_openai_prompt_caching, is_official_openai_endpoint,
};
use crate::providers::schema::{SchemaComplianceMode, convert_gemini_tools_to_openai};
use crate::token_limits::reconcile_max_tokens;

const MAX_STRICT_SCHEMA_DEPTH: usize = 128;
const MAX_STRICT_SCHEMA_NODES: usize = 16_384;
const PROVIDER_OUTPUT_BUDGET_KEYS: &[&str] = &["max_completion_tokens", "max_new_tokens"];

#[derive(Clone, Debug, Default)]
pub struct OpenAiPipelineConfig {
    pub request_context: OpenAiRequestContext,
    pub default_generation_config: Value,
    pub sampling_params: Option<Value>,
    pub reasoning: Option<Value>,
    /// Configured total request attempts; TypeScript's `maxRetries` value is
    /// converted to retries plus the initial request.
    pub retry_max_attempts: Option<usize>,
    pub retry_initial_delay_ms: Option<f64>,
    pub retry_max_delay_ms: Option<f64>,
    pub retry_error_codes: Vec<i64>,
    pub schema_compliance: SchemaComplianceMode,
    pub prefix_cache_config: OpenAiPrefixCacheConfig,
    pub enable_cache_control: Option<bool>,
    /// Keep provider-generated thinking disable controls off the wire for a
    /// model whose endpoint rejects them.
    pub thinking_mandatory: bool,
    pub provider_profile: OpenAiProviderProfile,
    pub extra_body: Option<Value>,
    pub session_id: Option<String>,
    pub channel: Option<String>,
    /// Omit for forked requests whose prefix must not share the parent's cache.
    pub cache_key_partition: Option<String>,
}

/// Build the generation-parameter portion of a Chat Completions request.
pub fn build_generate_content_config(request: &Value, config: &OpenAiPipelineConfig) -> Value {
    let request_config = request.get("config");
    let request_max_tokens = request_config
        .and_then(|config| config.get("maxOutputTokens"))
        .and_then(Value::as_f64);
    let mut output = Map::new();

    if let Some(sampling) = config.sampling_params.as_ref().and_then(Value::as_object) {
        let configured_max = sampling.get("max_tokens").and_then(Value::as_f64);
        let reconciled = reconcile_max_tokens(configured_max, request_max_tokens);
        let mut max_tokens = reconciled
            .map(|value| {
                if configured_max.is_some_and(|configured| configured <= value) {
                    sampling
                        .get("max_tokens")
                        .cloned()
                        .unwrap_or_else(|| json!(value))
                } else {
                    request_config
                        .and_then(|request_config| request_config.get("maxOutputTokens"))
                        .cloned()
                        .unwrap_or_else(|| json!(value))
                }
            })
            .or_else(|| {
                sampling
                    .get("max_tokens")
                    .filter(|value| !value.is_null())
                    .cloned()
            })
            .or_else(|| {
                (!has_provider_output_budget_key(sampling)).then(|| {
                    request_config
                        .and_then(|request_config| request_config.get("maxOutputTokens"))
                        .cloned()
                })?
            });

        output.extend(
            sampling
                .iter()
                .map(|(key, value)| (key.clone(), value.clone())),
        );
        if let Some(max_tokens) = max_tokens.take() {
            output.insert("max_tokens".to_owned(), max_tokens);
        }
        clamp_provider_output_budget_keys(&mut output, request_max_tokens, request_config);
        return Value::Object(output);
    }

    insert_parameter(
        &mut output,
        "temperature",
        request_config,
        "temperature",
        Some("temperature"),
        &effective_default_generation_config(config),
    );
    insert_parameter(
        &mut output,
        "top_p",
        request_config,
        "topP",
        Some("topP"),
        &config.default_generation_config,
    );
    insert_parameter(
        &mut output,
        "max_tokens",
        request_config,
        "maxOutputTokens",
        Some("maxOutputTokens"),
        &config.default_generation_config,
    );
    insert_parameter(
        &mut output,
        "top_k",
        request_config,
        "topK",
        Some("topK"),
        &config.default_generation_config,
    );
    insert_parameter(
        &mut output,
        "repetition_penalty",
        None,
        "",
        None,
        &config.default_generation_config,
    );
    insert_parameter(
        &mut output,
        "presence_penalty",
        request_config,
        "presencePenalty",
        Some("presencePenalty"),
        &config.default_generation_config,
    );
    insert_parameter(
        &mut output,
        "frequency_penalty",
        request_config,
        "frequencyPenalty",
        Some("frequencyPenalty"),
        &config.default_generation_config,
    );

    let thinking_disabled = request_config
        .and_then(|request_config| request_config.pointer("/thinkingConfig/includeThoughts"))
        .and_then(Value::as_bool)
        == Some(false);
    if !thinking_disabled
        && config
            .reasoning
            .as_ref()
            .is_some_and(|reasoning| *reasoning != Value::Bool(false))
    {
        output.insert(
            "reasoning".to_owned(),
            config.reasoning.clone().unwrap_or(Value::Null),
        );
    }
    Value::Object(output)
}

fn effective_default_generation_config(config: &OpenAiPipelineConfig) -> Value {
    let mut defaults = provider_default_generation_config(config.provider_profile);
    if let (Some(defaults), Some(overrides)) = (
        defaults.as_object_mut(),
        config.default_generation_config.as_object(),
    ) {
        defaults.extend(overrides.clone());
    } else if !config.default_generation_config.is_null() {
        return config.default_generation_config.clone();
    }
    Value::Object(defaults.as_object().cloned().unwrap_or_default())
}

fn insert_parameter(
    output: &mut Map<String, Value>,
    openai_key: &str,
    request_config: Option<&Value>,
    request_key: &str,
    default_key: Option<&str>,
    defaults: &Value,
) {
    let value = request_config
        .and_then(|request_config| request_config.get(request_key))
        .or_else(|| default_key.and_then(|key| defaults.get(key)));
    if let Some(value) = value {
        output.insert(openai_key.to_owned(), value.clone());
    }
}

fn has_provider_output_budget_key(sampling: &Map<String, Value>) -> bool {
    PROVIDER_OUTPUT_BUDGET_KEYS
        .iter()
        .any(|key| sampling.contains_key(*key))
}

fn clamp_provider_output_budget_keys(
    sampling: &mut Map<String, Value>,
    request_max_tokens: Option<f64>,
    request_config: Option<&Value>,
) {
    let Some(request_max_tokens) = request_max_tokens else {
        return;
    };
    let request_max_value = request_config
        .and_then(|request_config| request_config.get("maxOutputTokens"))
        .cloned()
        .unwrap_or_else(|| json!(request_max_tokens));
    for key in PROVIDER_OUTPUT_BUDGET_KEYS {
        let Some(value) = sampling.get(*key).and_then(Value::as_f64) else {
            continue;
        };
        if value > request_max_tokens {
            sampling.insert((*key).to_owned(), request_max_value.clone());
        }
    }
}

/// Return a strict OpenAI JSON-schema response format when the schema can be
/// represented by Structured Outputs. Unsupported shapes fall back to JSON
/// object mode, as in the TypeScript pipeline.
pub fn build_response_format(request: &Value, official_openai_endpoint: bool) -> Option<Value> {
    let request_config = request.get("config")?;
    if !official_openai_endpoint
        || request_config
            .get("responseMimeType")
            .and_then(Value::as_str)
            != Some("application/json")
    {
        return None;
    }
    let schema = request_config
        .get("responseJsonSchema")
        .filter(|schema| !schema.is_null())
        .or_else(|| {
            request_config
                .get("responseSchema")
                .filter(|schema| !schema.is_null())
        });
    let Some(schema) = schema else {
        return Some(json!({"type":"json_object"}));
    };
    let mut visited = 0;
    let strict_schema = normalize_openai_strict_schema(schema, 0, &mut visited);
    let Some(schema) = strict_schema else {
        return Some(json!({"type":"json_object"}));
    };
    Some(json!({
        "type":"json_schema",
        "json_schema":{"name":"response","schema":schema,"strict":true}
    }))
}

fn normalize_openai_strict_schema(
    schema: &Value,
    depth: usize,
    visited: &mut usize,
) -> Option<Value> {
    if depth > MAX_STRICT_SCHEMA_DEPTH || *visited >= MAX_STRICT_SCHEMA_NODES {
        return None;
    }
    *visited += 1;
    let source = schema.as_object()?;
    let source_type = source.get("type")?.as_str()?;
    let normalized_type = source_type.to_ascii_lowercase();
    if !matches!(
        normalized_type.as_str(),
        "object" | "array" | "string" | "number" | "integer" | "boolean" | "null"
    ) {
        return None;
    }
    let mut normalized = Map::new();
    normalized.insert("type".to_owned(), Value::String(normalized_type.clone()));
    for key in [
        "properties",
        "required",
        "additionalProperties",
        "items",
        "description",
        "enum",
    ] {
        if let Some(value) = source.get(key) {
            normalized.insert(key.to_owned(), value.clone());
        }
    }

    if normalized_type == "object" {
        let properties = source.get("properties")?.as_object()?;
        let required = source.get("required")?.as_array()?;
        if required.len() != properties.len()
            || !properties.keys().all(|key| {
                required
                    .iter()
                    .any(|required_key| required_key.as_str() == Some(key.as_str()))
            })
        {
            return None;
        }
        let mut normalized_properties = Map::new();
        for (key, property) in properties {
            normalized_properties.insert(
                key.clone(),
                normalize_openai_strict_schema(property, depth + 1, visited)?,
            );
        }
        normalized.insert(
            "properties".to_owned(),
            Value::Object(normalized_properties),
        );
        normalized.insert("required".to_owned(), Value::Array(required.clone()));
        normalized.insert("additionalProperties".to_owned(), Value::Bool(false));
    } else if normalized_type == "array" {
        let items = source.get("items")?;
        normalized.insert(
            "items".to_owned(),
            normalize_openai_strict_schema(items, depth + 1, visited)?,
        );
    }
    Some(Value::Object(normalized))
}

/// Build the OpenAI wire request through message/schema conversion, optional
/// provider enhancement, cache-key processing, and stream/compression flags.
pub fn build_openai_request<F>(
    request: &Value,
    config: &OpenAiPipelineConfig,
    user_prompt_id: &str,
    is_streaming: bool,
    provider_enhancement: F,
) -> Value
where
    F: FnOnce(Value, &str) -> Value,
{
    let model = request
        .get("model")
        .and_then(Value::as_str)
        .filter(|model| !model.is_empty())
        .unwrap_or(&config.request_context.model);
    let mut request_context = config.request_context.clone();
    request_context.model = model.to_owned();
    let messages = convert_gemini_request_to_openai(request, &request_context);
    let mut base = Map::new();
    base.insert("model".to_owned(), Value::String(model.to_owned()));
    base.insert("messages".to_owned(), Value::Array(messages));

    if let Value::Object(parameters) = build_generate_content_config(request, config) {
        base.extend(parameters);
    }
    let official = is_official_openai_endpoint(&config.prefix_cache_config);
    if let Some(response_format) = build_response_format(request, official) {
        base.insert("response_format".to_owned(), response_format);
    }
    base.insert("stream".to_owned(), Value::Bool(is_streaming));
    if is_streaming {
        base.insert("stream_options".to_owned(), json!({"include_usage":true}));
    }

    if let Some(tools) = request
        .pointer("/config/tools")
        .and_then(Value::as_array)
        .filter(|tools| !tools.is_empty())
    {
        base.insert(
            "tools".to_owned(),
            Value::Array(convert_gemini_tools_to_openai(
                &Value::Array(tools.clone()),
                config.schema_compliance,
            )),
        );
        match request
            .pointer("/config/toolConfig/functionCallingConfig/mode")
            .and_then(Value::as_str)
        {
            Some("ANY") => {
                base.insert(
                    "tool_choice".to_owned(),
                    Value::String("required".to_owned()),
                );
            }
            Some("NONE") => {
                base.insert("tool_choice".to_owned(), Value::String("none".to_owned()));
            }
            _ => {}
        }
    }

    let provider_request = provider_enhancement(Value::Object(base), user_prompt_id);
    let mut provider_request = if config.provider_profile == OpenAiProviderProfile::DashScope {
        build_dashscope_request(
            &provider_request,
            &DashScopeRequestConfig {
                configured_model: Some(config.request_context.model.clone()),
                sampling_params_configured: config.sampling_params.is_some(),
                enable_cache_control: config.enable_cache_control,
                extra_body: config.extra_body.clone(),
                session_id: config.session_id.clone(),
                channel: config.channel.clone(),
                reasoning_effort: config
                    .reasoning
                    .as_ref()
                    .and_then(|reasoning| reasoning.get("effort"))
                    .cloned(),
            },
            user_prompt_id,
        )
    } else {
        apply_openai_provider_profile(
            provider_request,
            config.provider_profile,
            config.sampling_params.is_some(),
            config.extra_body.as_ref(),
        )
    };
    if config.thinking_mandatory && model.eq_ignore_ascii_case(&config.request_context.model) {
        remove_thinking_disable_controls(&mut provider_request);
    }
    if config.enable_cache_control != Some(false) && official {
        provider_request = apply_official_openai_prompt_caching(
            &provider_request,
            config.session_id.as_deref(),
            request.get("promptCacheSharing").and_then(Value::as_bool) == Some(true),
            config.cache_key_partition.as_deref(),
        );
    }

    if is_compression_prompt_id(user_prompt_id) {
        if let Some(request_max) = request
            .pointer("/config/maxOutputTokens")
            .and_then(Value::as_f64)
            .filter(|value| value.is_finite() && *value > 0.0)
        {
            if let Some(provider_request) = provider_request.as_object_mut() {
                let current_max = provider_request.get("max_tokens").and_then(Value::as_f64);
                if let Some(max_tokens) = current_max {
                    if max_tokens > request_max {
                        provider_request.insert(
                            "max_tokens".to_owned(),
                            request
                                .pointer("/config/maxOutputTokens")
                                .cloned()
                                .unwrap_or_else(|| json!(request_max)),
                        );
                    }
                } else {
                    provider_request.insert(
                        "max_tokens".to_owned(),
                        request
                            .pointer("/config/maxOutputTokens")
                            .cloned()
                            .unwrap_or_else(|| json!(request_max)),
                    );
                }
                clamp_provider_output_budget_keys(
                    provider_request,
                    Some(request_max),
                    request.get("config"),
                );
            }
        }
    }
    provider_request
}

fn remove_thinking_disable_controls(request: &mut Value) {
    let Some(request) = request.as_object_mut() else {
        return;
    };
    if request.get("enable_thinking") == Some(&Value::Bool(false)) {
        request.remove("enable_thinking");
    }
    if request.get("reasoning_effort").and_then(Value::as_str) == Some("none") {
        request.remove("reasoning_effort");
    }
    let remove_chat_template_kwargs = if let Some(chat_template_kwargs) = request
        .get_mut("chat_template_kwargs")
        .and_then(Value::as_object_mut)
    {
        if chat_template_kwargs.get("enable_thinking") == Some(&Value::Bool(false)) {
            chat_template_kwargs.remove("enable_thinking");
        }
        chat_template_kwargs.is_empty()
    } else {
        false
    };
    if remove_chat_template_kwargs {
        request.remove("chat_template_kwargs");
    }
}

fn is_compression_prompt_id(prompt_id: &str) -> bool {
    prompt_id.starts_with("compress-") || prompt_id == "side-query:chat-compression"
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> OpenAiPipelineConfig {
        OpenAiPipelineConfig {
            request_context: OpenAiRequestContext {
                model: "configured-model".to_owned(),
                ..Default::default()
            },
            prefix_cache_config: OpenAiPrefixCacheConfig {
                auth_mode: crate::providers::prefix_caching::OpenAiAuthMode::OpenAi,
                base_url: Some("https://api.openai.com/v1".to_owned()),
            },
            ..Default::default()
        }
    }

    #[test]
    fn generation_config_respects_sampling_overrides_and_provider_budgets() {
        let mut config = test_config();
        config.sampling_params = Some(json!({
            "temperature":0.1,
            "max_tokens":8000,
            "max_completion_tokens":12000,
            "reasoning_effort":"high",
            "vendor_option":{"x":true}
        }));
        config.default_generation_config = json!({"topP":0.9,"temperature":0.7});
        let request = json!({"config":{"temperature":0.4,"maxOutputTokens":5000,"topP":0.8}});
        let generated = build_generate_content_config(&request, &config);
        assert_eq!(generated["temperature"], 0.1);
        assert_eq!(generated["max_tokens"], 5000);
        assert_eq!(generated["max_completion_tokens"], 5000);
        assert_eq!(generated["reasoning_effort"], "high");
        assert_eq!(generated["vendor_option"]["x"], true);
        assert!(generated.get("top_p").is_none());
    }

    #[test]
    fn provider_output_budget_suppresses_injected_max_tokens() {
        let mut config = test_config();
        config.sampling_params = Some(json!({"max_new_tokens":9000}));
        let generated =
            build_generate_content_config(&json!({"config":{"maxOutputTokens":5000}}), &config);
        assert_eq!(generated["max_new_tokens"], 5000);
        assert!(generated.get("max_tokens").is_none());
    }

    #[test]
    fn generation_config_uses_request_then_provider_defaults_and_reasoning_opt_out() {
        let mut config = test_config();
        config.default_generation_config = json!({
            "temperature":0.7,"topP":0.9,"maxOutputTokens":12000,
            "topK":40,"presencePenalty":0.2,"frequencyPenalty":0.3
        });
        config.reasoning = Some(json!({"effort":"high"}));
        let request =
            json!({"config":{"temperature":0.2,"topP":0.8,"maxOutputTokens":6000,"topK":30}});
        let generated = build_generate_content_config(&request, &config);
        assert_eq!(generated["temperature"], 0.2);
        assert_eq!(generated["top_p"], 0.8);
        assert_eq!(generated["max_tokens"], 6000);
        assert_eq!(generated["top_k"], 30);
        assert_eq!(generated["presence_penalty"], 0.2);
        assert_eq!(generated["frequency_penalty"], 0.3);
        assert_eq!(generated["reasoning"]["effort"], "high");
        let disabled = build_generate_content_config(
            &json!({"config":{"thinkingConfig":{"includeThoughts":false}}}),
            &config,
        );
        assert!(disabled.get("reasoning").is_none());
    }

    #[test]
    fn strict_response_format_validates_recursive_required_fields_and_depth() {
        let request = json!({"config":{
            "responseMimeType":"application/json",
            "responseJsonSchema":{"type":"OBJECT","properties":{"name":{"type":"STRING","minLength":2}},"required":["name"],"additionalProperties":true}
        }});
        let format = build_response_format(&request, true).unwrap();
        assert_eq!(format["type"], "json_schema");
        assert_eq!(format["json_schema"]["schema"]["type"], "object");
        assert_eq!(
            format["json_schema"]["schema"]["additionalProperties"],
            false
        );
        assert!(
            format["json_schema"]["schema"]["properties"]["name"]
                .get("minLength")
                .is_none()
        );

        let invalid = json!({"config":{
            "responseMimeType":"application/json",
            "responseSchema":{"type":"object","properties":{"name":{"type":"string"}},"required":[]}
        }});
        assert_eq!(
            build_response_format(&invalid, true).unwrap()["type"],
            "json_object"
        );
        assert!(build_response_format(&request, false).is_none());
    }

    #[test]
    fn builds_streaming_tools_cache_and_compression_request_shape() {
        let mut config = test_config();
        config.session_id = Some("session-1".to_owned());
        config.cache_key_partition = Some("agent-1".to_owned());
        config.sampling_params = Some(json!({"max_completion_tokens":10000}));
        let request = json!({
            "model":"gpt-5.6",
            "promptCacheSharing":true,
            "contents":[{"role":"user","parts":[{"text":"question"}]}],
            "config":{
                "maxOutputTokens":5000,
                "tools":[{"functionDeclarations":[{"name":"lookup","parameters":{"type":"OBJECT","properties":{"key":{"type":"STRING"}},"required":["key"]}}]}],
                "toolConfig":{"functionCallingConfig":{"mode":"ANY"}}
            }
        });
        let built = build_openai_request(&request, &config, "compress-turn", true, |request, _| {
            request
        });
        assert_eq!(built["model"], "gpt-5.6");
        assert_eq!(built["stream"], true);
        assert_eq!(built["stream_options"]["include_usage"], true);
        assert_eq!(built["max_completion_tokens"], 5000);
        assert_eq!(built["tool_choice"], "required");
        assert_eq!(built["tools"][0]["function"]["name"], "lookup");
        assert_eq!(built["prompt_cache_key"], "canopy-code:session-1:agent-1");
    }

    #[test]
    fn provider_enhancement_runs_before_official_prompt_caching() {
        let mut config = test_config();
        config.session_id = Some("session-1".to_owned());
        let request = json!({"model":"gpt-5.6","contents":["old"],"promptCacheSharing":true});
        let built = build_openai_request(&request, &config, "turn", false, |mut request, _| {
            request["provider_marker"] = json!(true);
            request
        });
        assert_eq!(built["provider_marker"], true);
        assert_eq!(built["prompt_cache_key"], "canopy-code:session-1");
    }

    #[test]
    fn applies_selected_provider_defaults_and_request_profile_after_shared_build() {
        let mut config = test_config();
        config.provider_profile = OpenAiProviderProfile::DeepSeek {
            official_hostname: true,
        };
        config.reasoning = Some(json!({"effort":"xhigh"}));
        config.extra_body = Some(json!({"vendor_flag":true}));
        let request = json!({
            "model":"deepseek-reasoner",
            "contents":[{"role":"user","parts":[{"text":"question"}]}]
        });
        let built = build_openai_request(&request, &config, "turn", false, |request, _| request);
        assert_eq!(built["temperature"], 0);
        assert_eq!(built["reasoning_effort"], "max");
        assert_eq!(built["vendor_flag"], true);
        assert_eq!(built["max_tokens"], 64_000);
    }

    #[test]
    fn pipeline_dispatches_dashscope_profile_through_its_request_builder() {
        let mut config = test_config();
        config.provider_profile = OpenAiProviderProfile::DashScope;
        config.request_context.model = "qwen3.8-max".to_owned();
        config.enable_cache_control = Some(false);
        config.session_id = Some("session-2".to_owned());
        config.channel = Some("analysis".to_owned());
        config.reasoning = Some(json!({"effort":"high"}));
        config.extra_body = Some(json!({"vendor_flag":true}));
        let request = json!({
            "model":"qwen3.8-max",
            "contents":[{"role":"user","parts":[{"text":"question"}]}]
        });
        let built = build_openai_request(&request, &config, "turn", false, |request, _| request);
        assert_eq!(built["reasoning_effort"], "high");
        assert_eq!(built["vendor_flag"], true);
        assert_eq!(built["metadata"]["sessionId"], "session-2");
        assert_eq!(built["metadata"]["channel"], "analysis");
        assert_eq!(built["preserve_thinking"], true);
        assert!(built["messages"][0]["content"].is_array());
        assert!(
            built["messages"][0]["content"][0]
                .get("cache_control")
                .is_none()
        );
    }
}
