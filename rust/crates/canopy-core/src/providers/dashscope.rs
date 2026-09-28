//! DashScope endpoint selection and message/tool cache compatibility.

use std::collections::BTreeMap;

use serde_json::{Map, Value, json};

use crate::modalities::{
    is_canopy_family_wire_model, is_glm_wire_model, is_tiered_effort_wire_model,
};
use crate::providers::openai_profiles::apply_default_output_limit;

pub const DEFAULT_DASHSCOPE_BASE_URL: &str = "https://dashscope.aliyuncs.com/compatible-mode/v1";
pub const DASHSCOPE_REGIONAL_HOSTS: &[&str] = &[
    "dashscope.aliyuncs.com",
    "dashscope-intl.aliyuncs.com",
    "dashscope-us.aliyuncs.com",
];

/// Match the source adapter's routing rules. The OAuth and missing-base-url
/// cases intentionally select DashScope before URL host matching.
pub fn is_dashscope_provider(
    auth_type: Option<&str>,
    base_url: Option<&str>,
    proxy_base_url: Option<&str>,
) -> bool {
    if auth_type == Some("canopy-oauth") || base_url.is_none() {
        return true;
    }
    let base_url = normalize_trailing_slash(base_url.unwrap_or_default());
    let hostname = reqwest::Url::parse(&base_url)
        .ok()
        .and_then(|url| url.host_str().map(str::to_ascii_lowercase));

    let is_regional = hostname.as_deref().is_some_and(|hostname| {
        DASHSCOPE_REGIONAL_HOSTS
            .iter()
            .any(|host| hostname == *host || hostname.ends_with(&format!(".{host}")))
    });
    let is_token_plan = hostname.as_deref().is_some_and(|hostname| {
        hostname.starts_with("token-plan.") && hostname.ends_with(".maas.aliyuncs.com")
    });
    let is_internal = hostname.as_deref().is_some_and(|hostname| {
        hostname.ends_with(".alibaba-inc.com") || hostname.ends_with(".aliyun-inc.com")
    });
    let is_api_gateway = hostname
        .as_deref()
        .is_some_and(|hostname| hostname.ends_with(".alicloudapi.com"));
    let proxy_match = proxy_base_url
        .is_some_and(|proxy| normalize_trailing_slash(proxy).eq_ignore_ascii_case(&base_url));

    is_regional || is_token_plan || is_internal || is_api_gateway || proxy_match
}

fn normalize_trailing_slash(value: &str) -> String {
    value.strip_suffix('/').unwrap_or(value).to_owned()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DashScopeCacheMode {
    SystemOnly,
    All,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DashScopeThinkingKnobSource {
    ExtraBody,
    SamplingParams,
    Reasoning,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DashScopeThinkingKnobSelection {
    pub source: DashScopeThinkingKnobSource,
    pub field: &'static str,
    pub value: Value,
}

/// Select the effective thinking knob using DashScope's layer precedence.
pub fn select_dashscope_thinking_knob(
    model: Option<&str>,
    extra_body: Option<&Value>,
    sampling_params: Option<&Value>,
    reasoning_effort: Option<&Value>,
) -> Option<DashScopeThinkingKnobSelection> {
    if !is_tiered_effort_wire_model(model) {
        return None;
    }
    let reasoning_selection = reasoning_effort.map(|value| DashScopeThinkingKnobSelection {
        source: DashScopeThinkingKnobSource::Reasoning,
        field: "reasoning_effort",
        value: value.clone(),
    });
    let extra = select_from_layer(DashScopeThinkingKnobSource::ExtraBody, extra_body);
    if extra.as_ref().is_some_and(|selection| {
        selection.field == "enable_thinking" && selection.value == Value::Bool(true)
    }) {
        return select_value_from_layer(
            DashScopeThinkingKnobSource::SamplingParams,
            sampling_params,
        )
        .or(reasoning_selection)
        .or(extra);
    }
    if extra.is_some() {
        return extra;
    }
    let sampling = select_from_layer(DashScopeThinkingKnobSource::SamplingParams, sampling_params);
    if sampling.as_ref().is_some_and(|selection| {
        selection.field == "enable_thinking" && selection.value == Value::Bool(true)
    }) {
        reasoning_selection.or(sampling)
    } else {
        sampling.or(reasoning_selection)
    }
}

fn select_from_layer(
    source: DashScopeThinkingKnobSource,
    layer: Option<&Value>,
) -> Option<DashScopeThinkingKnobSelection> {
    let layer = layer?.as_object()?;
    if layer.get("enable_thinking") == Some(&Value::Bool(false)) {
        return Some(DashScopeThinkingKnobSelection {
            source,
            field: "enable_thinking",
            value: Value::Bool(false),
        });
    }
    for field in ["reasoning_effort", "thinking_budget"] {
        if let Some(value) = layer.get(field).filter(|value| !value.is_null()) {
            return Some(DashScopeThinkingKnobSelection {
                source,
                field,
                value: value.clone(),
            });
        }
    }
    (layer.get("enable_thinking") == Some(&Value::Bool(true))).then(|| {
        DashScopeThinkingKnobSelection {
            source,
            field: "enable_thinking",
            value: Value::Bool(true),
        }
    })
}

fn select_value_from_layer(
    source: DashScopeThinkingKnobSource,
    layer: Option<&Value>,
) -> Option<DashScopeThinkingKnobSelection> {
    let layer = layer?.as_object()?;
    for field in ["reasoning_effort", "thinking_budget"] {
        if let Some(value) = layer.get(field).filter(|value| !value.is_null()) {
            return Some(DashScopeThinkingKnobSelection {
                source,
                field,
                value: value.clone(),
            });
        }
    }
    None
}

/// Remove null values for DashScope's thinking fields while preserving the
/// rest of a sampling/extra-body object.
pub fn without_nullish_thinking_knobs(layer: &Value) -> Value {
    let Some(layer) = layer.as_object() else {
        return layer.clone();
    };
    let mut sanitized = layer.clone();
    for field in ["enable_thinking", "reasoning_effort", "thinking_budget"] {
        if sanitized.get(field).is_some_and(Value::is_null) {
            sanitized.remove(field);
        }
    }
    Value::Object(sanitized)
}

/// Build DashScope-specific thinking extras for Canopy-family models.
pub fn build_canopy_effort_config(model: Option<&str>, effort: Option<&Value>) -> Value {
    let Some(effort) = effort else {
        return Value::Object(Map::new());
    };
    if is_tiered_effort_wire_model(model) {
        json!({"reasoning_effort":effort})
    } else if is_canopy_family_wire_model(model) {
        json!({"enable_thinking":true})
    } else {
        Value::Object(Map::new())
    }
}

/// Remove incompatible thinking knobs in-place. The returned fields let the
/// provider emit its one-time conflict warning without coupling this policy
/// to the logger.
pub fn drop_conflicting_thinking_knobs(
    model: Option<&str>,
    merged: &mut Map<String, Value>,
    selected: Option<&DashScopeThinkingKnobSelection>,
) -> Vec<String> {
    if !is_canopy_family_wire_model(model) {
        return Vec::new();
    }
    let tiered = is_tiered_effort_wire_model(model);
    if tiered
        && selected.is_some_and(|selection| {
            selection.field == "enable_thinking" && selection.value == Value::Bool(false)
        })
    {
        merged.insert(
            "reasoning_effort".to_owned(),
            Value::String("none".to_owned()),
        );
        let mut dropped = vec!["enable_thinking".to_owned()];
        if merged.contains_key("thinking_budget") {
            dropped.push("thinking_budget".to_owned());
        }
        for field in &dropped {
            merged.remove(field);
        }
        return dropped;
    }

    let Some(effort) = merged.get("reasoning_effort").and_then(Value::as_str) else {
        return Vec::new();
    };
    if tiered && effort == "none" {
        if merged.remove("thinking_budget").is_some() {
            return vec!["thinking_budget".to_owned()];
        }
        return Vec::new();
    }
    if tiered {
        if selected.is_some_and(|selection| {
            selection.field == "reasoning_effort" && merged.contains_key("enable_thinking")
        }) {
            merged.remove("enable_thinking");
            return vec!["enable_thinking".to_owned()];
        }
        return Vec::new();
    }
    if merged.contains_key("thinking_budget") {
        merged.remove("reasoning_effort");
        return vec!["reasoning_effort".to_owned()];
    }
    Vec::new()
}

/// Apply DashScope's cache markers to the first system message, optionally
/// the last history message, and the final tool declaration.
pub fn apply_dashscope_cache_control(request: &mut Value, mode: DashScopeCacheMode) {
    let Some(object) = request.as_object_mut() else {
        return;
    };
    let Some(messages) = object.get_mut("messages").and_then(Value::as_array_mut) else {
        return;
    };
    let first_system = messages
        .iter()
        .position(|message| message.get("role").and_then(Value::as_str) == Some("system"));
    let last = messages.len().checked_sub(1);
    for (index, message) in messages.iter_mut().enumerate() {
        let should_cache =
            first_system == Some(index) || (mode == DashScopeCacheMode::All && last == Some(index));
        if !should_cache {
            continue;
        }
        let Some(message) = message.as_object_mut() else {
            continue;
        };
        let Some(content) = message.get("content").filter(|content| !content.is_null()) else {
            continue;
        };
        let mut parts = match content {
            Value::String(text) => vec![json!({"type":"text","text":text})],
            Value::Array(parts) => parts.clone(),
            _ => continue,
        };
        let Some(last_part) = parts.last_mut() else {
            continue;
        };
        let mut part = match std::mem::take(last_part) {
            Value::Object(part) => part,
            _ => Map::new(),
        };
        part.insert("cache_control".to_owned(), json!({"type":"ephemeral"}));
        *last_part = Value::Object(part);
        message.insert("content".to_owned(), Value::Array(parts));
    }

    if mode == DashScopeCacheMode::All
        && let Some(tools) = object
            .get_mut("tools")
            .and_then(Value::as_array_mut)
            .filter(|tools| !tools.is_empty())
        && let Some(Value::Object(last_tool)) = tools.last_mut()
    {
        last_tool.insert("cache_control".to_owned(), json!({"type":"ephemeral"}));
    }
}

/// GLM on DashScope drops array-form messages for plain tool-less requests.
/// Flatten only non-empty arrays made entirely of text parts; any multimodal
/// part leaves the original value untouched.
pub fn flatten_glm_text_content_for_plain_requests(request: &mut Value) -> bool {
    let model = request.get("model").and_then(Value::as_str);
    if !is_glm_wire_model(model) || has_function_calling_context(request) {
        return false;
    }
    let Some(messages) = request.get_mut("messages").and_then(Value::as_array_mut) else {
        return false;
    };
    for message in messages {
        let Some(message) = message.as_object_mut() else {
            continue;
        };
        let Some(parts) = message.get("content").and_then(Value::as_array) else {
            continue;
        };
        if parts.is_empty()
            || !parts
                .iter()
                .all(|part| part.get("type").and_then(Value::as_str) == Some("text"))
        {
            continue;
        }
        let text = parts
            .iter()
            .map(|part| part.get("text").and_then(Value::as_str).unwrap_or_default())
            .collect::<Vec<_>>()
            .join("\n\n");
        message.insert("content".to_owned(), Value::String(text));
    }
    true
}

fn has_function_calling_context(request: &Value) -> bool {
    if request
        .get("tools")
        .and_then(Value::as_array)
        .is_some_and(|tools| !tools.is_empty())
    {
        return true;
    }
    request
        .get("messages")
        .and_then(Value::as_array)
        .is_some_and(|messages| {
            messages.iter().any(|message| {
                message.get("role").and_then(Value::as_str) == Some("tool")
                    || (message.get("role").and_then(Value::as_str) == Some("assistant")
                        && message
                            .get("tool_calls")
                            .and_then(Value::as_array)
                            .is_some_and(|calls| !calls.is_empty()))
            })
        })
}

/// Build the provider-specific headers. Custom entries replace defaults by
/// exact key, matching JavaScript object spread before HTTP normalization.
pub fn build_dashscope_headers(
    cli_version: Option<&str>,
    platform: &str,
    architecture: &str,
    auth_type: &str,
    custom_headers: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let user_agent = format!(
        "CanopyCode/{} ({platform}; {architecture})",
        cli_version
            .filter(|version| !version.is_empty())
            .unwrap_or("unknown")
    );
    let mut headers = BTreeMap::from([
        ("User-Agent".to_owned(), user_agent.clone()),
        ("X-DashScope-CacheControl".to_owned(), "enable".to_owned()),
        ("X-DashScope-UserAgent".to_owned(), user_agent),
        ("X-DashScope-AuthType".to_owned(), auth_type.to_owned()),
    ]);
    headers.extend(custom_headers.clone());
    headers
}

#[derive(Clone, Debug, Default)]
pub struct DashScopeRequestConfig {
    /// Fallback model used by the shared generator when a request omits one.
    pub configured_model: Option<String>,
    pub sampling_params_configured: bool,
    pub enable_cache_control: Option<bool>,
    pub extra_body: Option<Value>,
    pub session_id: Option<String>,
    pub channel: Option<String>,
    /// `reasoning.effort` from content-generator configuration.
    pub reasoning_effort: Option<Value>,
}

/// Build the DashScope wire request from an already converted OpenAI request.
/// This ports provider request mutation; authentication, retries and telemetry
/// stay in the transport/generation runtime.
pub fn build_dashscope_request(
    request: &Value,
    config: &DashScopeRequestConfig,
    user_prompt_id: &str,
) -> Value {
    let Some(source) = request.as_object() else {
        return request.clone();
    };
    let request_model = request.get("model").and_then(Value::as_str);
    let wire_model = request_model
        .or(config.configured_model.as_deref())
        .unwrap_or_default()
        .to_ascii_lowercase();
    let tiered = is_tiered_effort_wire_model(Some(&wire_model));
    let extra_body = config.extra_body.as_ref().map(|extra_body| {
        if tiered {
            without_nullish_thinking_knobs(extra_body)
        } else {
            extra_body.clone()
        }
    });

    let flatten_plain_text =
        is_glm_wire_model(request_model) && !has_function_calling_context(request);
    let mut prepared = request.clone();
    if flatten_plain_text {
        flatten_glm_text_content_for_plain_requests(&mut prepared);
    } else if config.enable_cache_control != Some(false) {
        let cache_mode = if request.get("stream").and_then(Value::as_bool) == Some(true) {
            DashScopeCacheMode::All
        } else {
            DashScopeCacheMode::SystemOnly
        };
        apply_dashscope_cache_control(&mut prepared, cache_mode);
    }

    let mut params = source.clone();
    if !config.sampling_params_configured {
        apply_default_output_limit(&mut params);
    }
    if tiered {
        params = without_nullish_thinking_knobs(&Value::Object(params))
            .as_object()
            .cloned()
            .unwrap_or_default();
    }
    let canopy_effort =
        build_canopy_effort_config(Some(&wire_model), config.reasoning_effort.as_ref());
    let mut canopy_effort = canopy_effort.as_object().cloned().unwrap_or_default();
    if params.contains_key("reasoning_effort") && canopy_effort.contains_key("reasoning_effort") {
        canopy_effort.insert(
            "reasoning_effort".to_owned(),
            params["reasoning_effort"].clone(),
        );
    }
    let selected = tiered
        .then(|| {
            select_dashscope_thinking_knob(
                Some(&wire_model),
                extra_body.as_ref(),
                Some(&Value::Object(params.clone())),
                canopy_effort.get("reasoning_effort"),
            )
        })
        .flatten();

    let mut result = params;
    if let Some(messages) = prepared.get("messages") {
        result.insert("messages".to_owned(), messages.clone());
    }
    if let Some(tools) = prepared.get("tools") {
        result.insert("tools".to_owned(), tools.clone());
    }
    let mut metadata = Map::new();
    if let Some(session_id) = &config.session_id {
        metadata.insert("sessionId".to_owned(), Value::String(session_id.clone()));
    }
    metadata.insert(
        "promptId".to_owned(),
        Value::String(user_prompt_id.to_owned()),
    );
    if let Some(channel) = &config.channel {
        if !channel.is_empty() {
            metadata.insert("channel".to_owned(), Value::String(channel.clone()));
        }
    }
    result.insert("metadata".to_owned(), Value::Object(metadata));

    let mut dashscope_extras = Map::new();
    dashscope_extras.insert("preserve_thinking".to_owned(), Value::Bool(true));
    if is_dashscope_vision_model(request_model) {
        dashscope_extras.insert("vl_high_resolution_images".to_owned(), Value::Bool(true));
    }
    dashscope_extras.extend(canopy_effort);
    result.extend(dashscope_extras);

    let has_canopy_effort =
        build_canopy_effort_config(Some(&wire_model), config.reasoning_effort.as_ref())
            .as_object()
            .is_some_and(|effort| !effort.is_empty());
    if has_canopy_effort {
        result.remove("reasoning");
    }
    if let Some(Value::Object(extra_body)) = extra_body.as_ref() {
        result.extend(extra_body.clone());
    }

    let selected_field = selected.as_ref().map(|selection| selection.field);
    if selected_field == Some("thinking_budget") {
        result.remove("reasoning_effort");
        if result.get("enable_thinking") == Some(&Value::Bool(false)) {
            result.remove("enable_thinking");
        }
    }
    if selected_field == Some("reasoning_effort") && result.contains_key("thinking_budget") {
        result.remove("thinking_budget");
    }
    drop_conflicting_thinking_knobs(Some(&wire_model), &mut result, selected.as_ref());
    Value::Object(result)
}

fn is_dashscope_vision_model(model: Option<&str>) -> bool {
    let Some(model) = model else {
        return false;
    };
    let model = model.to_ascii_lowercase();
    model == "coder-model"
        || [
            "qwen-vl",
            "qwen3-vl-plus",
            "qwen3.5-plus",
            "qwen3.6-plus",
            "qwen3.7-plus",
        ]
        .iter()
        .any(|prefix| model.starts_with(prefix))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_detection_matches_official_proxy_and_internal_domains() {
        for host in [
            "dashscope.aliyuncs.com",
            "dashscope-intl.aliyuncs.com",
            "dashscope-us.aliyuncs.com",
            "region.dashscope.aliyuncs.com",
            "token-plan.cn-shanghai.maas.aliyuncs.com",
            "private.aliyun-inc.com",
            "gw.alicloudapi.com",
        ] {
            assert!(
                is_dashscope_provider(None, Some(&format!("https://{host}/v1")), None),
                "{host}"
            );
        }
        assert!(is_dashscope_provider(
            Some("canopy-oauth"),
            Some("https://example.com"),
            None
        ));
        assert!(is_dashscope_provider(None, None, None));
        assert!(is_dashscope_provider(
            None,
            Some("https://custom.example/v1/"),
            Some("https://custom.example/v1")
        ));
        assert!(!is_dashscope_provider(
            None,
            Some("https://evil.example/dashscope.aliyuncs.com"),
            None
        ));
        assert!(!is_dashscope_provider(
            None,
            Some("https://xalicloudapi.com"),
            None
        ));
    }

    #[test]
    fn cache_control_marks_system_last_message_and_last_tool_by_mode() {
        let mut request = json!({
            "messages":[
                {"role":"system","content":"system"},
                {"role":"tool","content":"tool"},
                {"role":"user","content":"latest"}
            ],
            "tools":[{"type":"function","function":{"name":"one"}},{"type":"function","function":{"name":"two"}}]
        });
        apply_dashscope_cache_control(&mut request, DashScopeCacheMode::SystemOnly);
        assert_eq!(
            request["messages"][0]["content"][0]["cache_control"]["type"],
            "ephemeral"
        );
        assert_eq!(request["messages"][1]["content"], "tool");
        assert_eq!(request["messages"][2]["content"], "latest");
        assert!(request["tools"][1].get("cache_control").is_none());

        apply_dashscope_cache_control(&mut request, DashScopeCacheMode::All);
        assert_eq!(
            request["messages"][2]["content"][0]["cache_control"]["type"],
            "ephemeral"
        );
        assert_eq!(request["tools"][1]["cache_control"]["type"], "ephemeral");
    }

    #[test]
    fn glm_plain_text_fallback_flattens_only_text_only_toolless_requests() {
        let mut plain = json!({
            "model":"glm-5.2",
            "messages":[
                {"role":"system","content":[{"type":"text","text":"sys"}]},
                {"role":"user","content":[{"type":"text","text":"a"},{"type":"text","text":"b"}]}
            ]
        });
        assert!(flatten_glm_text_content_for_plain_requests(&mut plain));
        assert_eq!(plain["messages"][0]["content"], "sys");
        assert_eq!(plain["messages"][1]["content"], "a\n\nb");

        let mut tool_context = json!({
            "model":"glm-5.2",
            "tools":[{"type":"function"}],
            "messages":[{"role":"user","content":[{"type":"text","text":"a"}]}]
        });
        assert!(!flatten_glm_text_content_for_plain_requests(
            &mut tool_context
        ));
        assert!(tool_context["messages"][0]["content"].is_array());

        let mut multimodal = json!({
            "model":"glm-5.2",
            "messages":[{"role":"user","content":[{"type":"text","text":"a"},{"type":"image_url"}]}]
        });
        assert!(flatten_glm_text_content_for_plain_requests(&mut multimodal));
        assert!(multimodal["messages"][0]["content"].is_array());
    }

    #[test]
    fn custom_headers_override_default_header_values() {
        let headers = build_dashscope_headers(
            Some("1.2.3"),
            "darwin",
            "arm64",
            "api-key",
            &BTreeMap::from([("User-Agent".to_owned(), "custom".to_owned())]),
        );
        assert_eq!(headers["User-Agent"], "custom");
        assert_eq!(headers["X-DashScope-CacheControl"], "enable");
        assert_eq!(
            headers["X-DashScope-UserAgent"],
            "CanopyCode/1.2.3 (darwin; arm64)"
        );
    }

    #[test]
    fn thinking_knob_selection_obeys_layer_and_same_layer_precedence() {
        let model = Some("qwen3.8-max");
        let selected = select_dashscope_thinking_knob(
            model,
            Some(&json!({"enable_thinking":true})),
            Some(&json!({"thinking_budget":800})),
            Some(&json!("high")),
        )
        .unwrap();
        assert_eq!(selected.source, DashScopeThinkingKnobSource::SamplingParams);
        assert_eq!(selected.field, "thinking_budget");

        let effort = select_dashscope_thinking_knob(
            model,
            Some(&json!({"reasoning_effort":"low","thinking_budget":900})),
            None,
            Some(&json!("high")),
        )
        .unwrap();
        assert_eq!(effort.source, DashScopeThinkingKnobSource::ExtraBody);
        assert_eq!(effort.field, "reasoning_effort");
        assert_eq!(effort.value, "low");

        let disabled = select_dashscope_thinking_knob(
            model,
            Some(&json!({"enable_thinking":false})),
            Some(&json!({"reasoning_effort":"high"})),
            Some(&json!("medium")),
        )
        .unwrap();
        assert_eq!(disabled.field, "enable_thinking");
        assert_eq!(disabled.value, false);
    }

    #[test]
    fn thinking_sanitization_and_wire_conflict_resolution_match_model_family() {
        assert_eq!(
            without_nullish_thinking_knobs(&json!({
                "enable_thinking":null,
                "reasoning_effort":null,
                "thinking_budget":300,
                "other":null
            })),
            json!({"thinking_budget":300,"other":null})
        );
        assert_eq!(
            build_canopy_effort_config(Some("qwen3.8-max"), Some(&json!("high"))),
            json!({"reasoning_effort":"high"})
        );
        assert_eq!(
            build_canopy_effort_config(Some("qwen3.7-plus"), Some(&json!("high"))),
            json!({"enable_thinking":true})
        );

        let selected = DashScopeThinkingKnobSelection {
            source: DashScopeThinkingKnobSource::ExtraBody,
            field: "enable_thinking",
            value: Value::Bool(false),
        };
        let mut tiered = Map::from_iter([
            ("enable_thinking".to_owned(), Value::Bool(false)),
            ("thinking_budget".to_owned(), json!(100)),
            ("reasoning_effort".to_owned(), json!("high")),
        ]);
        assert_eq!(
            drop_conflicting_thinking_knobs(Some("qwen3.8-max"), &mut tiered, Some(&selected)),
            vec!["enable_thinking", "thinking_budget"]
        );
        assert_eq!(tiered["reasoning_effort"], "none");

        let mut legacy = Map::from_iter([
            ("thinking_budget".to_owned(), json!(100)),
            ("reasoning_effort".to_owned(), json!("high")),
        ]);
        assert_eq!(
            drop_conflicting_thinking_knobs(Some("qwen3.7-plus"), &mut legacy, None),
            vec!["reasoning_effort"]
        );
        assert_eq!(legacy["thinking_budget"], 100);
    }

    #[test]
    fn builds_streaming_vision_request_with_cache_markers_metadata_and_effort() {
        let request = json!({
            "model":"qwen3.6-plus",
            "stream":true,
            "messages":[
                {"role":"system","content":"system"},
                {"role":"user","content":"latest"}
            ],
            "tools":[{"type":"function","function":{"name":"lookup"}}],
            "reasoning":{"effort":"high"},
            "max_tokens":20000
        });
        let config = DashScopeRequestConfig {
            sampling_params_configured: true,
            extra_body: Some(json!({"preserve_thinking":false})),
            session_id: Some("session-1".to_owned()),
            channel: Some("analysis".to_owned()),
            reasoning_effort: Some(json!("medium")),
            ..Default::default()
        };
        let built = build_dashscope_request(&request, &config, "prompt-1");
        assert_eq!(built["preserve_thinking"], false);
        assert_eq!(built["enable_thinking"], true);
        assert!(built.get("reasoning").is_none());
        assert_eq!(built["vl_high_resolution_images"], true);
        assert_eq!(built["metadata"]["sessionId"], "session-1");
        assert_eq!(built["metadata"]["promptId"], "prompt-1");
        assert_eq!(built["metadata"]["channel"], "analysis");
        assert_eq!(
            built["messages"][0]["content"][0]["cache_control"]["type"],
            "ephemeral"
        );
        assert_eq!(
            built["messages"][1]["content"][0]["cache_control"]["type"],
            "ephemeral"
        );
        assert_eq!(built["tools"][0]["cache_control"]["type"], "ephemeral");
        assert_eq!(built["max_tokens"], 20_000);
    }

    #[test]
    fn tiered_explicit_disable_uses_none_and_glm_plain_request_skips_cache() {
        let tiered = build_dashscope_request(
            &json!({
                "model":"qwen3.8-max",
                "stream":false,
                "messages":[{"role":"user","content":"hi"}],
                "reasoning":{"effort":"high"}
            }),
            &DashScopeRequestConfig {
                sampling_params_configured: true,
                extra_body: Some(json!({"enable_thinking":false,"thinking_budget":100})),
                reasoning_effort: Some(json!("high")),
                ..Default::default()
            },
            "prompt-2",
        );
        assert_eq!(tiered["reasoning_effort"], "none");
        assert!(tiered.get("enable_thinking").is_none());
        assert!(tiered.get("thinking_budget").is_none());

        let glm = build_dashscope_request(
            &json!({
                "model":"glm-5.2",
                "stream":false,
                "messages":[
                    {"role":"system","content":[{"type":"text","text":"sys"}]},
                    {"role":"user","content":[{"type":"text","text":"ask"}]}
                ]
            }),
            &DashScopeRequestConfig {
                sampling_params_configured: true,
                ..Default::default()
            },
            "prompt-3",
        );
        assert_eq!(glm["messages"][0]["content"], "sys");
        assert_eq!(glm["messages"][1]["content"], "ask");
    }
}
