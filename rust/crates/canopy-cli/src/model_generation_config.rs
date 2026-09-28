//! Resolve model-scoped generation settings for the native CLI runtime.
//!
//! This follows the field-by-field precedence in `resolveGenerationConfig`:
//! a selected `modelProviders` entry wins over `model.generationConfig`, while
//! `CANOPY_CODE_API_TIMEOUT_MS` sits between provider and settings timeout.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use canopy_core::agent_runtime::AgentRuntimeConfig;
use canopy_core::providers::anthropic::AnthropicProviderConfig;
use canopy_core::providers::gemini::GeminiProviderConfig;
use canopy_core::providers::openai_compatible::OpenAiCompatibleConfig;
use canopy_core::providers::openai_request::{InputModalities, ToolResultContentFormat};
use canopy_core::providers::schema::SchemaComplianceMode;
use canopy_core::token_limits::{TokenLimitType, known_token_limit};
use serde_json::{Map, Value};

const GENERATION_CONFIG_FIELDS: &[&str] = &[
    "samplingParams",
    "timeout",
    "maxRetries",
    "retryInitialDelayMs",
    "retryMaxDelayMs",
    "retryErrorCodes",
    "enableCacheControl",
    "cacheRetention",
    "cacheRetentionByBlock",
    "forceGlobalCacheScope",
    "schemaCompliance",
    "reasoning",
    "contextWindowSize",
    "customHeaders",
    "extra_body",
    "thinkingMandatory",
    "modalities",
    "splitToolMedia",
    "toolResultContentFormat",
];
const DISABLED_REQUEST_TIMEOUT_MS: u64 = 2_147_483_647;
const MAX_SAFE_INTEGER: f64 = 9_007_199_254_740_991.0;

#[derive(Clone, Debug)]
pub(crate) struct NativeModelGenerationConfig {
    pub sampling_params: Option<Value>,
    pub reasoning: Option<Value>,
    pub retry_max_attempts: Option<usize>,
    pub retry_initial_delay_ms: Option<f64>,
    pub retry_max_delay_ms: Option<f64>,
    pub retry_error_codes: Vec<i64>,
    pub custom_headers: BTreeMap<String, String>,
    pub extra_body: Option<Value>,
    pub enable_cache_control: Option<bool>,
    pub cache_retention_1h: bool,
    pub cache_retention_by_block: BTreeMap<String, bool>,
    pub force_global_cache_scope: bool,
    pub schema_compliance: SchemaComplianceMode,
    pub context_window_size: Option<u64>,
    pub modalities: InputModalities,
    pub thinking_mandatory: bool,
    pub split_tool_media: bool,
    pub tool_result_content_format: ToolResultContentFormat,
    pub request_timeout: Option<Duration>,
}

impl NativeModelGenerationConfig {
    pub fn resolve(
        settings: &Value,
        model_provider: Option<&Value>,
        model_id: &str,
        effective_env: &HashMap<String, String>,
    ) -> Self {
        let settings_config = settings
            .pointer("/model/generationConfig")
            .and_then(Value::as_object);
        let provider_config = model_provider
            .and_then(|model| model.get("generationConfig"))
            .and_then(Value::as_object);
        let resolved = resolve_generation_fields(settings_config, provider_config);

        let mut reasoning = resolved.get("reasoning").cloned();
        if let Some(effort) = normalized_reasoning_effort(
            settings
                .pointer("/model/reasoningEffort")
                .and_then(Value::as_str),
        ) {
            if reasoning.as_ref() != Some(&Value::Bool(false)) {
                let mut value = reasoning
                    .take()
                    .filter(Value::is_object)
                    .unwrap_or_else(|| Value::Object(Map::new()));
                if let Some(object) = value.as_object_mut() {
                    object.insert("effort".to_owned(), Value::String(effort.to_owned()));
                }
                reasoning = Some(value);
            }
        }

        let context_window_size = resolved
            .get("contextWindowSize")
            .and_then(positive_integer)
            .or_else(|| known_token_limit(model_id, TokenLimitType::Input));
        let modalities = resolved
            .get("modalities")
            .and_then(parse_modalities)
            .unwrap_or_else(|| canopy_core::modalities::default_modalities(model_id));
        let custom_headers = resolved
            .get("customHeaders")
            .and_then(Value::as_object)
            .map(|headers| {
                headers
                    .iter()
                    .filter_map(|(name, value)| {
                        value.as_str().map(|value| (name.clone(), value.to_owned()))
                    })
                    .collect()
            })
            .unwrap_or_default();
        let provider_sets_timeout =
            provider_config.is_some_and(|config| config.contains_key("timeout"));
        let configured_timeout = || resolved.get("timeout").and_then(request_timeout);
        let request_timeout = if provider_sets_timeout {
            configured_timeout()
        } else if let Some(timeout) = effective_env
            .get("CANOPY_CODE_API_TIMEOUT_MS")
            .and_then(|value| parse_timeout_env(value))
        {
            Some(Duration::from_millis(if timeout == 0 {
                DISABLED_REQUEST_TIMEOUT_MS
            } else {
                timeout
            }))
        } else {
            configured_timeout()
        };
        let retry_max_attempts = resolved
            .get("maxRetries")
            .and_then(nonnegative_integer)
            .and_then(|retries| usize::try_from(retries).ok())
            .map(|retries| retries.saturating_add(1));
        let retry_error_codes = resolved
            .get("retryErrorCodes")
            .and_then(Value::as_array)
            .map(|values| values.iter().filter_map(Value::as_i64).collect())
            .unwrap_or_default();
        let cache_retention_by_block = resolved
            .get("cacheRetentionByBlock")
            .and_then(Value::as_object)
            .map(|values| {
                ["system", "tool", "user.last"]
                    .into_iter()
                    .filter_map(|anchor| {
                        values
                            .get(anchor)
                            .and_then(Value::as_str)
                            .map(|retention| (anchor.to_owned(), retention == "1h"))
                    })
                    .collect()
            })
            .unwrap_or_default();

        Self {
            sampling_params: resolved.get("samplingParams").cloned(),
            reasoning,
            retry_max_attempts,
            retry_initial_delay_ms: resolved
                .get("retryInitialDelayMs")
                .and_then(nonnegative_number),
            retry_max_delay_ms: resolved.get("retryMaxDelayMs").and_then(nonnegative_number),
            retry_error_codes,
            custom_headers,
            extra_body: resolved.get("extra_body").cloned(),
            enable_cache_control: resolved.get("enableCacheControl").and_then(Value::as_bool),
            cache_retention_1h: resolved.get("cacheRetention").and_then(Value::as_str)
                == Some("1h"),
            cache_retention_by_block,
            force_global_cache_scope: resolved
                .get("forceGlobalCacheScope")
                .and_then(Value::as_bool)
                == Some(true),
            schema_compliance: match resolved.get("schemaCompliance").and_then(Value::as_str) {
                Some("openapi_30") => SchemaComplianceMode::OpenApi30,
                _ => SchemaComplianceMode::Auto,
            },
            context_window_size,
            modalities,
            thinking_mandatory: resolved.get("thinkingMandatory").and_then(Value::as_bool)
                == Some(true),
            split_tool_media: resolved
                .get("splitToolMedia")
                .and_then(Value::as_bool)
                .unwrap_or(true),
            tool_result_content_format: match resolved
                .get("toolResultContentFormat")
                .and_then(Value::as_str)
            {
                Some("string") => ToolResultContentFormat::String,
                _ => ToolResultContentFormat::Parts,
            },
            request_timeout,
        }
    }

    pub fn apply_to_runtime(&self, config: &mut AgentRuntimeConfig) {
        config.pipeline.sampling_params = self.sampling_params.clone();
        config.pipeline.reasoning = self.reasoning.clone();
        config.pipeline.retry_max_attempts = self.retry_max_attempts;
        config.pipeline.retry_initial_delay_ms = self.retry_initial_delay_ms;
        config.pipeline.retry_max_delay_ms = self.retry_max_delay_ms;
        config.pipeline.retry_error_codes = self.retry_error_codes.clone();
        config.pipeline.extra_body = self.extra_body.clone();
        config.pipeline.enable_cache_control = self.enable_cache_control;
        config.pipeline.schema_compliance = self.schema_compliance;
        config.pipeline.thinking_mandatory = self.thinking_mandatory;
        config.pipeline.request_context.modalities = self.modalities;
        config.pipeline.request_context.split_tool_media = self.split_tool_media;
        config.pipeline.request_context.tool_result_content_format =
            self.tool_result_content_format;
        config.context_window_size = self.context_window_size;
    }

    pub fn apply_to_anthropic(&self, config: &mut AnthropicProviderConfig) {
        config.headers = self.custom_headers.clone();
        config.sampling_params = self.sampling_params.clone();
        config.reasoning = self.reasoning.clone();
        config.schema_compliance = self.schema_compliance;
        config.enable_cache_control = self.enable_cache_control.unwrap_or(true);
        config.cache_retention_1h = self.cache_retention_1h;
        config.cache_retention_by_block = self.cache_retention_by_block.clone();
        config.force_global_cache_scope = self.force_global_cache_scope;
        if let Some(timeout) = self.request_timeout {
            config.request_timeout = timeout;
        }
    }

    pub fn apply_to_openai_compatible(&self, config: &mut OpenAiCompatibleConfig) {
        config.headers = self.custom_headers.clone();
        if let Some(timeout) = self.request_timeout {
            config.request_timeout = timeout;
        }
    }

    pub fn apply_to_gemini(&self, config: &mut GeminiProviderConfig) {
        config.headers = self.custom_headers.clone();
        if let Some(timeout) = self.request_timeout {
            config.request_timeout = timeout;
        }
    }
}

pub(crate) fn resolve_generation_fields(
    settings: Option<&Map<String, Value>>,
    provider: Option<&Map<String, Value>>,
) -> Map<String, Value> {
    let mut resolved = Map::new();
    for field in GENERATION_CONFIG_FIELDS {
        if let Some(value) = provider.and_then(|config| config.get(*field)) {
            resolved.insert((*field).to_owned(), value.clone());
        } else if let Some(value) = settings.and_then(|config| config.get(*field)) {
            resolved.insert((*field).to_owned(), value.clone());
        }
    }
    resolved
}

fn positive_integer(value: &Value) -> Option<u64> {
    let number = value.as_f64()?;
    (number.is_finite()
        && number > 0.0
        && number.fract() == 0.0
        && number <= 9_007_199_254_740_991.0)
        .then_some(number as u64)
}

fn nonnegative_integer(value: &Value) -> Option<u64> {
    let number = value.as_f64()?;
    (number.is_finite() && number >= 0.0 && number.fract() == 0.0 && number <= MAX_SAFE_INTEGER)
        .then_some(number as u64)
}

fn nonnegative_number(value: &Value) -> Option<f64> {
    let number = value.as_f64()?;
    (number.is_finite() && number >= 0.0).then_some(number)
}

fn request_timeout(value: &Value) -> Option<Duration> {
    let milliseconds = value.as_f64()?;
    if !milliseconds.is_finite() {
        return None;
    }
    if milliseconds <= 0.0 {
        return Some(Duration::from_millis(DISABLED_REQUEST_TIMEOUT_MS));
    }

    let milliseconds = milliseconds.min(u64::MAX as f64);
    let whole_milliseconds = milliseconds.floor();
    let fractional_nanos = ((milliseconds - whole_milliseconds) * 1_000_000.0).round() as u64;
    Some(Duration::from_millis(whole_milliseconds as u64) + Duration::from_nanos(fractional_nanos))
}

fn parse_timeout_env(value: &str) -> Option<u64> {
    let value = value.trim();
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let milliseconds = value.parse::<u64>().ok()?;
    ((milliseconds as f64) <= MAX_SAFE_INTEGER).then_some(milliseconds)
}

fn parse_modalities(value: &Value) -> Option<InputModalities> {
    let object = value.as_object()?;
    Some(InputModalities {
        image: object
            .get("image")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        pdf: object.get("pdf").and_then(Value::as_bool).unwrap_or(false),
        audio: object
            .get("audio")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        video: object
            .get("video")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    })
}

fn normalized_reasoning_effort(value: Option<&str>) -> Option<&'static str> {
    let raw = value?.trim().to_ascii_lowercase();
    let normalized = raw.replace([' ', '_', '-'], "");
    match normalized.as_str() {
        "low" => Some("low"),
        "medium" | "med" => Some("medium"),
        "high" => Some("high"),
        "xhigh" | "extrahigh" => Some("xhigh"),
        "max" | "maximum" => Some("max"),
        _ => None,
    }
}
