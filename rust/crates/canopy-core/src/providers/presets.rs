// Copyright 2026 Canopy Team
// SPDX-License-Identifier: Apache-2.0
//!
//! Static provider preset catalog ported from `packages/core/src/providers/presets`
//! and the registry projections in `all-providers.ts`.
//!
//! The catalog and setup helpers mirror TypeScript provider presets and
//! provider-config behavior. Loading settings and persisting the resulting
//! installation state remain caller responsibilities.

use crate::providers::openai_request::InputModalities;
use crate::utils::error_parsing::AuthType;
use serde::ser::SerializeMap;
use serde::{Serialize, Serializer};
use serde_json::Number;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BaseUrlOption {
    pub id: &'static str,
    pub label: &'static str,
    pub url: &'static str,
    pub documentation_url: Option<&'static str>,
    pub api_key_url: Option<&'static str>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BaseUrlPreset {
    Fixed(&'static str),
    Options(&'static [BaseUrlOption]),
    UserProvided,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ModelPreset {
    pub id: &'static str,
    pub context_window_size: Option<u32>,
    pub enable_thinking: bool,
    pub thinking_mandatory: bool,
    pub modalities: Option<InputModalities>,
    pub description: Option<&'static str>,
    pub image_only: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ModelNamePrefixRule {
    pub base_url: &'static str,
    pub prefix: &'static str,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ModelNamePrefix {
    Fixed(&'static str),
    ByBaseUrl {
        default: &'static str,
        rules: &'static [ModelNamePrefixRule],
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UiLabels {
    pub flow_title: Option<&'static str>,
    pub base_url_step_title: Option<&'static str>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DynamicFeature {
    /// API-key environment variable is derived from protocol and entered URL.
    EnvironmentKeyDerivation,
    /// Provider provides custom `ownsModel` logic rather than the prefix rule.
    ModelOwnership,
    /// API-key validation callback executes during installation.
    ApiKeyValidation,
    /// Setup accepts user-defined model IDs without a preset model list.
    UserDefinedModels,
    /// Provider model prefix is selected by a callback in TypeScript.
    ModelNamePrefixCallback,
    /// Setup builds a model-provider settings patch from user inputs.
    InstallPlanProjection,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProviderPreset {
    pub id: &'static str,
    pub label: &'static str,
    pub description: &'static str,
    pub protocol: AuthType,
    pub protocol_options: &'static [AuthType],
    pub base_url: BaseUrlPreset,
    /// `None` is used for the custom provider because its key is derived from
    /// the selected protocol and endpoint by [`resolve_env_key`].
    pub env_key: Option<&'static str>,
    /// `None` denotes the custom provider's user-supplied model IDs.
    pub models: Option<&'static [ModelPreset]>,
    pub models_editable: bool,
    pub model_name_prefix: ModelNamePrefix,
    pub api_key_placeholder: Option<&'static str>,
    pub custom_headers: &'static [(&'static str, &'static str)],
    pub documentation_url: Option<&'static str>,
    pub ui_group: Option<&'static str>,
    pub ui_labels: Option<UiLabels>,
    pub show_advanced_config: bool,
    pub merge_models_by_identity: bool,
    pub dynamic_features: &'static [DynamicFeature],
}

/// Namespace written by TypeScript `resolveProviderState` when a provider has
/// a static model catalog.
pub const PROVIDER_METADATA_NS: &str = "providerMetadata";
pub const CUSTOM_API_KEY_ENV_PREFIX: &str = "CANOPY_CUSTOM_API_KEY_";
pub const CODING_PLAN_ENV_KEY: &str = "BAILIAN_CODING_PLAN_API_KEY";
pub const CODING_PLAN_CHINA_BASE_URL: &str = "https://coding.dashscope.aliyuncs.com/v1";
pub const CODING_PLAN_GLOBAL_BASE_URL: &str = "https://coding-intl.dashscope.aliyuncs.com/v1";
pub const TOKEN_PLAN_ENV_KEY: &str = "BAILIAN_TOKEN_PLAN_API_KEY";
pub const TOKEN_PLAN_CHINA_BASE_URL: &str =
    "https://token-plan.cn-beijing.maas.aliyuncs.com/compatible-mode/v1";
pub const TOKEN_PLAN_GLOBAL_BASE_URL: &str =
    "https://token-plan.ap-southeast-1.maas.aliyuncs.com/compatible-mode/v1";
/// The legacy/default Token Plan endpoint remains the China endpoint.
pub const TOKEN_PLAN_BASE_URL: &str = TOKEN_PLAN_CHINA_BASE_URL;
pub const GROK_ENV_KEY: &str = "XAI_API_KEY";
pub const GROK_BASE_URL: &str = "https://api.x.ai/v1";
pub const OPENROUTER_ENV_KEY: &str = "OPENROUTER_API_KEY";
pub const OPENROUTER_BASE_URL: &str = "https://openrouter.ai/api/v1";
pub const REQUESTY_ENV_KEY: &str = "REQUESTY_API_KEY";
pub const REQUESTY_BASE_URL: &str = "https://router.requesty.ai/v1";
pub const CODING_PLAN_INVALID_API_KEY_MESSAGE: &str =
    "Invalid API key. Coding Plan API keys start with \"sk-sp-\". Please check.";

/// The subset of a saved model record used by provider ownership callbacks.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ProviderModelIdentity<'a> {
    pub id: &'a str,
    pub name: Option<&'a str>,
    pub base_url: Option<&'a str>,
    pub env_key: Option<&'a str>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderSetupStep {
    Protocol,
    BaseUrl,
    ApiKey,
    Models,
    AdvancedConfig,
}

/// An ordered JSON object used for custom headers so the SHA-256 version
/// generated for provider metadata follows JavaScript object insertion order.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct OrderedHeaders(pub Vec<(String, String)>);

impl Serialize for OrderedHeaders {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (key, value) in &self.0 {
            map.serialize_entry(key, value)?;
        }
        map.end()
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub struct ConfiguredModalities {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pdf: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audio: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub video: Option<bool>,
}

impl ConfiguredModalities {
    fn has_enabled_modality(self) -> bool {
        self.image == Some(true)
            || self.pdf == Some(true)
            || self.audio == Some(true)
            || self.video == Some(true)
    }

    fn from_preset(modalities: InputModalities) -> Self {
        Self {
            image: modalities.image.then_some(true),
            pdf: modalities.pdf.then_some(true),
            audio: modalities.audio.then_some(true),
            video: modalities.video.then_some(true),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct ProviderGenerationConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extra_body: Option<EnableThinkingBody>,
    #[serde(rename = "thinkingMandatory", skip_serializing_if = "Option::is_none")]
    pub thinking_mandatory: Option<bool>,
    #[serde(rename = "contextWindowSize", skip_serializing_if = "Option::is_none")]
    pub context_window_size: Option<Number>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub modalities: Option<ConfiguredModalities>,
    #[serde(rename = "samplingParams", skip_serializing_if = "Option::is_none")]
    pub sampling_params: Option<SamplingParams>,
    #[serde(rename = "customHeaders", skip_serializing_if = "Option::is_none")]
    pub custom_headers: Option<OrderedHeaders>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct EnableThinkingBody {
    pub enable_thinking: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct SamplingParams {
    pub max_tokens: Number,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct ProviderModelConfig {
    pub id: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(rename = "baseUrl", skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    #[serde(rename = "envKey", skip_serializing_if = "Option::is_none")]
    pub env_key: Option<String>,
    #[serde(rename = "imageOnly", skip_serializing_if = "Option::is_none")]
    pub image_only: Option<bool>,
    #[serde(rename = "generationConfig", skip_serializing_if = "Option::is_none")]
    pub generation_config: Option<ProviderGenerationConfig>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct AdvancedProviderConfig {
    pub enable_thinking: bool,
    pub multimodal: Option<ConfiguredModalities>,
    pub context_window_size: Option<f64>,
    pub max_tokens: Option<f64>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ProviderSetupInputs {
    pub protocol: Option<AuthType>,
    pub base_url: String,
    pub api_key: String,
    pub model_ids: Vec<String>,
    pub prebuilt_models: Option<Vec<ProviderModelConfig>>,
    pub advanced_config: Option<AdvancedProviderConfig>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderModelSelection {
    pub model_id: String,
    pub base_url: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ProviderModelProvidersPatch {
    pub auth_type: AuthType,
    pub models: Vec<ProviderModelConfig>,
    /// TypeScript always uses `prepend-and-remove-owned` for this flow.
    pub merge_strategy: &'static str,
    /// When present, this preset's ownership callback should be applied.
    pub owns_model_preset_id: Option<&'static str>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ProviderInstallPlan {
    pub provider_id: &'static str,
    pub auth_type: AuthType,
    pub env: BTreeMap<String, String>,
    pub model_selection: Option<ProviderModelSelection>,
    pub model_providers: Vec<ProviderModelProvidersPatch>,
    pub provider_state: Option<BTreeMap<String, BTreeMap<String, String>>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProviderInstallPlanError {
    NoModelsConfigured(&'static str),
    InvalidMetadataKey(String),
    EnvironmentKeyUnavailable(&'static str),
}

impl ProviderModelProvidersPatch {
    pub fn owns_model(&self, model: ProviderModelIdentity<'_>) -> Option<bool> {
        self.owns_model_preset_id
            .and_then(find_provider_by_id)
            .map(|preset| owns_model(preset, model))
    }
}

const OPENAI_PROTOCOL: AuthType = AuthType::OpenAi;
const OPENAI_PROTOCOL_ONLY: &[AuthType] = &[AuthType::OpenAi];
const CUSTOM_PROTOCOL_OPTIONS: &[AuthType] =
    &[AuthType::OpenAi, AuthType::Anthropic, AuthType::Gemini];
const ALIBABA_UI_LABELS: UiLabels = UiLabels {
    flow_title: Some("Alibaba ModelStudio"),
    base_url_step_title: Some("Region"),
};

const EMPTY_HEADERS: &[(&str, &str)] = &[];
const HEADER_PAIR_OPENROUTER: &[(&str, &str)] = &[
    ("HTTP-Referer", "https://github.com/QwenLM/canopy-code.git"),
    ("X-OpenRouter-Title", "Canopy Code"),
];
const HEADER_PAIR_REQUESTY: &[(&str, &str)] = &[
    ("HTTP-Referer", "https://github.com/QwenLM/canopy-code.git"),
    ("X-Title", "Canopy Code"),
];

const CODING_PLAN_CHINA_URL: &str = CODING_PLAN_CHINA_BASE_URL;
const CODING_PLAN_GLOBAL_URL: &str = CODING_PLAN_GLOBAL_BASE_URL;
const TOKEN_PLAN_CHINA_URL: &str = TOKEN_PLAN_CHINA_BASE_URL;
const TOKEN_PLAN_GLOBAL_URL: &str = TOKEN_PLAN_GLOBAL_BASE_URL;

const CODING_PLAN_BASE_URLS: &[BaseUrlOption] = &[
    BaseUrlOption {
        id: "aliyun",
        label: "China (Beijing)",
        url: CODING_PLAN_CHINA_URL,
        documentation_url: Some("https://help.aliyun.com/zh/model-studio/coding-plan"),
        api_key_url: None,
    },
    BaseUrlOption {
        id: "alibabacloud",
        label: "Singapore (International)",
        url: CODING_PLAN_GLOBAL_URL,
        documentation_url: Some("https://www.alibabacloud.com/help/en/model-studio/coding-plan"),
        api_key_url: None,
    },
];

const TOKEN_PLAN_BASE_URLS: &[BaseUrlOption] = &[
    BaseUrlOption {
        id: "cn-beijing",
        label: "China (Beijing)",
        url: TOKEN_PLAN_CHINA_URL,
        documentation_url: Some(
            "https://bailian.console.aliyun.com/cn-beijing?tab=doc#/doc/?type=model&url=3028856",
        ),
        api_key_url: None,
    },
    BaseUrlOption {
        id: "ap-southeast-1",
        label: "Singapore (International)",
        url: TOKEN_PLAN_GLOBAL_URL,
        documentation_url: Some(
            "https://modelstudio.console.alibabacloud.com/ap-southeast-1?tab=doc#/doc/?type=model",
        ),
        api_key_url: None,
    },
];

const ALIBABA_STANDARD_BASE_URLS: &[BaseUrlOption] = &[
    BaseUrlOption {
        id: "cn-beijing",
        label: "China (Beijing)",
        url: "https://dashscope.aliyuncs.com/compatible-mode/v1",
        documentation_url: Some("https://bailian.console.aliyun.com/cn-beijing?tab=api#/api"),
        api_key_url: None,
    },
    BaseUrlOption {
        id: "sg-singapore",
        label: "Singapore",
        url: "https://dashscope-intl.aliyuncs.com/compatible-mode/v1",
        documentation_url: Some(
            "https://modelstudio.console.alibabacloud.com/ap-southeast-1?tab=api#/api/?type=model&url=2712195",
        ),
        api_key_url: None,
    },
    BaseUrlOption {
        id: "us-virginia",
        label: "US (Virginia)",
        url: "https://dashscope-us.aliyuncs.com/compatible-mode/v1",
        documentation_url: Some(
            "https://modelstudio.console.alibabacloud.com/us-east-1?tab=api#/api/?type=model&url=2712195",
        ),
        api_key_url: None,
    },
    BaseUrlOption {
        id: "cn-hongkong",
        label: "China (Hong Kong)",
        url: "https://cn-hongkong.dashscope.aliyuncs.com/compatible-mode/v1",
        documentation_url: Some(
            "https://modelstudio.console.alibabacloud.com/cn-hongkong?tab=api#/api/?type=model&url=2712195",
        ),
        api_key_url: None,
    },
];

const MINIMAX_BASE_URLS: &[BaseUrlOption] = &[
    BaseUrlOption {
        id: "international",
        label: "International",
        url: "https://api.minimax.io/v1",
        documentation_url: Some("https://www.minimax.io/platform"),
        api_key_url: None,
    },
    BaseUrlOption {
        id: "china",
        label: "China",
        url: "https://api.minimaxi.com/v1",
        documentation_url: Some("https://platform.minimaxi.com"),
        api_key_url: None,
    },
];

const ZAI_BASE_URLS: &[BaseUrlOption] = &[
    BaseUrlOption {
        id: "standard-api-key",
        label: "Standard API Key",
        url: "https://api.z.ai/api/paas/v4",
        documentation_url: Some("https://docs.z.ai/"),
        api_key_url: None,
    },
    BaseUrlOption {
        id: "coding-plan",
        label: "Coding Plan",
        url: "https://api.z.ai/api/coding/paas/v4",
        documentation_url: Some("https://docs.z.ai/"),
        api_key_url: None,
    },
];

const CODING_PLAN_PREFIX_RULES: &[ModelNamePrefixRule] = &[ModelNamePrefixRule {
    base_url: CODING_PLAN_GLOBAL_URL,
    prefix: "ModelStudio Coding Plan for Global/Intl",
}];
const TOKEN_PLAN_PREFIX_RULES: &[ModelNamePrefixRule] = &[ModelNamePrefixRule {
    base_url: TOKEN_PLAN_GLOBAL_URL,
    prefix: "ModelStudio Token Plan for Global/Intl",
}];

const fn modalities(image: bool, pdf: bool, audio: bool, video: bool) -> Option<InputModalities> {
    Some(InputModalities {
        image,
        pdf,
        audio,
        video,
    })
}

const CODING_PLAN_MODELS: &[ModelPreset] = &[
    ModelPreset {
        id: "qwen3.5-plus",
        context_window_size: Some(1_000_000),
        enable_thinking: true,
        thinking_mandatory: false,
        modalities: modalities(true, false, false, true),
        description: None,
        image_only: false,
    },
    ModelPreset {
        id: "qwen3.6-plus",
        context_window_size: Some(1_000_000),
        enable_thinking: true,
        thinking_mandatory: false,
        modalities: modalities(true, false, false, true),
        description: Some("Currently available to Pro subscribers only."),
        image_only: false,
    },
    ModelPreset {
        id: "qwen3.7-plus",
        context_window_size: Some(1_000_000),
        enable_thinking: true,
        thinking_mandatory: false,
        modalities: None,
        description: None,
        image_only: false,
    },
    ModelPreset {
        id: "glm-5",
        context_window_size: Some(202_752),
        enable_thinking: true,
        thinking_mandatory: false,
        modalities: None,
        description: None,
        image_only: false,
    },
    ModelPreset {
        id: "kimi-k2.5",
        context_window_size: Some(262_144),
        enable_thinking: true,
        thinking_mandatory: false,
        modalities: modalities(true, false, false, true),
        description: None,
        image_only: false,
    },
    ModelPreset {
        id: "MiniMax-M2.5",
        context_window_size: Some(196_608),
        enable_thinking: true,
        thinking_mandatory: false,
        modalities: None,
        description: None,
        image_only: false,
    },
    ModelPreset {
        id: "qwen3-coder-plus",
        context_window_size: Some(1_000_000),
        enable_thinking: false,
        thinking_mandatory: false,
        modalities: None,
        description: None,
        image_only: false,
    },
    ModelPreset {
        id: "qwen3-coder-next",
        context_window_size: Some(262_144),
        enable_thinking: false,
        thinking_mandatory: false,
        modalities: None,
        description: None,
        image_only: false,
    },
    ModelPreset {
        id: "qwen3-max-2026-01-23",
        context_window_size: Some(262_144),
        enable_thinking: true,
        thinking_mandatory: false,
        modalities: None,
        description: None,
        image_only: false,
    },
    ModelPreset {
        id: "glm-4.7",
        context_window_size: Some(202_752),
        enable_thinking: true,
        thinking_mandatory: false,
        modalities: None,
        description: None,
        image_only: false,
    },
];

const TOKEN_PLAN_MODELS: &[ModelPreset] = &[
    ModelPreset {
        id: "qwen3.7-plus",
        context_window_size: Some(1_000_000),
        enable_thinking: true,
        thinking_mandatory: false,
        modalities: modalities(true, false, false, true),
        description: None,
        image_only: false,
    },
    ModelPreset {
        id: "qwen3.6-plus",
        context_window_size: Some(1_000_000),
        enable_thinking: true,
        thinking_mandatory: false,
        modalities: modalities(true, false, false, true),
        description: None,
        image_only: false,
    },
    ModelPreset {
        id: "qwen3.7-max",
        context_window_size: Some(1_000_000),
        enable_thinking: true,
        thinking_mandatory: false,
        modalities: None,
        description: None,
        image_only: false,
    },
    ModelPreset {
        id: "qwen3.8-max-preview",
        context_window_size: Some(1_000_000),
        enable_thinking: true,
        thinking_mandatory: true,
        modalities: modalities(true, false, false, true),
        description: None,
        image_only: false,
    },
    ModelPreset {
        id: "qwen3.6-flash",
        context_window_size: Some(1_000_000),
        enable_thinking: true,
        thinking_mandatory: false,
        modalities: None,
        description: None,
        image_only: false,
    },
    ModelPreset {
        id: "deepseek-v4-pro",
        context_window_size: Some(1_000_000),
        enable_thinking: false,
        thinking_mandatory: false,
        modalities: None,
        description: None,
        image_only: false,
    },
    ModelPreset {
        id: "deepseek-v4-flash-0731",
        context_window_size: Some(1_000_000),
        enable_thinking: false,
        thinking_mandatory: false,
        modalities: None,
        description: None,
        image_only: false,
    },
    ModelPreset {
        id: "deepseek-v3.2",
        context_window_size: Some(131_072),
        enable_thinking: false,
        thinking_mandatory: false,
        modalities: None,
        description: None,
        image_only: false,
    },
    ModelPreset {
        id: "kimi-k2.7-code",
        context_window_size: Some(262_144),
        enable_thinking: true,
        thinking_mandatory: false,
        modalities: modalities(true, false, false, true),
        description: None,
        image_only: false,
    },
    ModelPreset {
        id: "kimi-k2.6",
        context_window_size: Some(262_144),
        enable_thinking: true,
        thinking_mandatory: false,
        modalities: None,
        description: None,
        image_only: false,
    },
    ModelPreset {
        id: "kimi-k2.5",
        context_window_size: Some(262_144),
        enable_thinking: true,
        thinking_mandatory: false,
        modalities: modalities(true, false, false, true),
        description: None,
        image_only: false,
    },
    ModelPreset {
        id: "glm-5.2",
        context_window_size: Some(1_000_000),
        enable_thinking: true,
        thinking_mandatory: false,
        modalities: None,
        description: None,
        image_only: false,
    },
    ModelPreset {
        id: "glm-5.1",
        context_window_size: Some(202_752),
        enable_thinking: true,
        thinking_mandatory: false,
        modalities: None,
        description: None,
        image_only: false,
    },
    ModelPreset {
        id: "glm-5",
        context_window_size: Some(202_752),
        enable_thinking: true,
        thinking_mandatory: false,
        modalities: None,
        description: None,
        image_only: false,
    },
    ModelPreset {
        id: "MiniMax-M2.5",
        context_window_size: Some(196_608),
        enable_thinking: false,
        thinking_mandatory: false,
        modalities: None,
        description: None,
        image_only: false,
    },
];

const ALIBABA_STANDARD_MODELS: &[ModelPreset] = &[
    ModelPreset {
        id: "qwen3.6-plus",
        context_window_size: Some(1_000_000),
        enable_thinking: true,
        thinking_mandatory: false,
        modalities: None,
        description: None,
        image_only: false,
    },
    ModelPreset {
        id: "qwen3.7-plus",
        context_window_size: Some(1_000_000),
        enable_thinking: true,
        thinking_mandatory: false,
        modalities: None,
        description: None,
        image_only: false,
    },
    ModelPreset {
        id: "qwen3.7-max",
        context_window_size: Some(1_000_000),
        enable_thinking: true,
        thinking_mandatory: false,
        modalities: None,
        description: None,
        image_only: false,
    },
    ModelPreset {
        id: "glm-5.1",
        context_window_size: Some(202_752),
        enable_thinking: true,
        thinking_mandatory: false,
        modalities: None,
        description: None,
        image_only: false,
    },
    ModelPreset {
        id: "deepseek-v4-pro",
        context_window_size: Some(1_000_000),
        enable_thinking: true,
        thinking_mandatory: false,
        modalities: None,
        description: None,
        image_only: false,
    },
    ModelPreset {
        id: "deepseek-v4-flash",
        context_window_size: Some(1_000_000),
        enable_thinking: false,
        thinking_mandatory: false,
        modalities: None,
        description: None,
        image_only: false,
    },
];

const DEEPSEEK_MODELS: &[ModelPreset] = &[
    ModelPreset {
        id: "deepseek-v4-pro",
        context_window_size: Some(1_000_000),
        enable_thinking: true,
        thinking_mandatory: false,
        modalities: None,
        description: None,
        image_only: false,
    },
    ModelPreset {
        id: "deepseek-v4-flash",
        context_window_size: Some(1_000_000),
        enable_thinking: false,
        thinking_mandatory: false,
        modalities: None,
        description: None,
        image_only: false,
    },
];

const GROK_MODELS: &[ModelPreset] = &[
    ModelPreset {
        id: "grok-4.5",
        context_window_size: Some(500_000),
        enable_thinking: false,
        thinking_mandatory: false,
        modalities: None,
        description: None,
        image_only: false,
    },
    ModelPreset {
        id: "grok-4.3",
        context_window_size: Some(1_000_000),
        enable_thinking: false,
        thinking_mandatory: false,
        modalities: None,
        description: None,
        image_only: false,
    },
    ModelPreset {
        id: "grok-4.20-0309-reasoning",
        context_window_size: Some(1_000_000),
        enable_thinking: false,
        thinking_mandatory: false,
        modalities: None,
        description: None,
        image_only: false,
    },
    ModelPreset {
        id: "grok-4.20-0309-non-reasoning",
        context_window_size: Some(1_000_000),
        enable_thinking: false,
        thinking_mandatory: false,
        modalities: None,
        description: None,
        image_only: false,
    },
    ModelPreset {
        id: "grok-4.20-multi-agent-0309",
        context_window_size: Some(1_000_000),
        enable_thinking: false,
        thinking_mandatory: false,
        modalities: None,
        description: None,
        image_only: false,
    },
    ModelPreset {
        id: "grok-build-0.1",
        context_window_size: Some(262_144),
        enable_thinking: false,
        thinking_mandatory: false,
        modalities: None,
        description: None,
        image_only: false,
    },
];

const MINIMAX_MODELS: &[ModelPreset] = &[
    ModelPreset {
        id: "MiniMax-M3",
        context_window_size: Some(1_000_000),
        enable_thinking: false,
        thinking_mandatory: false,
        modalities: modalities(true, false, false, true),
        description: None,
        image_only: false,
    },
    ModelPreset {
        id: "MiniMax-M2.7",
        context_window_size: Some(204_800),
        enable_thinking: false,
        thinking_mandatory: false,
        modalities: None,
        description: None,
        image_only: false,
    },
    ModelPreset {
        id: "MiniMax-M2.7-highspeed",
        context_window_size: Some(204_800),
        enable_thinking: false,
        thinking_mandatory: false,
        modalities: None,
        description: None,
        image_only: false,
    },
    ModelPreset {
        id: "MiniMax-M2.5",
        context_window_size: Some(196_608),
        enable_thinking: false,
        thinking_mandatory: false,
        modalities: None,
        description: None,
        image_only: false,
    },
    ModelPreset {
        id: "MiniMax-M2.5-highspeed",
        context_window_size: Some(196_608),
        enable_thinking: false,
        thinking_mandatory: false,
        modalities: None,
        description: None,
        image_only: false,
    },
];

const ZAI_MODELS: &[ModelPreset] = &[
    ModelPreset {
        id: "GLM-5.2",
        context_window_size: Some(1_000_000),
        enable_thinking: true,
        thinking_mandatory: false,
        modalities: None,
        description: None,
        image_only: false,
    },
    ModelPreset {
        id: "GLM-5.1",
        context_window_size: Some(204_800),
        enable_thinking: true,
        thinking_mandatory: false,
        modalities: None,
        description: None,
        image_only: false,
    },
    ModelPreset {
        id: "GLM-5",
        context_window_size: Some(204_800),
        enable_thinking: false,
        thinking_mandatory: false,
        modalities: None,
        description: None,
        image_only: false,
    },
    ModelPreset {
        id: "GLM-5-Turbo",
        context_window_size: Some(204_800),
        enable_thinking: false,
        thinking_mandatory: false,
        modalities: None,
        description: None,
        image_only: false,
    },
];

const IDEALAB_MODELS: &[ModelPreset] = &[
    ModelPreset {
        id: "Canopy3.6-Plus-DogFooding",
        context_window_size: Some(1_000_000),
        enable_thinking: true,
        thinking_mandatory: false,
        modalities: modalities(true, false, false, true),
        description: None,
        image_only: false,
    },
    ModelPreset {
        id: "bailian/deepseek-v4-pro",
        context_window_size: Some(1_000_000),
        enable_thinking: true,
        thinking_mandatory: false,
        modalities: None,
        description: None,
        image_only: false,
    },
    ModelPreset {
        id: "bailian/deepseek-v4-flash",
        context_window_size: Some(1_000_000),
        enable_thinking: true,
        thinking_mandatory: false,
        modalities: None,
        description: None,
        image_only: false,
    },
    ModelPreset {
        id: "bailian/kimi-k2.6",
        context_window_size: Some(262_144),
        enable_thinking: true,
        thinking_mandatory: false,
        modalities: None,
        description: None,
        image_only: false,
    },
];

const MODELSCOPE_MODELS: &[ModelPreset] = &[
    ModelPreset {
        id: "deepseek-ai/DeepSeek-V4-Flash",
        context_window_size: Some(1_000_000),
        enable_thinking: true,
        thinking_mandatory: false,
        modalities: None,
        description: None,
        image_only: false,
    },
    ModelPreset {
        id: "Canopy/Canopy3.5-397B-A17B",
        context_window_size: Some(1_000_000),
        enable_thinking: true,
        thinking_mandatory: false,
        modalities: None,
        description: None,
        image_only: false,
    },
    ModelPreset {
        id: "ZhipuAI/GLM-5.1",
        context_window_size: Some(1_000_000),
        enable_thinking: true,
        thinking_mandatory: false,
        modalities: None,
        description: None,
        image_only: false,
    },
];

const OPENROUTER_MODELS: &[ModelPreset] = &[
    ModelPreset {
        id: "z-ai/glm-4.5-air:free",
        context_window_size: Some(128_000),
        enable_thinking: false,
        thinking_mandatory: false,
        modalities: None,
        description: None,
        image_only: false,
    },
    ModelPreset {
        id: "openai/gpt-oss-120b:free",
        context_window_size: Some(131_072),
        enable_thinking: false,
        thinking_mandatory: false,
        modalities: None,
        description: None,
        image_only: false,
    },
];

const REQUESTY_MODELS: &[ModelPreset] = &[
    ModelPreset {
        id: "openai/gpt-4o-mini",
        context_window_size: Some(128_000),
        enable_thinking: false,
        thinking_mandatory: false,
        modalities: None,
        description: None,
        image_only: false,
    },
    ModelPreset {
        id: "openai/gpt-4o",
        context_window_size: Some(128_000),
        enable_thinking: false,
        thinking_mandatory: false,
        modalities: None,
        description: None,
        image_only: false,
    },
];

const CODING_PLAN_DYNAMIC: &[DynamicFeature] = &[
    DynamicFeature::ModelOwnership,
    DynamicFeature::ApiKeyValidation,
    DynamicFeature::ModelNamePrefixCallback,
    DynamicFeature::InstallPlanProjection,
];
const TOKEN_PLAN_DYNAMIC: &[DynamicFeature] = &[
    DynamicFeature::ModelOwnership,
    DynamicFeature::ModelNamePrefixCallback,
    DynamicFeature::InstallPlanProjection,
];
const OWNERSHIP_AND_INSTALL_DYNAMIC: &[DynamicFeature] = &[
    DynamicFeature::ModelOwnership,
    DynamicFeature::InstallPlanProjection,
];
const CUSTOM_PROVIDER_DYNAMIC: &[DynamicFeature] = &[
    DynamicFeature::EnvironmentKeyDerivation,
    DynamicFeature::ModelOwnership,
    DynamicFeature::UserDefinedModels,
    DynamicFeature::InstallPlanProjection,
];
const STATIC_INSTALL_PROJECTION: &[DynamicFeature] = &[DynamicFeature::InstallPlanProjection];

/// Presets are in the same display order as TypeScript `ALL_PROVIDERS`.
pub const PROVIDER_PRESETS: &[ProviderPreset] = &[
    ProviderPreset {
        id: "coding-plan",
        label: "Coding Plan",
        description: "For individual developers · Weekly quota included",
        protocol: OPENAI_PROTOCOL,
        protocol_options: OPENAI_PROTOCOL_ONLY,
        base_url: BaseUrlPreset::Options(CODING_PLAN_BASE_URLS),
        env_key: Some(CODING_PLAN_ENV_KEY),
        models: Some(CODING_PLAN_MODELS),
        models_editable: true,
        model_name_prefix: ModelNamePrefix::ByBaseUrl {
            default: "ModelStudio Coding Plan",
            rules: CODING_PLAN_PREFIX_RULES,
        },
        api_key_placeholder: Some("sk-sp-..."),
        custom_headers: EMPTY_HEADERS,
        documentation_url: None,
        ui_group: Some("alibaba"),
        ui_labels: Some(ALIBABA_UI_LABELS),
        show_advanced_config: false,
        merge_models_by_identity: false,
        dynamic_features: CODING_PLAN_DYNAMIC,
    },
    ProviderPreset {
        id: "token-plan",
        label: "Token Plan",
        description: "For teams and companies · Usage-based billing with dedicated endpoint",
        protocol: OPENAI_PROTOCOL,
        protocol_options: OPENAI_PROTOCOL_ONLY,
        base_url: BaseUrlPreset::Options(TOKEN_PLAN_BASE_URLS),
        env_key: Some(TOKEN_PLAN_ENV_KEY),
        models: Some(TOKEN_PLAN_MODELS),
        models_editable: true,
        model_name_prefix: ModelNamePrefix::ByBaseUrl {
            default: "ModelStudio Token Plan",
            rules: TOKEN_PLAN_PREFIX_RULES,
        },
        api_key_placeholder: None,
        custom_headers: EMPTY_HEADERS,
        documentation_url: None,
        ui_group: Some("alibaba"),
        ui_labels: Some(ALIBABA_UI_LABELS),
        show_advanced_config: false,
        merge_models_by_identity: false,
        dynamic_features: TOKEN_PLAN_DYNAMIC,
    },
    ProviderPreset {
        id: "alibabaStandard",
        label: "Standard API Key",
        description: "Connect with an existing ModelStudio API key",
        protocol: OPENAI_PROTOCOL,
        protocol_options: OPENAI_PROTOCOL_ONLY,
        base_url: BaseUrlPreset::Options(ALIBABA_STANDARD_BASE_URLS),
        env_key: Some("DASHSCOPE_API_KEY"),
        models: Some(ALIBABA_STANDARD_MODELS),
        models_editable: true,
        model_name_prefix: ModelNamePrefix::Fixed("ModelStudio Standard"),
        api_key_placeholder: None,
        custom_headers: EMPTY_HEADERS,
        documentation_url: None,
        ui_group: Some("alibaba"),
        ui_labels: Some(ALIBABA_UI_LABELS),
        show_advanced_config: false,
        merge_models_by_identity: false,
        dynamic_features: STATIC_INSTALL_PROJECTION,
    },
    ProviderPreset {
        id: "deepseek",
        label: "DeepSeek API Key",
        description: "Quick setup for DeepSeek (deepseek-v4-flash, deepseek-v4-pro)",
        protocol: OPENAI_PROTOCOL,
        protocol_options: OPENAI_PROTOCOL_ONLY,
        base_url: BaseUrlPreset::Fixed("https://api.deepseek.com"),
        env_key: Some("DEEPSEEK_API_KEY"),
        models: Some(DEEPSEEK_MODELS),
        models_editable: true,
        model_name_prefix: ModelNamePrefix::Fixed("DeepSeek"),
        api_key_placeholder: None,
        custom_headers: EMPTY_HEADERS,
        documentation_url: Some("https://api-docs.deepseek.com/zh-cn/"),
        ui_group: Some("third-party"),
        ui_labels: None,
        show_advanced_config: false,
        merge_models_by_identity: false,
        dynamic_features: STATIC_INSTALL_PROJECTION,
    },
    ProviderPreset {
        id: "grok",
        label: "Grok (xAI) API Key",
        description: "Quick setup for xAI Grok chat & code models",
        protocol: OPENAI_PROTOCOL,
        protocol_options: OPENAI_PROTOCOL_ONLY,
        base_url: BaseUrlPreset::Fixed(GROK_BASE_URL),
        env_key: Some(GROK_ENV_KEY),
        models: Some(GROK_MODELS),
        models_editable: true,
        model_name_prefix: ModelNamePrefix::Fixed("Grok"),
        api_key_placeholder: None,
        custom_headers: EMPTY_HEADERS,
        documentation_url: Some("https://docs.x.ai/docs"),
        ui_group: Some("third-party"),
        ui_labels: None,
        show_advanced_config: false,
        merge_models_by_identity: false,
        dynamic_features: STATIC_INSTALL_PROJECTION,
    },
    ProviderPreset {
        id: "minimax",
        label: "MiniMax API Key",
        description: "Quick setup for MiniMax models",
        protocol: OPENAI_PROTOCOL,
        protocol_options: OPENAI_PROTOCOL_ONLY,
        base_url: BaseUrlPreset::Options(MINIMAX_BASE_URLS),
        env_key: Some("MINIMAX_API_KEY"),
        models: Some(MINIMAX_MODELS),
        models_editable: true,
        model_name_prefix: ModelNamePrefix::Fixed("MiniMax"),
        api_key_placeholder: None,
        custom_headers: EMPTY_HEADERS,
        documentation_url: None,
        ui_group: Some("third-party"),
        ui_labels: None,
        show_advanced_config: false,
        merge_models_by_identity: false,
        dynamic_features: STATIC_INSTALL_PROJECTION,
    },
    ProviderPreset {
        id: "zai",
        label: "Z.AI API Key",
        description: "Quick setup for Z.AI models",
        protocol: OPENAI_PROTOCOL,
        protocol_options: OPENAI_PROTOCOL_ONLY,
        base_url: BaseUrlPreset::Options(ZAI_BASE_URLS),
        env_key: Some("ZAI_API_KEY"),
        models: Some(ZAI_MODELS),
        models_editable: true,
        model_name_prefix: ModelNamePrefix::Fixed("Z.AI"),
        api_key_placeholder: None,
        custom_headers: EMPTY_HEADERS,
        documentation_url: None,
        ui_group: Some("third-party"),
        ui_labels: None,
        show_advanced_config: false,
        merge_models_by_identity: false,
        dynamic_features: STATIC_INSTALL_PROJECTION,
    },
    ProviderPreset {
        id: "idealab",
        label: "Idealab API Key",
        description: "Alibaba internal LLM service (Canopy3.6-Plus-DogFooding, DeepSeek V4, Kimi K2.6)",
        protocol: OPENAI_PROTOCOL,
        protocol_options: OPENAI_PROTOCOL_ONLY,
        base_url: BaseUrlPreset::Fixed("https://idealab.alibaba-inc.com/api/openai/v1"),
        env_key: Some("IDEALAB_API_KEY"),
        models: Some(IDEALAB_MODELS),
        models_editable: true,
        model_name_prefix: ModelNamePrefix::Fixed("Idealab"),
        api_key_placeholder: None,
        custom_headers: EMPTY_HEADERS,
        documentation_url: None,
        ui_group: Some("third-party"),
        ui_labels: None,
        show_advanced_config: false,
        merge_models_by_identity: false,
        dynamic_features: STATIC_INSTALL_PROJECTION,
    },
    ProviderPreset {
        id: "modelscope",
        label: "ModelScope API Key",
        description: "Quick setup for ModelScope API Inference",
        protocol: OPENAI_PROTOCOL,
        protocol_options: OPENAI_PROTOCOL_ONLY,
        base_url: BaseUrlPreset::Fixed("https://api-inference.modelscope.cn/v1"),
        env_key: Some("MODELSCOPE_API_KEY"),
        models: Some(MODELSCOPE_MODELS),
        models_editable: true,
        model_name_prefix: ModelNamePrefix::Fixed("ModelScope"),
        api_key_placeholder: None,
        custom_headers: EMPTY_HEADERS,
        documentation_url: Some("https://modelscope.cn/docs/model-service/API-Inference/intro"),
        ui_group: Some("third-party"),
        ui_labels: None,
        show_advanced_config: false,
        merge_models_by_identity: false,
        dynamic_features: STATIC_INSTALL_PROJECTION,
    },
    ProviderPreset {
        id: "openrouter",
        label: "OpenRouter",
        description: "Connect with an OpenRouter API key (get one from openrouter.ai/keys)",
        protocol: OPENAI_PROTOCOL,
        protocol_options: OPENAI_PROTOCOL_ONLY,
        base_url: BaseUrlPreset::Fixed(OPENROUTER_BASE_URL),
        env_key: Some(OPENROUTER_ENV_KEY),
        models: Some(OPENROUTER_MODELS),
        models_editable: true,
        model_name_prefix: ModelNamePrefix::Fixed("OpenRouter"),
        api_key_placeholder: None,
        custom_headers: HEADER_PAIR_OPENROUTER,
        documentation_url: Some("https://openrouter.ai/docs"),
        ui_group: Some("third-party"),
        ui_labels: None,
        show_advanced_config: false,
        merge_models_by_identity: false,
        dynamic_features: OWNERSHIP_AND_INSTALL_DYNAMIC,
    },
    ProviderPreset {
        id: "requesty",
        label: "Requesty",
        description: "Connect with a Requesty API key (get one from app.requesty.ai/api-keys)",
        protocol: OPENAI_PROTOCOL,
        protocol_options: OPENAI_PROTOCOL_ONLY,
        base_url: BaseUrlPreset::Fixed(REQUESTY_BASE_URL),
        env_key: Some(REQUESTY_ENV_KEY),
        models: Some(REQUESTY_MODELS),
        models_editable: true,
        model_name_prefix: ModelNamePrefix::Fixed("Requesty"),
        api_key_placeholder: None,
        custom_headers: HEADER_PAIR_REQUESTY,
        documentation_url: Some("https://docs.requesty.ai"),
        ui_group: Some("third-party"),
        ui_labels: None,
        show_advanced_config: false,
        merge_models_by_identity: false,
        dynamic_features: OWNERSHIP_AND_INSTALL_DYNAMIC,
    },
    ProviderPreset {
        id: "custom-openai-compatible",
        label: "Custom Provider",
        description: "Manually connect a local server, proxy, or unsupported provider",
        protocol: OPENAI_PROTOCOL,
        protocol_options: CUSTOM_PROTOCOL_OPTIONS,
        base_url: BaseUrlPreset::UserProvided,
        env_key: None,
        models: None,
        models_editable: false,
        model_name_prefix: ModelNamePrefix::Fixed(""),
        api_key_placeholder: None,
        custom_headers: EMPTY_HEADERS,
        documentation_url: None,
        ui_group: Some("custom"),
        ui_labels: None,
        show_advanced_config: true,
        merge_models_by_identity: true,
        dynamic_features: CUSTOM_PROVIDER_DYNAMIC,
    },
];

/// Look up a static provider preset by its stable id.
pub fn find_provider_by_id(id: &str) -> Option<&'static ProviderPreset> {
    PROVIDER_PRESETS.iter().find(|preset| preset.id == id)
}

/// Filter providers by their UI grouping while preserving registry order.
/// This mirrors the `ALIBABA_PROVIDERS` and `THIRD_PARTY_PROVIDERS` lists.
pub fn providers_by_ui_group(group: &str) -> impl Iterator<Item = &'static ProviderPreset> + '_ {
    PROVIDER_PRESETS
        .iter()
        .filter(move |preset| preset.ui_group == Some(group))
}

/// Return the endpoint choices for a provider in source order.
pub fn base_url_options(preset: &ProviderPreset) -> &'static [BaseUrlOption] {
    match preset.base_url {
        BaseUrlPreset::Fixed(_) | BaseUrlPreset::UserProvided => &[],
        BaseUrlPreset::Options(options) => options,
    }
}

/// Resolve the provider's endpoint using the same trailing-slash-insensitive
/// option matching as the setup projection. Fixed URLs always take precedence;
/// custom providers fall back to the selected user-entered URL.
pub fn resolve_base_url(preset: &ProviderPreset, selected: Option<&str>) -> String {
    match preset.base_url {
        BaseUrlPreset::Fixed(url) => url.to_owned(),
        BaseUrlPreset::Options(options) => {
            let normalized = selected.unwrap_or_default().trim_end_matches('/');
            options
                .iter()
                .find(|option| option.url.trim_end_matches('/') == normalized)
                .map(|option| option.url.to_owned())
                .or_else(|| options.first().map(|option| option.url.to_owned()))
                .or_else(|| selected.map(str::to_owned))
                .unwrap_or_default()
        }
        BaseUrlPreset::UserProvided => selected.unwrap_or_default().to_owned(),
    }
}

/// Resolve the user-visible model name prefix for a selected endpoint.
pub fn model_name_prefix(preset: &ProviderPreset, base_url: &str) -> &'static str {
    match preset.model_name_prefix {
        ModelNamePrefix::Fixed(prefix) => prefix,
        ModelNamePrefix::ByBaseUrl { default, rules } => rules
            .iter()
            .find(|rule| rule.base_url == base_url)
            .map_or(default, |rule| rule.prefix),
    }
}

/// Return every statically configured endpoint, preserving `ALL_PROVIDERS`
/// order and each provider's endpoint order. The user-provided custom endpoint
/// is intentionally absent.
pub fn all_provider_base_urls() -> Vec<&'static str> {
    PROVIDER_PRESETS
        .iter()
        .flat_map(|preset| match preset.base_url {
            BaseUrlPreset::Fixed(url) => vec![url],
            BaseUrlPreset::Options(options) => options.iter().map(|option| option.url).collect(),
            BaseUrlPreset::UserProvided => Vec::new(),
        })
        .collect()
}

/// Default model IDs in their configured order. Custom providers require user
/// input and therefore return an empty slice.
pub fn default_model_ids(preset: &ProviderPreset) -> impl Iterator<Item = &'static str> + '_ {
    preset
        .models
        .unwrap_or_default()
        .iter()
        .map(|model| model.id)
}

/// Resolve the environment variable used for this provider installation.
/// Custom endpoints derive an isolated key from both protocol and URL, just as
/// `generateCustomEnvKey` does in the TypeScript provider setup.
pub fn resolve_env_key(
    preset: &ProviderPreset,
    protocol: AuthType,
    base_url: &str,
) -> Option<String> {
    if preset
        .dynamic_features
        .contains(&DynamicFeature::EnvironmentKeyDerivation)
    {
        Some(generate_custom_env_key(protocol, base_url))
    } else {
        preset.env_key.map(str::to_owned)
    }
}

/// Derive a custom-provider API-key environment name. The readable segment is
/// intentionally lossy; the 48-bit suffix prevents distinct URLs that
/// normalize to the same environment-safe spelling from sharing a key.
pub fn generate_custom_env_key(protocol: AuthType, base_url: &str) -> String {
    let canonical_url = strip_trailing_slashes(trim_js_whitespace(base_url));
    let mut hasher = Sha256::new();
    hasher.update(protocol.as_str().as_bytes());
    hasher.update([0]);
    hasher.update(canonical_url.as_bytes());
    let digest = hasher.finalize();
    let suffix = digest[..6]
        .iter()
        .map(|byte| format!("{byte:02X}"))
        .collect::<String>();
    format!(
        "{CUSTOM_API_KEY_ENV_PREFIX}{}_{}_{}",
        normalize_env_segment(protocol.as_str()),
        normalize_env_segment(base_url),
        suffix
    )
}

fn normalize_env_segment(value: &str) -> String {
    let upper = trim_js_whitespace(value).to_uppercase();
    let mut result = String::with_capacity(upper.len());
    let mut previous_was_separator = false;
    for character in upper.chars() {
        if character.is_ascii_alphanumeric() {
            result.push(character);
            previous_was_separator = false;
        } else if !previous_was_separator {
            result.push('_');
            previous_was_separator = true;
        }
    }
    result.trim_matches('_').to_owned()
}

fn trim_js_whitespace(value: &str) -> &str {
    value.trim_matches(|character| {
        matches!(
            character,
            '\u{0009}'..='\u{000D}'
                | '\u{0020}'
                | '\u{00A0}'
                | '\u{1680}'
                | '\u{2000}'..='\u{200A}'
                | '\u{2028}'
                | '\u{2029}'
                | '\u{202F}'
                | '\u{205F}'
                | '\u{3000}'
                | '\u{FEFF}'
        )
    })
}

fn strip_trailing_slashes(value: &str) -> &str {
    value.trim_end_matches('/')
}

/// Match TypeScript's preset-specific API-key validation. A missing return
/// means the key passed validation; only Coding Plan currently validates.
pub fn validate_api_key(preset: &ProviderPreset, key: &str) -> Option<&'static str> {
    (preset.id == "coding-plan" && !key.starts_with("sk-sp-"))
        .then_some(CODING_PLAN_INVALID_API_KEY_MESSAGE)
}

/// Whether a provider's setup wizard should render a given step.
pub fn should_show_step(preset: &ProviderPreset, step: ProviderSetupStep) -> bool {
    match step {
        ProviderSetupStep::Protocol => preset.protocol_options.len() > 1,
        ProviderSetupStep::BaseUrl => !matches!(preset.base_url, BaseUrlPreset::Fixed(_)),
        ProviderSetupStep::ApiKey => true,
        ProviderSetupStep::Models => preset.models.is_none() || preset.models_editable,
        ProviderSetupStep::AdvancedConfig => preset.show_advanced_config,
    }
}

/// Return the metadata key used for a provider with a static model list.
/// Dots are rejected because the TypeScript settings adapter writes this key
/// through dotted-path traversal.
pub fn resolve_metadata_key(preset: &ProviderPreset) -> Result<Option<&'static str>, String> {
    if preset.models.is_none() {
        return Ok(None);
    }
    if preset.id.contains('.') {
        return Err(format!(
            "Provider id must not contain '.' (would corrupt {PROVIDER_METADATA_NS}.{} dotted writes): {}",
            preset.id, preset.id
        ));
    }
    Ok(Some(preset.id))
}

/// Apply the provider's model ownership policy to a saved model record.
/// These callback rules mirror `resolveOwnsModel` and the explicit callbacks
/// on custom, Alibaba Token Plan, OpenRouter, and Requesty presets.
pub fn owns_model(preset: &ProviderPreset, model: ProviderModelIdentity<'_>) -> bool {
    match preset.id {
        "custom-openai-compatible" => model
            .env_key
            .is_some_and(|key| key.starts_with(CUSTOM_API_KEY_ENV_PREFIX)),
        "coding-plan" => {
            model.env_key == preset.env_key
                && model.base_url.is_some_and(|url| {
                    base_url_options(preset)
                        .iter()
                        .any(|option| option.url == url)
                })
        }
        "token-plan" => {
            model.env_key == preset.env_key
                && (model.base_url.is_some_and(|url| {
                    base_url_options(preset)
                        .iter()
                        .any(|option| option.url == url)
                }) || model
                    .name
                    .is_some_and(|name| name.starts_with("[ModelStudio Token Plan]")))
        }
        "openrouter" => {
            model.env_key == preset.env_key
                && model
                    .base_url
                    .and_then(parse_host)
                    .is_some_and(|host| host == "openrouter.ai" || host.ends_with(".openrouter.ai"))
        }
        "requesty" => {
            model.env_key == preset.env_key
                && model.base_url.and_then(parse_host).is_some_and(|host| {
                    host == "router.requesty.ai" || host.ends_with(".requesty.ai")
                })
        }
        _ => {
            let Some(env_key) = preset.env_key else {
                return false;
            };
            if model.env_key != Some(env_key) {
                return false;
            }
            let prefix = model_name_prefix(preset, model.base_url.unwrap_or_default());
            if prefix.is_empty() {
                true
            } else {
                model
                    .name
                    .is_some_and(|name| name.starts_with(&format!("[{prefix}] ")))
            }
        }
    }
}

/// Merge provider attribution headers with any headers already configured on
/// a model. Existing model values win, matching the TypeScript object spread
/// order in `applyProviderCustomHeaders`.
pub fn merged_custom_headers(
    preset: &ProviderPreset,
    model_headers: &[(&str, &str)],
) -> Vec<(String, String)> {
    let mut merged = preset
        .custom_headers
        .iter()
        .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
        .collect::<Vec<_>>();
    for (name, value) in model_headers {
        if let Some((_, current_value)) = merged.iter_mut().find(|(key, _)| key.as_str() == *name) {
            *current_value = (*value).to_owned();
        } else {
            merged.push(((*name).to_owned(), (*value).to_owned()));
        }
    }
    merged
}

fn parse_host(base_url: &str) -> Option<String> {
    reqwest::Url::parse(base_url)
        .ok()?
        .host_str()
        .map(str::to_ascii_lowercase)
}

/// Match a configured provider against a saved (base URL, environment key)
/// credential pair. Custom providers try every protocol option because the
/// selected protocol is not separately persisted alongside the model.
pub fn provider_matches_credentials(
    preset: &ProviderPreset,
    base_url: Option<&str>,
    env_key: Option<&str>,
) -> bool {
    let Some(base_url) = base_url else {
        return false;
    };
    let matches_key = if preset
        .dynamic_features
        .contains(&DynamicFeature::EnvironmentKeyDerivation)
    {
        preset
            .protocol_options
            .iter()
            .any(|protocol| resolve_env_key(preset, *protocol, base_url).as_deref() == env_key)
    } else {
        preset.env_key == env_key
    };
    if !matches_key {
        return false;
    }
    match preset.base_url {
        BaseUrlPreset::Fixed(url) => url == base_url,
        BaseUrlPreset::Options(options) => options.iter().any(|option| option.url == base_url),
        BaseUrlPreset::UserProvided => !base_url.is_empty(),
    }
}

/// Find the first provider in registry order matching saved credentials.
pub fn find_provider_by_credentials(
    base_url: Option<&str>,
    env_key: Option<&str>,
) -> Option<&'static ProviderPreset> {
    PROVIDER_PRESETS
        .iter()
        .find(|preset| provider_matches_credentials(preset, base_url, env_key))
}

/// TS-style name retained for callers porting `getAllProviderBaseUrls`.
pub fn get_all_provider_base_urls() -> Vec<&'static str> {
    all_provider_base_urls()
}

/// Resolve the placeholder endpoint used for user-entered custom providers.
pub fn get_default_base_url_for_protocol(protocol: Option<AuthType>) -> &'static str {
    match protocol {
        Some(AuthType::OpenAi) => "https://api.openai.com/v1",
        Some(AuthType::Anthropic) => "https://api.anthropic.com/v1",
        Some(AuthType::Gemini) => "https://generativelanguage.googleapis.com",
        _ => "",
    }
}

/// Project a provider config and user setup answers into the model records
/// consumed by `buildInstallPlan` in TypeScript.
pub fn build_model_configs(
    preset: &ProviderPreset,
    inputs: &ProviderSetupInputs,
) -> Vec<ProviderModelConfig> {
    let protocol = inputs.protocol.unwrap_or(preset.protocol);
    let Some(env_key) = resolve_env_key(preset, protocol, &inputs.base_url) else {
        return Vec::new();
    };
    let prefix = model_name_prefix(preset, &inputs.base_url);
    let mut models: Vec<ProviderModelConfig> =
        if let Some(specs) = preset.models.filter(|_| !preset.models_editable) {
            specs
                .iter()
                .map(|spec| spec_to_model_config(spec, prefix, &inputs.base_url, &env_key))
                .collect()
        } else if let Some(specs) = preset.models {
            inputs
                .model_ids
                .iter()
                .map(|id| {
                    if let Some(spec) = specs.iter().find(|spec| spec.id == id.as_str()) {
                        spec_to_model_config(spec, prefix, &inputs.base_url, &env_key)
                    } else {
                        model_config_from_advanced(
                            id,
                            prefix,
                            &inputs.base_url,
                            &env_key,
                            inputs.advanced_config.as_ref(),
                        )
                    }
                })
                .collect()
        } else {
            inputs
                .model_ids
                .iter()
                .map(|id| {
                    model_config_from_advanced(
                        id,
                        prefix,
                        &inputs.base_url,
                        &env_key,
                        inputs.advanced_config.as_ref(),
                    )
                })
                .collect()
        };

    apply_provider_custom_headers(&mut models, preset);
    models
}

fn spec_to_model_config(
    spec: &ModelPreset,
    prefix: &str,
    base_url: &str,
    env_key: &str,
) -> ProviderModelConfig {
    let mut generation_config = ProviderGenerationConfig::default();
    if spec.enable_thinking {
        generation_config.extra_body = Some(EnableThinkingBody {
            enable_thinking: true,
        });
    }
    if spec.thinking_mandatory {
        generation_config.thinking_mandatory = Some(true);
    }
    if let Some(size) = spec.context_window_size {
        generation_config.context_window_size = Some(Number::from(size));
    }
    if let Some(modalities) = spec.modalities {
        let modalities = ConfiguredModalities::from_preset(modalities);
        if modalities.has_enabled_modality() {
            generation_config.modalities = Some(modalities);
        }
    }
    ProviderModelConfig {
        id: spec.id.to_owned(),
        name: display_model_name(prefix, spec.id),
        description: spec.description.map(str::to_owned),
        base_url: Some(base_url.to_owned()),
        env_key: Some(env_key.to_owned()),
        image_only: spec.image_only.then_some(true),
        generation_config: generation_config_if_nonempty(generation_config),
    }
}

fn model_config_from_advanced(
    id: &str,
    prefix: &str,
    base_url: &str,
    env_key: &str,
    advanced: Option<&AdvancedProviderConfig>,
) -> ProviderModelConfig {
    let mut generation_config = ProviderGenerationConfig::default();
    if let Some(advanced) = advanced {
        if advanced.enable_thinking {
            generation_config.extra_body = Some(EnableThinkingBody {
                enable_thinking: true,
            });
        }
        if let Some(modalities) = advanced
            .multimodal
            .filter(|modalities| modalities.has_enabled_modality())
        {
            generation_config.modalities = Some(modalities);
        }
        if let Some(size) = advanced.context_window_size.filter(|size| *size > 0.0) {
            generation_config.context_window_size = number_from_f64(size);
        }
        if let Some(max_tokens) = advanced.max_tokens.filter(|max_tokens| *max_tokens > 0.0) {
            generation_config.sampling_params =
                number_from_f64(max_tokens).map(|max_tokens| SamplingParams { max_tokens });
        }
    }
    ProviderModelConfig {
        id: id.to_owned(),
        name: display_model_name(prefix, id),
        description: None,
        base_url: Some(base_url.to_owned()),
        env_key: Some(env_key.to_owned()),
        image_only: None,
        generation_config: generation_config_if_nonempty(generation_config),
    }
}

fn display_model_name(prefix: &str, id: &str) -> String {
    if prefix.is_empty() {
        id.to_owned()
    } else {
        format!("[{prefix}] {id}")
    }
}

fn generation_config_if_nonempty(
    generation_config: ProviderGenerationConfig,
) -> Option<ProviderGenerationConfig> {
    if generation_config.extra_body.is_none()
        && generation_config.thinking_mandatory.is_none()
        && generation_config.context_window_size.is_none()
        && generation_config.modalities.is_none()
        && generation_config.sampling_params.is_none()
        && generation_config.custom_headers.is_none()
    {
        None
    } else {
        Some(generation_config)
    }
}

fn number_from_f64(value: f64) -> Option<Number> {
    if !value.is_finite() {
        return None;
    }
    if value.fract() == 0.0 && (0.0..18_446_744_073_709_551_616.0).contains(&value) {
        Some(Number::from(value as u64))
    } else if value.fract() == 0.0 && value >= i64::MIN as f64 && value < -(i64::MIN as f64) {
        Some(Number::from(value as i64))
    } else {
        Number::from_f64(value)
    }
}

fn apply_provider_custom_headers(models: &mut [ProviderModelConfig], preset: &ProviderPreset) {
    if preset.custom_headers.is_empty() {
        return;
    }
    for model in models {
        let generation_config = model
            .generation_config
            .get_or_insert_with(ProviderGenerationConfig::default);
        let existing = generation_config.custom_headers.take().unwrap_or_default();
        let mut headers = preset
            .custom_headers
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect::<Vec<_>>();
        for (name, value) in existing.0 {
            if let Some((_, current_value)) = headers
                .iter_mut()
                .find(|(key, _)| key.as_str() == name.as_str())
            {
                *current_value = value;
            } else {
                headers.push((name, value));
            }
        }
        generation_config.custom_headers = Some(OrderedHeaders(headers));
    }
}

/// Compute the lower-case SHA-256 model-list version used in
/// `providerMetadata.<provider-id>.version`.
pub fn compute_model_list_version(models: &[ProviderModelConfig]) -> String {
    let serialized =
        serde_json::to_vec(models).expect("provider model config structs always serialize to JSON");
    let digest = Sha256::digest(serialized);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Build the provider install projection, including the API-key environment
/// binding and version metadata. Settings persistence itself stays with the
/// caller, as it does in the TypeScript provider-config layer.
pub fn build_install_plan(
    preset: &'static ProviderPreset,
    inputs: &ProviderSetupInputs,
) -> Result<ProviderInstallPlan, ProviderInstallPlanError> {
    let protocol = inputs.protocol.unwrap_or(preset.protocol);
    let env_key = resolve_env_key(preset, protocol, &inputs.base_url).ok_or(
        ProviderInstallPlanError::EnvironmentKeyUnavailable(preset.id),
    )?;
    let models = if let Some(models) = &inputs.prebuilt_models {
        models.clone()
    } else {
        build_model_configs(preset, inputs)
    };
    if models.is_empty() {
        return Err(ProviderInstallPlanError::NoModelsConfigured(preset.id));
    }
    let first_model = &models[0];
    let model_selection = Some(ProviderModelSelection {
        model_id: first_model.id.clone(),
        base_url: (preset.merge_models_by_identity)
            .then(|| first_model.base_url.clone())
            .flatten()
            .filter(|url| !url.is_empty()),
    });
    let owns_model_preset_id = if preset.merge_models_by_identity {
        None
    } else if preset
        .dynamic_features
        .contains(&DynamicFeature::ModelOwnership)
        || (preset.env_key.is_some()
            && matches!(preset.model_name_prefix, ModelNamePrefix::Fixed(_)))
    {
        Some(preset.id)
    } else {
        None
    };
    let provider_state =
        match resolve_metadata_key(preset).map_err(ProviderInstallPlanError::InvalidMetadataKey)? {
            Some(key) => {
                let mut state = BTreeMap::new();
                state.insert(
                    format!("{PROVIDER_METADATA_NS}.{key}"),
                    BTreeMap::from([
                        ("version".to_owned(), compute_model_list_version(&models)),
                        ("baseUrl".to_owned(), inputs.base_url.clone()),
                    ]),
                );
                Some(state)
            }
            None => None,
        };
    let mut env = BTreeMap::new();
    env.insert(env_key, inputs.api_key.clone());
    Ok(ProviderInstallPlan {
        provider_id: preset.id,
        auth_type: protocol,
        env,
        model_selection,
        model_providers: vec![ProviderModelProvidersPatch {
            auth_type: protocol,
            models,
            merge_strategy: "prepend-and-remove-owned",
            owns_model_preset_id,
        }],
        provider_state,
    })
}

/// Build template model records used by provider version checks and updates.
pub fn build_provider_template(
    preset: &'static ProviderPreset,
    selected_base_url: Option<&str>,
) -> Vec<ProviderModelConfig> {
    let base_url = resolve_base_url(preset, selected_base_url);
    let inputs = ProviderSetupInputs {
        base_url,
        api_key: String::new(),
        model_ids: default_model_ids(preset).map(str::to_owned).collect(),
        ..ProviderSetupInputs::default()
    };
    build_model_configs(preset, &inputs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shipped_provider_credentials_and_catalog_metadata_match_typescript_fixtures() {
        let fixtures = [
            (
                "coding-plan",
                Some("BAILIAN_CODING_PLAN_API_KEY"),
                &[
                    "qwen3.5-plus",
                    "qwen3.6-plus",
                    "qwen3.7-plus",
                    "glm-5",
                    "kimi-k2.5",
                    "MiniMax-M2.5",
                    "qwen3-coder-plus",
                    "qwen3-coder-next",
                    "qwen3-max-2026-01-23",
                    "glm-4.7",
                ][..],
                "ModelStudio Coding Plan",
            ),
            (
                "token-plan",
                Some("BAILIAN_TOKEN_PLAN_API_KEY"),
                &[
                    "qwen3.7-plus",
                    "qwen3.6-plus",
                    "qwen3.7-max",
                    "qwen3.8-max-preview",
                    "qwen3.6-flash",
                    "deepseek-v4-pro",
                    "deepseek-v4-flash-0731",
                    "deepseek-v3.2",
                    "kimi-k2.7-code",
                    "kimi-k2.6",
                    "kimi-k2.5",
                    "glm-5.2",
                    "glm-5.1",
                    "glm-5",
                    "MiniMax-M2.5",
                ][..],
                "ModelStudio Token Plan",
            ),
            (
                "alibabaStandard",
                Some("DASHSCOPE_API_KEY"),
                &[
                    "qwen3.6-plus",
                    "qwen3.7-plus",
                    "qwen3.7-max",
                    "glm-5.1",
                    "deepseek-v4-pro",
                    "deepseek-v4-flash",
                ][..],
                "ModelStudio Standard",
            ),
            (
                "deepseek",
                Some("DEEPSEEK_API_KEY"),
                &["deepseek-v4-pro", "deepseek-v4-flash"][..],
                "DeepSeek",
            ),
            (
                "grok",
                Some("XAI_API_KEY"),
                &[
                    "grok-4.5",
                    "grok-4.3",
                    "grok-4.20-0309-reasoning",
                    "grok-4.20-0309-non-reasoning",
                    "grok-4.20-multi-agent-0309",
                    "grok-build-0.1",
                ][..],
                "Grok",
            ),
            (
                "minimax",
                Some("MINIMAX_API_KEY"),
                &[
                    "MiniMax-M3",
                    "MiniMax-M2.7",
                    "MiniMax-M2.7-highspeed",
                    "MiniMax-M2.5",
                    "MiniMax-M2.5-highspeed",
                ][..],
                "MiniMax",
            ),
            (
                "zai",
                Some("ZAI_API_KEY"),
                &["GLM-5.2", "GLM-5.1", "GLM-5", "GLM-5-Turbo"][..],
                "Z.AI",
            ),
            (
                "idealab",
                Some("IDEALAB_API_KEY"),
                &[
                    "Canopy3.6-Plus-DogFooding",
                    "bailian/deepseek-v4-pro",
                    "bailian/deepseek-v4-flash",
                    "bailian/kimi-k2.6",
                ][..],
                "Idealab",
            ),
            (
                "modelscope",
                Some("MODELSCOPE_API_KEY"),
                &[
                    "deepseek-ai/DeepSeek-V4-Flash",
                    "Canopy/Canopy3.5-397B-A17B",
                    "ZhipuAI/GLM-5.1",
                ][..],
                "ModelScope",
            ),
            (
                "openrouter",
                Some("OPENROUTER_API_KEY"),
                &["z-ai/glm-4.5-air:free", "openai/gpt-oss-120b:free"][..],
                "OpenRouter",
            ),
            (
                "requesty",
                Some("REQUESTY_API_KEY"),
                &["openai/gpt-4o-mini", "openai/gpt-4o"][..],
                "Requesty",
            ),
            ("custom-openai-compatible", None, &[][..], ""),
        ];

        for (id, env_key, expected_model_ids, prefix) in fixtures {
            let preset = find_provider_by_id(id).unwrap_or_else(|| panic!("missing preset {id}"));
            assert_eq!(preset.env_key, env_key, "env key for {id}");
            assert_eq!(
                default_model_ids(preset).collect::<Vec<_>>(),
                expected_model_ids,
                "model IDs for {id}"
            );
            assert_eq!(
                model_name_prefix(preset, ""),
                prefix,
                "model prefix for {id}"
            );
            assert_eq!(
                resolve_metadata_key(preset).unwrap(),
                preset.models.map(|_| id),
                "metadata key for {id}"
            );
        }
    }

    #[test]
    fn shipped_provider_labels_descriptions_docs_and_setup_flags_match_typescript() {
        let fixtures = [
            (
                "coding-plan",
                "Coding Plan",
                "For individual developers · Weekly quota included",
                None,
                Some("alibaba"),
                Some("sk-sp-..."),
                true,
            ),
            (
                "token-plan",
                "Token Plan",
                "For teams and companies · Usage-based billing with dedicated endpoint",
                None,
                Some("alibaba"),
                None,
                true,
            ),
            (
                "alibabaStandard",
                "Standard API Key",
                "Connect with an existing ModelStudio API key",
                None,
                Some("alibaba"),
                None,
                true,
            ),
            (
                "deepseek",
                "DeepSeek API Key",
                "Quick setup for DeepSeek (deepseek-v4-flash, deepseek-v4-pro)",
                Some("https://api-docs.deepseek.com/zh-cn/"),
                Some("third-party"),
                None,
                true,
            ),
            (
                "grok",
                "Grok (xAI) API Key",
                "Quick setup for xAI Grok chat & code models",
                Some("https://docs.x.ai/docs"),
                Some("third-party"),
                None,
                true,
            ),
            (
                "minimax",
                "MiniMax API Key",
                "Quick setup for MiniMax models",
                None,
                Some("third-party"),
                None,
                true,
            ),
            (
                "zai",
                "Z.AI API Key",
                "Quick setup for Z.AI models",
                None,
                Some("third-party"),
                None,
                true,
            ),
            (
                "idealab",
                "Idealab API Key",
                "Alibaba internal LLM service (Canopy3.6-Plus-DogFooding, DeepSeek V4, Kimi K2.6)",
                None,
                Some("third-party"),
                None,
                true,
            ),
            (
                "modelscope",
                "ModelScope API Key",
                "Quick setup for ModelScope API Inference",
                Some("https://modelscope.cn/docs/model-service/API-Inference/intro"),
                Some("third-party"),
                None,
                true,
            ),
            (
                "openrouter",
                "OpenRouter",
                "Connect with an OpenRouter API key (get one from openrouter.ai/keys)",
                Some("https://openrouter.ai/docs"),
                Some("third-party"),
                None,
                true,
            ),
            (
                "requesty",
                "Requesty",
                "Connect with a Requesty API key (get one from app.requesty.ai/api-keys)",
                Some("https://docs.requesty.ai"),
                Some("third-party"),
                None,
                true,
            ),
            (
                "custom-openai-compatible",
                "Custom Provider",
                "Manually connect a local server, proxy, or unsupported provider",
                None,
                Some("custom"),
                None,
                false,
            ),
        ];

        for (id, label, description, docs, group, api_key_placeholder, editable) in fixtures {
            let preset = find_provider_by_id(id).unwrap();
            assert_eq!(preset.label, label, "label for {id}");
            assert_eq!(preset.description, description, "description for {id}");
            assert_eq!(preset.documentation_url, docs, "docs for {id}");
            assert_eq!(preset.ui_group, group, "UI group for {id}");
            assert_eq!(
                preset.api_key_placeholder, api_key_placeholder,
                "placeholder for {id}"
            );
            assert_eq!(preset.protocol, AuthType::OpenAi, "protocol for {id}");
            assert_eq!(preset.models_editable, editable, "editable for {id}");
        }

        assert_eq!(
            find_provider_by_id("custom-openai-compatible")
                .unwrap()
                .protocol_options,
            CUSTOM_PROTOCOL_OPTIONS
        );
        assert!(
            find_provider_by_id("custom-openai-compatible")
                .unwrap()
                .show_advanced_config
        );
        assert!(
            find_provider_by_id("custom-openai-compatible")
                .unwrap()
                .merge_models_by_identity
        );
    }

    #[test]
    fn custom_provider_environment_keys_match_protocol_and_url_fixtures() {
        let custom = find_provider_by_id("custom-openai-compatible").unwrap();
        let fixtures = [
            (
                AuthType::OpenAi,
                "https://api.example.com/v1",
                "CANOPY_CUSTOM_API_KEY_OPENAI_HTTPS_API_EXAMPLE_COM_V1_1B0B29200D0E",
            ),
            (
                AuthType::Anthropic,
                "https://api.example.com/v1///",
                "CANOPY_CUSTOM_API_KEY_ANTHROPIC_HTTPS_API_EXAMPLE_COM_V1_88A222D29CF7",
            ),
        ];

        for (protocol, url, expected) in fixtures {
            assert_eq!(generate_custom_env_key(protocol, url), expected);
            assert_eq!(
                resolve_env_key(custom, protocol, url).as_deref(),
                Some(expected)
            );
        }
        assert_eq!(
            generate_custom_env_key(
                AuthType::OpenAi,
                " \u{feff}https://api.example.com/v1///\u{feff} "
            ),
            fixtures[0].2
        );
        assert_ne!(
            generate_custom_env_key(AuthType::OpenAi, "https://api.example.com/v1"),
            generate_custom_env_key(AuthType::OpenAi, "https://api-example.com/v1")
        );
    }

    #[test]
    fn preset_api_key_validation_matches_coding_plan_rules() {
        let coding = find_provider_by_id("coding-plan").unwrap();
        let other = find_provider_by_id("token-plan").unwrap();
        assert_eq!(validate_api_key(coding, "sk-sp-valid"), None);
        assert_eq!(
            validate_api_key(coding, ""),
            Some(CODING_PLAN_INVALID_API_KEY_MESSAGE)
        );
        assert_eq!(
            validate_api_key(coding, "sk-invalid"),
            Some(CODING_PLAN_INVALID_API_KEY_MESSAGE)
        );
        assert_eq!(validate_api_key(other, "anything"), None);
    }

    #[test]
    fn provider_credential_lookup_and_custom_ownership_match_source_callbacks() {
        let coding = find_provider_by_id("coding-plan").unwrap();
        let router = find_provider_by_id("openrouter").unwrap();
        let requesty = find_provider_by_id("requesty").unwrap();
        let token_plan = find_provider_by_id("token-plan").unwrap();
        let custom = find_provider_by_id("custom-openai-compatible").unwrap();

        assert_eq!(
            find_provider_by_credentials(
                Some("https://coding-intl.dashscope.aliyuncs.com/v1"),
                Some("BAILIAN_CODING_PLAN_API_KEY")
            )
            .map(|preset| preset.id),
            Some("coding-plan")
        );
        assert!(owns_model(
            coding,
            ProviderModelIdentity {
                id: "m",
                name: Some("[ModelStudio Coding Plan] m"),
                base_url: Some(CODING_PLAN_CHINA_URL),
                env_key: Some("BAILIAN_CODING_PLAN_API_KEY"),
            }
        ));
        assert!(owns_model(
            token_plan,
            ProviderModelIdentity {
                id: "legacy",
                name: Some("[ModelStudio Token Plan] legacy"),
                base_url: Some("https://old.example/v1"),
                env_key: Some("BAILIAN_TOKEN_PLAN_API_KEY"),
            }
        ));
        assert!(owns_model(
            router,
            ProviderModelIdentity {
                id: "m",
                name: None,
                base_url: Some("https://eu.openrouter.ai/api/v1"),
                env_key: Some("OPENROUTER_API_KEY"),
            }
        ));
        assert!(!owns_model(
            router,
            ProviderModelIdentity {
                id: "m",
                name: None,
                base_url: Some("https://openrouter.ai.attacker.test/v1"),
                env_key: Some("OPENROUTER_API_KEY"),
            }
        ));
        assert!(owns_model(
            requesty,
            ProviderModelIdentity {
                id: "m",
                name: None,
                base_url: Some("https://router.requesty.ai/v1"),
                env_key: Some("REQUESTY_API_KEY"),
            }
        ));
        assert!(owns_model(
            custom,
            ProviderModelIdentity {
                id: "m",
                name: None,
                base_url: Some("http://localhost:11434/v1"),
                env_key: Some("CANOPY_CUSTOM_API_KEY_OPENAI_LOCALHOST_123456789ABC"),
            }
        ));

        let custom_url = "https://custom.example/v1";
        let custom_key = generate_custom_env_key(AuthType::Anthropic, custom_url);
        assert!(provider_matches_credentials(
            custom,
            Some(custom_url),
            Some(&custom_key)
        ));
        assert!(!provider_matches_credentials(
            custom,
            Some(custom_url),
            Some("WRONG_KEY")
        ));

        for preset in PROVIDER_PRESETS
            .iter()
            .filter(|preset| preset.env_key.is_some())
        {
            let url = match preset.base_url {
                BaseUrlPreset::Fixed(url) => url,
                BaseUrlPreset::Options(options) => options[0].url,
                BaseUrlPreset::UserProvided => unreachable!("custom provider has no literal key"),
            };
            assert!(
                provider_matches_credentials(preset, Some(url), preset.env_key),
                "expected credentials to match {}",
                preset.id
            );
            assert!(
                !provider_matches_credentials(preset, Some(url), Some("OTHER_API_KEY")),
                "wrong key matched {}",
                preset.id
            );
            assert!(
                !provider_matches_credentials(
                    preset,
                    Some("https://wrong.example/v1"),
                    preset.env_key
                ),
                "wrong URL matched {}",
                preset.id
            );
        }
    }

    #[test]
    fn url_resolution_and_wizard_steps_match_provider_config_edge_cases() {
        let coding = find_provider_by_id("coding-plan").unwrap();
        let custom = find_provider_by_id("custom-openai-compatible").unwrap();
        let fixed = find_provider_by_id("deepseek").unwrap();

        assert_eq!(
            resolve_base_url(
                coding,
                Some("https://coding-intl.dashscope.aliyuncs.com/v1///")
            ),
            CODING_PLAN_GLOBAL_URL
        );
        assert_eq!(
            resolve_base_url(coding, Some("https://unknown.test/v1")),
            CODING_PLAN_CHINA_URL
        );
        assert_eq!(
            resolve_base_url(custom, Some("  http://localhost:1///  ")),
            "  http://localhost:1///  "
        );
        assert_eq!(
            resolve_base_url(fixed, Some("https://ignored.test")),
            "https://api.deepseek.com"
        );

        assert!(!should_show_step(coding, ProviderSetupStep::Protocol));
        assert!(should_show_step(custom, ProviderSetupStep::Protocol));
        assert!(should_show_step(custom, ProviderSetupStep::BaseUrl));
        assert!(!should_show_step(fixed, ProviderSetupStep::BaseUrl));
        assert!(should_show_step(fixed, ProviderSetupStep::ApiKey));
        assert!(should_show_step(custom, ProviderSetupStep::Models));
        assert!(!should_show_step(fixed, ProviderSetupStep::AdvancedConfig));
        assert!(should_show_step(custom, ProviderSetupStep::AdvancedConfig));
        assert_eq!(
            get_default_base_url_for_protocol(Some(AuthType::OpenAi)),
            "https://api.openai.com/v1"
        );
        assert_eq!(
            get_default_base_url_for_protocol(Some(AuthType::Anthropic)),
            "https://api.anthropic.com/v1"
        );
        assert_eq!(
            get_default_base_url_for_protocol(Some(AuthType::Gemini)),
            "https://generativelanguage.googleapis.com"
        );
        assert_eq!(get_default_base_url_for_protocol(None), "");
    }

    #[test]
    fn shipped_custom_headers_and_base_url_key_metadata_are_complete() {
        let expected_headers = [
            (
                "openrouter",
                &[
                    ("HTTP-Referer", "https://github.com/QwenLM/canopy-code.git"),
                    ("X-OpenRouter-Title", "Canopy Code"),
                ][..],
            ),
            (
                "requesty",
                &[
                    ("HTTP-Referer", "https://github.com/QwenLM/canopy-code.git"),
                    ("X-Title", "Canopy Code"),
                ][..],
            ),
        ];
        for (id, expected) in expected_headers {
            assert_eq!(find_provider_by_id(id).unwrap().custom_headers, expected);
        }
        let merged = merged_custom_headers(
            find_provider_by_id("openrouter").unwrap(),
            &[
                ("HTTP-Referer", "https://model.example"),
                ("X-Custom", "yes"),
            ],
        );
        assert_eq!(
            merged,
            vec![
                (
                    "HTTP-Referer".to_owned(),
                    "https://model.example".to_owned()
                ),
                ("X-OpenRouter-Title".to_owned(), "Canopy Code".to_owned()),
                ("X-Custom".to_owned(), "yes".to_owned()),
            ]
        );

        let expected_options = [
            (
                "coding-plan",
                "aliyun",
                "China (Beijing)",
                CODING_PLAN_CHINA_URL,
                Some("https://help.aliyun.com/zh/model-studio/coding-plan"),
            ),
            (
                "coding-plan",
                "alibabacloud",
                "Singapore (International)",
                CODING_PLAN_GLOBAL_URL,
                Some("https://www.alibabacloud.com/help/en/model-studio/coding-plan"),
            ),
            (
                "token-plan",
                "cn-beijing",
                "China (Beijing)",
                TOKEN_PLAN_CHINA_URL,
                Some(
                    "https://bailian.console.aliyun.com/cn-beijing?tab=doc#/doc/?type=model&url=3028856",
                ),
            ),
            (
                "token-plan",
                "ap-southeast-1",
                "Singapore (International)",
                TOKEN_PLAN_GLOBAL_URL,
                Some(
                    "https://modelstudio.console.alibabacloud.com/ap-southeast-1?tab=doc#/doc/?type=model",
                ),
            ),
            (
                "alibabaStandard",
                "cn-beijing",
                "China (Beijing)",
                "https://dashscope.aliyuncs.com/compatible-mode/v1",
                Some("https://bailian.console.aliyun.com/cn-beijing?tab=api#/api"),
            ),
            (
                "alibabaStandard",
                "sg-singapore",
                "Singapore",
                "https://dashscope-intl.aliyuncs.com/compatible-mode/v1",
                Some(
                    "https://modelstudio.console.alibabacloud.com/ap-southeast-1?tab=api#/api/?type=model&url=2712195",
                ),
            ),
            (
                "alibabaStandard",
                "us-virginia",
                "US (Virginia)",
                "https://dashscope-us.aliyuncs.com/compatible-mode/v1",
                Some(
                    "https://modelstudio.console.alibabacloud.com/us-east-1?tab=api#/api/?type=model&url=2712195",
                ),
            ),
            (
                "alibabaStandard",
                "cn-hongkong",
                "China (Hong Kong)",
                "https://cn-hongkong.dashscope.aliyuncs.com/compatible-mode/v1",
                Some(
                    "https://modelstudio.console.alibabacloud.com/cn-hongkong?tab=api#/api/?type=model&url=2712195",
                ),
            ),
            (
                "minimax",
                "international",
                "International",
                "https://api.minimax.io/v1",
                Some("https://www.minimax.io/platform"),
            ),
            (
                "minimax",
                "china",
                "China",
                "https://api.minimaxi.com/v1",
                Some("https://platform.minimaxi.com"),
            ),
            (
                "zai",
                "standard-api-key",
                "Standard API Key",
                "https://api.z.ai/api/paas/v4",
                Some("https://docs.z.ai/"),
            ),
            (
                "zai",
                "coding-plan",
                "Coding Plan",
                "https://api.z.ai/api/coding/paas/v4",
                Some("https://docs.z.ai/"),
            ),
        ];
        let options = PROVIDER_PRESETS
            .iter()
            .flat_map(|preset| {
                base_url_options(preset)
                    .iter()
                    .map(move |option| (preset.id, option))
            })
            .collect::<Vec<_>>();
        assert_eq!(options.len(), expected_options.len());
        for ((provider_id, option), (expected_provider, id, label, url, docs)) in
            options.into_iter().zip(expected_options)
        {
            assert_eq!(provider_id, expected_provider);
            assert_eq!(option.id, id);
            assert_eq!(option.label, label);
            assert_eq!(option.url, url);
            assert_eq!(option.documentation_url, docs);
            assert_eq!(option.api_key_url, None);
        }
    }

    #[test]
    fn metadata_key_rejects_dotted_static_provider_ids() {
        let mut dotted = *find_provider_by_id("deepseek").unwrap();
        dotted.id = "company.ai";
        assert!(
            resolve_metadata_key(&dotted)
                .unwrap_err()
                .contains("providerMetadata.company.ai")
        );

        let mut user_models_only = *find_provider_by_id("custom-openai-compatible").unwrap();
        user_models_only.id = "company.ai";
        assert_eq!(resolve_metadata_key(&user_models_only).unwrap(), None);
    }

    #[test]
    fn shipped_model_generation_metadata_matches_typescript_specs() {
        let both_visual_modalities = InputModalities {
            image: true,
            pdf: false,
            audio: false,
            video: true,
        };
        let fixtures = [
            (
                "coding-plan",
                "qwen3.6-plus",
                Some(1_000_000),
                true,
                false,
                Some(both_visual_modalities),
                Some("Currently available to Pro subscribers only."),
            ),
            (
                "coding-plan",
                "qwen3-coder-plus",
                Some(1_000_000),
                false,
                false,
                None,
                None,
            ),
            (
                "token-plan",
                "qwen3.8-max-preview",
                Some(1_000_000),
                true,
                true,
                Some(both_visual_modalities),
                None,
            ),
            (
                "token-plan",
                "deepseek-v4-pro",
                Some(1_000_000),
                false,
                false,
                None,
                None,
            ),
            (
                "alibabaStandard",
                "deepseek-v4-pro",
                Some(1_000_000),
                true,
                false,
                None,
                None,
            ),
            (
                "deepseek",
                "deepseek-v4-pro",
                Some(1_000_000),
                true,
                false,
                None,
                None,
            ),
            (
                "minimax",
                "MiniMax-M3",
                Some(1_000_000),
                false,
                false,
                Some(both_visual_modalities),
                None,
            ),
            (
                "idealab",
                "Canopy3.6-Plus-DogFooding",
                Some(1_000_000),
                true,
                false,
                Some(both_visual_modalities),
                None,
            ),
            (
                "modelscope",
                "ZhipuAI/GLM-5.1",
                Some(1_000_000),
                true,
                false,
                None,
                None,
            ),
            ("zai", "GLM-5.1", Some(204_800), true, false, None, None),
            (
                "openrouter",
                "z-ai/glm-4.5-air:free",
                Some(128_000),
                false,
                false,
                None,
                None,
            ),
        ];

        for (provider_id, model_id, window, thinking, mandatory, modalities, description) in
            fixtures
        {
            let model = find_provider_by_id(provider_id)
                .unwrap()
                .models
                .unwrap()
                .iter()
                .find(|model| model.id == model_id)
                .unwrap_or_else(|| panic!("missing {provider_id} model {model_id}"));
            assert_eq!(model.context_window_size, window, "window for {model_id}");
            assert_eq!(model.enable_thinking, thinking, "thinking for {model_id}");
            assert_eq!(
                model.thinking_mandatory, mandatory,
                "mandatory for {model_id}"
            );
            assert_eq!(model.modalities, modalities, "modalities for {model_id}");
            assert_eq!(model.description, description, "description for {model_id}");
            assert!(!model.image_only, "imageOnly for {model_id}");
        }
    }

    #[test]
    fn install_plan_model_json_and_metadata_version_match_typescript_projection() {
        let deepseek = find_provider_by_id("deepseek").unwrap();
        let plan = build_install_plan(
            deepseek,
            &ProviderSetupInputs {
                base_url: "https://api.deepseek.com".to_owned(),
                api_key: "sk-test".to_owned(),
                model_ids: vec!["deepseek-v4-pro".to_owned()],
                ..ProviderSetupInputs::default()
            },
        )
        .unwrap();

        let models = &plan.model_providers[0].models;
        assert_eq!(
            serde_json::to_string(models).unwrap(),
            "[{\"id\":\"deepseek-v4-pro\",\"name\":\"[DeepSeek] deepseek-v4-pro\",\"baseUrl\":\"https://api.deepseek.com\",\"envKey\":\"DEEPSEEK_API_KEY\",\"generationConfig\":{\"extra_body\":{\"enable_thinking\":true},\"contextWindowSize\":1000000}}]"
        );
        assert_eq!(
            plan.provider_state.as_ref().unwrap()["providerMetadata.deepseek"]["version"],
            "3e05170492d02eec7b204dd4256f628810fd19b66cf0a5380ddb9de65ba004d7"
        );
        assert_eq!(
            plan.provider_state.as_ref().unwrap()["providerMetadata.deepseek"]["baseUrl"],
            "https://api.deepseek.com"
        );
        assert_eq!(plan.env["DEEPSEEK_API_KEY"], "sk-test");
        assert_eq!(
            plan.model_selection,
            Some(ProviderModelSelection {
                model_id: "deepseek-v4-pro".to_owned(),
                base_url: None,
            })
        );
        assert_eq!(
            plan.model_providers[0].owns_model(ProviderModelIdentity {
                id: "deepseek-v4-pro",
                name: Some("[DeepSeek] deepseek-v4-pro"),
                base_url: Some("https://api.deepseek.com"),
                env_key: Some("DEEPSEEK_API_KEY"),
            }),
            Some(true)
        );
    }

    #[test]
    fn install_plan_applies_advanced_config_custom_headers_and_protocol_override() {
        let custom = find_provider_by_id("custom-openai-compatible").unwrap();
        let plan = build_install_plan(
            custom,
            &ProviderSetupInputs {
                protocol: Some(AuthType::Anthropic),
                base_url: "https://proxy.example/v1".to_owned(),
                api_key: "secret".to_owned(),
                model_ids: vec!["alias-model".to_owned()],
                advanced_config: Some(AdvancedProviderConfig {
                    enable_thinking: true,
                    multimodal: Some(ConfiguredModalities {
                        image: Some(true),
                        video: Some(false),
                        ..ConfiguredModalities::default()
                    }),
                    context_window_size: Some(128_000.0),
                    max_tokens: Some(4_096.0),
                }),
                ..ProviderSetupInputs::default()
            },
        )
        .unwrap();
        let expected_key = generate_custom_env_key(AuthType::Anthropic, "https://proxy.example/v1");
        assert_eq!(plan.auth_type, AuthType::Anthropic);
        assert_eq!(
            plan.env.get(&expected_key).map(String::as_str),
            Some("secret")
        );
        assert_eq!(plan.provider_state, None);
        assert_eq!(
            plan.model_selection.as_ref().unwrap().base_url.as_deref(),
            Some("https://proxy.example/v1")
        );
        assert_eq!(plan.model_providers[0].owns_model_preset_id, None);
        assert_eq!(
            serde_json::to_string(&plan.model_providers[0].models[0]).unwrap(),
            format!(
                "{{\"id\":\"alias-model\",\"name\":\"alias-model\",\"baseUrl\":\"https://proxy.example/v1\",\"envKey\":\"{expected_key}\",\"generationConfig\":{{\"extra_body\":{{\"enable_thinking\":true}},\"contextWindowSize\":128000,\"modalities\":{{\"image\":true,\"video\":false}},\"samplingParams\":{{\"max_tokens\":4096}}}}}}"
            )
        );

        let router = find_provider_by_id("openrouter").unwrap();
        let router_plan = build_install_plan(
            router,
            &ProviderSetupInputs {
                base_url: OPENROUTER_BASE_URL.to_owned(),
                api_key: "router-token".to_owned(),
                model_ids: vec!["openai/custom".to_owned()],
                ..ProviderSetupInputs::default()
            },
        )
        .unwrap();
        assert_eq!(
            router_plan.model_providers[0].models[0]
                .generation_config
                .as_ref()
                .unwrap()
                .custom_headers,
            Some(OrderedHeaders(vec![
                (
                    "HTTP-Referer".to_owned(),
                    "https://github.com/QwenLM/canopy-code.git".to_owned()
                ),
                ("X-OpenRouter-Title".to_owned(), "Canopy Code".to_owned()),
            ]))
        );
    }

    #[test]
    fn install_plan_rejects_empty_models_and_preserves_prebuilt_configs() {
        let custom = find_provider_by_id("custom-openai-compatible").unwrap();
        let empty = build_install_plan(
            custom,
            &ProviderSetupInputs {
                base_url: "https://custom.example/v1".to_owned(),
                ..ProviderSetupInputs::default()
            },
        );
        assert_eq!(
            empty,
            Err(ProviderInstallPlanError::NoModelsConfigured(custom.id))
        );

        let prebuilt = ProviderModelConfig {
            id: "from-api".to_owned(),
            name: "Remote Model".to_owned(),
            base_url: None,
            env_key: None,
            ..ProviderModelConfig::default()
        };
        let router = find_provider_by_id("openrouter").unwrap();
        let plan = build_install_plan(
            router,
            &ProviderSetupInputs {
                base_url: OPENROUTER_BASE_URL.to_owned(),
                model_ids: vec!["ignored-default".to_owned()],
                prebuilt_models: Some(vec![prebuilt.clone()]),
                ..ProviderSetupInputs::default()
            },
        )
        .unwrap();
        assert_eq!(plan.model_providers[0].models, [prebuilt]);
        assert_eq!(plan.model_selection.as_ref().unwrap().model_id, "from-api");
    }

    #[test]
    fn provider_template_uses_default_model_ids_and_normalized_region_choice() {
        let coding = find_provider_by_id("coding-plan").unwrap();
        let template = build_provider_template(
            coding,
            Some("https://coding-intl.dashscope.aliyuncs.com/v1/"),
        );
        assert_eq!(template.len(), 10);
        assert_eq!(template[0].id, "qwen3.5-plus");
        assert_eq!(
            template[0].name,
            "[ModelStudio Coding Plan for Global/Intl] qwen3.5-plus"
        );
        assert_eq!(
            template[0].base_url.as_deref(),
            Some(CODING_PLAN_GLOBAL_URL)
        );
        assert_eq!(template[0].env_key.as_deref(), Some(CODING_PLAN_ENV_KEY));

        let router = find_provider_by_id("openrouter").unwrap();
        let router_template = build_provider_template(router, None);
        assert_eq!(router_template.len(), 2);
        assert_eq!(
            router_template[0]
                .generation_config
                .as_ref()
                .unwrap()
                .custom_headers
                .as_ref()
                .unwrap()
                .0[1],
            ("X-OpenRouter-Title".to_owned(), "Canopy Code".to_owned())
        );
    }

    #[test]
    fn catalog_matches_typescript_registry_order_and_groups() {
        let ids: Vec<_> = PROVIDER_PRESETS
            .iter()
            .map(|provider| provider.id)
            .collect();
        assert_eq!(
            ids,
            [
                "coding-plan",
                "token-plan",
                "alibabaStandard",
                "deepseek",
                "grok",
                "minimax",
                "zai",
                "idealab",
                "modelscope",
                "openrouter",
                "requesty",
                "custom-openai-compatible",
            ]
        );
        assert_eq!(
            providers_by_ui_group("alibaba")
                .map(|provider| provider.id)
                .collect::<Vec<_>>(),
            ["coding-plan", "token-plan", "alibabaStandard"]
        );
        assert_eq!(
            find_provider_by_id("openrouter").unwrap().ui_group,
            Some("third-party")
        );
        assert_eq!(find_provider_by_id("missing"), None);
    }

    #[test]
    fn endpoint_projection_preserves_order_and_region_specific_prefixes() {
        assert_eq!(TOKEN_PLAN_BASE_URL, TOKEN_PLAN_CHINA_BASE_URL);
        assert_eq!(CODING_PLAN_ENV_KEY, "BAILIAN_CODING_PLAN_API_KEY");
        assert_eq!(TOKEN_PLAN_ENV_KEY, "BAILIAN_TOKEN_PLAN_API_KEY");
        assert_eq!(GROK_BASE_URL, "https://api.x.ai/v1");
        assert_eq!(OPENROUTER_BASE_URL, "https://openrouter.ai/api/v1");
        assert_eq!(REQUESTY_BASE_URL, "https://router.requesty.ai/v1");
        let urls = all_provider_base_urls();
        assert_eq!(
            urls,
            [
                CODING_PLAN_CHINA_URL,
                CODING_PLAN_GLOBAL_URL,
                TOKEN_PLAN_CHINA_URL,
                TOKEN_PLAN_GLOBAL_URL,
                "https://dashscope.aliyuncs.com/compatible-mode/v1",
                "https://dashscope-intl.aliyuncs.com/compatible-mode/v1",
                "https://dashscope-us.aliyuncs.com/compatible-mode/v1",
                "https://cn-hongkong.dashscope.aliyuncs.com/compatible-mode/v1",
                "https://api.deepseek.com",
                "https://api.x.ai/v1",
                "https://api.minimax.io/v1",
                "https://api.minimaxi.com/v1",
                "https://api.z.ai/api/paas/v4",
                "https://api.z.ai/api/coding/paas/v4",
                "https://idealab.alibaba-inc.com/api/openai/v1",
                "https://api-inference.modelscope.cn/v1",
                "https://openrouter.ai/api/v1",
                "https://router.requesty.ai/v1",
            ]
        );

        let coding = find_provider_by_id("coding-plan").unwrap();
        assert_eq!(
            resolve_base_url(
                coding,
                Some("https://coding-intl.dashscope.aliyuncs.com/v1/")
            ),
            CODING_PLAN_GLOBAL_URL
        );
        assert_eq!(
            model_name_prefix(coding, CODING_PLAN_GLOBAL_URL),
            "ModelStudio Coding Plan for Global/Intl"
        );
        assert_eq!(
            model_name_prefix(coding, CODING_PLAN_CHINA_URL),
            "ModelStudio Coding Plan"
        );
        assert_eq!(resolve_base_url(coding, None), CODING_PLAN_CHINA_URL);
    }

    #[test]
    fn preset_model_metadata_and_custom_provider_dynamic_gaps_are_kept() {
        let token_plan = find_provider_by_id("token-plan").unwrap();
        let models = token_plan.models.unwrap();
        assert_eq!(models.len(), 15);
        assert_eq!(models[3].id, "qwen3.8-max-preview");
        assert!(models[3].thinking_mandatory);
        assert!(models[0].modalities.unwrap().image);
        assert!(models[0].modalities.unwrap().video);

        let custom = find_provider_by_id("custom-openai-compatible").unwrap();
        assert_eq!(custom.base_url, BaseUrlPreset::UserProvided);
        assert_eq!(custom.env_key, None);
        assert_eq!(custom.models, None);
        assert!(custom.show_advanced_config);
        assert!(custom.merge_models_by_identity);
        assert!(
            custom
                .dynamic_features
                .contains(&DynamicFeature::EnvironmentKeyDerivation)
        );
        assert!(
            custom
                .dynamic_features
                .contains(&DynamicFeature::UserDefinedModels)
        );
        assert_eq!(custom.protocol_options, CUSTOM_PROTOCOL_OPTIONS);
    }

    #[test]
    fn custom_headers_and_provider_display_metadata_are_retained() {
        let router = find_provider_by_id("openrouter").unwrap();
        assert_eq!(router.custom_headers, HEADER_PAIR_OPENROUTER);
        assert_eq!(router.documentation_url, Some("https://openrouter.ai/docs"));
        let plan = find_provider_by_id("coding-plan").unwrap();
        assert_eq!(
            plan.ui_labels.unwrap().flow_title,
            Some("Alibaba ModelStudio")
        );
        assert_eq!(plan.api_key_placeholder, Some("sk-sp-..."));
        assert_eq!(
            default_model_ids(router).collect::<Vec<_>>(),
            ["z-ai/glm-4.5-air:free", "openai/gpt-oss-120b:free"]
        );
    }
}
