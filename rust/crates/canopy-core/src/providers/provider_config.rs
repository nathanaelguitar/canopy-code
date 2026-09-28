//! Build provider install plans from provider configuration and setup input.
//!
//! Port of `packages/core/src/providers/provider-config.ts`. Static presets in
//! [`super::presets`] provide the catalog-specific values; this module handles
//! arbitrary provider definitions and their dynamic callback seams.

use std::fmt;
use std::sync::Arc;

use indexmap::IndexMap;
use serde_json::{Map, Number, Value};
use sha2::{Digest, Sha256};

use super::install::{
    InstallModelProvidersPatch, LegacyCredentials, MergeStrategy, ProviderModelSelection,
};
use super::presets::{
    AdvancedProviderConfig, ConfiguredModalities, EnableThinkingBody, OrderedHeaders,
    ProviderGenerationConfig, ProviderModelConfig, ProviderSetupInputs, SamplingParams,
};
use crate::utils::error_parsing::AuthType;

pub type EnvKeyCallback = Arc<dyn Fn(AuthType, &str) -> String + Send + Sync>;
pub type ModelNamePrefixCallback = Arc<dyn Fn(&str) -> String + Send + Sync>;
pub type OwnsModelCallback = Arc<dyn Fn(&ProviderModelConfig) -> bool + Send + Sync>;
pub type ApiKeyValidationCallback = Arc<dyn Fn(&str, &str) -> Option<String> + Send + Sync>;
pub type ProviderTextCallback = Arc<dyn Fn(&str) -> String + Send + Sync>;

/// A fixed or dynamically derived environment-variable key.
#[derive(Clone)]
pub enum ProviderEnvKey {
    Static(String),
    Dynamic(EnvKeyCallback),
}

impl ProviderEnvKey {
    pub fn resolve(&self, protocol: AuthType, base_url: &str) -> String {
        match self {
            Self::Static(key) => key.clone(),
            Self::Dynamic(callback) => callback(protocol, base_url),
        }
    }
}

impl From<String> for ProviderEnvKey {
    fn from(value: String) -> Self {
        Self::Static(value)
    }
}

impl From<&str> for ProviderEnvKey {
    fn from(value: &str) -> Self {
        Self::Static(value.to_owned())
    }
}

impl fmt::Debug for ProviderEnvKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Static(key) => formatter.debug_tuple("Static").field(key).finish(),
            Self::Dynamic(_) => formatter.write_str("Dynamic(..)"),
        }
    }
}

/// A fixed or base-URL-dependent model display-name prefix.
#[derive(Clone)]
pub enum ProviderModelNamePrefix {
    Static(String),
    Dynamic(ModelNamePrefixCallback),
}

impl ProviderModelNamePrefix {
    pub fn resolve(&self, base_url: &str) -> String {
        match self {
            Self::Static(prefix) => prefix.clone(),
            Self::Dynamic(callback) => callback(base_url),
        }
    }
}

impl From<String> for ProviderModelNamePrefix {
    fn from(value: String) -> Self {
        Self::Static(value)
    }
}

impl From<&str> for ProviderModelNamePrefix {
    fn from(value: &str) -> Self {
        Self::Static(value.to_owned())
    }
}

impl fmt::Debug for ProviderModelNamePrefix {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Static(prefix) => formatter.debug_tuple("Static").field(prefix).finish(),
            Self::Dynamic(_) => formatter.write_str("Dynamic(..)"),
        }
    }
}

/// A static or base-URL-dependent documentation URL used by provider setup UI.
#[derive(Clone)]
pub enum ProviderDocumentationUrl {
    Static(String),
    Dynamic(ProviderTextCallback),
}

impl ProviderDocumentationUrl {
    pub fn resolve(&self, base_url: &str) -> String {
        match self {
            Self::Static(url) => url.clone(),
            Self::Dynamic(callback) => callback(base_url),
        }
    }
}

impl fmt::Debug for ProviderDocumentationUrl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Static(url) => formatter.debug_tuple("Static").field(url).finish(),
            Self::Dynamic(_) => formatter.write_str("Dynamic(..)"),
        }
    }
}

/// Provider base-URL setup data.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProviderBaseUrl {
    Fixed(String),
    Options(Vec<ProviderBaseUrlOption>),
    UserProvided,
}

impl ProviderBaseUrl {
    /// Match TypeScript `resolveBaseUrl`: fixed URLs win, options select an
    /// exact match or fall back to their first entry, and user-entered URLs
    /// default to the empty string.
    pub fn resolve(&self, selected_base_url: Option<&str>) -> String {
        match self {
            Self::Fixed(url) => url.clone(),
            Self::Options(options) => options
                .iter()
                .find(|option| Some(option.url.as_str()) == selected_base_url)
                .or_else(|| options.first())
                .map(|option| option.url.clone())
                .unwrap_or_default(),
            Self::UserProvided => selected_base_url.unwrap_or_default().to_owned(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderBaseUrlOption {
    pub id: String,
    pub label: String,
    pub url: String,
    pub documentation_url: Option<String>,
    pub api_key_url: Option<String>,
}

/// Model metadata used by fixed and editable provider model lists.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ModelSpec {
    pub id: String,
    pub context_window_size: Option<Number>,
    pub enable_thinking: bool,
    pub thinking_mandatory: bool,
    pub modalities: Option<ConfiguredModalities>,
    pub description: Option<String>,
    pub image_only: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ProviderUiLabels {
    pub flow_title: Option<String>,
    pub base_url_step_title: Option<String>,
}

/// Provider catalog/config metadata plus the parts used to construct an
/// install plan. Callback values are explicit, thread-safe Rust closures.
#[derive(Clone)]
pub struct ProviderConfig {
    pub id: String,
    pub label: String,
    pub description: String,
    pub protocol: AuthType,
    pub base_url: Option<ProviderBaseUrl>,
    pub env_key: ProviderEnvKey,
    pub models: Option<Vec<ModelSpec>>,
    pub models_editable: bool,
    pub model_name_prefix: ProviderModelNamePrefix,
    pub protocol_options: Vec<AuthType>,
    pub show_advanced_config: bool,
    pub validate_api_key: Option<ApiKeyValidationCallback>,
    pub api_key_placeholder: Option<String>,
    /// Ordered to retain provider custom-header insertion order in version
    /// metadata hashes.
    pub custom_headers: Option<IndexMap<String, String>>,
    pub documentation_url: Option<ProviderDocumentationUrl>,
    pub owns_model: Option<OwnsModelCallback>,
    pub merge_models_by_identity: bool,
    pub ui_group: Option<String>,
    pub ui_labels: Option<ProviderUiLabels>,
}

impl ProviderConfig {
    pub fn new(
        id: impl Into<String>,
        label: impl Into<String>,
        description: impl Into<String>,
        protocol: AuthType,
        env_key: impl Into<ProviderEnvKey>,
        model_name_prefix: impl Into<ProviderModelNamePrefix>,
    ) -> Self {
        Self {
            id: id.into(),
            label: label.into(),
            description: description.into(),
            protocol,
            base_url: None,
            env_key: env_key.into(),
            models: None,
            models_editable: false,
            model_name_prefix: model_name_prefix.into(),
            protocol_options: Vec::new(),
            show_advanced_config: false,
            validate_api_key: None,
            api_key_placeholder: None,
            custom_headers: None,
            documentation_url: None,
            owns_model: None,
            merge_models_by_identity: false,
            ui_group: None,
            ui_labels: None,
        }
    }

    pub fn resolve_documentation_url(&self, base_url: &str) -> Option<String> {
        self.documentation_url
            .as_ref()
            .map(|documentation_url| documentation_url.resolve(base_url))
    }

    pub fn validate_api_key(&self, key: &str, base_url: &str) -> Option<String> {
        self.validate_api_key
            .as_ref()
            .and_then(|callback| callback(key, base_url))
    }
}

impl fmt::Debug for ProviderConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderConfig")
            .field("id", &self.id)
            .field("label", &self.label)
            .field("description", &self.description)
            .field("protocol", &self.protocol)
            .field("base_url", &self.base_url)
            .field("env_key", &self.env_key)
            .field("models", &self.models)
            .field("models_editable", &self.models_editable)
            .field("model_name_prefix", &self.model_name_prefix)
            .field("protocol_options", &self.protocol_options)
            .field("show_advanced_config", &self.show_advanced_config)
            .field("has_validate_api_key", &self.validate_api_key.is_some())
            .field("api_key_placeholder", &self.api_key_placeholder)
            .field("custom_headers", &self.custom_headers)
            .field("documentation_url", &self.documentation_url)
            .field("has_owns_model", &self.owns_model.is_some())
            .field("merge_models_by_identity", &self.merge_models_by_identity)
            .field("ui_group", &self.ui_group)
            .field("ui_labels", &self.ui_labels)
            .finish()
    }
}

/// Optional success feedback supported by the wider TypeScript install-plan
/// type. `build_install_plan` currently leaves it absent, as the source helper
/// does; UI callers can attach their own display data afterward.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ProviderInstallDisplay {
    pub success_message: Option<String>,
    pub next_steps: Option<Vec<String>>,
}

/// Provider-config plan shape, including optional legacy and display fields
/// from the TypeScript interface. The install adapter can consume it through
/// [`into_install_plan`](Self::into_install_plan).
#[derive(Clone, Debug)]
pub struct ProviderConfigInstallPlan {
    pub provider_id: String,
    pub auth_type: AuthType,
    pub env: Vec<(String, String)>,
    pub legacy_credentials: Option<LegacyCredentials>,
    pub model_selection: Option<ProviderModelSelection>,
    pub model_providers: Vec<InstallModelProvidersPatch>,
    pub provider_state: IndexMap<String, IndexMap<String, String>>,
    pub display: Option<ProviderInstallDisplay>,
}

impl ProviderConfigInstallPlan {
    /// Drop UI-only plan metadata and adapt to the shared install executor.
    pub fn into_install_plan(self) -> super::install::ProviderInstallPlan {
        super::install::ProviderInstallPlan {
            provider_id: self.provider_id,
            auth_type: self.auth_type,
            env: self.env,
            legacy_credentials: self.legacy_credentials,
            model_selection: self.model_selection,
            model_providers: self.model_providers,
            provider_state: self.provider_state,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProviderConfigError {
    NoModelsConfigured(String),
    InvalidMetadataKey(String),
}

impl fmt::Display for ProviderConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoModelsConfigured(provider_id) => write!(
                formatter,
                "No models configured for provider \"{provider_id}\". Check model list or provider configuration."
            ),
            Self::InvalidMetadataKey(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for ProviderConfigError {}

/// Resolve a provider environment key, using the setup protocol override when
/// present and otherwise the provider's configured protocol.
pub fn resolve_env_key(config: &ProviderConfig, inputs: &ProviderSetupInputs) -> String {
    config
        .env_key
        .resolve(inputs.protocol.unwrap_or(config.protocol), &inputs.base_url)
}

/// Resolve a model-name prefix for a setup endpoint.
pub fn resolve_model_name_prefix(config: &ProviderConfig, base_url: &str) -> String {
    config.model_name_prefix.resolve(base_url)
}

/// Derive ownership from the config callback, or from static env-key/prefix
/// values exactly when both are non-dynamic. Dynamic config requires an
/// explicit ownership closure.
pub fn resolve_owns_model(config: &ProviderConfig) -> Option<super::install::ModelOwnership> {
    if let Some(owns_model) = &config.owns_model {
        return Some(super::install::ModelOwnership::Predicate(Arc::clone(
            owns_model,
        )));
    }

    let (ProviderEnvKey::Static(env_key), ProviderModelNamePrefix::Static(prefix)) =
        (&config.env_key, &config.model_name_prefix)
    else {
        return None;
    };
    let env_key = env_key.clone();
    let prefix = prefix.clone();
    Some(super::install::ModelOwnership::Predicate(Arc::new(
        move |model| {
            if model.env_key.as_deref() != Some(env_key.as_str()) {
                return false;
            }
            if prefix.is_empty() {
                return true;
            }
            model.name.starts_with(&format!("[{prefix}] "))
        },
    )))
}

/// Resolve the provider metadata key. Configs without a static model list do
/// not receive model-version state; dotted IDs are rejected before writes.
pub fn resolve_metadata_key(
    config: &ProviderConfig,
) -> Result<Option<String>, ProviderConfigError> {
    if config.models.is_none() {
        return Ok(None);
    }
    if config.id.contains('.') {
        return Err(ProviderConfigError::InvalidMetadataKey(format!(
            "Provider id must not contain '.' (would corrupt providerMetadata.{} dotted writes): {}",
            config.id, config.id
        )));
    }
    Ok(Some(config.id.clone()))
}

/// Build model configs from fixed, editable, or user-defined model lists.
pub fn build_model_configs(
    config: &ProviderConfig,
    inputs: &ProviderSetupInputs,
) -> Vec<ProviderModelConfig> {
    build_model_configs_with_projection(config, inputs).0
}

/// Build the full install plan from provider config and setup answers.
pub fn build_install_plan(
    config: &ProviderConfig,
    inputs: &ProviderSetupInputs,
) -> Result<ProviderConfigInstallPlan, ProviderConfigError> {
    let protocol = inputs.protocol.unwrap_or(config.protocol);
    // The TypeScript helper resolves envKey once for the plan and, unless
    // models are prebuilt, again while building model records. Keep that
    // ordering in case a host callback has state.
    let env_key = config.env_key.resolve(protocol, &inputs.base_url);
    let (models, version_projection) = if let Some(prebuilt_models) = &inputs.prebuilt_models {
        (
            prebuilt_models.clone(),
            prebuilt_models
                .iter()
                .map(model_to_prebuilt_projection)
                .collect(),
        )
    } else {
        build_model_configs_with_projection(config, inputs)
    };
    let Some(first_model) = models.first() else {
        return Err(ProviderConfigError::NoModelsConfigured(config.id.clone()));
    };

    let owns_model = if config.merge_models_by_identity {
        None
    } else {
        resolve_owns_model(config)
    };
    let model_selection = Some(ProviderModelSelection {
        model_id: first_model.id.clone(),
        base_url: if config.merge_models_by_identity {
            first_model
                .base_url
                .clone()
                .filter(|base_url| !base_url.is_empty())
        } else {
            None
        },
    });

    let provider_state = match resolve_metadata_key(config)? {
        Some(metadata_key) => {
            let mut values = IndexMap::new();
            values.insert(
                "version".to_owned(),
                compute_projected_model_list_version(&version_projection),
            );
            values.insert("baseUrl".to_owned(), inputs.base_url.clone());
            let mut state = IndexMap::new();
            state.insert(
                format!("{}.{metadata_key}", super::presets::PROVIDER_METADATA_NS),
                values,
            );
            state
        }
        None => IndexMap::new(),
    };

    Ok(ProviderConfigInstallPlan {
        provider_id: config.id.clone(),
        auth_type: protocol,
        env: vec![(env_key, inputs.api_key.clone())],
        legacy_credentials: None,
        model_selection,
        model_providers: vec![InstallModelProvidersPatch {
            auth_type: protocol,
            models,
            merge_strategy: MergeStrategy::PrependAndRemoveOwned,
            owns_model,
        }],
        provider_state,
        display: None,
    })
}

/// Hash a model list using its serialized Rust model-config representation.
/// For plans built here, provider-state hashing uses the more precise ordered
/// projection retained during model construction.
pub fn compute_model_list_version(models: &[ProviderModelConfig]) -> String {
    let projection = models
        .iter()
        .map(model_to_prebuilt_projection)
        .collect::<Vec<_>>();
    compute_projected_model_list_version(&projection)
}

fn build_model_configs_with_projection(
    config: &ProviderConfig,
    inputs: &ProviderSetupInputs,
) -> (Vec<ProviderModelConfig>, Vec<Value>) {
    let protocol = inputs.protocol.unwrap_or(config.protocol);
    let env_key = config.env_key.resolve(protocol, &inputs.base_url);
    let prefix = config.model_name_prefix.resolve(&inputs.base_url);
    let mut models = Vec::new();
    let mut projection = Vec::new();

    match config.models.as_ref() {
        Some(specs) if !config.models_editable => {
            for spec in specs {
                let model = spec_to_model_config(spec, &prefix, &inputs.base_url, &env_key);
                projection.push(model_projection(&model, ProjectionOrder::Spec));
                models.push(model);
            }
        }
        Some(specs) => {
            for id in &inputs.model_ids {
                if let Some(spec) = specs.iter().find(|spec| spec.id == *id) {
                    let model = spec_to_model_config(spec, &prefix, &inputs.base_url, &env_key);
                    projection.push(model_projection(&model, ProjectionOrder::Spec));
                    models.push(model);
                } else {
                    let model = advanced_model_config(
                        id,
                        &prefix,
                        &inputs.base_url,
                        &env_key,
                        inputs.advanced_config.as_ref(),
                    );
                    projection.push(model_projection(&model, ProjectionOrder::Advanced));
                    models.push(model);
                }
            }
        }
        None => {
            for id in &inputs.model_ids {
                let model = advanced_model_config(
                    id,
                    &prefix,
                    &inputs.base_url,
                    &env_key,
                    inputs.advanced_config.as_ref(),
                );
                projection.push(model_projection(&model, ProjectionOrder::Advanced));
                models.push(model);
            }
        }
    }

    if let Some(custom_headers) = &config.custom_headers {
        for (model, serialized_model) in models.iter_mut().zip(&mut projection) {
            apply_custom_headers(model, custom_headers);
            *serialized_model = model_projection_for_headers(serialized_model, model);
        }
    }

    (models, projection)
}

fn spec_to_model_config(
    spec: &ModelSpec,
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
    if let Some(size) = spec
        .context_window_size
        .as_ref()
        .filter(|size| number_is_truthy(size))
    {
        generation_config.context_window_size = Some(normalize_js_number(size));
    }
    if let Some(modalities) = spec
        .modalities
        .filter(|modalities| has_enabled_modality(*modalities))
    {
        generation_config.modalities = Some(modalities);
    }

    ProviderModelConfig {
        id: spec.id.clone(),
        name: display_model_name(prefix, &spec.id),
        description: spec
            .description
            .clone()
            .filter(|description| !description.is_empty()),
        base_url: Some(base_url.to_owned()),
        env_key: Some(env_key.to_owned()),
        image_only: spec.image_only.then_some(true),
        generation_config: generation_config_if_nonempty(generation_config),
    }
}

fn advanced_model_config(
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
            .filter(|modalities| has_enabled_modality(*modalities))
        {
            generation_config.modalities = Some(modalities);
        }
        if let Some(size) = advanced
            .context_window_size
            .filter(|size| *size > 0.0)
            .and_then(number_from_f64)
        {
            generation_config.context_window_size = Some(size);
        }
        if let Some(max_tokens) = advanced
            .max_tokens
            .filter(|max_tokens| *max_tokens > 0.0)
            .and_then(number_from_f64)
        {
            generation_config.sampling_params = Some(SamplingParams { max_tokens });
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

fn apply_custom_headers(
    model: &mut ProviderModelConfig,
    custom_headers: &IndexMap<String, String>,
) {
    let generation_config = model
        .generation_config
        .get_or_insert_with(ProviderGenerationConfig::default);
    let existing = generation_config.custom_headers.take().unwrap_or_default();
    let mut headers = custom_headers
        .iter()
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect::<Vec<_>>();
    for (name, value) in existing.0 {
        if let Some((_, existing_value)) = headers
            .iter_mut()
            .find(|(existing_name, _)| existing_name.as_str() == name.as_str())
        {
            *existing_value = value;
        } else {
            headers.push((name, value));
        }
    }
    generation_config.custom_headers = Some(OrderedHeaders(headers));
}

fn display_model_name(prefix: &str, model_id: &str) -> String {
    if prefix.is_empty() {
        model_id.to_owned()
    } else {
        format!("[{prefix}] {model_id}")
    }
}

fn generation_config_if_nonempty(
    generation_config: ProviderGenerationConfig,
) -> Option<ProviderGenerationConfig> {
    (generation_config.extra_body.is_some()
        || generation_config.thinking_mandatory.is_some()
        || generation_config.context_window_size.is_some()
        || generation_config.modalities.is_some()
        || generation_config.sampling_params.is_some()
        || generation_config.custom_headers.is_some())
    .then_some(generation_config)
}

fn has_enabled_modality(modalities: ConfiguredModalities) -> bool {
    modalities.image == Some(true)
        || modalities.pdf == Some(true)
        || modalities.audio == Some(true)
        || modalities.video == Some(true)
}

fn number_is_truthy(number: &Number) -> bool {
    number.as_f64().is_some_and(|value| value != 0.0)
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

fn normalize_js_number(number: &Number) -> Number {
    if let Some(integer) = number.as_i64() {
        return Number::from(integer);
    }
    if let Some(integer) = number.as_u64() {
        return Number::from(integer);
    }
    number
        .as_f64()
        .and_then(number_from_f64)
        .unwrap_or_else(|| number.clone())
}

#[derive(Clone, Copy)]
enum ProjectionOrder {
    Spec,
    Advanced,
}

fn model_projection(model: &ProviderModelConfig, order: ProjectionOrder) -> Value {
    let mut value = serde_json::to_value(model)
        .expect("ProviderModelConfig always serializes to a JSON object");
    let Some(generation_config) = model.generation_config.as_ref() else {
        return value;
    };
    let mut generation = Map::new();
    match order {
        ProjectionOrder::Spec => {
            insert_generation_fields(
                &mut generation,
                generation_config,
                &[
                    "extra_body",
                    "thinkingMandatory",
                    "contextWindowSize",
                    "modalities",
                    "samplingParams",
                    "customHeaders",
                ],
            );
        }
        ProjectionOrder::Advanced => {
            insert_generation_fields(
                &mut generation,
                generation_config,
                &[
                    "extra_body",
                    "modalities",
                    "contextWindowSize",
                    "samplingParams",
                    "customHeaders",
                ],
            );
        }
    }
    if let Some(object) = value.as_object_mut() {
        object.insert("generationConfig".to_owned(), Value::Object(generation));
    }
    value
}

fn insert_generation_fields(
    target: &mut Map<String, Value>,
    config: &ProviderGenerationConfig,
    order: &[&str],
) {
    for field in order {
        match *field {
            "extra_body" => insert_serialized(target, field, &config.extra_body),
            "thinkingMandatory" => insert_serialized(target, field, &config.thinking_mandatory),
            "contextWindowSize" => insert_serialized(target, field, &config.context_window_size),
            "modalities" => insert_serialized(target, field, &config.modalities),
            "samplingParams" => insert_serialized(target, field, &config.sampling_params),
            "customHeaders" => insert_serialized(target, field, &config.custom_headers),
            _ => unreachable!("generation field order is declared locally"),
        }
    }
}

fn insert_serialized<T: serde::Serialize>(target: &mut Map<String, Value>, key: &str, value: &T) {
    let value = serde_json::to_value(value).expect("provider generation field serializes");
    if !value.is_null() {
        target.insert(key.to_owned(), value);
    }
}

fn model_to_prebuilt_projection(model: &ProviderModelConfig) -> Value {
    let mut value = serde_json::to_value(model)
        .expect("ProviderModelConfig always serializes to a JSON object");
    if let (Some(config), Some(object)) = (model.generation_config.as_ref(), value.as_object_mut())
    {
        // Prebuilt models do not expose their original JavaScript property
        // order through the Rust model type, so retain the shared struct's
        // serialization order as the best available projection.
        let serialized = serde_json::to_value(config)
            .expect("ProviderGenerationConfig always serializes to an object");
        object.insert("generationConfig".to_owned(), serialized);
    }
    value
}

fn model_projection_for_headers(original: &Value, model: &ProviderModelConfig) -> Value {
    let Some(generation_config) = model.generation_config.as_ref() else {
        return original.clone();
    };
    let mut value = original.clone();
    let Some(object) = value.as_object_mut() else {
        return value;
    };
    let mut generation = object
        .get("generationConfig")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    if let Some(headers) = &generation_config.custom_headers {
        let serialized = serde_json::to_value(headers)
            .expect("OrderedHeaders always serializes to a JSON object");
        generation.insert("customHeaders".to_owned(), serialized);
    }
    object.insert("generationConfig".to_owned(), Value::Object(generation));
    value
}

fn compute_projected_model_list_version(models: &[Value]) -> String {
    let serialized = serde_json::to_vec(models).expect("provider projections serialize to JSON");
    let digest = Sha256::digest(serialized);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}
