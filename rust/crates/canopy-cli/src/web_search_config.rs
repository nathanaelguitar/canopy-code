// Copyright 2026 Canopy Team
// SPDX-License-Identifier: Apache-2.0
//! WebSearch setting and backend resolution for the CLI host.
//!
//! The resolver accepts model entries already flattened by the host. Each
//! entry must carry the effective provider auth type and the generation
//! config headers for that exact entry, since `RuntimeSettings` does not
//! currently retain provider metadata.

use std::collections::HashMap;

use canopy_core::providers::dashscope::DEFAULT_DASHSCOPE_BASE_URL;
use canopy_core::utils::error_parsing::AuthType;
use serde_json::Value;

const DASH_SCOPE_REGIONAL_HOSTS: &[&str] = &[
    "dashscope.aliyuncs.com",
    "dashscope-intl.aliyuncs.com",
    "dashscope-us.aliyuncs.com",
];
const DASH_SCOPE_EXTRA_HOST_SUFFIXES: &[&str] =
    &["maas.aliyuncs.com", "alibaba-inc.com", "aliyun-inc.com"];

/// Resolved `tools.webSearch` settings with the TypeScript environment
/// override precedence applied.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WebSearchSettings {
    pub enabled: Option<bool>,
    pub model: Option<String>,
    pub web_extractor: Option<bool>,
    pub base_url: Option<String>,
    pub api_key_env: Option<String>,
}

impl WebSearchSettings {
    pub fn is_enabled(&self) -> bool {
        self.enabled == Some(true)
    }
}

/// Read WebSearch configuration from merged settings and an effective
/// environment snapshot. Empty environment values are treated as unset.
pub fn resolve_web_search_settings(
    settings: &Value,
    environment: &HashMap<String, String>,
) -> Option<WebSearchSettings> {
    let web_search = settings
        .get("tools")
        .and_then(|tools| tools.get("webSearch"));

    let enabled = nonempty_env(environment, "ENABLE_WEB_SEARCH")
        .map(|value| is_truthy(value))
        .or_else(|| {
            web_search
                .and_then(|value| value.get("enabled"))
                .and_then(Value::as_bool)
        });
    let model = nonempty_env(environment, "WEB_SEARCH_MODEL")
        .map(str::to_owned)
        .or_else(|| {
            web_search
                .and_then(|value| value.get("model"))
                .and_then(Value::as_str)
                .map(str::to_owned)
        });
    let web_extractor = nonempty_env(environment, "WEB_SEARCH_EXTRACTOR")
        .map(|value| is_truthy(value))
        .or_else(|| {
            web_search
                .and_then(|value| value.get("webExtractor"))
                .and_then(Value::as_bool)
        });
    let base_url = nonempty_env(environment, "WEB_SEARCH_BASE_URL").map(str::to_owned);
    let api_key_env = base_url.as_ref().map(|_| {
        if nonempty_env(environment, "WEB_SEARCH_API_KEY").is_some() {
            "WEB_SEARCH_API_KEY".to_owned()
        } else {
            "DASHSCOPE_API_KEY".to_owned()
        }
    });

    if enabled.is_none() && model.is_none() && web_extractor.is_none() && base_url.is_none() {
        return None;
    }

    Some(WebSearchSettings {
        enabled,
        model,
        web_extractor,
        base_url,
        api_key_env,
    })
}

/// One fully resolved model-provider entry in the same order used by the
/// host's `getAllConfiguredModels`. The host supplies headers from the exact
/// resolved entry rather than the metadata-only model list.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SearchModelEntry {
    pub auth_type: AuthType,
    pub id: String,
    pub base_url: Option<String>,
    pub env_key: Option<String>,
    pub custom_headers: Option<Vec<(String, String)>>,
}

/// Model data required to mirror `buildModelIdContext` and the subsequent
/// `modelProviders` lookup.
#[derive(Clone, Copy, Debug, Default)]
pub struct WebSearchModelContext<'a> {
    pub current_model: Option<&'a str>,
    pub current_auth_type: Option<AuthType>,
    pub fast_model: Option<&'a str>,
    /// Flattened entries must preserve configured provider/model order.
    pub model_entries: &'a [SearchModelEntry],
}

/// Backend values needed to create the core WebSearch executor. The CLI host
/// converts `custom_headers` to `reqwest::HeaderMap` at the executor boundary.
#[derive(Clone, Eq, PartialEq)]
pub struct ResolvedSearchBackendConfig {
    pub model_id: String,
    pub api_key: String,
    pub api_key_env_key: String,
    pub base_url: String,
    pub web_extractor: bool,
    pub custom_headers: Vec<(String, String)>,
}

impl std::fmt::Debug for ResolvedSearchBackendConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResolvedSearchBackendConfig")
            .field("model_id", &self.model_id)
            .field("api_key", &"[REDACTED]")
            .field("api_key_env_key", &self.api_key_env_key)
            .field("base_url", &self.base_url)
            .field("web_extractor", &self.web_extractor)
            .field("custom_headers", &self.custom_headers)
            .finish()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WebSearchGateNotice {
    pub message: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WebSearchGateResult {
    Ready(ResolvedSearchBackendConfig),
    Notice(WebSearchGateNotice),
}

/// Evaluate whether a configured search selector and backend can be used.
/// `enabled` remains a separate host decision, matching the TypeScript gate:
/// callers invoke this only when registering or rechecking an enabled tool.
pub fn evaluate_web_search_gate(
    settings: Option<&WebSearchSettings>,
    model_context: &WebSearchModelContext<'_>,
    environment: &HashMap<String, String>,
) -> WebSearchGateResult {
    let selector = settings
        .and_then(|settings| settings.model.as_deref())
        .map(str::trim)
        .filter(|selector| !selector.is_empty());
    let Some(selector) = selector else {
        return notice(format!(
            "WebSearch is enabled but no search model is configured.\n\
Add a search model to settings.json (recommended: qwen3.6-plus):\n\
  {{\n\
    \"tools\": {{ \"webSearch\": {{ \"enabled\": true, \"model\": \"qwen3.6-plus\" }} }},\n\
    \"modelProviders\": {{\n\
      \"openai\": [{{ \"id\": \"qwen3.6-plus\",\n\
        \"baseUrl\": \"{DEFAULT_DASHSCOPE_BASE_URL}\",\n\
        \"envKey\": \"DASHSCOPE_API_KEY\" }}]\n\
    }}\n\
  }}\n\
Or via env: ENABLE_WEB_SEARCH=true WEB_SEARCH_MODEL=qwen3.6-plus\n\
WEB_SEARCH_BASE_URL={DEFAULT_DASHSCOPE_BASE_URL} (plus WEB_SEARCH_API_KEY)."
        ));
    };

    let resolved = match resolve_model_selector(selector, model_context) {
        Ok(resolved) => resolved,
        Err(error) => {
            return notice(format!(
                "WebSearch is enabled but the search model selector \"{selector}\" is invalid: {error}"
            ));
        }
    };

    if let Some(settings) = settings.filter(|settings| settings.base_url.is_some()) {
        let base_url = settings.base_url.as_deref().unwrap_or_default();
        match classify_dashscope_base_url(base_url) {
            BaseUrlIssue::Compatible => {}
            BaseUrlIssue::Insecure => {
                return notice(format!(
                    "WebSearch is enabled but WEB_SEARCH_BASE_URL ({base_url}) uses plaintext HTTP. The search request carries a bearer API key; use an https:// endpoint."
                ));
            }
            BaseUrlIssue::Invalid | BaseUrlIssue::UnknownHost => {
                return notice(format!(
                    "WebSearch is enabled but WEB_SEARCH_BASE_URL ({base_url}) is not a DashScope-compatible endpoint."
                ));
            }
        }
        let key_env = settings
            .api_key_env
            .as_deref()
            .unwrap_or("DASHSCOPE_API_KEY");
        let Some(api_key) = nonempty_env(environment, key_env) else {
            return notice(format!(
                "WebSearch is enabled with WEB_SEARCH_BASE_URL but the API key variable {key_env} is not set. Set WEB_SEARCH_API_KEY (or DASHSCOPE_API_KEY)."
            ));
        };
        let Some(resolved) = resolved else {
            return notice(format!(
                "WebSearch is enabled but the search model selector \"{selector}\" could not be resolved."
            ));
        };
        return WebSearchGateResult::Ready(ResolvedSearchBackendConfig {
            model_id: resolved.model_id,
            api_key: api_key.to_owned(),
            api_key_env_key: key_env.to_owned(),
            base_url: base_url.to_owned(),
            web_extractor: settings.web_extractor != Some(false),
            custom_headers: Vec::new(),
        });
    }

    let Some(resolved) = resolved else {
        return notice(format!(
            "WebSearch is enabled but the search model selector \"{selector}\" could not be resolved."
        ));
    };
    let matches = model_context
        .model_entries
        .iter()
        .filter(|entry| {
            entry.id == resolved.model_id
                && resolved
                    .auth_type
                    .is_none_or(|auth_type| entry.auth_type == auth_type)
        })
        .collect::<Vec<_>>();
    if matches.is_empty() {
        return notice(format!(
            "WebSearch is enabled but the search model \"{selector}\" does not match any model declared under modelProviders."
        ));
    }
    let entry = matches
        .iter()
        .copied()
        .find(|entry| is_usable_entry(entry, environment))
        .unwrap_or(matches[0]);

    if entry.auth_type == AuthType::CanopyOauth {
        return notice(format!(
            "WebSearch search model \"{selector}\" resolves to a Canopy OAuth entry. The search side channel needs a modelProviders entry with a direct API key (envKey); OAuth tokens cannot back it. Use an authType-qualified selector (e.g. \"openai:<model-id>\") to target a specific entry."
        ));
    }
    let Some(base_url) = entry.base_url.as_deref() else {
        return notice(format!(
            "WebSearch search model \"{selector}\" resolves to a non-DashScope endpoint (no baseUrl). The web_search backend requires a DashScope-compatible baseUrl."
        ));
    };
    match classify_dashscope_base_url(base_url) {
        BaseUrlIssue::Compatible => {}
        BaseUrlIssue::Insecure => {
            return notice(format!(
                "WebSearch search model \"{selector}\" resolves to a plaintext-HTTP endpoint ({base_url}). The search request carries a bearer API key; use an https:// baseUrl."
            ));
        }
        BaseUrlIssue::Invalid | BaseUrlIssue::UnknownHost => {
            return notice(format!(
                "WebSearch search model \"{selector}\" resolves to a non-DashScope endpoint ({base_url}). The web_search backend requires a DashScope-compatible baseUrl."
            ));
        }
    }
    let Some(key_env) = entry.env_key.as_deref() else {
        return notice(format!(
            "WebSearch search model \"{selector}\" has no envKey on its modelProviders entry. Declare the API key environment variable name there."
        ));
    };
    let Some(api_key) = nonempty_env(environment, key_env) else {
        return notice(format!(
            "WebSearch search model \"{selector}\" reads its API key from {key_env}, which is not set in the environment."
        ));
    };

    WebSearchGateResult::Ready(ResolvedSearchBackendConfig {
        model_id: entry.id.clone(),
        api_key: api_key.to_owned(),
        api_key_env_key: key_env.to_owned(),
        base_url: base_url.to_owned(),
        web_extractor: settings.is_none_or(|settings| settings.web_extractor != Some(false)),
        custom_headers: entry.custom_headers.clone().unwrap_or_default(),
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ResolvedModelSelector {
    auth_type: Option<AuthType>,
    model_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ModelSelector {
    Inherit,
    Fast,
    Model {
        auth_type: Option<AuthType>,
        model_id: String,
    },
}

fn resolve_model_selector(
    selector: &str,
    context: &WebSearchModelContext<'_>,
) -> Result<Option<ResolvedModelSelector>, &'static str> {
    match parse_model_selector(selector)? {
        ModelSelector::Inherit => Ok(context.current_model.map(|model_id| ResolvedModelSelector {
            auth_type: context.current_auth_type,
            model_id: model_id.to_owned(),
        })),
        ModelSelector::Model {
            auth_type,
            model_id,
        } => {
            let resolved_auth_type =
                auth_type.or_else(|| {
                    if let Some(current_auth_type) = context.current_auth_type {
                        if context.model_entries.iter().any(|entry| {
                            entry.auth_type == current_auth_type && entry.id == model_id
                        }) {
                            return Some(current_auth_type);
                        }
                    }
                    context
                        .model_entries
                        .iter()
                        .find(|entry| entry.id == model_id)
                        .map(|entry| entry.auth_type)
                        .or(context.current_auth_type)
                });
            Ok(Some(ResolvedModelSelector {
                auth_type: resolved_auth_type,
                model_id,
            }))
        }
        ModelSelector::Fast => {
            let Some(fast_model) = context.fast_model else {
                return Ok(None);
            };
            match parse_model_selector(fast_model)? {
                ModelSelector::Fast => Ok(None),
                ModelSelector::Inherit => {
                    Ok(context.current_model.map(|model_id| ResolvedModelSelector {
                        auth_type: context.current_auth_type,
                        model_id: model_id.to_owned(),
                    }))
                }
                ModelSelector::Model {
                    auth_type,
                    model_id,
                } => {
                    let narrowed_context = WebSearchModelContext {
                        fast_model: None,
                        ..*context
                    };
                    resolve_model_selector(
                        &format_selector(auth_type, &model_id),
                        &narrowed_context,
                    )
                }
            }
        }
    }
}

fn format_selector(auth_type: Option<AuthType>, model_id: &str) -> String {
    match auth_type {
        Some(auth_type) => format!("{}:{model_id}", auth_type.as_str()),
        None => model_id.to_owned(),
    }
}

fn parse_model_selector(selector: &str) -> Result<ModelSelector, &'static str> {
    let selector = selector.trim();
    if selector.is_empty() || selector == "inherit" {
        return Ok(ModelSelector::Inherit);
    }
    if selector == "fast" {
        return Ok(ModelSelector::Fast);
    }
    let Some((prefix, model_id)) = selector.split_once(':') else {
        return Ok(ModelSelector::Model {
            auth_type: None,
            model_id: selector.to_owned(),
        });
    };
    let maybe_auth_type = prefix.trim();
    let Some(auth_type) = parse_auth_type(maybe_auth_type) else {
        return Ok(ModelSelector::Model {
            auth_type: None,
            model_id: selector.to_owned(),
        });
    };
    let model_id = model_id.trim();
    if model_id.is_empty() {
        return Err("Model selector must include a model ID after the authType");
    }
    Ok(ModelSelector::Model {
        auth_type: Some(auth_type),
        model_id: model_id.to_owned(),
    })
}

fn parse_auth_type(value: &str) -> Option<AuthType> {
    match value {
        "openai" => Some(AuthType::OpenAi),
        "canopy-oauth" => Some(AuthType::CanopyOauth),
        "chatgpt-oauth" => Some(AuthType::ChatgptOauth),
        "gemini" => Some(AuthType::Gemini),
        "vertex-ai" => Some(AuthType::VertexAi),
        "anthropic" => Some(AuthType::Anthropic),
        _ => None,
    }
}

fn is_usable_entry(entry: &SearchModelEntry, environment: &HashMap<String, String>) -> bool {
    entry.auth_type != AuthType::CanopyOauth
        && entry.base_url.as_deref().is_some_and(|base_url| {
            classify_dashscope_base_url(base_url) == BaseUrlIssue::Compatible
        })
        && entry
            .env_key
            .as_deref()
            .is_some_and(|key| nonempty_env(environment, key).is_some())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BaseUrlIssue {
    Compatible,
    Invalid,
    Insecure,
    UnknownHost,
}

fn classify_dashscope_base_url(base_url: &str) -> BaseUrlIssue {
    let value = base_url.trim();
    let Some((scheme, after_scheme)) = value.split_once("://") else {
        return BaseUrlIssue::Invalid;
    };
    if scheme.is_empty()
        || !scheme.as_bytes()[0].is_ascii_alphabetic()
        || !scheme
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'-' | b'.'))
    {
        return BaseUrlIssue::Invalid;
    }
    let authority = after_scheme
        .split(['/', '?', '#'])
        .next()
        .unwrap_or_default();
    let authority = authority.rsplit('@').next().unwrap_or_default();
    if authority.is_empty() || authority.chars().any(char::is_whitespace) {
        return BaseUrlIssue::Invalid;
    }
    let host = if let Some(bracketed) = authority.strip_prefix('[') {
        let Some((host, remainder)) = bracketed.split_once(']') else {
            return BaseUrlIssue::Invalid;
        };
        if !remainder.is_empty() && (!remainder.starts_with(':') || !valid_port(&remainder[1..])) {
            return BaseUrlIssue::Invalid;
        }
        host
    } else {
        let (host, port) = match authority.rsplit_once(':') {
            Some((host, port)) if !host.contains(':') => {
                if !valid_port(port) {
                    return BaseUrlIssue::Invalid;
                }
                (host, Some(port))
            }
            Some(_) => return BaseUrlIssue::Invalid,
            None => (authority, None),
        };
        let _ = port;
        host
    };
    if host.is_empty() || host.ends_with('.') || host.starts_with('.') {
        return BaseUrlIssue::Invalid;
    }
    if scheme.eq_ignore_ascii_case("https") {
        let hostname = host.to_ascii_lowercase();
        let compatible = DASH_SCOPE_REGIONAL_HOSTS
            .iter()
            .chain(DASH_SCOPE_EXTRA_HOST_SUFFIXES)
            .any(|suffix| hostname == *suffix || hostname.ends_with(&format!(".{suffix}")));
        if compatible {
            BaseUrlIssue::Compatible
        } else {
            BaseUrlIssue::UnknownHost
        }
    } else {
        BaseUrlIssue::Insecure
    }
}

fn valid_port(port: &str) -> bool {
    !port.is_empty() && port.parse::<u16>().is_ok()
}

fn nonempty_env<'a>(environment: &'a HashMap<String, String>, name: &str) -> Option<&'a str> {
    environment
        .get(name)
        .map(String::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn is_truthy(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

fn notice(message: impl Into<String>) -> WebSearchGateResult {
    WebSearchGateResult::Notice(WebSearchGateNotice {
        message: message.into(),
    })
}
