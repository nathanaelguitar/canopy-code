//! Provider-neutral image-generation model selection.
//!
//! The host supplies configured model candidates; this module parses the
//! selector, disambiguates the route, and validates the explicitly configured
//! HTTPS endpoint without owning the model registry.

use url::Url;

const AUTH_TYPES: &[&str] = &[
    "openai",
    "canopy-oauth",
    "chatgpt-oauth",
    "gemini",
    "vertex-ai",
    "anthropic",
];

/// Parsed `imageModel` / `visionModel` selector and its optional endpoint pin.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedVisionModelSetting {
    pub selector: String,
    pub base_url: Option<String>,
}

/// Minimal configured-model data needed to resolve image generation.
///
/// `selected_base_url` is the base URL used to match an optional selector
/// endpoint. `registry_base_url` is the explicit endpoint registered by the
/// host and is required for image generation; protocol defaults are not used.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ImageGenerationModelCandidate {
    pub model_id: String,
    pub auth_type: String,
    pub selected_base_url: Option<String>,
    pub registry_base_url: Option<String>,
    pub env_key: Option<String>,
    pub image_only: bool,
    pub fast_only: bool,
    pub voice_only: bool,
}

/// Validated configuration consumed by the image-generation tool.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ImageGenerationConfig {
    pub model: String,
    pub base_url: String,
    pub api_key_env: String,
}

/// Parse a selector with an optional NUL-delimited exact base URL.
///
/// This mirrors `parseVisionModelSetting`: an empty whole setting or an empty
/// selector is absent, and an empty suffix after the delimiter means no URL
/// pin. The base URL itself is preserved byte-for-byte for route matching.
pub fn parse_vision_model_setting(setting: Option<&str>) -> Option<ParsedVisionModelSetting> {
    let setting = setting?;
    if setting.is_empty() {
        return None;
    }
    if let Some((selector, base_url)) = setting.split_once('\0') {
        if selector.is_empty() {
            return None;
        }
        Some(ParsedVisionModelSetting {
            selector: selector.to_owned(),
            base_url: (!base_url.is_empty()).then(|| base_url.to_owned()),
        })
    } else {
        Some(ParsedVisionModelSetting {
            selector: setting.to_owned(),
            base_url: None,
        })
    }
}

/// Resolve a selected image-only model route. The selector must match exactly
/// one host-provided candidate after model ID, optional auth type, optional
/// selected base URL, and capability filtering.
pub fn resolve_image_generation_model(
    setting: Option<&str>,
    candidates: &[ImageGenerationModelCandidate],
) -> Option<ImageGenerationConfig> {
    let parsed = parse_vision_model_setting(setting)?;
    let selector = resolve_model_selector(&parsed.selector)?;

    let mut matches = candidates.iter().filter(|candidate| {
        candidate.image_only
            && !candidate.fast_only
            && !candidate.voice_only
            && candidate.model_id == selector.model_id
            && selector
                .auth_type
                .as_deref()
                .is_none_or(|auth_type| candidate.auth_type == auth_type)
            && parsed
                .base_url
                .as_deref()
                .is_none_or(|base_url| candidate.selected_base_url.as_deref() == Some(base_url))
    });
    let candidate = matches.next()?;
    if matches.next().is_some() {
        return None;
    }

    let api_key_env = candidate
        .env_key
        .as_deref()
        .map(trim_ecmascript_whitespace)
        .filter(|value| !value.is_empty())?;
    let registry_base_url = candidate
        .registry_base_url
        .as_deref()
        .map(trim_ecmascript_whitespace)
        .filter(|value| !value.is_empty())?;
    let base_url = normalize_image_generation_base_url(
        parsed.base_url.as_deref().or(Some(registry_base_url)),
    )?;

    Some(ImageGenerationConfig {
        model: candidate.model_id.clone(),
        base_url,
        api_key_env: api_key_env.to_owned(),
    })
}

/// Resolve the configured image model unless bare or safe mode disables the
/// feature, matching `Config.getImageGenerationConfig()`.
pub fn get_image_generation_config(
    setting: Option<&str>,
    candidates: &[ImageGenerationModelCandidate],
    bare_mode: bool,
    safe_mode: bool,
) -> Option<ImageGenerationConfig> {
    if bare_mode || safe_mode {
        return None;
    }
    resolve_image_generation_model(setting, candidates)
}

pub fn is_image_generation_enabled(
    setting: Option<&str>,
    candidates: &[ImageGenerationModelCandidate],
    bare_mode: bool,
    safe_mode: bool,
) -> bool {
    get_image_generation_config(setting, candidates, bare_mode, safe_mode).is_some()
}

/// Validate and normalize a model's explicitly configured image-generation
/// endpoint. Only HTTPS URLs without credentials or nonempty query/fragment
/// components are accepted; all trailing slash characters are removed.
pub fn normalize_image_generation_base_url(value: Option<&str>) -> Option<String> {
    let value = value.map(trim_ecmascript_whitespace)?;
    if value.is_empty() {
        return None;
    }
    let parsed = Url::parse(value).ok()?;
    if parsed.scheme() != "https"
        || !parsed.username().is_empty()
        || parsed
            .password()
            .is_some_and(|password| !password.is_empty())
        || parsed.query().is_some_and(|query| !query.is_empty())
        || parsed
            .fragment()
            .is_some_and(|fragment| !fragment.is_empty())
    {
        return None;
    }
    Some(parsed.as_str().trim_end_matches('/').to_owned())
}

struct ResolvedSelector {
    model_id: String,
    auth_type: Option<String>,
}

/// This slice has no configured parent/fast model context, so `inherit` and
/// `fast` selectors cannot resolve. Qualified forms mirror resolveModelId's
/// known-auth-prefix behavior; unknown prefixes remain part of the model ID.
fn resolve_model_selector(selector: &str) -> Option<ResolvedSelector> {
    let selector = trim_ecmascript_whitespace(selector);
    if selector.is_empty() || selector == "inherit" || selector == "fast" {
        return None;
    }
    let Some((maybe_auth_type, model_id)) = selector.split_once(':') else {
        return Some(ResolvedSelector {
            model_id: selector.to_owned(),
            auth_type: None,
        });
    };
    let maybe_auth_type = trim_ecmascript_whitespace(maybe_auth_type);
    let model_id = trim_ecmascript_whitespace(model_id);
    if AUTH_TYPES.contains(&maybe_auth_type) {
        if model_id.is_empty() {
            return None;
        }
        Some(ResolvedSelector {
            model_id: model_id.to_owned(),
            auth_type: Some(maybe_auth_type.to_owned()),
        })
    } else {
        Some(ResolvedSelector {
            model_id: selector.to_owned(),
            auth_type: None,
        })
    }
}

fn trim_ecmascript_whitespace(value: &str) -> &str {
    value.trim_matches(is_ecmascript_whitespace)
}

fn is_ecmascript_whitespace(character: char) -> bool {
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
}
