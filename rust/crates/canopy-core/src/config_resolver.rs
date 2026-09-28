//! Resolve configuration values from ordered sources.
//!
//! Port of `packages/core/src/utils/configResolver.ts`.

use std::collections::HashMap;

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

/// Known categories of configuration value sources.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ConfigSourceKind {
    Cli,
    Env,
    Settings,
    ModelProviders,
    Default,
    Computed,
    Programmatic,
    Unknown,
}

/// Metadata describing where a resolved configuration value came from.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigSource {
    pub kind: ConfigSourceKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub env_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub settings_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_id: Option<String>,
    /// The source from which this value was derived.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub via: Option<Box<ConfigSource>>,
}

impl ConfigSource {
    pub fn new(kind: ConfigSourceKind) -> Self {
        Self {
            kind,
            detail: None,
            env_key: None,
            settings_path: None,
            auth_type: None,
            model_id: None,
            via: None,
        }
    }
}

/// Map of configuration field names to source metadata.
/// Field source metadata in insertion order, like the source JavaScript record.
pub type ConfigSources = IndexMap<String, ConfigSource>;

/// A potential value in the precedence-ordered configuration stack.
#[derive(Clone, Debug, PartialEq)]
pub struct ConfigLayer<T> {
    /// `None` means that this layer did not supply a value.
    pub value: Option<T>,
    pub source: ConfigSource,
}

/// A resolved value paired with the metadata for its winning source.
#[derive(Clone, Debug, PartialEq)]
pub struct ResolvedField<T> {
    pub value: T,
    pub source: ConfigSource,
}

/// Values can customize the source resolver's absent-value rules.
///
/// JavaScript's resolver treats `undefined`, `null`, and whitespace-only
/// strings as absent. `Option<T>` represents undefined and also supports
/// nullable values recursively; `serde_json::Value::Null` represents JSON
/// null. Other values are present by default when implementing this trait.
pub trait ConfigValuePresence {
    fn is_config_value_present(&self) -> bool;
}

impl ConfigValuePresence for String {
    fn is_config_value_present(&self) -> bool {
        !self.trim().is_empty()
    }
}

impl ConfigValuePresence for str {
    fn is_config_value_present(&self) -> bool {
        !self.trim().is_empty()
    }
}

impl ConfigValuePresence for &str {
    fn is_config_value_present(&self) -> bool {
        !self.trim().is_empty()
    }
}

impl<T: ConfigValuePresence> ConfigValuePresence for Option<T> {
    fn is_config_value_present(&self) -> bool {
        self.as_ref()
            .is_some_and(ConfigValuePresence::is_config_value_present)
    }
}

impl ConfigValuePresence for serde_json::Value {
    fn is_config_value_present(&self) -> bool {
        match self {
            serde_json::Value::Null => false,
            serde_json::Value::String(value) => !value.trim().is_empty(),
            _ => true,
        }
    }
}

macro_rules! impl_always_present {
    ($($type:ty),+ $(,)?) => {
        $(
            impl ConfigValuePresence for $type {
                fn is_config_value_present(&self) -> bool { true }
            }
        )+
    };
}

impl_always_present!(
    bool, char, u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, i128, isize, f32, f64
);

impl<T> ConfigValuePresence for Vec<T> {
    fn is_config_value_present(&self) -> bool {
        true
    }
}

impl<K, V, S> ConfigValuePresence for HashMap<K, V, S> {
    fn is_config_value_present(&self) -> bool {
        true
    }
}

/// Resolve to the first present layer, or use the supplied default source.
pub fn resolve_field<T: ConfigValuePresence + Clone>(
    layers: &[ConfigLayer<T>],
    default_value: T,
) -> ResolvedField<T> {
    resolve_field_with_source(layers, default_value, default_source(None::<String>))
}

/// Resolve to the first present layer, using explicit metadata for the default.
pub fn resolve_field_with_source<T: ConfigValuePresence + Clone>(
    layers: &[ConfigLayer<T>],
    default_value: T,
    default_source: ConfigSource,
) -> ResolvedField<T> {
    resolve_optional_field(layers).unwrap_or(ResolvedField {
        value: default_value,
        source: default_source,
    })
}

/// Resolve the first present layer, returning `None` when no layer is present.
pub fn resolve_optional_field<T: ConfigValuePresence + Clone>(
    layers: &[ConfigLayer<T>],
) -> Option<ResolvedField<T>> {
    layers.iter().find_map(|layer| {
        layer
            .value
            .as_ref()
            .filter(|value| value.is_config_value_present())
            .map(|value| ResolvedField {
                value: value.clone(),
                source: layer.source.clone(),
            })
    })
}

pub fn cli_source(detail: impl Into<String>) -> ConfigSource {
    let mut source = ConfigSource::new(ConfigSourceKind::Cli);
    source.detail = Some(detail.into());
    source
}

pub fn env_source(env_key: impl Into<String>) -> ConfigSource {
    let mut source = ConfigSource::new(ConfigSourceKind::Env);
    source.env_key = Some(env_key.into());
    source
}

pub fn settings_source(settings_path: impl Into<String>) -> ConfigSource {
    let mut source = ConfigSource::new(ConfigSourceKind::Settings);
    source.settings_path = Some(settings_path.into());
    source
}

pub fn model_providers_source(
    auth_type: impl Into<String>,
    model_id: impl Into<String>,
    detail: Option<String>,
) -> ConfigSource {
    let mut source = ConfigSource::new(ConfigSourceKind::ModelProviders);
    source.auth_type = Some(auth_type.into());
    source.model_id = Some(model_id.into());
    source.detail = detail;
    source
}

pub fn default_source(detail: Option<String>) -> ConfigSource {
    let mut source = ConfigSource::new(ConfigSourceKind::Default);
    source.detail = detail;
    source
}

pub fn computed_source(detail: Option<String>) -> ConfigSource {
    let mut source = ConfigSource::new(ConfigSourceKind::Computed);
    source.detail = detail;
    source
}

/// Build a layer from an environment map without transforming its string value.
pub fn env_layer(env: &HashMap<String, String>, key: impl Into<String>) -> ConfigLayer<String> {
    let key = key.into();
    ConfigLayer {
        value: env.get(&key).cloned(),
        source: env_source(key),
    }
}

/// Build a layer from an environment map, applying `transform` when the key exists.
///
/// As in the TypeScript source, empty input is still passed to the transform;
/// absence is decided later by `resolve_field` or `resolve_optional_field`.
pub fn env_layer_with<T>(
    env: &HashMap<String, String>,
    key: impl Into<String>,
    transform: impl FnOnce(&str) -> T,
) -> ConfigLayer<T> {
    let key = key.into();
    ConfigLayer {
        value: env.get(&key).map(|raw| transform(raw)),
        source: env_source(key),
    }
}

/// Build a layer from a value and its source metadata.
pub fn layer<T>(value: Option<T>, source: ConfigSource) -> ConfigLayer<T> {
    ConfigLayer { value, source }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn resolves_first_present_layer_and_preserves_its_source() {
        let layers = vec![
            layer(None::<String>, cli_source("--model")),
            layer(Some("model-from-env".to_owned()), env_source("MODEL")),
            layer(
                Some("model-from-settings".to_owned()),
                settings_source("model.name"),
            ),
        ];

        assert_eq!(
            resolve_field(&layers, "fallback".to_owned()),
            ResolvedField {
                value: "model-from-env".to_owned(),
                source: env_source("MODEL"),
            }
        );
    }

    #[test]
    fn resolve_uses_default_and_explicit_default_source_when_layers_are_absent() {
        let layers = vec![
            layer(None::<String>, cli_source("--model")),
            layer(Some(" \t\n ".to_owned()), settings_source("model.name")),
        ];
        let source = default_source(Some("configured default".to_owned()));

        assert_eq!(
            resolve_field_with_source(&layers, "fallback".to_owned(), source.clone()),
            ResolvedField {
                value: "fallback".to_owned(),
                source,
            }
        );
    }

    #[test]
    fn optional_resolution_skips_missing_null_and_whitespace_string_values() {
        let layers = vec![
            layer(None::<serde_json::Value>, cli_source("--model")),
            layer(Some(json!(null)), env_source("MODEL_NULL")),
            layer(Some(json!("  \t")), env_source("MODEL_BLANK")),
            layer(Some(json!("chosen")), settings_source("model.name")),
        ];

        assert_eq!(
            resolve_optional_field(&layers),
            Some(ResolvedField {
                value: json!("chosen"),
                source: settings_source("model.name"),
            })
        );
    }

    #[test]
    fn optional_resolution_returns_none_if_no_layer_is_present() {
        let layers = vec![
            layer(None::<String>, cli_source("--model")),
            layer(Some(String::new()), env_source("MODEL")),
            layer(Some("\n\t ".to_owned()), settings_source("model.name")),
        ];
        assert_eq!(resolve_optional_field(&layers), None);
    }

    #[test]
    fn nullable_values_and_non_string_falsy_values_follow_source_presence_rules() {
        let nested_none: Option<Option<String>> = Some(None);
        assert!(!nested_none.is_config_value_present());
        assert!(Some(Some("value".to_owned())).is_config_value_present());

        let bool_layers = vec![layer(Some(false), computed_source(None::<String>))];
        assert!(!resolve_optional_field(&bool_layers).unwrap().value);
        let zero_layers = vec![layer(Some(0_i64), computed_source(None::<String>))];
        assert_eq!(resolve_optional_field(&zero_layers).unwrap().value, 0);
        let null_layers = vec![layer(Some(serde_json::Value::Null), env_source("NULL"))];
        assert_eq!(resolve_optional_field(&null_layers), None);
    }

    #[test]
    fn source_constructors_populate_kind_and_metadata_fields() {
        let mut via = env_source("ORIGINAL");
        via.detail = Some("underlying source".to_owned());
        let source = model_providers_source("oauth", "model-1", Some("provider config".to_owned()));
        let mut derived = computed_source(Some("resolved alias".to_owned()));
        derived.via = Some(Box::new(via.clone()));

        assert_eq!(cli_source("--model").detail.as_deref(), Some("--model"));
        assert_eq!(env_source("KEY").env_key.as_deref(), Some("KEY"));
        assert_eq!(
            settings_source("model.name").settings_path.as_deref(),
            Some("model.name")
        );
        assert_eq!(source.kind, ConfigSourceKind::ModelProviders);
        assert_eq!(source.auth_type.as_deref(), Some("oauth"));
        assert_eq!(source.model_id.as_deref(), Some("model-1"));
        assert_eq!(source.detail.as_deref(), Some("provider config"));
        assert_eq!(
            default_source(None::<String>).kind,
            ConfigSourceKind::Default
        );
        assert_eq!(derived.kind, ConfigSourceKind::Computed);
        assert_eq!(derived.via.as_deref(), Some(&via));
    }

    #[test]
    fn source_metadata_serializes_to_the_typescript_field_names_and_kind_values() {
        let mut source = model_providers_source("oauth", "model-1", None);
        source.via = Some(Box::new(settings_source("model.name")));

        assert_eq!(
            serde_json::to_value(source).unwrap(),
            json!({
                "kind": "modelProviders",
                "authType": "oauth",
                "modelId": "model-1",
                "via": {"kind": "settings", "settingsPath": "model.name"}
            })
        );
    }

    #[test]
    fn environment_layer_records_key_and_applies_transform_only_when_present() {
        let env = HashMap::from([
            ("COUNT".to_owned(), " 42 ".to_owned()),
            ("BLANK".to_owned(), "   ".to_owned()),
        ]);
        let count = env_layer_with(&env, "COUNT", |raw| raw.trim().parse::<u32>().unwrap());
        assert_eq!(count.value, Some(42));
        assert_eq!(count.source, env_source("COUNT"));

        let mut called = false;
        let absent = env_layer_with(&env, "MISSING", |_| {
            called = true;
            1_u32
        });
        assert_eq!(absent.value, None);
        assert!(!called);

        let blank = env_layer(&env, "BLANK");
        assert_eq!(blank.value.as_deref(), Some("   "));
        assert_eq!(resolve_optional_field(&[blank]), None);

        let blank_transform = env_layer_with(&env, "BLANK", |raw| raw.trim().len());
        assert_eq!(blank_transform.value, Some(0));
    }

    #[test]
    fn static_layer_keeps_the_supplied_source_and_value() {
        let static_value = layer(Some(7_u32), cli_source("--workers"));
        assert_eq!(static_value.value, Some(7));
        assert_eq!(static_value.source, cli_source("--workers"));
    }
}
