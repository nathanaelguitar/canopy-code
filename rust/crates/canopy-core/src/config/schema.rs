//! Rust view of Canopy's canonical settings schema.
//!
//! `settings-schema.json` is generated from
//! `packages/cli/src/config/settingsSchema.ts` with `getSettingsSchema()`. It
//! carries nested setting types, defaults, validation bounds, editor metadata,
//! and the merge strategies used by the settings loader.

use std::sync::OnceLock;

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::settings_merge::MergeStrategy;

pub const MAX_SETTING_STRING_VALUE_LENGTH: usize = 1024;
const CANONICAL_SETTINGS_SCHEMA: &str = include_str!("settings-schema.json");

pub type SettingsSchema = IndexMap<String, SettingDefinition>;
/// JSON Schema setting type spellings used by the canonical schema.
pub type SettingType = String;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SettingMergeStrategy {
    Replace,
    Concat,
    Union,
    ShallowMerge,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SettingDefinition {
    #[serde(rename = "type")]
    pub setting_type: String,
    #[serde(default)]
    pub label: String,
    #[serde(default)]
    pub category: String,
    #[serde(default)]
    pub requires_restart: bool,
    #[serde(default)]
    pub default: Option<Value>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub parent_key: Option<String>,
    #[serde(default)]
    pub key: Option<String>,
    #[serde(default)]
    pub properties: Option<SettingsSchema>,
    #[serde(default)]
    pub show_in_dialog: Option<bool>,
    #[serde(default)]
    pub merge_strategy: Option<SettingMergeStrategy>,
    #[serde(default)]
    pub options: Vec<SettingEnumOption>,
    #[serde(default)]
    pub items: Option<Value>,
    #[serde(default)]
    pub minimum: Option<f64>,
    #[serde(default)]
    pub maximum: Option<f64>,
    /// Raw JSON Schema constraints supplied for settings whose public schema
    /// is more expressive than the editor's simple `type` field.
    #[serde(default)]
    pub json_schema_override: Option<Value>,
    /// Deprecated value types accepted only for migration compatibility.
    #[serde(default)]
    pub legacy_types: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SettingEnumOption {
    pub value: Value,
    #[serde(default)]
    pub label: String,
}

/// Returns the canonical nested schema. The schema JSON is a generated
/// snapshot, so callers can use it without a Node runtime.
pub fn settings_schema() -> &'static SettingsSchema {
    static SCHEMA: OnceLock<SettingsSchema> = OnceLock::new();
    SCHEMA.get_or_init(|| {
        serde_json::from_str(CANONICAL_SETTINGS_SCHEMA)
            .expect("embedded canonical Canopy settings schema must be valid")
    })
}

pub fn setting_definition(path: &str) -> Option<&'static SettingDefinition> {
    let mut schema = settings_schema();
    let segments = path.split('.').collect::<Vec<_>>();
    let mut definition = None;
    for (index, segment) in segments.iter().enumerate() {
        let current = schema.get(*segment)?;
        definition = Some(current);
        if index + 1 < segments.len() {
            schema = current.properties.as_ref()?;
        }
    }
    definition
}

/// Lookup the merge strategy declared for a nested schema path.
pub fn merge_strategy_for_path(path: &[String]) -> Option<MergeStrategy> {
    let mut schema = settings_schema();
    let mut definition = None;
    for (index, segment) in path.iter().enumerate() {
        let current = schema.get(segment)?;
        definition = Some(current);
        if index + 1 < path.len() {
            schema = current.properties.as_ref()?;
        }
    }
    definition.and_then(|definition| {
        definition.merge_strategy.map(|strategy| match strategy {
            SettingMergeStrategy::Replace => MergeStrategy::Replace,
            SettingMergeStrategy::Concat => MergeStrategy::Concat,
            SettingMergeStrategy::Union => MergeStrategy::Union,
            SettingMergeStrategy::ShallowMerge => MergeStrategy::ShallowMerge,
        })
    })
}

/// Mirrors `validateSettingValue` in `packages/cli/src/utils/settingsUtils.ts`.
/// This validation is used by the settings mutation API; startup loading does
/// not reject unknown or schema-invalid user values.
pub fn validate_setting_value(definition: &SettingDefinition, value: &Value) -> Option<String> {
    match definition.setting_type.as_str() {
        "boolean" => {
            if !value.is_boolean() {
                return Some("Value must be a boolean".to_owned());
            }
        }
        "number" => {
            let Some(number) = value.as_f64() else {
                return Some("Value must be a finite number".to_owned());
            };
            if definition.minimum.is_some_and(|minimum| number < minimum) {
                return Some(format!("Value must be >= {}", definition.minimum.unwrap()));
            }
            if definition.maximum.is_some_and(|maximum| number > maximum) {
                return Some(format!("Value must be <= {}", definition.maximum.unwrap()));
            }
        }
        "integer" => {
            let Some(number) = value.as_f64() else {
                return Some("Value must be a finite integer".to_owned());
            };
            if number.fract() != 0.0 {
                return Some("Value must be an integer".to_owned());
            }
            if definition.minimum.is_some_and(|minimum| number < minimum) {
                return Some(format!("Value must be >= {}", definition.minimum.unwrap()));
            }
            if definition.maximum.is_some_and(|maximum| number > maximum) {
                return Some(format!("Value must be <= {}", definition.maximum.unwrap()));
            }
        }
        "string" => {
            let Some(string) = value.as_str() else {
                return Some("Value must be a string".to_owned());
            };
            if string.encode_utf16().count() > MAX_SETTING_STRING_VALUE_LENGTH {
                return Some(format!(
                    "Value exceeds {MAX_SETTING_STRING_VALUE_LENGTH}-character limit"
                ));
            }
        }
        "enum" => {
            if !definition
                .options
                .iter()
                .any(|option| js_strict_primitive_equal(&option.value, value))
            {
                let allowed = definition
                    .options
                    .iter()
                    .map(|option| js_string(&option.value))
                    .collect::<Vec<_>>()
                    .join(", ");
                return Some(format!("Value must be one of: {allowed}"));
            }
        }
        "object" => {
            if !value.is_object() {
                return Some("Value must be an object".to_owned());
            }
        }
        other => {
            return Some(format!(
                "Settings of type '{other}' cannot be modified via this API"
            ));
        }
    }
    None
}

fn js_strict_primitive_equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Null, Value::Null) => true,
        (Value::Bool(left), Value::Bool(right)) => left == right,
        (Value::Number(left), Value::Number(right)) => left.as_f64() == right.as_f64(),
        (Value::String(left), Value::String(right)) => left == right,
        _ => false,
    }
}

fn js_string(value: &Value) -> String {
    match value {
        Value::Null => "null".to_owned(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => value.clone(),
        Value::Array(_) => value
            .as_array()
            .expect("array value")
            .iter()
            .map(|item| match item {
                Value::Null => String::new(),
                _ => js_string(item),
            })
            .collect::<Vec<_>>()
            .join(","),
        Value::Object(_) => "[object Object]".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        MAX_SETTING_STRING_VALUE_LENGTH, merge_strategy_for_path, setting_definition,
        settings_schema, validate_setting_value,
    };
    use crate::settings_merge::MergeStrategy;

    #[test]
    fn embedded_schema_exposes_settings_metadata_and_nested_lookups() {
        assert!(settings_schema().contains_key("modelProviders"));
        let definition = setting_definition("tools.approvalMode").expect("known setting");
        assert_eq!(definition.setting_type, "enum");
        assert_eq!(definition.category, "Tools");
        assert!(!definition.requires_restart);
        assert_eq!(definition.default, Some(json!("auto")));
        assert!(setting_definition("tools.unknown").is_none());
        assert!(
            setting_definition("telemetry")
                .unwrap()
                .json_schema_override
                .is_some()
        );
        assert_eq!(
            setting_definition("general.gitCoAuthor")
                .unwrap()
                .legacy_types,
            vec!["boolean"]
        );
    }

    #[test]
    fn merge_strategy_lookup_uses_full_dotted_path() {
        assert_eq!(
            merge_strategy_for_path(&["mcpServers".to_owned()]),
            Some(MergeStrategy::ShallowMerge)
        );
        assert_eq!(
            merge_strategy_for_path(&["tools".to_owned(), "exclude".to_owned()]),
            Some(MergeStrategy::Union)
        );
        assert_eq!(merge_strategy_for_path(&["unknown".to_owned()]), None);
    }

    #[test]
    fn validation_matches_boolean_enum_and_object_rules() {
        let boolean = setting_definition("general.vimMode").unwrap();
        assert_eq!(validate_setting_value(boolean, &json!(true)), None);
        assert_eq!(
            validate_setting_value(boolean, &json!("true")),
            Some("Value must be a boolean".to_owned())
        );

        let approval = setting_definition("tools.approvalMode").unwrap();
        assert_eq!(validate_setting_value(approval, &json!("auto")), None);
        assert_eq!(
            validate_setting_value(approval, &json!("unsafe")),
            Some("Value must be one of: plan, default, auto-edit, auto, yolo".to_owned())
        );

        let servers = setting_definition("mcpServers").unwrap();
        assert_eq!(validate_setting_value(servers, &json!({"local": {}})), None);
        assert_eq!(
            validate_setting_value(servers, &json!([])),
            Some("Value must be an object".to_owned())
        );
    }

    #[test]
    fn validation_uses_utf16_string_length_and_number_bounds() {
        let string_definition = setting_definition("general.preferredEditor").unwrap();
        let exact_limit = "😀".repeat(MAX_SETTING_STRING_VALUE_LENGTH / 2);
        assert_eq!(
            validate_setting_value(string_definition, &json!(exact_limit)),
            None
        );
        let over_limit = format!("{}x", "a".repeat(MAX_SETTING_STRING_VALUE_LENGTH));
        assert_eq!(
            validate_setting_value(string_definition, &json!(over_limit)),
            Some(format!(
                "Value exceeds {MAX_SETTING_STRING_VALUE_LENGTH}-character limit"
            ))
        );

        let integer = setting_definition("serve.maxConcurrentSubSessionsTotal").unwrap();
        assert_eq!(validate_setting_value(integer, &json!(24)), None);
        assert_eq!(
            validate_setting_value(integer, &json!(1.5)),
            Some("Value must be an integer".to_owned())
        );
        assert_eq!(
            validate_setting_value(integer, &json!(1025)),
            Some("Value must be <= 1024".to_owned())
        );
    }

    #[test]
    fn array_and_unrecognized_types_are_not_editable_through_the_setting_api() {
        let array = setting_definition("serve.channels").unwrap();
        assert_eq!(
            validate_setting_value(array, &json!([])),
            Some("Settings of type 'array' cannot be modified via this API".to_owned())
        );
    }
}
