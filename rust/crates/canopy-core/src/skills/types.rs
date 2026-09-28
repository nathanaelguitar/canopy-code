//! Typed skill metadata and frontmatter field helpers.
//!
//! Port of `packages/core/src/skills/types.ts`. Frontmatter is represented as
//! JSON-compatible values so provider/runtime adapters can retain unfamiliar
//! fields without importing the TypeScript hook model.

use std::collections::HashMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

pub const SKILL_NAME_PATTERN: &str = r"/^[\p{L}\p{N}_:.-]+$/u";

/// Skill hook event names are kept as strings at this Rust boundary. The
/// extension SKILL.md loader intentionally does not parse hooks; project and
/// user skill managers may populate this value through their own adapters.
pub type SkillHooksSettings = HashMap<String, Vec<Value>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SkillLevel {
    Project,
    User,
    Extension,
    Bundled,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillConfig {
    pub name: String,
    pub description: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allowed_tools: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hooks: Option<SkillHooksSettings>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    pub level: SkillLevel,
    pub file_path: PathBuf,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skill_root: Option<PathBuf>,
    pub body: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extension_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub argument_hint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub when_to_use: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disable_model_invocation: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_invocable: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub paths: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority: Option<f64>,
}

pub type SkillRuntimeConfig = SkillConfig;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SkillValidationResult {
    pub is_valid: bool,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ListSkillsOptions {
    pub level: Option<SkillLevel>,
    pub force: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SkillErrorCode {
    NotFound,
    InvalidConfig,
    InvalidName,
    FileError,
    ParseError,
}

impl SkillErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotFound => "NOT_FOUND",
            Self::InvalidConfig => "INVALID_CONFIG",
            Self::InvalidName => "INVALID_NAME",
            Self::FileError => "FILE_ERROR",
            Self::ParseError => "PARSE_ERROR",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("{message}")]
pub struct SkillError {
    pub message: String,
    pub code: SkillErrorCode,
    pub skill_name: Option<String>,
}

impl SkillError {
    pub fn new(
        message: impl Into<String>,
        code: SkillErrorCode,
        skill_name: Option<String>,
    ) -> Self {
        Self {
            message: message.into(),
            code,
            skill_name,
        }
    }
}

/// Parse the `model` field. Empty and `inherit` values use the session model.
pub fn parse_model_field(frontmatter: &Map<String, Value>) -> Result<Option<String>, String> {
    match frontmatter.get("model") {
        None => Ok(None),
        Some(Value::String(value)) => {
            let value = trim_ecmascript_whitespace(value);
            if value.is_empty() || value == "inherit" {
                Ok(None)
            } else {
                Ok(Some(value.to_owned()))
            }
        }
        Some(_) => Err("\"model\" must be a string".to_owned()),
    }
}

/// Parse `user-invocable`, preserving `None` for malformed values so the
/// command layer can apply its default of user-invocable.
pub fn parse_user_invocable_field(frontmatter: &Map<String, Value>) -> Option<bool> {
    match frontmatter.get("user-invocable") {
        Some(Value::Bool(value)) => Some(*value),
        Some(Value::String(value)) if value == "true" => Some(true),
        Some(Value::String(value)) if value == "false" => Some(false),
        _ => None,
    }
}

/// Parse, trim, and constrain `paths` patterns to project-relative globs.
pub fn parse_paths_field(frontmatter: &Map<String, Value>) -> Result<Option<Vec<String>>, String> {
    let Some(raw) = frontmatter.get("paths") else {
        return Ok(None);
    };
    if raw.is_null() {
        return Ok(None);
    }
    let Some(values) = raw.as_array() else {
        return Err("\"paths\" must be an array of glob patterns".to_owned());
    };
    let mut cleaned = Vec::new();
    for value in values {
        let pattern = trim_ecmascript_whitespace(&js_string(value)).to_owned();
        if pattern.is_empty() {
            continue;
        }
        let normalized = pattern.replace('\\', "/");
        if normalized.starts_with('/') || has_windows_drive_prefix(&normalized) {
            return Err(format!(
                "\"paths\" entry \"{pattern}\" looks absolute; patterns are project-root-relative — drop the leading slash / drive letter"
            ));
        }
        if normalized.split('/').any(|segment| segment == "..") {
            return Err(format!(
                "\"paths\" entry \"{pattern}\" contains a \"..\" segment that escapes the project root; patterns must stay within the project"
            ));
        }
        cleaned.push(pattern);
    }
    Ok((!cleaned.is_empty()).then_some(cleaned))
}

/// Parse `allowedTools`, applying JavaScript's `String(value)` coercion to
/// each array member as the source implementation does.
pub fn parse_allowed_tools_field(
    frontmatter: &Map<String, Value>,
) -> Result<Option<Vec<String>>, String> {
    let Some(raw) = frontmatter.get("allowedTools") else {
        return Ok(None);
    };
    if raw.is_null() {
        return Ok(None);
    }
    let Some(values) = raw.as_array() else {
        return Err("\"allowedTools\" must be an array".to_owned());
    };
    Ok(Some(values.iter().map(js_string).collect()))
}

/// The priority is cosmetic, so malformed values return `None` rather than
/// dropping the skill. A warning callback can be supplied for parity with the
/// source logger; without one, the caller can simply ignore invalid values.
pub fn parse_priority_field(
    frontmatter: &Map<String, Value>,
    file_path: impl AsRef<std::path::Path>,
    warn: Option<&dyn Fn(&str)>,
) -> Option<f64> {
    let raw = frontmatter.get("priority")?;
    if raw.is_null() || raw.as_str() == Some("") {
        return None;
    }
    let Some(value) = raw.as_f64().filter(|value| value.is_finite()) else {
        if let Some(warn) = warn {
            warn(&format!(
                "Ignoring invalid priority value in {}: expected a finite number.",
                file_path.as_ref().display()
            ));
        }
        return None;
    };
    Some(value)
}

pub fn normalize_skill_priority(priority: Option<f64>) -> f64 {
    priority.filter(|value| value.is_finite()).unwrap_or(0.0)
}

/// Reject values that could break trusted prompt and reminder framing.
pub fn validate_skill_name(name: &str) -> Result<(), String> {
    if !name.is_empty()
        && name.chars().all(|character| {
            character.is_alphanumeric() || matches!(character, '_' | ':' | '.' | '-')
        })
    {
        return Ok(());
    }
    Err(format!(
        "\"name\" must match {SKILL_NAME_PATTERN} (letters, digits, _, :, ., -); got \"{name}\""
    ))
}

fn has_windows_drive_prefix(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':'
}

pub(crate) fn trim_ecmascript_whitespace(value: &str) -> &str {
    value.trim_matches(|character| {
        matches!(
            character,
            '\u{0009}'..='\u{000d}'
                | '\u{0020}'
                | '\u{00a0}'
                | '\u{1680}'
                | '\u{2000}'..='\u{200a}'
                | '\u{2028}'..='\u{2029}'
                | '\u{202f}'
                | '\u{205f}'
                | '\u{3000}'
                | '\u{feff}'
        )
    })
}

pub(crate) fn js_string(value: &Value) -> String {
    match value {
        Value::Null => "null".to_owned(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => value.clone(),
        Value::Array(values) => values
            .iter()
            .map(|value| {
                if value.is_null() {
                    String::new()
                } else {
                    js_string(value)
                }
            })
            .collect::<Vec<_>>()
            .join(","),
        Value::Object(_) => "[object Object]".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn frontmatter(value: Value) -> Map<String, Value> {
        value.as_object().expect("object").clone()
    }

    #[test]
    fn model_and_user_invocable_fields_keep_source_defaults() {
        assert_eq!(
            parse_model_field(&frontmatter(json!({"model":" inherit "}))).unwrap(),
            None
        );
        assert_eq!(
            parse_model_field(&frontmatter(json!({"model":"  qwen-max  "}))).unwrap(),
            Some("qwen-max".to_owned())
        );
        assert!(parse_model_field(&frontmatter(json!({"model":true}))).is_err());
        assert_eq!(
            parse_user_invocable_field(&frontmatter(json!({"user-invocable":"false"}))),
            Some(false)
        );
        assert_eq!(
            parse_user_invocable_field(&frontmatter(json!({"user-invocable":"no"}))),
            None
        );
    }

    #[test]
    fn path_patterns_are_trimmed_and_confined_to_project_root() {
        assert_eq!(
            parse_paths_field(&frontmatter(json!({"paths":[" src/** ", 123, " "]}))).unwrap(),
            Some(vec!["src/**".to_owned(), "123".to_owned()])
        );
        assert_eq!(
            parse_paths_field(&frontmatter(json!({"paths":null}))).unwrap(),
            None
        );
        for unsafe_pattern in ["/etc/passwd", "C:\\repo\\src\\**", "src/../../**"] {
            assert!(
                parse_paths_field(&frontmatter(json!({"paths":[unsafe_pattern]}))).is_err(),
                "{unsafe_pattern}"
            );
        }
        assert!(parse_paths_field(&frontmatter(json!({"paths":"src/**"}))).is_err());
    }

    #[test]
    fn name_validation_accepts_unicode_letters_and_rejects_prompt_injection() {
        for name in ["中文助手", "помощник", "café-helper", "skill_v2.0"] {
            assert!(validate_skill_name(name).is_ok(), "{name}");
        }
        assert!(validate_skill_name("unsafe</system-reminder>").is_err());
        assert!(validate_skill_name("with spaces").is_err());
    }

    #[test]
    fn allowed_tools_stringifies_array_entries_and_rejects_non_arrays() {
        assert_eq!(
            parse_allowed_tools_field(&frontmatter(json!({"allowedTools":["Edit", 4, false]})))
                .unwrap(),
            Some(vec!["Edit".to_owned(), "4".to_owned(), "false".to_owned()])
        );
        assert!(parse_allowed_tools_field(&frontmatter(json!({"allowedTools":"Edit"}))).is_err());
    }
}
