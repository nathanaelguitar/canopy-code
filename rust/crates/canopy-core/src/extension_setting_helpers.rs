// Copyright 2025 Google LLC
// SPDX-License-Identifier: Apache-2.0
//! Pure helpers for extension setting validation and change classification.

use serde::{Deserialize, Serialize};

const INVALID_ENV_VAR_MESSAGE: &str =
    "Extension setting \"envVar\" must be a valid environment variable name.";

/// An extension setting as declared by an extension manifest.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExtensionSetting {
    pub name: String,
    pub description: String,
    pub env_var: String,
    /// Missing source values default to non-sensitive.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sensitive: Option<bool>,
}

impl ExtensionSetting {
    pub fn is_sensitive(&self) -> bool {
        self.sensitive.unwrap_or(false)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ExtensionSettingsChanges {
    pub prompt_for_sensitive: Vec<ExtensionSetting>,
    pub remove_sensitive: Vec<ExtensionSetting>,
    pub prompt_for_env: Vec<ExtensionSetting>,
    pub remove_env: Vec<ExtensionSetting>,
}

/// Check a setting's environment variable name against the source's ASCII
/// pattern: `^[A-Za-z_][A-Za-z0-9_]*$`.
pub fn is_valid_extension_setting_env_var(env_var: &str) -> bool {
    let mut characters = env_var.bytes();
    let Some(first) = characters.next() else {
        return false;
    };
    (first.is_ascii_alphabetic() || first == b'_')
        && characters.all(|character| character.is_ascii_alphanumeric() || character == b'_')
}

/// Validate an optional settings list. The error text matches the TypeScript
/// validation error exactly.
pub fn validate_extension_setting_env_vars(
    settings: Option<&[ExtensionSetting]>,
) -> Result<(), String> {
    if settings.is_some_and(|settings| {
        settings
            .iter()
            .any(|setting| !is_valid_extension_setting_env_var(&setting.env_var))
    }) {
        return Err(INVALID_ENV_VAR_MESSAGE.to_owned());
    }
    Ok(())
}

/// Classify which settings need prompting or removal based only on env var
/// name and effective sensitivity. Missing `sensitive` is treated as false.
pub fn get_settings_changes(
    settings: &[ExtensionSetting],
    old_settings: &[ExtensionSetting],
) -> ExtensionSettingsChanges {
    ExtensionSettingsChanges {
        prompt_for_sensitive: settings
            .iter()
            .filter(|setting| setting.is_sensitive())
            .filter(|setting| !old_settings.iter().any(|old| same_setting(setting, old)))
            .cloned()
            .collect(),
        remove_sensitive: old_settings
            .iter()
            .filter(|setting| setting.is_sensitive())
            .filter(|setting| !settings.iter().any(|new| same_setting(setting, new)))
            .cloned()
            .collect(),
        prompt_for_env: settings
            .iter()
            .filter(|setting| !setting.is_sensitive())
            .filter(|setting| !old_settings.iter().any(|old| same_setting(setting, old)))
            .cloned()
            .collect(),
        remove_env: old_settings
            .iter()
            .filter(|setting| !setting.is_sensitive())
            .filter(|setting| !settings.iter().any(|new| same_setting(setting, new)))
            .cloned()
            .collect(),
    }
}

fn same_setting(left: &ExtensionSetting, right: &ExtensionSetting) -> bool {
    left.env_var == right.env_var && left.is_sensitive() == right.is_sensitive()
}

/// Format ordered environment entries as dotenv lines. Values are wrapped in
/// literal double quotes only when they contain an ASCII space, matching the
/// source formatter; quotes and other characters are not escaped.
pub fn format_env_content<K, V>(settings: &[(K, V)]) -> String
where
    K: AsRef<str>,
    V: AsRef<str>,
{
    let mut content = String::new();
    for (key, value) in settings {
        let value = value.as_ref();
        content.push_str(key.as_ref());
        content.push('=');
        if value.contains(' ') {
            content.push('"');
            content.push_str(value);
            content.push('"');
        } else {
            content.push_str(value);
        }
        content.push('\n');
    }
    content
}
