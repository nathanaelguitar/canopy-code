//! User-facing settings warnings collected by the TypeScript settings loader.
//!
//! Source: `packages/cli/src/config/settings.ts`.

use std::collections::HashSet;
use std::path::PathBuf;

use serde_json::Value;

use crate::config::loader::{LoadedSettings, SettingScope, SettingsFile};
use crate::config::migrations::{is_v2_container_key, v1_to_v2_migration_map};
use crate::config::schema::settings_schema;

/// Collects migration, ignored legacy-key, model-provider, and workspace
/// security warnings in the same order as Canopy's settings UI.
pub fn get_settings_warnings(settings: &LoadedSettings) -> Vec<String> {
    let mut warnings = Vec::new();
    let mut seen = HashSet::new();

    for warning in &settings.migration_warnings {
        push_unique(&mut warnings, &mut seen, format!("Warning: {warning}"));
    }

    for scope in [SettingScope::User, SettingScope::Workspace] {
        let file = settings.for_scope(scope);
        if file.raw_json.is_none() {
            continue;
        }
        for warning in legacy_key_warnings(file) {
            push_unique(&mut warnings, &mut seen, warning);
        }
    }

    for warning in model_provider_override_warnings(settings) {
        push_unique(&mut warnings, &mut seen, warning);
    }

    let workspace = &settings.workspace;
    if workspace.raw_json.is_some() {
        for key in ["allowPrivateNetworkHooks", "allowedInsecureVoiceBaseUrls"] {
            if workspace
                .original_settings
                .get("security")
                .and_then(Value::as_object)
                .is_some_and(|security| security.contains_key(key))
            {
                push_unique(
                    &mut warnings,
                    &mut seen,
                    format!(
                        "Warning: security.{key} in workspace settings ({}) is ignored. This setting is only honored from User, System, or SystemDefaults scope settings.",
                        workspace.path.display()
                    ),
                );
            }
        }
    }

    warnings
}

/// Lists unknown top-level keys in current-version user and workspace files.
/// TypeScript sends these to its debug logger instead of returning them from
/// `getSettingsWarnings`; callers can preserve that distinction at integration.
pub fn unknown_setting_keys(settings: &LoadedSettings) -> Vec<(PathBuf, String)> {
    let schema = settings_schema();
    let mut unknown = Vec::new();
    for scope in [SettingScope::User, SettingScope::Workspace] {
        let file = settings.for_scope(scope);
        if file.raw_json.is_none() || !is_current_version_file(file) {
            continue;
        }
        let ignored_legacy_keys = v1_to_v2_migration_map()
            .iter()
            .filter_map(|(old_key, new_path)| {
                if old_key == new_path || !file.original_settings.contains_key(*old_key) {
                    return None;
                }
                let value = &file.original_settings[*old_key];
                if is_v2_container_key(old_key) && value.is_object() {
                    None
                } else {
                    Some(*old_key)
                }
            })
            .collect::<HashSet<_>>();
        for key in file.original_settings.keys() {
            if key != "$version"
                && !ignored_legacy_keys.contains(key.as_str())
                && !schema.contains_key(key)
            {
                unknown.push((file.path.clone(), key.clone()));
            }
        }
    }
    unknown
}

fn legacy_key_warnings(file: &SettingsFile) -> Vec<String> {
    if !is_current_version_file(file) {
        return Vec::new();
    }
    let mut warnings = Vec::new();
    for (old_key, new_path) in v1_to_v2_migration_map() {
        if old_key == new_path || !file.original_settings.contains_key(*old_key) {
            continue;
        }
        let value = &file.original_settings[*old_key];
        if is_v2_container_key(old_key) && value.is_object() {
            continue;
        }
        warnings.push(format!(
            "Warning: Legacy setting '{old_key}' will be ignored in {}. Please use '{new_path}' instead.",
            file.path.display()
        ));
    }
    warnings
}

fn is_current_version_file(file: &SettingsFile) -> bool {
    file.original_settings
        .get("$version")
        .and_then(Value::as_f64)
        .is_some_and(|version| version >= 4.0)
}

fn model_provider_override_warnings(settings: &LoadedSettings) -> Vec<String> {
    if !settings.is_trusted {
        return Vec::new();
    }

    let user = &settings.user.original_settings;
    let workspace = &settings.workspace.original_settings;
    let (Some(user_providers), Some(workspace_providers)) =
        (user.get("modelProviders"), workspace.get("modelProviders"))
    else {
        return Vec::new();
    };

    let Some(workspace_object) = workspace_providers.as_object() else {
        return Vec::new();
    };
    if !workspace_object.is_empty() || !has_provider_entries(user_providers) {
        return Vec::new();
    }

    vec![format!(
        "Warning: '{}' defines an empty 'modelProviders' object. This has no effect with current merge behavior, but may indicate a configuration error. If REPLACE semantics are introduced for 'modelProviders' in the future, this would override user-level model providers in '{}'.",
        settings.workspace.path.display(),
        settings.user.path.display()
    )]
}

fn has_provider_entries(value: &Value) -> bool {
    value.as_object().is_some_and(|providers| {
        providers
            .values()
            .any(|models| models.as_array().is_some_and(|models| !models.is_empty()))
    })
}

fn push_unique(warnings: &mut Vec<String>, seen: &mut HashSet<String>, warning: String) {
    if seen.insert(warning.clone()) {
        warnings.push(warning);
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use serde_json::json;

    use super::{get_settings_warnings, unknown_setting_keys};
    use crate::config::loader::{LoadedSettings, SettingsFile};

    fn file(settings: serde_json::Map<String, serde_json::Value>, name: &str) -> SettingsFile {
        SettingsFile {
            original_settings: settings.clone(),
            settings,
            path: PathBuf::from(name),
            raw_json: Some("{}".to_owned()),
        }
    }

    fn loaded(user: SettingsFile, workspace: SettingsFile) -> LoadedSettings {
        let empty = SettingsFile::default();
        LoadedSettings {
            system: empty.clone(),
            system_defaults: empty,
            user,
            workspace,
            is_trusted: true,
            migrated_in_memory_scopes: Default::default(),
            migration_warnings: vec!["migration notice".to_owned(), "migration notice".to_owned()],
            corrupted_path: None,
            was_recovered: false,
            workspace_settings_active: true,
            settings_errors: Vec::new(),
            merged: Default::default(),
            runtime_environment: Default::default(),
        }
    }

    #[test]
    fn emits_deduplicated_legacy_provider_and_security_warnings() {
        let user = file(
            json!({
                "$version": 4,
                "theme": "legacy",
                "modelProviders": {"openai": [{"name": "gpt"}]},
                "mystery": true
            })
            .as_object()
            .unwrap()
            .clone(),
            "user.json",
        );
        let workspace = file(
            json!({
                "$version": 4,
                "modelProviders": {},
                "security": {"allowPrivateNetworkHooks": true}
            })
            .as_object()
            .unwrap()
            .clone(),
            "workspace.json",
        );
        let loaded = loaded(user, workspace);
        let warnings = get_settings_warnings(&loaded);
        assert_eq!(warnings[0], "Warning: migration notice");
        assert_eq!(warnings.len(), 4);
        assert!(warnings[1].contains("Legacy setting 'theme'"));
        assert!(warnings[2].contains("empty 'modelProviders' object"));
        assert!(warnings[3].contains("security.allowPrivateNetworkHooks"));
        assert_eq!(
            unknown_setting_keys(&loaded),
            vec![(PathBuf::from("user.json"), "mystery".to_owned())]
        );
    }

    #[test]
    fn ignores_warnings_for_untrusted_workspace_and_precurrent_versions() {
        let user = file(
            json!({"$version": 3, "theme": "old"})
                .as_object()
                .unwrap()
                .clone(),
            "user",
        );
        let mut workspace = file(
            json!({"$version": 4, "security": {"allowedInsecureVoiceBaseUrls": ["x"]}})
                .as_object()
                .unwrap()
                .clone(),
            "workspace",
        );
        workspace.raw_json = None;
        let mut loaded = loaded(user, workspace);
        loaded.is_trusted = false;
        assert_eq!(
            get_settings_warnings(&loaded),
            vec!["Warning: migration notice"]
        );
        assert!(unknown_setting_keys(&loaded).is_empty());
    }
}
