//! Settings migrations used by the TypeScript settings loader.
//!
//! Source: `packages/cli/src/config/migration/{index,scheduler}.ts` and the
//! version files under `packages/cli/src/config/migration/versions/`.

use serde_json::{Map, Number, Value};

pub const SETTINGS_VERSION: u64 = 4;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MigrationStep {
    pub from_version: u64,
    pub to_version: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MigrationResult {
    pub settings: Value,
    pub final_version: f64,
    pub executed_migrations: Vec<MigrationStep>,
    pub warnings: Vec<String>,
}

/// Runs the same forward and downgrade migration chain as Canopy's settings
/// loader. The input is never mutated.
pub fn run_migrations(settings: &Value, scope: &str) -> MigrationResult {
    let mut current = settings.clone();
    let mut executed_migrations = Vec::new();
    let mut warnings = Vec::new();
    let scope = format_scope(scope);

    if v1_to_v2_applies(&current) {
        current = migrate_v1_to_v2(&current);
        executed_migrations.push(MigrationStep {
            from_version: 1,
            to_version: 2,
        });
    }
    if get_version(&current).is_some_and(|version| version == 2.0) {
        let (next, next_warnings) = migrate_v2_to_v3(&current, &scope);
        current = next;
        warnings.extend(next_warnings);
        executed_migrations.push(MigrationStep {
            from_version: 2,
            to_version: 3,
        });
    }
    if v3_to_v4_applies(&current) {
        let (next, next_warnings) = migrate_v3_to_v4(&current, &scope);
        current = next;
        warnings.extend(next_warnings);
        executed_migrations.push(MigrationStep {
            from_version: 3,
            to_version: 4,
        });
    }
    if v5_to_v4_applies(&current) {
        let (next, next_warnings) = migrate_v5_to_v4(&current);
        current = next;
        warnings.extend(next_warnings);
        executed_migrations.push(MigrationStep {
            from_version: 5,
            to_version: 4,
        });
    }

    let final_version = get_version(&current).unwrap_or(1.0);
    MigrationResult {
        settings: current,
        final_version,
        executed_migrations,
        warnings,
    }
}

/// Returns whether at least one registered migration can run for `settings`.
/// A current version file is left alone even if legacy-shaped keys remain.
pub fn settings_need_migration(settings: &Value) -> bool {
    let Some(object) = settings.as_object() else {
        return false;
    };
    let current_version = object.get("$version").and_then(Value::as_f64);
    if current_version == Some(SETTINGS_VERSION as f64) {
        return false;
    }
    v1_to_v2_applies(settings)
        || current_version == Some(2.0)
        || v3_to_v4_applies(settings)
        || v5_to_v4_applies(settings)
}

pub fn format_scope(scope: &str) -> String {
    if scope == "SystemDefaults" {
        "system default".to_owned()
    } else {
        scope.to_lowercase()
    }
}

fn get_version(settings: &Value) -> Option<f64> {
    settings.as_object()?.get("$version")?.as_f64()
}

fn is_plain_object(value: &Value) -> bool {
    value.is_object()
}

const V1_INDICATORS: &[&str] = &[
    "theme",
    "model",
    "autoAccept",
    "hideTips",
    "vimMode",
    "checkpointing",
    "accessibility",
    "allowedTools",
    "allowMCPServers",
    "autoConfigureMaxOldSpaceSize",
    "bugCommand",
    "chatCompression",
    "coreTools",
    "contextFileName",
    "customThemes",
    "customWittyPhrases",
    "debugKeystrokeLogging",
    "dnsResolutionOrder",
    "enforcedAuthType",
    "excludeTools",
    "excludeMCPServers",
    "excludedProjectEnvVars",
    "fileFiltering",
    "folderTrustFeature",
    "folderTrust",
    "hasSeenIdeIntegrationNudge",
    "hideWindowTitle",
    "showStatusInTitle",
    "showLineNumbers",
    "showCitations",
    "ideMode",
    "includeDirectories",
    "loadMemoryFromIncludeDirectories",
    "maxSessionTurns",
    "mcpServerCommand",
    "memoryImportFormat",
    "preferredEditor",
    "sandbox",
    "selectedAuthType",
    "shouldUseNodePtyShell",
    "shellPager",
    "shellShowColor",
    "skipNextSpeakerCheck",
    "toolDiscoveryCommand",
    "toolCallCommand",
    "usageStatisticsEnabled",
    "useExternalAuth",
    "useRipgrep",
    "enableWelcomeBack",
    "approvalMode",
    "sessionTokenLimit",
    "contentGenerator",
    "skipLoopDetection",
    "skipStartupContext",
    "enableOpenAILogging",
    "tavilyApiKey",
    "disableAutoUpdate",
    "disableUpdateNag",
    "disableLoadingPhrases",
    "disableFuzzySearch",
    "disableCacheControl",
];

const V2_CONTAINERS: &[&str] = &[
    "ui",
    "tools",
    "mcp",
    "advanced",
    "model",
    "general",
    "context",
    "security",
    "ide",
    "privacy",
    "telemetry",
    "extensions",
];

/// Returns whether an object-valued top-level key is already a V2 container.
/// Used by settings diagnostics to avoid treating modern nested settings as
/// ignored V1 aliases.
pub fn is_v2_container_key(key: &str) -> bool {
    V2_CONTAINERS.contains(&key)
}

/// Exposes the canonical V1 key remapping table to warning/reporting code.
pub fn v1_to_v2_migration_map() -> &'static [(&'static str, &'static str)] {
    V1_TO_V2
}

/// Pairs are kept in source object order because nested-write collisions use
/// last-writer behavior.
const V1_TO_V2: &[(&str, &str)] = &[
    ("accessibility", "ui.accessibility"),
    ("allowedTools", "tools.allowed"),
    ("allowMCPServers", "mcp.allowed"),
    ("autoAccept", "tools.autoAccept"),
    (
        "autoConfigureMaxOldSpaceSize",
        "advanced.autoConfigureMemory",
    ),
    ("bugCommand", "advanced.bugCommand"),
    ("chatCompression", "model.chatCompression"),
    ("checkpointing", "general.checkpointing"),
    ("coreTools", "tools.core"),
    ("contextFileName", "context.fileName"),
    ("customThemes", "ui.customThemes"),
    ("customWittyPhrases", "ui.customWittyPhrases"),
    ("debugKeystrokeLogging", "general.debugKeystrokeLogging"),
    ("dnsResolutionOrder", "advanced.dnsResolutionOrder"),
    ("enforcedAuthType", "security.auth.enforcedType"),
    ("excludeTools", "tools.exclude"),
    ("excludeMCPServers", "mcp.excluded"),
    ("excludedProjectEnvVars", "advanced.excludedEnvVars"),
    ("extensions", "extensions"),
    ("fileFiltering", "context.fileFiltering"),
    ("folderTrustFeature", "security.folderTrust.featureEnabled"),
    ("folderTrust", "security.folderTrust.enabled"),
    ("hasSeenIdeIntegrationNudge", "ide.hasSeenNudge"),
    ("hideWindowTitle", "ui.hideWindowTitle"),
    ("showStatusInTitle", "ui.showStatusInTitle"),
    ("hideTips", "ui.hideTips"),
    ("showLineNumbers", "ui.showLineNumbers"),
    ("showCitations", "ui.showCitations"),
    ("ideMode", "ide.enabled"),
    ("includeDirectories", "context.includeDirectories"),
    (
        "loadMemoryFromIncludeDirectories",
        "context.loadFromIncludeDirectories",
    ),
    ("maxSessionTurns", "model.maxSessionTurns"),
    ("mcpServers", "mcpServers"),
    ("mcpServerCommand", "mcp.serverCommand"),
    ("memoryImportFormat", "context.importFormat"),
    ("model", "model.name"),
    ("preferredEditor", "general.preferredEditor"),
    ("sandbox", "tools.sandbox"),
    ("selectedAuthType", "security.auth.selectedType"),
    (
        "shouldUseNodePtyShell",
        "tools.shell.enableInteractiveShell",
    ),
    ("shellPager", "tools.shell.pager"),
    ("shellShowColor", "tools.shell.showColor"),
    ("skipNextSpeakerCheck", "model.skipNextSpeakerCheck"),
    ("telemetry", "telemetry"),
    ("theme", "ui.theme"),
    ("toolDiscoveryCommand", "tools.discoveryCommand"),
    ("toolCallCommand", "tools.callCommand"),
    ("usageStatisticsEnabled", "privacy.usageStatisticsEnabled"),
    ("useExternalAuth", "security.auth.useExternal"),
    ("useRipgrep", "tools.useRipgrep"),
    ("vimMode", "general.vimMode"),
    ("enableWelcomeBack", "ui.enableWelcomeBack"),
    ("approvalMode", "tools.approvalMode"),
    ("sessionTokenLimit", "model.sessionTokenLimit"),
    ("contentGenerator", "model.generationConfig"),
    ("skipLoopDetection", "model.skipLoopDetection"),
    ("skipStartupContext", "model.skipStartupContext"),
    ("enableOpenAILogging", "model.enableOpenAILogging"),
    ("tavilyApiKey", "advanced.tavilyApiKey"),
];

const V1_DISABLE_MAP: &[(&str, &str)] = &[
    ("disableAutoUpdate", "general.disableAutoUpdate"),
    ("disableUpdateNag", "general.disableUpdateNag"),
    (
        "disableLoadingPhrases",
        "ui.accessibility.disableLoadingPhrases",
    ),
    (
        "disableFuzzySearch",
        "context.fileFiltering.disableFuzzySearch",
    ),
    (
        "disableCacheControl",
        "model.generationConfig.disableCacheControl",
    ),
];

fn v1_to_v2_applies(settings: &Value) -> bool {
    let Some(object) = settings.as_object() else {
        return false;
    };
    if get_version(settings).is_some_and(|version| version >= 2.0) {
        return false;
    }
    V1_INDICATORS.iter().any(|key| {
        object
            .get(*key)
            .is_some_and(|value| !is_plain_object(value))
    })
}

fn migrate_v1_to_v2(settings: &Value) -> Value {
    let source = settings
        .as_object()
        .expect("migration applicability checked");
    let mut result = Map::<String, Value>::new();
    let mut processed = Vec::<String>::new();

    for (old_key, path) in V1_TO_V2 {
        let Some(value) = source.get(*old_key) else {
            continue;
        };
        if V2_CONTAINERS.contains(old_key) && value.is_object() {
            result.insert((*old_key).to_owned(), value.clone());
            processed.push((*old_key).to_owned());
            continue;
        }
        if let Some(value_object) = value
            .as_object()
            .filter(|_| path.starts_with(&format!("{old_key}.")))
        {
            for (nested_key, nested_value) in value_object {
                set_nested_safe(
                    &mut result,
                    &format!("{path}.{nested_key}"),
                    nested_value.clone(),
                );
            }
        } else {
            set_nested_safe(&mut result, path, value.clone());
        }
        processed.push((*old_key).to_owned());
    }

    for (old_key, path) in V1_DISABLE_MAP {
        let Some(value) = source.get(*old_key) else {
            continue;
        };
        if matches!(*old_key, "disableAutoUpdate" | "disableUpdateNag") {
            set_nested_safe(&mut result, path, Value::Bool(value == &Value::Bool(true)));
        } else if value.is_boolean() {
            set_nested_safe(&mut result, path, value.clone());
        }
        processed.push((*old_key).to_owned());
    }

    if let Some(value) = source.get("mcpServers") {
        result.insert("mcpServers".to_owned(), value.clone());
        processed.push("mcpServers".to_owned());
    }

    for (key, value) in source {
        if processed.iter().any(|processed_key| processed_key == key) {
            continue;
        }
        let is_parent_of_migrated_path = processed.iter().any(|processed_key| {
            let mapped_path = mapped_path_for_legacy_key(processed_key);
            mapped_path.is_some_and(|path| path.starts_with(&format!("{key}.")))
        });
        if is_parent_of_migrated_path {
            if let Some(object) = value.as_object() {
                for (nested_key, nested_value) in object {
                    let full_path = format!("{key}.{nested_key}");
                    let already_processed = processed
                        .iter()
                        .filter_map(|processed_key| mapped_path_for_legacy_key(processed_key))
                        .any(|path| path == full_path);
                    if !already_processed {
                        set_nested_safe(&mut result, &full_path, nested_value.clone());
                    }
                }
            } else {
                result.insert(key.clone(), value.clone());
            }
        } else {
            result.insert(key.clone(), value.clone());
        }
    }

    result.insert("$version".to_owned(), Value::Number(Number::from(2)));
    Value::Object(result)
}

fn mapped_path_for_legacy_key(key: &str) -> Option<&'static str> {
    V1_TO_V2
        .iter()
        .find_map(|(old_key, path)| (*old_key == key).then_some(*path))
        .or_else(|| {
            V1_DISABLE_MAP
                .iter()
                .find_map(|(old_key, path)| (*old_key == key).then_some(*path))
        })
}

const V2_TO_V3_BOOLEAN_MAP: &[(&str, &str)] = &[
    ("general.disableAutoUpdate", "general.enableAutoUpdate"),
    ("general.disableUpdateNag", "general.enableAutoUpdate"),
    (
        "ui.accessibility.disableLoadingPhrases",
        "ui.accessibility.enableLoadingPhrases",
    ),
    (
        "context.fileFiltering.disableFuzzySearch",
        "context.fileFiltering.enableFuzzySearch",
    ),
    (
        "model.generationConfig.disableCacheControl",
        "model.generationConfig.enableCacheControl",
    ),
];

fn normalize_disable_value(value: Option<&Value>) -> (bool, Option<bool>) {
    match value {
        None => (false, None),
        Some(Value::Bool(value)) => (true, Some(*value)),
        Some(Value::String(value)) => match value.trim().to_lowercase().as_str() {
            "true" => (true, Some(true)),
            "false" => (true, Some(false)),
            _ => (true, None),
        },
        Some(_) => (true, None),
    }
}

fn migrate_v2_to_v3(settings: &Value, scope: &str) -> (Value, Vec<String>) {
    let mut result = settings.clone();
    let mut warnings = Vec::new();
    let mut processed = Vec::<&str>::new();
    let mut has_any_disable = false;
    let mut has_any_valid_value = false;

    for (old_path, new_path) in &V2_TO_V3_BOOLEAN_MAP[..2] {
        let value = get_nested(&result, old_path);
        let (is_present, normalized) = normalize_disable_value(value);
        if !is_present {
            continue;
        }
        delete_nested_safe(&mut result, old_path);
        processed.push(old_path);
        if let Some(value) = normalized {
            has_any_valid_value = true;
            has_any_disable |= value;
        } else {
            warnings.push(format!(
                "Removed deprecated setting '{old_path}' from {scope} settings because the value is invalid. Expected boolean."
            ));
        }
        let _ = new_path;
    }
    if has_any_valid_value {
        set_nested_safe(
            result.as_object_mut().expect("settings root object"),
            "general.enableAutoUpdate",
            Value::Bool(!has_any_disable),
        );
    }

    for (old_path, new_path) in &V2_TO_V3_BOOLEAN_MAP[2..] {
        let value = get_nested(&result, old_path);
        let (is_present, normalized) = normalize_disable_value(value);
        if !is_present {
            continue;
        }
        if !processed.contains(old_path) {
            delete_nested_safe(&mut result, old_path);
        }
        if let Some(value) = normalized {
            if let Some(object) = result.as_object_mut() {
                set_nested_safe(object, new_path, Value::Bool(!value));
            }
        } else {
            warnings.push(format!(
                "Removed deprecated setting '{old_path}' from {scope} settings because the value is invalid. Expected boolean or string \"true\"/\"false\"."
            ));
        }
    }

    result
        .as_object_mut()
        .expect("settings root object")
        .insert("$version".to_owned(), Value::Number(Number::from(3)));
    (result, warnings)
}

fn v3_to_v4_applies(settings: &Value) -> bool {
    if get_version(settings) == Some(3.0) {
        return true;
    }
    if settings
        .as_object()
        .is_none_or(|object| object.contains_key("$version"))
    {
        return false;
    }
    let value = get_nested(settings, "general.gitCoAuthor");
    value.is_some_and(|value| !value.is_object())
}

fn migrate_v3_to_v4(settings: &Value, scope: &str) -> (Value, Vec<String>) {
    let mut result = settings.clone();
    let mut warnings = Vec::new();
    if let Some(value) = get_nested(settings, "general.gitCoAuthor").cloned() {
        let converted = match value {
            Value::Bool(value) => Some(serde_json::json!({"commit": value, "pr": value})),
            Value::String(value) => {
                let normalized = value.trim().to_lowercase();
                let enabled = match normalized.as_str() {
                    "true" | "yes" | "on" | "1" | "enabled" => Some(true),
                    "false" | "no" | "off" | "0" | "disabled" | "" => Some(false),
                    _ => None,
                };
                if enabled.is_none() {
                    warnings.push(format!(
                        "Reset 'general.gitCoAuthor' in {scope} settings to {{commit: false, pr: false}} because the stored string '{value}' was not a recognized boolean form."
                    ));
                }
                let enabled = enabled.unwrap_or(false);
                Some(serde_json::json!({"commit": enabled, "pr": enabled}))
            }
            Value::Object(_) => None,
            _ => {
                warnings.push(format!(
                    "Reset 'general.gitCoAuthor' in {scope} settings to {{commit: false, pr: false}} because the stored value was not a boolean or object."
                ));
                Some(serde_json::json!({"commit": false, "pr": false}))
            }
        };
        if let Some(converted) = converted {
            set_nested_safe(
                result.as_object_mut().expect("settings root object"),
                "general.gitCoAuthor",
                converted,
            );
        }
    }
    result
        .as_object_mut()
        .expect("settings root object")
        .insert("$version".to_owned(), Value::Number(Number::from(4)));
    (result, warnings)
}

fn v5_to_v4_applies(settings: &Value) -> bool {
    let Some(object) = settings.as_object() else {
        return false;
    };
    if object.get("$version").and_then(Value::as_f64) == Some(5.0) {
        return true;
    }
    if object.contains_key("$version") {
        return false;
    }
    object
        .get("modelProviders")
        .is_some_and(|providers| match providers {
            Value::Object(providers) => providers.values().any(Value::is_object),
            Value::Array(providers) => providers.iter().any(Value::is_object),
            _ => false,
        })
}

fn migrate_v5_to_v4(settings: &Value) -> (Value, Vec<String>) {
    let mut result = settings.clone();
    let mut warnings = Vec::new();
    let protocols = [
        ("openai", "openai"),
        ("canopy-oauth", "canopy-oauth"),
        ("gemini", "gemini"),
        ("vertex-ai", "gemini"),
        ("anthropic", "anthropic"),
    ];
    if let Some(providers) = result
        .as_object_mut()
        .and_then(|object| object.get_mut("modelProviders"))
    {
        match providers {
            Value::Object(providers) => {
                for (key, value) in providers.iter_mut() {
                    downgrade_provider(key, value, &protocols, &mut warnings);
                }
            }
            Value::Array(providers) => {
                for (index, value) in providers.iter_mut().enumerate() {
                    downgrade_provider(&index.to_string(), value, &protocols, &mut warnings);
                }
            }
            _ => {}
        }
    }
    result
        .as_object_mut()
        .expect("settings root object")
        .insert("$version".to_owned(), Value::Number(Number::from(4)));
    (result, warnings)
}

fn downgrade_provider(
    key: &str,
    value: &mut Value,
    protocols: &[(&str, &str)],
    warnings: &mut Vec<String>,
) {
    let Some(provider) = value.as_object() else {
        return;
    };
    let derived = protocols
        .iter()
        .find_map(|(provider_key, protocol)| (*provider_key == key).then_some(*protocol));
    if let Some(derived) = derived {
        if let Some(explicit) = provider.get("protocol").and_then(Value::as_str) {
            if explicit != derived {
                warnings.push(format!(
                    "Provider \"{key}\" declared protocol \"{explicit}\", but V4 derives protocol \"{derived}\" from the provider key. The explicit protocol has been dropped."
                ));
            }
        }
    }
    let models = provider
        .get("models")
        .filter(|models| models.is_array())
        .cloned()
        .unwrap_or_else(|| Value::Array(Vec::new()));
    *value = models;
}

fn get_nested<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = value;
    for segment in path.split('.') {
        current = current.as_object()?.get(segment)?;
    }
    Some(current)
}

fn set_nested_safe(object: &mut Map<String, Value>, path: &str, value: Value) {
    let segments = path.split('.').collect::<Vec<_>>();
    if segments.is_empty()
        || segments
            .iter()
            .any(|segment| matches!(*segment, "__proto__" | "constructor" | "prototype"))
    {
        return;
    }
    let mut current = object;
    for segment in &segments[..segments.len() - 1] {
        if !current.contains_key(*segment) {
            current.insert((*segment).to_owned(), Value::Object(Map::new()));
        }
        let Some(Value::Object(next)) = current.get_mut(*segment) else {
            return;
        };
        current = next;
    }
    current.insert(segments[segments.len() - 1].to_owned(), value);
}

fn delete_nested_safe(value: &mut Value, path: &str) {
    let segments = path.split('.').collect::<Vec<_>>();
    if segments.is_empty() {
        return;
    }
    let Some(mut current) = value.as_object_mut() else {
        return;
    };
    for segment in &segments[..segments.len() - 1] {
        let Some(next) = current.get_mut(*segment).and_then(Value::as_object_mut) else {
            return;
        };
        current = next;
    }
    current.remove(segments[segments.len() - 1]);
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{run_migrations, settings_need_migration};

    #[test]
    fn v1_migration_moves_legacy_fields_merges_partial_containers_and_stamps_version() {
        let input = json!({
            "theme": "dark",
            "model": "model-x",
            "ui": {"hideTips": true, "theme": "old"},
            "accessibility": {"screenReader": true},
            "disableAutoUpdate": "false",
            "mcpServers": {"local": {"command": "run"}},
            "extensionData": {"opaque": true}
        });
        let result = run_migrations(&input, "Workspace");
        assert_eq!(result.settings["$version"], 4);
        assert_eq!(result.settings["model"]["name"], "model-x");
        assert_eq!(result.settings["ui"]["theme"], "dark");
        assert_eq!(result.settings["ui"]["hideTips"], true);
        assert_eq!(result.settings["ui"]["accessibility"]["screenReader"], true);
        assert_eq!(result.settings["general"]["enableAutoUpdate"], true);
        assert_eq!(result.settings["mcpServers"]["local"]["command"], "run");
        assert_eq!(result.settings["extensionData"]["opaque"], true);
        assert_eq!(input["theme"], "dark");
        assert_eq!(result.executed_migrations.len(), 3);
    }

    #[test]
    fn v1_container_objects_are_not_double_nested_and_parent_scalars_are_preserved() {
        let input = json!({"$version": 1, "theme": "dark", "model": {"name": "new"}, "ui": false});
        let result = run_migrations(&input, "User");
        assert_eq!(result.settings["model"]["name"], "new");
        assert_eq!(result.settings["ui"], false);
        assert_eq!(result.executed_migrations[0].to_version, 2);
    }

    #[test]
    fn v2_disable_migration_coerces_strings_drops_invalids_and_consolidates_updates() {
        let input = json!({
            "$version": 2,
            "general": {"disableAutoUpdate": "true", "disableUpdateNag": false},
            "ui": {"accessibility": {"disableLoadingPhrases": " FALSE "}},
            "context": {"fileFiltering": {"disableFuzzySearch": 1}},
            "model": {"generationConfig": {"disableCacheControl": null}}
        });
        let result = run_migrations(&input, "SystemDefaults");
        assert_eq!(result.settings["general"]["enableAutoUpdate"], false);
        assert_eq!(
            result.settings["ui"]["accessibility"]["enableLoadingPhrases"],
            true
        );
        assert!(result.settings["context"]["fileFiltering"]["disableFuzzySearch"].is_null());
        assert!(result.settings["model"]["generationConfig"]["disableCacheControl"].is_null());
        assert_eq!(result.warnings.len(), 2);
        assert!(
            result
                .warnings
                .iter()
                .all(|warning| warning.contains("system default"))
        );
    }

    #[test]
    fn versionless_git_coauthor_is_migrated_but_current_object_shape_is_preserved() {
        let legacy = json!({"general": {"gitCoAuthor": "off"}});
        let result = run_migrations(&legacy, "User");
        assert_eq!(
            result.settings["general"]["gitCoAuthor"],
            json!({"commit": false, "pr": false})
        );
        assert_eq!(result.settings["$version"], 4);

        let current = json!({"$version": 4, "general": {"gitCoAuthor": {"commit": true}}});
        let result = run_migrations(&current, "User");
        assert_eq!(result.settings["general"]["gitCoAuthor"]["commit"], true);
        assert!(result.executed_migrations.is_empty());
    }

    #[test]
    fn v5_provider_wrappers_downgrade_and_warn_when_protocol_cannot_be_represented() {
        let input = json!({
            "$version": 5,
            "modelProviders": {
                "openai": {"protocol": "gemini", "models": [{"id": "m"}]},
                "custom": {"protocol": "anthropic", "models": "invalid"},
                "primitive": "untouched"
            }
        });
        let result = run_migrations(&input, "User");
        assert_eq!(result.settings["$version"], 4);
        assert_eq!(
            result.settings["modelProviders"]["openai"],
            json!([{"id": "m"}])
        );
        assert_eq!(result.settings["modelProviders"]["custom"], json!([]));
        assert_eq!(result.settings["modelProviders"]["primitive"], "untouched");
        assert_eq!(result.warnings.len(), 1);
    }

    #[test]
    fn versionless_array_model_providers_follow_javascript_object_values() {
        let input = json!({
            "modelProviders": [
                {"protocol": "wrong", "models": [{"name": "kept"}]}
            ]
        });
        let result = run_migrations(&input, "User");
        assert_eq!(
            result.settings["modelProviders"][0],
            json!([{"name":"kept"}])
        );
        assert!(result.warnings.is_empty());
    }

    #[test]
    fn migration_detection_matches_scheduler_guardrails() {
        assert!(!settings_need_migration(
            &json!({"$version": 4, "theme": "dark"})
        ));
        assert!(!settings_need_migration(
            &json!({"$version": 9, "theme": "dark"})
        ));
        assert!(settings_need_migration(&json!({"$version": 2})));
        assert!(settings_need_migration(&json!({"model": "legacy"})));
        assert!(!settings_need_migration(
            &json!({"$version": "2", "model": {"name": "x"}})
        ));
    }

    #[test]
    fn migrations_then_legacy_permissions_migration_can_be_called_without_mutating_input() {
        let input = json!({"tools": {"allowed": ["read"]}});
        let migrated = run_migrations(&input, "User").settings;
        let permissions = crate::settings_migration::migrate_legacy_permissions(&migrated)
            .expect("legacy permissions convert after v1 key move");
        assert_eq!(permissions["permissions"]["allow"], json!(["read"]));
        assert_eq!(input["tools"]["allowed"], json!(["read"]));
    }
}
