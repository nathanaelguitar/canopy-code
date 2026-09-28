//! Host-side ingestion for configured hook definitions.
//!
//! Port of the configuration-processing path in
//! `packages/core/src/hooks/hookRegistry.ts`. The host supplies already-read
//! user/project/extension JSON and decides whether project hooks are trusted.
//! This module validates each definition, resolves runtime-only function
//! callbacks through an injected adapter, and returns registry entries in
//! source order.

use serde_json::Value;

use super::function_runner::FunctionHookConfig;
use super::native_dispatch_executor::JsonFunctionHookResolver;
use super::planner::HookEventName;
use super::registry::{HookRegistry, HookRegistryEntry, HooksConfigSource};

/// Top-level hook JSON from the host's configuration sources.
///
/// Pass `project_hooks: None` when project hooks are unavailable or the folder
/// has not passed the host's trust policy. User hooks are processed before
/// project hooks, followed by active extensions in vector order.
#[derive(Clone, Debug, Default)]
pub struct HookConfigSources {
    pub user_hooks: Option<Value>,
    pub project_hooks: Option<Value>,
    pub extensions: Vec<ExtensionHookSource>,
}

/// Hook configuration exposed by one extension.
#[derive(Clone, Debug, Default)]
pub struct ExtensionHookSource {
    pub is_active: bool,
    pub hooks: Option<Value>,
}

/// Kind of issue found while reading loosely typed JSON.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HookConfigIssueKind {
    RootNotObject,
    InvalidEventName,
    EventDefinitionsNotArray,
    InvalidDefinition,
    InvalidHookConfiguration,
    UnresolvedFunctionCallback,
}

/// A recoverable warning for the host to report through its logger or UI.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HookConfigIssue {
    pub kind: HookConfigIssueKind,
    pub source: HooksConfigSource,
    pub event_name: Option<String>,
    pub message: String,
}

/// Entries accepted by the registry plus warnings the host may surface.
#[derive(Clone, Debug, Default)]
pub struct HookConfigLoadResult {
    pub entries: Vec<HookRegistryEntry>,
    pub issues: Vec<HookConfigIssue>,
}

/// Read user, project, and active extension hook JSON.
///
/// `function_resolver` is required to accept JSON `type: "function"` hooks.
/// JSON cannot carry a Rust callback, so unresolved or callback-less function
/// entries are dropped like the TypeScript registry drops configs without a
/// callable `callback`. The host should provide the same resolver to the
/// native dispatch adapter when constructing it.
pub fn load_hook_entries(
    sources: &HookConfigSources,
    function_resolver: Option<&dyn JsonFunctionHookResolver>,
) -> HookConfigLoadResult {
    let mut entries = Vec::new();
    let mut issues = Vec::new();

    if let Some(hooks) = &sources.user_hooks {
        process_source(
            hooks,
            HooksConfigSource::User,
            function_resolver,
            &mut entries,
            &mut issues,
        );
    }
    if let Some(hooks) = &sources.project_hooks {
        process_source(
            hooks,
            HooksConfigSource::Project,
            function_resolver,
            &mut entries,
            &mut issues,
        );
    }
    for extension in &sources.extensions {
        if extension.is_active {
            if let Some(hooks) = &extension.hooks {
                process_source(
                    hooks,
                    HooksConfigSource::Extensions,
                    function_resolver,
                    &mut entries,
                    &mut issues,
                );
            }
        }
    }

    // Reuse the registry's identity rule so first-seen duplicates are kept
    // and non-function configs receive their source field in the same place
    // and order as normal registry initialization.
    let mut registry = HookRegistry::new();
    registry.initialize(entries);

    HookConfigLoadResult {
        entries: registry.get_all_hooks(),
        issues,
    }
}

fn process_source(
    hooks: &Value,
    source: HooksConfigSource,
    function_resolver: Option<&dyn JsonFunctionHookResolver>,
    entries: &mut Vec<HookRegistryEntry>,
    issues: &mut Vec<HookConfigIssue>,
) {
    let Some(events) = hooks.as_object() else {
        issues.push(issue(
            HookConfigIssueKind::RootNotObject,
            source,
            None,
            "Hook configuration must be a JSON object; skipping this source.",
        ));
        return;
    };

    for (event_name, definitions) in events {
        // These are config-level controls, never event names.
        if matches!(
            event_name.as_str(),
            "enabled" | "disabled" | "notifications"
        ) {
            continue;
        }

        let Ok(event) = serde_json::from_value::<HookEventName>(Value::String(event_name.clone()))
        else {
            issues.push(issue(
                HookConfigIssueKind::InvalidEventName,
                source,
                Some(event_name.clone()),
                format!("Invalid hook event name {event_name:?}; skipping."),
            ));
            continue;
        };

        let Some(definitions) = definitions.as_array() else {
            issues.push(issue(
                HookConfigIssueKind::EventDefinitionsNotArray,
                source,
                Some(event_name.clone()),
                format!("Hook definitions for {event_name:?} must be an array; skipping."),
            ));
            continue;
        };

        for definition in definitions {
            let Some(definition) = definition.as_object() else {
                issues.push(issue(
                    HookConfigIssueKind::InvalidDefinition,
                    source,
                    Some(event_name.clone()),
                    format!("Discarding invalid hook definition for {event_name:?}."),
                ));
                continue;
            };
            let Some(hook_configs) = definition.get("hooks").and_then(Value::as_array) else {
                issues.push(issue(
                    HookConfigIssueKind::InvalidDefinition,
                    source,
                    Some(event_name.clone()),
                    format!("Hook definition for {event_name:?} has no hooks array; skipping."),
                ));
                continue;
            };

            // TS types constrain these fields; malformed JSON values cannot
            // be represented in HookRegistryEntry and are treated as absent.
            let matcher = definition
                .get("matcher")
                .and_then(Value::as_str)
                .map(str::to_owned);
            let sequential = definition.get("sequential").and_then(Value::as_bool);

            for hook_config in hook_configs {
                if !validate_hook_config(hook_config, source, function_resolver, issues, event_name)
                {
                    continue;
                }
                entries.push(HookRegistryEntry {
                    config: hook_config.clone(),
                    source,
                    event_name: event,
                    matcher: matcher.clone(),
                    sequential,
                    enabled: true,
                    agent_scope: None,
                });
            }
        }
    }
}

fn validate_hook_config(
    hook_config: &Value,
    source: HooksConfigSource,
    function_resolver: Option<&dyn JsonFunctionHookResolver>,
    issues: &mut Vec<HookConfigIssue>,
    event_name: &str,
) -> bool {
    let Some(config) = hook_config.as_object() else {
        issues.push(issue(
            HookConfigIssueKind::InvalidHookConfiguration,
            source,
            Some(event_name.to_owned()),
            format!("Discarding non-object hook config for {event_name:?} from {source:?}."),
        ));
        return false;
    };

    let hook_type = config.get("type").and_then(Value::as_str);
    let required_field = match hook_type {
        Some("command") => Some("command"),
        Some("http") => Some("url"),
        Some("function") => None,
        Some("prompt") => Some("prompt"),
        _ => {
            issues.push(issue(
                HookConfigIssueKind::InvalidHookConfiguration,
                source,
                Some(event_name.to_owned()),
                format!("Invalid hook type {hook_type:?} for {event_name:?} from {source:?}."),
            ));
            return false;
        }
    };

    if let Some(field) = required_field {
        let valid = config
            .get(field)
            .and_then(Value::as_str)
            .is_some_and(|value| !value.is_empty());
        if !valid {
            issues.push(issue(
                HookConfigIssueKind::InvalidHookConfiguration,
                source,
                Some(event_name.to_owned()),
                format!(
                    "{} hook for {event_name:?} from {source:?} is missing a non-empty {field} string.",
                    hook_type.unwrap_or_default()
                ),
            ));
            return false;
        }
    }

    if hook_type == Some("function") {
        let resolved = function_resolver
            .and_then(|resolver| resolver.resolve(hook_config))
            .is_some_and(|FunctionHookConfig { callback, .. }| callback.is_some());
        if !resolved {
            issues.push(issue(
                HookConfigIssueKind::UnresolvedFunctionCallback,
                source,
                Some(event_name.to_owned()),
                format!(
                    "Function hook for {event_name:?} from {source:?} has no host-resolved callback; skipping."
                ),
            ));
            return false;
        }
    }

    true
}

fn issue(
    kind: HookConfigIssueKind,
    source: HooksConfigSource,
    event_name: Option<String>,
    message: impl Into<String>,
) -> HookConfigIssue {
    HookConfigIssue {
        kind,
        source,
        event_name,
        message: message.into(),
    }
}
