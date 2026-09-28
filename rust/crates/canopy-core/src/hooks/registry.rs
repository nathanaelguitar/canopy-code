//! In-memory registry state for configured and ephemeral hooks.
//!
//! Source: `packages/core/src/hooks/hookRegistry.ts`. This module preserves
//! entry ordering, source-priority listing, enabled state, duplicate identity,
//! agent-scoped insertion/removal, and transactional configured reloads.
//! Callers supply entries after loading and validating configuration. It does
//! not read settings, enforce trusted-folder policy, activate extensions,
//! validate hook schemas/callbacks, or emit feedback and debug logs. The
//! reduced `HookConfig` in `trusted_hooks.rs` contains trust-key fields only,
//! so full execution configs are retained here as JSON values.
//! Unnamed prompt display names are shortened to 30 Unicode scalar values;
//! JavaScript slices UTF-16 code units, so non-BMP identities can differ.
//!
//! Agent scope is recorded and used for duplicate detection and removal, as in
//! the source. It is not a runtime filter: scoped entries are returned for
//! every event while registered.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::planner::HookEventName;

/// Source of a configured hook entry.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum HooksConfigSource {
    Project,
    User,
    System,
    Extensions,
    Session,
}

impl HooksConfigSource {
    fn priority(self) -> u16 {
        match self {
            Self::Project => 1,
            Self::User => 2,
            Self::System => 3,
            Self::Extensions => 4,
            Self::Session => 999,
        }
    }
}

/// A single hook configuration registered for one event.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HookRegistryEntry {
    /// Complete execution config. The host validates this before registration.
    pub config: Value,
    pub source: HooksConfigSource,
    pub event_name: HookEventName,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matcher: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sequential: Option<bool>,
    pub enabled: bool,
    /// Runtime-only scope for ephemeral subagent hooks.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_scope: Option<String>,
}

impl HookRegistryEntry {
    /// Stable duplicate and reload identity for this entry.
    pub fn state_key(&self) -> HookRegistryStateKey {
        HookRegistryStateKey {
            event_name: self.event_name,
            source: self.source,
            agent_scope: self.agent_scope.clone(),
            hook_identity: get_hook_identity(&self.config),
            matcher: self.matcher.clone(),
            sequential: self.sequential,
        }
    }

    /// Display name used by enabled-state toggles.
    pub fn hook_name(&self) -> String {
        get_hook_name(&self.config)
    }
}

/// Fields that define a hook's stable registry state across reloads.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct HookRegistryStateKey {
    pub event_name: HookEventName,
    pub source: HooksConfigSource,
    pub agent_scope: Option<String>,
    pub hook_identity: String,
    pub matcher: Option<String>,
    pub sequential: Option<bool>,
}

/// Return the stable identity used for duplicate detection.
///
/// This follows the source's `name || kind-specific identity` rule and keeps
/// the complete identity string; it is not truncated for deduplication.
pub fn get_hook_identity(config: &Value) -> String {
    if let Some(name) = config
        .get("name")
        .and_then(Value::as_str)
        .filter(|name| !name.is_empty())
    {
        return name.to_owned();
    }

    match config.get("type").and_then(Value::as_str) {
        Some("command") => nonempty_string(config.get("command"))
            .unwrap_or("unknown-command")
            .to_owned(),
        Some("http") => nonempty_string(config.get("url"))
            .unwrap_or("unknown-url")
            .to_owned(),
        Some("function") => nonempty_string(config.get("id"))
            .unwrap_or("unknown-function")
            .to_owned(),
        Some("prompt") => nonempty_string(config.get("prompt"))
            .unwrap_or("prompt-hook")
            .to_owned(),
        _ => "unknown-hook".to_owned(),
    }
}

/// Return the user-facing hook name, truncating unnamed prompt identities.
pub fn get_hook_name(config: &Value) -> String {
    let identity = get_hook_identity(config);
    let has_name = config
        .get("name")
        .and_then(Value::as_str)
        .is_some_and(|name| !name.is_empty());
    if !has_name && config.get("type").and_then(Value::as_str) == Some("prompt") {
        let mut chars = identity.chars();
        let shortened = chars.by_ref().take(30).collect::<String>();
        if chars.next().is_some() {
            return format!("{shortened}...");
        }
    }
    identity
}

/// In-memory registry of hook entries.
#[derive(Clone, Debug, Default)]
pub struct HookRegistry {
    entries: Vec<HookRegistryEntry>,
}

impl HookRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Replace all entries with already-loaded configured entries.
    ///
    /// Duplicate entries are skipped in first-seen order, matching the source
    /// configuration processor. Initialization clears ephemeral agent entries
    /// and enabled overrides just as the TypeScript initializer does.
    pub fn initialize(&mut self, configured_entries: impl IntoIterator<Item = HookRegistryEntry>) {
        self.entries.clear();
        for mut entry in configured_entries {
            entry.agent_scope = None;
            entry.enabled = true;
            self.append_unique(entry);
        }
    }

    /// Reload configured entries transactionally while retaining agent hooks
    /// and matching enabled overrides by stable state key.
    ///
    /// `load_configured` runs in the caller's configuration layer. If it
    /// returns an error, this registry remains untouched. The generated
    /// entries should already reflect trust policy, extension activation,
    /// validation, and source selection.
    pub fn reload_configured_hooks<E>(
        &mut self,
        load_configured: impl FnOnce() -> Result<Vec<HookRegistryEntry>, E>,
    ) -> Result<(), E> {
        let configured_entries = load_configured()?;
        let enabled_snapshot = self
            .entries
            .iter()
            .map(|entry| (entry.state_key(), entry.enabled))
            .collect::<HashMap<_, _>>();
        let mut replacement = self
            .entries
            .iter()
            .filter(|entry| entry.agent_scope.is_some())
            .cloned()
            .collect::<Vec<_>>();

        for mut entry in configured_entries {
            entry.agent_scope = None;
            entry.enabled = true;
            append_unique_to(&mut replacement, entry);
        }
        for entry in &mut replacement {
            if let Some(enabled) = enabled_snapshot.get(&entry.state_key()) {
                entry.enabled = *enabled;
            }
        }

        self.entries = replacement;
        Ok(())
    }

    /// Return enabled entries for an event in source-priority order.
    ///
    /// Entries with equal priority retain their insertion order, matching the
    /// stable `Array.sort` used by the TypeScript implementation.
    pub fn get_hooks_for_event(&self, event_name: HookEventName) -> Vec<HookRegistryEntry> {
        let mut entries = self
            .entries
            .iter()
            .filter(|entry| entry.event_name == event_name && entry.enabled)
            .cloned()
            .collect::<Vec<_>>();
        entries.sort_by_key(|entry| entry.source.priority());
        entries
    }

    /// Return a snapshot of all registered entries in insertion order.
    pub fn get_all_hooks(&self) -> Vec<HookRegistryEntry> {
        self.entries.clone()
    }

    /// Append ephemeral hooks for an agent scope and return the number added.
    ///
    /// The source registers agent hooks as `Session` entries and scopes their
    /// duplicate identity. This method assigns both fields and starts them
    /// enabled. It does not validate the hook config.
    pub fn add_agent_hooks(
        &mut self,
        agent_scope: impl Into<String>,
        entries: impl IntoIterator<Item = HookRegistryEntry>,
    ) -> usize {
        let agent_scope = agent_scope.into();
        let mut added = 0;
        for mut entry in entries {
            entry.source = HooksConfigSource::Session;
            entry.agent_scope = Some(agent_scope.clone());
            entry.enabled = true;
            if self.append_unique(entry) {
                added += 1;
            }
        }
        added
    }

    /// Remove all ephemeral entries belonging to one agent scope.
    pub fn remove_agent_hooks(&mut self, agent_scope: &str) -> usize {
        let old_len = self.entries.len();
        self.entries
            .retain(|entry| entry.agent_scope.as_deref() != Some(agent_scope));
        old_len - self.entries.len()
    }

    /// Enable or disable every entry matching the display hook name.
    ///
    /// Returns the number of changed entries. The TypeScript implementation
    /// emits an info or warning log instead of returning this count.
    pub fn set_hook_enabled(&mut self, hook_name: &str, enabled: bool) -> usize {
        let mut updated = 0;
        for entry in &mut self.entries {
            if entry.hook_name() == hook_name {
                entry.enabled = enabled;
                updated += 1;
            }
        }
        updated
    }

    /// Number of registered entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether there are no registered entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn append_unique(&mut self, entry: HookRegistryEntry) -> bool {
        append_unique_to(&mut self.entries, entry)
    }
}

fn append_unique_to(entries: &mut Vec<HookRegistryEntry>, mut entry: HookRegistryEntry) -> bool {
    let key = entry.state_key();
    if entries.iter().any(|existing| existing.state_key() == key) {
        return false;
    }

    // The source stamps the source into non-function configs after duplicate
    // detection. Function callbacks remain runtime-only and are not stamped.
    if entry.config.get("type").and_then(Value::as_str) != Some("function") {
        if let Some(config) = entry.config.as_object_mut() {
            config.insert("source".to_owned(), json!(entry.source));
        }
    }
    entries.push(entry);
    true
}

fn nonempty_string(value: Option<&Value>) -> Option<&str> {
    value
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
}
