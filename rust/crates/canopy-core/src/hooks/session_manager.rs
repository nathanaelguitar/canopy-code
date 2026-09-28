//! Per-session runtime hooks.
//!
//! Port of `packages/core/src/hooks/sessionHooksManager.ts`. Session and event
//! registries preserve insertion order, and the matcher path uses the shared
//! Rust tool-alias table plus anchored regular expressions.

use std::time::{SystemTime, UNIX_EPOCH};

use indexmap::IndexMap;
use regex::Regex;
use serde_json::Value;
use uuid::Uuid;

use super::function_runner::{
    FunctionHookCallback, FunctionHookConfig, FunctionHookSuccessCallback,
};
use super::planner::{
    HookEventName, HookMatcherTargetKind, get_hook_matcher_target, get_tool_matcher_targets,
};

/// Runtime config for a function callback or a source command/HTTP hook
/// payload. The latter stay as JSON because their execution config structs are
/// maintained by their respective runners.
#[derive(Clone, Debug)]
pub enum SessionHookConfig {
    Function(FunctionHookConfig),
    Json(Value),
}

/// One hook registered for a session and event.
#[derive(Clone, Debug)]
pub struct SessionHookEntry {
    pub hook_id: String,
    pub event_name: HookEventName,
    pub matcher: String,
    pub config: SessionHookConfig,
    pub sequential: Option<bool>,
    pub skill_root: Option<String>,
}

#[derive(Clone, Debug, Default)]
struct SessionHooksStorage {
    hooks: IndexMap<HookEventName, Vec<SessionHookEntry>>,
}

/// Options for registering a function hook.
#[derive(Clone, Default)]
pub struct FunctionHookOptions {
    pub timeout_ms: Option<f64>,
    pub id: Option<String>,
    pub name: Option<String>,
    pub description: Option<String>,
    pub status_message: Option<String>,
    pub on_hook_success: Option<FunctionHookSuccessCallback>,
    pub skill_root: Option<String>,
}

impl std::fmt::Debug for FunctionHookOptions {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FunctionHookOptions")
            .field("timeout_ms", &self.timeout_ms)
            .field("id", &self.id)
            .field("name", &self.name)
            .field("description", &self.description)
            .field("status_message", &self.status_message)
            .field("has_on_hook_success", &self.on_hook_success.is_some())
            .field("skill_root", &self.skill_root)
            .finish()
    }
}

/// Options for registering a command or HTTP hook payload.
#[derive(Clone, Debug, Default)]
pub struct SessionHookOptions {
    pub sequential: Option<bool>,
    pub skill_root: Option<String>,
}

/// In-memory session hook registry.
#[derive(Clone, Debug, Default)]
pub struct SessionHooksManager {
    sessions: IndexMap<String, SessionHooksStorage>,
}

impl SessionHooksManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a function hook. A non-empty caller-provided ID is retained and
    /// also copied into the function config; otherwise a new session-hook ID
    /// is generated.
    pub fn add_function_hook(
        &mut self,
        session_id: impl Into<String>,
        event: HookEventName,
        matcher: impl Into<String>,
        callback: FunctionHookCallback,
        error_message: impl Into<String>,
        options: Option<FunctionHookOptions>,
    ) -> String {
        let options = options.unwrap_or_default();
        let hook_id = options
            .id
            .as_deref()
            .filter(|id| !id.is_empty())
            .map(str::to_owned)
            .unwrap_or_else(generate_hook_id);

        let mut config = FunctionHookConfig::new(error_message, move |input, context| {
            callback(input, context)
        });
        config.id = Some(hook_id.clone());
        config.name = options.name;
        config.description = options.description;
        config.timeout_ms = options.timeout_ms;
        config.status_message = options.status_message;
        config.on_hook_success = options.on_hook_success;

        let entry = SessionHookEntry {
            hook_id: hook_id.clone(),
            event_name: event,
            matcher: matcher.into(),
            config: SessionHookConfig::Function(config),
            sequential: None,
            skill_root: options.skill_root,
        };
        self.push_entry(session_id.into(), entry);
        hook_id
    }

    /// Add a session command or HTTP hook payload.
    pub fn add_session_hook(
        &mut self,
        session_id: impl Into<String>,
        event: HookEventName,
        matcher: impl Into<String>,
        hook: Value,
        options: Option<SessionHookOptions>,
    ) -> String {
        let options = options.unwrap_or_default();
        let hook_id = generate_hook_id();
        let entry = SessionHookEntry {
            hook_id: hook_id.clone(),
            event_name: event,
            matcher: matcher.into(),
            config: SessionHookConfig::Json(hook),
            sequential: options.sequential,
            skill_root: options.skill_root,
        };
        self.push_entry(session_id.into(), entry);
        hook_id
    }

    /// Remove the first matching ID under the specified event.
    pub fn remove_function_hook(
        &mut self,
        session_id: &str,
        event: HookEventName,
        hook_id: &str,
    ) -> bool {
        let Some(event_hooks) = self
            .sessions
            .get_mut(session_id)
            .and_then(|storage| storage.hooks.get_mut(&event))
        else {
            return false;
        };
        let Some(index) = event_hooks
            .iter()
            .position(|entry| entry.hook_id == hook_id)
        else {
            return false;
        };
        event_hooks.remove(index);
        true
    }

    /// Remove the first matching ID, searching events in insertion order.
    pub fn remove_hook(&mut self, session_id: &str, hook_id: &str) -> bool {
        let Some(storage) = self.sessions.get_mut(session_id) else {
            return false;
        };
        for event_hooks in storage.hooks.values_mut() {
            if let Some(index) = event_hooks
                .iter()
                .position(|entry| entry.hook_id == hook_id)
            {
                event_hooks.remove(index);
                return true;
            }
        }
        false
    }

    /// Return event hooks in registration order. The returned vector is a
    /// snapshot, unlike JavaScript's internal mutable array reference.
    pub fn get_hooks_for_event(
        &self,
        session_id: &str,
        event: HookEventName,
    ) -> Vec<SessionHookEntry> {
        self.sessions
            .get(session_id)
            .and_then(|storage| storage.hooks.get(&event))
            .cloned()
            .unwrap_or_default()
    }

    /// Whether any session has hooks for the event, or whether the optional
    /// session has any.
    pub fn has_hooks_for_event(&self, event: HookEventName, session_id: Option<&str>) -> bool {
        if let Some(session_id) = session_id {
            return self
                .sessions
                .get(session_id)
                .and_then(|storage| storage.hooks.get(&event))
                .is_some_and(|entries| !entries.is_empty());
        }
        self.sessions.values().any(|storage| {
            storage
                .hooks
                .get(&event)
                .is_some_and(|entries| !entries.is_empty())
        })
    }

    /// Return matching hooks in their original registration order.
    pub fn get_matching_hooks(
        &self,
        session_id: &str,
        event: HookEventName,
        target: &str,
    ) -> Vec<SessionHookEntry> {
        self.get_hooks_for_event(session_id, event)
            .into_iter()
            .filter(|entry| {
                if is_tool_matcher_event(event) {
                    matches_tool_pattern(&entry.matcher, target)
                } else {
                    matches_pattern(&entry.matcher, target)
                }
            })
            .collect()
    }

    pub fn has_session_hooks(&self, session_id: &str) -> bool {
        self.sessions
            .get(session_id)
            .is_some_and(|storage| storage.hooks.values().any(|entries| !entries.is_empty()))
    }

    /// Remove the session's storage. Removals alone retain empty session
    /// storage, matching the source map's lifecycle.
    pub fn clear_session_hooks(&mut self, session_id: &str) {
        self.sessions.shift_remove(session_id);
    }

    /// Return session IDs in the order their storage was first created.
    pub fn get_active_sessions(&self) -> Vec<String> {
        self.sessions.keys().cloned().collect()
    }

    pub fn get_hook_count(&self, session_id: &str) -> usize {
        self.sessions
            .get(session_id)
            .map(|storage| storage.hooks.values().map(Vec::len).sum())
            .unwrap_or_default()
    }

    /// Return all session hooks ordered first by event insertion, then by
    /// hook insertion within each event.
    pub fn get_all_session_hooks(&self, session_id: &str) -> Vec<SessionHookEntry> {
        self.sessions
            .get(session_id)
            .map(|storage| {
                storage
                    .hooks
                    .values()
                    .flat_map(|entries| entries.iter().cloned())
                    .collect()
            })
            .unwrap_or_default()
    }

    fn push_entry(&mut self, session_id: String, entry: SessionHookEntry) {
        self.sessions
            .entry(session_id)
            .or_default()
            .hooks
            .entry(entry.event_name)
            .or_default()
            .push(entry);
    }
}

fn is_tool_matcher_event(event: HookEventName) -> bool {
    get_hook_matcher_target(event, None)
        .is_some_and(|target| target.kind == HookMatcherTargetKind::ToolName)
}

fn matches_tool_pattern(pattern: &str, tool_name: &str) -> bool {
    if pattern.contains('|') && !pattern.starts_with('^') && !pattern.starts_with('(') {
        return pattern
            .split('|')
            .map(str::trim)
            .any(|alternative| matches_tool_pattern(alternative, tool_name));
    }

    if get_tool_matcher_targets(tool_name)
        .iter()
        .any(|target| target == pattern)
    {
        return true;
    }
    matches_pattern(pattern, tool_name)
}

/// Match `*`, split alternatives, and anchored regexes. Invalid regex syntax
/// falls back to exact equality, matching the TypeScript manager.
fn matches_pattern(pattern: &str, target: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if pattern.contains('|') && !pattern.starts_with('^') && !pattern.starts_with('(') {
        return pattern
            .split('|')
            .map(str::trim)
            .any(|alternative| matches_pattern(alternative, target));
    }

    match Regex::new(&format!("^{pattern}$")) {
        Ok(regex) => regex.is_match(target),
        Err(_) => pattern == target,
    }
}

fn generate_hook_id() -> String {
    let timestamp_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let random = Uuid::new_v4().as_u128();
    format!("session_hook_{timestamp_ms}_{}", base36_suffix(random))
}

fn base36_suffix(mut value: u128) -> String {
    const DIGITS: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut suffix = [b'0'; 7];
    for digit in suffix.iter_mut().rev() {
        *digit = DIGITS[(value % 36) as usize];
        value /= 36;
    }
    String::from_utf8_lossy(&suffix).into_owned()
}
