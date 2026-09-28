//! Hook event matcher routing and execution-plan selection.
//!
//! Port of `packages/core/src/hooks/hookPlanner.ts`. The caller supplies the
//! event's registry entries in registry order (enabled entries only), matching
//! `HookRegistry.getHooksForEvent`'s contract. Hook configs stay as JSON values
//! so planning preserves fields used by later execution stages.
//!
//! Matcher regexes use Rust's `regex` syntax. JavaScript-only constructs such
//! as look-around and backreferences therefore follow the source's invalid
//! regex fallback here, and otherwise-valid JavaScript patterns may differ.

use std::collections::HashSet;

use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::tool_utils::get_alias_set_for_tool;

/// Events supported by the TypeScript hook planner.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub enum HookEventName {
    PreToolUse,
    PostToolUse,
    PostToolUseFailure,
    PostToolBatch,
    Notification,
    UserPromptSubmit,
    UserPromptExpansion,
    SessionStart,
    Stop,
    MessageDisplay,
    SubagentStart,
    SubagentStop,
    PreCompact,
    PostCompact,
    SessionEnd,
    SessionDelete,
    PermissionRequest,
    PermissionDenied,
    StopFailure,
    TodoCreated,
    TodoCompleted,
    InstructionsLoaded,
}

/// Event-specific field used to route a configured hook matcher.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HookMatcherTargetKind {
    ToolName,
    CommandName,
    AgentType,
    Trigger,
    SessionTrigger,
    Error,
    NotificationType,
    FilePath,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HookMatcherTarget {
    pub kind: HookMatcherTargetKind,
    pub target: String,
}

/// Optional context fields consumed by event-specific matchers.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HookEventContext {
    pub tool_name: Option<String>,
    pub command_name: Option<String>,
    pub trigger: Option<String>,
    pub notification_type: Option<String>,
    pub agent_type: Option<String>,
    pub error: Option<String>,
    pub file_path: Option<String>,
}

/// Registry entry subset consumed by the planner.
///
/// `config` is the complete hook config object, including execution-only
/// fields. Entries are assumed to have already been filtered for event and
/// enabled state and sorted using registry source priority.
#[derive(Clone, Debug, PartialEq)]
pub struct HookPlannerEntry {
    pub config: Value,
    pub matcher: Option<String>,
    pub sequential: Option<bool>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct HookExecutionPlan {
    pub event_name: HookEventName,
    pub hook_configs: Vec<Value>,
    pub sequential: bool,
}

/// Return the matcher target for an event, including an empty target when the
/// event has matcher semantics but its corresponding context field is absent.
pub fn get_hook_matcher_target(
    event_name: HookEventName,
    context: Option<&HookEventContext>,
) -> Option<HookMatcherTarget> {
    let field_target = |kind, value: Option<&String>| HookMatcherTarget {
        kind,
        target: value.cloned().unwrap_or_default(),
    };

    match event_name {
        HookEventName::PreToolUse
        | HookEventName::PostToolUse
        | HookEventName::PostToolUseFailure
        | HookEventName::PermissionRequest
        | HookEventName::PermissionDenied => Some(field_target(
            HookMatcherTargetKind::ToolName,
            context.and_then(|context| context.tool_name.as_ref()),
        )),
        HookEventName::SubagentStart | HookEventName::SubagentStop => Some(field_target(
            HookMatcherTargetKind::AgentType,
            context.and_then(|context| context.agent_type.as_ref()),
        )),
        HookEventName::PreCompact | HookEventName::PostCompact => Some(field_target(
            HookMatcherTargetKind::Trigger,
            context.and_then(|context| context.trigger.as_ref()),
        )),
        HookEventName::SessionStart | HookEventName::SessionEnd => Some(field_target(
            HookMatcherTargetKind::SessionTrigger,
            context.and_then(|context| context.trigger.as_ref()),
        )),
        HookEventName::StopFailure => Some(field_target(
            HookMatcherTargetKind::Error,
            context.and_then(|context| context.error.as_ref()),
        )),
        HookEventName::Notification => Some(field_target(
            HookMatcherTargetKind::NotificationType,
            context.and_then(|context| context.notification_type.as_ref()),
        )),
        HookEventName::InstructionsLoaded => Some(field_target(
            HookMatcherTargetKind::FilePath,
            context.and_then(|context| context.file_path.as_ref()),
        )),
        HookEventName::UserPromptExpansion => Some(field_target(
            HookMatcherTargetKind::CommandName,
            context.and_then(|context| context.command_name.as_ref()),
        )),
        HookEventName::UserPromptSubmit
        | HookEventName::Stop
        | HookEventName::MessageDisplay
        | HookEventName::PostToolBatch
        | HookEventName::SessionDelete
        | HookEventName::TodoCreated
        | HookEventName::TodoCompleted => None,
    }
}

/// Whether an event has matcher semantics, independent of context contents.
pub fn hook_event_supports_matcher(event_name: HookEventName) -> bool {
    get_hook_matcher_target(event_name, None).is_some()
}

/// Return aliases used for exact hook matcher checks.
///
/// The Rust tool alias helper stores aliases in a sorted set; unlike the
/// TypeScript `Set`, this returned vector is lexically ordered. Matching uses
/// membership only, so alias behavior is unchanged.
pub fn get_tool_matcher_targets(tool_name: &str) -> Vec<String> {
    get_alias_set_for_tool(tool_name).into_iter().collect()
}

/// Build a plan from entries returned for one event by the hook registry.
pub fn create_execution_plan(
    event_name: HookEventName,
    entries: &[HookPlannerEntry],
    context: Option<&HookEventContext>,
) -> Option<HookExecutionPlan> {
    if entries.is_empty() {
        return None;
    }

    let matching_entries = entries
        .iter()
        .filter(|entry| matches_context(entry, event_name, context))
        .collect::<Vec<_>>();
    if matching_entries.is_empty() {
        return None;
    }

    let mut seen = HashSet::new();
    let mut hook_configs = Vec::new();
    let mut sequential = false;
    for entry in matching_entries {
        let key = get_hook_key(&entry.config);
        if seen.insert(key) {
            sequential |= entry.sequential == Some(true);
            hook_configs.push(entry.config.clone());
        }
    }

    Some(HookExecutionPlan {
        event_name,
        hook_configs,
        sequential,
    })
}

/// Source-compatible config identity used by planner duplicate elimination.
pub fn get_hook_key(config: &Value) -> String {
    let name = config.get("name").and_then(Value::as_str).unwrap_or("");
    let hook_type = config.get("type").and_then(Value::as_str).unwrap_or("");
    match hook_type {
        "command" => keyed_value(name, config.get("command").and_then(Value::as_str)),
        "http" => keyed_value(name, config.get("url").and_then(Value::as_str)),
        "function" => {
            let id = config
                .get("id")
                .and_then(Value::as_str)
                .unwrap_or("function");
            if name.is_empty() {
                id.to_owned()
            } else {
                format!("{name}:{id}")
            }
        }
        "prompt" => keyed_value(name, config.get("prompt").and_then(Value::as_str)),
        _ if name.is_empty() => "unknown".to_owned(),
        _ => name.to_owned(),
    }
}

fn keyed_value(name: &str, value: Option<&str>) -> String {
    let value = value.unwrap_or("");
    if name.is_empty() {
        value.to_owned()
    } else {
        format!("{name}:{value}")
    }
}

fn matches_context(
    entry: &HookPlannerEntry,
    event_name: HookEventName,
    context: Option<&HookEventContext>,
) -> bool {
    let (Some(raw_matcher), Some(context)) = (entry.matcher.as_deref(), context) else {
        return true;
    };

    let matcher = raw_matcher.trim();
    if matcher.is_empty() || matcher == "*" {
        return true;
    }

    let Some(target) = get_hook_matcher_target(event_name, Some(context)) else {
        return true;
    };
    if target.target.is_empty() {
        return true;
    }

    match target.kind {
        HookMatcherTargetKind::ToolName => matches_tool_name(matcher, &target.target),
        HookMatcherTargetKind::CommandName => matches_regex_or_exact(matcher, &target.target),
        HookMatcherTargetKind::AgentType => matches_regex_or_exact(matcher, &target.target),
        HookMatcherTargetKind::Trigger | HookMatcherTargetKind::Error => matcher == target.target,
        HookMatcherTargetKind::NotificationType => matcher == target.target,
        HookMatcherTargetKind::FilePath | HookMatcherTargetKind::SessionTrigger => {
            matches_regex_or_exact(matcher, &target.target)
        }
    }
}

fn matches_tool_name(matcher: &str, tool_name: &str) -> bool {
    let targets = get_alias_set_for_tool(tool_name);
    if matcher.contains('|') && !matcher.starts_with('^') && !matcher.starts_with('(') {
        if matcher
            .split('|')
            .map(str::trim)
            .any(|alternative| targets.contains(alternative))
        {
            return true;
        }
    }

    if targets.contains(matcher) {
        return true;
    }

    // Alias expansion is exact; regexes are evaluated against only the
    // runtime identifier, preserving negative/exclusion matcher behavior.
    Regex::new(matcher)
        .map(|regex| regex.is_match(tool_name))
        .unwrap_or(false)
}

fn matches_regex_or_exact(matcher: &str, target: &str) -> bool {
    Regex::new(matcher)
        .map(|regex| regex.is_match(target))
        // The TypeScript planner falls back to a literal comparison when a
        // matcher is not a valid JavaScript regular expression.
        .unwrap_or_else(|_| matcher == target)
}
