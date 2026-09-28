//! Event-specific aggregation of hook execution outputs.
//!
//! Port of `packages/core/src/hooks/hookAggregator.ts`. Input and output
//! payloads remain `serde_json::Value` so provider-specific hook fields survive
//! merging without narrowing the TypeScript schema.

use serde_json::{Map, Value};

use super::planner::HookEventName;

/// One hook's fields consumed by the aggregator.
///
/// The TypeScript execution result has additional hook/event metadata. The
/// aggregator only reads these four fields, so callers can adapt at that
/// boundary without duplicating unrelated fields here.
#[derive(Clone, Debug, PartialEq)]
pub struct HookExecutionResult {
    pub success: bool,
    pub output: Option<Value>,
    /// Rust counterpart to `Error.message`; only included when `success` is
    /// false, matching the source aggregator's error policy.
    pub error: Option<String>,
    pub duration: f64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SpecificHookOutputKind {
    Default,
    PreToolUse,
    PostToolUse,
    PostToolUseFailure,
    UserPromptExpansion,
    PostToolBatch,
    Stop,
    PermissionRequest,
}

/// Final output plus the source event's specialized output class.
///
/// `value` contains the fields that the corresponding TypeScript output class
/// would expose. Class-only accessor behavior is represented by `kind` for
/// downstream Rust code.
#[derive(Clone, Debug, PartialEq)]
pub struct SpecificHookOutput {
    pub kind: SpecificHookOutputKind,
    pub value: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AggregatedHookResult {
    pub success: bool,
    pub all_outputs: Vec<Value>,
    pub errors: Vec<String>,
    pub total_duration: f64,
    pub final_output: Option<SpecificHookOutput>,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct HookAggregator;

impl HookAggregator {
    /// Aggregate execution results using the event's merge policy.
    pub fn aggregate_results(
        &self,
        results: &[HookExecutionResult],
        event_name: HookEventName,
    ) -> AggregatedHookResult {
        aggregate_results(results, event_name)
    }
}

/// Aggregate execution results using the event's merge policy.
pub fn aggregate_results(
    results: &[HookExecutionResult],
    event_name: HookEventName,
) -> AggregatedHookResult {
    if event_name == HookEventName::StopFailure {
        return AggregatedHookResult {
            success: true,
            all_outputs: Vec::new(),
            errors: Vec::new(),
            total_duration: results.iter().map(|result| result.duration).sum(),
            final_output: None,
        };
    }

    let mut all_outputs = Vec::new();
    let mut errors = Vec::new();
    let mut total_duration = 0.0;

    for result in results {
        total_duration += result.duration;
        if !result.success {
            if let Some(error) = &result.error {
                errors.push(error.clone());
            }
        }
        if let Some(output) = &result.output {
            if js_truthy(output) {
                all_outputs.push(output.clone());
            }
        }
    }

    let final_output = merge_outputs(&all_outputs, event_name);
    AggregatedHookResult {
        success: errors.is_empty(),
        all_outputs,
        errors,
        total_duration,
        final_output,
    }
}

fn merge_outputs(outputs: &[Value], event_name: HookEventName) -> Option<SpecificHookOutput> {
    if outputs.is_empty() {
        return None;
    }

    if outputs.len() == 1 {
        return Some(create_specific_hook_output(outputs[0].clone(), event_name));
    }

    let merged = match event_name {
        HookEventName::PreToolUse
        | HookEventName::PostToolUse
        | HookEventName::PostToolUseFailure
        | HookEventName::PostToolBatch
        | HookEventName::Stop
        | HookEventName::UserPromptSubmit
        | HookEventName::UserPromptExpansion
        | HookEventName::SubagentStop
        | HookEventName::TodoCreated
        | HookEventName::TodoCompleted => merge_with_or_logic(outputs),
        HookEventName::PermissionRequest => merge_permission_request_outputs(outputs),
        _ => merge_simple(outputs),
    };
    Some(create_specific_hook_output(merged, event_name))
}

fn merge_with_or_logic(outputs: &[Value]) -> Value {
    let mut merged = Map::new();
    let mut reasons = Vec::new();
    let mut additional_contexts = Vec::new();
    let mut artifacts = Vec::new();
    let mut has_block = false;
    let mut has_continue_false = false;
    let mut stop_reason: Option<Value> = None;
    let mut other_specific_fields = Map::new();

    for output in outputs {
        let Some(output_object) = output.as_object() else {
            continue;
        };

        let decision = output_object.get("decision");
        if decision.is_some_and(|decision| {
            decision.as_str() == Some("block") || decision.as_str() == Some("deny")
        }) {
            has_block = true;
        }

        if let Some(reason) = output_object.get("reason").filter(|value| js_truthy(value)) {
            reasons.push(js_string(reason));
        }

        if output_object.get("continue").and_then(Value::as_bool) == Some(false) {
            has_continue_false = true;
            if let Some(reason) = output_object
                .get("stopReason")
                .filter(|value| js_truthy(value))
            {
                stop_reason = Some(reason.clone());
            }
        }

        let Some(specific) = output_object
            .get("hookSpecificOutput")
            .filter(|value| js_truthy(value))
            .and_then(Value::as_object)
        else {
            copy_or_fields(output_object, &mut merged);
            continue;
        };

        if let Some(Value::String(context)) = specific.get("additionalContext") {
            additional_contexts.push(context.clone());
        }

        if let Some(Value::Array(items)) = specific.get("artifacts") {
            artifacts.extend(
                items
                    .iter()
                    .filter(|artifact| is_tool_artifact_like(artifact))
                    .cloned(),
            );
        }

        for (key, value) in specific {
            if key != "additionalContext" && key != "artifacts" {
                other_specific_fields.insert(key.clone(), value.clone());
            }
        }

        copy_or_fields(output_object, &mut merged);
    }

    merge_terminal_sequences(outputs, &mut merged);

    if has_block {
        merged.insert("decision".to_owned(), Value::String("block".to_owned()));
    } else if outputs
        .iter()
        .any(|output| output.get("decision").and_then(Value::as_str) == Some("allow"))
    {
        merged.insert("decision".to_owned(), Value::String("allow".to_owned()));
    }

    if !reasons.is_empty() {
        merged.insert("reason".to_owned(), Value::String(reasons.join("\n")));
    }

    if has_continue_false {
        merged.insert("continue".to_owned(), Value::Bool(false));
        if let Some(stop_reason) = stop_reason {
            merged.insert("stopReason".to_owned(), stop_reason);
        }
    }

    if !additional_contexts.is_empty() {
        other_specific_fields.insert(
            "additionalContext".to_owned(),
            Value::String(additional_contexts.join("\n")),
        );
    }
    if !artifacts.is_empty() {
        other_specific_fields.insert("artifacts".to_owned(), Value::Array(artifacts));
    }
    if !other_specific_fields.is_empty() {
        merged.insert(
            "hookSpecificOutput".to_owned(),
            Value::Object(other_specific_fields),
        );
    }

    Value::Object(merged)
}

fn copy_or_fields(output: &Map<String, Value>, merged: &mut Map<String, Value>) {
    for field in ["suppressOutput", "systemMessage"] {
        if let Some(value) = output.get(field) {
            merged.insert(field.to_owned(), value.clone());
        }
    }
}

fn merge_permission_request_outputs(outputs: &[Value]) -> Value {
    let mut merged = Map::new();
    let mut messages = Vec::new();
    let mut has_deny = false;
    let mut has_allow = false;
    let mut interrupt = false;
    let mut updated_input: Option<Value> = None;
    let mut all_updated_permissions = Vec::new();

    for output in outputs {
        let Some(output_object) = output.as_object() else {
            continue;
        };
        let Some(specific) = output_object
            .get("hookSpecificOutput")
            .and_then(Value::as_object)
        else {
            continue;
        };
        let Some(decision_value) = specific.get("decision").filter(|value| js_truthy(value)) else {
            continue;
        };
        let Some(decision) = decision_value.as_object() else {
            continue;
        };

        match decision.get("behavior").and_then(Value::as_str) {
            Some("deny") => has_deny = true,
            Some("allow") => has_allow = true,
            _ => {}
        }

        if let Some(message) = decision.get("message").filter(|value| js_truthy(value)) {
            messages.push(js_string(message));
        }
        if decision.get("interrupt").and_then(Value::as_bool) == Some(true) {
            interrupt = true;
        }
        if let Some(input) = decision
            .get("updatedInput")
            .filter(|value| js_truthy(value))
        {
            updated_input = Some(input.clone());
        }
        if let Some(Value::Array(permissions)) = decision
            .get("updatedPermissions")
            .filter(|value| js_truthy(value))
        {
            all_updated_permissions.extend(permissions.iter().cloned());
        }

        for field in ["continue", "reason"] {
            if let Some(value) = output_object.get(field) {
                merged.insert(field.to_owned(), value.clone());
            }
        }
    }

    let mut merged_decision = Map::new();
    if has_deny {
        merged_decision.insert("behavior".to_owned(), Value::String("deny".to_owned()));
    } else if has_allow {
        merged_decision.insert("behavior".to_owned(), Value::String("allow".to_owned()));
    }
    if !messages.is_empty() {
        merged_decision.insert("message".to_owned(), Value::String(messages.join("\n")));
    }
    if interrupt {
        merged_decision.insert("interrupt".to_owned(), Value::Bool(true));
    }
    if let Some(updated_input) = updated_input {
        merged_decision.insert("updatedInput".to_owned(), updated_input);
    }
    if !all_updated_permissions.is_empty() {
        merged_decision.insert(
            "updatedPermissions".to_owned(),
            Value::Array(all_updated_permissions),
        );
    }

    let mut specific = Map::new();
    specific.insert("decision".to_owned(), Value::Object(merged_decision));
    merged.insert("hookSpecificOutput".to_owned(), Value::Object(specific));
    merge_terminal_sequences(outputs, &mut merged);
    Value::Object(merged)
}

fn merge_simple(outputs: &[Value]) -> Value {
    let mut merged = Map::new();
    let mut additional_contexts = Vec::new();

    for output in outputs {
        let Some(output_object) = output.as_object() else {
            continue;
        };
        if let Some(specific) = output_object
            .get("hookSpecificOutput")
            .filter(|value| js_truthy(value))
            .and_then(Value::as_object)
        {
            if let Some(Value::String(context)) = specific.get("additionalContext") {
                additional_contexts.push(context.clone());
            }
        }
        for (key, value) in output_object {
            if key != "terminalSequence" {
                merged.insert(key.clone(), value.clone());
            }
        }
    }

    if !additional_contexts.is_empty() {
        let mut specific = merged
            .remove("hookSpecificOutput")
            .and_then(|value| value.as_object().cloned())
            .unwrap_or_default();
        specific.insert(
            "additionalContext".to_owned(),
            Value::String(additional_contexts.join("\n")),
        );
        merged.insert("hookSpecificOutput".to_owned(), Value::Object(specific));
    }

    merge_terminal_sequences(outputs, &mut merged);
    Value::Object(merged)
}

fn create_specific_hook_output(output: Value, event_name: HookEventName) -> SpecificHookOutput {
    let kind = match event_name {
        HookEventName::PreToolUse => SpecificHookOutputKind::PreToolUse,
        HookEventName::PostToolUse => SpecificHookOutputKind::PostToolUse,
        HookEventName::PostToolUseFailure => SpecificHookOutputKind::PostToolUseFailure,
        HookEventName::UserPromptExpansion => SpecificHookOutputKind::UserPromptExpansion,
        HookEventName::PostToolBatch => SpecificHookOutputKind::PostToolBatch,
        HookEventName::Stop | HookEventName::SubagentStop => SpecificHookOutputKind::Stop,
        HookEventName::PermissionRequest => SpecificHookOutputKind::PermissionRequest,
        _ => SpecificHookOutputKind::Default,
    };

    // Every source output class copies only the HookOutput base fields. The
    // PostToolUse class additionally installs these defaults on missing/null.
    let mut value = Map::new();
    if let Some(object) = output.as_object() {
        for field in [
            "continue",
            "stopReason",
            "suppressOutput",
            "systemMessage",
            "terminalSequence",
            "decision",
            "reason",
            "hookSpecificOutput",
        ] {
            if let Some(field_value) = object.get(field) {
                value.insert(field.to_owned(), field_value.clone());
            }
        }
    }
    if event_name == HookEventName::PostToolUse {
        if value.get("decision").is_none_or(Value::is_null) {
            value.insert("decision".to_owned(), Value::String("allow".to_owned()));
        }
        if value.get("reason").is_none_or(Value::is_null) {
            value.insert(
                "reason".to_owned(),
                Value::String("No reason provided".to_owned()),
            );
        }
    }

    SpecificHookOutput {
        kind,
        value: Value::Object(value),
    }
}

fn merge_terminal_sequences(outputs: &[Value], merged: &mut Map<String, Value>) {
    let sequences = outputs
        .iter()
        .filter_map(|output| output.get("terminalSequence").and_then(Value::as_str))
        .filter(|sequence| !sequence.is_empty())
        .collect::<Vec<_>>();
    if sequences.is_empty() {
        merged.remove("terminalSequence");
    } else {
        merged.insert(
            "terminalSequence".to_owned(),
            Value::String(sequences.concat()),
        );
    }
}

fn js_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_f64().is_some_and(|value| value != 0.0),
        Value::String(value) => !value.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}

fn js_string(value: &Value) -> String {
    match value {
        Value::Null => "null".to_owned(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => value.clone(),
        Value::Array(values) => values.iter().map(js_string).collect::<Vec<_>>().join(","),
        Value::Object(_) => "[object Object]".to_owned(),
    }
}

fn is_tool_artifact_like(value: &Value) -> bool {
    let Some(object) = value.as_object() else {
        return false;
    };
    if !object.get("title").is_some_and(Value::is_string) {
        return false;
    }

    for key in [
        "kind",
        "storage",
        "description",
        "workspacePath",
        "managedId",
        "url",
        "mimeType",
    ] {
        if object.get(key).is_some_and(|value| !value.is_string()) {
            return false;
        }
    }

    if let Some(size) = object.get("sizeBytes") {
        let Some(size) = size.as_f64() else {
            return false;
        };
        const MAX_SAFE_INTEGER: f64 = 9_007_199_254_740_991.0;
        if !size.is_finite() || size.fract() != 0.0 || !(0.0..=MAX_SAFE_INTEGER).contains(&size) {
            return false;
        }
    }

    if let Some(metadata) = object.get("metadata") {
        let Some(metadata) = metadata.as_object() else {
            return false;
        };
        if metadata.values().any(|value| {
            !matches!(
                value,
                Value::Null | Value::String(_) | Value::Bool(_) | Value::Number(_)
            )
        }) {
            return false;
        }
    }

    true
}
