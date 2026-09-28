//! Host-neutral builders for hook event inputs.
//!
//! Port of the input-construction portions of
//! `packages/core/src/hooks/hookEventHandler.ts`. Configuration reads,
//! timestamp generation, event execution, logging, and telemetry stay in the
//! host. The host passes the shared input fields and snapshots into this
//! module.

use serde::Serialize;
use serde_json::{Map, Value};

use super::planner::{HookEventContext, HookEventName};

/// Shared hook input fields supplied by the host.
///
/// `source_type` and `source_id` are omitted from the JSON payload when absent,
/// matching the TypeScript object spread in `createBaseInput`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HookBaseInput {
    pub session_id: String,
    pub source_type: Option<String>,
    pub source_id: Option<String>,
    pub transcript_path: String,
    pub cwd: String,
    pub timestamp: String,
}

/// Host snapshots appended to Stop and SubagentStop payloads.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct HookEventSnapshots {
    pub background_tasks: Vec<Value>,
    pub crons: Vec<Value>,
}

/// Input and matcher context needed by the planner for one hook event.
#[derive(Clone, Debug, PartialEq)]
pub struct HookEventPayload {
    pub event_name: HookEventName,
    pub input: Value,
    pub matcher_context: Option<HookEventContext>,
}

/// Stop event context usage fields.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ContextUsageData {
    pub context_usage: f64,
    pub context_limit: f64,
    pub input_tokens: f64,
}

/// Pure input builder. Host runtime values are supplied to each builder call.
#[derive(Clone, Copy, Debug, Default)]
pub struct HookEventInputBuilder;

impl HookEventInputBuilder {
    pub fn new() -> Self {
        Self
    }

    pub fn user_prompt_submit(
        &self,
        base: &HookBaseInput,
        prompt: &str,
        submitted_prompt: Option<&str>,
    ) -> HookEventPayload {
        let event = HookEventName::UserPromptSubmit;
        let mut input = base_object(base, event);
        put(&mut input, "prompt", prompt);
        if let Some(submitted_prompt) = submitted_prompt.filter(|value| !value.trim().is_empty()) {
            put(&mut input, "submitted_prompt", submitted_prompt);
        }
        payload(event, input, None)
    }

    pub fn instructions_loaded(
        &self,
        base: &HookBaseInput,
        file_path: &str,
        memory_type: &str,
        load_reason: &str,
        trigger_file_path: Option<&str>,
        parent_file_path: Option<&str>,
    ) -> HookEventPayload {
        let event = HookEventName::InstructionsLoaded;
        let mut input = base_object(base, event);
        put(&mut input, "file_path", file_path);
        put(&mut input, "memory_type", memory_type);
        put(&mut input, "load_reason", load_reason);
        put_opt(&mut input, "trigger_file_path", trigger_file_path);
        put_opt(&mut input, "parent_file_path", parent_file_path);
        payload(
            event,
            input,
            Some(HookEventContext {
                file_path: Some(file_path.to_owned()),
                ..HookEventContext::default()
            }),
        )
    }

    pub fn user_prompt_expansion(
        &self,
        base: &HookBaseInput,
        command_name: &str,
        command_args: &str,
        prompt: &str,
    ) -> HookEventPayload {
        let event = HookEventName::UserPromptExpansion;
        let mut input = base_object(base, event);
        put(&mut input, "command_name", command_name);
        put(&mut input, "command_args", command_args);
        put(&mut input, "prompt", prompt);
        payload(
            event,
            input,
            Some(HookEventContext {
                command_name: Some(command_name.to_owned()),
                ..HookEventContext::default()
            }),
        )
    }

    pub fn stop(
        &self,
        base: &HookBaseInput,
        snapshots: &HookEventSnapshots,
        stop_hook_active: Option<bool>,
        last_assistant_message: Option<&str>,
        context_usage: Option<ContextUsageData>,
    ) -> HookEventPayload {
        let event = HookEventName::Stop;
        let mut input = base_object(base, event);
        put(
            &mut input,
            "stop_hook_active",
            stop_hook_active.unwrap_or(false),
        );
        put(
            &mut input,
            "last_assistant_message",
            last_assistant_message.unwrap_or(""),
        );
        put(
            &mut input,
            "background_tasks",
            Value::Array(snapshots.background_tasks.clone()),
        );
        put(&mut input, "crons", Value::Array(snapshots.crons.clone()));
        if let Some(context_usage) = context_usage {
            put(&mut input, "context_usage", context_usage.context_usage);
            put(&mut input, "context_limit", context_usage.context_limit);
            put(&mut input, "input_tokens", context_usage.input_tokens);
        }
        payload(event, input, None)
    }

    pub fn message_display(
        &self,
        base: &HookBaseInput,
        message_id: &str,
        displayed_text: &str,
        is_final: bool,
    ) -> HookEventPayload {
        let event = HookEventName::MessageDisplay;
        let mut input = base_object(base, event);
        put(&mut input, "message_id", message_id);
        put(&mut input, "displayed_text", displayed_text);
        put(&mut input, "is_final", is_final);
        payload(event, input, None)
    }

    pub fn session_start(
        &self,
        base: &HookBaseInput,
        source: &str,
        model: &str,
        permission_mode: Option<&str>,
        agent_type: Option<&str>,
    ) -> HookEventPayload {
        let event = HookEventName::SessionStart;
        let mut input = base_object(base, event);
        put(
            &mut input,
            "permission_mode",
            permission_mode.unwrap_or("default"),
        );
        put(&mut input, "source", source);
        put(&mut input, "model", model);
        put_opt(&mut input, "agent_type", agent_type);
        payload(
            event,
            input,
            Some(HookEventContext {
                trigger: Some(source.to_owned()),
                ..HookEventContext::default()
            }),
        )
    }

    pub fn session_end(&self, base: &HookBaseInput, reason: &str) -> HookEventPayload {
        let event = HookEventName::SessionEnd;
        let mut input = base_object(base, event);
        put(&mut input, "reason", reason);
        payload(
            event,
            input,
            Some(HookEventContext {
                trigger: Some(reason.to_owned()),
                ..HookEventContext::default()
            }),
        )
    }

    pub fn session_delete(
        &self,
        base: &HookBaseInput,
        deleted_session_id: &str,
    ) -> HookEventPayload {
        let event = HookEventName::SessionDelete;
        let mut input = base_object(base, event);
        put(&mut input, "deleted_session_id", deleted_session_id);
        payload(event, input, None)
    }

    pub fn pre_tool_use(
        &self,
        base: &HookBaseInput,
        permission_mode: &str,
        tool_name: &str,
        tool_input: Value,
        tool_use_id: &str,
        tool_call_id: Option<&str>,
    ) -> HookEventPayload {
        let event = HookEventName::PreToolUse;
        let mut input = base_object(base, event);
        put(&mut input, "permission_mode", permission_mode);
        put(&mut input, "tool_name", tool_name);
        put(&mut input, "tool_input", tool_input);
        put(&mut input, "tool_use_id", tool_use_id);
        put_truthy_opt(&mut input, "tool_call_id", tool_call_id);
        payload(event, input, Some(tool_context(tool_name)))
    }

    pub fn post_tool_use(
        &self,
        base: &HookBaseInput,
        permission_mode: &str,
        tool_name: &str,
        tool_input: Value,
        tool_response: Value,
        tool_use_id: &str,
        tool_call_id: Option<&str>,
    ) -> HookEventPayload {
        let event = HookEventName::PostToolUse;
        let mut input = base_object(base, event);
        put(&mut input, "permission_mode", permission_mode);
        put(&mut input, "tool_name", tool_name);
        put(&mut input, "tool_input", tool_input);
        put(&mut input, "tool_response", tool_response);
        put(&mut input, "tool_use_id", tool_use_id);
        put_truthy_opt(&mut input, "tool_call_id", tool_call_id);
        payload(event, input, Some(tool_context(tool_name)))
    }

    pub fn post_tool_use_failure(
        &self,
        base: &HookBaseInput,
        tool_use_id: &str,
        tool_name: &str,
        tool_input: Value,
        error_message: &str,
        is_interrupt: Option<bool>,
        permission_mode: Option<&str>,
        tool_call_id: Option<&str>,
    ) -> HookEventPayload {
        let event = HookEventName::PostToolUseFailure;
        let mut input = base_object(base, event);
        put(
            &mut input,
            "permission_mode",
            permission_mode.unwrap_or("default"),
        );
        put(&mut input, "tool_use_id", tool_use_id);
        put_truthy_opt(&mut input, "tool_call_id", tool_call_id);
        put(&mut input, "tool_name", tool_name);
        put(&mut input, "tool_input", tool_input);
        put(&mut input, "error", error_message);
        put_opt(&mut input, "is_interrupt", is_interrupt);
        payload(event, input, Some(tool_context(tool_name)))
    }

    pub fn pre_compact(
        &self,
        base: &HookBaseInput,
        trigger: &str,
        custom_instructions: Option<&str>,
    ) -> HookEventPayload {
        let event = HookEventName::PreCompact;
        let mut input = base_object(base, event);
        put(&mut input, "trigger", trigger);
        put(
            &mut input,
            "custom_instructions",
            custom_instructions.unwrap_or(""),
        );
        payload(event, input, Some(trigger_context(trigger)))
    }

    pub fn post_tool_batch(
        &self,
        base: &HookBaseInput,
        tool_calls: Value,
        permission_mode: Option<&str>,
    ) -> HookEventPayload {
        let event = HookEventName::PostToolBatch;
        let mut input = base_object(base, event);
        put(
            &mut input,
            "permission_mode",
            permission_mode.unwrap_or("default"),
        );
        put(&mut input, "tool_calls", tool_calls);
        payload(event, input, None)
    }

    pub fn notification(
        &self,
        base: &HookBaseInput,
        message: &str,
        notification_type: &str,
        title: Option<&str>,
    ) -> HookEventPayload {
        let event = HookEventName::Notification;
        let mut input = base_object(base, event);
        put(&mut input, "message", message);
        put(&mut input, "notification_type", notification_type);
        put_opt(&mut input, "title", title);
        payload(
            event,
            input,
            Some(HookEventContext {
                notification_type: Some(notification_type.to_owned()),
                ..HookEventContext::default()
            }),
        )
    }

    pub fn permission_request(
        &self,
        base: &HookBaseInput,
        permission_mode: &str,
        tool_name: &str,
        tool_input: Value,
        permission_suggestions: Option<Vec<Value>>,
    ) -> HookEventPayload {
        let event = HookEventName::PermissionRequest;
        let mut input = base_object(base, event);
        put(&mut input, "permission_mode", permission_mode);
        put(&mut input, "tool_name", tool_name);
        put(&mut input, "tool_input", tool_input);
        put_opt(&mut input, "permission_suggestions", permission_suggestions);
        payload(event, input, Some(tool_context(tool_name)))
    }

    pub fn permission_denied(
        &self,
        base: &HookBaseInput,
        tool_name: &str,
        tool_input: Value,
        tool_use_id: &str,
        reason: &str,
        tool_call_id: Option<&str>,
    ) -> HookEventPayload {
        let event = HookEventName::PermissionDenied;
        let mut input = base_object(base, event);
        put(&mut input, "tool_name", tool_name);
        put(&mut input, "tool_input", tool_input);
        put(&mut input, "tool_use_id", tool_use_id);
        put_truthy_opt(&mut input, "tool_call_id", tool_call_id);
        put(&mut input, "reason", reason);
        payload(event, input, Some(tool_context(tool_name)))
    }

    pub fn subagent_start(
        &self,
        base: &HookBaseInput,
        agent_id: &str,
        agent_type: &str,
        permission_mode: &str,
    ) -> HookEventPayload {
        let event = HookEventName::SubagentStart;
        let mut input = base_object(base, event);
        put(&mut input, "permission_mode", permission_mode);
        put(&mut input, "agent_id", agent_id);
        put(&mut input, "agent_type", agent_type);
        payload(event, input, Some(agent_context(agent_type)))
    }

    pub fn subagent_stop(
        &self,
        base: &HookBaseInput,
        snapshots: &HookEventSnapshots,
        agent_id: &str,
        agent_type: &str,
        agent_transcript_path: &str,
        last_assistant_message: &str,
        stop_hook_active: bool,
        permission_mode: &str,
    ) -> HookEventPayload {
        let event = HookEventName::SubagentStop;
        let mut input = base_object(base, event);
        put(&mut input, "permission_mode", permission_mode);
        put(&mut input, "stop_hook_active", stop_hook_active);
        put(&mut input, "agent_id", agent_id);
        put(&mut input, "agent_type", agent_type);
        put(&mut input, "agent_transcript_path", agent_transcript_path);
        put(&mut input, "last_assistant_message", last_assistant_message);
        put(
            &mut input,
            "background_tasks",
            Value::Array(snapshots.background_tasks.clone()),
        );
        put(&mut input, "crons", Value::Array(snapshots.crons.clone()));
        payload(event, input, Some(agent_context(agent_type)))
    }

    pub fn stop_failure(
        &self,
        base: &HookBaseInput,
        error: &str,
        error_details: Option<&str>,
        last_assistant_message: Option<&str>,
    ) -> HookEventPayload {
        let event = HookEventName::StopFailure;
        let mut input = base_object(base, event);
        put(&mut input, "error", error);
        put_opt(&mut input, "error_details", error_details);
        put_opt(&mut input, "last_assistant_message", last_assistant_message);
        payload(
            event,
            input,
            Some(HookEventContext {
                error: Some(error.to_owned()),
                ..HookEventContext::default()
            }),
        )
    }

    pub fn post_compact(
        &self,
        base: &HookBaseInput,
        trigger: &str,
        compact_summary: &str,
    ) -> HookEventPayload {
        let event = HookEventName::PostCompact;
        let mut input = base_object(base, event);
        put(&mut input, "trigger", trigger);
        put(&mut input, "compact_summary", compact_summary);
        payload(event, input, Some(trigger_context(trigger)))
    }

    pub fn todo_created(
        &self,
        base: &HookBaseInput,
        todo_id: &str,
        todo_content: &str,
        todo_status: &str,
        all_todos: Value,
        phase: &str,
    ) -> HookEventPayload {
        let event = HookEventName::TodoCreated;
        let mut input = base_object(base, event);
        // Source assigns this literal a second time after spreading createBaseInput.
        put(&mut input, "hook_event_name", "TodoCreated");
        put(&mut input, "todo_id", todo_id);
        put(&mut input, "todo_content", todo_content);
        put(&mut input, "todo_status", todo_status);
        put(&mut input, "all_todos", all_todos);
        put(&mut input, "phase", phase);
        payload(event, input, None)
    }

    pub fn todo_completed(
        &self,
        base: &HookBaseInput,
        todo_id: &str,
        todo_content: &str,
        previous_status: &str,
        all_todos: Value,
        phase: &str,
    ) -> HookEventPayload {
        let event = HookEventName::TodoCompleted;
        let mut input = base_object(base, event);
        // Source assigns this literal a second time after spreading createBaseInput.
        put(&mut input, "hook_event_name", "TodoCompleted");
        put(&mut input, "todo_id", todo_id);
        put(&mut input, "todo_content", todo_content);
        put(&mut input, "previous_status", previous_status);
        put(&mut input, "all_todos", all_todos);
        put(&mut input, "phase", phase);
        payload(event, input, None)
    }
}

fn base_object(base: &HookBaseInput, event: HookEventName) -> Map<String, Value> {
    let mut input = Map::new();
    put(&mut input, "session_id", &base.session_id);
    put_opt(&mut input, "source_type", base.source_type.as_deref());
    put_opt(&mut input, "source_id", base.source_id.as_deref());
    put(&mut input, "transcript_path", &base.transcript_path);
    put(&mut input, "cwd", &base.cwd);
    put(
        &mut input,
        "hook_event_name",
        serde_json::to_value(event).expect("HookEventName is JSON serializable"),
    );
    put(&mut input, "timestamp", &base.timestamp);
    input
}

fn payload(
    event_name: HookEventName,
    input: Map<String, Value>,
    matcher_context: Option<HookEventContext>,
) -> HookEventPayload {
    HookEventPayload {
        event_name,
        input: Value::Object(input),
        matcher_context,
    }
}

fn put<T: Serialize>(object: &mut Map<String, Value>, key: &str, value: T) {
    object.insert(
        key.to_owned(),
        serde_json::to_value(value).expect("hook input field is JSON serializable"),
    );
}

fn put_opt<T: Serialize>(object: &mut Map<String, Value>, key: &str, value: Option<T>) {
    if let Some(value) = value {
        put(object, key, value);
    }
}

fn put_truthy_opt(object: &mut Map<String, Value>, key: &str, value: Option<&str>) {
    if let Some(value) = value.filter(|value| !value.is_empty()) {
        put(object, key, value);
    }
}

fn tool_context(tool_name: &str) -> HookEventContext {
    HookEventContext {
        tool_name: Some(tool_name.to_owned()),
        ..HookEventContext::default()
    }
}

fn trigger_context(trigger: &str) -> HookEventContext {
    HookEventContext {
        trigger: Some(trigger.to_owned()),
        ..HookEventContext::default()
    }
}

fn agent_context(agent_type: &str) -> HookEventContext {
    HookEventContext {
        agent_type: Some(agent_type.to_owned()),
        ..HookEventContext::default()
    }
}
