//! Typed event-method façade for the native hook system.
//!
//! Port of the public `fire...Event` methods in
//! `packages/core/src/hooks/hookSystem.ts`. Host event data is projected by
//! [`HookEventInputBuilder`], then dispatched through [`HookSystem`].

use serde_json::{Map, Value};

use super::aggregator::{AggregatedHookResult, SpecificHookOutput};
use super::event_inputs::{
    ContextUsageData, HookBaseInput, HookEventInputBuilder, HookEventPayload, HookEventSnapshots,
};
use super::prompt_runner::PromptModelExecutor;
use super::system::HookSystem;
use crate::utils::cancellation::CancellationToken;

/// Per-invocation function-hook context and cancellation for one event.
///
/// The TypeScript hook system obtains messages from a shared provider and
/// accepts an `AbortSignal` per method. Rust callers pass the message snapshot
/// and cancellation token explicitly so concurrent events can use independent
/// values.
#[derive(Clone, Default)]
pub struct HookEventExecutionOptions<'a> {
    pub messages: Option<Vec<Map<String, Value>>>,
    pub cancellation: Option<&'a CancellationToken>,
}

impl<P: PromptModelExecutor + Send + Sync + 'static> HookSystem<P> {
    /// Fire `UserPromptSubmit`. Returns the event's specialized output when
    /// hooks produced one.
    pub async fn fire_user_prompt_submit_event(
        &self,
        base: &HookBaseInput,
        prompt: &str,
        submitted_prompt: Option<&str>,
        options: HookEventExecutionOptions<'_>,
    ) -> Option<SpecificHookOutput> {
        let payload =
            HookEventInputBuilder::new().user_prompt_submit(base, prompt, submitted_prompt);
        self.execute_for_output(&payload, options).await
    }

    /// Fire `InstructionsLoaded` with optional include/parent file context.
    pub async fn fire_instructions_loaded_event(
        &self,
        base: &HookBaseInput,
        file_path: &str,
        memory_type: &str,
        load_reason: &str,
        trigger_file_path: Option<&str>,
        parent_file_path: Option<&str>,
        options: HookEventExecutionOptions<'_>,
    ) -> Option<SpecificHookOutput> {
        let payload = HookEventInputBuilder::new().instructions_loaded(
            base,
            file_path,
            memory_type,
            load_reason,
            trigger_file_path,
            parent_file_path,
        );
        self.execute_for_output(&payload, options).await
    }

    /// Fire `UserPromptExpansion` after a slash command expands into a prompt.
    pub async fn fire_user_prompt_expansion_event(
        &self,
        base: &HookBaseInput,
        command_name: &str,
        command_args: &str,
        prompt: &str,
        options: HookEventExecutionOptions<'_>,
    ) -> Option<SpecificHookOutput> {
        let payload = HookEventInputBuilder::new().user_prompt_expansion(
            base,
            command_name,
            command_args,
            prompt,
        );
        self.execute_for_output(&payload, options).await
    }

    /// Fire `Stop`, including the host snapshots and optional context usage.
    pub async fn fire_stop_event(
        &self,
        base: &HookBaseInput,
        snapshots: &HookEventSnapshots,
        stop_hook_active: Option<bool>,
        last_assistant_message: Option<&str>,
        context_usage: Option<ContextUsageData>,
        options: HookEventExecutionOptions<'_>,
    ) -> AggregatedHookResult {
        let payload = HookEventInputBuilder::new().stop(
            base,
            snapshots,
            stop_hook_active,
            last_assistant_message,
            context_usage,
        );
        self.execute_aggregated(&payload, options).await
    }

    /// Fire `MessageDisplay` while assistant output is being streamed.
    pub async fn fire_message_display_event(
        &self,
        base: &HookBaseInput,
        message_id: &str,
        displayed_text: &str,
        is_final: bool,
        options: HookEventExecutionOptions<'_>,
    ) -> AggregatedHookResult {
        let payload = HookEventInputBuilder::new().message_display(
            base,
            message_id,
            displayed_text,
            is_final,
        );
        self.execute_aggregated(&payload, options).await
    }

    /// Fire `SessionStart`; the builder defaults an absent permission mode to
    /// `default` and omits an absent agent type.
    pub async fn fire_session_start_event(
        &self,
        base: &HookBaseInput,
        source: &str,
        model: &str,
        permission_mode: Option<&str>,
        agent_type: Option<&str>,
        options: HookEventExecutionOptions<'_>,
    ) -> Option<SpecificHookOutput> {
        let payload = HookEventInputBuilder::new().session_start(
            base,
            source,
            model,
            permission_mode,
            agent_type,
        );
        self.execute_for_output(&payload, options).await
    }

    /// Fire `SessionEnd` with the source-compatible reason matcher context.
    pub async fn fire_session_end_event(
        &self,
        base: &HookBaseInput,
        reason: &str,
        options: HookEventExecutionOptions<'_>,
    ) -> Option<SpecificHookOutput> {
        let payload = HookEventInputBuilder::new().session_end(base, reason);
        self.execute_for_output(&payload, options).await
    }

    /// Fire `SessionDelete` after a session is explicitly deleted.
    pub async fn fire_session_delete_event(
        &self,
        base: &HookBaseInput,
        deleted_session_id: &str,
        options: HookEventExecutionOptions<'_>,
    ) -> Option<SpecificHookOutput> {
        let payload = HookEventInputBuilder::new().session_delete(base, deleted_session_id);
        self.execute_for_output(&payload, options).await
    }

    /// Fire `PreToolUse` before a tool call.
    pub async fn fire_pre_tool_use_event(
        &self,
        base: &HookBaseInput,
        permission_mode: &str,
        tool_name: &str,
        tool_input: Map<String, Value>,
        tool_use_id: &str,
        tool_call_id: Option<&str>,
        options: HookEventExecutionOptions<'_>,
    ) -> Option<SpecificHookOutput> {
        let payload = HookEventInputBuilder::new().pre_tool_use(
            base,
            permission_mode,
            tool_name,
            Value::Object(tool_input),
            tool_use_id,
            tool_call_id,
        );
        self.execute_for_output(&payload, options).await
    }

    /// Fire `PostToolUse` after a successful tool call.
    pub async fn fire_post_tool_use_event(
        &self,
        base: &HookBaseInput,
        permission_mode: &str,
        tool_name: &str,
        tool_input: Map<String, Value>,
        tool_response: Map<String, Value>,
        tool_use_id: &str,
        tool_call_id: Option<&str>,
        options: HookEventExecutionOptions<'_>,
    ) -> Option<SpecificHookOutput> {
        let payload = HookEventInputBuilder::new().post_tool_use(
            base,
            permission_mode,
            tool_name,
            Value::Object(tool_input),
            Value::Object(tool_response),
            tool_use_id,
            tool_call_id,
        );
        self.execute_for_output(&payload, options).await
    }

    /// Fire `PostToolUseFailure` after a failed tool call.
    pub async fn fire_post_tool_use_failure_event(
        &self,
        base: &HookBaseInput,
        tool_use_id: &str,
        tool_name: &str,
        tool_input: Map<String, Value>,
        error_message: &str,
        is_interrupt: Option<bool>,
        permission_mode: Option<&str>,
        tool_call_id: Option<&str>,
        options: HookEventExecutionOptions<'_>,
    ) -> Option<SpecificHookOutput> {
        let payload = HookEventInputBuilder::new().post_tool_use_failure(
            base,
            tool_use_id,
            tool_name,
            Value::Object(tool_input),
            error_message,
            is_interrupt,
            permission_mode,
            tool_call_id,
        );
        self.execute_for_output(&payload, options).await
    }

    /// Fire `PostToolBatch` after all calls in a batch resolve. An absent
    /// permission mode defaults to `default` in the input builder.
    pub async fn fire_post_tool_batch_event(
        &self,
        base: &HookBaseInput,
        tool_calls: Vec<Value>,
        permission_mode: Option<&str>,
        options: HookEventExecutionOptions<'_>,
    ) -> Option<SpecificHookOutput> {
        let payload = HookEventInputBuilder::new().post_tool_batch(
            base,
            Value::Array(tool_calls),
            permission_mode,
        );
        self.execute_for_output(&payload, options).await
    }

    /// Fire `PreCompact` before conversation compaction. Missing custom
    /// instructions default to an empty string in the input builder.
    pub async fn fire_pre_compact_event(
        &self,
        base: &HookBaseInput,
        trigger: &str,
        custom_instructions: Option<&str>,
        options: HookEventExecutionOptions<'_>,
    ) -> Option<SpecificHookOutput> {
        let payload = HookEventInputBuilder::new().pre_compact(base, trigger, custom_instructions);
        self.execute_for_output(&payload, options).await
    }

    /// Fire `Notification`; the notification type is also the matcher target.
    pub async fn fire_notification_event(
        &self,
        base: &HookBaseInput,
        message: &str,
        notification_type: &str,
        title: Option<&str>,
        options: HookEventExecutionOptions<'_>,
    ) -> Option<SpecificHookOutput> {
        let payload =
            HookEventInputBuilder::new().notification(base, message, notification_type, title);
        self.execute_for_output(&payload, options).await
    }

    /// Fire `SubagentStart` after a subagent is spawned.
    pub async fn fire_subagent_start_event(
        &self,
        base: &HookBaseInput,
        agent_id: &str,
        agent_type: &str,
        permission_mode: &str,
        options: HookEventExecutionOptions<'_>,
    ) -> Option<SpecificHookOutput> {
        let payload = HookEventInputBuilder::new().subagent_start(
            base,
            agent_id,
            agent_type,
            permission_mode,
        );
        self.execute_for_output(&payload, options).await
    }

    /// Fire `SubagentStop` with the host's current background task and cron
    /// snapshots.
    pub async fn fire_subagent_stop_event(
        &self,
        base: &HookBaseInput,
        snapshots: &HookEventSnapshots,
        agent_id: &str,
        agent_type: &str,
        agent_transcript_path: &str,
        last_assistant_message: &str,
        stop_hook_active: bool,
        permission_mode: &str,
        options: HookEventExecutionOptions<'_>,
    ) -> Option<SpecificHookOutput> {
        let payload = HookEventInputBuilder::new().subagent_stop(
            base,
            snapshots,
            agent_id,
            agent_type,
            agent_transcript_path,
            last_assistant_message,
            stop_hook_active,
            permission_mode,
        );
        self.execute_for_output(&payload, options).await
    }

    /// Fire-and-forget `StopFailure`. The full aggregate is returned just as
    /// it is by the TypeScript hook system, though callers may ignore it.
    pub async fn fire_stop_failure_event(
        &self,
        base: &HookBaseInput,
        error: &str,
        error_details: Option<&str>,
        last_assistant_message: Option<&str>,
        options: HookEventExecutionOptions<'_>,
    ) -> AggregatedHookResult {
        let payload = HookEventInputBuilder::new().stop_failure(
            base,
            error,
            error_details,
            last_assistant_message,
        );
        self.execute_aggregated(&payload, options).await
    }

    /// Fire `PostCompact` after conversation compaction completes.
    pub async fn fire_post_compact_event(
        &self,
        base: &HookBaseInput,
        trigger: &str,
        compact_summary: &str,
        options: HookEventExecutionOptions<'_>,
    ) -> Option<SpecificHookOutput> {
        let payload = HookEventInputBuilder::new().post_compact(base, trigger, compact_summary);
        self.execute_for_output(&payload, options).await
    }

    /// Fire `PermissionRequest` before presenting a permission dialog.
    pub async fn fire_permission_request_event(
        &self,
        base: &HookBaseInput,
        permission_mode: &str,
        tool_name: &str,
        tool_input: Map<String, Value>,
        permission_suggestions: Option<Vec<Value>>,
        options: HookEventExecutionOptions<'_>,
    ) -> Option<SpecificHookOutput> {
        let payload = HookEventInputBuilder::new().permission_request(
            base,
            permission_mode,
            tool_name,
            Value::Object(tool_input),
            permission_suggestions,
        );
        self.execute_for_output(&payload, options).await
    }

    /// Fire `PermissionDenied` when a request is rejected before a dialog.
    pub async fn fire_permission_denied_event(
        &self,
        base: &HookBaseInput,
        tool_name: &str,
        tool_input: Map<String, Value>,
        tool_use_id: &str,
        reason: &str,
        tool_call_id: Option<&str>,
        options: HookEventExecutionOptions<'_>,
    ) -> Option<SpecificHookOutput> {
        let payload = HookEventInputBuilder::new().permission_denied(
            base,
            tool_name,
            Value::Object(tool_input),
            tool_use_id,
            reason,
            tool_call_id,
        );
        self.execute_for_output(&payload, options).await
    }

    /// Fire `TodoCreated` with the validation or post-write phase.
    pub async fn fire_todo_created_event(
        &self,
        base: &HookBaseInput,
        todo_id: &str,
        todo_content: &str,
        todo_status: &str,
        all_todos: Vec<Value>,
        phase: &str,
        options: HookEventExecutionOptions<'_>,
    ) -> AggregatedHookResult {
        let payload = HookEventInputBuilder::new().todo_created(
            base,
            todo_id,
            todo_content,
            todo_status,
            Value::Array(all_todos),
            phase,
        );
        self.execute_aggregated(&payload, options).await
    }

    /// Fire `TodoCompleted` with the validation or post-write phase.
    pub async fn fire_todo_completed_event(
        &self,
        base: &HookBaseInput,
        todo_id: &str,
        todo_content: &str,
        previous_status: &str,
        all_todos: Vec<Value>,
        phase: &str,
        options: HookEventExecutionOptions<'_>,
    ) -> AggregatedHookResult {
        let payload = HookEventInputBuilder::new().todo_completed(
            base,
            todo_id,
            todo_content,
            previous_status,
            Value::Array(all_todos),
            phase,
        );
        self.execute_aggregated(&payload, options).await
    }

    async fn execute_aggregated(
        &self,
        payload: &HookEventPayload,
        options: HookEventExecutionOptions<'_>,
    ) -> AggregatedHookResult {
        self.execute_event(payload, options.messages, options.cancellation)
            .await
    }

    async fn execute_for_output(
        &self,
        payload: &HookEventPayload,
        options: HookEventExecutionOptions<'_>,
    ) -> Option<SpecificHookOutput> {
        self.execute_aggregated(payload, options).await.final_output
    }
}
