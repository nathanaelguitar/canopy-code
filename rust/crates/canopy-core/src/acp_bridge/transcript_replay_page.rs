//! Converts paged durable transcript records into the ACP session-update
//! events consumed by the bridge transcript UI.
//!
//! This is the native counterpart of the CLI's `history-replay-page.ts` and
//! `history-replayer.ts`. The reader owns record selection and signed cursor
//! validation; this module owns replay state and event projection.

use std::collections::{HashMap, HashSet};

use chrono::DateTime;
use indexmap::IndexMap;
use serde::Serialize;
use serde_json::{Map, Value, json};
use thiserror::Error;

use crate::acp_bridge::event_bus::BridgeEvent;
use crate::goals::{
    GoalCheckpointBookkeepingInput, GoalSnapshotV2, GoalStateCause,
    is_goal_checkpoint_bookkeeping_record, parse_goal_snapshot_v2, parse_goal_state_cause,
    parse_goal_state_record_payload_v2, project_goal_state_to_legacy,
};
use crate::services::session_transcript_reader::{
    SessionTranscriptDirection, SessionTranscriptReadPageOptions, SessionTranscriptReader,
    SessionTranscriptReaderError, SessionTranscriptRecordPage,
};
use crate::transcript::project_user_transcript_for_display;

pub const TRANSCRIPT_REPLAY_MAX_EVENTS: usize = 20_000;
pub const TRANSCRIPT_REPLAY_MAX_OUTPUT_BYTES: usize = 8 * 1024 * 1024;
pub const TRANSCRIPT_REPLAY_MAX_CURSOR_BYTES: usize = 64 * 1024;
const REPLAY_CONVERSION_ERROR: &str = "Replay conversion failed for this page";
const MISSING_TOOL_RESULT_MESSAGE: &str = "Tool result missing from saved history; the previous run likely ended before this tool completed.";
const HISTORY_GAP_MESSAGE: &str = "⚠️ History gap: earlier conversation was lost before this point (storage interruption) and could not be recovered.";

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BridgeSessionTranscriptPage {
    pub v: u8,
    pub session_id: String,
    pub events: Vec<BridgeEvent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    pub has_more: bool,
    pub start_time: String,
    pub last_updated: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partial: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replay_error: Option<String>,
}

#[derive(Debug, Error)]
pub enum TranscriptReplayPageError {
    #[error(transparent)]
    Reader(#[from] SessionTranscriptReaderError),
    #[error("unsupported transcript replay state version")]
    UnsupportedReplayVersion,
}

/// Read one bounded transcript page and project its records into the event
/// contract used by the TypeScript serve handler:
/// `{v:1,type:"session_update",data:update}`.
///
/// `read_page` bounds source records (at most 500 and 4 MiB by default). Replay
/// amplification is bounded independently by event count, total serialized
/// event bytes, and encoded cursor size. A replay output limit returns the
/// events emitted so far as a partial page and withholds the continuation.
pub fn read_transcript_replay_page(
    reader: &SessionTranscriptReader,
    session_id: &str,
    options: SessionTranscriptReadPageOptions<'_>,
    finalize_dangling: bool,
) -> Result<BridgeSessionTranscriptPage, TranscriptReplayPageError> {
    let page = reader.read_page(session_id, options)?;
    replay_transcript_record_page(reader, &page, finalize_dangling)
}

/// Project an already-read page. This entry point is useful to hosts that need
/// to flush recording before a backward read or choose finalization based on
/// active-prompt state.
pub fn replay_transcript_record_page(
    reader: &SessionTranscriptReader,
    page: &SessionTranscriptRecordPage,
    finalize_dangling: bool,
) -> Result<BridgeSessionTranscriptPage, TranscriptReplayPageError> {
    let mut state = ReplayState::parse(page.replay.as_ref())?;
    let backward = page.direction == SessionTranscriptDirection::Backward;
    let mut replay = Replayer::new(
        &page.gaps,
        if backward {
            Vec::new()
        } else {
            std::mem::take(&mut state.pending_tool_calls)
        },
        state.cumulative_usage,
        state.goal_state,
        state.goal_cause,
    );

    let mut conversion_failed = false;
    for record in &page.records {
        if replay.project_record(record).is_err() {
            conversion_failed = true;
            break;
        }
    }
    if !conversion_failed
        && finalize_dangling
        && (backward || !page.has_more)
        && replay.finalize().is_err()
    {
        conversion_failed = true;
    }

    replay.decorate_branch_points(page);
    if replay.trim_to_output_budget() {
        conversion_failed = true;
    }

    let updated_replay = replay.cursor_value();
    let mut next_cursor = None;
    if !conversion_failed {
        if let Some(mut cursor) = page.next_cursor_state.clone() {
            if !backward {
                cursor.replay = Some(updated_replay);
            }
            let encoded = reader.encode_cursor_state(&cursor)?;
            if encoded.len() <= TRANSCRIPT_REPLAY_MAX_CURSOR_BYTES {
                next_cursor = Some(encoded);
            } else {
                conversion_failed = true;
            }
        }
    }

    Ok(BridgeSessionTranscriptPage {
        v: 1,
        session_id: page.session_id.clone(),
        events: replay.events,
        next_cursor: if conversion_failed { None } else { next_cursor },
        has_more: !conversion_failed && page.has_more,
        start_time: page.start_time.clone(),
        last_updated: page.last_updated.clone(),
        partial: conversion_failed.then_some(true),
        replay_error: conversion_failed.then(|| REPLAY_CONVERSION_ERROR.to_owned()),
    })
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PendingCall {
    call_id: String,
    tool_name: String,
    source_record_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_timestamp: Option<String>,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CumulativeUsage {
    prompt_tokens: f64,
    cached_tokens: f64,
    candidate_tokens: f64,
    api_time_ms: f64,
}

impl Default for CumulativeUsage {
    fn default() -> Self {
        Self {
            prompt_tokens: 0.0,
            cached_tokens: 0.0,
            candidate_tokens: 0.0,
            api_time_ms: 0.0,
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ReplayCursorState<'a> {
    v: u8,
    pending_tool_calls: &'a [PendingCall],
    cumulative_usage: CumulativeUsage,
    #[serde(skip_serializing_if = "Option::is_none")]
    goal_state: Option<&'a GoalSnapshotV2>,
    #[serde(skip_serializing_if = "Option::is_none")]
    goal_cause: Option<GoalStateCause>,
}

struct ReplayState {
    pending_tool_calls: Vec<PendingCall>,
    cumulative_usage: CumulativeUsage,
    goal_state: Option<GoalSnapshotV2>,
    goal_cause: Option<GoalStateCause>,
}

impl ReplayState {
    fn parse(value: Option<&Value>) -> Result<Self, TranscriptReplayPageError> {
        let mut state = Self {
            pending_tool_calls: Vec::new(),
            cumulative_usage: CumulativeUsage::default(),
            goal_state: None,
            goal_cause: None,
        };
        let Some(value) = value.filter(|value| value.is_object()) else {
            return Ok(state);
        };
        if value
            .get("v")
            .is_some_and(|version| version.as_f64() != Some(1.0))
        {
            return Err(TranscriptReplayPageError::UnsupportedReplayVersion);
        }
        if let Some(calls) = value.get("pendingToolCalls").and_then(Value::as_array) {
            for call in calls {
                let (Some(call_id), Some(tool_name)) = (
                    call.get("callId").and_then(Value::as_str),
                    call.get("toolName").and_then(Value::as_str),
                ) else {
                    continue;
                };
                let source_record_id = call
                    .get("sourceRecordId")
                    .or_else(|| call.get("recordId"))
                    .and_then(Value::as_str);
                let Some(source_record_id) = source_record_id else {
                    continue;
                };
                let source_timestamp = call
                    .get("sourceTimestamp")
                    .or_else(|| call.get("timestamp"))
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                let pending = PendingCall {
                    call_id: call_id.to_owned(),
                    tool_name: tool_name.to_owned(),
                    source_record_id: source_record_id.to_owned(),
                    source_timestamp,
                };
                if let Some(existing) = state
                    .pending_tool_calls
                    .iter_mut()
                    .find(|existing| existing.call_id == pending.call_id)
                {
                    *existing = pending;
                } else {
                    state.pending_tool_calls.push(pending);
                }
            }
        }
        if let Some(usage) = value.get("cumulativeUsage") {
            let parsed = [
                usage.get("promptTokens").and_then(Value::as_f64),
                usage.get("cachedTokens").and_then(Value::as_f64),
                usage.get("candidateTokens").and_then(Value::as_f64),
                usage.get("apiTimeMs").and_then(Value::as_f64),
            ];
            if let [
                Some(prompt_tokens),
                Some(cached_tokens),
                Some(candidate_tokens),
                Some(api_time_ms),
            ] = parsed
            {
                state.cumulative_usage = CumulativeUsage {
                    prompt_tokens,
                    cached_tokens,
                    candidate_tokens,
                    api_time_ms,
                };
            }
        }
        state.goal_state = value.get("goalState").and_then(parse_goal_snapshot_v2);
        state.goal_cause = value.get("goalCause").and_then(parse_goal_state_cause);
        Ok(state)
    }
}

struct Replayer<'a> {
    gaps: HashMap<&'a str, &'a crate::transcript::TranscriptReplayGap>,
    events: Vec<BridgeEvent>,
    output_bytes: usize,
    pending: Vec<PendingCall>,
    used_call_ids: HashSet<String>,
    cumulative_usage: CumulativeUsage,
    goal_state: Option<GoalSnapshotV2>,
    goal_cause: Option<GoalStateCause>,
}

impl<'a> Replayer<'a> {
    fn new(
        gaps: &'a [crate::transcript::TranscriptReplayGap],
        pending: Vec<PendingCall>,
        cumulative_usage: CumulativeUsage,
        goal_state: Option<GoalSnapshotV2>,
        goal_cause: Option<GoalStateCause>,
    ) -> Self {
        let mut used_call_ids = HashSet::new();
        used_call_ids.extend(pending.iter().map(|call| call.call_id.clone()));
        Self {
            gaps: gaps
                .iter()
                .map(|gap| (gap.child_uuid.as_str(), gap))
                .collect(),
            events: Vec::new(),
            output_bytes: 2,
            pending,
            used_call_ids,
            cumulative_usage,
            goal_state,
            goal_cause,
        }
    }

    fn project_record(&mut self, record: &Value) -> Result<(), ()> {
        let record_id = record
            .get("uuid")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let timestamp = record.get("timestamp").and_then(Value::as_str);
        if self.gaps.contains_key(record_id) {
            let mut extra = Map::new();
            extra.insert("qwenDiscreteMessage".to_owned(), Value::Bool(true));
            self.emit_message(
                "assistant",
                HISTORY_GAP_MESSAGE,
                record_id,
                timestamp,
                extra,
            )?;
        }
        match record.get("type").and_then(Value::as_str) {
            Some("user") => self.project_user(record, record_id, timestamp),
            Some("assistant") => self.project_assistant(record, record_id, timestamp),
            Some("tool_result") => self.project_tool_result(record, record_id, timestamp),
            Some("system") => self.project_system(record, record_id, timestamp),
            _ => Ok(()),
        }
    }

    fn project_user(
        &mut self,
        record: &Value,
        record_id: &str,
        timestamp: Option<&str>,
    ) -> Result<(), ()> {
        let subtype = record
            .get("subtype")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let payload = record
            .get("systemPayload")
            .filter(|value| value.is_object());
        let display_text = payload
            .and_then(|payload| payload.get("displayText"))
            .and_then(Value::as_str);
        let mut extra = Map::new();
        if subtype == "realtime_message" {
            extra.insert("source".to_owned(), json!("realtime_voice"));
            extra.insert("qwenDiscreteMessage".to_owned(), Value::Bool(true));
        }
        if subtype == "mid_turn_user_message" {
            extra.insert("source".to_owned(), json!("mid_turn_message_injected"));
            extra.insert("qwenDiscreteMessage".to_owned(), Value::Bool(true));
        }
        if matches!(
            subtype,
            "goal_runtime" | "notification" | "cron" | "mid_turn_user_message"
        ) {
            if subtype == "mid_turn_user_message" && display_text == Some("") {
                let emitted =
                    self.project_media_references(payload, record_id, timestamp, &extra)?;
                if emitted {
                    return Ok(());
                }
            }
            if let Some(text) = display_text.filter(|text| !text.is_empty()) {
                let mut display_extra = Map::new();
                if subtype == "notification" {
                    display_extra.insert("source".to_owned(), json!("background_notification"));
                    display_extra.insert("qwenDiscreteMessage".to_owned(), Value::Bool(true));
                    if let Some(background_task) = payload
                        .and_then(|payload| payload.get("backgroundTask"))
                        .filter(|value| value.is_object())
                    {
                        display_extra.insert("backgroundTask".to_owned(), background_task.clone());
                    }
                } else if subtype == "cron" {
                    display_extra.insert("source".to_owned(), json!("cron"));
                } else {
                    display_extra = extra.clone();
                }
                self.emit_message("user", text, record_id, timestamp, display_extra)?;
                self.project_media_references(payload, record_id, timestamp, &extra)?;
                return Ok(());
            }
            if subtype != "mid_turn_user_message" {
                return Ok(());
            }
        }

        let projection =
            project_user_transcript_for_display(record.get("message"), record.get("systemPayload"));
        let parts = if let Some(display_text) = projection.display_text {
            replace_display_text_parts(record, display_text)
        } else {
            projection.parts
        };
        self.project_parts(&parts, false, record_id, timestamp, &extra)?;
        self.project_media_references(payload, record_id, timestamp, &extra)?;
        Ok(())
    }

    fn project_media_references(
        &mut self,
        payload: Option<&Value>,
        record_id: &str,
        timestamp: Option<&str>,
        extra: &Map<String, Value>,
    ) -> Result<bool, ()> {
        let Some(references) = payload
            .and_then(|payload| payload.get("mediaReferences"))
            .and_then(Value::as_array)
        else {
            return Ok(false);
        };
        let mut emitted = false;
        for reference in references {
            let kind = reference.get("type").and_then(Value::as_str);
            if !matches!(kind, Some("image" | "audio"))
                || reference.get("mediaId").and_then(Value::as_str).is_none()
                || reference.get("mimeType").and_then(Value::as_str).is_none()
                || reference.get("size").and_then(Value::as_f64).is_none()
            {
                continue;
            }
            let update = json!({
                "sessionUpdate": "user_message_chunk",
                "content": {
                    "type": kind,
                    "mediaId": reference["mediaId"],
                    "mimeType": reference["mimeType"],
                    "size": reference["size"],
                },
                "_meta": self.meta(record_id, timestamp, extra.clone()),
            });
            self.emit(update)?;
            emitted = true;
        }
        Ok(emitted)
    }

    fn project_assistant(
        &mut self,
        record: &Value,
        record_id: &str,
        timestamp: Option<&str>,
    ) -> Result<(), ()> {
        let usage = record
            .get("usageMetadata")
            .filter(|value| value.is_object());
        let mut emitted_usage = false;
        let empty_extra = Map::new();
        let parts = record
            .get("message")
            .and_then(|message| message.get("parts"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        for (part_index, part) in parts.iter().enumerate() {
            self.project_part(
                part,
                true,
                record_id,
                timestamp,
                &empty_extra,
                part_index,
                usage,
                &mut emitted_usage,
            )?;
        }
        if let Some(usage) = usage {
            if !emitted_usage {
                self.add_usage(usage);
                self.emit(self.usage_update(usage, record_id, timestamp, &empty_extra))?;
            }
        }
        Ok(())
    }

    fn project_parts(
        &mut self,
        parts: &[Value],
        assistant: bool,
        record_id: &str,
        timestamp: Option<&str>,
        extra: &Map<String, Value>,
    ) -> Result<(), ()> {
        let mut usage_emitted = false;
        for (part_index, part) in parts.iter().enumerate() {
            self.project_part(
                part,
                assistant,
                record_id,
                timestamp,
                extra,
                part_index,
                None,
                &mut usage_emitted,
            )?;
        }
        Ok(())
    }

    fn project_part(
        &mut self,
        part: &Value,
        assistant: bool,
        record_id: &str,
        timestamp: Option<&str>,
        extra: &Map<String, Value>,
        part_index: usize,
        usage_before_tool_call: Option<&Value>,
        usage_emitted: &mut bool,
    ) -> Result<(), ()> {
        if let Some(text) = part
            .get("text")
            .and_then(Value::as_str)
            .filter(|text| !text.is_empty())
        {
            self.emit_message(
                if assistant && part.get("thought").and_then(Value::as_bool) == Some(true) {
                    "thought"
                } else if assistant {
                    "assistant"
                } else {
                    "user"
                },
                text,
                record_id,
                timestamp,
                extra.clone(),
            )?;
        }
        if let Some(inline) = part.get("inlineData").filter(|value| value.is_object()) {
            if !assistant
                && inline.get("data").and_then(Value::as_str).is_some()
                && inline
                    .get("mimeType")
                    .and_then(Value::as_str)
                    .is_some_and(|mime| mime.starts_with("image/"))
            {
                self.emit(json!({
                    "sessionUpdate": "user_message_chunk",
                    "content": {
                        "type": "image",
                        "data": inline["data"],
                        "mimeType": inline["mimeType"],
                    },
                    "_meta": self.meta(record_id, timestamp, extra.clone()),
                }))?;
            }
        }
        if let Some(function_call) = part.get("functionCall").filter(|value| value.is_object()) {
            if let Some(usage) = usage_before_tool_call.filter(|_| !*usage_emitted) {
                *usage_emitted = true;
                self.add_usage(usage);
                let update = self.usage_update(usage, record_id, timestamp, extra);
                self.emit(update)?;
            }
            let tool_name = function_call
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if tool_name.is_empty() || tool_name == "todo_write" {
                return Ok(());
            }
            let args = function_call
                .get("args")
                .filter(|value| value.is_object())
                .cloned()
                .unwrap_or_else(|| json!({}));
            let explicit_id = function_call
                .get("id")
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty());
            let call_id = self.allocate_call_id(
                explicit_id
                    .map(str::to_owned)
                    .unwrap_or_else(|| format!("qwen-replay-tool:{record_id}:{part_index}")),
                false,
            );
            let title = args
                .get("description")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|description| !description.is_empty())
                .map_or_else(
                    || tool_name.to_owned(),
                    |description| format!("{tool_name}: {description}"),
                );
            let (provenance, server_id) = tool_provenance(tool_name);
            let mut meta_extra = extra.clone();
            meta_extra.insert("toolName".to_owned(), json!(tool_name));
            meta_extra.insert("provenance".to_owned(), json!(provenance));
            if let Some(server_id) = server_id {
                meta_extra.insert("serverId".to_owned(), json!(server_id));
            }
            self.emit(json!({
                "sessionUpdate": "tool_call",
                "toolCallId": call_id,
                "status": "in_progress",
                "title": title,
                "content": [],
                "locations": [],
                "kind": "other",
                "rawInput": args,
                "_meta": self.meta(record_id, timestamp, meta_extra),
            }))?;
            if assistant {
                self.pending.push(PendingCall {
                    call_id,
                    tool_name: tool_name.to_owned(),
                    source_record_id: record_id.to_owned(),
                    source_timestamp: timestamp.map(str::to_owned),
                });
            }
        }
        Ok(())
    }

    fn project_tool_result(
        &mut self,
        record: &Value,
        record_id: &str,
        timestamp: Option<&str>,
    ) -> Result<(), ()> {
        let result = record
            .get("toolCallResult")
            .filter(|value| value.is_object());
        let parts = record
            .get("message")
            .and_then(|message| message.get("parts"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let tool_name = parts
            .iter()
            .filter_map(|part| part.get("functionResponse"))
            .find_map(|response| response.get("name").and_then(Value::as_str))
            .unwrap_or_default();
        let explicit_call_id = result
            .and_then(|result| result.get("callId"))
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .or_else(|| {
                parts.iter().find_map(|part| {
                    part.get("functionResponse")
                        .and_then(|response| response.get("id"))
                        .and_then(Value::as_str)
                        .filter(|id| !id.is_empty())
                })
            });
        let call_id = if let Some(explicit) = explicit_call_id {
            self.allocate_call_id(explicit.to_owned(), true)
        } else {
            let candidates = self
                .pending
                .iter()
                .filter(|pending| pending.tool_name == tool_name)
                .map(|pending| pending.call_id.clone())
                .collect::<Vec<_>>();
            if candidates.len() == 1 {
                candidates[0].clone()
            } else {
                self.allocate_call_id(format!("qwen-replay-tool:{record_id}:result"), false)
            }
        };
        self.pending.retain(|pending| pending.call_id != call_id);

        let result_display = result.and_then(|result| result.get("resultDisplay"));
        if tool_name == "todo_write" {
            if let Some((plan_id, todos)) = extract_todo_plan(result_display) {
                let mut extra = Map::new();
                extra.insert(
                    "stats".to_owned(),
                    serde_json::to_value(self.cumulative_usage).unwrap_or_default(),
                );
                if let Some(plan_id) = plan_id {
                    extra.insert("qwenTodoPlan".to_owned(), json!({"id": plan_id}));
                }
                let mut meta = self.meta(record_id, timestamp, extra);
                if let Some(transcript) = meta
                    .get_mut("qwenTranscript")
                    .and_then(Value::as_object_mut)
                {
                    transcript.insert("planToolCallId".to_owned(), json!(call_id));
                }
                let entries = todos
                    .iter()
                    .map(|todo| {
                        let mut entry = json!({
                            "content": todo["content"],
                            "priority": "medium",
                            "status": todo["status"],
                        });
                        let mut qwen_todo = Map::new();
                        if let Some(id) = todo.get("id") {
                            qwen_todo.insert("id".to_owned(), id.clone());
                        }
                        if let Some(blocked_by) = todo.get("blockedBy") {
                            qwen_todo.insert("blockedBy".to_owned(), blocked_by.clone());
                        }
                        if !qwen_todo.is_empty() {
                            entry["_meta"] = json!({"qwenTodo": qwen_todo});
                        }
                        entry
                    })
                    .collect::<Vec<_>>();
                self.emit(json!({"sessionUpdate":"plan", "entries":entries, "_meta":meta}))?;
            }
            return Ok(());
        }

        let success = match result.and_then(|result| result.get("status")) {
            None => !result
                .and_then(|result| result.get("error"))
                .is_some_and(js_truthy),
            Some(status) => {
                status.as_str() == Some("success")
                    && !result
                        .and_then(|result| result.get("error"))
                        .is_some_and(js_truthy)
            }
        };
        let error_message = result
            .and_then(|result| result.get("error"))
            .and_then(|error| error.get("message"))
            .and_then(Value::as_str)
            .filter(|message| !message.is_empty());
        let mut content = result_content_prefix(result_display);
        if let Some(diff) = result_display.and_then(diff_content) {
            content.push(diff);
        } else if let Some(error_message) = error_message {
            content
                .push(json!({"type":"content", "content":{"type":"text", "text":error_message}}));
        } else {
            for part in &parts {
                if let Some(text) = part
                    .get("text")
                    .and_then(Value::as_str)
                    .filter(|text| !text.is_empty())
                {
                    content.push(json!({"type":"content", "content":{"type":"text", "text":text}}));
                }
                let Some(response) = part.get("functionResponse") else {
                    continue;
                };
                let Some(payload) = response.get("response").filter(|value| value.is_object())
                else {
                    continue;
                };
                let text = payload
                    .get("output")
                    .and_then(Value::as_str)
                    .or_else(|| payload.get("error").and_then(Value::as_str))
                    .map(str::to_owned)
                    .or_else(|| serde_json::to_string(payload).ok());
                if let Some(text) = text {
                    content.push(json!({"type":"content", "content":{"type":"text", "text":text}}));
                }
            }
        }
        let mut extra = Map::new();
        extra.insert("toolName".to_owned(), json!(tool_name));
        let (provenance, server_id) = tool_provenance(tool_name);
        extra.insert("provenance".to_owned(), json!(provenance));
        if let Some(server_id) = server_id {
            extra.insert("serverId".to_owned(), json!(server_id));
        }
        if let Some(artifacts) = result
            .and_then(|result| result.get("artifacts"))
            .and_then(Value::as_array)
            .filter(|artifacts| !artifacts.is_empty())
        {
            extra.insert("artifacts".to_owned(), Value::Array(artifacts.clone()));
        }
        let mut update = json!({
            "sessionUpdate":"tool_call_update",
            "toolCallId":call_id,
            "status":if success {"completed"} else {"failed"},
            "content":content,
            "_meta":self.meta(record_id, timestamp, extra),
        });
        let truncated_diff = result_display.is_some_and(is_truncated_diff);
        if let Some(result_display) = result_display.filter(|_| !truncated_diff) {
            update["rawOutput"] = result_display.clone();
        }
        self.emit(update)?;

        if result_display
            .and_then(|display| display.get("type"))
            .and_then(Value::as_str)
            == Some("task_execution")
        {
            if let Some(summary) = result_display
                .and_then(|display| display.get("executionSummary"))
                .filter(|value| value.is_object())
            {
                let usage = task_usage(summary);
                if !usage.is_empty() {
                    let usage_value = Value::Object(usage);
                    self.add_usage(&usage_value);
                    self.emit(self.usage_update(&usage_value, record_id, timestamp, &Map::new()))?;
                }
            }
        }
        Ok(())
    }

    fn project_system(
        &mut self,
        record: &Value,
        record_id: &str,
        timestamp: Option<&str>,
    ) -> Result<(), ()> {
        let subtype = record
            .get("subtype")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let payload = record.get("systemPayload");
        if subtype == "goal_state" {
            let Some(payload) = payload.and_then(parse_goal_state_record_payload_v2) else {
                return Ok(());
            };
            let previous_state = self.goal_state.as_ref();
            let bookkeeping =
                is_goal_checkpoint_bookkeeping_record(GoalCheckpointBookkeepingInput {
                    cause: payload.cause,
                    previous_cause: self.goal_cause,
                    previous: previous_state,
                    next: &payload.snapshot,
                });
            let projection = project_goal_state_to_legacy(
                &payload,
                previous_state.and_then(|snapshot| snapshot.goal.as_ref()),
            );
            self.goal_state = Some(payload.snapshot.clone());
            self.goal_cause = Some(payload.cause);
            if bookkeeping {
                return Ok(());
            }
            let mut goal_status = serde_json::to_value(projection.goal_status).unwrap_or_default();
            if let Some(status) = goal_status.as_object_mut() {
                status.remove("type");
            }
            let mut extra = Map::new();
            extra.insert(
                "goalState".to_owned(),
                serde_json::to_value(payload.snapshot).unwrap_or_default(),
            );
            extra.insert("goalStatus".to_owned(), goal_status);
            if let Some(terminal) = projection.goal_terminal {
                extra.insert(
                    "goalTerminal".to_owned(),
                    serde_json::to_value(terminal).unwrap_or_default(),
                );
            }
            extra.insert("qwen.session.recordId".to_owned(), json!(record_id));
            self.emit_message("assistant", "", record_id, timestamp, extra)?;
            return Ok(());
        }
        if subtype != "slash_command"
            || payload
                .and_then(|payload| payload.get("phase"))
                .and_then(Value::as_str)
                != Some("result")
        {
            return Ok(());
        }
        let Some(items) = payload
            .and_then(|payload| payload.get("outputHistoryItems"))
            .and_then(Value::as_array)
        else {
            return Ok(());
        };
        for item in items {
            if let Some(goal_status) = parse_legacy_goal_status(item) {
                if goal_status
                    .get("condition")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .is_empty()
                    || goal_status.get("kind").and_then(Value::as_str) == Some("checking")
                {
                    continue;
                }
                let mut extra = Map::new();
                extra.insert("goalStatus".to_owned(), goal_status);
                self.emit_message("assistant", "", record_id, timestamp, extra)?;
                continue;
            }
            if let Some(text) = item.get("text").and_then(Value::as_str) {
                let mut extra = Map::new();
                extra.insert("source".to_owned(), json!("slash_command"));
                self.emit_message(
                    "assistant",
                    &text.replace('\n', "  \n"),
                    record_id,
                    timestamp,
                    extra,
                )?;
            }
        }
        Ok(())
    }

    fn finalize(&mut self) -> Result<(), ()> {
        let pending = std::mem::take(&mut self.pending);
        for call in pending {
            let mut extra = Map::new();
            extra.insert("toolName".to_owned(), json!(call.tool_name));
            let (provenance, server_id) = tool_provenance(&call.tool_name);
            extra.insert("provenance".to_owned(), json!(provenance));
            if let Some(server_id) = server_id {
                extra.insert("serverId".to_owned(), json!(server_id));
            }
            let update = json!({
                "sessionUpdate":"tool_call_update",
                "toolCallId":call.call_id,
                "status":"failed",
                "content":[{"type":"content", "content":{"type":"text", "text":MISSING_TOOL_RESULT_MESSAGE}}],
                "_meta":self.meta(&call.source_record_id, call.source_timestamp.as_deref(), extra),
            });
            self.emit(update)?;
        }
        Ok(())
    }

    fn emit_message(
        &mut self,
        role: &str,
        text: &str,
        record_id: &str,
        timestamp: Option<&str>,
        extra: Map<String, Value>,
    ) -> Result<(), ()> {
        let session_update = match role {
            "user" => "user_message_chunk",
            "thought" => "agent_thought_chunk",
            _ => "agent_message_chunk",
        };
        let mut update = json!({
            "sessionUpdate":session_update,
            "content":{"type":"text", "text":text},
            "_meta":self.meta(record_id, timestamp, extra),
        });
        if let Some(epoch) = timestamp_epoch_ms(timestamp) {
            update["timestamp"] = json!(epoch);
        }
        self.emit(update)
    }

    fn meta(
        &self,
        record_id: &str,
        timestamp: Option<&str>,
        mut extra: Map<String, Value>,
    ) -> Value {
        if let Some(epoch) = timestamp_epoch_ms(timestamp) {
            extra.insert("timestamp".to_owned(), json!(epoch));
        }
        extra.insert(
            "qwenTranscript".to_owned(),
            json!({"sourceRecordIds":[record_id]}),
        );
        extra.insert(
            "canopyTranscript".to_owned(),
            json!({"sourceRecordIds":[record_id]}),
        );
        extra.insert("canopy.session.recordId".to_owned(), json!(record_id));
        Value::Object(extra)
    }

    fn usage_update(
        &self,
        metadata: &Value,
        record_id: &str,
        timestamp: Option<&str>,
        extra: &Map<String, Value>,
    ) -> Value {
        let mut usage = Map::new();
        usage.insert(
            "inputTokens".to_owned(),
            json!(finite(metadata.get("promptTokenCount"))),
        );
        usage.insert(
            "outputTokens".to_owned(),
            json!(finite(metadata.get("candidatesTokenCount"))),
        );
        usage.insert(
            "totalTokens".to_owned(),
            json!(finite(metadata.get("totalTokenCount"))),
        );
        if let Some(value) = metadata.get("thoughtsTokenCount").and_then(Value::as_f64) {
            usage.insert("thoughtTokens".to_owned(), json!(value));
        }
        if let Some(value) = metadata
            .get("cachedContentTokenCount")
            .and_then(Value::as_f64)
        {
            usage.insert("cachedReadTokens".to_owned(), json!(value));
        }
        let mut meta_extra = extra.clone();
        meta_extra.insert("usage".to_owned(), Value::Object(usage));
        self.message_value("assistant", "", record_id, timestamp, meta_extra)
    }

    fn message_value(
        &self,
        role: &str,
        text: &str,
        record_id: &str,
        timestamp: Option<&str>,
        extra: Map<String, Value>,
    ) -> Value {
        let mut update = json!({
            "sessionUpdate":if role == "user" {"user_message_chunk"} else {"agent_message_chunk"},
            "content":{"type":"text", "text":text},
            "_meta":self.meta(record_id, timestamp, extra),
        });
        if let Some(epoch) = timestamp_epoch_ms(timestamp) {
            update["timestamp"] = json!(epoch);
        }
        update
    }

    fn add_usage(&mut self, metadata: &Value) {
        self.cumulative_usage.prompt_tokens += finite(metadata.get("promptTokenCount"));
        self.cumulative_usage.candidate_tokens += finite(metadata.get("candidatesTokenCount"));
        self.cumulative_usage.cached_tokens += finite(metadata.get("cachedContentTokenCount"));
    }

    fn allocate_call_id(&mut self, candidate: String, reuse: bool) -> String {
        if reuse
            && self
                .pending
                .iter()
                .any(|pending| pending.call_id == candidate)
        {
            self.used_call_ids.insert(candidate.clone());
            return candidate;
        }
        if self.used_call_ids.insert(candidate.clone()) {
            return candidate;
        }
        let mut occurrence = 2usize;
        loop {
            let value = format!("{candidate}:{occurrence}");
            if self.used_call_ids.insert(value.clone()) {
                return value;
            }
            occurrence += 1;
        }
    }

    fn emit(&mut self, update: Value) -> Result<(), ()> {
        if self.events.len() >= TRANSCRIPT_REPLAY_MAX_EVENTS {
            return Err(());
        }
        let event = BridgeEvent::new("session_update", update);
        let bytes = serde_json::to_vec(&event).map_err(|_| ())?.len();
        let next_bytes = self
            .output_bytes
            .saturating_add(bytes)
            .saturating_add(usize::from(!self.events.is_empty()));
        if next_bytes > TRANSCRIPT_REPLAY_MAX_OUTPUT_BYTES {
            return Err(());
        }
        self.output_bytes = next_bytes;
        self.events.push(event);
        Ok(())
    }

    fn decorate_branch_points(&mut self, page: &SessionTranscriptRecordPage) {
        let Some(branch_points) = page.branch_points_by_assistant_uuid.as_ref() else {
            return;
        };
        let mut last_chunk_by_record = IndexMap::<String, usize>::new();
        for (index, event) in self.events.iter().enumerate() {
            if event.data.get("sessionUpdate").and_then(Value::as_str)
                != Some("agent_message_chunk")
                || event
                    .data
                    .get("content")
                    .and_then(|content| content.get("text"))
                    .and_then(Value::as_str)
                    .is_none_or(str::is_empty)
            {
                continue;
            }
            let Some(ids) = event
                .data
                .get("_meta")
                .and_then(|meta| meta.get("qwenTranscript"))
                .and_then(|meta| meta.get("sourceRecordIds"))
                .and_then(Value::as_array)
            else {
                continue;
            };
            for record_id in ids.iter().filter_map(Value::as_str) {
                if branch_points.contains_key(record_id) {
                    last_chunk_by_record.insert(record_id.to_owned(), index);
                }
            }
        }
        let mut decorated_indexes = HashSet::new();
        for (record_id, index) in last_chunk_by_record {
            if !decorated_indexes.insert(index) {
                continue;
            }
            let Some(checkpoint_id) = branch_points.get(&record_id) else {
                continue;
            };
            let Some(meta) = self.events[index]
                .data
                .get_mut("_meta")
                .and_then(Value::as_object_mut)
            else {
                continue;
            };
            let transcript = meta.entry("canopyTranscript").or_insert_with(|| json!({}));
            if let Some(transcript) = transcript.as_object_mut() {
                transcript.insert("branchRecordId".to_owned(), json!(checkpoint_id));
            }
        }
    }

    fn trim_to_output_budget(&mut self) -> bool {
        let mut total = 2usize;
        for event in &self.events {
            let Ok(bytes) = serde_json::to_vec(event) else {
                return true;
            };
            total = total.saturating_add(bytes.len()).saturating_add(1);
            if total > TRANSCRIPT_REPLAY_MAX_OUTPUT_BYTES {
                while total > TRANSCRIPT_REPLAY_MAX_OUTPUT_BYTES {
                    let Some(removed) = self.events.pop() else {
                        break;
                    };
                    let bytes = serde_json::to_vec(&removed).map_or(0, |bytes| bytes.len());
                    total = total.saturating_sub(bytes).saturating_sub(1);
                }
                self.output_bytes = total;
                return true;
            }
        }
        self.output_bytes = total;
        false
    }

    fn cursor_value(&self) -> Value {
        serde_json::to_value(ReplayCursorState {
            v: 1,
            pending_tool_calls: &self.pending,
            cumulative_usage: self.cumulative_usage,
            goal_state: self.goal_state.as_ref(),
            goal_cause: self.goal_cause,
        })
        .unwrap_or_else(|_| json!({"v":1,"pendingToolCalls":[],"cumulativeUsage":{"promptTokens":0,"cachedTokens":0,"candidateTokens":0,"apiTimeMs":0}}))
    }
}

fn replace_display_text_parts(record: &Value, display_text: String) -> Vec<Value> {
    let Some(parts) = record
        .get("message")
        .and_then(|message| message.get("parts"))
        .and_then(Value::as_array)
    else {
        return if display_text.is_empty() {
            Vec::new()
        } else {
            vec![json!({"text":display_text})]
        };
    };
    let mut replaced = false;
    let mut output = Vec::new();
    for part in parts {
        if part.get("text").and_then(Value::as_str).is_some() {
            if !replaced && !display_text.is_empty() {
                output.push(json!({"text":display_text}));
            }
            replaced = true;
        } else {
            output.push(part.clone());
        }
    }
    if !replaced && !display_text.is_empty() {
        output.push(json!({"text":display_text}));
    }
    output
}

fn timestamp_epoch_ms(timestamp: Option<&str>) -> Option<i64> {
    let timestamp = timestamp?;
    DateTime::parse_from_rfc3339(timestamp)
        .ok()
        .map(|date| date.timestamp_millis())
}

fn finite(value: Option<&Value>) -> f64 {
    value
        .and_then(Value::as_f64)
        .filter(|value| value.is_finite())
        .unwrap_or(0.0)
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

fn tool_provenance(tool_name: &str) -> (&'static str, Option<&str>) {
    if let Some(rest) = tool_name.strip_prefix("mcp__") {
        if let Some((server_id, _)) = rest.split_once("__") {
            if !server_id.is_empty() {
                return ("mcp", Some(server_id));
            }
        }
    }
    ("builtin", None)
}

fn result_content_prefix(display: Option<&Value>) -> Vec<Value> {
    let Some(display) = display.filter(|display| {
        display.get("type").and_then(Value::as_str) == Some("vision_bridge_notice")
    }) else {
        return Vec::new();
    };
    let (Some(summary), Some(notice)) = (
        display.get("summary").and_then(Value::as_str),
        display.get("notice").and_then(Value::as_str),
    ) else {
        return Vec::new();
    };
    vec![
        json!({"type":"content", "content":{"type":"text", "text":format!("{summary}\n{notice}")}}),
    ]
}

fn is_truncated_diff(display: &Value) -> bool {
    display.get("truncatedForSession").and_then(Value::as_bool) == Some(true)
        && display.get("fileName").is_some()
        && display.get("newContent").is_some()
}

fn diff_content(display: &Value) -> Option<Value> {
    if display.get("fileName").is_none() || display.get("newContent").is_none() {
        return None;
    }
    if is_truncated_diff(display) {
        let file_name = display
            .get("fileName")
            .and_then(Value::as_str)
            .unwrap_or("the edited file");
        let text = if display.get("fileDiffTruncated").and_then(Value::as_bool) == Some(true) {
            let length = display
                .get("fileDiffLength")
                .and_then(Value::as_f64)
                .map(|length| format!(" Original fileDiff length: {length} chars."))
                .unwrap_or_default();
            format!("Full diff omitted from saved session history for {file_name}.{length}")
        } else {
            format!(
                "Saved session preview only for {file_name}; full original and new file contents are unavailable."
            )
        };
        return Some(json!({"type":"content", "content":{"type":"text", "text":text}}));
    }
    Some(json!({
        "type":"diff",
        "path":display.get("fileName").and_then(Value::as_str).unwrap_or_default(),
        "oldText":display.get("originalContent").and_then(Value::as_str).unwrap_or_default(),
        "newText":display.get("newContent").and_then(Value::as_str).unwrap_or_default(),
    }))
}

fn extract_todo_plan(display: Option<&Value>) -> Option<(Option<String>, Vec<Value>)> {
    let display = display?;
    let parsed;
    let value = if let Some(text) = display.as_str() {
        parsed = serde_json::from_str::<Value>(text).ok()?;
        &parsed
    } else {
        display
    };
    if value.get("type").and_then(Value::as_str) != Some("todo_list") {
        return None;
    }
    let todos = value
        .get("todos")?
        .as_array()?
        .iter()
        .filter_map(|todo| {
            let content = todo.get("content")?.as_str()?;
            let status = todo.get("status")?.as_str()?;
            if !matches!(status, "pending" | "in_progress" | "completed") {
                return None;
            }
            let mut normalized = json!({"content":content,"status":status});
            if let Some(id) = todo.get("id").and_then(Value::as_str) {
                normalized["id"] = json!(id);
            }
            if let Some(blocked_by) = todo
                .get("blockedBy")
                .and_then(Value::as_array)
                .filter(|values| values.iter().all(Value::is_string))
            {
                normalized["blockedBy"] = Value::Array(blocked_by.clone());
            }
            Some(normalized)
        })
        .collect();
    Some((
        value
            .get("planId")
            .and_then(Value::as_str)
            .map(str::to_owned),
        todos,
    ))
}

fn task_usage(summary: &Value) -> Map<String, Value> {
    let mut usage = Map::new();
    for (source, target) in [
        ("inputTokens", "promptTokenCount"),
        ("outputTokens", "candidatesTokenCount"),
        ("thoughtTokens", "thoughtsTokenCount"),
        ("cachedTokens", "cachedContentTokenCount"),
        ("totalTokens", "totalTokenCount"),
    ] {
        if let Some(value) = summary
            .get(source)
            .and_then(Value::as_f64)
            .filter(|value| value.is_finite())
        {
            usage.insert(target.to_owned(), json!(value));
        }
    }
    usage
}

fn parse_legacy_goal_status(value: &Value) -> Option<Value> {
    if value.get("type").and_then(Value::as_str) != Some("goal_status") {
        return None;
    }
    let kind = value.get("kind").and_then(Value::as_str)?;
    if !matches!(
        kind,
        "set" | "achieved" | "cleared" | "failed" | "aborted" | "paused" | "checking"
    ) {
        return None;
    }
    let condition = value.get("condition").and_then(Value::as_str)?;
    let mut status = json!({"kind":kind,"condition":condition});
    for key in ["iterations", "setAt", "durationMs"] {
        if let Some(number) = value
            .get(key)
            .and_then(Value::as_f64)
            .filter(|number| number.is_finite())
        {
            status[key] = json!(number);
        }
    }
    if let Some(reason) = value.get("lastReason").and_then(Value::as_str) {
        status["lastReason"] = json!(reason);
    }
    Some(status)
}
