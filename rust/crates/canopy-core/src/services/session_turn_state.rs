//! Incremental restoration of session turn counters and relationships.
//!
//! This ports `packages/core/src/services/session-turn-state.ts`. It accepts
//! JSON records so transcript recovery and future UI adapters can share the
//! same prompt-id, parent-UUID, and background-notification projection.

use std::collections::HashSet;

use serde::Serialize;
use serde_json::Value;

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionTurnState {
    pub initial_turn: f64,
    pub turn_parent_uuids: Vec<Option<String>>,
    pub background_notification_task_ids: Vec<String>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct SessionTurnRecordHint {
    pub prompt_turn: Option<f64>,
    pub counts_as_user_prompt: bool,
    /// `None` means the record did not contribute a parent entry; `Some(None)`
    /// represents a user record whose parent UUID is absent or JSON null.
    pub turn_parent_uuid: Option<Option<String>>,
    pub background_notification_task_id: Option<String>,
}

/// Collect session startup state with insertion-ordered unique task IDs.
pub struct SessionTurnStateAccumulator {
    session_id: String,
    max_prompt_turn: f64,
    user_message_count: usize,
    turn_parent_uuids: Vec<Option<String>>,
    background_notification_task_ids: Vec<String>,
    seen_background_notification_task_ids: HashSet<String>,
}

impl SessionTurnStateAccumulator {
    pub fn new(session_id: impl Into<String>) -> Self {
        Self {
            session_id: session_id.into(),
            max_prompt_turn: 0.0,
            user_message_count: 0,
            turn_parent_uuids: Vec::new(),
            background_notification_task_ids: Vec::new(),
            seen_background_notification_task_ids: HashSet::new(),
        }
    }

    pub fn add(&mut self, record: &Value) {
        self.add_hint(get_session_turn_record_hint(record, &self.session_id));
    }

    pub fn add_hint(&mut self, hint: SessionTurnRecordHint) {
        if hint.counts_as_user_prompt {
            self.user_message_count = self.user_message_count.saturating_add(1);
        }
        if let Some(prompt_turn) = hint.prompt_turn {
            self.max_prompt_turn = self.max_prompt_turn.max(prompt_turn);
        }
        if let Some(parent_uuid) = hint.turn_parent_uuid {
            self.turn_parent_uuids.push(parent_uuid);
        }
        if let Some(task_id) = hint.background_notification_task_id {
            if self
                .seen_background_notification_task_ids
                .insert(task_id.clone())
            {
                self.background_notification_task_ids.push(task_id);
            }
        }
    }

    pub fn finish(self) -> SessionTurnState {
        SessionTurnState {
            initial_turn: if self.max_prompt_turn > 0.0 {
                self.max_prompt_turn
            } else {
                self.user_message_count as f64
            },
            turn_parent_uuids: self.turn_parent_uuids,
            background_notification_task_ids: self.background_notification_task_ids,
        }
    }
}

pub fn get_session_turn_record_hint(record: &Value, session_id: &str) -> SessionTurnRecordHint {
    let mut prompt_turn: Option<f64> = None;
    for prompt_id in record_prompt_ids(record) {
        if let Some(candidate) = parse_session_prompt_turn(&prompt_id, session_id) {
            prompt_turn = Some(prompt_turn.map_or(candidate, |current| current.max(candidate)));
        }
    }

    let kind = string_field(record, "type");
    let subtype = string_field(record, "subtype");
    let turn_parent_uuid = (kind == Some("user")
        && !matches!(
            subtype,
            Some(
                "goal_runtime"
                    | "notification"
                    | "cron"
                    | "mid_turn_user_message"
                    | "realtime_message"
            )
        ))
    .then(|| {
        record
            .get("parentUuid")
            .and_then(Value::as_str)
            .map(str::to_owned)
    });

    let background_notification_task_id = if subtype == Some("notification") {
        record
            .get("systemPayload")
            .and_then(|payload| payload.get("backgroundTask"))
            .and_then(|task| task.get("taskId"))
            .and_then(Value::as_str)
            .map(str::to_owned)
    } else {
        None
    };

    SessionTurnRecordHint {
        prompt_turn,
        counts_as_user_prompt: string_field(record, "sessionId") == Some(session_id)
            && is_user_prompt_record(record),
        turn_parent_uuid,
        background_notification_task_id,
    }
}

pub fn collect_session_turn_state(records: &[Value], session_id: &str) -> SessionTurnState {
    let mut accumulator = SessionTurnStateAccumulator::new(session_id);
    for record in records {
        accumulator.add(record);
    }
    accumulator.finish()
}

pub fn compute_initial_turn_from_history(records: &[Value], session_id: &str) -> f64 {
    collect_session_turn_state(records, session_id).initial_turn
}

fn record_prompt_ids(record: &Value) -> Vec<String> {
    let mut prompt_ids = Vec::with_capacity(2);
    if let Some(prompt_id) = record.get("promptId").and_then(Value::as_str) {
        prompt_ids.push(prompt_id.to_owned());
    }
    if let Some(prompt_id) = record
        .get("systemPayload")
        .and_then(|payload| payload.get("uiEvent"))
        .and_then(|event| event.get("prompt_id"))
        .and_then(Value::as_str)
    {
        prompt_ids.push(prompt_id.to_owned());
    }
    prompt_ids
}

fn parse_session_prompt_turn(prompt_id: &str, session_id: &str) -> Option<f64> {
    let prefix = format!("{session_id}########");
    let suffix = prompt_id.strip_prefix(&prefix)?;
    if suffix.is_empty() || !suffix.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    suffix.parse::<f64>().ok()
}

fn is_user_prompt_record(record: &Value) -> bool {
    if string_field(record, "type") != Some("user")
        || string_field(record, "subtype") == Some("realtime_message")
    {
        return false;
    }
    record
        .get("message")
        .and_then(|message| message.get("parts"))
        .and_then(Value::as_array)
        .is_some_and(|parts| {
            parts.iter().any(|part| {
                part.get("text")
                    .and_then(Value::as_str)
                    .is_some_and(|text| !text.trim().is_empty())
            })
        })
}

fn string_field<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value.get(key).and_then(Value::as_str)
}
