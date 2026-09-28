use std::collections::{BTreeMap, HashMap, HashSet};

use chrono::{DateTime, NaiveDate};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;

pub const USER_PROMPT_SUBMIT_CONTEXT_OPEN: &str = "<canopy:user-prompt-submit-context>";
pub const USER_PROMPT_SUBMIT_CONTEXT_CLOSE: &str = "</canopy:user-prompt-submit-context>";

const ARTIFACT_RECORD_SUBTYPES: &[&str] = &["session_artifact_event", "session_artifact_snapshot"];

const KNOWN_RECORD_SUBTYPES: &[&str] = &[
    "chat_compression",
    "slash_command",
    "ui_telemetry",
    "at_command",
    "attribution_snapshot",
    "notification",
    "cron",
    "mid_turn_user_message",
    "realtime_message",
    "custom_title",
    "parent_session",
    "rewind",
    "agent_bootstrap",
    "agent_launch_prompt",
    "agent_retry",
    "file_history_snapshot",
    "session_source",
    "branch_checkpoint",
    "goal_state",
    "goal_runtime",
    "session_artifact_event",
    "session_artifact_snapshot",
    "tool_execution_intent",
];

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TranscriptRecordType {
    User,
    Assistant,
    ToolResult,
    System,
}

impl TranscriptRecordType {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "user" => Some(Self::User),
            "assistant" => Some(Self::Assistant),
            "tool_result" => Some(Self::ToolResult),
            "system" => Some(Self::System),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TranscriptMessage {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parts: Option<Vec<Value>>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TranscriptRecord {
    pub uuid: String,
    pub parent_uuid: Option<String>,
    pub session_id: String,
    #[serde(rename = "type")]
    pub record_type: TranscriptRecordType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subtype: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<TranscriptMessage>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DiagnosticSeverity {
    Info,
    Warning,
    Error,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TranscriptProjectionDiagnostic {
    pub code: String,
    pub severity: DiagnosticSeverity,
    pub message: String,
    pub affects_completeness: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub record_index: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub record_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TranscriptReplayGap {
    pub child_uuid: String,
    pub missing_parent_uuid: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PreparedTranscriptRecords {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub records: Vec<TranscriptRecord>,
    pub gaps: Vec<TranscriptReplayGap>,
    pub diagnostics: Vec<TranscriptProjectionDiagnostic>,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("{message}")]
pub struct TranscriptRecordPreparationError {
    pub code: TranscriptRecordPreparationErrorCode,
    pub message: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TranscriptRecordPreparationErrorCode {
    InvalidRecords,
    LeafNotFound,
    MixedSessionIds,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct UserTranscriptDisplayProjection {
    pub display_text: Option<String>,
    pub parts: Vec<Value>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TranscriptUuidChain {
    pub uuids: Vec<String>,
    pub gaps: Vec<TranscriptReplayGap>,
    pub cycle_uuid: Option<String>,
}

fn diagnostic(
    code: &str,
    message: &str,
    affects_completeness: bool,
    record_index: Option<usize>,
    record_id: Option<String>,
    path: Option<&str>,
) -> TranscriptProjectionDiagnostic {
    TranscriptProjectionDiagnostic {
        code: code.to_owned(),
        severity: if affects_completeness {
            DiagnosticSeverity::Warning
        } else {
            DiagnosticSeverity::Info
        },
        message: message.to_owned(),
        affects_completeness,
        record_index,
        record_id,
        path: path.map(str::to_owned),
    }
}

pub fn wrap_user_prompt_submit_context(context: &str) -> String {
    format!("{USER_PROMPT_SUBMIT_CONTEXT_OPEN}\n{context}\n{USER_PROMPT_SUBMIT_CONTEXT_CLOSE}")
}

pub fn is_user_prompt_submit_context_part_text(text: &str) -> bool {
    let trimmed = text.trim();
    let prefix = format!("{USER_PROMPT_SUBMIT_CONTEXT_OPEN}\n");
    let suffix = format!("\n{USER_PROMPT_SUBMIT_CONTEXT_CLOSE}");
    if !trimmed.starts_with(&prefix) || !trimmed.ends_with(&suffix) {
        return false;
    }
    let body = &trimmed[prefix.len()..trimmed.len() - suffix.len()];
    !body.contains(USER_PROMPT_SUBMIT_CONTEXT_OPEN)
        && !body.contains(USER_PROMPT_SUBMIT_CONTEXT_CLOSE)
}

/// Drop a trailing model-injected UserPromptSubmit context part while keeping
/// the user's sole matching part intact. Returns the original slice when no
/// part is removed, matching `stripTrailingUserPromptSubmitContextPart`.
pub fn strip_trailing_user_prompt_submit_context_part(parts: &[Value]) -> &[Value] {
    if parts.len() <= 1 {
        return parts;
    }
    let Some(text) = parts
        .last()
        .and_then(|part| part.get("text"))
        .and_then(Value::as_str)
    else {
        return parts;
    };
    if is_user_prompt_submit_context_part_text(text) {
        &parts[..parts.len() - 1]
    } else {
        parts
    }
}

fn is_user_prompt_submit_context_part(part: &Value) -> bool {
    part.get("text")
        .and_then(Value::as_str)
        .is_some_and(is_user_prompt_submit_context_part_text)
}

pub fn project_user_transcript_for_display(
    message: Option<&Value>,
    system_payload: Option<&Value>,
) -> UserTranscriptDisplayProjection {
    let parts = message
        .and_then(|message| message.get("parts"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let has_final_hook_context_part =
        parts.len() > 1 && parts.last().is_some_and(is_user_prompt_submit_context_part);
    let payload = system_payload.filter(|value| value.is_object());
    let is_user_prompt_payload = payload
        .and_then(|value| value.get("hookContext"))
        .and_then(Value::as_str)
        .is_some()
        || has_final_hook_context_part;
    let display_text = payload
        .filter(|_| is_user_prompt_payload)
        .and_then(|value| value.get("displayText"))
        .and_then(Value::as_str)
        .map(str::to_owned);

    if display_text.is_some() {
        return UserTranscriptDisplayProjection {
            display_text,
            parts: parts
                .into_iter()
                .filter(|part| {
                    !(part.is_object() && part.get("text").and_then(Value::as_str).is_some())
                })
                .collect(),
        };
    }

    if payload.is_none() && has_final_hook_context_part {
        return UserTranscriptDisplayProjection {
            display_text: None,
            parts: parts[..parts.len() - 1].to_vec(),
        };
    }

    UserTranscriptDisplayProjection {
        display_text: None,
        parts,
    }
}

pub fn is_transcript_artifact_record(record: &TranscriptRecord) -> bool {
    record.record_type == TranscriptRecordType::System
        && record
            .subtype
            .as_deref()
            .is_some_and(|subtype| ARTIFACT_RECORD_SUBTYPES.contains(&subtype))
}

pub fn is_transcript_conversation_record(record: &TranscriptRecord) -> bool {
    !is_transcript_artifact_record(record)
}

fn is_valid_timestamp(value: &str) -> bool {
    DateTime::parse_from_rfc3339(value).is_ok()
        || NaiveDate::parse_from_str(value, "%Y-%m-%d").is_ok()
}

fn string_field<'a>(object: &'a Map<String, Value>, key: &str) -> Option<&'a str> {
    object.get(key).and_then(Value::as_str)
}

pub fn validate_transcript_record(
    value: &Value,
    record_index: Option<usize>,
) -> (
    Option<TranscriptRecord>,
    Vec<TranscriptProjectionDiagnostic>,
) {
    let mut diagnostics = Vec::new();
    let Some(object) = value.as_object() else {
        diagnostics.push(diagnostic(
            "invalid_record",
            "Skipped a transcript record that is not an object.",
            true,
            record_index,
            None,
            None,
        ));
        return (None, diagnostics);
    };

    let uuid = string_field(object, "uuid");
    let parent = object.get("parentUuid");
    let session_id = string_field(object, "sessionId");
    let raw_type = string_field(object, "type");
    let record_id = uuid.map(str::to_owned);
    if uuid.is_none_or(str::is_empty)
        || parent.is_none_or(|parent| !parent.is_null() && !parent.is_string())
        || session_id.is_none_or(str::is_empty)
    {
        diagnostics.push(diagnostic(
            "invalid_record",
            "Skipped a transcript record with invalid identity fields.",
            true,
            record_index,
            record_id,
            None,
        ));
        return (None, diagnostics);
    }
    let Some(record_type) = raw_type.and_then(TranscriptRecordType::parse) else {
        diagnostics.push(diagnostic(
            "unknown_record_or_part",
            "Skipped a transcript record with an unknown record type.",
            true,
            record_index,
            record_id,
            None,
        ));
        return (None, diagnostics);
    };

    let timestamp = object.get("timestamp");
    let valid_timestamp = timestamp
        .and_then(Value::as_str)
        .filter(|value| is_valid_timestamp(value));
    if timestamp.is_some() && valid_timestamp.is_none() {
        diagnostics.push(diagnostic(
            "invalid_timestamp",
            "Ignored an invalid transcript record timestamp.",
            false,
            record_index,
            record_id.clone(),
            Some("timestamp"),
        ));
    }

    let raw_subtype = object.get("subtype");
    let subtype = raw_subtype.and_then(Value::as_str);
    if raw_subtype.is_some()
        && subtype.is_none_or(|subtype| !KNOWN_RECORD_SUBTYPES.contains(&subtype))
    {
        diagnostics.push(diagnostic(
            "unknown_record_or_part",
            "The transcript record has an unknown subtype.",
            true,
            record_index,
            record_id.clone(),
            Some("subtype"),
        ));
    }

    let message = match object.get("message") {
        None => None,
        Some(message_value) => {
            if let Some(message_object) = message_value.as_object() {
                let raw_parts = message_object.get("parts");
                if raw_parts.is_some_and(|parts| !parts.is_array()) {
                    diagnostics.push(diagnostic(
                        "malformed_part",
                        "Ignored malformed transcript message parts.",
                        true,
                        record_index,
                        record_id.clone(),
                        Some("message.parts"),
                    ));
                }
                Some(TranscriptMessage {
                    role: string_field(message_object, "role").map(str::to_owned),
                    parts: raw_parts.and_then(Value::as_array).cloned(),
                })
            } else {
                diagnostics.push(diagnostic(
                    "malformed_part",
                    "Ignored a malformed transcript message payload.",
                    true,
                    record_index,
                    record_id.clone(),
                    Some("message"),
                ));
                None
            }
        }
    };

    let extra = object
        .iter()
        .filter(|(key, _)| {
            !matches!(
                key.as_str(),
                "uuid" | "parentUuid" | "sessionId" | "type" | "subtype" | "timestamp" | "message"
            )
        })
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    let record = TranscriptRecord {
        uuid: uuid.unwrap_or_default().to_owned(),
        parent_uuid: parent.and_then(Value::as_str).map(str::to_owned),
        session_id: session_id.unwrap_or_default().to_owned(),
        record_type,
        subtype: subtype.map(str::to_owned),
        timestamp: valid_timestamp.map(str::to_owned),
        message,
        extra,
    };
    (Some(record), diagnostics)
}

pub fn select_transcript_leaf(
    records: &[TranscriptRecord],
    leaf_uuid: Option<&str>,
) -> Option<String> {
    if let Some(leaf_uuid) = leaf_uuid {
        return records
            .iter()
            .any(|record| record.uuid == leaf_uuid && is_transcript_conversation_record(record))
            .then(|| leaf_uuid.to_owned());
    }
    records
        .iter()
        .rev()
        .find(|record| is_transcript_conversation_record(record))
        .map(|record| record.uuid.clone())
}

pub fn walk_transcript_uuid_chain<'a>(
    leaf_uuid: &str,
    mut lookup: impl FnMut(&str) -> Option<&'a TranscriptRecord>,
) -> TranscriptUuidChain {
    let mut uuids = Vec::new();
    let mut gaps = Vec::new();
    let mut visited = HashSet::new();
    let mut current_uuid = Some(leaf_uuid);
    let mut cycle_uuid = None;

    while let Some(uuid) = current_uuid {
        if !visited.insert(uuid.to_owned()) {
            cycle_uuid = Some(uuid.to_owned());
            break;
        }
        let Some(record) = lookup(uuid) else {
            break;
        };
        uuids.push(uuid.to_owned());
        let Some(parent_uuid) = record.parent_uuid.as_deref() else {
            break;
        };
        if lookup(parent_uuid).is_none() {
            gaps.push(TranscriptReplayGap {
                child_uuid: uuid.to_owned(),
                missing_parent_uuid: parent_uuid.to_owned(),
            });
            break;
        }
        current_uuid = Some(parent_uuid);
    }
    uuids.reverse();
    TranscriptUuidChain {
        uuids,
        gaps,
        cycle_uuid,
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

pub fn aggregate_transcript_record_fragments(
    records: &[TranscriptRecord],
) -> Result<TranscriptRecord, &'static str> {
    let Some(first) = records.first() else {
        return Err("Cannot aggregate empty transcript record array");
    };
    let mut result = first.clone();
    for record in records.iter().skip(1) {
        if let Some(fragment_message) = &record.message {
            result.message = Some(match result.message.take() {
                Some(mut message) => {
                    let mut parts = message.parts.take().unwrap_or_default();
                    parts.extend(fragment_message.parts.clone().unwrap_or_default());
                    message.parts = Some(parts);
                    message
                }
                None => TranscriptMessage {
                    role: fragment_message.role.clone(),
                    parts: Some(fragment_message.parts.clone().unwrap_or_default()),
                },
            });
        }
        if let Some(value) = record
            .extra
            .get("usageMetadata")
            .filter(|value| js_truthy(value))
        {
            result
                .extra
                .insert("usageMetadata".to_owned(), value.clone());
        }
        if record.extra.get("toolCallResult").is_some_and(js_truthy)
            && !result.extra.get("toolCallResult").is_some_and(js_truthy)
        {
            if let Some(value) = record.extra.get("toolCallResult") {
                result
                    .extra
                    .insert("toolCallResult".to_owned(), value.clone());
            }
        }
        if record.extra.get("model").is_some_and(js_truthy)
            && !result.extra.get("model").is_some_and(js_truthy)
        {
            if let Some(value) = record.extra.get("model") {
                result.extra.insert("model".to_owned(), value.clone());
            }
        }
        if let Some(timestamp) = &record.timestamp {
            if result
                .timestamp
                .as_ref()
                .is_none_or(|base| timestamp > base)
            {
                result.timestamp = Some(timestamp.clone());
            }
        }
    }
    Ok(result)
}

pub fn prepare_transcript_records(
    value: &Value,
    leaf_uuid: Option<&str>,
) -> Result<PreparedTranscriptRecords, TranscriptRecordPreparationError> {
    let values = value
        .as_array()
        .ok_or_else(|| TranscriptRecordPreparationError {
            code: TranscriptRecordPreparationErrorCode::InvalidRecords,
            message: "Transcript records must be an array.".to_owned(),
        })?;

    let mut diagnostics = Vec::new();
    let mut indexed_records = Vec::new();
    for (index, value) in values.iter().enumerate() {
        let (record, record_diagnostics) = validate_transcript_record(value, Some(index));
        diagnostics.extend(record_diagnostics);
        if let Some(record) = record {
            indexed_records.push((record, index));
        }
    }

    let records = indexed_records
        .iter()
        .map(|(record, _)| record.clone())
        .collect::<Vec<_>>();
    let session_ids = indexed_records
        .iter()
        .map(|(record, _)| record.session_id.as_str())
        .collect::<HashSet<_>>();
    if session_ids.len() > 1 {
        return Err(TranscriptRecordPreparationError {
            code: TranscriptRecordPreparationErrorCode::MixedSessionIds,
            message: "Transcript records contain multiple session ids.".to_owned(),
        });
    }
    let session_id = indexed_records
        .first()
        .map(|(record, _)| record.session_id.clone());
    let leaf = select_transcript_leaf(&records, leaf_uuid);
    if leaf_uuid.is_some() && leaf.is_none() {
        return Err(TranscriptRecordPreparationError {
            code: TranscriptRecordPreparationErrorCode::LeafNotFound,
            message: "The requested transcript leaf was not found.".to_owned(),
        });
    }
    let Some(leaf) = leaf else {
        if !records.is_empty() {
            diagnostics.push(diagnostic(
                "artifact_only",
                "The input contains no conversation records.",
                false,
                None,
                None,
                None,
            ));
        }
        return Ok(PreparedTranscriptRecords {
            session_id,
            records: Vec::new(),
            gaps: Vec::new(),
            diagnostics,
        });
    };

    let mut fragments_by_uuid: HashMap<String, Vec<(TranscriptRecord, usize)>> = HashMap::new();
    let mut first_by_uuid: HashMap<String, TranscriptRecord> = HashMap::new();
    for (record, index) in indexed_records
        .iter()
        .filter(|(record, _)| is_transcript_conversation_record(record))
    {
        if let Some(fragments) = fragments_by_uuid.get_mut(&record.uuid) {
            if fragments[0].0.parent_uuid != record.parent_uuid {
                diagnostics.push(diagnostic(
                    "conflicting_parent_uuid",
                    "Duplicate transcript record fragments disagree on parentUuid.",
                    true,
                    Some(*index),
                    Some(record.uuid.clone()),
                    Some("parentUuid"),
                ));
            }
            fragments.push((record.clone(), *index));
        } else {
            fragments_by_uuid.insert(record.uuid.clone(), vec![(record.clone(), *index)]);
            first_by_uuid.insert(record.uuid.clone(), record.clone());
        }
    }

    let chain = walk_transcript_uuid_chain(&leaf, |uuid| first_by_uuid.get(uuid));
    for gap in &chain.gaps {
        diagnostics.push(diagnostic(
            "history_gap",
            "The active transcript chain is missing a parent record.",
            true,
            None,
            Some(gap.child_uuid.clone()),
            None,
        ));
    }
    if let Some(cycle_uuid) = &chain.cycle_uuid {
        diagnostics.push(diagnostic(
            "parent_cycle",
            "The active transcript chain contains a parent cycle.",
            true,
            None,
            Some(cycle_uuid.clone()),
            None,
        ));
    }
    let prepared_records = chain
        .uuids
        .iter()
        .filter_map(|uuid| fragments_by_uuid.get(uuid))
        .map(|fragments| {
            let fragments = fragments
                .iter()
                .map(|(record, _)| record.clone())
                .collect::<Vec<_>>();
            aggregate_transcript_record_fragments(&fragments)
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|message| TranscriptRecordPreparationError {
            code: TranscriptRecordPreparationErrorCode::InvalidRecords,
            message: message.to_owned(),
        })?;
    Ok(PreparedTranscriptRecords {
        session_id,
        records: prepared_records,
        gaps: chain.gaps,
        diagnostics,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn record(uuid: &str, parent: Value, overrides: Value) -> Value {
        let mut result = json!({
            "uuid": uuid,
            "parentUuid": parent,
            "sessionId": "session-1",
            "timestamp": "2026-07-14T00:00:00.000Z",
            "type": "user",
            "message": { "role": "user", "parts": [{ "text": uuid }] }
        });
        if let (Some(base), Some(overrides)) = (result.as_object_mut(), overrides.as_object()) {
            for (key, value) in overrides {
                base.insert(key.clone(), value.clone());
            }
        }
        result
    }

    fn codes(diagnostics: &[TranscriptProjectionDiagnostic]) -> HashSet<&str> {
        diagnostics.iter().map(|item| item.code.as_str()).collect()
    }

    #[test]
    fn wraps_and_recognizes_only_complete_context_parts() {
        let wrapped = wrap_user_prompt_submit_context("extra");
        assert!(is_user_prompt_submit_context_part_text(&wrapped));
        assert!(!is_user_prompt_submit_context_part_text(
            "<canopy:user-prompt-submit-context>\n<canopy:user-prompt-submit-context>\nx\n</canopy:user-prompt-submit-context>\n</canopy:user-prompt-submit-context>"
        ));
        assert!(!is_user_prompt_submit_context_part_text("ordinary text"));
    }

    #[test]
    fn display_projection_uses_authoritative_text_and_retains_media() {
        let message = json!({"parts":[{"text":"model prompt"},{"inlineData":"image"}]});
        let payload = json!({"hookContext":"added by hook","displayText":"visible text"});
        let projection = project_user_transcript_for_display(Some(&message), Some(&payload));
        assert_eq!(projection.display_text.as_deref(), Some("visible text"));
        assert_eq!(projection.parts, vec![json!({"inlineData":"image"})]);
    }

    #[test]
    fn display_projection_removes_only_a_final_complete_hook_context_tag() {
        let message = json!({"parts":[
            {"text":"visible"},
            {"text":wrap_user_prompt_submit_context("hidden")}
        ]});
        let projection = project_user_transcript_for_display(Some(&message), None);
        assert_eq!(projection.parts, vec![json!({"text":"visible"})]);
        let incomplete = json!({"parts":[
            {"text":"visible"},
            {"text":"<canopy:user-prompt-submit-context> partial"}
        ]});
        assert_eq!(
            project_user_transcript_for_display(Some(&incomplete), None)
                .parts
                .len(),
            2
        );
    }

    #[test]
    fn selects_active_branch_and_merges_duplicate_fragments() {
        let prepared = prepare_transcript_records(
            &json!([
                record("root", Value::Null, json!({})),
                record("abandoned", json!("root"), json!({})),
                record(
                    "active",
                    json!("root"),
                    json!({
                        "type":"assistant",
                        "message":{"role":"model","parts":[{"text":"first"}]}
                    })
                ),
                record(
                    "active",
                    json!("root"),
                    json!({
                        "type":"assistant",
                        "timestamp":"2026-07-14T00:00:01.000Z",
                        "message":{"role":"model","parts":[{"text":"second"}]}
                    })
                )
            ]),
            None,
        )
        .unwrap();
        assert_eq!(
            prepared
                .records
                .iter()
                .map(|item| item.uuid.as_str())
                .collect::<Vec<_>>(),
            vec!["root", "active"]
        );
        assert_eq!(
            prepared.records[1]
                .message
                .as_ref()
                .unwrap()
                .parts
                .as_ref()
                .unwrap(),
            &vec![json!({"text":"first"}), json!({"text":"second"})]
        );
        assert_eq!(
            prepared.records[1].timestamp.as_deref(),
            Some("2026-07-14T00:00:01.000Z")
        );
    }

    #[test]
    fn ignores_artifact_when_selecting_leaf_and_reports_artifact_only_input() {
        let prepared = prepare_transcript_records(
            &json!([
                record("root", Value::Null, json!({})),
                record("reply", json!("root"), json!({"type":"assistant"})),
                record(
                    "artifact",
                    json!("reply"),
                    json!({
                        "type":"system",
                        "subtype":"session_artifact_event"
                    })
                )
            ]),
            None,
        )
        .unwrap();
        assert_eq!(prepared.records.len(), 2);
        let artifact_only = prepare_transcript_records(
            &json!([record(
                "artifact",
                Value::Null,
                json!({
                    "type":"system","subtype":"session_artifact_event"
                })
            )]),
            None,
        )
        .unwrap();
        assert_eq!(artifact_only.records.len(), 0);
        assert!(codes(&artifact_only.diagnostics).contains("artifact_only"));
    }

    #[test]
    fn reports_missing_parent_and_parent_cycle() {
        let gap = prepare_transcript_records(
            &json!([
                record("orphan", json!("missing"), json!({})),
                record("leaf", json!("orphan"), json!({}))
            ]),
            None,
        )
        .unwrap();
        assert_eq!(
            gap.gaps,
            vec![TranscriptReplayGap {
                child_uuid: "orphan".to_owned(),
                missing_parent_uuid: "missing".to_owned()
            }]
        );
        assert!(codes(&gap.diagnostics).contains("history_gap"));

        let cycle = prepare_transcript_records(
            &json!([
                record("a", json!("b"), json!({})),
                record("b", json!("a"), json!({}))
            ]),
            Some("a"),
        )
        .unwrap();
        assert!(codes(&cycle.diagnostics).contains("parent_cycle"));
    }

    #[test]
    fn skips_malformed_records_but_keeps_diagnostics() {
        let prepared = prepare_transcript_records(
            &json!([
                null,
                record("valid", Value::Null, json!({"timestamp":"not-a-date"})),
                record("unknown", Value::Null, json!({"type":"future"}))
            ]),
            None,
        )
        .unwrap();
        assert_eq!(prepared.records.len(), 1);
        assert_eq!(prepared.records[0].timestamp, None);
        assert!(codes(&prepared.diagnostics).contains("invalid_record"));
        assert!(codes(&prepared.diagnostics).contains("invalid_timestamp"));
        assert!(codes(&prepared.diagnostics).contains("unknown_record_or_part"));
    }

    #[test]
    fn rejects_mixed_sessions_and_missing_requested_leaf() {
        let mixed = prepare_transcript_records(
            &json!([
                record("one", Value::Null, json!({})),
                record("two", Value::Null, json!({"sessionId":"session-2"}))
            ]),
            None,
        )
        .unwrap_err();
        assert_eq!(
            mixed.code,
            TranscriptRecordPreparationErrorCode::MixedSessionIds
        );

        let missing = prepare_transcript_records(
            &json!([record("one", Value::Null, json!({}))]),
            Some("missing"),
        )
        .unwrap_err();
        assert_eq!(
            missing.code,
            TranscriptRecordPreparationErrorCode::LeafNotFound
        );
    }

    #[test]
    fn reports_duplicate_parent_conflicts_and_unknown_subtypes() {
        let prepared = prepare_transcript_records(
            &json!([
                record("root", Value::Null, json!({})),
                record(
                    "active",
                    json!("root"),
                    json!({
                        "type":"assistant","subtype":"future_visible_record"
                    })
                ),
                record("active", Value::Null, json!({"type":"assistant"}))
            ]),
            Some("active"),
        )
        .unwrap();
        let found = codes(&prepared.diagnostics);
        assert!(found.contains("conflicting_parent_uuid"));
        assert!(found.contains("unknown_record_or_part"));
        let conflict = prepared
            .diagnostics
            .iter()
            .find(|item| item.code == "conflicting_parent_uuid")
            .unwrap();
        assert_eq!(conflict.record_index, Some(2));
    }

    #[test]
    fn accepts_all_current_system_record_subtypes() {
        for subtype in [
            "session_source",
            "agent_retry",
            "realtime_message",
            "goal_state",
            "goal_runtime",
            "branch_checkpoint",
        ] {
            let prepared = prepare_transcript_records(
                &json!([record(
                    "known",
                    Value::Null,
                    json!({"type":"system","subtype":subtype})
                )]),
                None,
            )
            .unwrap();
            assert_eq!(prepared.records.len(), 1, "subtype {subtype}");
            assert!(
                !prepared
                    .diagnostics
                    .iter()
                    .any(|item| item.code == "unknown_record_or_part"),
                "subtype {subtype}"
            );
        }
    }

    #[test]
    fn rejects_explicit_artifact_leaf_and_serializes_error_code() {
        let error = prepare_transcript_records(
            &json!([record(
                "artifact",
                Value::Null,
                json!({"type":"system","subtype":"session_artifact_snapshot"})
            )]),
            Some("artifact"),
        )
        .unwrap_err();
        assert_eq!(
            error.code,
            TranscriptRecordPreparationErrorCode::LeafNotFound
        );
        assert_eq!(
            serde_json::to_value(error.code).unwrap(),
            json!("leaf_not_found")
        );
    }

    #[test]
    fn display_projection_accepts_empty_and_released_display_metadata() {
        let image = json!({"inlineData":{"mimeType":"image/png","data":"data"}});
        let empty_display_message = json!({"parts":[
            image.clone(),
            {"text":wrap_user_prompt_submit_context("hook context")}
        ]});
        let empty_payload = json!({"displayText":"","hookContext":"hook context"});
        let projection =
            project_user_transcript_for_display(Some(&empty_display_message), Some(&empty_payload));
        assert_eq!(projection.display_text.as_deref(), Some(""));
        assert_eq!(projection.parts, vec![image.clone()]);

        let released_message = json!({"parts":[
            image.clone(),
            {"text":"expanded model prompt"},
            {"text":wrap_user_prompt_submit_context("hook context")}
        ]});
        let released_payload = json!({"displayText":"raw @file prompt"});
        let projection =
            project_user_transcript_for_display(Some(&released_message), Some(&released_payload));
        assert_eq!(projection.display_text.as_deref(), Some("raw @file prompt"));
        assert_eq!(projection.parts, vec![image]);
    }

    #[test]
    fn notification_display_text_is_not_user_prompt_metadata() {
        let message = json!({"parts":[{"text":"notification model text"}]});
        let payload = json!({"displayText":"Background agent completed"});
        let projection = project_user_transcript_for_display(Some(&message), Some(&payload));
        assert_eq!(projection.display_text, None);
        assert_eq!(
            projection.parts,
            vec![json!({"text":"notification model text"})]
        );
    }

    #[test]
    fn non_object_payloads_are_absent_but_legacy_user_text_is_preserved() {
        let user = json!({"text":"user text"});
        let tagged = json!({"text":wrap_user_prompt_submit_context("hook context")});
        let message = json!({"parts":[user.clone(), tagged.clone()]});
        let projection = project_user_transcript_for_display(Some(&message), Some(&Value::Null));
        assert_eq!(projection.display_text, None);
        assert_eq!(projection.parts, vec![user.clone()]);

        let legacy_message = json!({"parts":[user.clone(),{"text":"bare hook context"}]});
        let legacy = project_user_transcript_for_display(Some(&legacy_message), None);
        assert_eq!(legacy.display_text, None);
        assert_eq!(legacy.parts.len(), 2);

        let user_authored_tag = json!({"parts":[tagged.clone()]});
        assert_eq!(
            project_user_transcript_for_display(Some(&user_authored_tag), None).parts,
            vec![tagged]
        );
    }

    #[test]
    fn bare_display_text_requires_provenance_or_a_final_context_tag() {
        let tagged_part = json!({
            "text":"<canopy:user-prompt-submit-context>user-authored text</canopy:user-prompt-submit-context>"
        });
        let message = json!({"parts":[{"text":"user text"}, tagged_part.clone()]});
        let payload = json!({"displayText":"notification label"});
        let projection = project_user_transcript_for_display(Some(&message), Some(&payload));
        assert_eq!(projection.display_text, None);
        assert_eq!(
            projection.parts,
            vec![json!({"text":"user text"}), tagged_part]
        );
    }

    #[test]
    fn malformed_message_and_part_are_ignored_with_path_diagnostics() {
        let mut malformed_message = record("message", Value::Null, json!({}));
        malformed_message["message"] = json!("not an object");
        let (_, message_diagnostics) = validate_transcript_record(&malformed_message, Some(0));
        assert_eq!(message_diagnostics[0].code, "malformed_part");
        assert_eq!(message_diagnostics[0].path.as_deref(), Some("message"));

        let mut malformed_parts = record("parts", Value::Null, json!({}));
        malformed_parts["message"]["parts"] = json!("not an array");
        let (validated, parts_diagnostics) = validate_transcript_record(&malformed_parts, Some(1));
        assert!(validated.is_some());
        assert_eq!(parts_diagnostics[0].code, "malformed_part");
        assert_eq!(parts_diagnostics[0].path.as_deref(), Some("message.parts"));
    }

    #[test]
    fn preserves_last_usage_and_first_truthy_model_across_fragments() {
        let (first, _) = validate_transcript_record(
            &record(
                "same",
                Value::Null,
                json!({"model":"model-a","usageMetadata":{"a":1}}),
            ),
            Some(0),
        );
        let (second, _) = validate_transcript_record(
            &record(
                "same",
                Value::Null,
                json!({"model":"model-b","usageMetadata":{"b":2}}),
            ),
            Some(1),
        );
        let merged =
            aggregate_transcript_record_fragments(&[first.unwrap(), second.unwrap()]).unwrap();
        assert_eq!(merged.extra.get("model"), Some(&json!("model-a")));
        assert_eq!(merged.extra.get("usageMetadata"), Some(&json!({"b":2})));
    }

    #[test]
    fn ignores_falsy_usage_and_takes_first_truthy_tool_result() {
        let (first, _) = validate_transcript_record(
            &record(
                "same",
                Value::Null,
                json!({"usageMetadata":{"kept":true},"toolCallResult":{"result":"first"}}),
            ),
            Some(0),
        );
        let (second, _) = validate_transcript_record(
            &record(
                "same",
                Value::Null,
                json!({"usageMetadata":null,"toolCallResult":{"result":"second"}}),
            ),
            Some(1),
        );
        let merged =
            aggregate_transcript_record_fragments(&[first.unwrap(), second.unwrap()]).unwrap();
        assert_eq!(
            merged.extra.get("usageMetadata"),
            Some(&json!({"kept":true}))
        );
        assert_eq!(
            merged.extra.get("toolCallResult"),
            Some(&json!({"result":"first"}))
        );
    }
}
