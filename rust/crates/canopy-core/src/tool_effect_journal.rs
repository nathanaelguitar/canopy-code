//! Write-ahead intent tracking for tools with external side effects.
//!
//! A durable intent is appended before invoking a tool. A persisted
//! `tool_result` record closes that intent. An intent left open after restart
//! means execution may have happened and must not be silently replayed.

use std::collections::HashMap;

use serde_json::{Value, json};

use crate::recording::SessionRecorder;
use crate::session_writer::SessionWriterError;

pub const TOOL_EXECUTION_INTENT_SUBTYPE: &str = "tool_execution_intent";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolEffectIntent {
    pub call_id: String,
    pub name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PendingIntent {
    sequence: usize,
    intent: ToolEffectIntent,
}

/// Persist the call identity before a tool handler is allowed to run. The
/// assistant's preceding record already contains its arguments, so the intent
/// stores no second copy of potentially large or sensitive arguments.
pub fn record_tool_execution_intent(
    recorder: &mut SessionRecorder,
    call_id: impl Into<String>,
    name: impl Into<String>,
) -> Result<String, SessionWriterError> {
    let call_id = call_id.into();
    let name = name.into();
    recorder.record_system_event(
        TOOL_EXECUTION_INTENT_SUBTYPE,
        json!({"callId":call_id,"name":name}),
    )
}

/// Find intents without a corresponding persisted tool result. A returned
/// intent is ambiguous after a crash: the side effect may have completed
/// before the process died, so callers must request confirmation before
/// issuing it again.
pub fn find_unresolved_tool_effect_intents(records: &[Value]) -> Vec<ToolEffectIntent> {
    let mut pending: HashMap<String, PendingIntent> = HashMap::new();

    for (sequence, record) in records.iter().enumerate() {
        if record.get("type").and_then(Value::as_str) == Some("system")
            && record.get("subtype").and_then(Value::as_str) == Some(TOOL_EXECUTION_INTENT_SUBTYPE)
        {
            let Some(payload) = record.get("systemPayload") else {
                continue;
            };
            let Some(call_id) = payload.get("callId").and_then(Value::as_str) else {
                continue;
            };
            if call_id.is_empty() {
                continue;
            }
            let name = payload
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_owned();
            pending.insert(
                call_id.to_owned(),
                PendingIntent {
                    sequence,
                    intent: ToolEffectIntent {
                        call_id: call_id.to_owned(),
                        name,
                    },
                },
            );
            continue;
        }

        if record.get("type").and_then(Value::as_str) != Some("tool_result") {
            continue;
        }
        let Some(parts) = record
            .get("message")
            .and_then(|message| message.get("parts"))
            .and_then(Value::as_array)
        else {
            continue;
        };
        for part in parts {
            if let Some(call_id) = part
                .get("functionResponse")
                .and_then(|response| response.get("id"))
                .and_then(Value::as_str)
            {
                pending.remove(call_id);
            }
        }
    }

    let mut remaining: Vec<PendingIntent> = pending.into_values().collect();
    remaining.sort_unstable_by_key(|pending| pending.sequence);
    remaining
        .into_iter()
        .map(|pending| pending.intent)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session_paths::SessionArchiveState;
    use crate::session_store::SessionStore;
    use crate::session_writer::SessionWriterProcessKind;
    use serde_json::json;
    use uuid::Uuid;

    #[test]
    fn leaves_an_intent_open_until_the_result_is_persisted() {
        let records = vec![json!({
            "type":"system",
            "subtype":"tool_execution_intent",
            "systemPayload":{"callId":"call-1","name":"write_file"}
        })];
        assert_eq!(
            find_unresolved_tool_effect_intents(&records),
            vec![ToolEffectIntent {
                call_id: "call-1".into(),
                name: "write_file".into()
            }]
        );
    }

    #[test]
    fn persisted_success_or_error_results_close_intents() {
        for response in [json!({"output":"saved"}), json!({"error":"failed"})] {
            let records = vec![
                json!({"type":"system","subtype":"tool_execution_intent","systemPayload":{"callId":"call-1","name":"write_file"}}),
                json!({"type":"tool_result","message":{"role":"user","parts":[{"functionResponse":{"id":"call-1","response":response}}]}}),
            ];
            assert!(find_unresolved_tool_effect_intents(&records).is_empty());
        }
    }

    #[test]
    fn only_matching_results_close_parallel_intents() {
        let records = vec![
            json!({"type":"system","subtype":"tool_execution_intent","systemPayload":{"callId":"a","name":"write_file"}}),
            json!({"type":"system","subtype":"tool_execution_intent","systemPayload":{"callId":"b","name":"shell"}}),
            json!({"type":"tool_result","message":{"role":"user","parts":[{"functionResponse":{"id":"a","response":{"output":"ok"}}}]}}),
        ];
        assert_eq!(
            find_unresolved_tool_effect_intents(&records),
            vec![ToolEffectIntent {
                call_id: "b".into(),
                name: "shell".into()
            }]
        );
    }

    #[test]
    fn an_intent_reissued_after_a_result_is_unresolved_again() {
        let intent = json!({"type":"system","subtype":"tool_execution_intent","systemPayload":{"callId":"a","name":"shell"}});
        let result =
            json!({"type":"tool_result","message":{"parts":[{"functionResponse":{"id":"a"}}]}});
        assert_eq!(
            find_unresolved_tool_effect_intents(&[intent.clone(), result, intent,])[0].call_id,
            "a"
        );
    }

    #[test]
    fn intent_helper_syncs_the_marker_before_returning() {
        let root = std::env::temp_dir().join(format!("canopy-tool-intent-{}", Uuid::new_v4()));
        let store = SessionStore::new(root.join("state"), "/work/project");
        let (session_id, mut recorder) = store
            .create_session(SessionWriterProcessKind::Interactive, "test", None)
            .unwrap();
        record_tool_execution_intent(&mut recorder, "call-1", "write_file").unwrap();
        recorder.close().unwrap();
        let records = store
            .read_transcript(&session_id, SessionArchiveState::Active)
            .unwrap();
        assert_eq!(
            find_unresolved_tool_effect_intents(&records),
            vec![ToolEffectIntent {
                call_id: "call-1".into(),
                name: "write_file".into()
            }]
        );
        std::fs::remove_dir_all(root).unwrap();
    }
}
