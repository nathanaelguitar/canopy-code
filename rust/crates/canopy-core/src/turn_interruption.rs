//! Recoverable end states inferred from persisted model history.
//!
//! This mirrors packages/core/src/core/turn-interruption.ts. It operates on
//! the JSON representation of GenAI Content values so history remains
//! detached from the provider SDK and can be read from Canopy's JSONL journal.

use serde_json::{Value, json};

pub const TURN_INTERRUPTION_HISTORY_TAIL_COUNT: usize = 50;
pub const SYSTEM_REMINDER_OPEN: &str = "<system-reminder>";
pub const SYSTEM_REMINDER_CLOSE: &str = "</system-reminder>";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DanglingToolCall {
    pub call_id: String,
    pub name: String,
}

#[derive(Clone, Debug, PartialEq)]
pub enum TurnInterruption {
    None,
    InterruptedPrompt {
        parts: Vec<Value>,
    },
    InterruptedTurn {
        dangling_calls: Vec<DanglingToolCall>,
    },
}

fn role(content: &Value) -> Option<&str> {
    content.get("role").and_then(Value::as_str)
}

fn parts(content: &Value) -> &[Value] {
    content
        .get("parts")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default()
}

fn is_system_reminder_content(content: &Value) -> bool {
    let content_parts = parts(content);
    !content_parts.is_empty()
        && content_parts.iter().all(|part| {
            part.get("text")
                .and_then(Value::as_str)
                .is_some_and(|text| {
                    text.starts_with(SYSTEM_REMINDER_OPEN)
                        && text.trim_end().ends_with(SYSTEM_REMINDER_CLOSE)
                })
        })
}

/// Inspect the persisted history tail without mutating or aliasing its parts.
///
/// A user tail can be retried. A model tail with unanswered, identified tool
/// calls requires synthetic failures. Clean model output and structural
/// reminder-only entries are not continuations.
pub fn detect_turn_interruption(history: &[Value]) -> TurnInterruption {
    let Some(last) = history.last() else {
        return TurnInterruption::None;
    };

    match role(last) {
        Some("user") => {
            let mut trailing_users = Vec::new();
            for entry in history.iter().rev() {
                if role(entry) != Some("user") || is_system_reminder_content(entry) {
                    break;
                }
                trailing_users.push(entry);
            }

            let all_parts: Vec<&Value> = trailing_users.into_iter().rev().flat_map(parts).collect();
            let mut continuation_parts = Vec::with_capacity(all_parts.len());
            continuation_parts.extend(
                all_parts
                    .iter()
                    .filter(|part| part.get("functionResponse").is_some())
                    .map(|part| (*part).clone()),
            );
            continuation_parts.extend(
                all_parts
                    .iter()
                    .filter(|part| part.get("functionResponse").is_none())
                    .map(|part| (*part).clone()),
            );

            if continuation_parts.is_empty() {
                TurnInterruption::None
            } else {
                TurnInterruption::InterruptedPrompt {
                    parts: continuation_parts,
                }
            }
        }
        Some("model") => {
            let dangling_calls: Vec<DanglingToolCall> = parts(last)
                .iter()
                .filter_map(|part| {
                    let call = part.get("functionCall")?;
                    let call_id = call.get("id")?.as_str()?;
                    if call_id.is_empty() {
                        return None;
                    }
                    Some(DanglingToolCall {
                        call_id: call_id.to_owned(),
                        name: call
                            .get("name")
                            .and_then(Value::as_str)
                            .unwrap_or("unknown")
                            .to_owned(),
                    })
                })
                .collect();
            if dangling_calls.is_empty() {
                TurnInterruption::None
            } else {
                TurnInterruption::InterruptedTurn { dangling_calls }
            }
        }
        _ => TurnInterruption::None,
    }
}

/// Construct a provider-compatible error result for each unanswered call.
pub fn build_synthetic_tool_response_parts(
    dangling_calls: &[DanglingToolCall],
    reason: &str,
) -> Vec<Value> {
    dangling_calls
        .iter()
        .map(|call| {
            json!({
                "functionResponse": {
                    "id": call.call_id,
                    "name": call.name,
                    "response": { "error": reason }
                }
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn reminder(text: &str) -> Value {
        json!({"text": format!("{SYSTEM_REMINDER_OPEN}\n{text}\n{SYSTEM_REMINDER_CLOSE}")})
    }

    #[test]
    fn history_tail_count_is_bounded_for_callers() {
        assert_eq!(TURN_INTERRUPTION_HISTORY_TAIL_COUNT, 50);
    }

    #[test]
    fn empty_or_clean_model_history_is_not_interrupted() {
        assert_eq!(detect_turn_interruption(&[]), TurnInterruption::None);
        assert_eq!(
            detect_turn_interruption(&[
                json!({"role":"user","parts":[{"text":"hello"}]}),
                json!({"role":"model","parts":[{"text":"hi"}]}),
            ]),
            TurnInterruption::None
        );
    }

    #[test]
    fn a_pure_system_reminder_tail_is_not_a_prompt() {
        assert_eq!(
            detect_turn_interruption(&[
                json!({"role":"model","parts":[{"text":"done"}]}),
                json!({"role":"user","parts":[reminder("tool added")]}),
            ]),
            TurnInterruption::None
        );
    }

    #[test]
    fn trailing_user_parts_are_retried_in_order_with_tool_results_first() {
        let history = [
            json!({"role":"model","parts":[{"text":"waiting"}]}),
            json!({"role":"user","parts":[{"text":"IDE context"}]}),
            json!({
                "role":"user",
                "parts":[
                    {"text":"prompt reminder"},
                    {"functionResponse":{"id":"call-1","name":"read","response":{"output":"contents"}}}
                ]
            }),
        ];
        assert_eq!(
            detect_turn_interruption(&history),
            TurnInterruption::InterruptedPrompt {
                parts: vec![
                    json!({"functionResponse":{"id":"call-1","name":"read","response":{"output":"contents"}}}),
                    json!({"text":"IDE context"}),
                    json!({"text":"prompt reminder"}),
                ]
            }
        );
    }

    #[test]
    fn reminder_parts_riding_with_a_real_prompt_are_preserved() {
        assert_eq!(
            detect_turn_interruption(&[json!({
                "role":"user",
                "parts":[reminder("plan mode"), {"text":"real prompt"}]
            })]),
            TurnInterruption::InterruptedPrompt {
                parts: vec![reminder("plan mode"), json!({"text":"real prompt"})]
            }
        );
    }

    #[test]
    fn returned_prompt_parts_are_detached_from_history() {
        let mut history = vec![json!({"role":"user","parts":[{"text":"original"}]})];
        let TurnInterruption::InterruptedPrompt { parts } = detect_turn_interruption(&history)
        else {
            panic!("expected interrupted prompt");
        };
        history[0]["parts"][0]["text"] = json!("mutated");
        assert_eq!(parts[0]["text"], "original");
    }

    #[test]
    fn model_tail_reports_only_id_bearing_calls() {
        assert_eq!(
            detect_turn_interruption(&[json!({
                "role":"model",
                "parts":[
                    {"text":"running"},
                    {"functionCall":{"id":"call-1","name":"shell"}},
                    {"functionCall":{"id":"call-2","name":"read"}},
                    {"functionCall":{"name":"unpairable"}}
                ]
            })]),
            TurnInterruption::InterruptedTurn {
                dangling_calls: vec![
                    DanglingToolCall {
                        call_id: "call-1".to_owned(),
                        name: "shell".to_owned()
                    },
                    DanglingToolCall {
                        call_id: "call-2".to_owned(),
                        name: "read".to_owned()
                    },
                ]
            }
        );
    }

    #[test]
    fn call_without_name_uses_unknown_and_empty_ids_are_ignored() {
        assert_eq!(
            detect_turn_interruption(&[json!({
                "role":"model",
                "parts":[
                    {"functionCall":{"id":"call-9"}},
                    {"functionCall":{"id":""}}
                ]
            })]),
            TurnInterruption::InterruptedTurn {
                dangling_calls: vec![DanglingToolCall {
                    call_id: "call-9".to_owned(),
                    name: "unknown".to_owned(),
                }]
            }
        );
    }

    #[test]
    fn a_clean_final_model_turn_ignores_earlier_unanswered_calls() {
        assert_eq!(
            detect_turn_interruption(&[
                json!({"role":"model","parts":[{"functionCall":{"id":"old","name":"shell"}}]}),
                json!({"role":"user","parts":[{"text":"new prompt"}]}),
                json!({"role":"model","parts":[{"text":"ok"}]}),
            ]),
            TurnInterruption::None
        );
    }

    #[test]
    fn empty_user_parts_are_not_a_retryable_prompt() {
        assert_eq!(
            detect_turn_interruption(&[json!({"role":"user","parts":[]})]),
            TurnInterruption::None
        );
    }

    #[test]
    fn synthetic_tool_responses_match_the_persisted_provider_shape() {
        assert_eq!(
            build_synthetic_tool_response_parts(
                &[
                    DanglingToolCall {
                        call_id: "call-1".to_owned(),
                        name: "shell".to_owned()
                    },
                    DanglingToolCall {
                        call_id: "call-2".to_owned(),
                        name: "read".to_owned()
                    },
                ],
                "interrupted",
            ),
            vec![
                json!({"functionResponse":{"id":"call-1","name":"shell","response":{"error":"interrupted"}}}),
                json!({"functionResponse":{"id":"call-2","name":"read","response":{"error":"interrupted"}}}),
            ]
        );
    }
}
