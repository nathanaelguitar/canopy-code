//! Reconstruct provider-facing model history from persisted session records.
//!
//! The input records are consumed so large tool results can be moved into the
//! API history without a second deep copy. This mirrors
//! `packages/core/src/services/session-api-history.ts`.

use serde_json::{Map, Value};
use thiserror::Error;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BuildApiHistoryOptions {
    pub strip_thoughts_from_history: bool,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum SessionApiHistoryError {
    #[error("chat compression record has a non-array compressedHistory value")]
    InvalidCompressedHistory,
}

#[derive(Clone, Debug, Default)]
enum CompressionCandidate {
    #[default]
    None,
    ValidArray,
    Invalid,
}

#[derive(Clone, Debug, Default)]
pub struct SessionApiHistoryAccumulator {
    history: Vec<Value>,
    compression_candidate: CompressionCandidate,
}

fn js_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_f64().is_some_and(|number| number != 0.0),
        Value::String(value) => !value.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}

fn role(content: &Value) -> Option<&str> {
    content.get("role").and_then(Value::as_str)
}

fn append_record(history: &mut Vec<Value>, mut record: Map<String, Value>) {
    if record.get("subtype").and_then(Value::as_str) == Some("realtime_message") {
        return;
    }
    let Some(mut message) = record.remove("message").filter(js_truthy) else {
        return;
    };

    if record.get("subtype").and_then(Value::as_str) == Some("mid_turn_user_message")
        && history.last().and_then(role) == Some("user")
    {
        let message_parts = message
            .as_object_mut()
            .and_then(|content| content.remove("parts"))
            .and_then(|parts| match parts {
                Value::Array(parts) => Some(parts),
                _ => None,
            })
            .unwrap_or_default();
        if let Some(previous) = history.last_mut() {
            let previous_content_parts = previous
                .as_object_mut()
                .and_then(|content| content.remove("parts"))
                .and_then(|parts| match parts {
                    Value::Array(parts) => Some(parts),
                    _ => None,
                })
                .unwrap_or_default();
            let mut combined = previous_content_parts;
            combined.extend(message_parts);
            previous["parts"] = Value::Array(combined);
            return;
        }
    }

    history.push(message);
}

fn take_compression_history(record: &mut Map<String, Value>) -> Option<Value> {
    if record.get("type").and_then(Value::as_str) != Some("system")
        || record.get("subtype").and_then(Value::as_str) != Some("chat_compression")
    {
        return None;
    }
    let payload = record.get_mut("systemPayload")?.as_object_mut()?;
    let compressed_history = payload.remove("compressedHistory")?;
    js_truthy(&compressed_history).then_some(compressed_history)
}

fn strip_thoughts(mut content: Value) -> Option<Value> {
    let Some(content_object) = content.as_object_mut() else {
        return Some(content);
    };
    let Some(Value::Array(parts)) = content_object.remove("parts") else {
        return Some(content);
    };

    let filtered: Vec<Value> = parts
        .into_iter()
        .filter(|part| !part.get("thought").is_some_and(js_truthy))
        .collect();
    if filtered.is_empty() {
        return None;
    }
    if let Some(content_object) = content.as_object_mut() {
        content_object.insert("parts".to_owned(), Value::Array(filtered));
    }
    Some(content)
}

impl SessionApiHistoryAccumulator {
    /// Add one persisted ChatRecord. The record is consumed to avoid copying
    /// large response payloads while rebuilding history.
    pub fn add(&mut self, record: Value) {
        let Value::Object(mut record) = record else {
            return;
        };
        if record.get("type").and_then(Value::as_str) == Some("system") {
            let Some(compressed_history) = take_compression_history(&mut record) else {
                return;
            };
            match compressed_history {
                Value::Array(history) => {
                    self.compression_candidate = CompressionCandidate::ValidArray;
                    self.history = history;
                }
                _ => {
                    self.compression_candidate = CompressionCandidate::Invalid;
                    self.history.clear();
                }
            }
            return;
        }

        if matches!(self.compression_candidate, CompressionCandidate::Invalid) {
            return;
        }
        append_record(&mut self.history, record);
    }

    /// Finish reconstruction. Malformed, truthy compression snapshots are
    /// reported explicitly instead of panicking while attempting to iterate.
    pub fn finish(
        mut self,
        options: BuildApiHistoryOptions,
    ) -> Result<Vec<Value>, SessionApiHistoryError> {
        if matches!(self.compression_candidate, CompressionCandidate::Invalid) {
            return Err(SessionApiHistoryError::InvalidCompressedHistory);
        }
        if options.strip_thoughts_from_history {
            self.history = self
                .history
                .into_iter()
                .filter_map(strip_thoughts)
                .collect();
        }
        Ok(self.history)
    }
}

/// True when the record marks a truthy compressed-history checkpoint.
pub fn is_api_history_compression_candidate(record: &Value) -> bool {
    if record.get("type").and_then(Value::as_str) != Some("system")
        || record.get("subtype").and_then(Value::as_str) != Some("chat_compression")
    {
        return false;
    }
    record
        .get("systemPayload")
        .and_then(|payload| payload.get("compressedHistory"))
        .is_some_and(js_truthy)
}

/// Build API Content history from records in chronological order.
pub fn build_api_history_from_conversation(
    records: impl IntoIterator<Item = Value>,
    options: BuildApiHistoryOptions,
) -> Result<Vec<Value>, SessionApiHistoryError> {
    let mut accumulator = SessionApiHistoryAccumulator::default();
    for record in records {
        accumulator.add(record);
    }
    accumulator.finish(options)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn record(record_type: &str, message: Value) -> Value {
        json!({"type":record_type,"message":message})
    }

    #[test]
    fn copies_messages_into_history_and_ignores_records_without_messages() {
        let history = build_api_history_from_conversation(
            vec![
                json!({"type":"system","subtype":"slash_command"}),
                record("user", json!({"role":"user","parts":[{"text":"hi"}]})),
                record(
                    "assistant",
                    json!({"role":"model","parts":[{"text":"hello"}]}),
                ),
            ],
            BuildApiHistoryOptions::default(),
        )
        .unwrap();
        assert_eq!(history.len(), 2);
        assert_eq!(history[0]["role"], "user");
        assert_eq!(history[1]["role"], "model");
    }

    #[test]
    fn excludes_realtime_messages_from_backend_history() {
        let history = build_api_history_from_conversation(
            vec![
                json!({"type":"user","subtype":"realtime_message","message":{"role":"user","parts":[{"text":"voice"}]}}),
                record("user", json!({"role":"user","parts":[{"text":"task"}]})),
            ],
            BuildApiHistoryOptions::default(),
        )
        .unwrap();
        assert_eq!(
            history,
            vec![json!({"role":"user","parts":[{"text":"task"}]})]
        );
    }

    #[test]
    fn merges_mid_turn_user_messages_into_the_previous_user_content() {
        let history = build_api_history_from_conversation(
            vec![
                record("tool_result", json!({"role":"user","parts":[{"functionResponse":{"id":"c1","response":{"output":"ok"}}}]})),
                json!({"type":"user","subtype":"mid_turn_user_message","message":{"role":"user","parts":[{"text":"stop"}]}}),
            ],
            BuildApiHistoryOptions::default(),
        )
        .unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0]["parts"].as_array().unwrap().len(), 2);
        assert_eq!(history[0]["parts"][1]["text"], "stop");
    }

    #[test]
    fn compression_checkpoint_replaces_earlier_history_and_keeps_later_records() {
        let history = build_api_history_from_conversation(
            vec![
                record("user", json!({"role":"user","parts":[{"text":"old"}]})),
                json!({"type":"system","subtype":"chat_compression","systemPayload":{"compressedHistory":[{"role":"user","parts":[{"text":"summary"}]}]}}),
                record("user", json!({"role":"user","parts":[{"text":"new"}]})),
            ],
            BuildApiHistoryOptions::default(),
        )
        .unwrap();
        assert_eq!(
            history,
            vec![
                json!({"role":"user","parts":[{"text":"summary"}]}),
                json!({"role":"user","parts":[{"text":"new"}]}),
            ]
        );
    }

    #[test]
    fn strips_thought_parts_and_drops_entries_that_only_contain_thoughts() {
        let history = build_api_history_from_conversation(
            vec![
                record("assistant", json!({"role":"model","parts":[{"text":"think","thought":true}]})),
                record("assistant", json!({"role":"model","parts":[{"text":"think","thought":true},{"text":"answer"}]})),
            ],
            BuildApiHistoryOptions { strip_thoughts_from_history: true },
        )
        .unwrap();
        assert_eq!(
            history,
            vec![json!({"role":"model","parts":[{"text":"answer"}]})]
        );
    }

    #[test]
    fn truthy_non_array_compression_snapshot_is_an_error() {
        let result = build_api_history_from_conversation(
            vec![
                json!({"type":"system","subtype":"chat_compression","systemPayload":{"compressedHistory":{"bad":true}}}),
            ],
            BuildApiHistoryOptions::default(),
        );
        assert_eq!(
            result,
            Err(SessionApiHistoryError::InvalidCompressedHistory)
        );
    }
}
