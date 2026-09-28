//! Durable branch checkpoint validation for recorded conversation chains.
//!
//! The TypeScript implementation consumes `ChatRecord` and provider `Part`
//! objects. This port keeps transcript records and parts as JSON values so it
//! can read older and provider-specific transcript shapes without requiring a
//! rigid Rust schema.

use indexmap::IndexMap;
use serde_json::Value;
use std::collections::{HashMap, HashSet};

/// A chat record represented in the persisted transcript JSON shape.
pub type BranchPointRecord = Value;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BranchCheckpointRecordPayloadV1 {
    pub v: u8,
    pub start_exclusive_record_uuid: Option<String>,
    pub assistant_record_uuid: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BranchCandidate {
    pub start_exclusive_record_uuid: Option<String>,
    pub end_inclusive_record_uuid: String,
    pub assistant_record_uuid: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BranchPoint {
    pub start_exclusive_record_uuid: Option<String>,
    pub end_inclusive_record_uuid: String,
    pub assistant_record_uuid: String,
    pub checkpoint_uuid: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BranchToolCallIdentity {
    pub id: Option<String>,
    pub name: Option<String>,
}

#[derive(Clone, Debug)]
struct PendingToolCall {
    identity: BranchToolCallIdentity,
    carried_from_prefix: bool,
}

#[derive(Clone, Debug)]
struct ValidCheckpoint {
    checkpoint_index: usize,
    checkpoint_uuid: String,
    start_index: isize,
    payload: BranchCheckpointRecordPayloadV1,
}

fn string_field<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value.get(key).and_then(Value::as_str)
}

fn record_uuid(record: &Value) -> Option<&str> {
    string_field(record, "uuid")
}

fn record_parts(record: &Value) -> impl Iterator<Item = &Value> {
    record
        .get("message")
        .and_then(|message| message.get("parts"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        // Transcript JSONL can contain null parts. TypeScript also keeps
        // arrays here because `typeof [] === 'object'`; neither has call/text
        // properties, so retaining either is harmless.
        .filter(|part| part.is_object() || part.is_array())
}

fn non_empty_string_field(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty())
        .map(str::to_owned)
}

fn is_json_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_f64().is_some_and(|number| number != 0.0),
        Value::String(value) => !value.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}

fn tool_calls(record: &Value) -> Vec<BranchToolCallIdentity> {
    record_parts(record)
        .filter_map(|part| part.get("functionCall").filter(|call| is_json_truthy(call)))
        .map(|call| BranchToolCallIdentity {
            id: non_empty_string_field(call, "id"),
            name: non_empty_string_field(call, "name"),
        })
        .collect()
}

fn tool_responses(record: &Value) -> Vec<BranchToolCallIdentity> {
    record_parts(record)
        .filter_map(|part| {
            part.get("functionResponse")
                .filter(|response| is_json_truthy(response))
        })
        .map(|response| BranchToolCallIdentity {
            id: non_empty_string_field(response, "id"),
            name: non_empty_string_field(response, "name"),
        })
        .collect()
}

fn is_ecmascript_trim_whitespace(character: char) -> bool {
    matches!(
        character,
        '\u{0009}'
            | '\u{000A}'
            | '\u{000B}'
            | '\u{000C}'
            | '\u{000D}'
            | '\u{0020}'
            | '\u{00A0}'
            | '\u{1680}'
            | '\u{2000}'
            ..='\u{200A}'
                | '\u{2028}'
                | '\u{2029}'
                | '\u{202F}'
                | '\u{205F}'
                | '\u{3000}'
                | '\u{FEFF}'
    )
}

fn has_visible_text(record: &Value) -> bool {
    record_parts(record).any(|part| {
        part.get("thought").and_then(Value::as_bool) != Some(true)
            && part
                .get("text")
                .and_then(Value::as_str)
                .is_some_and(|text| !text.trim_matches(is_ecmascript_trim_whitespace).is_empty())
    })
}

fn unique_name_match(pending: &[PendingToolCall], matching_indexes: &[usize]) -> Option<usize> {
    if matching_indexes.len() == 1 {
        return matching_indexes.first().copied();
    }
    // A dangling call inherited from before the boundary must not make a
    // unique call issued in this candidate interval impossible to match.
    let mut fresh = matching_indexes
        .iter()
        .copied()
        .filter(|&index| !pending[index].carried_from_prefix);
    let only_fresh = fresh.next()?;
    fresh.next().is_none().then_some(only_fresh)
}

fn close_tool_call(pending: &mut Vec<PendingToolCall>, response: &BranchToolCallIdentity) -> bool {
    let index = if let Some(response_id) = response.id.as_deref() {
        pending
            .iter()
            .position(|call| call.identity.id.as_deref() == Some(response_id))
            .or_else(|| {
                let name = response.name.as_deref()?;
                let matching: Vec<_> = pending
                    .iter()
                    .enumerate()
                    .filter_map(|(index, call)| {
                        (call.identity.id.is_none() && call.identity.name.as_deref() == Some(name))
                            .then_some(index)
                    })
                    .collect();
                unique_name_match(pending, &matching)
            })
    } else if let Some(response_name) = response.name.as_deref() {
        let matching: Vec<_> = pending
            .iter()
            .enumerate()
            .filter_map(|(index, call)| {
                (call.identity.name.as_deref() == Some(response_name)).then_some(index)
            })
            .collect();
        unique_name_match(pending, &matching)
    } else {
        None
    };
    if let Some(index) = index {
        pending.remove(index);
        true
    } else {
        false
    }
}

fn update_pending_with_record(pending: &mut Vec<PendingToolCall>, record: &Value) {
    pending.extend(
        tool_calls(record)
            .into_iter()
            .map(|identity| PendingToolCall {
                identity,
                carried_from_prefix: false,
            }),
    );
    for response in tool_responses(record) {
        close_tool_call(pending, &response);
    }
}

/// Add function calls in `record` and close any calls matched by its responses.
/// Unmatched responses are ignored, matching the transcript accumulation API.
pub fn update_pending_branch_tool_calls(
    pending_calls: &mut Vec<BranchToolCallIdentity>,
    record: &BranchPointRecord,
) {
    let mut pending: Vec<_> = pending_calls
        .drain(..)
        .map(|identity| PendingToolCall {
            identity,
            carried_from_prefix: false,
        })
        .collect();
    update_pending_with_record(&mut pending, record);
    pending_calls.extend(pending.into_iter().map(|call| call.identity));
}

/// Return function calls that remain unpaired across the supplied records.
pub fn collect_pending_branch_tool_calls(
    records: &[BranchPointRecord],
) -> Vec<BranchToolCallIdentity> {
    let mut pending = Vec::new();
    for record in records {
        update_pending_branch_tool_calls(&mut pending, record);
    }
    pending
}

/// Parse a v1 branch checkpoint `systemPayload` value.
pub fn parse_branch_checkpoint_payload(value: &Value) -> Option<BranchCheckpointRecordPayloadV1> {
    let payload = value.as_object()?;
    let version = payload.get("v")?.as_number()?.as_f64()?;
    if version != 1.0 {
        return None;
    }
    let start = match payload.get("startExclusiveRecordUuid")? {
        Value::Null => None,
        Value::String(value) if !value.is_empty() => Some(value.clone()),
        _ => return None,
    };
    let assistant = payload
        .get("assistantRecordUuid")?
        .as_str()
        .filter(|value| !value.is_empty())?
        .to_owned();
    Some(BranchCheckpointRecordPayloadV1 {
        v: 1,
        start_exclusive_record_uuid: start,
        assistant_record_uuid: assistant,
    })
}

fn resolve_candidate_in_range(
    active_chain: &[BranchPointRecord],
    start_index: isize,
    end_index: usize,
    start_exclusive_record_uuid: Option<String>,
    pending_calls_at_start: &[BranchToolCallIdentity],
) -> Option<BranchCandidate> {
    let mut pending: Vec<_> = pending_calls_at_start
        .iter()
        .cloned()
        .map(|identity| PendingToolCall {
            identity,
            carried_from_prefix: true,
        })
        .collect();
    let mut last_tool_result_index = start_index;

    for index in (start_index + 1) as usize..=end_index {
        let record = active_chain.get(index)?;
        pending.extend(
            tool_calls(record)
                .into_iter()
                .map(|identity| PendingToolCall {
                    identity,
                    carried_from_prefix: false,
                }),
        );
        let responses = tool_responses(record);
        if string_field(record, "type") == Some("tool_result") || !responses.is_empty() {
            last_tool_result_index = index as isize;
        }
        for response in responses {
            if !close_tool_call(&mut pending, &response) {
                return None;
            }
        }
    }
    if pending.iter().any(|call| !call.carried_from_prefix) {
        return None;
    }

    let mut assistant_record_uuid = None;
    for index in (last_tool_result_index + 1) as usize..=end_index {
        let record = active_chain.get(index)?;
        if string_field(record, "type") != Some("assistant")
            || !tool_calls(record).is_empty()
            || !has_visible_text(record)
        {
            continue;
        }
        if assistant_record_uuid.is_some() {
            return None;
        }
        assistant_record_uuid = record_uuid(record).map(str::to_owned);
    }
    Some(BranchCandidate {
        start_exclusive_record_uuid,
        end_inclusive_record_uuid: record_uuid(active_chain.get(end_index)?)?.to_owned(),
        assistant_record_uuid: assistant_record_uuid?,
    })
}

/// Resolve a completed turn from an already bounded record slice.
pub fn resolve_completed_turn_branch_candidate_from_records(
    records: &[BranchPointRecord],
    start_exclusive_record_uuid: Option<String>,
    pending_calls_at_start: &[BranchToolCallIdentity],
) -> Option<BranchCandidate> {
    if records.is_empty() {
        return None;
    }
    resolve_candidate_in_range(
        records,
        -1,
        records.len() - 1,
        start_exclusive_record_uuid,
        pending_calls_at_start,
    )
}

/// Validate durable branch checkpoints against the active transcript chain.
/// The returned `IndexMap` retains checkpoint order, like JavaScript `Map`.
pub fn resolve_branch_points(active_chain: &[BranchPointRecord]) -> IndexMap<String, BranchPoint> {
    let mut points = IndexMap::new();
    let mut record_indexes = HashMap::<String, usize>::new();
    for (index, record) in active_chain.iter().enumerate() {
        let Some(uuid) = record_uuid(record).filter(|uuid| !uuid.is_empty()) else {
            return points;
        };
        if record_indexes.insert(uuid.to_owned(), index).is_some() {
            return IndexMap::new();
        }
    }

    let mut checkpoints = Vec::new();
    let mut boundary_indexes = HashSet::new();
    for (index, checkpoint) in active_chain.iter().enumerate() {
        if string_field(checkpoint, "type") != Some("system")
            || string_field(checkpoint, "subtype") != Some("branch_checkpoint")
            || checkpoint.get("parentUuid").is_none_or(Value::is_null)
        {
            continue;
        }
        let parent = string_field(checkpoint, "parentUuid");
        if index == 0 || record_uuid(&active_chain[index - 1]) != parent {
            continue;
        }
        let Some(payload) = checkpoint
            .get("systemPayload")
            .and_then(parse_branch_checkpoint_payload)
        else {
            continue;
        };
        let start_index = match payload.start_exclusive_record_uuid.as_deref() {
            None => -1,
            Some(start_uuid) => record_indexes
                .get(start_uuid)
                .map_or(-1, |index| *index as isize),
        };
        if payload.start_exclusive_record_uuid.is_some()
            && (start_index < 0 || start_index >= index as isize - 1)
        {
            continue;
        }
        let Some(checkpoint_uuid) = record_uuid(checkpoint).map(str::to_owned) else {
            continue;
        };
        if start_index >= 0 {
            boundary_indexes.insert(start_index as usize);
        }
        checkpoints.push(ValidCheckpoint {
            checkpoint_index: index,
            checkpoint_uuid,
            start_index,
            payload,
        });
    }

    let mut pending_at_boundary = IndexMap::<usize, Vec<BranchToolCallIdentity>>::new();
    let mut pending = Vec::<BranchToolCallIdentity>::new();
    for (index, record) in active_chain.iter().enumerate() {
        update_pending_branch_tool_calls(&mut pending, record);
        if boundary_indexes.contains(&index) {
            pending_at_boundary.insert(index, pending.clone());
        }
    }

    let mut checkpoint_by_assistant = IndexMap::<String, String>::new();
    for checkpoint in checkpoints {
        let pending_calls_at_start = if checkpoint.start_index < 0 {
            &[][..]
        } else {
            pending_at_boundary
                .get(&(checkpoint.start_index as usize))
                .map(Vec::as_slice)
                .unwrap_or(&[])
        };
        let candidate = resolve_candidate_in_range(
            active_chain,
            checkpoint.start_index,
            checkpoint.checkpoint_index - 1,
            checkpoint.payload.start_exclusive_record_uuid.clone(),
            pending_calls_at_start,
        );
        let Some(candidate) = candidate.filter(|candidate| {
            candidate.assistant_record_uuid == checkpoint.payload.assistant_record_uuid
        }) else {
            continue;
        };
        if let Some(previous_checkpoint) =
            checkpoint_by_assistant.get(&candidate.assistant_record_uuid)
        {
            points.shift_remove(previous_checkpoint);
            continue;
        }
        let assistant_uuid = candidate.assistant_record_uuid.clone();
        points.insert(
            checkpoint.checkpoint_uuid.clone(),
            BranchPoint {
                start_exclusive_record_uuid: candidate.start_exclusive_record_uuid,
                end_inclusive_record_uuid: candidate.end_inclusive_record_uuid,
                assistant_record_uuid: candidate.assistant_record_uuid,
                checkpoint_uuid: checkpoint.checkpoint_uuid.clone(),
            },
        );
        checkpoint_by_assistant.insert(assistant_uuid, checkpoint.checkpoint_uuid);
    }
    points
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    fn record(uuid: &str, parent: Value, kind: &str, parts: Value) -> Value {
        json!({
            "uuid": uuid,
            "parentUuid": parent,
            "type": kind,
            "message": {"parts": parts},
        })
    }

    fn checkpoint(uuid: &str, parent: &str, assistant: &str, start: Value) -> Value {
        json!({
            "uuid": uuid,
            "parentUuid": parent,
            "type": "system",
            "subtype": "branch_checkpoint",
            "systemPayload": {
                "v": 1,
                "startExclusiveRecordUuid": start,
                "assistantRecordUuid": assistant,
            },
        })
    }

    fn simple_turn() -> Vec<Value> {
        vec![
            record("u1", Value::Null, "user", json!([{"text":"question"}])),
            record("a1", json!("u1"), "assistant", json!([{"text":"answer"}])),
        ]
    }

    #[test]
    fn resolves_a_durable_checkpoint_for_a_text_turn() {
        let mut chain = simple_turn();
        chain.push(checkpoint("c1", "a1", "a1", Value::Null));
        assert_eq!(
            resolve_branch_points(&chain).get("c1"),
            Some(&BranchPoint {
                start_exclusive_record_uuid: None,
                end_inclusive_record_uuid: "a1".into(),
                assistant_record_uuid: "a1".into(),
                checkpoint_uuid: "c1".into(),
            })
        );
    }

    #[test]
    fn accepts_a_closed_tool_loop_and_skips_null_parts() {
        let chain = vec![
            record("u1", Value::Null, "user", json!([{"text":"question"}])),
            record(
                "a-tool",
                json!("u1"),
                "assistant",
                json!([null, {"functionCall":{"id":"call-1","name":"read_file","args":{}}}]),
            ),
            record(
                "tool",
                json!("a-tool"),
                "tool_result",
                json!([null, {"functionResponse":{"id":"call-1","name":"read_file","response":{"output":"ok"}}}]),
            ),
            record(
                "a-final",
                json!("tool"),
                "assistant",
                json!([null, {"text":"done"}]),
            ),
            checkpoint("c1", "a-final", "a-final", Value::Null),
        ];
        assert_eq!(
            resolve_branch_points(&chain)["c1"].assistant_record_uuid,
            "a-final"
        );
    }

    #[test]
    fn rejects_dangling_mismatched_ambiguous_and_orphan_calls() {
        let invalid_chains = [
            vec![
                record("u1", Value::Null, "user", json!([])),
                record(
                    "a1",
                    json!("u1"),
                    "assistant",
                    json!([{"functionCall":{"id":"x","name":"read_file"}}]),
                ),
            ],
            vec![
                record("u1", Value::Null, "user", json!([])),
                record(
                    "ac",
                    json!("u1"),
                    "assistant",
                    json!([{"functionCall":{"id":"x","name":"read_file"}}]),
                ),
                record(
                    "tool",
                    json!("ac"),
                    "tool_result",
                    json!([{"functionResponse":{"id":"wrong","name":"read_file"}}]),
                ),
                record("a1", json!("tool"), "assistant", json!([{"text":"done"}])),
            ],
            vec![
                record("u1", Value::Null, "user", json!([])),
                record(
                    "ac",
                    json!("u1"),
                    "assistant",
                    json!([
                        {"functionCall":{"name":"read_file","args":{"path":"a"}}},
                        {"functionCall":{"name":"read_file","args":{"path":"b"}}}
                    ]),
                ),
                record(
                    "tool",
                    json!("ac"),
                    "tool_result",
                    json!([{"functionResponse":{"name":"read_file"}}]),
                ),
                record("a1", json!("tool"), "assistant", json!([{"text":"done"}])),
            ],
            vec![
                record("u1", Value::Null, "user", json!([])),
                record(
                    "tool",
                    json!("u1"),
                    "tool_result",
                    json!([{"functionResponse":{"id":"ghost","name":"read_file"}}]),
                ),
                record("a1", json!("tool"), "assistant", json!([{"text":"done"}])),
            ],
        ];
        for mut chain in invalid_chains {
            let end = record_uuid(chain.last().unwrap()).unwrap().to_owned();
            chain.push(checkpoint("c1", &end, &end, Value::Null));
            assert!(resolve_branch_points(&chain).is_empty());
        }
    }

    #[test]
    fn requires_exactly_one_visible_final_assistant_after_the_last_tool_result() {
        let cases = [
            vec![
                record("u1", Value::Null, "user", json!([])),
                record("a1", json!("u1"), "assistant", json!([{"text":"first"}])),
                record("a2", json!("a1"), "assistant", json!([{"text":"second"}])),
            ],
            vec![
                record("u1", Value::Null, "user", json!([])),
                record(
                    "a1",
                    json!("u1"),
                    "assistant",
                    json!([{"thought":true,"text":"internal"}]),
                ),
            ],
        ];
        for mut chain in cases {
            let end = record_uuid(chain.last().unwrap()).unwrap().to_owned();
            chain.push(checkpoint("c1", &end, &end, Value::Null));
            assert!(resolve_branch_points(&chain).is_empty());
        }
    }

    #[test]
    fn visible_text_uses_ecmascript_whitespace_trimming() {
        assert!(!has_visible_text(&record(
            "bom-only",
            Value::Null,
            "assistant",
            json!([{"text":"\u{feff} \n"}]),
        )));
        assert!(has_visible_text(&record(
            "nel-is-visible",
            Value::Null,
            "assistant",
            json!([{"text":"\u{0085}"}]),
        )));
    }

    #[test]
    fn carried_prefix_call_does_not_veto_a_unique_fresh_name_match() {
        let records = vec![
            record(
                "u1",
                json!("old-tail"),
                "user",
                json!([{"text":"continue"}]),
            ),
            record(
                "ac",
                json!("u1"),
                "assistant",
                json!([{"functionCall":{"name":"read_file"}}]),
            ),
            record(
                "tool",
                json!("ac"),
                "tool_result",
                json!([{"functionResponse":{"name":"read_file"}}]),
            ),
            record("a1", json!("tool"), "assistant", json!([{"text":"done"}])),
        ];
        let candidate = resolve_completed_turn_branch_candidate_from_records(
            &records,
            Some("old-tail".into()),
            &[BranchToolCallIdentity {
                id: None,
                name: Some("read_file".into()),
            }],
        );
        assert_eq!(candidate.unwrap().assistant_record_uuid, "a1");
    }

    #[test]
    fn pending_call_collection_matches_exact_ids_and_name_fallbacks() {
        let records = vec![record(
            "mixed",
            Value::Null,
            "assistant",
            json!([
                {"functionCall":{"id":"call-1","name":"read_file"}},
                {"functionCall":{"name":"write_file"}},
                {"functionResponse":{"id":"call-1","name":"read_file"}},
                {"functionResponse":{"id":"provider-id","name":"write_file"}},
                null
            ]),
        )];
        assert!(collect_pending_branch_tool_calls(&records).is_empty());
    }

    #[test]
    fn duplicate_ids_detached_checkpoints_and_wrong_payloads_are_rejected() {
        let duplicate = vec![
            record("same", Value::Null, "user", json!([])),
            record(
                "same",
                json!("same"),
                "assistant",
                json!([{"text":"answer"}]),
            ),
            checkpoint("c1", "same", "same", Value::Null),
        ];
        assert!(resolve_branch_points(&duplicate).is_empty());

        let valid_prefix = simple_turn();
        for invalid in [
            checkpoint("c1", "u1", "a1", Value::Null),
            checkpoint("c1", "a1", "a1", json!("missing")),
            checkpoint("c1", "a1", "other", Value::Null),
            json!({"uuid":"c1","parentUuid":"a1","type":"system","subtype":"branch_checkpoint","systemPayload":{"v":2,"startExclusiveRecordUuid":null,"assistantRecordUuid":"a1"}}),
            json!({"uuid":"c1","parentUuid":"a1","type":"system","subtype":"branch_checkpoint","systemPayload":{"v":1,"startExclusiveRecordUuid":null,"assistantRecordUuid":""}}),
            json!({"uuid":"c1","parentUuid":"a1","type":"system","subtype":"branch_checkpoint","systemPayload":{"v":1,"startExclusiveRecordUuid":42,"assistantRecordUuid":"a1"}}),
        ] {
            let mut chain = valid_prefix.clone();
            chain.push(invalid);
            assert!(resolve_branch_points(&chain).is_empty());
        }
    }

    #[test]
    fn resolves_successive_checkpoints_and_invalidates_duplicate_assistant_targets() {
        let chain = vec![
            record("u1", Value::Null, "user", json!([{"text":"first"}])),
            record(
                "a1",
                json!("u1"),
                "assistant",
                json!([{"text":"first answer"}]),
            ),
            checkpoint("c1", "a1", "a1", Value::Null),
            record("u2", json!("c1"), "user", json!([{"text":"second"}])),
            record(
                "a2",
                json!("u2"),
                "assistant",
                json!([{"text":"second answer"}]),
            ),
            checkpoint("c2", "a2", "a2", json!("c1")),
        ];
        let points = resolve_branch_points(&chain);
        assert_eq!(
            points.keys().map(String::as_str).collect::<Vec<_>>(),
            ["c1", "c2"]
        );

        let duplicate_target = vec![
            record("u1", Value::Null, "user", json!([])),
            record("a1", json!("u1"), "assistant", json!([{"text":"answer"}])),
            checkpoint("c1", "a1", "a1", Value::Null),
            checkpoint("c2", "c1", "a1", Value::Null),
        ];
        assert!(resolve_branch_points(&duplicate_target).is_empty());
    }

    #[test]
    fn checkpoint_payload_accepts_js_numeric_one_and_requires_explicit_null_boundary() {
        assert_eq!(
            parse_branch_checkpoint_payload(&json!({
                "v": 1.0,
                "startExclusiveRecordUuid": null,
                "assistantRecordUuid": "a1"
            })),
            Some(BranchCheckpointRecordPayloadV1 {
                v: 1,
                start_exclusive_record_uuid: None,
                assistant_record_uuid: "a1".into(),
            })
        );
        assert!(
            parse_branch_checkpoint_payload(&json!({"v":1,"assistantRecordUuid":"a1"})).is_none()
        );
    }
}
