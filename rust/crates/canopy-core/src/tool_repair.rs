//! Repair persisted model tool calls that have missing or misplaced results.
//!
//! This is the Rust counterpart of `repairOrphanedToolUseTurns` in
//! `packages/core/src/core/geminiChat.ts`. It preserves real tool results,
//! synthesizes failures only for missing results, moves late results next to
//! their calls, and removes duplicate results.

use std::collections::HashMap;

use serde_json::{Value, json};

pub const ORPHAN_TOOL_USE_REPAIR_REASON: &str = "Tool execution result was not recorded — likely interrupted by network failure, abort, or process exit. Treat as failure and retry if needed.";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolRepairEntry {
    pub call_id: String,
    pub name: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ToolRepairResult {
    /// Calls for which this pass synthesized an error response.
    pub injected: Vec<ToolRepairEntry>,
    /// Extra real response copies removed after the first canonical result.
    pub dropped_duplicates: Vec<ToolRepairEntry>,
}

#[derive(Clone, Debug)]
struct FunctionResponseLocation {
    turn_idx: usize,
    part_idx: usize,
}

#[derive(Clone, Debug)]
struct ScanResult {
    model_idx: usize,
    /// A Vec preserves JavaScript Map insertion order and repeated IDs update
    /// their name without changing that order.
    expected: Vec<(String, String)>,
    matched: HashMap<String, Vec<FunctionResponseLocation>>,
    scan_end: usize,
}

#[derive(Clone, Debug)]
struct RemovalTarget {
    turn_idx: usize,
    part_idx: usize,
}

#[derive(Clone, Debug)]
struct RepairPlan {
    model_idx: usize,
    scan_end: usize,
    synthesize: Vec<(String, String)>,
    hoisted_locations: Vec<(usize, usize)>,
    removal_targets: Vec<RemovalTarget>,
    dropped_duplicates: Vec<ToolRepairEntry>,
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

fn truthy_id(value: Option<&Value>) -> Option<&str> {
    value.and_then(Value::as_str).filter(|id| !id.is_empty())
}

fn is_truthy(value: Option<&Value>) -> bool {
    match value {
        None | Some(Value::Null) | Some(Value::Bool(false)) => false,
        Some(Value::Bool(true)) | Some(Value::Array(_)) | Some(Value::Object(_)) => true,
        Some(Value::Number(number)) => number.as_f64().is_some_and(|number| number != 0.0),
        Some(Value::String(value)) => !value.is_empty(),
    }
}

/// Read one model turn and the consecutive user turns after it. The scan is
/// deliberately read-only; mutation happens only after a complete plan has
/// been computed.
fn scan_model_turn(history: &[Value], model_idx: usize) -> ScanResult {
    let mut expected: Vec<(String, String)> = Vec::new();
    for part in parts(&history[model_idx]) {
        let Some(call) = part.get("functionCall") else {
            continue;
        };
        let Some(call_id) = truthy_id(call.get("id")) else {
            continue;
        };
        let name = call
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_owned();
        if let Some((_, existing_name)) = expected.iter_mut().find(|(id, _)| id == call_id) {
            *existing_name = name;
        } else {
            expected.push((call_id.to_owned(), name));
        }
    }

    let mut matched: HashMap<String, Vec<FunctionResponseLocation>> = HashMap::new();
    let mut scan_idx = model_idx + 1;
    while scan_idx < history.len() && role(&history[scan_idx]) == Some("user") {
        for (part_idx, part) in parts(&history[scan_idx]).iter().enumerate() {
            let Some(response_id) = part
                .get("functionResponse")
                .and_then(|response| truthy_id(response.get("id")))
            else {
                continue;
            };
            matched
                .entry(response_id.to_owned())
                .or_default()
                .push(FunctionResponseLocation {
                    turn_idx: scan_idx,
                    part_idx,
                });
        }
        scan_idx += 1;
    }

    ScanResult {
        model_idx,
        expected,
        matched,
        scan_end: scan_idx,
    }
}

/// Classify missing results, non-adjacent real results, and duplicates.
fn plan_repair(scan: ScanResult) -> RepairPlan {
    let mut synthesize = Vec::new();
    let mut hoisted_locations = Vec::new();
    let mut removal_targets = Vec::new();
    let mut dropped_duplicates = Vec::new();
    let adjacent_idx = scan.model_idx + 1;

    for (call_id, name) in &scan.expected {
        let Some(locations) = scan.matched.get(call_id).filter(|items| !items.is_empty()) else {
            synthesize.push((call_id.clone(), name.clone()));
            continue;
        };

        let survivor = &locations[0];
        if survivor.turn_idx != adjacent_idx {
            hoisted_locations.push((survivor.turn_idx, survivor.part_idx));
            removal_targets.push(RemovalTarget {
                turn_idx: survivor.turn_idx,
                part_idx: survivor.part_idx,
            });
        }
        for duplicate in locations.iter().skip(1) {
            removal_targets.push(RemovalTarget {
                turn_idx: duplicate.turn_idx,
                part_idx: duplicate.part_idx,
            });
            dropped_duplicates.push(ToolRepairEntry {
                call_id: call_id.clone(),
                name: name.clone(),
            });
        }
    }

    RepairPlan {
        model_idx: scan.model_idx,
        scan_end: scan.scan_end,
        synthesize,
        hoisted_locations,
        removal_targets,
        dropped_duplicates,
    }
}

fn apply_repair(history: &mut Vec<Value>, plan: &RepairPlan, reason: &str) -> usize {
    if plan.synthesize.is_empty() && plan.removal_targets.is_empty() {
        return 0;
    }

    let synthetic_parts: Vec<Value> = plan
        .synthesize
        .iter()
        .map(|(call_id, name)| {
            json!({
                "functionResponse": {
                    "id": call_id,
                    "name": name,
                    "response": {"error": reason}
                }
            })
        })
        .collect();
    // Remove parts in descending location order so earlier part indices in a
    // turn continue to refer to their original elements. Move hoisted values
    // out of history instead of cloning potentially large tool outputs.
    let mut removals: Vec<(usize, usize, bool)> = plan
        .removal_targets
        .iter()
        .map(|target| {
            (
                target.turn_idx,
                target.part_idx,
                plan.hoisted_locations
                    .contains(&(target.turn_idx, target.part_idx)),
            )
        })
        .collect();
    removals.sort_unstable_by_key(|target| std::cmp::Reverse((target.0, target.1)));
    let mut moved_parts = HashMap::new();
    for (turn_idx, part_idx, is_hoisted) in removals {
        let Some(turn_parts) = history
            .get_mut(turn_idx)
            .and_then(|turn| turn.get_mut("parts"))
            .and_then(Value::as_array_mut)
        else {
            continue;
        };
        if part_idx < turn_parts.len() {
            let removed = turn_parts.remove(part_idx);
            if is_hoisted {
                moved_parts.insert((turn_idx, part_idx), removed);
            }
        }
    }
    let mut parts_to_inject = synthetic_parts;
    parts_to_inject.extend(
        plan.hoisted_locations
            .iter()
            .filter_map(|location| moved_parts.remove(location)),
    );

    // Remove later user entries that became empty after moving their only
    // response. Keep the adjacent entry, which receives the repaired parts.
    let adjacent_idx = plan.model_idx + 1;
    let cleanup_start = adjacent_idx.saturating_add(1);
    let cleanup_end = plan.scan_end.min(history.len());
    if cleanup_start < cleanup_end {
        for idx in (cleanup_start..cleanup_end).rev() {
            if role(&history[idx]) == Some("user") && parts(&history[idx]).is_empty() {
                history.remove(idx);
            }
        }
    }

    // Function responses must lead the adjacent user message. This includes
    // pre-existing real results, then synthesized results, then hoisted ones.
    if history.get(adjacent_idx).and_then(role) == Some("user") {
        let existing = history[adjacent_idx]
            .get_mut("parts")
            .and_then(Value::as_array_mut)
            .map(std::mem::take)
            .unwrap_or_default();
        let insert_at = existing
            .iter()
            .position(|part| !is_truthy(part.get("functionResponse")))
            .unwrap_or(existing.len());
        let mut updated = existing;
        updated.splice(insert_at..insert_at, parts_to_inject);
        history[adjacent_idx]["parts"] = Value::Array(updated);
        0
    } else {
        history.insert(
            adjacent_idx,
            json!({"role":"user", "parts":parts_to_inject}),
        );
        1
    }
}

/// Repair the persisted history using Canopy's default interruption reason.
pub fn repair_orphaned_tool_use_turns(history: &mut Vec<Value>) -> ToolRepairResult {
    repair_orphaned_tool_use_turns_with_reason(history, ORPHAN_TOOL_USE_REPAIR_REASON)
}

/// Repair persisted history in place and return synthetic and duplicate IDs.
/// Hoisted real results are not reported as injected because their payloads
/// already existed in history.
pub fn repair_orphaned_tool_use_turns_with_reason(
    history: &mut Vec<Value>,
    reason: &str,
) -> ToolRepairResult {
    let mut result = ToolRepairResult::default();
    let mut idx = 0;
    while idx < history.len() {
        if role(&history[idx]) != Some("model") {
            idx += 1;
            continue;
        }

        let scan = scan_model_turn(history, idx);
        if scan.expected.is_empty() {
            idx += 1;
            continue;
        }
        let plan = plan_repair(scan);
        if plan.synthesize.is_empty() && plan.removal_targets.is_empty() {
            idx += 1;
            continue;
        }

        let inserted_before = apply_repair(history, &plan, reason);
        result.injected.extend(
            plan.synthesize
                .iter()
                .map(|(call_id, name)| ToolRepairEntry {
                    call_id: call_id.clone(),
                    name: name.clone(),
                }),
        );
        result
            .dropped_duplicates
            .extend(plan.dropped_duplicates.iter().cloned());
        idx += 1 + inserted_before;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn function_call(id: &str, name: &str) -> Value {
        json!({"functionCall":{"id":id,"name":name,"args":{}}})
    }

    fn function_response(id: &str, response: Value) -> Value {
        json!({"functionResponse":{"id":id,"name":"read_file","response":response}})
    }

    #[test]
    fn synthesizes_a_missing_trailing_response() {
        let mut history = vec![json!({"role":"model","parts":[function_call("c1", "read_file")]})];
        let result = repair_orphaned_tool_use_turns(&mut history);
        assert_eq!(
            result.injected,
            vec![ToolRepairEntry {
                call_id: "c1".into(),
                name: "read_file".into()
            }]
        );
        assert_eq!(history[1]["parts"][0]["functionResponse"]["id"], "c1");
        assert!(
            history[1]["parts"][0]["functionResponse"]["response"]["error"]
                .as_str()
                .unwrap()
                .contains("interrupted")
        );
    }

    #[test]
    fn synthetic_results_are_hoisted_before_retry_text() {
        let mut history = vec![
            json!({"role":"model","parts":[function_call("c1", "read_file")]}),
            json!({"role":"user","parts":[{"text":"retry"}]}),
        ];
        repair_orphaned_tool_use_turns(&mut history);
        assert_eq!(history[1]["parts"][0]["functionResponse"]["id"], "c1");
        assert_eq!(history[1]["parts"][1], json!({"text":"retry"}));
    }

    #[test]
    fn synthesis_follows_real_results_and_precedes_text() {
        let mut history = vec![
            json!({"role":"model","parts":[function_call("a", "read_file"), function_call("b", "read_file")]}),
            json!({"role":"user","parts":[function_response("a", json!({"output":"real"})), {"text":"retry"}]}),
        ];
        repair_orphaned_tool_use_turns(&mut history);
        let result_ids: Vec<&str> = history[1]["parts"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|part| part.get("functionResponse")?.get("id")?.as_str())
            .collect();
        assert_eq!(result_ids, vec!["a", "b"]);
        assert_eq!(history[1]["parts"][2], json!({"text":"retry"}));
    }

    #[test]
    fn parallel_partial_responses_keep_real_payloads_and_synthesize_only_missing() {
        let mut history = vec![
            json!({"role":"model","parts":[function_call("a", "read_file"), function_call("b", "read_file"), function_call("c", "read_file")]}),
            json!({"role":"user","parts":[function_response("a", json!({"output":"real a"}))]}),
        ];
        let result = repair_orphaned_tool_use_turns(&mut history);
        assert_eq!(
            result
                .injected
                .iter()
                .map(|entry| entry.call_id.as_str())
                .collect::<Vec<_>>(),
            vec!["b", "c"]
        );
        assert_eq!(
            history[1]["parts"][0]["functionResponse"]["response"]["output"],
            "real a"
        );
        assert_eq!(history[1]["parts"][1]["functionResponse"]["id"], "b");
        assert_eq!(history[1]["parts"][2]["functionResponse"]["id"], "c");
    }

    #[test]
    fn already_paired_history_is_unchanged() {
        let mut history = vec![
            json!({"role":"model","parts":[function_call("c1", "read_file")]}),
            json!({"role":"user","parts":[function_response("c1", json!({"output":"ok"}))]}),
        ];
        let original = history.clone();
        let result = repair_orphaned_tool_use_turns(&mut history);
        assert_eq!(result, ToolRepairResult::default());
        assert_eq!(history, original);
    }

    #[test]
    fn repairs_separate_dangling_model_turns_in_one_pass() {
        let mut history = vec![
            json!({"role":"model","parts":[function_call("early", "glob")]}),
            json!({"role":"user","parts":[{"text":"next"}]}),
            json!({"role":"model","parts":[function_call("late", "read_file")]}),
        ];
        let result = repair_orphaned_tool_use_turns(&mut history);
        assert_eq!(result.injected.len(), 2);
        assert_eq!(history.len(), 4);
        assert_eq!(history[1]["parts"][0]["functionResponse"]["id"], "early");
        assert_eq!(history[3]["parts"][0]["functionResponse"]["id"], "late");
    }

    #[test]
    fn model_turn_without_calls_is_unchanged() {
        let mut history = vec![json!({"role":"model","parts":[{"text":"done"}]})];
        let original = history.clone();
        assert_eq!(
            repair_orphaned_tool_use_turns(&mut history),
            ToolRepairResult::default()
        );
        assert_eq!(history, original);
    }

    #[test]
    fn caller_reason_is_used_for_synthetic_error() {
        let mut history = vec![json!({"role":"model","parts":[function_call("c1", "read_file")]})];
        repair_orphaned_tool_use_turns_with_reason(&mut history, "custom reason");
        assert_eq!(
            history[1]["parts"][0]["functionResponse"]["response"]["error"],
            "custom reason"
        );
    }

    #[test]
    fn hoists_non_adjacent_real_result_and_removes_empty_source_turn() {
        let mut history = vec![
            json!({"role":"model","parts":[function_call("c1", "read_file")]}),
            json!({"role":"user","parts":[{"text":"follow up"}]}),
            json!({"role":"user","parts":[function_response("c1", json!({"output":"real"}))]}),
        ];
        let result = repair_orphaned_tool_use_turns(&mut history);
        assert!(result.injected.is_empty());
        assert_eq!(history.len(), 2);
        assert_eq!(
            history[1]["parts"][0]["functionResponse"]["response"]["output"],
            "real"
        );
        assert_eq!(history[1]["parts"][1], json!({"text":"follow up"}));
    }

    #[test]
    fn synthesizes_missing_and_hoists_real_result_for_parallel_calls() {
        let mut history = vec![
            json!({"role":"model","parts":[function_call("a", "read_file"), function_call("b", "read_file")]}),
            json!({"role":"user","parts":[{"text":"follow up"}]}),
            json!({"role":"user","parts":[function_response("a", json!({"output":"real a"}))]}),
        ];
        let result = repair_orphaned_tool_use_turns(&mut history);
        assert_eq!(
            result
                .injected
                .iter()
                .map(|entry| entry.call_id.as_str())
                .collect::<Vec<_>>(),
            vec!["b"]
        );
        assert_eq!(history.len(), 2);
        assert_eq!(history[1]["parts"][0]["functionResponse"]["id"], "b");
        assert_eq!(history[1]["parts"][1]["functionResponse"]["id"], "a");
        assert_eq!(history[1]["parts"][2], json!({"text":"follow up"}));
    }

    #[test]
    fn preserves_non_response_content_in_source_turn() {
        let mut history = vec![
            json!({"role":"model","parts":[function_call("c1", "read_file")]}),
            json!({"role":"user","parts":[{"text":"follow up"}]}),
            json!({"role":"user","parts":[function_response("c1", json!({"output":"real"})), {"text":"keep this"}]}),
        ];
        repair_orphaned_tool_use_turns(&mut history);
        assert_eq!(history.len(), 3);
        assert_eq!(history[1]["parts"][0]["functionResponse"]["id"], "c1");
        assert_eq!(history[2]["parts"], json!([{"text":"keep this"}]));
    }

    #[test]
    fn keeps_first_real_result_and_drops_later_duplicates() {
        let response = function_response("c1", json!({"output":"real"}));
        let mut history = vec![
            json!({"role":"model","parts":[function_call("c1", "read_file")]}),
            json!({"role":"user","parts":[{"text":"follow up"}]}),
            json!({"role":"user","parts":[response.clone()]}),
            json!({"role":"user","parts":[response]}),
        ];
        let result = repair_orphaned_tool_use_turns(&mut history);
        assert_eq!(
            result.dropped_duplicates,
            vec![ToolRepairEntry {
                call_id: "c1".into(),
                name: "read_file".into()
            }]
        );
        assert_eq!(history.len(), 2);
        assert_eq!(history[1]["parts"][0]["functionResponse"]["id"], "c1");
        assert_eq!(history[1]["parts"][1], json!({"text":"follow up"}));
    }

    #[test]
    fn drops_duplicate_even_when_first_real_result_is_adjacent() {
        let response = function_response("c1", json!({"output":"real"}));
        let mut history = vec![
            json!({"role":"model","parts":[function_call("c1", "read_file")]}),
            json!({"role":"user","parts":[response.clone()]}),
            json!({"role":"user","parts":[response, {"text":"follow up"}]}),
        ];
        let result = repair_orphaned_tool_use_turns(&mut history);
        assert_eq!(result.injected, Vec::<ToolRepairEntry>::new());
        assert_eq!(result.dropped_duplicates.len(), 1);
        assert_eq!(history.len(), 3);
        assert_eq!(history[1]["parts"].as_array().unwrap().len(), 1);
        assert_eq!(history[2]["parts"], json!([{"text":"follow up"}]));
    }
}
