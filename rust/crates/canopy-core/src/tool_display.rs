//! Bounded tool-result previews matching Canopy's
//! `utils/toolResultDisplayCompaction.ts` and recorder sanitization rules.

use serde_json::{Map, Value};

pub const MAX_RETAINED_TOOL_RESULT_DISPLAY_CHARS: usize = 32_000;
pub const MAX_RETAINED_AGENT_FIELD_CHARS: usize = 8_000;
pub const MAX_RETAINED_FILE_DIFF_CHARS: usize = 50_000;
pub const MAX_RETAINED_FILE_CONTENT_CHARS: usize = 16_000;
pub const MAX_RETAINED_ANSI_OUTPUT_LINES: usize = 200;
const SESSION_FILE_DIFF_AGGREGATE_CHAR_LIMIT: usize = 100_000;

#[derive(Clone, Copy)]
enum Purpose {
    History,
    Recording,
}

fn utf16_len(value: &str) -> usize {
    value.encode_utf16().count()
}

fn byte_index_at_utf16(value: &str, target: usize, head: bool) -> usize {
    let mut units = 0usize;
    for (byte_index, character) in value.char_indices() {
        let next = units + character.len_utf16();
        if units == target {
            return byte_index;
        }
        if units < target && target < next {
            // `head` keeps the high/low pair out of the head. A tail start
            // moves past the pair, matching the JS safeTailStart helper.
            return if head {
                byte_index
            } else {
                byte_index + character.len_utf8()
            };
        }
        units = next;
    }
    value.len()
}

fn marker(value: &str, purpose: Purpose) -> String {
    let length = utf16_len(value);
    match purpose {
        Purpose::Recording => format!(
            "\n[... truncated for saved session preview; original length: {length} characters ...]\n"
        ),
        Purpose::History => {
            format!("\n[... truncated from {length} characters for CLI history display ...]\n")
        }
    }
}

fn compact_string(value: String, purpose: Purpose, limit: usize) -> String {
    let length = utf16_len(&value);
    if length <= limit {
        return value;
    }
    let marker = marker(&value, purpose);
    let marker_length = utf16_len(&marker);
    if marker_length >= limit {
        let end = byte_index_at_utf16(&value, limit, true);
        return value[..end].to_owned();
    }
    let content_budget = limit - marker_length;
    let head_units = (content_budget * 3).div_ceil(5);
    let tail_units = content_budget - head_units;
    let head_end = byte_index_at_utf16(&value, head_units, true);
    let tail_start = byte_index_at_utf16(&value, length.saturating_sub(tail_units), false);
    let mut output = String::with_capacity(head_end + marker.len() + value.len() - tail_start);
    output.push_str(&value[..head_end]);
    output.push_str(&marker);
    output.push_str(&value[tail_start..]);
    output
}

fn truncate_middle_for_session(value: String, limit: usize) -> String {
    let length = utf16_len(&value);
    if length <= limit {
        return value;
    }
    let marker = format!(
        "\n[... truncated for saved session preview; original length: {length} characters ...]\n"
    );
    let marker_length = utf16_len(&marker);
    let content_budget = limit.saturating_sub(marker_length);
    let head_units = (content_budget * 3).div_ceil(5);
    let tail_units = content_budget - head_units;
    let head_end = byte_index_at_utf16(&value, head_units, true);
    let tail_start = byte_index_at_utf16(&value, length.saturating_sub(tail_units), false);
    let mut output = String::with_capacity(head_end + marker.len() + value.len() - tail_start);
    output.push_str(&value[..head_end]);
    output.push_str(&marker);
    output.push_str(&value[tail_start..]);
    output
}

pub fn compact_string_for_history(value: String, limit: usize) -> String {
    compact_string(value, Purpose::History, limit)
}

pub fn compact_string_for_recording(value: String, limit: usize) -> String {
    compact_string(value, Purpose::Recording, limit)
}

fn object_mut(value: &mut Value) -> Option<&mut Map<String, Value>> {
    value.as_object_mut()
}

fn take_string(map: &mut Map<String, Value>, key: &str) -> Option<String> {
    match map.remove(key) {
        Some(Value::String(value)) => Some(value),
        Some(other) => {
            map.insert(key.to_owned(), other);
            None
        }
        None => None,
    }
}

fn compact_field(map: &mut Map<String, Value>, key: &str, purpose: Purpose, limit: usize) {
    if let Some(value) = take_string(map, key) {
        map.insert(
            key.to_owned(),
            Value::String(compact_string(value, purpose, limit)),
        );
    }
}

fn is_file_diff(value: &Value) -> bool {
    value.as_object().is_some_and(is_file_diff_map)
}

fn is_file_diff_map(map: &Map<String, Value>) -> bool {
    map.get("fileDiff").is_some_and(Value::is_string)
        && map.get("fileName").is_some_and(Value::is_string)
        && map
            .get("originalContent")
            .is_some_and(|value| value.is_null() || value.is_string())
        && map.get("newContent").is_some_and(Value::is_string)
}

/// Apply the recorder's file-diff preview policy. Oversized full diffs are
/// replaced with a synthetic summary; source and result snippets retain their
/// beginning and end around a truncation marker.
pub fn sanitize_file_diff_for_recording(mut display: Value) -> Value {
    if !is_file_diff(&display) {
        return display;
    }
    let Some(map) = object_mut(&mut display) else {
        return display;
    };
    let file_diff_length = map["fileDiff"].as_str().map(utf16_len).unwrap_or_default();
    let original_content_length = map["originalContent"]
        .as_str()
        .map(utf16_len)
        .unwrap_or_default();
    let new_content_length = map["newContent"]
        .as_str()
        .map(utf16_len)
        .unwrap_or_default();
    let file_diff_truncated = file_diff_length > MAX_RETAINED_FILE_DIFF_CHARS;
    let original_content_truncated = original_content_length > MAX_RETAINED_FILE_CONTENT_CHARS;
    let new_content_truncated = new_content_length > MAX_RETAINED_FILE_CONTENT_CHARS;
    if file_diff_length + original_content_length + new_content_length
        <= SESSION_FILE_DIFF_AGGREGATE_CHAR_LIMIT
        && !file_diff_truncated
        && !original_content_truncated
        && !new_content_truncated
    {
        return display;
    }

    if file_diff_truncated {
        let file_name = map["fileName"].as_str().unwrap_or_default().to_owned();
        let original_length = original_content_length;
        let new_length = new_content_length;
        let preview = format!(
            "--- {file_name}\n+++ {file_name}\n@@ -1 +1 @@\n-Full diff omitted from saved session history; original fileDiff length: {file_diff_length} characters.\n+Saved session preview only; originalContent length: {original_length} characters, newContent length: {new_length} characters."
        );
        map.insert("fileDiff".to_owned(), Value::String(preview));
    }
    if original_content_truncated {
        if let Some(original) = take_string(map, "originalContent") {
            map.insert(
                "originalContent".to_owned(),
                Value::String(truncate_middle_for_session(
                    original,
                    MAX_RETAINED_FILE_CONTENT_CHARS,
                )),
            );
        }
    }
    if new_content_truncated {
        if let Some(new_content) = take_string(map, "newContent") {
            map.insert(
                "newContent".to_owned(),
                Value::String(truncate_middle_for_session(
                    new_content,
                    MAX_RETAINED_FILE_CONTENT_CHARS,
                )),
            );
        }
    }
    map.insert("truncatedForSession".to_owned(), Value::Bool(true));
    map.insert("fileDiffLength".to_owned(), Value::from(file_diff_length));
    map.insert(
        "originalContentLength".to_owned(),
        Value::from(original_content_length),
    );
    map.insert(
        "newContentLength".to_owned(),
        Value::from(new_content_length),
    );
    map.insert(
        "fileDiffTruncated".to_owned(),
        Value::Bool(file_diff_truncated),
    );
    map.insert(
        "originalContentTruncated".to_owned(),
        Value::Bool(original_content_truncated),
    );
    map.insert(
        "newContentTruncated".to_owned(),
        Value::Bool(new_content_truncated),
    );
    display
}

fn compact_ansi_line(mut line: Value, purpose: Purpose) -> Value {
    if let Some(tokens) = line.as_array_mut() {
        for token in tokens {
            if let Some(map) = token.as_object_mut() {
                compact_field(map, "text", purpose, MAX_RETAINED_TOOL_RESULT_DISPLAY_CHARS);
            }
        }
    }
    line
}

fn marker_ansi_line(text: String) -> Value {
    Value::Array(vec![Value::Object(serde_json::Map::from_iter([
        ("text".to_owned(), Value::String(text)),
        ("bold".to_owned(), Value::Bool(false)),
        ("italic".to_owned(), Value::Bool(false)),
        ("underline".to_owned(), Value::Bool(false)),
        ("dim".to_owned(), Value::Bool(true)),
        ("inverse".to_owned(), Value::Bool(false)),
        ("fg".to_owned(), Value::String(String::new())),
        ("bg".to_owned(), Value::String(String::new())),
    ]))])
}

fn compact_ansi_output(mut display: Value, purpose: Purpose) -> Value {
    let Some(map) = object_mut(&mut display) else {
        return display;
    };
    let Some(output) = map.get_mut("ansiOutput").and_then(Value::as_array_mut) else {
        return display;
    };
    if output.len() > MAX_RETAINED_ANSI_OUTPUT_LINES {
        let omitted = output.len() - MAX_RETAINED_ANSI_OUTPUT_LINES + 1;
        let keep_from = output.len() - (MAX_RETAINED_ANSI_OUTPUT_LINES - 1);
        let tail = output.split_off(keep_from);
        let target = match purpose {
            Purpose::Recording => "saved session preview",
            Purpose::History => "CLI history display",
        };
        let marker = marker_ansi_line(format!(
            "[... {omitted} terminal lines omitted from {target} ...]"
        ));
        *output = std::iter::once(marker)
            .chain(
                tail.into_iter()
                    .map(|line| compact_ansi_line(line, purpose)),
            )
            .collect();
    } else {
        for line in output {
            *line = compact_ansi_line(std::mem::take(line), purpose);
        }
    }
    display
}

fn compact_agent_result(mut display: Value, purpose: Purpose) -> Value {
    let Some(map) = object_mut(&mut display) else {
        return display;
    };
    compact_field(
        map,
        "taskDescription",
        purpose,
        MAX_RETAINED_AGENT_FIELD_CHARS,
    );
    compact_field(map, "taskPrompt", purpose, MAX_RETAINED_AGENT_FIELD_CHARS);
    compact_field(
        map,
        "terminateReason",
        purpose,
        MAX_RETAINED_AGENT_FIELD_CHARS,
    );
    compact_field(
        map,
        "result",
        purpose,
        MAX_RETAINED_TOOL_RESULT_DISPLAY_CHARS,
    );
    if let Some(calls) = map.get_mut("toolCalls").and_then(Value::as_array_mut) {
        for call in calls {
            if let Some(call) = call.as_object_mut() {
                for key in ["args", "responseParts", "result"] {
                    call.remove(key);
                }
                compact_field(call, "description", purpose, MAX_RETAINED_AGENT_FIELD_CHARS);
                compact_field(call, "error", purpose, MAX_RETAINED_AGENT_FIELD_CHARS);
                compact_field(
                    call,
                    "resultDisplay",
                    purpose,
                    MAX_RETAINED_TOOL_RESULT_DISPLAY_CHARS,
                );
            }
        }
    }
    display
}

fn compact_typed_display(mut display: Value, purpose: Purpose) -> Value {
    let Some(map) = object_mut(&mut display) else {
        return display;
    };
    match map.get("type").and_then(Value::as_str) {
        Some("todo_list") => {
            if let Some(todos) = map.get_mut("todos").and_then(Value::as_array_mut) {
                for todo in todos {
                    if let Some(todo) = todo.as_object_mut() {
                        compact_field(todo, "content", purpose, MAX_RETAINED_AGENT_FIELD_CHARS);
                    }
                }
            }
        }
        Some("plan_summary") => {
            compact_field(map, "message", purpose, MAX_RETAINED_AGENT_FIELD_CHARS);
            compact_field(map, "plan", purpose, MAX_RETAINED_TOOL_RESULT_DISPLAY_CHARS);
        }
        Some("mcp_tool_progress") => {
            compact_field(map, "message", purpose, MAX_RETAINED_AGENT_FIELD_CHARS);
        }
        Some("team_result") => {
            compact_field(map, "teamName", purpose, MAX_RETAINED_AGENT_FIELD_CHARS);
        }
        Some("task_list") => {
            if let Some(tasks) = map.get_mut("tasks").and_then(Value::as_array_mut) {
                for task in tasks {
                    if let Some(task) = task.as_object_mut() {
                        compact_field(task, "subject", purpose, MAX_RETAINED_AGENT_FIELD_CHARS);
                        compact_field(task, "owner", purpose, MAX_RETAINED_AGENT_FIELD_CHARS);
                    }
                }
            }
        }
        _ => {}
    }
    display
}

pub fn compact_tool_result_display_for_history(display: Value) -> Value {
    compact_tool_result_display(display, Purpose::History)
}

pub fn compact_tool_result_display_for_recording(display: Value) -> Value {
    compact_tool_result_display(display, Purpose::Recording)
}

fn compact_tool_result_display(display: Value, purpose: Purpose) -> Value {
    match display {
        Value::String(value) => Value::String(compact_string(
            value,
            purpose,
            MAX_RETAINED_TOOL_RESULT_DISPLAY_CHARS,
        )),
        Value::Object(map) if is_file_diff_map(&map) => {
            // Generic history/recording compaction uses the same bounded
            // excerpts for FileDiff values. The recorder's synthetic preview
            // policy is exposed separately and used by SessionRecorder.
            let before_file_diff_length =
                map["fileDiff"].as_str().map(utf16_len).unwrap_or_default();
            let before_original_length = map["originalContent"]
                .as_str()
                .map(utf16_len)
                .unwrap_or_default();
            let before_new_length = map["newContent"]
                .as_str()
                .map(utf16_len)
                .unwrap_or_default();
            let file_diff_truncated = before_file_diff_length > MAX_RETAINED_FILE_DIFF_CHARS;
            let original_content_truncated =
                before_original_length > MAX_RETAINED_FILE_CONTENT_CHARS;
            let new_content_truncated = before_new_length > MAX_RETAINED_FILE_CONTENT_CHARS;
            if !file_diff_truncated && !original_content_truncated && !new_content_truncated {
                return Value::Object(map);
            }
            let mut display = Value::Object(map);
            let map = display.as_object_mut().unwrap();
            compact_field(map, "fileDiff", purpose, MAX_RETAINED_FILE_DIFF_CHARS);
            compact_field(
                map,
                "originalContent",
                purpose,
                MAX_RETAINED_FILE_CONTENT_CHARS,
            );
            compact_field(map, "newContent", purpose, MAX_RETAINED_FILE_CONTENT_CHARS);
            map.insert("truncatedForSession".to_owned(), Value::Bool(true));
            map.insert(
                "fileDiffLength".to_owned(),
                Value::from(before_file_diff_length),
            );
            map.insert(
                "originalContentLength".to_owned(),
                Value::from(before_original_length),
            );
            map.insert(
                "newContentLength".to_owned(),
                Value::from(before_new_length),
            );
            map.insert(
                "fileDiffTruncated".to_owned(),
                Value::Bool(file_diff_truncated),
            );
            map.insert(
                "originalContentTruncated".to_owned(),
                Value::Bool(original_content_truncated),
            );
            map.insert(
                "newContentTruncated".to_owned(),
                Value::Bool(new_content_truncated),
            );
            display
        }
        Value::Object(map) if map.get("type").and_then(Value::as_str) == Some("task_execution") => {
            compact_agent_result(Value::Object(map), purpose)
        }
        Value::Object(map) if map.contains_key("ansiOutput") && map["ansiOutput"].is_array() => {
            compact_ansi_output(Value::Object(map), purpose)
        }
        Value::Object(map) if map.contains_key("type") => {
            compact_typed_display(Value::Object(map), purpose)
        }
        other => other,
    }
}

/// Compact and remove recorder-only fields from an enriched tool result.
pub fn sanitize_tool_call_result_for_recording(mut result: Value) -> Value {
    let Some(map) = result.as_object_mut() else {
        return result;
    };
    map.remove("persistedOutputFiles");
    map.remove("boundaryArtifact");
    let Some(display) = map.remove("resultDisplay") else {
        return result;
    };
    let mut display = if is_file_diff(&display) {
        sanitize_file_diff_for_recording(display)
    } else {
        compact_tool_result_display_for_recording(display)
    };
    if display
        .as_object()
        .and_then(|display| display.get("type"))
        .and_then(Value::as_str)
        == Some("task_execution")
    {
        if let Some(tool_calls) = display
            .as_object_mut()
            .and_then(|display| display.get_mut("toolCalls"))
        {
            *tool_calls = Value::Array(Vec::new());
        }
    }
    map.insert("resultDisplay".to_owned(), display);
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn compacted_strings_use_utf16_lengths_and_the_requested_context_marker() {
        let input = format!("{}😀{}", "a".repeat(400), "z".repeat(400));
        let history = compact_string_for_history(input.clone(), 180);
        let recording = compact_string_for_recording(input, 180);
        assert!(utf16_len(&history) <= 180);
        assert!(utf16_len(&recording) <= 180);
        assert!(history.contains("CLI history display"));
        assert!(recording.contains("saved session preview"));
        assert!(!history.contains('\u{fffd}'));
    }

    #[test]
    fn compaction_keeps_exact_head_tail_budgets_around_surrogate_pairs() {
        let limit = 80;
        let source_units = 200;
        let marker = marker(&"x".repeat(source_units), Purpose::History);
        let content_budget = limit - utf16_len(&marker);
        let head_units = (content_budget * 3).div_ceil(5);
        let tail_units = content_budget - head_units;
        let tail_start = source_units - tail_units;

        // Put a supplementary-plane character across both raw cut points.
        // The source TypeScript backs the head cut up and the tail cut forward
        // to keep each UTF-16 surrogate pair intact.
        let mut units = vec![b'x' as u16; source_units];
        let emoji = [0xd83d, 0xde00];
        units[head_units - 1] = emoji[0];
        units[head_units] = emoji[1];
        units[tail_start - 1] = emoji[0];
        units[tail_start] = emoji[1];
        let value = String::from_utf16(&units).expect("test input is valid UTF-16");

        let compacted = compact_string_for_history(value, limit);
        let expected_head = String::from_utf16(&units[..head_units - 1])
            .expect("head stops before the first surrogate pair");
        let expected_tail = String::from_utf16(&units[tail_start + 1..])
            .expect("tail starts after the second surrogate pair");

        assert_eq!(compacted, format!("{expected_head}{marker}{expected_tail}"));
        assert!(utf16_len(&compacted) <= limit);
        assert!(!compacted.contains('😀'));
    }

    #[test]
    fn custom_string_limits_respect_marker_fit_and_surrogate_safe_hard_cuts() {
        for purpose in [Purpose::History, Purpose::Recording] {
            let value = "😀".repeat(40);
            for limit in [8, 9] {
                let compacted = compact_string(value.clone(), purpose, limit);
                assert!(utf16_len(&compacted) <= limit);
                assert_eq!(compacted, "😀".repeat(4));
                assert!(!compacted.contains("truncated"));
            }

            let hard_cut = compact_string("x".repeat(200), purpose, 10);
            assert_eq!(utf16_len(&hard_cut), 10);
            assert!(!hard_cut.contains("truncated"));

            let with_marker = compact_string("x".repeat(5_000), purpose, 500);
            assert!(utf16_len(&with_marker) <= 500);
            assert!(with_marker.contains("truncated"));
        }
    }

    #[test]
    fn file_diff_compaction_preserves_metadata_and_uses_utf16_lengths() {
        let original = json!({
            "fileName": "large.rs",
            "fileDiff": "😀".repeat(25_001),
            "originalContent": null,
            "newContent": "😎".repeat(8_001),
            "diffStat": {
                "model_added_lines": 2,
                "model_removed_lines": 1,
                "model_added_chars": 8,
                "model_removed_chars": 4,
                "user_added_lines": 0,
                "user_removed_lines": 0,
                "user_added_chars": 0,
                "user_removed_chars": 0
            }
        });

        let compacted = compact_tool_result_display_for_history(original);

        assert_eq!(compacted["fileName"], "large.rs");
        assert_eq!(compacted["originalContent"], Value::Null);
        assert_eq!(compacted["fileDiffLength"], 50_002);
        assert_eq!(compacted["originalContentLength"], 0);
        assert_eq!(compacted["newContentLength"], 16_002);
        assert_eq!(compacted["fileDiffTruncated"], true);
        assert_eq!(compacted["originalContentTruncated"], false);
        assert_eq!(compacted["newContentTruncated"], true);
        assert!(utf16_len(compacted["fileDiff"].as_str().unwrap()) <= MAX_RETAINED_FILE_DIFF_CHARS);
        assert!(
            utf16_len(compacted["newContent"].as_str().unwrap()) <= MAX_RETAINED_FILE_CONTENT_CHARS
        );
        assert_eq!(
            compacted["diffStat"],
            json!({
                "model_added_lines": 2,
                "model_removed_lines": 1,
                "model_added_chars": 8,
                "model_removed_chars": 4,
                "user_added_lines": 0,
                "user_removed_lines": 0,
                "user_added_chars": 0,
                "user_removed_chars": 0
            })
        );
    }

    #[test]
    fn agent_compaction_prunes_only_source_fields_and_compacts_nested_fields() {
        let display = json!({
            "type": "task_execution",
            "taskDescription": "d".repeat(MAX_RETAINED_AGENT_FIELD_CHARS + 10),
            "taskPrompt": "prompt",
            "status": "completed",
            "toolCalls": [{
                "callId": "call-1",
                "name": "read_file",
                "status": "success",
                "args": {"payload": "large"},
                "responseParts": [{"text": "large"}],
                "result": "large",
                "boundaryArtifact": {"state": "reusable", "kinds": ["file"]},
                "description": "x".repeat(MAX_RETAINED_AGENT_FIELD_CHARS + 10),
                "error": "failure",
                "resultDisplay": "r".repeat(MAX_RETAINED_TOOL_RESULT_DISPLAY_CHARS + 10)
            }]
        });

        let compacted = compact_tool_result_display_for_history(display);
        let call = &compacted["toolCalls"][0];

        assert!(
            compacted["taskDescription"]
                .as_str()
                .unwrap()
                .contains("truncated")
        );
        assert!(call.get("args").is_none());
        assert!(call.get("responseParts").is_none());
        assert!(call.get("result").is_none());
        assert_eq!(call["boundaryArtifact"]["state"], "reusable");
        assert!(call["description"].as_str().unwrap().contains("truncated"));
        assert_eq!(call["error"], "failure");
        assert!(
            call["resultDisplay"]
                .as_str()
                .unwrap()
                .contains("truncated")
        );
        assert_eq!(call["callId"], "call-1");
        assert_eq!(call["name"], "read_file");
    }

    #[test]
    fn recorder_file_diff_uses_synthetic_preview_and_retains_source_edges() {
        let compacted = sanitize_file_diff_for_recording(json!({
            "fileDiff":"d".repeat(MAX_RETAINED_FILE_DIFF_CHARS + 1),
            "fileName":"large.rs",
            "originalContent":format!("head{}tail", "o".repeat(MAX_RETAINED_FILE_CONTENT_CHARS)),
            "newContent":format!("start{}end", "n".repeat(MAX_RETAINED_FILE_CONTENT_CHARS))
        }));
        assert!(compacted["truncatedForSession"].as_bool().unwrap());
        assert!(
            compacted["fileDiff"]
                .as_str()
                .unwrap()
                .contains("Full diff omitted")
        );
        assert!(
            compacted["originalContent"]
                .as_str()
                .unwrap()
                .starts_with("head")
        );
        assert!(
            compacted["originalContent"]
                .as_str()
                .unwrap()
                .ends_with("tail")
        );
        assert_eq!(
            compacted["fileDiffLength"],
            MAX_RETAINED_FILE_DIFF_CHARS + 1
        );
    }

    #[test]
    fn agent_and_ansi_previews_drop_heavy_fields_and_old_output() {
        let task = sanitize_tool_call_result_for_recording(json!({
            "resultDisplay": {
                "type":"task_execution",
                "taskDescription":"task",
                "taskPrompt":"prompt",
                "toolCalls":[{"callId":"call-1","args":{"large":"x"},"responseParts":[{"text":"large"}],"result":"large","description":"nested"}]
            },
            "persistedOutputFiles":["/tmp/result"],
            "boundaryArtifact":{"state":"reusable"}
        }));
        assert!(task.get("persistedOutputFiles").is_none());
        assert!(task.get("boundaryArtifact").is_none());
        assert_eq!(task["resultDisplay"]["toolCalls"], json!([]));

        let ansi = compact_tool_result_display_for_history(json!({
            "ansiOutput": (0..MAX_RETAINED_ANSI_OUTPUT_LINES + 4)
                .map(|index| vec![json!({"text":format!("line-{index}")})])
                .collect::<Vec<_>>()
        }));
        let lines = ansi["ansiOutput"].as_array().unwrap();
        assert_eq!(lines.len(), MAX_RETAINED_ANSI_OUTPUT_LINES);
        assert!(lines[0][0]["text"].as_str().unwrap().contains("omitted"));
        assert_eq!(lines.last().unwrap()[0]["text"], "line-203");
    }
}
