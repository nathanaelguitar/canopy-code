use regex::Regex;
use serde_json::{Value, json};
use std::io::{self, Read};
use std::sync::LazyLock;

const MAX_HOOK_INPUT_BYTES: usize = 1024 * 1024;
const REMEMBER_TOOL_NAME: &str = "mcp__external-context__context_remember";
const INVALID_REASON: &str = "External context memory write confirmation request is invalid.";
const ASK_PERMISSION_MODES: &[&str] = &["default", "auto", "auto_edit", "auto-edit", "yolo"];

static FORMAT_CHARACTER: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\p{Cf}$").expect("valid Unicode category"));

fn main() {
    let input = read_input().and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok());
    let output = match input {
        Some(value) => run_write_confirmation(&value),
        None => deny(INVALID_REASON),
    };
    println!("{}", output);
}

fn read_input() -> Option<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut bounded_stdin = io::stdin().take((MAX_HOOK_INPUT_BYTES + 1) as u64);
    bounded_stdin.read_to_end(&mut bytes).ok()?;
    (bytes.len() <= MAX_HOOK_INPUT_BYTES).then_some(bytes)
}

fn run_write_confirmation(value: &Value) -> Value {
    let Some(object) = value.as_object() else {
        return deny(INVALID_REASON);
    };

    if object.get("hook_event_name").and_then(Value::as_str) != Some("PreToolUse")
        || object.get("tool_name").and_then(Value::as_str) != Some(REMEMBER_TOOL_NAME)
    {
        return json!({});
    }

    let permission_mode = object.get("permission_mode").and_then(Value::as_str);
    let tool_input = object.get("tool_input").and_then(Value::as_object);
    let content = tool_input
        .and_then(|input| input.get("content"))
        .and_then(Value::as_str);

    let (Some(permission_mode), Some(content)) = (permission_mode, content) else {
        return deny(INVALID_REASON);
    };
    if !is_valid_memory_content(content) {
        return deny(INVALID_REASON);
    }
    if permission_mode == "plan" {
        return deny("External context memory writes are not allowed in plan mode.");
    }
    if !ASK_PERMISSION_MODES.contains(&permission_mode) {
        return deny("External context memory write permission mode is unsupported.");
    }

    let reason = format!(
        "Save this exact content to the bound Mem0 repository memory?\n{}",
        render_memory_content_for_confirmation(content)
    );
    json!({
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "ask",
            "permissionDecisionReason": reason
        }
    })
}

fn deny(reason: &str) -> Value {
    json!({
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "deny",
            "permissionDecisionReason": reason
        }
    })
}

fn is_valid_memory_content(content: &str) -> bool {
    let mut has_visible = false;
    let mut characters = 0usize;
    for character in content.chars() {
        characters += 1;
        if characters > 4_000 {
            return false;
        }
        if !is_non_content_character(character) {
            has_visible = true;
        }
    }
    has_visible
}

fn is_non_content_character(character: char) -> bool {
    character.is_whitespace()
        || character.is_control()
        || FORMAT_CHARACTER.is_match(&character.to_string())
}

fn render_memory_content_for_confirmation(content: &str) -> String {
    let json_string = serde_json::to_string(content).expect("serializing a string cannot fail");
    let mut escaped = String::with_capacity(json_string.len());
    for character in json_string.chars() {
        if should_display_escape(character) {
            for code_unit in character.encode_utf16(&mut [0; 2]).iter() {
                escaped.push_str(&format!("\\u{code_unit:04x}"));
            }
        } else {
            escaped.push(character);
        }
    }
    escaped
}

fn should_display_escape(character: char) -> bool {
    matches!(character as u32, 0x7f..=0x9f | 0x2028..=0x2029)
        || FORMAT_CHARACTER.is_match(&character.to_string())
}
