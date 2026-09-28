//! Pure conversion helpers for Claude Code subagent configuration.
//!
//! This mirrors `convertClaudeAgentConfig` and
//! `claudeBuildInToolsTransform` in `packages/core/src/extension/claude-converter.ts`.
//! Values that the TypeScript converter passes through without interpreting
//! (`hooks`, `mcpServers`, `skills`, and `disallowedTools`) remain arbitrary JSON.

use serde_json::{Map, Value};
use std::fmt;

/// Error produced when a non-empty Claude `tools` value cannot be iterated
/// like the array expected by the TypeScript converter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClaudeAgentConversionError {
    field: &'static str,
}

impl fmt::Display for ClaudeAgentConversionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Claude agent field `{}` must be an array when it has a non-zero length",
            self.field
        )
    }
}

impl std::error::Error for ClaudeAgentConversionError {}

/// Error produced when a Claude plugin configuration has no truthy name.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClaudePluginConfigError;

impl fmt::Display for ClaudePluginConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Claude plugin config must have name field")
    }
}

impl std::error::Error for ClaudePluginConfigError {}

/// Convert a Claude subagent configuration object to Canopy's subagent shape.
///
/// The function applies the same truthy checks as JavaScript for optional
/// scalar fields. `tools` is transformed using
/// [`claude_builtin_tools_transform`]. Hooks, MCP server overrides, skills,
/// and disallowed tools are copied as-is when the source converter's
/// truthiness/length condition includes them.
///
/// Missing `name` or `description` keys are omitted because JSON has no
/// representation for JavaScript's `undefined` object values; present values,
/// including `null`, are preserved.
pub fn convert_claude_agent_config(
    claude_agent: &Value,
) -> Result<Value, ClaudeAgentConversionError> {
    let mut canopy_agent = Map::new();

    if let Some(name) = claude_agent.get("name") {
        canopy_agent.insert("name".to_owned(), name.clone());
    }
    if let Some(description) = claude_agent.get("description") {
        canopy_agent.insert("description".to_owned(), description.clone());
    }

    copy_if_truthy(claude_agent, &mut canopy_agent, "color");
    copy_if_truthy(claude_agent, &mut canopy_agent, "systemPrompt");

    if let Some(tools) = claude_agent.get("tools") {
        if js_truthy(tools) && js_has_nonzero_length(tools) {
            let Some(tools) = tools.as_array() else {
                return Err(ClaudeAgentConversionError { field: "tools" });
            };
            canopy_agent.insert(
                "tools".to_owned(),
                Value::Array(claude_builtin_tools_transform(tools)),
            );
        }
    }

    copy_if_truthy(claude_agent, &mut canopy_agent, "model");

    if let Some(permission_mode) = claude_agent.get("permissionMode") {
        if js_truthy(permission_mode) {
            let mapped = match permission_mode.as_str() {
                Some("default" | "dontAsk") => Value::String("default".to_owned()),
                Some("plan") => Value::String("plan".to_owned()),
                Some("acceptEdits" | "auto") => Value::String("auto-edit".to_owned()),
                Some("bypassPermissions") => Value::String("yolo".to_owned()),
                _ => permission_mode.clone(),
            };
            canopy_agent.insert("approvalMode".to_owned(), mapped);
        }
    }

    copy_if_truthy(claude_agent, &mut canopy_agent, "hooks");
    copy_if_truthy(claude_agent, &mut canopy_agent, "mcpServers");
    copy_if_nonempty_length(claude_agent, &mut canopy_agent, "skills");
    copy_if_nonempty_length(claude_agent, &mut canopy_agent, "disallowedTools");

    Ok(Value::Object(canopy_agent))
}

/// Map Claude built-in tool names to Canopy built-in tool names.
///
/// `BashOutput` and `KillShell` are intentionally dropped. Unknown tools pass
/// through unchanged, matching the TypeScript converter.
pub fn claude_builtin_tools_transform(tools: &[Value]) -> Vec<Value> {
    let mut transformed_tools = Vec::with_capacity(tools.len());
    for tool in tools {
        let Some(name) = tool.as_str() else {
            // The TypeScript contract is `string[]`; if a caller violates that
            // contract at runtime, unknown values are still pushed through.
            transformed_tools.push(tool.clone());
            continue;
        };

        let mapped = match name {
            "AskUserQuestion" => Some("AskUserQuestion"),
            "Bash" => Some("Shell"),
            "BashOutput" | "KillShell" => None,
            "Edit" => Some("Edit"),
            "ExitPlanMode" => Some("ExitPlanMode"),
            "Glob" => Some("Glob"),
            "Grep" => Some("Grep"),
            "NotebookEdit" => Some("NotebookEdit"),
            "Read" => Some("ReadFile"),
            "Skill" => Some("Skill"),
            "Task" => Some("Task"),
            "TodoWrite" => Some("TodoList"),
            "WebFetch" => Some("WebFetch"),
            "WebSearch" => Some("WebSearch"),
            "Write" => Some("WriteFile"),
            "LS" => Some("ListFiles"),
            _ => Some(name),
        };

        if let Some(mapped) = mapped {
            transformed_tools.push(Value::String(mapped.to_owned()));
        }
    }
    transformed_tools
}

/// Normalize one Claude MCP server entry to Canopy's transport shape.
///
/// Claude's type: "http" URL transport becomes httpUrl; other string URL
/// transports remain url (SSE). For entries already shaped with command,
/// httpUrl, or tcp, non-SDK type values are removed. type: "sdk" is retained
/// in either shape. All other fields pass through unchanged.
pub fn normalize_claude_mcp_server(raw: &Value) -> Value {
    let already_shaped = ["command", "httpUrl", "tcp"]
        .iter()
        .any(|field| raw.get(field).is_some_and(js_truthy));

    if already_shaped {
        let server_type = raw.get("type");
        if server_type.is_none() || server_type.and_then(Value::as_str) == Some("sdk") {
            return raw.clone();
        }

        if let Some(server) = raw.as_object() {
            let mut normalized = server.clone();
            normalized.remove("type");
            return Value::Object(normalized);
        }
        return raw.clone();
    }

    let Some(url) = raw.get("url").filter(|value| value.is_string()) else {
        return raw.clone();
    };
    let mut normalized = raw.as_object().cloned().unwrap_or_default();
    normalized.remove("url");

    let server_type = raw.get("type");
    if server_type.and_then(Value::as_str) != Some("sdk") {
        normalized.remove("type");
    }
    let target_key = if server_type.and_then(Value::as_str) == Some("http") {
        "httpUrl"
    } else {
        "url"
    };
    normalized.insert(target_key.to_owned(), url.clone());
    Value::Object(normalized)
}

/// Convert a Claude plugin configuration to Canopy's extension config shape.
///
/// The TypeScript converter warns and skips MCP server and hook string paths;
/// this pure Rust projection likewise omits those unsupported path fields.
/// Arbitrary JSON hook, MCP, and LSP values are otherwise retained.
pub fn convert_claude_to_canopy_config(
    claude_config: &Value,
) -> Result<Value, ClaudePluginConfigError> {
    let Some(name) = claude_config.get("name").filter(|name| js_truthy(name)) else {
        return Err(ClaudePluginConfigError);
    };

    let mut canopy_config = Map::new();
    canopy_config.insert("name".to_owned(), name.clone());
    for field in ["version", "description", "lspServers"] {
        if let Some(value) = claude_config.get(field) {
            canopy_config.insert(field.to_owned(), value.clone());
        }
    }

    if let Some(mcp_servers) = claude_config
        .get("mcpServers")
        .filter(|servers| js_truthy(servers))
    {
        // A string denotes a file path in Claude config. The TypeScript
        // converter logs a warning and defers file loading to its package path;
        // this pure conversion helper cannot resolve that path.
        if !mcp_servers.is_string() {
            canopy_config.insert(
                "mcpServers".to_owned(),
                normalize_claude_mcp_servers(mcp_servers),
            );
        }
    }

    if let Some(hooks) = claude_config.get("hooks").filter(|hooks| js_truthy(hooks)) {
        // A string denotes a hook file path resolved by the package converter.
        if !hooks.is_string() {
            canopy_config.insert("hooks".to_owned(), hooks.clone());
        }
    }

    Ok(Value::Object(canopy_config))
}

fn normalize_claude_mcp_servers(servers: &Value) -> Value {
    let mut normalized = Map::new();
    match servers {
        Value::Object(servers) => {
            for (name, server) in servers {
                normalized.insert(name.clone(), normalize_claude_mcp_server(server));
            }
        }
        Value::Array(servers) => {
            for (index, server) in servers.iter().enumerate() {
                normalized.insert(index.to_string(), normalize_claude_mcp_server(server));
            }
        }
        // Object.entries on a truthy JSON primitive returns no entries.
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
    Value::Object(normalized)
}

fn copy_if_truthy(source: &Value, destination: &mut Map<String, Value>, key: &str) {
    if let Some(value) = source.get(key).filter(|value| js_truthy(value)) {
        destination.insert(key.to_owned(), value.clone());
    }
}

fn copy_if_nonempty_length(source: &Value, destination: &mut Map<String, Value>, key: &str) {
    if let Some(value) = source
        .get(key)
        .filter(|value| js_truthy(value) && js_has_nonzero_length(value))
    {
        destination.insert(key.to_owned(), value.clone());
    }
}

/// JavaScript truthiness for values representable in JSON.
fn js_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_f64().is_some_and(|number| number != 0.0),
        Value::String(value) => !value.is_empty(),
        // JavaScript treats even empty arrays and objects as truthy.
        Value::Array(_) | Value::Object(_) => true,
    }
}

/// Approximate the JavaScript `value.length > 0` check for JSON values.
fn js_has_nonzero_length(value: &Value) -> bool {
    match value {
        Value::Array(values) => !values.is_empty(),
        Value::String(value) => value.encode_utf16().count() > 0,
        Value::Object(value) => value
            .get("length")
            .is_some_and(|length| js_number(length).is_some_and(|number| number > 0.0)),
        Value::Null | Value::Bool(_) | Value::Number(_) => false,
    }
}

fn js_number(value: &Value) -> Option<f64> {
    match value {
        Value::Null => Some(0.0),
        Value::Bool(value) => Some(if *value { 1.0 } else { 0.0 }),
        Value::Number(value) => value.as_f64(),
        Value::String(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                Some(0.0)
            } else {
                trimmed.parse().ok()
            }
        }
        // Arrays and objects have JavaScript ToPrimitive behavior. They are
        // outside the typed Claude config shape, so avoid inventing coercions.
        Value::Array(_) | Value::Object(_) => None,
    }
}
