// Copyright 2026 Canopy Team
// SPDX-License-Identifier: Apache-2.0
//! Pure extension URL redaction and variable hydration helpers.

use std::fmt;
use std::sync::LazyLock;

use indexmap::IndexMap;
use regex::{Captures, Regex};
use serde_json::Value;

pub const REDACTED_URL_CREDENTIAL: &str = "***REDACTED***";

static URL_CREDENTIALS_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)(?-u:\b)([a-z][a-z0-9+.-]*://)(?:[^/\s]+@)+")
        .expect("URL credentials pattern is valid")
});
static UPLOAD_IDENTITY_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?-u:\b)upload:v1:[0-9a-fA-F-]+:").expect("upload identity pattern is valid")
});

/// Redact URL userinfo and opaque upload identities from extension source
/// strings before they are logged or shown to a user.
pub fn redact_url_credentials(source: &str) -> String {
    let without_upload_identity = UPLOAD_IDENTITY_PATTERN
        .replace_all(source, "upload:")
        .into_owned();
    URL_CREDENTIALS_PATTERN
        .replace_all(&without_upload_identity, |captures: &Captures<'_>| {
            format!("{}{}@", &captures[1], REDACTED_URL_CREDENTIAL)
        })
        .into_owned()
}

/// Variables are kept in insertion order to mirror JavaScript object lookup
/// and schema iteration behavior.
pub type VariableContext = IndexMap<String, String>;
pub type VariableSchema = IndexMap<String, VariableDefinition>;
pub type JsonValue = Value;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VariableDefinition {
    pub r#type: &'static str,
    pub description: &'static str,
    pub default: Option<&'static str>,
    pub required: bool,
}

impl VariableDefinition {
    fn optional_path(description: &'static str) -> Self {
        Self {
            r#type: "string",
            description,
            default: None,
            required: false,
        }
    }
}

/// Built-in extension variables from `variableSchema.ts`.
pub static VARIABLE_SCHEMA: LazyLock<VariableSchema> = LazyLock::new(|| {
    let mut schema = IndexMap::new();
    schema.insert(
        "extensionPath".to_owned(),
        VariableDefinition::optional_path("The path of the extension in the filesystem."),
    );
    schema.insert(
        "CLAUDE_PLUGIN_ROOT".to_owned(),
        VariableDefinition::optional_path("The path of the extension in the filesystem."),
    );
    schema.insert(
        "workspacePath".to_owned(),
        VariableDefinition::optional_path("The absolute path of the current workspace."),
    );
    let path_separator = VariableDefinition::optional_path("The path separator.");
    schema.insert("/".to_owned(), path_separator.clone());
    schema.insert("pathSeparator".to_owned(), path_separator);
    schema
});

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VariableValidationError {
    pub key: String,
}

impl fmt::Display for VariableValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "Missing required variable: {}", self.key)
    }
}

impl std::error::Error for VariableValidationError {}

/// Validate required entries in a variable schema. Empty strings follow
/// JavaScript truthiness and count as missing.
pub fn validate_variables(
    variables: &VariableContext,
    schema: &VariableSchema,
) -> Result<(), VariableValidationError> {
    for (key, definition) in schema {
        if definition.required && variables.get(key).is_none_or(|value| value.is_empty()) {
            return Err(VariableValidationError { key: key.clone() });
        }
    }
    Ok(())
}

/// Replace `${name}` references whose values exist in `context`.
///
/// Values are inserted literally, matching the TypeScript callback form of
/// `String.replace` (so dollar sequences in a path are not interpreted).
pub fn hydrate_string(source: &str, context: &VariableContext) -> String {
    validate_variables(context, &VARIABLE_SCHEMA)
        .expect("all built-in extension variables are optional");

    let mut matches = Vec::new();
    let mut search_from = 0;
    while search_from < source.len() {
        let Some(relative_start) = source[search_from..].find("${") else {
            break;
        };
        let start = search_from + relative_start;
        let key_start = start + 2;
        let mut closing = None;
        for (offset, character) in source[key_start..].char_indices() {
            if matches_js_line_terminator(character) {
                break;
            }
            if character == '}' {
                closing = Some(key_start + offset);
                break;
            }
        }
        if let Some(close) = closing {
            let end = close + 1;
            matches.push((start, end, &source[key_start..close]));
            search_from = end;
        } else {
            search_from = start + 1;
        }
    }

    let mut hydrated = String::with_capacity(source.len());
    let mut copied_through = 0;
    for (start, end, key) in matches {
        hydrated.push_str(&source[copied_through..start]);
        if let Some(value) = context.get(key) {
            hydrated.push_str(value);
        } else {
            hydrated.push_str(&source[start..end]);
        }
        copied_through = end;
    }
    hydrated.push_str(&source[copied_through..]);
    hydrated
}

fn matches_js_line_terminator(character: char) -> bool {
    matches!(character, '\n' | '\r' | '\u{2028}' | '\u{2029}')
}

/// Recursively hydrate string values in JSON arrays and objects. Object keys
/// and non-string values are copied unchanged.
pub fn recursively_hydrate_strings(value: &JsonValue, context: &VariableContext) -> JsonValue {
    match value {
        Value::String(source) => Value::String(hydrate_string(source, context)),
        Value::Array(items) => Value::Array(
            items
                .iter()
                .map(|item| recursively_hydrate_strings(item, context))
                .collect(),
        ),
        Value::Object(object) => Value::Object(
            object
                .iter()
                .map(|(key, item)| (key.clone(), recursively_hydrate_strings(item, context)))
                .collect(),
        ),
        _ => value.clone(),
    }
}

/// Clone hook JSON and substitute `${CLAUDE_PLUGIN_ROOT}` in command hooks.
/// The base path is kept as an opaque UTF-8 string and is never normalized.
pub fn substitute_hook_variables(hooks: Option<&JsonValue>, base_path: &str) -> Option<JsonValue> {
    let hooks = hooks?;
    let mut cloned = hooks.clone();
    let Some(events) = cloned.as_object_mut() else {
        return Some(cloned);
    };

    for event_hooks in events.values_mut() {
        let Some(event_hooks) = event_hooks.as_array_mut() else {
            continue;
        };
        for hook_definition in event_hooks {
            let Some(hook_configs) = hook_definition
                .get_mut("hooks")
                .and_then(Value::as_array_mut)
            else {
                continue;
            };
            for hook_config in hook_configs {
                if hook_config.get("type").and_then(Value::as_str) != Some("command") {
                    continue;
                }
                let Some(Value::String(command)) = hook_config.get_mut("command") else {
                    continue;
                };
                if command.is_empty() {
                    continue;
                }
                *command = replace_plugin_root(command, base_path);
            }
        }
    }
    Some(cloned)
}

const CLAUDE_PLUGIN_ROOT: &str = "${CLAUDE_PLUGIN_ROOT}";

/// Implement the replacement-string substitutions used by JavaScript's
/// `String.replace` when its second argument is a string rather than a
/// callback. The source pattern has no capture groups.
fn replace_plugin_root(source: &str, replacement: &str) -> String {
    let mut output = String::with_capacity(source.len());
    let mut copied_through = 0;
    for (start, matched) in source.match_indices(CLAUDE_PLUGIN_ROOT) {
        let end = start + matched.len();
        output.push_str(&source[copied_through..start]);
        append_js_replacement(&mut output, replacement, source, start, end, matched);
        copied_through = end;
    }
    output.push_str(&source[copied_through..]);
    output
}

fn append_js_replacement(
    output: &mut String,
    replacement: &str,
    source: &str,
    match_start: usize,
    match_end: usize,
    matched: &str,
) {
    let mut chars = replacement.char_indices().peekable();
    while let Some((_, character)) = chars.next() {
        if character != '$' {
            output.push(character);
            continue;
        }
        let Some((_, next_character)) = chars.peek().copied() else {
            output.push('$');
            continue;
        };
        match next_character {
            '$' => {
                output.push('$');
                chars.next();
            }
            '&' => {
                output.push_str(matched);
                chars.next();
            }
            '`' => {
                output.push_str(&source[..match_start]);
                chars.next();
            }
            '\'' => {
                output.push_str(&source[match_end..]);
                chars.next();
            }
            // With no capture groups (or named captures), JS preserves these
            // sequences literally. Leave the following character unconsumed.
            _ => output.push('$'),
        }
    }
}
