//! Pure projection helpers for marketplace sources and plugin metadata.
//!
//! This module mirrors the source classification and Discover-view projection
//! in `packages/core/src/extension/sourceRegistry.ts`. It deliberately does
//! not fetch marketplace data or persist registry state.
//!
//! URL parsing uses Rust's `reqwest::Url`, and local absolute-path checks use
//! the host platform's `std::path::Path`, matching the broad intent of Node's
//! `URL` and `path` APIs. Their acceptance/canonicalization details can differ
//! for malformed URLs, backslashes, and paths from a different operating
//! system. Rust Unicode case conversion and serde_json string handling can
//! also differ from JavaScript for uncommon Unicode edge cases.

pub use crate::extension_source_store::ExtensionSourceType;
use reqwest::Url;
use serde_json::{Map, Number, Value};
use std::collections::HashSet;
use std::iter::Peekable;
use std::path::Path;
use std::str::Chars;

/// Classify a source using the TypeScript source registry's format heuristics.
pub fn parse_extension_source_type(source: &str) -> ExtensionSourceType {
    let trimmed = trim_ecmascript_whitespace(source);
    let lower = trimmed.to_lowercase();

    if lower.starts_with("git@") || lower.starts_with("sso://") {
        return ExtensionSourceType::Git;
    }
    if lower.starts_with("http://") || lower.starts_with("https://") {
        return if is_github_host(trimmed) {
            ExtensionSourceType::Github
        } else {
            ExtensionSourceType::Http
        };
    }
    if is_owner_repo_shorthand(trimmed) {
        return ExtensionSourceType::Github;
    }
    ExtensionSourceType::Local
}

fn is_github_host(source: &str) -> bool {
    Url::parse(source)
        .ok()
        .and_then(|url| url.host_str().map(str::to_owned))
        .is_some_and(|host| host == "github.com")
}

fn is_owner_repo_shorthand(source: &str) -> bool {
    let Some((owner, repository)) = source.split_once('/') else {
        return false;
    };
    !owner.is_empty()
        && !repository.is_empty()
        && !repository.contains('/')
        && owner
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_.-".contains(&byte))
        && repository
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_.-".contains(&byte))
}

fn trim_ecmascript_whitespace(value: &str) -> &str {
    value.trim_matches(is_ecmascript_whitespace)
}

fn is_ecmascript_whitespace(character: char) -> bool {
    matches!(
        character,
        '\u{0009}'..='\u{000D}'
            | '\u{0020}'
            | '\u{00A0}'
            | '\u{1680}'
            | '\u{2000}'..='\u{200A}'
            | '\u{2028}'..='\u{2029}'
            | '\u{202F}'
            | '\u{205F}'
            | '\u{3000}'
            | '\u{FEFF}'
    )
}

/// Whether a source supplied by an HTTP marketplace looks like a local path.
/// The check intentionally mirrors Node's host-platform `path.isAbsolute`,
/// followed by exact `.` and `~` prefix checks (with no trimming).
pub fn is_remote_marketplace_local_path(source: &str) -> bool {
    Path::new(source).is_absolute() || source.starts_with('.') || source.starts_with('~')
}

/// Resolve a plugin's installer source from JSON marketplace records.
///
/// The warning callback receives the same message used by the TypeScript
/// source when a remote marketplace points at a local path.
pub fn resolve_install_source(
    marketplace_type: &str,
    marketplace_source: &str,
    plugin: &Value,
    mut warning: impl FnMut(&str),
) -> String {
    let plugin_name = plugin
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or_default();

    if marketplace_type != "http" {
        return format!("{marketplace_source}:{plugin_name}");
    }

    let Some(source) = plugin.get("source") else {
        return plugin_name.to_owned();
    };
    if let Some(source) = source.as_str() {
        if is_remote_marketplace_local_path(source) {
            warn_local_source(source, marketplace_source, &mut warning);
            return plugin_name.to_owned();
        }
        return if source.contains(':') {
            source.to_owned()
        } else {
            format!("{source}:{plugin_name}")
        };
    }

    match source.get("source").and_then(Value::as_str) {
        Some("github") => {
            let repository = source
                .get("repo")
                .and_then(Value::as_str)
                .unwrap_or_default();
            format!("{repository}:{plugin_name}")
        }
        Some("url") => {
            let Some(url) = source.get("url").and_then(Value::as_str) else {
                return plugin_name.to_owned();
            };
            if is_remote_marketplace_local_path(url) {
                warn_local_source(url, marketplace_source, &mut warning);
                plugin_name.to_owned()
            } else {
                url.to_owned()
            }
        }
        _ => plugin_name.to_owned(),
    }
}

fn warn_local_source(source: &str, marketplace_source: &str, warning: &mut impl FnMut(&str)) {
    warning(&format!(
        "Ignoring local path source \"{source}\" from remote marketplace \"{marketplace_source}\"."
    ));
}

/// Remove VT/ANSI terminal sequences and remaining C0/C1 controls from text
/// that is rendered as untrusted marketplace metadata.
pub fn sanitize_display(text: &str) -> String {
    let mut chars = text.chars().peekable();
    let mut output = String::with_capacity(text.len());
    while let Some(character) = chars.next() {
        match character {
            '\u{001B}' => skip_escape_sequence(&mut chars),
            '\u{009B}' => skip_csi_sequence(&mut chars),
            '\u{009D}' => skip_string_sequence(&mut chars, true),
            '\u{0090}' | '\u{0098}' | '\u{009E}' | '\u{009F}' => {
                skip_string_sequence(&mut chars, false)
            }
            '\u{0000}'..='\u{001F}' | '\u{007F}'..='\u{009F}' => {}
            _ => output.push(character),
        }
    }
    output
}

fn skip_escape_sequence(chars: &mut Peekable<Chars<'_>>) {
    let Some(first) = chars.next() else {
        return;
    };
    match first {
        '[' => skip_csi_sequence(chars),
        ']' => skip_string_sequence(chars, true),
        'P' | 'X' | '^' | '_' => skip_string_sequence(chars, false),
        '\u{0020}'..='\u{002F}' => {
            for character in chars.by_ref() {
                if ('\u{0030}'..='\u{007E}').contains(&character) {
                    break;
                }
            }
        }
        _ => {}
    }
}

fn skip_csi_sequence(chars: &mut Peekable<Chars<'_>>) {
    for character in chars.by_ref() {
        if ('@'..='~').contains(&character) {
            break;
        }
    }
}

fn skip_string_sequence(chars: &mut Peekable<Chars<'_>>, bell_terminates: bool) {
    while let Some(character) = chars.next() {
        if character == '\u{009C}' || (bell_terminates && character == '\u{0007}') {
            break;
        }
        if character == '\u{001B}' && chars.next_if_eq(&'\\').is_some() {
            break;
        }
    }
}

/// Project marketplace config JSON into the ordered Discover plugin records.
///
/// Plugin order and component-name order follow the input JSON order. The
/// installed lookup and installer source use raw plugin identifiers, while
/// metadata and component labels are sanitized for terminal display.
pub fn plugins_from_config(
    marketplace: &Value,
    config: &Value,
    installed_names: &HashSet<String>,
    mut warning: impl FnMut(&str),
) -> Vec<Value> {
    let marketplace_source = marketplace
        .get("source")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let marketplace_type = marketplace
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let config_name = config
        .get("name")
        .and_then(Value::as_str)
        .filter(|name| !name.is_empty())
        .or_else(|| marketplace.get("name").and_then(Value::as_str))
        .unwrap_or_default();
    let Some(plugins) = config.get("plugins").and_then(Value::as_array) else {
        return Vec::new();
    };

    plugins
        .iter()
        .map(|plugin| {
            let raw_name = plugin
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let mut projected = Map::new();
            projected.insert(
                "marketplaceName".to_owned(),
                Value::String(sanitize_display(config_name)),
            );
            projected.insert("name".to_owned(), Value::String(sanitize_display(raw_name)));
            insert_sanitized_string(&mut projected, "description", plugin.get("description"));
            insert_sanitized_string(&mut projected, "version", plugin.get("version"));
            insert_sanitized_string(
                &mut projected,
                "author",
                plugin.get("author").and_then(|author| author.get("name")),
            );
            insert_sanitized_string(&mut projected, "homepage", plugin.get("homepage"));
            insert_sanitized_string(&mut projected, "category", plugin.get("category"));

            if let Some(last_updated) = plugin_last_updated(plugin) {
                projected.insert(
                    "lastUpdated".to_owned(),
                    Value::String(sanitize_display(last_updated)),
                );
            }
            if let Some(installs) = plugin_installs(plugin) {
                projected.insert("installs".to_owned(), Value::Number(installs));
            }
            if let Some(components) = plugin_components(plugin) {
                projected.insert("components".to_owned(), components);
            }

            projected.insert(
                "installSource".to_owned(),
                Value::String(resolve_install_source(
                    marketplace_type,
                    marketplace_source,
                    plugin,
                    &mut warning,
                )),
            );
            projected.insert(
                "installed".to_owned(),
                Value::Bool(installed_names.contains(raw_name)),
            );
            Value::Object(projected)
        })
        .collect()
}

fn insert_sanitized_string(projected: &mut Map<String, Value>, key: &str, value: Option<&Value>) {
    if let Some(value) = value.and_then(Value::as_str) {
        projected.insert(key.to_owned(), Value::String(sanitize_display(value)));
    }
}

fn plugin_last_updated(plugin: &Value) -> Option<&str> {
    ["lastUpdated", "updatedAt", "updated"]
        .into_iter()
        .filter_map(|key| plugin.get(key))
        .find(|value| !value.is_null())
        .and_then(Value::as_str)
}

fn plugin_installs(plugin: &Value) -> Option<Number> {
    ["installs", "installCount", "downloads"]
        .into_iter()
        .filter_map(|key| plugin.get(key))
        .find(|value| !value.is_null())
        .and_then(Value::as_number)
        .filter(|value| value.as_f64().is_some_and(f64::is_finite))
        .cloned()
}

fn plugin_components(plugin: &Value) -> Option<Value> {
    let mut components = Map::new();
    insert_name_list(&mut components, "skills", plugin.get("skills"));
    insert_name_list(&mut components, "commands", plugin.get("commands"));
    insert_name_list(&mut components, "agents", plugin.get("agents"));
    insert_mcp_names(&mut components, plugin.get("mcpServers"));
    (!components.is_empty()).then_some(Value::Object(components))
}

fn insert_name_list(components: &mut Map<String, Value>, key: &str, value: Option<&Value>) {
    let Some(values) = value
        .and_then(Value::as_array)
        .filter(|values| !values.is_empty())
    else {
        return;
    };
    let names = values
        .iter()
        .filter_map(Value::as_str)
        .map(sanitize_display)
        .map(Value::String)
        .collect();
    components.insert(key.to_owned(), Value::Array(names));
}

fn insert_mcp_names(components: &mut Map<String, Value>, value: Option<&Value>) {
    let names = match value {
        Some(Value::Object(object)) => object.keys().cloned().collect::<Vec<_>>(),
        // JavaScript's `Object.keys` also accepts arrays, yielding their
        // decimal index strings. Keep that behavior for malformed JSON input.
        Some(Value::Array(values)) => (0..values.len()).map(|index| index.to_string()).collect(),
        _ => return,
    };
    if names.is_empty() {
        return;
    }
    components.insert(
        "mcpServers".to_owned(),
        Value::Array(
            names
                .into_iter()
                .map(|name| Value::String(sanitize_display(&name)))
                .collect(),
        ),
    );
}
