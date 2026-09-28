// Copyright 2026 Canopy Team
// SPDX-License-Identifier: Apache-2.0
//! Agent Plugins v1 root manifest validation and contained path resolution.
//!
//! This module is intentionally standalone. Hosts can supply a warning sink
//! when loading a manifest to connect diagnostics to their debug logger.

use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};

use serde_json::{Map, Value};

pub const AGENT_PLUGIN_MANIFEST: &str = "plugin.json";
pub const AGENT_PLUGIN_SCHEMA: &str = "https://agent-plugins.org/schemas/1.0.0/plugin.schema.json";
pub const AGENT_PLUGIN_SCHEMA_PREFIX: &str = "https://agent-plugins.org/schemas/";
pub const DEFAULT_AGENT_PLUGIN_VERSION: &str = "1.0.0";

const MANIFEST_FIELDS: &[&str] = &[
    "$schema",
    "name",
    "version",
    "description",
    "author",
    "homepage",
    "repository",
    "license",
    "keywords",
    "extensions",
];
const AUTHOR_FIELDS: &[&str] = &["name", "email", "url"];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AgentPluginSchemaStatus {
    Supported,
    Unsupported,
    Unrelated,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AgentPluginExtensionConfig {
    pub name: String,
    pub version: String,
    pub display_name: String,
    pub description: Option<String>,
}

/// Check the root manifest's schema without requiring its other fields to be
/// valid. Missing paths, non-files, unreadable files, and malformed JSON are
/// unrelated; path errors other than ENOENT/ENOTDIR and escaping symlinks are
/// classified as supported, matching the source status probe.
pub fn get_agent_plugin_schema_status(plugin_root: &str) -> AgentPluginSchemaStatus {
    let manifest_path = Path::new(plugin_root).join(AGENT_PLUGIN_MANIFEST);
    let resolved_manifest_path =
        match resolve_contained_existing_path(plugin_root, &manifest_path.to_string_lossy()) {
            Ok(path) => path,
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::NotFound | io::ErrorKind::NotADirectory
                ) =>
            {
                return AgentPluginSchemaStatus::Unrelated;
            }
            Err(_) => return AgentPluginSchemaStatus::Supported,
        };

    let value = match fs::metadata(&resolved_manifest_path) {
        Ok(metadata) if metadata.is_file() => match read_json_value(&resolved_manifest_path) {
            Ok(value) => value,
            Err(_) => return AgentPluginSchemaStatus::Unrelated,
        },
        _ => return AgentPluginSchemaStatus::Unrelated,
    };
    let Some(schema) = value
        .as_object()
        .and_then(|object| object.get("$schema"))
        .and_then(Value::as_str)
    else {
        return AgentPluginSchemaStatus::Unrelated;
    };

    if schema == AGENT_PLUGIN_SCHEMA {
        AgentPluginSchemaStatus::Supported
    } else if schema.starts_with(AGENT_PLUGIN_SCHEMA_PREFIX) {
        AgentPluginSchemaStatus::Unsupported
    } else {
        AgentPluginSchemaStatus::Unrelated
    }
}

/// Load and validate the portable metadata exposed by an Agent Plugins v1
/// root manifest. Warnings are discarded; use
/// [`load_agent_plugin_manifest_with_warning`] to connect a debug logger.
pub fn load_agent_plugin_manifest(plugin_root: &str) -> Result<AgentPluginExtensionConfig, String> {
    load_agent_plugin_manifest_with_warning(plugin_root, |_| {})
}

/// Load and validate a manifest, passing source-compatible warning messages
/// to `warn` for unknown root fields and a non-object `extensions` value.
pub fn load_agent_plugin_manifest_with_warning(
    plugin_root: &str,
    mut warn: impl FnMut(&str),
) -> Result<AgentPluginExtensionConfig, String> {
    let manifest_path = Path::new(plugin_root).join(AGENT_PLUGIN_MANIFEST);
    let resolved_manifest_path =
        resolve_contained_existing_path(plugin_root, &manifest_path.to_string_lossy())
            .map_err(|error| error.to_string())?;

    let metadata = fs::metadata(&resolved_manifest_path).map_err(|error| error.to_string())?;
    if !metadata.is_file() {
        return Err("Agent Plugins root plugin.json must be a regular file.".to_owned());
    }

    let value = read_json_value(&resolved_manifest_path).map_err(|error| error.to_string())?;
    load_agent_plugin_manifest_value_with_warning(&value, &mut warn)
}

/// Validate an already-read Agent Plugins manifest value.
///
/// Hosts that need strict read-size limits can read `plugin.json` once with a
/// bounded reader and pass the parsed value here instead of using the
/// filesystem convenience loader above.
pub fn load_agent_plugin_manifest_value(
    value: &Value,
) -> Result<AgentPluginExtensionConfig, String> {
    load_agent_plugin_manifest_value_with_warning(value, |_| {})
}

/// Validate an already-read manifest, passing source-compatible warning
/// messages to `warn` for unknown fields and a non-object `extensions` value.
pub fn load_agent_plugin_manifest_value_with_warning(
    value: &Value,
    mut warn: impl FnMut(&str),
) -> Result<AgentPluginExtensionConfig, String> {
    let Some(object) = value.as_object() else {
        return Err("Agent Plugins root plugin.json must contain an object.".to_owned());
    };

    for field in object.keys() {
        if !MANIFEST_FIELDS.contains(&field.as_str()) {
            warn(&format!(
                "Ignoring unknown Agent Plugins manifest field \"{field}\"."
            ));
        }
    }

    let schema = object
        .get("$schema")
        .and_then(Value::as_str)
        .ok_or_else(|| "Agent Plugins manifest is missing string \"$schema\".".to_owned())?;
    if schema != AGENT_PLUGIN_SCHEMA {
        return Err(if schema.starts_with(AGENT_PLUGIN_SCHEMA_PREFIX) {
            format!(
                "Unsupported Agent Plugins schema \"{schema}\". Supported schema: \"{AGENT_PLUGIN_SCHEMA}\"."
            )
        } else {
            format!(
                "Root plugin.json is not an Agent Plugins v1 manifest. Expected \"$schema\" \"{AGENT_PLUGIN_SCHEMA}\"."
            )
        });
    }

    let name = object.get("name").and_then(Value::as_str).ok_or_else(|| {
        "Agent Plugins name must be 1-64 lowercase letters, numbers, dots, or hyphens without leading, trailing, or consecutive separators."
            .to_owned()
    })?;
    if !is_valid_plugin_name(name) {
        return Err(
            "Agent Plugins name must be 1-64 lowercase letters, numbers, dots, or hyphens without leading, trailing, or consecutive separators."
                .to_owned(),
        );
    }

    validate_optional_string(object, "version")?;
    validate_optional_string(object, "description")?;
    validate_optional_string(object, "homepage")?;
    validate_optional_string(object, "repository")?;
    validate_optional_string(object, "license")?;
    validate_author(object.get("author"))?;
    validate_keywords(object.get("keywords"))?;

    if object
        .get("extensions")
        .is_some_and(|extensions| !extensions.is_object())
    {
        warn("Ignoring non-object Agent Plugins extensions field.");
    }

    let version = optional_trimmed_string(object.get("version"))
        .unwrap_or_else(|| DEFAULT_AGENT_PLUGIN_VERSION.to_owned());
    let description = optional_trimmed_string(object.get("description"));
    Ok(AgentPluginExtensionConfig {
        name: name.to_owned(),
        version,
        display_name: name.to_owned(),
        description,
    })
}

pub fn is_valid_plugin_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    if bytes.is_empty() || bytes.len() > 64 {
        return false;
    }
    if !is_name_alphanumeric(bytes[0]) || !is_name_alphanumeric(*bytes.last().unwrap()) {
        return false;
    }
    if bytes.windows(2).any(|pair| pair == b"--" || pair == b"..") {
        return false;
    }
    bytes
        .iter()
        .all(|byte| is_name_alphanumeric(*byte) || matches!(byte, b'.' | b'-'))
}

fn is_name_alphanumeric(byte: u8) -> bool {
    byte.is_ascii_lowercase() || byte.is_ascii_digit()
}

fn validate_optional_string(object: &Map<String, Value>, field: &str) -> Result<(), String> {
    if object.get(field).is_some_and(|value| !value.is_string()) {
        return Err(format!(
            "Agent Plugins manifest \"{field}\" must be a string."
        ));
    }
    Ok(())
}

fn validate_author(value: Option<&Value>) -> Result<(), String> {
    let Some(value) = value else {
        return Ok(());
    };
    let Some(author) = value.as_object() else {
        return Err("Agent Plugins manifest \"author\" must be an object.".to_owned());
    };
    for (field, field_value) in author {
        if !AUTHOR_FIELDS.contains(&field.as_str()) {
            return Err(format!("Unknown Agent Plugins author field \"{field}\"."));
        }
        if !field_value.is_string() {
            return Err(format!(
                "Agent Plugins author field \"{field}\" must be a string."
            ));
        }
    }
    Ok(())
}

fn validate_keywords(value: Option<&Value>) -> Result<(), String> {
    let Some(value) = value else {
        return Ok(());
    };
    if !value
        .as_array()
        .is_some_and(|keywords| keywords.iter().all(Value::is_string))
    {
        return Err("Agent Plugins manifest \"keywords\" must be an array of strings.".to_owned());
    }
    Ok(())
}

fn optional_trimmed_string(value: Option<&Value>) -> Option<String> {
    let trimmed = value?.as_str()?.trim_matches(is_javascript_whitespace);
    (!trimmed.is_empty()).then(|| trimmed.to_owned())
}

fn is_javascript_whitespace(character: char) -> bool {
    matches!(
        character,
        '\u{0009}'..='\u{000D}'
            | '\u{0020}'
            | '\u{00A0}'
            | '\u{1680}'
            | '\u{2000}'..='\u{200A}'
            | '\u{2028}'
            | '\u{2029}'
            | '\u{202F}'
            | '\u{205F}'
            | '\u{3000}'
            | '\u{FEFF}'
    )
}

fn read_json_value(path: &Path) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    let bytes = fs::read(path)?;
    let decoded = String::from_utf8_lossy(&bytes);
    Ok(serde_json::from_str(&decoded)?)
}

/// Return whether `candidate` is the root itself or is lexically contained by
/// `root`. Both paths are resolved against the process working directory and
/// normalized before comparing path components.
pub fn is_path_within(root: &str, candidate: &str) -> bool {
    let Ok(cwd) = std::env::current_dir() else {
        return false;
    };
    let root = normalize_absolute(Path::new(root), &cwd);
    let candidate = normalize_absolute(Path::new(candidate), &cwd);
    absolute_path_is_within(&root, &candidate)
}

/// Resolve an existing root and candidate through symlinks and reject a
/// candidate whose real path leaves the root.
pub fn resolve_contained_existing_path(root: &str, candidate: &str) -> io::Result<PathBuf> {
    let resolved_root = fs::canonicalize(root)?;
    let resolved_candidate = fs::canonicalize(candidate)?;
    if !absolute_path_is_within(&resolved_root, &resolved_candidate) {
        return Err(outside_root_error(root, candidate));
    }
    Ok(resolved_candidate)
}

/// Resolve a candidate which may not exist yet, following every existing
/// symlink prefix before checking containment.
pub fn resolve_contained_potential_path(root: &str, candidate: &str) -> io::Result<PathBuf> {
    let resolved_root = fs::canonicalize(root)?;
    let resolved_candidate = resolve_existing_path_prefix(candidate)?;
    if !absolute_path_is_within(&resolved_root, &resolved_candidate) {
        return Err(outside_root_error(root, candidate));
    }
    Ok(resolved_candidate)
}

/// Resolve an existing path prefix through symlinks and append any missing
/// trailing segments. Only ENOENT is treated as a missing tail; ENOTDIR and
/// all other filesystem errors are returned unchanged.
pub fn resolve_existing_path_prefix(candidate: &str) -> io::Result<PathBuf> {
    let cwd = std::env::current_dir()?;
    let mut current = normalize_absolute(Path::new(candidate), &cwd);
    let mut missing_segments = Vec::new();

    loop {
        match fs::canonicalize(&current) {
            Ok(resolved) => {
                let mut output = resolved;
                for segment in missing_segments.iter().rev() {
                    output.push(segment);
                }
                return Ok(output);
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let parent = current.parent().unwrap_or(&current);
                if parent == current.as_path() {
                    return Err(error);
                }
                let Some(name) = current.file_name() else {
                    return Err(error);
                };
                missing_segments.push(name.to_os_string());
                current = parent.to_path_buf();
            }
            Err(error) => return Err(error),
        }
    }
}

fn outside_root_error(root: &str, candidate: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::PermissionDenied,
        format!("Path \"{candidate}\" resolves outside plugin root \"{root}\"."),
    )
}

fn absolute_path_is_within(root: &Path, candidate: &Path) -> bool {
    let mut root_components = root.components();
    let mut candidate_components = candidate.components();
    loop {
        match root_components.next() {
            None => return true,
            Some(root_component) => match candidate_components.next() {
                Some(candidate_component)
                    if components_equal(root_component, candidate_component) =>
                {
                    continue;
                }
                _ => return false,
            },
        }
    }
}

#[cfg(windows)]
fn components_equal(left: Component<'_>, right: Component<'_>) -> bool {
    left.as_os_str()
        .to_string_lossy()
        .eq_ignore_ascii_case(&right.as_os_str().to_string_lossy())
}

#[cfg(not(windows))]
fn components_equal(left: Component<'_>, right: Component<'_>) -> bool {
    left == right
}

fn normalize_absolute(path: &Path, cwd: &Path) -> PathBuf {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        cwd.join(path)
    };
    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                if normalized.file_name().is_some() {
                    normalized.pop();
                }
            }
            Component::Normal(part) => normalized.push(part),
        }
    }
    normalized
}
