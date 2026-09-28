// Copyright 2026 Canopy Team
// SPDX-License-Identifier: Apache-2.0
//! Agent Plugins v1 skill discovery and SKILL.md validation.

use std::fs;
use std::io;
use std::path::Path;

use serde_json::{Map, Number, Value};

use crate::agent_plugins::resolve_contained_existing_path;
use crate::skills::{SkillConfig, SkillLevel, normalize_content};

const SKILLS_DIRECTORY: &str = "skills";
const SKILL_MANIFEST: &str = "SKILL.md";
const MAX_FRONTMATTER_CONVERSION_DEPTH: usize = 128;

/// Discover valid direct-child Agent Skills under `<pluginRoot>/skills`.
/// Warning messages are discarded; use
/// [`load_agent_plugin_skills_with_warning`] to connect a debug logger.
pub async fn load_agent_plugin_skills(plugin_root: &str) -> Vec<SkillConfig> {
    load_agent_plugin_skills_with_warning(plugin_root, |_| {}).await
}

/// Discover direct-child Agent Skills and pass source-compatible warning
/// messages to `warn` for disabled roots and skipped entries.
pub async fn load_agent_plugin_skills_with_warning(
    plugin_root: &str,
    mut warn: impl FnMut(&str) + Send,
) -> Vec<SkillConfig> {
    let skills_path = Path::new(plugin_root).join(SKILLS_DIRECTORY);
    let resolved_skills_path =
        match resolve_contained_existing_path(plugin_root, &skills_path.to_string_lossy()) {
            Ok(path) => path,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Vec::new(),
            Err(error) => {
                warn(&format!("Disabling Agent Plugins skills: {}", error));
                return Vec::new();
            }
        };

    match fs::metadata(&resolved_skills_path) {
        Ok(metadata) if metadata.is_dir() => {}
        Ok(_) => {
            warn("Agent Plugins skills path is not a directory.");
            return Vec::new();
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Vec::new(),
        Err(error) => {
            warn(&format!("Disabling Agent Plugins skills: {}", error));
            return Vec::new();
        }
    }

    let mut directory = match tokio::fs::read_dir(&resolved_skills_path).await {
        Ok(directory) => directory,
        Err(error) => {
            warn(&format!("Disabling Agent Plugins skills: {}", error));
            return Vec::new();
        }
    };
    let mut entries = Vec::new();
    loop {
        match directory.next_entry().await {
            Ok(Some(entry)) => entries.push(entry),
            Ok(None) => break,
            Err(error) => {
                warn(&format!("Disabling Agent Plugins skills: {}", error));
                return Vec::new();
            }
        }
    }

    let mut skills = Vec::new();
    for entry in entries {
        let directory_name = entry.file_name().to_string_lossy().into_owned();
        let skill_dir = entry.path();
        let resolved_skill_dir =
            match resolve_contained_existing_path(plugin_root, &skill_dir.to_string_lossy()) {
                Ok(path) => path,
                Err(error) => {
                    warn(&format!(
                        "Skipping Agent Plugins skill \"{directory_name}\": {error}"
                    ));
                    continue;
                }
            };
        match fs::metadata(&resolved_skill_dir) {
            Ok(metadata) if metadata.is_dir() => {}
            Ok(_) => continue,
            Err(error) => {
                warn(&format!(
                    "Skipping Agent Plugins skill \"{directory_name}\": {error}"
                ));
                continue;
            }
        }

        let skill_manifest = resolved_skill_dir.join(SKILL_MANIFEST);
        let resolved_manifest =
            match resolve_contained_existing_path(plugin_root, &skill_manifest.to_string_lossy()) {
                Ok(path) => path,
                Err(error) => {
                    warn(&format!(
                        "Skipping Agent Plugins skill \"{directory_name}\": {error}"
                    ));
                    continue;
                }
            };
        match fs::metadata(&resolved_manifest) {
            Ok(metadata) if metadata.is_file() => {}
            Ok(_) => continue,
            Err(error) => {
                warn(&format!(
                    "Skipping Agent Plugins skill \"{directory_name}\": {error}"
                ));
                continue;
            }
        }

        let content = match tokio::fs::read(&resolved_manifest).await {
            Ok(content) => String::from_utf8_lossy(&content).into_owned(),
            Err(error) => {
                warn(&format!(
                    "Skipping Agent Plugins skill \"{directory_name}\": {error}"
                ));
                continue;
            }
        };
        match parse_agent_plugin_skill_in_directory(&content, &resolved_manifest, &directory_name) {
            Ok(skill) => skills.push(skill),
            Err(error) => warn(&format!(
                "Skipping Agent Plugins skill \"{directory_name}\": {error}"
            )),
        }
    }
    skills
}

/// Parse an Agent Skills SKILL.md file. The parent directory name is derived
/// from `file_path`, matching the source function's default argument.
pub fn parse_agent_plugin_skill(
    content: &str,
    file_path: impl AsRef<Path>,
) -> Result<SkillConfig, String> {
    let file_path = file_path.as_ref();
    let directory_name = file_path
        .parent()
        .and_then(Path::file_name)
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    parse_agent_plugin_skill_in_directory(content, file_path, &directory_name)
}

/// Parse an Agent Skills SKILL.md while using the directory entry name as the
/// expected frontmatter name. Discovery calls this with the direct child name.
pub fn parse_agent_plugin_skill_in_directory(
    content: &str,
    file_path: impl AsRef<Path>,
    directory_name: &str,
) -> Result<SkillConfig, String> {
    let file_path = file_path.as_ref();
    let normalized = normalize_content(content);
    let (frontmatter_yaml, body) =
        split_frontmatter(&normalized).ok_or_else(|| "Missing YAML frontmatter.".to_owned())?;
    let frontmatter = parse_frontmatter(frontmatter_yaml)?;

    let name = frontmatter
        .get("name")
        .and_then(Value::as_str)
        .filter(|name| is_valid_agent_skill_name(name))
        .ok_or_else(|| "Invalid Agent Skills name.".to_owned())?;
    if name != directory_name {
        return Err("Agent Skills name must match its parent directory.".to_owned());
    }

    let description = frontmatter
        .get("description")
        .and_then(Value::as_str)
        .filter(|description| !description.is_empty() && description.chars().count() <= 1024)
        .ok_or_else(|| "Agent Skills description must be 1-1024 characters.".to_owned())?;

    validate_optional_string(&frontmatter, "license")?;
    validate_compatibility(frontmatter.get("compatibility"))?;
    validate_metadata(frontmatter.get("metadata"))?;
    validate_allowed_tools(frontmatter.get("allowed-tools"))?;

    Ok(SkillConfig {
        name: name.to_owned(),
        description: description.to_owned(),
        allowed_tools: None,
        hooks: None,
        model: None,
        level: SkillLevel::Extension,
        file_path: file_path.to_path_buf(),
        skill_root: file_path.parent().map(Path::to_path_buf),
        body: trim_ecmascript_whitespace(body).to_owned(),
        extension_name: None,
        argument_hint: None,
        when_to_use: None,
        disable_model_invocation: None,
        user_invocable: None,
        paths: None,
        priority: None,
    })
}

fn is_valid_agent_skill_name(name: &str) -> bool {
    if name.is_empty() || name.chars().count() > 64 {
        return false;
    }
    let Some(first) = name.chars().next() else {
        return false;
    };
    let Some(last) = name.chars().next_back() else {
        return false;
    };
    if !is_ascii_name_character(first) || !is_ascii_name_character(last) {
        return false;
    }
    if name.contains("--") {
        return false;
    }
    name.chars()
        .all(|character| is_ascii_name_character(character) || character == '-')
}

fn is_ascii_name_character(character: char) -> bool {
    character.is_ascii_lowercase() || character.is_ascii_digit()
}

fn validate_optional_string(frontmatter: &Map<String, Value>, field: &str) -> Result<(), String> {
    if frontmatter
        .get(field)
        .is_some_and(|value| !value.is_string())
    {
        return Err(format!("Agent Skills {field} must be a string."));
    }
    Ok(())
}

fn validate_compatibility(value: Option<&Value>) -> Result<(), String> {
    let Some(value) = value else {
        return Ok(());
    };
    if !value.as_str().is_some_and(|compatibility| {
        !compatibility.is_empty() && compatibility.chars().count() <= 500
    }) {
        return Err("Agent Skills compatibility must be a 1-500 character string.".to_owned());
    }
    Ok(())
}

fn validate_metadata(value: Option<&Value>) -> Result<(), String> {
    let Some(value) = value else {
        return Ok(());
    };
    if !value
        .as_object()
        .is_some_and(|metadata| metadata.values().all(Value::is_string))
    {
        return Err("Agent Skills metadata values must be strings.".to_owned());
    }
    Ok(())
}

fn validate_allowed_tools(value: Option<&Value>) -> Result<(), String> {
    if value.is_some_and(|value| !value.is_string()) {
        return Err("Agent Skills allowed-tools must be a string.".to_owned());
    }
    Ok(())
}

fn split_frontmatter(content: &str) -> Option<(&str, &str)> {
    if !content.starts_with("---\n") {
        return None;
    }
    let mut search_from = 4;
    while let Some(offset) = content.get(search_from..)?.find("\n---") {
        let delimiter_start = search_from + offset + 1;
        let after_delimiter = delimiter_start + 3;
        if after_delimiter == content.len()
            || content.as_bytes().get(after_delimiter) == Some(&b'\n')
        {
            let yaml = content.get(4..delimiter_start - 1)?;
            let body_start = if after_delimiter < content.len() {
                after_delimiter + 1
            } else {
                after_delimiter
            };
            return Some((yaml, content.get(body_start..)?));
        }
        search_from = delimiter_start + 1;
    }
    None
}

fn parse_frontmatter(input: &str) -> Result<Map<String, Value>, String> {
    let yaml =
        yaml_serde::from_str::<yaml_serde::Value>(input).map_err(|error| error.to_string())?;
    let yaml_serde::Value::Mapping(mapping) = yaml else {
        return Err("Frontmatter must be an object.".to_owned());
    };

    let mut frontmatter = Map::new();
    for (key, value) in mapping {
        if let Some(value) = yaml_value_to_json(value, 0) {
            frontmatter.insert(yaml_value_to_string(key), value);
        }
    }
    Ok(frontmatter)
}

fn yaml_value_to_json(value: yaml_serde::Value, depth: usize) -> Option<Value> {
    if depth > MAX_FRONTMATTER_CONVERSION_DEPTH {
        return None;
    }
    Some(match value {
        yaml_serde::Value::Null => Value::Null,
        yaml_serde::Value::Bool(value) => Value::Bool(value),
        yaml_serde::Value::Number(number) => {
            let number = number
                .as_i64()
                .map(Number::from)
                .or_else(|| number.as_u64().map(Number::from))
                .or_else(|| number.as_f64().and_then(Number::from_f64));
            number.map_or(Value::Null, Value::Number)
        }
        yaml_serde::Value::String(value) => Value::String(value),
        yaml_serde::Value::Sequence(values) => Value::Array(
            values
                .into_iter()
                .filter_map(|value| yaml_value_to_json(value, depth + 1))
                .collect(),
        ),
        yaml_serde::Value::Mapping(values) => {
            let mut object = Map::new();
            for (key, value) in values {
                if let Some(value) = yaml_value_to_json(value, depth + 1) {
                    object.insert(yaml_value_to_string(key), value);
                }
            }
            Value::Object(object)
        }
        yaml_serde::Value::Tagged(tagged) => yaml_value_to_json(tagged.value, depth + 1)?,
    })
}

fn yaml_value_to_string(value: yaml_serde::Value) -> String {
    match value {
        yaml_serde::Value::Null => "null".to_owned(),
        yaml_serde::Value::Bool(value) => value.to_string(),
        yaml_serde::Value::Number(value) => value.to_string(),
        yaml_serde::Value::String(value) => value,
        yaml_serde::Value::Sequence(values) => values
            .into_iter()
            .map(|value| match value {
                yaml_serde::Value::Null => String::new(),
                value => yaml_value_to_string(value),
            })
            .collect::<Vec<_>>()
            .join(","),
        yaml_serde::Value::Mapping(_) => "[object Object]".to_owned(),
        yaml_serde::Value::Tagged(tagged) => yaml_value_to_string(tagged.value),
    }
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
            | '\u{2028}'
            | '\u{2029}'
            | '\u{202F}'
            | '\u{205F}'
            | '\u{3000}'
            | '\u{FEFF}'
    )
}
