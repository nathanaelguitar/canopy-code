//! Extension skill frontmatter parsing and directory discovery.
//!
//! Port of `packages/core/src/skills/skill-load.ts`. Symlink directory
//! entries use the shared target validator; external symlink targets remain
//! allowed to support shared skill repositories.

use std::path::Path;

use base64::Engine as _;
use serde_json::{Map, Number, Value};

use super::symlink_scope::{SymlinkTargetCheck, validate_symlink_target};
use super::types::{
    SkillConfig, SkillError, SkillErrorCode, SkillLevel, SkillValidationResult,
    parse_allowed_tools_field, parse_model_field, parse_paths_field, parse_priority_field,
    parse_user_invocable_field, trim_ecmascript_whitespace, validate_skill_name,
};

pub const SKILL_MANIFEST_FILE: &str = "SKILL.md";

/// Load extension skills from a directory. A missing/unreadable root and
/// individual invalid entries are best-effort omissions, matching the source
/// loader's behavior.
pub async fn load_skills_from_dir(base_dir: impl AsRef<Path>) -> Vec<SkillConfig> {
    let mut entries = match tokio::fs::read_dir(base_dir.as_ref()).await {
        Ok(entries) => entries,
        Err(_) => return Vec::new(),
    };
    // Node's `readdir` resolves with the complete directory listing or throws.
    // Buffer the Rust iterator first so an iteration error cannot return a
    // partial set of skills after some entries have already been loaded.
    let mut directory_entries = Vec::new();
    loop {
        match entries.next_entry().await {
            Ok(Some(entry)) => directory_entries.push(entry),
            Ok(None) => break,
            Err(_) => return Vec::new(),
        }
    }

    let mut skills = Vec::new();
    for entry in directory_entries {
        let entry_type = match entry.file_type().await {
            Ok(file_type) => file_type,
            Err(_) => continue,
        };
        if !entry_type.is_dir() && !entry_type.is_symlink() {
            continue;
        }

        let skill_dir = entry.path();
        if entry_type.is_symlink() {
            match validate_symlink_target(&skill_dir) {
                SymlinkTargetCheck::Valid { .. } => {}
                SymlinkTargetCheck::Invalid { .. } => continue,
            }
        }

        let skill_manifest = skill_dir.join(SKILL_MANIFEST_FILE);
        let content = match tokio::fs::read(&skill_manifest).await {
            Ok(content) => String::from_utf8_lossy(&content).into_owned(),
            Err(_) => continue,
        };
        if let Ok(config) = parse_skill_content(&content, &skill_manifest) {
            skills.push(config);
        }
    }
    skills
}

/// Parse the SKILL.md format used by extension skills.
pub fn parse_skill_content(
    content: &str,
    file_path: impl AsRef<Path>,
) -> Result<SkillConfig, SkillError> {
    let file_path = file_path.as_ref();
    let normalized = normalize_content(content);
    let Some((frontmatter_yaml, body)) = split_frontmatter(&normalized) else {
        return Err(SkillError::new(
            "Invalid format: missing YAML frontmatter",
            SkillErrorCode::ParseError,
            None,
        ));
    };
    let frontmatter = parse_yaml_frontmatter(frontmatter_yaml)
        .map_err(|message| SkillError::new(message, SkillErrorCode::ParseError, None))?;

    let name_raw = frontmatter.get("name");
    if name_raw.is_none_or(Value::is_null) || name_raw == Some(&Value::String(String::new())) {
        return Err(SkillError::new(
            "Missing \"name\" in frontmatter",
            SkillErrorCode::ParseError,
            None,
        ));
    }
    let name = js_string(name_raw.expect("checked name"));
    validate_skill_name(&name).map_err(|message| {
        SkillError::new(message, SkillErrorCode::InvalidName, Some(name.clone()))
    })?;

    let description_raw = frontmatter.get("description");
    if description_raw.is_none_or(Value::is_null)
        || description_raw == Some(&Value::String(String::new()))
    {
        return Err(SkillError::new(
            "Missing \"description\" in frontmatter",
            SkillErrorCode::ParseError,
            Some(name.clone()),
        ));
    }
    let description = js_string(description_raw.expect("checked description"));

    // The source uses JavaScript `String(value)` when mapping these arrays.
    // Convert members with the loader's JS-compatible coercion before using
    // the shared validators, whose JSON number formatting is Rust-specific.
    let allowed_tools_frontmatter = stringify_array_members(&frontmatter, "allowedTools");
    let allowed_tools =
        parse_allowed_tools_field(&allowed_tools_frontmatter).map_err(|message| {
            SkillError::new(message, SkillErrorCode::ParseError, Some(name.clone()))
        })?;
    let model = parse_model_field(&frontmatter).map_err(|message| {
        SkillError::new(message, SkillErrorCode::ParseError, Some(name.clone()))
    })?;
    let paths_frontmatter = stringify_array_members(&frontmatter, "paths");
    let paths = parse_paths_field(&paths_frontmatter).map_err(|message| {
        SkillError::new(message, SkillErrorCode::ParseError, Some(name.clone()))
    })?;
    let warn_priority = |message: &str| eprintln!("[SKILL_LOAD] {message}");
    let priority = parse_priority_field(&frontmatter, file_path, Some(&warn_priority));
    let argument_hint = frontmatter
        .get("argument-hint")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let when_to_use = frontmatter
        .get("when_to_use")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let disable_model_invocation = match frontmatter.get("disable-model-invocation") {
        Some(Value::Bool(true)) => Some(true),
        Some(Value::String(value)) if value == "true" => Some(true),
        _ => None,
    };
    let user_invocable = parse_user_invocable_field(&frontmatter);
    let skill_root = file_path.parent().map(Path::to_path_buf);
    let config = SkillConfig {
        name: name.clone(),
        description,
        allowed_tools,
        hooks: None,
        model,
        level: SkillLevel::Extension,
        file_path: file_path.to_path_buf(),
        skill_root,
        body: trim_ecmascript_whitespace(body).to_owned(),
        extension_name: None,
        argument_hint,
        when_to_use,
        disable_model_invocation,
        user_invocable,
        paths,
        priority,
    };

    let validation = validate_config(&config);
    if !validation.is_valid {
        return Err(SkillError::new(
            format!("Validation failed: {}", validation.errors.join(", ")),
            SkillErrorCode::InvalidConfig,
            Some(name),
        ));
    }
    Ok(config)
}

/// Validate the statically typed fields that can be supplied by callers
/// outside the frontmatter parser. Parser-only type and name checks happen
/// before `SkillConfig` is constructed.
pub fn validate_config(config: &SkillConfig) -> SkillValidationResult {
    let mut result = SkillValidationResult {
        is_valid: true,
        errors: Vec::new(),
        warnings: Vec::new(),
    };
    if trim_ecmascript_whitespace(&config.name).is_empty() {
        result.errors.push("\"name\" cannot be empty".to_owned());
    }
    if trim_ecmascript_whitespace(&config.description).is_empty() {
        result
            .errors
            .push("\"description\" cannot be empty".to_owned());
    }
    if config
        .priority
        .is_some_and(|priority| !priority.is_finite())
    {
        result
            .errors
            .push("\"priority\" must be a finite number".to_owned());
    }
    if trim_ecmascript_whitespace(&config.body).is_empty() {
        result.warnings.push("Skill body is empty".to_owned());
    }
    result.is_valid = result.errors.is_empty();
    result
}

/// Normalize a UTF-8 skill document by stripping its BOM and folding CRLF or
/// legacy CR line endings to LF.
pub fn normalize_content(content: &str) -> String {
    content
        .strip_prefix('\u{feff}')
        .unwrap_or(content)
        .replace("\r\n", "\n")
        .replace('\r', "\n")
}

fn split_frontmatter(content: &str) -> Option<(&str, &str)> {
    if !content.starts_with("---\n") {
        return None;
    }
    let mut search_from = 4;
    while let Some(offset) = content[search_from..].find("\n---") {
        let delimiter_start = search_from + offset + 1;
        let after_delimiter = delimiter_start + 3;
        if after_delimiter == content.len()
            || content.as_bytes().get(after_delimiter) == Some(&b'\n')
        {
            let yaml = &content[4..delimiter_start - 1];
            let body_start = if after_delimiter < content.len() {
                after_delimiter + 1
            } else {
                after_delimiter
            };
            return Some((yaml, &content[body_start..]));
        }
        search_from = delimiter_start + 1;
    }
    None
}

/// Parse the core YAML values used by skill frontmatter: root mappings,
/// scalar values, inline arrays/maps, indented arrays/maps, and literal/folded
/// block strings. YAML anchors, tags, and complex-key syntax are outside the
/// skill metadata contract; this parser never evaluates tags or aliases.
pub(super) fn parse_yaml_frontmatter(input: &str) -> Result<Map<String, Value>, String> {
    let parsed = yaml_serde::from_str::<yaml_serde::Value>(input).or_else(|_| {
        let normalized = quote_plain_yaml_colon_values(input);
        yaml_serde::from_str::<yaml_serde::Value>(&normalized)
    });
    if let Ok(yaml) = parsed
        && let yaml_serde::Value::Mapping(mapping) = yaml
    {
        let mut result = Map::new();
        for (key, value) in mapping {
            if let Some(value) = sanitize_yaml_value(value, 0) {
                result.insert(yaml_value_to_js_string(key), value);
            }
        }
        return Ok(result);
    }

    // The TypeScript loader retries malformed documents with its deliberately
    // permissive legacy parser. Keep that behavior for old skill frontmatter.
    Ok(parse_simple_frontmatter(input))
}

/// The source YAML parser accepts colon-space inside block plain scalars even
/// though stricter YAML 1.2 parsers reserve that sequence. Quote those scalar
/// values on a retry so existing Canopy frontmatter keeps parsing unchanged.
fn quote_plain_yaml_colon_values(input: &str) -> String {
    input
        .lines()
        .map(|line| {
            let (indent, content) = line.split_at(line.len() - line.trim_start().len());
            let (sequence_prefix, content) = content
                .strip_prefix("- ")
                .map_or(("", content), |content| ("- ", content));
            let Some((key, value)) = content.split_once(": ") else {
                return line.to_owned();
            };
            let value_start = value.trim_start();
            if value_start.is_empty()
                || value_start.chars().next().is_some_and(|character| {
                    matches!(
                        character,
                        '\'' | '"' | '[' | '{' | '|' | '>' | '!' | '&' | '*'
                    )
                })
            {
                return line.to_owned();
            }
            let comment = yaml_inline_comment_start(value);
            let (plain, suffix) = comment
                .map(|index| (&value[..index], &value[index..]))
                .unwrap_or((value, ""));
            if !plain.contains(": ") {
                return line.to_owned();
            }
            let plain = plain.trim_end();
            let escaped = plain.replace('\\', "\\\\").replace('"', "\\\"");
            let comment_suffix = if suffix.is_empty() {
                String::new()
            } else {
                format!(" {suffix}")
            };
            format!("{indent}{sequence_prefix}{key}: \"{escaped}\"{comment_suffix}")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn yaml_inline_comment_start(value: &str) -> Option<usize> {
    let mut single_quoted = false;
    let mut double_quoted = false;
    let bytes = value.as_bytes();
    for (index, character) in value.char_indices() {
        match character {
            '\'' if !double_quoted => single_quoted = !single_quoted,
            '"' if !single_quoted => double_quoted = !double_quoted,
            '#' if !single_quoted
                && !double_quoted
                && index > 0
                && bytes[index - 1].is_ascii_whitespace() =>
            {
                return Some(index);
            }
            _ => {}
        }
    }
    None
}

const MAX_YAML_CONVERSION_DEPTH: usize = 128;

fn sanitize_yaml_value(value: yaml_serde::Value, depth: usize) -> Option<Value> {
    if depth > MAX_YAML_CONVERSION_DEPTH {
        return None;
    }
    match value {
        yaml_serde::Value::Null => None,
        yaml_serde::Value::Bool(value) => Some(Value::Bool(value)),
        yaml_serde::Value::Number(number) => {
            if let Some(number) = number
                .as_i64()
                .map(Number::from)
                .or_else(|| number.as_u64().map(Number::from))
                .or_else(|| number.as_f64().and_then(Number::from_f64))
            {
                Some(Value::Number(number))
            } else {
                Some(Value::String(yaml_number_to_js_string(&number)))
            }
        }
        yaml_serde::Value::String(value) => Some(Value::String(value)),
        yaml_serde::Value::Sequence(values) => Some(Value::Array(
            values
                .into_iter()
                .filter_map(|value| sanitize_yaml_value(value, depth + 1))
                .collect(),
        )),
        yaml_serde::Value::Mapping(values) => {
            let mut object = Map::new();
            for (key, value) in values {
                if let Some(value) = sanitize_yaml_value(value, depth + 1) {
                    object.insert(yaml_value_to_js_string(key), value);
                }
            }
            Some(Value::Object(object))
        }
        yaml_serde::Value::Tagged(tagged) => {
            let tag = tagged.tag.to_string().to_ascii_lowercase();
            if tag.contains("binary") {
                let encoded = yaml_value_to_js_string(tagged.value.clone());
                if let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(encoded) {
                    return Some(Value::String(String::from_utf8_lossy(&bytes).into_owned()));
                }
            }
            sanitize_yaml_value(tagged.value, depth + 1)
        }
    }
}

fn yaml_value_to_js_string(value: yaml_serde::Value) -> String {
    match value {
        yaml_serde::Value::Null => "null".to_owned(),
        yaml_serde::Value::Bool(value) => value.to_string(),
        yaml_serde::Value::Number(value) => yaml_number_to_js_string(&value),
        yaml_serde::Value::String(value) => value,
        yaml_serde::Value::Sequence(values) => values
            .into_iter()
            .map(|value| match value {
                yaml_serde::Value::Null => String::new(),
                value => yaml_value_to_js_string(value),
            })
            .collect::<Vec<_>>()
            .join(","),
        yaml_serde::Value::Mapping(_) => "[object Object]".to_owned(),
        yaml_serde::Value::Tagged(value) => yaml_value_to_js_string(value.value),
    }
}

fn yaml_number_to_js_string(value: &yaml_serde::Number) -> String {
    if let Some(number) = value.as_f64().and_then(Number::from_f64) {
        return js_number_to_string(&number);
    }
    let rendered = value.to_string();
    match rendered.to_ascii_lowercase().as_str() {
        ".inf" | "+.inf" => "Infinity".to_owned(),
        "-.inf" => "-Infinity".to_owned(),
        ".nan" => "NaN".to_owned(),
        _ => rendered.strip_suffix(".0").unwrap_or(&rendered).to_owned(),
    }
}

fn parse_simple_frontmatter(input: &str) -> Map<String, Value> {
    let lines = input
        .lines()
        .filter(|line| {
            let trimmed = trim_ecmascript_whitespace(line);
            !trimmed.is_empty() && !trimmed.starts_with('#')
        })
        .collect::<Vec<_>>();
    let mut result = Map::new();
    let mut current_key = String::new();
    let mut current_array = Vec::new();
    let mut in_array = false;
    let mut current_object = Map::new();
    let mut in_object = false;
    let mut object_key = String::new();

    for (index, line) in lines.iter().enumerate() {
        if let Some(item_raw) = line.strip_prefix("  - ") {
            if !in_array {
                in_array = true;
                current_array.clear();
            }
            current_array.push(parse_simple_yaml_value(trim_ecmascript_whitespace(
                item_raw,
            )));
            continue;
        }

        if in_array {
            result.insert(
                std::mem::take(&mut current_key),
                Value::Array(std::mem::take(&mut current_array)),
            );
            in_array = false;
        }

        if line.starts_with("  ") && in_object {
            if let Some((key, value)) = split_simple_yaml_pair(trim_ecmascript_whitespace(line)) {
                current_object.insert(
                    trim_ecmascript_whitespace(key).to_owned(),
                    parse_simple_yaml_value(trim_ecmascript_whitespace(value)),
                );
            }
            continue;
        }

        if in_object && !line.starts_with("  ") {
            result.insert(
                std::mem::take(&mut object_key),
                Value::Object(std::mem::take(&mut current_object)),
            );
            in_object = false;
        }

        let Some((key, value)) = split_simple_yaml_pair(line) else {
            continue;
        };
        let key = trim_ecmascript_whitespace(key).to_owned();
        let value = trim_ecmascript_whitespace(value);
        if value.is_empty() {
            current_key = key;
            if let Some(next_line) = lines.get(index + 1) {
                if next_line.starts_with("  - ") {
                    continue;
                }
                if next_line.starts_with("  ") {
                    in_object = true;
                    object_key = std::mem::take(&mut current_key);
                    current_object = Map::new();
                    continue;
                }
            }
        } else {
            result.insert(key, parse_simple_yaml_value(value));
        }
    }

    if in_array {
        result.insert(current_key, Value::Array(current_array));
    }
    if in_object {
        result.insert(object_key, Value::Object(current_object));
    }

    sanitize_json_nulls(Value::Object(result))
        .and_then(|value| value.as_object().cloned())
        .unwrap_or_default()
}

fn split_simple_yaml_pair(line: &str) -> Option<(&str, &str)> {
    line.split_once(':')
}

fn parse_simple_yaml_value(value: &str) -> Value {
    let value = trim_ecmascript_whitespace(value);
    match value {
        "true" => return Value::Bool(true),
        "false" => return Value::Bool(false),
        "null" => return Value::Null,
        "" => return Value::String(String::new()),
        _ => {}
    }
    if value.len() >= 2 && value.starts_with('"') && value.ends_with('"') {
        return Value::String(
            value[1..value.len() - 1]
                .replace("\\\"", "\"")
                .replace("\\\\", "\\"),
        );
    }
    if let Some(number) = parse_simple_js_number(value) {
        return Value::Number(number);
    }
    Value::String(value.to_owned())
}

fn parse_simple_js_number(input: &str) -> Option<Number> {
    if let Some(value) = input
        .strip_prefix("0x")
        .or_else(|| input.strip_prefix("0X"))
    {
        return u64::from_str_radix(value, 16).ok().map(Number::from);
    }
    if let Some(value) = input
        .strip_prefix("0b")
        .or_else(|| input.strip_prefix("0B"))
    {
        return u64::from_str_radix(value, 2).ok().map(Number::from);
    }
    if let Some(value) = input
        .strip_prefix("0o")
        .or_else(|| input.strip_prefix("0O"))
    {
        return u64::from_str_radix(value, 8).ok().map(Number::from);
    }
    let number = input.parse::<f64>().ok()?;
    number
        .is_finite()
        .then(|| Number::from_f64(number))
        .flatten()
}

fn sanitize_json_nulls(value: Value) -> Option<Value> {
    match value {
        Value::Null => None,
        Value::Array(values) => Some(Value::Array(
            values.into_iter().filter_map(sanitize_json_nulls).collect(),
        )),
        Value::Object(values) => Some(Value::Object(
            values
                .into_iter()
                .filter_map(|(key, value)| sanitize_json_nulls(value).map(|value| (key, value)))
                .collect(),
        )),
        value => Some(value),
    }
}

fn js_string(value: &Value) -> String {
    match value {
        Value::Null => "null".to_owned(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => js_number_to_string(value),
        Value::String(value) => value.clone(),
        Value::Array(values) => values
            .iter()
            .map(|value| {
                if value.is_null() {
                    String::new()
                } else {
                    js_string(value)
                }
            })
            .collect::<Vec<_>>()
            .join(","),
        Value::Object(_) => "[object Object]".to_owned(),
    }
}

/// Prepare an optional frontmatter array for a shared parser that applies
/// `String(value)` to each member. Pre-stringifying keeps JS coercion semantics
/// for numbers (including integral floats and large integers).
fn stringify_array_members(frontmatter: &Map<String, Value>, key: &str) -> Map<String, Value> {
    let mut normalized = frontmatter.clone();
    if let Some(Value::Array(values)) = normalized.get_mut(key) {
        *values = values
            .iter()
            .map(|value| Value::String(js_string(value)))
            .collect();
    }
    normalized
}

/// Render a JSON number as JavaScript `String(number)` does. Both runtimes use
/// shortest round-trippable decimal digits, but JavaScript first rounds every
/// YAML number to binary64 and switches between fixed/scientific notation at
/// 1e-6 and 1e21.
fn js_number_to_string(value: &Number) -> String {
    let Some(number) = value.as_f64() else {
        return value.to_string();
    };
    if number == 0.0 {
        return "0".to_owned();
    }

    let negative = number.is_sign_negative();
    let absolute = number.abs();
    let rendered = absolute.to_string().to_ascii_lowercase();
    let (mantissa, explicit_exponent) = rendered
        .split_once('e')
        .map_or((rendered.as_str(), 0i32), |(mantissa, exponent)| {
            (mantissa, exponent.parse::<i32>().unwrap_or(0))
        });
    let decimal_position = mantissa.find('.').unwrap_or(mantissa.len()) as i32;
    let mut digits = mantissa
        .chars()
        .filter(|character| *character != '.')
        .collect::<String>();
    let mut decimal_position = decimal_position + explicit_exponent;
    while digits.starts_with('0') {
        digits.remove(0);
        decimal_position -= 1;
    }
    while digits.len() > 1 && digits.ends_with('0') {
        digits.pop();
    }

    let mut output = if !(1e-6..1e21).contains(&absolute) {
        let exponent = decimal_position - 1;
        let mut scientific = digits[..1].to_owned();
        if digits.len() > 1 {
            scientific.push('.');
            scientific.push_str(&digits[1..]);
        }
        scientific.push('e');
        if exponent >= 0 {
            scientific.push('+');
        }
        scientific.push_str(&exponent.to_string());
        scientific
    } else if decimal_position <= 0 {
        format!("0.{}{}", "0".repeat((-decimal_position) as usize), digits)
    } else if decimal_position as usize >= digits.len() {
        format!(
            "{}{}",
            digits,
            "0".repeat(decimal_position as usize - digits.len())
        )
    } else {
        let split = decimal_position as usize;
        format!("{}.{}", &digits[..split], &digits[split..])
    };
    if negative {
        output.insert(0, '-');
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_root(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "canopy-skill-load-{label}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos()
        ))
    }

    #[test]
    fn parses_bom_crlf_arrays_metadata_and_body() {
        let content = "\u{feff}---\r\nname: 中文助手\r\ndescription: A helper: with detail\r\nallowedTools:\r\n  - read_file\r\n  - write_file\r\nargument-hint: '[topic]'\r\nmodel: ' qwen-max '\r\nwhen_to_use: Use for focused tasks\r\ndisable-model-invocation: true\r\nuser-invocable: 'false'\r\npaths:\r\n  - 'src/**/*.rs'\r\npriority: 7.5\r\n---\r\n\r\n  Skill body.  \r\n";
        let config = parse_skill_content(content, "/extensions/skills/helper/SKILL.md").unwrap();
        assert_eq!(config.name, "中文助手");
        assert_eq!(config.description, "A helper: with detail");
        assert_eq!(
            config.allowed_tools,
            Some(vec!["read_file".to_owned(), "write_file".to_owned()])
        );
        assert_eq!(config.argument_hint.as_deref(), Some("[topic]"));
        assert_eq!(config.model.as_deref(), Some("qwen-max"));
        assert_eq!(config.when_to_use.as_deref(), Some("Use for focused tasks"));
        assert_eq!(config.disable_model_invocation, Some(true));
        assert_eq!(config.user_invocable, Some(false));
        assert_eq!(config.paths, Some(vec!["src/**/*.rs".to_owned()]));
        assert_eq!(config.priority, Some(7.5));
        assert_eq!(config.body, "Skill body.");
        assert_eq!(
            config.skill_root,
            Some(PathBuf::from("/extensions/skills/helper"))
        );
        assert_eq!(config.level, SkillLevel::Extension);
    }

    #[test]
    fn allows_frontmatter_without_body_or_trailing_newline() {
        let config = parse_skill_content(
            "---\nname: bare\ndescription: Bare skill\n---",
            "/skills/bare/SKILL.md",
        )
        .unwrap();
        assert_eq!(config.body, "");
        assert_eq!(
            validate_config(&config).warnings,
            vec!["Skill body is empty"]
        );
    }

    #[test]
    fn invalid_names_and_escaping_path_patterns_fail_with_typed_codes() {
        let invalid_name = parse_skill_content(
            "---\nname: 'bad name'\ndescription: Helper\n---\nbody",
            "/skills/bad/SKILL.md",
        )
        .unwrap_err();
        assert_eq!(invalid_name.code, SkillErrorCode::InvalidName);
        let escaping_paths = parse_skill_content(
            "---\nname: safe\ndescription: Helper\npaths:\n  - '../secret/**'\n---\nbody",
            "/skills/safe/SKILL.md",
        )
        .unwrap_err();
        assert_eq!(escaping_paths.code, SkillErrorCode::ParseError);
        assert!(escaping_paths.message.contains("escapes the project root"));
    }

    #[tokio::test]
    async fn discovery_skips_files_and_loads_external_directory_symlinks() {
        let root = temp_root("discover");
        let skills = root.join("skills");
        let real_skill = root.join("shared/helper");
        tokio::fs::create_dir_all(&skills).await.unwrap();
        tokio::fs::create_dir_all(&real_skill).await.unwrap();
        tokio::fs::write(skills.join("not-skill.txt"), "ignored")
            .await
            .unwrap();
        tokio::fs::write(
            real_skill.join(SKILL_MANIFEST_FILE),
            "---\nname: shared-helper\ndescription: Shared\n---\nbody\n",
        )
        .await
        .unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(&real_skill, skills.join("shared-link")).unwrap();
        #[cfg(windows)]
        std::os::windows::fs::symlink_dir(&real_skill, skills.join("shared-link")).unwrap();

        let loaded = load_skills_from_dir(&skills).await;
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].name, "shared-helper");
        assert_eq!(loaded[0].file_path, skills.join("shared-link/SKILL.md"));
        tokio::fs::remove_dir_all(root).await.unwrap();
    }

    #[tokio::test]
    async fn missing_roots_and_invalid_entries_return_no_skills() {
        let missing = temp_root("missing");
        assert!(load_skills_from_dir(missing).await.is_empty());
        let root = temp_root("invalid");
        tokio::fs::create_dir_all(&root).await.unwrap();
        tokio::fs::create_dir(root.join("invalid")).await.unwrap();
        tokio::fs::write(root.join("invalid/SKILL.md"), "not frontmatter")
            .await
            .unwrap();
        assert!(load_skills_from_dir(&root).await.is_empty());
        tokio::fs::remove_dir_all(root).await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn discovery_skips_symlinks_to_files_and_dangling_symlinks() {
        let root = temp_root("invalid-links");
        let skills = root.join("skills");
        tokio::fs::create_dir_all(&skills).await.unwrap();
        let file = root.join("not-a-skill-dir");
        tokio::fs::write(&file, "plain file").await.unwrap();
        std::os::unix::fs::symlink(&file, skills.join("file-link")).unwrap();
        std::os::unix::fs::symlink(root.join("missing-dir"), skills.join("broken-link")).unwrap();
        assert!(load_skills_from_dir(&skills).await.is_empty());
        tokio::fs::remove_dir_all(root).await.unwrap();
    }

    #[test]
    fn yaml_parser_supports_flow_sequences_maps_and_block_strings() {
        let parsed = parse_yaml_frontmatter(
            "name: x\ndescription: >\n  folded\n  description\npaths: [src/**, 'tests/**']\nmetadata: { key: value }\n",
        )
        .unwrap();
        assert_eq!(parsed["description"], json!("folded description\n"));
        assert_eq!(parsed["paths"], json!(["src/**", "tests/**"]));
        assert_eq!(parsed["metadata"], json!({"key":"value"}));
    }

    #[test]
    fn yaml_parser_supports_nested_values_and_aliases() {
        let parsed = parse_yaml_frontmatter(
            "name: x\ndescription: helper\nmetadata: &shared\n  nested:\n    enabled: true\nalias: *shared\n",
        )
        .unwrap();
        assert_eq!(parsed["metadata"], json!({"nested":{"enabled":true}}));
        assert_eq!(parsed["alias"], json!({"nested":{"enabled":true}}));
    }

    #[test]
    fn malformed_yaml_uses_the_permissive_source_fallback() {
        let config = parse_skill_content(
            "---\nname: test-skill\ndescription: a test skill\nextra: {key: [nested unclosed\n---\nBody.\n",
            "/test/extension/skills/test-skill/SKILL.md",
        )
        .unwrap();
        assert_eq!(config.name, "test-skill");
        assert_eq!(config.description, "a test skill");
    }

    #[test]
    fn plain_scalars_keep_apostrophes_and_strip_yaml_comments() {
        let parsed =
            parse_yaml_frontmatter("name: don't-break\ndescription: It's a helper # author note\n")
                .unwrap();
        assert_eq!(parsed["name"], json!("don't-break"));
        assert_eq!(parsed["description"], json!("It's a helper"));
    }
}
