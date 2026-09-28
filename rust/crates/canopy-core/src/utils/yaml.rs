//! YAML parsing and serialization helpers.
//!
//! Port of `packages/core/src/utils/yaml-parser.ts`. Full YAML documents are
//! parsed with `yaml_serde`; malformed input falls back to the small parser
//! used for frontmatter. Null values are removed recursively from parsed
//! objects and arrays.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use chrono::{DateTime, NaiveDate, NaiveDateTime, SecondsFormat, Utc};
use serde_json::{Map, Number as JsonNumber, Value as JsonValue};
use yaml_serde::value::{Mapping, TaggedValue};
use yaml_serde::{Number as YamlNumber, Value as YamlValue};

pub type YamlObject = Map<String, JsonValue>;

/// Optional formatting controls corresponding to `yaml.stringify` options.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StringifyOptions {
    /// Maximum preferred output line width. Zero disables wrapping.
    pub line_width: Option<usize>,
    /// Minimum content width retained before wrapping a line.
    pub min_content_width: Option<usize>,
}

/// Parse YAML into an object, falling back to the source's permissive
/// frontmatter parser if the full parser fails or returns a non-object value.
pub fn parse(yaml_string: &str) -> YamlObject {
    // The YAML spec clips the final line break for `>` and `|` block
    // scalars, including when the source text ends immediately after the last
    // content line. Supplying the missing document line terminator lets
    // libyaml preserve that clipped newline in the decoded scalar.
    let parse_input = if yaml_string.ends_with('\n') {
        yaml_string.to_owned()
    } else {
        format!("{yaml_string}\n")
    };
    let tagged_input = preserve_explicit_binary_tag(&parse_input);
    if let Ok(value) = yaml_serde::from_str::<YamlValue>(&tagged_input) {
        let value = unwrap_tags(value);
        if let YamlValue::Mapping(mapping) = value {
            return sanitize_mapping(mapping);
        }
    }
    strip_json_nulls_from_map(parse_simple(yaml_string))
}

/// Serialize an object as YAML. The default line width is zero, matching the
/// TypeScript helper's no-wrap default.
pub fn stringify(obj: &YamlObject, options: Option<StringifyOptions>) -> String {
    let options = options.unwrap_or_default();
    let line_width = options.line_width.unwrap_or(0);
    if line_width == 0 {
        return yaml_serde::to_string(obj).unwrap_or_else(|_| "{}\n".to_owned());
    }

    let min_content_width = options.min_content_width.unwrap_or(20);
    let mut output = String::new();
    write_mapping(obj, 0, line_width, min_content_width, &mut output);
    output.push('\n');
    output
}

fn sanitize_mapping(mapping: Mapping) -> YamlObject {
    let mut output = Map::new();
    for (key, value) in mapping {
        if let Some(value) = sanitize_yaml_value(value) {
            output.insert(yaml_key_to_string(key), value);
        }
    }
    output
}

fn sanitize_yaml_value(value: YamlValue) -> Option<JsonValue> {
    match value {
        YamlValue::Null => None,
        YamlValue::Bool(value) => Some(JsonValue::Bool(value)),
        YamlValue::Number(value) => json_number_from_yaml(value),
        YamlValue::String(value) => Some(JsonValue::String(value)),
        YamlValue::Sequence(values) => Some(JsonValue::Array(
            values.into_iter().filter_map(sanitize_yaml_value).collect(),
        )),
        YamlValue::Mapping(mapping) => Some(JsonValue::Object(sanitize_mapping(mapping))),
        YamlValue::Tagged(tagged) => sanitize_tagged_value(*tagged),
    }
}

fn sanitize_tagged_value(tagged: TaggedValue) -> Option<JsonValue> {
    let TaggedValue { tag, value } = tagged;
    if tag == "timestamp" || tag == "tag:yaml.org,2002:timestamp" {
        return Some(JsonValue::String(normalize_timestamp(&yaml_scalar_string(
            &value,
        ))));
    }
    if tag == "binary" || tag == "tag:yaml.org,2002:binary" || tag == "canopy-binary" {
        let encoded = yaml_scalar_string(&value);
        let compact = encoded
            .chars()
            .filter(|character| !character.is_whitespace())
            .collect::<String>();
        let decoded = BASE64_STANDARD
            .decode(compact.as_bytes())
            .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
            .unwrap_or(encoded);
        return Some(JsonValue::String(decoded));
    }
    sanitize_yaml_value(value)
}

/// yaml_serde resolves `!!binary` to a string and discards the standard tag,
/// while the source parser turns that value into bytes before sanitization.
/// Rewrite only explicit tag tokens to a local tag so the value survives as a
/// `TaggedValue` and can be decoded by `sanitize_tagged_value`.
fn preserve_explicit_binary_tag(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut block_scalar_indent = None;
    for line in input.split_inclusive('\n') {
        let line_body = line.strip_suffix('\n').unwrap_or(line);
        let content = line_body.strip_suffix('\r').unwrap_or(line_body);
        let line_ending = if line.ends_with("\r\n") {
            "\r\n"
        } else if line.ends_with('\n') {
            "\n"
        } else {
            ""
        };
        let indent = content.bytes().take_while(|byte| *byte == b' ').count();
        if let Some(base_indent) = block_scalar_indent {
            if content.trim().is_empty() || indent > base_indent {
                output.push_str(line);
                continue;
            }
            block_scalar_indent = None;
        }

        output.push_str(&rewrite_binary_tag_in_line(content));
        output.push_str(line_ending);
        if is_block_scalar_header(content) {
            block_scalar_indent = Some(indent);
        }
    }
    output
}

fn rewrite_binary_tag_in_line(line: &str) -> String {
    let bytes = line.as_bytes();
    let tag = b"!!binary";
    let mut output = String::with_capacity(line.len());
    let mut copied_until = 0;
    let mut index = 0;
    let mut quote = None;

    while index < bytes.len() {
        match quote {
            Some(b'\'') => {
                if bytes[index] == b'\'' {
                    if bytes.get(index + 1) == Some(&b'\'') {
                        index += 2;
                    } else {
                        quote = None;
                        index += 1;
                    }
                } else {
                    index += 1;
                }
            }
            Some(b'"') => {
                if bytes[index] == b'\\' {
                    index += 2;
                } else if bytes[index] == b'"' {
                    quote = None;
                    index += 1;
                } else {
                    index += 1;
                }
            }
            _ => {
                let preceded_by_boundary = index == 0
                    || bytes[index - 1].is_ascii_whitespace()
                    || matches!(bytes[index - 1], b':' | b'[' | b'{' | b',');
                if bytes[index..].starts_with(tag) {
                    let after_tag = index + tag.len();
                    let followed_by_boundary = after_tag == bytes.len()
                        || bytes[after_tag].is_ascii_whitespace()
                        || matches!(bytes[after_tag], b',' | b']' | b'}');
                    if preceded_by_boundary && followed_by_boundary {
                        output.push_str(&line[copied_until..index]);
                        output.push_str("!canopy-binary");
                        index = after_tag;
                        copied_until = index;
                    } else {
                        index += 1;
                    }
                } else if bytes[index] == b'#'
                    && (index == 0 || matches!(bytes[index - 1], b' ' | b'\t'))
                {
                    break;
                } else if bytes[index] == b'\'' || bytes[index] == b'"' {
                    quote = Some(bytes[index]);
                    index += 1;
                } else {
                    index += 1;
                }
            }
        }
    }
    output.push_str(&line[copied_until..]);
    output
}

fn is_block_scalar_header(line: &str) -> bool {
    let header = yaml_comment_offset(line)
        .map(|offset| &line[..offset])
        .unwrap_or(line)
        .trim_end();
    let Some(indicator) = header.split_whitespace().last() else {
        return false;
    };
    let Some(marker) = indicator.chars().next() else {
        return false;
    };
    matches!(marker, '|' | '>')
        && indicator[marker.len_utf8()..]
            .chars()
            .all(|character| matches!(character, '+' | '-' | '0'..='9'))
}

fn yaml_comment_offset(line: &str) -> Option<usize> {
    let bytes = line.as_bytes();
    let mut index = 0;
    let mut quote = None;
    while index < bytes.len() {
        match quote {
            Some(b'\'') => {
                if bytes[index] == b'\'' {
                    if bytes.get(index + 1) == Some(&b'\'') {
                        index += 2;
                    } else {
                        quote = None;
                        index += 1;
                    }
                } else {
                    index += 1;
                }
            }
            Some(b'"') => {
                if bytes[index] == b'\\' {
                    index += 2;
                } else if bytes[index] == b'"' {
                    quote = None;
                    index += 1;
                } else {
                    index += 1;
                }
            }
            _ if bytes[index] == b'#'
                && (index == 0 || matches!(bytes[index - 1], b' ' | b'\t')) =>
            {
                return Some(index);
            }
            _ if bytes[index] == b'\'' || bytes[index] == b'"' => {
                quote = Some(bytes[index]);
                index += 1;
            }
            _ => index += 1,
        }
    }
    None
}

fn json_number_from_yaml(value: YamlNumber) -> Option<JsonValue> {
    let number = value.as_f64()?;
    if !number.is_finite() {
        // serde_json cannot represent NaN or infinities. Keep YAML's spelling
        // as a string rather than turning the entire document into fallback.
        return Some(JsonValue::String(value.to_string()));
    }
    const MAX_SAFE_INTEGER: f64 = 9_007_199_254_740_991.0;
    if number.fract() == 0.0 && number.abs() <= MAX_SAFE_INTEGER {
        if number < 0.0 {
            return Some(JsonValue::Number(JsonNumber::from(number as i64)));
        }
        return Some(JsonValue::Number(JsonNumber::from(number as u64)));
    }
    JsonNumber::from_f64(number).map(JsonValue::Number)
}

fn yaml_key_to_string(key: YamlValue) -> String {
    match unwrap_tags(key) {
        YamlValue::Null => "null".to_owned(),
        YamlValue::Bool(value) => value.to_string(),
        YamlValue::Number(value) => value.to_string(),
        YamlValue::String(value) => value,
        YamlValue::Sequence(values) => values
            .into_iter()
            .map(|value| match value {
                YamlValue::Null => String::new(),
                value => yaml_scalar_string(&value),
            })
            .collect::<Vec<_>>()
            .join(","),
        YamlValue::Mapping(_) => "[object Object]".to_owned(),
        YamlValue::Tagged(_) => unreachable!("tags are removed by unwrap_tags"),
    }
}

fn yaml_scalar_string(value: &YamlValue) -> String {
    match value {
        YamlValue::Null => "null".to_owned(),
        YamlValue::Bool(value) => value.to_string(),
        YamlValue::Number(value) => value.to_string(),
        YamlValue::String(value) => value.clone(),
        YamlValue::Sequence(_) => "".to_owned(),
        YamlValue::Mapping(_) => "[object Object]".to_owned(),
        YamlValue::Tagged(tagged) => yaml_scalar_string(&tagged.value),
    }
}

fn unwrap_tags(mut value: YamlValue) -> YamlValue {
    while let YamlValue::Tagged(tagged) = value {
        value = tagged.value;
    }
    value
}

fn normalize_timestamp(value: &str) -> String {
    if let Ok(timestamp) = DateTime::parse_from_rfc3339(value) {
        return timestamp
            .with_timezone(&Utc)
            .to_rfc3339_opts(SecondsFormat::Millis, true);
    }

    if let Ok(date) = NaiveDate::parse_from_str(value, "%Y-%m-%d") {
        return date
            .and_hms_opt(0, 0, 0)
            .expect("midnight is valid")
            .and_utc()
            .to_rfc3339_opts(SecondsFormat::Millis, true);
    }

    let rfc3339_candidate = value.replacen(' ', "T", 1);
    if let Ok(timestamp) = DateTime::parse_from_rfc3339(&rfc3339_candidate) {
        return timestamp
            .with_timezone(&Utc)
            .to_rfc3339_opts(SecondsFormat::Millis, true);
    }

    for format in ["%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%dT%H:%M:%S%.f"] {
        if let Ok(timestamp) = NaiveDateTime::parse_from_str(value, format) {
            return timestamp
                .and_utc()
                .to_rfc3339_opts(SecondsFormat::Millis, true);
        }
    }
    value.to_owned()
}

fn parse_simple(yaml_string: &str) -> YamlObject {
    let lines = yaml_string
        .split('\n')
        .filter(|line| !line.trim().is_empty() && !line.trim().starts_with('#'))
        .collect::<Vec<_>>();
    let mut result = Map::new();

    let mut current_key = String::new();
    let mut current_array = Vec::<JsonValue>::new();
    let mut in_array = false;
    let mut current_object = Map::new();
    let mut in_object = false;
    let mut object_key = String::new();

    let mut index = 0;
    while index < lines.len() {
        let line = lines[index];
        if let Some(item_raw) = line.strip_prefix("  - ") {
            if !in_array {
                in_array = true;
                current_array = Vec::new();
            }
            current_array.push(parse_simple_value(item_raw.trim()));
            index += 1;
            continue;
        }

        if in_array && !line.starts_with("  - ") {
            result.insert(
                std::mem::take(&mut current_key),
                JsonValue::Array(std::mem::take(&mut current_array)),
            );
            in_array = false;
        }

        if line.starts_with("  ") && in_object {
            let (key, value) = split_first_colon(line.trim());
            current_object.insert(key.trim().to_owned(), parse_simple_value(value.trim()));
            index += 1;
            continue;
        }

        if in_object && !line.starts_with("  ") {
            result.insert(
                std::mem::take(&mut object_key),
                JsonValue::Object(std::mem::take(&mut current_object)),
            );
            in_object = false;
        }

        if let Some((key, value)) = line.split_once(':') {
            let value = value.trim();
            if value.is_empty() {
                current_key = key.trim().to_owned();
                if let Some(next_line) = lines.get(index + 1) {
                    if next_line.starts_with("  - ") {
                        index += 1;
                        continue;
                    } else if next_line.starts_with("  ") {
                        in_object = true;
                        object_key = std::mem::take(&mut current_key);
                        current_object = Map::new();
                        index += 1;
                        continue;
                    }
                }
            } else {
                result.insert(key.trim().to_owned(), parse_simple_value(value));
            }
        }
        index += 1;
    }

    if in_array {
        result.insert(current_key, JsonValue::Array(current_array));
    }
    if in_object {
        result.insert(object_key, JsonValue::Object(current_object));
    }
    result
}

fn split_first_colon(value: &str) -> (&str, &str) {
    value.split_once(':').unwrap_or((value, ""))
}

fn parse_simple_value(value: &str) -> JsonValue {
    match value {
        "true" => return JsonValue::Bool(true),
        "false" => return JsonValue::Bool(false),
        "null" => return JsonValue::Null,
        "" => return JsonValue::String(String::new()),
        _ => {}
    }

    if value.starts_with('"') && value.ends_with('"') && value.len() >= 2 {
        let unquoted = &value[1..value.len() - 1];
        return JsonValue::String(unquoted.replace("\\\"", "\"").replace("\\\\", "\\"));
    }

    if let Some(number) = parse_finite_js_number(value) {
        return number;
    }
    JsonValue::String(value.to_owned())
}

fn parse_finite_js_number(value: &str) -> Option<JsonValue> {
    let (digits, radix) = if let Some(digits) = value.strip_prefix("0x") {
        (digits, 16)
    } else if let Some(digits) = value.strip_prefix("0b") {
        (digits, 2)
    } else if let Some(digits) = value.strip_prefix("0o") {
        (digits, 8)
    } else {
        let number = value.parse::<f64>().ok()?;
        return json_number_from_yaml(YamlNumber::from(number));
    };
    let number = u64::from_str_radix(digits, radix).ok()?;
    json_number_from_yaml(YamlNumber::from(number))
}

fn strip_json_nulls_from_map(mut object: YamlObject) -> YamlObject {
    object.retain(|_, value| strip_json_nulls(value));
    object
}

fn strip_json_nulls(value: &mut JsonValue) -> bool {
    match value {
        JsonValue::Null => false,
        JsonValue::Array(values) => {
            values.retain_mut(strip_json_nulls);
            true
        }
        JsonValue::Object(object) => {
            object.retain(|_, value| strip_json_nulls(value));
            true
        }
        _ => true,
    }
}

fn write_mapping(
    object: &YamlObject,
    indent: usize,
    line_width: usize,
    min_content_width: usize,
    output: &mut String,
) {
    if object.is_empty() {
        output.push_str("{}");
        return;
    }
    for (index, (key, value)) in object.iter().enumerate() {
        if index > 0 {
            output.push('\n');
        }
        write_indent(indent, output);
        output.push_str(&serde_json::to_string(key).unwrap_or_else(|_| "\"\"".to_owned()));
        output.push(':');
        write_value_after_prefix(value, indent, line_width, min_content_width, output);
    }
}

fn write_sequence(
    values: &[JsonValue],
    indent: usize,
    line_width: usize,
    min_content_width: usize,
    output: &mut String,
) {
    if values.is_empty() {
        output.push_str("[]");
        return;
    }
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            output.push('\n');
        }
        write_indent(indent, output);
        output.push('-');
        write_value_after_prefix(value, indent, line_width, min_content_width, output);
    }
}

fn write_value_after_prefix(
    value: &JsonValue,
    indent: usize,
    line_width: usize,
    min_content_width: usize,
    output: &mut String,
) {
    match value {
        JsonValue::Object(object) if !object.is_empty() => {
            output.push('\n');
            write_mapping(object, indent + 2, line_width, min_content_width, output);
        }
        JsonValue::Array(values) if !values.is_empty() => {
            output.push('\n');
            write_sequence(values, indent + 2, line_width, min_content_width, output);
        }
        JsonValue::String(value) => {
            let content_indent = indent + 2;
            if let Some(lines) =
                wrap_single_line_string(value, line_width, min_content_width, content_indent)
            {
                output.push_str(" >-\n");
                for (index, line) in lines.iter().enumerate() {
                    if index > 0 {
                        output.push('\n');
                    }
                    write_indent(content_indent, output);
                    output.push_str(line);
                }
            } else {
                output.push(' ');
                output.push_str(&scalar_yaml(&JsonValue::String(value.clone())));
            }
        }
        _ => {
            output.push(' ');
            output.push_str(&scalar_yaml(value));
        }
    }
}

fn scalar_yaml(value: &JsonValue) -> String {
    if let JsonValue::String(value) = value
        && (value.contains('\n') || value.contains('\r'))
    {
        return serde_json::to_string(value).unwrap_or_else(|_| "\"\"".to_owned());
    }
    yaml_serde::to_string(value)
        .unwrap_or_else(|_| "null\n".to_owned())
        .trim_end_matches('\n')
        .to_owned()
}

fn wrap_single_line_string(
    value: &str,
    line_width: usize,
    min_content_width: usize,
    indent: usize,
) -> Option<Vec<String>> {
    if value.is_empty()
        || line_width == 0
        || value.contains(['\n', '\r', '\t'])
        || value.chars().any(char::is_control)
        || value.trim() != value
        || value.contains("  ")
        || !value.contains(' ')
    {
        return None;
    }
    let width = line_width.saturating_sub(indent);
    if utf16_len(value) <= width {
        return None;
    }
    let min_width = if line_width < min_content_width {
        0
    } else {
        min_content_width
    };

    let mut lines = Vec::new();
    let mut current = String::new();
    let mut current_width = 0;
    for word in value.split(' ') {
        let word_width = utf16_len(word);
        let next_width = if current.is_empty() {
            word_width
        } else {
            current_width + 1 + word_width
        };
        if !current.is_empty() && next_width > width && current_width >= min_width {
            lines.push(std::mem::take(&mut current));
            current_width = 0;
        }
        if !current.is_empty() {
            current.push(' ');
            current_width += 1;
        }
        current.push_str(word);
        current_width += word_width;
    }
    if !current.is_empty() {
        lines.push(current);
    }
    (lines.len() > 1).then_some(lines)
}

fn utf16_len(value: &str) -> usize {
    value.encode_utf16().count()
}

fn write_indent(indent: usize, output: &mut String) {
    output.push_str(&" ".repeat(indent));
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::{StringifyOptions, parse, preserve_explicit_binary_tag, stringify};

    #[test]
    fn parses_simple_arrays_objects_and_full_block_scalars() {
        assert_eq!(
            Value::Object(parse("name: test\ndescription: A test config")),
            json!({"name": "test", "description": "A test config"})
        );
        assert_eq!(
            Value::Object(parse("tools:\n  - file\n  - shell")),
            json!({"tools": ["file", "shell"]})
        );
        assert_eq!(
            Value::Object(parse("modelConfig:\n  temperature: 0.7\n  maxTokens: 1000")),
            json!({"modelConfig": {"temperature": 0.7, "maxTokens": 1000}})
        );
        assert_eq!(
            parse("description: >\n  This is a folded\n  multiline description.")
                .get("description"),
            Some(&json!("This is a folded multiline description.\n"))
        );
        assert_eq!(
            parse("description: |\n  Line one.\n  Line two.").get("description"),
            Some(&json!("Line one.\nLine two.\n"))
        );
        assert_eq!(
            parse("description: >-\n  Folded without trailing newline.").get("description"),
            Some(&json!("Folded without trailing newline."))
        );
    }

    #[test]
    fn strips_nulls_recursively_and_preserves_core_schema_strings() {
        assert_eq!(
            Value::Object(parse(
                "a: null\nb: ~\nitems: [one, null, two]\nanswer: yes\nother: no"
            )),
            json!({"items": ["one", "two"], "answer": "yes", "other": "no"})
        );
        assert_eq!(
            Value::Object(parse("metadata:\n  remove: null\n  keep: hello")),
            json!({"metadata": {"keep": "hello"}})
        );
        assert_eq!(
            parse("created: 2024-01-01").get("created"),
            Some(&json!("2024-01-01"))
        );
    }

    #[test]
    fn sanitizes_explicit_timestamp_and_binary_tags() {
        let parsed = parse("created: !!timestamp 2024-01-01\ncontent: !!binary SGVsbG8=");
        assert!(parsed.get("created").and_then(Value::as_str).is_some());
        assert_eq!(parsed.get("content"), Some(&json!("Hello")));
        let nested = parse("metadata:\n  created: !!timestamp 2024-01-01\n  note: hello");
        let metadata = nested.get("metadata").and_then(Value::as_object).unwrap();
        assert!(metadata.get("created").and_then(Value::as_str).is_some());
        assert_eq!(metadata.get("note"), Some(&json!("hello")));
    }

    #[test]
    fn rewrites_binary_tags_outside_quoted_comments_and_block_text() {
        let input = concat!(
            "quoted: \"!!binary SGVsbG8=\"\n",
            "comment: hello # !!binary SGVsbG8=\n",
            "literal: |\n",
            "  !!binary SGVsbG8=\n",
            "decoded: !!binary SGVsbG8=\n",
        );
        let expected = concat!(
            "quoted: \"!!binary SGVsbG8=\"\n",
            "comment: hello # !!binary SGVsbG8=\n",
            "literal: |\n",
            "  !!binary SGVsbG8=\n",
            "decoded: !canopy-binary SGVsbG8=\n",
        );
        assert_eq!(preserve_explicit_binary_tag(input), expected);
    }

    #[test]
    fn malformed_and_non_object_documents_use_simple_fallback() {
        let fallback = parse("name: test\noptional: null\nbroken: [unclosed");
        assert_eq!(fallback.get("name"), Some(&json!("test")));
        assert!(!fallback.contains_key("optional"));
        assert_eq!(fallback.get("broken"), Some(&json!("[unclosed")));
        assert!(parse("[not, an, object]").is_empty());
        assert!(parse("").is_empty());
        assert!(parse("# comment only").is_empty());
    }

    #[test]
    fn preserves_proto_keys_as_regular_map_entries_in_full_and_fallback_paths() {
        let parsed = parse("name: legit\n__proto__:\n  polluted: true");
        assert_eq!(parsed.get("name"), Some(&json!("legit")));
        assert_eq!(
            parsed
                .get("__proto__")
                .and_then(Value::as_object)
                .and_then(|object| object.get("polluted")),
            Some(&json!(true))
        );
        let fallback = parse("__proto__:\n  polluted: true\nname: test\nbroken: [unclosed");
        assert_eq!(fallback.get("name"), Some(&json!("test")));
        assert_eq!(
            fallback
                .get("__proto__")
                .and_then(Value::as_object)
                .and_then(|object| object.get("polluted")),
            Some(&json!(true))
        );
    }

    #[test]
    fn stringifies_nested_values_and_round_trips_string_content() {
        let cases = [
            "simplevalue",
            "value with \"quotes\"",
            "value with \\ backslash",
            "value with \\\" sequence",
            "C:\\Program Files\\\"App\"\\file.txt",
            "value:with:colons",
            "value#with#hash",
            " value with spaces ",
            "line one\nline two\nline three",
            "中文 — naïve café",
        ];
        for value in cases {
            let object = json!({"key": value}).as_object().unwrap().clone();
            assert_eq!(parse(&stringify(&object, None)), object, "{value:?}");
        }

        let object = json!({
            "mcpServers": {
                "filesystem": {
                    "type": "stdio",
                    "command": "node",
                    "args": ["/path/to/server.js"]
                }
            },
            "hooks": {
                "PreToolUse": [{
                    "matcher": "Bash",
                    "hooks": [{"type": "command", "command": "echo before"}]
                }]
            },
            "numbers": [11, 25, 333],
        })
        .as_object()
        .unwrap()
        .clone();
        assert_eq!(parse(&stringify(&object, None)), object);
    }

    #[test]
    fn line_width_and_min_content_width_wrap_folded_scalars() {
        let object = json!({
            "description": "one two three four five six"
        })
        .as_object()
        .unwrap()
        .clone();
        let wrapped = stringify(
            &object,
            Some(StringifyOptions {
                line_width: Some(12),
                min_content_width: Some(0),
            }),
        );
        assert!(wrapped.contains(": >-\n"));
        assert_eq!(parse(&wrapped), object);

        let minimum_content = stringify(
            &object,
            Some(StringifyOptions {
                line_width: Some(12),
                min_content_width: Some(8),
            }),
        );
        assert!(minimum_content.contains("one two three"));
        assert_eq!(parse(&minimum_content), object);

        let unwrapped = stringify(
            &object,
            Some(StringifyOptions {
                line_width: Some(0),
                min_content_width: Some(20),
            }),
        );
        assert!(!unwrapped.contains(": >-"));
        assert_eq!(parse(&unwrapped), object);
    }

    #[test]
    fn simple_fallback_coerces_boolean_numeric_and_quoted_values() {
        let parsed = parse("name: \"11\"\nage: 25\nenabled: true\npath: \"a\\\\b\"");
        assert_eq!(parsed.get("name"), Some(&json!("11")));
        assert_eq!(parsed.get("age"), Some(&json!(25)));
        assert_eq!(parsed.get("enabled"), Some(&json!(true)));
        assert_eq!(parsed.get("path"), Some(&json!("a\\b")));
    }
}
