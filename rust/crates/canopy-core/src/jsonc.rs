//! JSON with line and block comments, used by Canopy settings files.

use serde_json::{Map, Value};
use thiserror::Error;

#[derive(Debug, Error, Eq, PartialEq)]
pub enum JsoncError {
    #[error("{0}")]
    Parse(String),
    #[error("JSONC document root is not a JSON object.")]
    RootNotObject,
}

/// Replaces comment bytes with spaces while preserving strings and line
/// boundaries, so JSON parser diagnostics still refer to the original lines.
pub fn strip_json_comments(input: &str) -> String {
    let mut bytes = input.as_bytes().to_vec();
    let mut index = 0;
    let mut in_string = false;
    let mut escaped = false;
    while index < bytes.len() {
        if in_string {
            match bytes[index] {
                b'\\' if !escaped => escaped = true,
                b'"' if !escaped => in_string = false,
                _ => escaped = false,
            }
            index += 1;
            continue;
        }
        match bytes[index] {
            b'"' => {
                in_string = true;
                index += 1;
            }
            b'/' if bytes.get(index + 1) == Some(&b'/') => {
                bytes[index] = b' ';
                bytes[index + 1] = b' ';
                index += 2;
                while index < bytes.len() && bytes[index] != b'\n' {
                    if bytes[index] != b'\r' {
                        bytes[index] = b' ';
                    }
                    index += 1;
                }
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                bytes[index] = b' ';
                bytes[index + 1] = b' ';
                index += 2;
                while index < bytes.len() {
                    if bytes[index] == b'*' && bytes.get(index + 1) == Some(&b'/') {
                        bytes[index] = b' ';
                        bytes[index + 1] = b' ';
                        index += 2;
                        break;
                    }
                    if bytes[index] != b'\n' && bytes[index] != b'\r' {
                        bytes[index] = b' ';
                    }
                    index += 1;
                }
            }
            _ => index += 1,
        }
    }
    String::from_utf8(bytes).unwrap_or_else(|_| input.to_owned())
}

/// Parse an object using the JSONC rules used by the CLI settings editor.
/// A UTF-8 BOM, comments, and trailing commas are accepted.
pub fn parse_jsonc_object(input: &str) -> Result<Map<String, Value>, JsoncError> {
    let input = input.strip_prefix('\u{feff}').unwrap_or(input);
    let mut cleaned = strip_json_comments(input).into_bytes();
    strip_trailing_commas(&mut cleaned);
    let parsed: Value =
        serde_json::from_slice(&cleaned).map_err(|error| JsoncError::Parse(error.to_string()))?;
    parsed.as_object().cloned().ok_or(JsoncError::RootNotObject)
}

/// Applies the CLI JSONC editor's deep-merge or recursive-sync semantics to
/// parsed settings. `replace_path` identifies one updated object subtree that
/// is replaced exactly. Prototype-pollution keys in updates are ignored.
pub fn apply_jsonc_updates(
    mut current: Map<String, Value>,
    updates: &Map<String, Value>,
    sync: bool,
    replace_path: &[String],
) -> Map<String, Value> {
    apply_updates_at_path(&mut current, updates, sync, replace_path, &[]);
    current
}

/// Produces updated JSONC while retaining comments and formatting for every
/// existing object property that does not need to change.
pub fn update_jsonc_content(
    input: &str,
    updates: &Map<String, Value>,
    sync: bool,
    replace_path: &[String],
) -> Result<String, JsoncError> {
    let current = parse_jsonc_object(input)?;
    let target = apply_jsonc_updates(current, updates, sync, replace_path);
    update_jsonc_object_content(input, &target)
}

fn apply_updates_at_path(
    current: &mut Map<String, Value>,
    updates: &Map<String, Value>,
    sync: bool,
    replace_path: &[String],
    current_path: &[String],
) {
    if sync {
        current.retain(|key, _| updates.contains_key(key));
    }

    for (key, value) in updates {
        if matches!(key.as_str(), "__proto__" | "constructor" | "prototype") {
            continue;
        }
        let mut next_path = current_path.to_vec();
        next_path.push(key.clone());
        let update_object = value.as_object().filter(|object| !object.is_empty());

        if next_path == replace_path {
            if let Some(update_object) = update_object {
                let mut replacement = Map::new();
                apply_updates_at_path(&mut replacement, update_object, false, &[], &[]);
                current.insert(key.clone(), Value::Object(replacement));
            } else {
                current.insert(key.clone(), value.clone());
            }
            continue;
        }

        if let Some(update_object) = update_object {
            let mut base = current
                .remove(key)
                .and_then(|value| value.as_object().cloned())
                .unwrap_or_default();
            apply_updates_at_path(&mut base, update_object, sync, replace_path, &next_path);
            current.insert(key.clone(), Value::Object(base));
        } else {
            current.insert(key.clone(), value.clone());
        }
    }
}

/// Synchronize a root JSONC object while keeping the source text for retained
/// properties. The current Canopy caller is the trusted-folders map, whose
/// values are strings; the parser itself accepts any JSON value so unchanged
/// nested settings remain byte-for-byte intact.
pub fn update_jsonc_object_content(
    input: &str,
    target: &Map<String, Value>,
) -> Result<String, JsoncError> {
    let bom = input.starts_with('\u{feff}');
    let editable = input.strip_prefix('\u{feff}').unwrap_or(input);
    let current = parse_jsonc_object(editable)?;
    let scanned = scan_root_object(editable)?;

    let last_occurrence = scanned
        .members
        .iter()
        .enumerate()
        .map(|(index, member)| (member.key.as_str(), index))
        .collect::<std::collections::HashMap<_, _>>();
    let keep_member = scanned
        .members
        .iter()
        .enumerate()
        .map(|(index, member)| {
            last_occurrence.get(member.key.as_str()) == Some(&index)
                && target.contains_key(&member.key)
        })
        .collect::<Vec<_>>();
    let duplicates = last_occurrence.len() != scanned.members.len();
    let changed = current.len() != target.len()
        || current
            .iter()
            .any(|(key, value)| target.get(key) != Some(value));
    if !changed && !duplicates {
        return Ok(input.to_owned());
    }

    let mut output = String::with_capacity(editable.len().saturating_add(128));
    output.push_str(&editable[..scanned.open + 1]);

    let mut retained = Vec::new();
    for (index, member) in scanned.members.iter().enumerate() {
        let is_effective_occurrence = last_occurrence.get(member.key.as_str()) == Some(&index);
        if !keep_member[index] || !is_effective_occurrence {
            continue;
        }
        let mut segment = editable[member.segment_start..member.segment_end].to_owned();
        let target_value = &target[&member.key];
        let mut effective_value_end = member.value_end;
        if current.get(&member.key) != Some(target_value) {
            let value_start = member.value_start - member.segment_start;
            let value_end = member.value_end - member.segment_start;
            let replacement = match (current.get(&member.key), target_value.as_object()) {
                (Some(Value::Object(_)), Some(target_object)) => update_jsonc_object_content(
                    &editable[member.value_start..member.value_end],
                    target_object,
                )?,
                _ => serde_json::to_string(target_value)
                    .map_err(|error| JsoncError::Parse(error.to_string()))?,
            };
            effective_value_end = member.segment_start + value_start + replacement.len();
            segment.replace_range(value_start..value_end, &replacement);
        }
        if index > 0 && !keep_member[index - 1] {
            effective_value_end =
                effective_value_end.saturating_sub(remove_leading_inline_comment(&mut segment));
        }
        retained.push((
            segment,
            member.comma_after,
            effective_value_end,
            member.segment_start,
        ));
    }

    let mut retained_keys = std::collections::HashSet::new();
    for member in &scanned.members {
        if last_occurrence
            .get(member.key.as_str())
            .is_some_and(|index| {
                scanned
                    .members
                    .get(*index)
                    .is_some_and(|effective| target.contains_key(&effective.key))
            })
        {
            retained_keys.insert(member.key.as_str());
        }
    }

    let additions = target
        .iter()
        .filter(|(key, _)| !retained_keys.contains(key.as_str()))
        .map(|(key, value)| {
            Ok((
                serde_json::to_string(key).map_err(|error| JsoncError::Parse(error.to_string()))?,
                serde_json::to_string(value)
                    .map_err(|error| JsoncError::Parse(error.to_string()))?,
            ))
        })
        .collect::<Result<Vec<_>, JsoncError>>()?;

    if !additions.is_empty() {
        if let Some((last_segment, comma_after, value_end, segment_start)) = retained.last_mut() {
            if !*comma_after {
                let local_value_end = value_end.saturating_sub(*segment_start);
                let local_value_end = local_value_end.min(last_segment.len());
                last_segment.insert(local_value_end, ',');
            }
        }
    }

    for (segment, comma_after, _, _) in retained {
        output.push_str(&segment);
        if comma_after {
            output.push(',');
        }
    }

    let trailing_start = scanned.members.last().map_or(scanned.open + 1, |member| {
        member.segment_end + usize::from(member.comma_after)
    });
    let tail = &editable[trailing_start..scanned.close_indent_start];
    output.push_str(tail);

    if !additions.is_empty() {
        let eol = if editable.contains("\r\n") {
            "\r\n"
        } else {
            "\n"
        };
        let indent = detect_indent(editable, &scanned);
        if editable.contains('\n') || scanned.members.is_empty() {
            if !output.ends_with('\n') && !output.ends_with('\r') {
                output.push_str(eol);
            }
            let addition_count = additions.len();
            for (index, (key, value)) in additions.into_iter().enumerate() {
                output.push_str(&indent);
                output.push_str(&key);
                output.push_str(": ");
                output.push_str(&value);
                if index + 1 < addition_count {
                    output.push(',');
                }
                output.push_str(eol);
            }
        } else {
            if !output.ends_with('{') {
                output.push(' ');
            }
            for (index, (key, value)) in additions.into_iter().enumerate() {
                if index > 0 {
                    output.push_str(", ");
                }
                output.push_str(&key);
                output.push_str(": ");
                output.push_str(&value);
            }
            output.push(' ');
        }
    }

    output.push_str(&editable[scanned.close..]);

    let reparsed = parse_jsonc_object(&output)?;
    if !json_objects_equal(&reparsed, target) {
        return Err(JsoncError::Parse(
            "Edited JSONC does not match the intended object.".to_owned(),
        ));
    }
    if bom {
        output.insert(0, '\u{feff}');
    }
    Ok(output)
}

#[derive(Debug)]
struct JsoncMember {
    key: String,
    key_start: usize,
    segment_start: usize,
    segment_end: usize,
    value_start: usize,
    value_end: usize,
    comma_after: bool,
}

#[derive(Debug)]
struct JsoncRoot {
    open: usize,
    close: usize,
    close_indent_start: usize,
    members: Vec<JsoncMember>,
}

fn scan_root_object(input: &str) -> Result<JsoncRoot, JsoncError> {
    let bytes = input.as_bytes();
    let mut cursor = skip_trivia(bytes, 0)?;
    if bytes.get(cursor) != Some(&b'{') {
        return Err(JsoncError::RootNotObject);
    }
    let open = cursor;
    cursor += 1;
    let mut segment_start = cursor;
    let mut members: Vec<JsoncMember> = Vec::new();
    loop {
        cursor = skip_trivia(bytes, cursor)?;
        if bytes.get(cursor) == Some(&b'}') {
            let close = cursor;
            let close_indent_start = trailing_indent_start(input, close);
            if let Some(last) = members.last_mut() {
                if !last.comma_after {
                    last.segment_end = close_indent_start.max(last.value_end);
                }
            }
            return Ok(JsoncRoot {
                open,
                close,
                close_indent_start,
                members,
            });
        }
        let key_start = cursor;
        let key_end = scan_string(bytes, key_start)?;
        let key = serde_json::from_slice::<String>(&bytes[key_start..key_end])
            .map_err(|error| JsoncError::Parse(error.to_string()))?;
        cursor = skip_trivia(bytes, key_end)?;
        if bytes.get(cursor) != Some(&b':') {
            return Err(JsoncError::Parse(
                "Expected ':' after object key.".to_owned(),
            ));
        }
        cursor = skip_trivia(bytes, cursor + 1)?;
        let value_start = cursor;
        let value_end = scan_value(bytes, value_start)?;
        cursor = skip_trivia(bytes, value_end)?;
        let delimiter = bytes.get(cursor).copied();
        if !matches!(delimiter, Some(b',') | Some(b'}')) {
            return Err(JsoncError::Parse(
                "Expected ',' or '}' after object value.".to_owned(),
            ));
        }
        let comma_after = delimiter == Some(b',');
        let segment_end = if comma_after { cursor } else { value_end };
        members.push(JsoncMember {
            key,
            key_start,
            segment_start,
            segment_end,
            value_start,
            value_end,
            comma_after,
        });
        if comma_after {
            cursor += 1;
            segment_start = cursor;
        } else {
            // The object is valid JSONC and serde has already checked its
            // closing brace. Preserve comments and whitespace preceding it.
            let close = cursor;
            let close_indent_start = trailing_indent_start(input, close);
            if let Some(last) = members.last_mut() {
                last.segment_end = close_indent_start.max(last.value_end);
            }
            return Ok(JsoncRoot {
                open,
                close,
                close_indent_start,
                members,
            });
        }
    }
}

fn scan_string(bytes: &[u8], start: usize) -> Result<usize, JsoncError> {
    if bytes.get(start) != Some(&b'"') {
        return Err(JsoncError::Parse("Expected a JSON string.".to_owned()));
    }
    let mut cursor = start + 1;
    let mut escaped = false;
    while let Some(byte) = bytes.get(cursor).copied() {
        cursor += 1;
        if escaped {
            escaped = false;
        } else if byte == b'\\' {
            escaped = true;
        } else if byte == b'"' {
            return Ok(cursor);
        }
    }
    Err(JsoncError::Parse("Unterminated JSON string.".to_owned()))
}

fn scan_value(bytes: &[u8], start: usize) -> Result<usize, JsoncError> {
    match bytes.get(start) {
        Some(b'"') => scan_string(bytes, start),
        Some(b'{') | Some(b'[') => {
            let mut stack = vec![if bytes[start] == b'{' { b'}' } else { b']' }];
            let mut cursor = start + 1;
            let mut in_string = false;
            let mut escaped = false;
            while let Some(byte) = bytes.get(cursor).copied() {
                if in_string {
                    if escaped {
                        escaped = false;
                    } else if byte == b'\\' {
                        escaped = true;
                    } else if byte == b'"' {
                        in_string = false;
                    }
                    cursor += 1;
                    continue;
                }
                if byte == b'"' {
                    in_string = true;
                    cursor += 1;
                    continue;
                }
                if byte == b'/' && bytes.get(cursor + 1) == Some(&b'/') {
                    cursor = skip_line_comment(bytes, cursor + 2);
                    continue;
                }
                if byte == b'/' && bytes.get(cursor + 1) == Some(&b'*') {
                    cursor = skip_block_comment(bytes, cursor)?;
                    continue;
                }
                if byte == b'{' {
                    stack.push(b'}');
                } else if byte == b'[' {
                    stack.push(b']');
                } else if byte == b'}' || byte == b']' {
                    if stack.pop() != Some(byte) {
                        return Err(JsoncError::Parse("Mismatched JSON container.".to_owned()));
                    }
                    if stack.is_empty() {
                        return Ok(cursor + 1);
                    }
                }
                cursor += 1;
            }
            Err(JsoncError::Parse("Unterminated JSON container.".to_owned()))
        }
        Some(_) => {
            let mut cursor = start;
            while let Some(byte) = bytes.get(cursor).copied() {
                if byte.is_ascii_whitespace() || matches!(byte, b',' | b'}' | b']') {
                    break;
                }
                if byte == b'/' && matches!(bytes.get(cursor + 1), Some(b'/') | Some(b'*')) {
                    break;
                }
                cursor += 1;
            }
            if cursor == start {
                Err(JsoncError::Parse("Expected a JSON value.".to_owned()))
            } else {
                Ok(cursor)
            }
        }
        None => Err(JsoncError::Parse("Expected a JSON value.".to_owned())),
    }
}

fn skip_trivia(bytes: &[u8], mut cursor: usize) -> Result<usize, JsoncError> {
    loop {
        while bytes.get(cursor).is_some_and(u8::is_ascii_whitespace) {
            cursor += 1;
        }
        if bytes.get(cursor) == Some(&b'/') && bytes.get(cursor + 1) == Some(&b'/') {
            cursor = skip_line_comment(bytes, cursor + 2);
        } else if bytes.get(cursor) == Some(&b'/') && bytes.get(cursor + 1) == Some(&b'*') {
            cursor = skip_block_comment(bytes, cursor)?;
        } else {
            return Ok(cursor);
        }
    }
}

fn skip_line_comment(bytes: &[u8], mut cursor: usize) -> usize {
    while let Some(byte) = bytes.get(cursor) {
        if *byte == b'\n' {
            return cursor + 1;
        }
        cursor += 1;
    }
    cursor
}

fn skip_block_comment(bytes: &[u8], start: usize) -> Result<usize, JsoncError> {
    let mut cursor = start + 2;
    while cursor + 1 < bytes.len() {
        if bytes[cursor] == b'*' && bytes[cursor + 1] == b'/' {
            return Ok(cursor + 2);
        }
        cursor += 1;
    }
    Err(JsoncError::Parse("Unterminated block comment.".to_owned()))
}

fn trailing_indent_start(input: &str, close: usize) -> usize {
    let bytes = input.as_bytes();
    let mut start = close;
    while start > 0 && matches!(bytes[start - 1], b' ' | b'\t') {
        start -= 1;
    }
    start
}

fn remove_leading_inline_comment(segment: &mut String) -> usize {
    let bytes = segment.as_bytes();
    let mut cursor = 0;
    while matches!(bytes.get(cursor), Some(b' ' | b'\t')) {
        cursor += 1;
    }
    if bytes.get(cursor) != Some(&b'/') {
        return 0;
    }
    match bytes.get(cursor + 1) {
        Some(b'/') => {
            let end = bytes[cursor..]
                .iter()
                .position(|byte| *byte == b'\n' || *byte == b'\r')
                .map_or(bytes.len(), |offset| cursor + offset);
            segment.replace_range(cursor..end, "");
            end - cursor
        }
        Some(b'*') => {
            if let Some(end) = segment[cursor + 2..].find("*/") {
                let end = cursor + end + 4;
                segment.replace_range(cursor..end, "");
                end - cursor
            } else {
                0
            }
        }
        _ => 0,
    }
}

fn detect_indent(input: &str, root: &JsoncRoot) -> String {
    let Some(member) = root.members.first() else {
        return "  ".to_owned();
    };
    let line_start = input[..member.key_start]
        .rfind('\n')
        .map_or(0, |index| index + 1);
    let prefix = &input[line_start..member.key_start];
    if prefix.chars().all(|ch| matches!(ch, ' ' | '\t')) {
        prefix.to_owned()
    } else {
        "  ".to_owned()
    }
}

fn json_objects_equal(left: &Map<String, Value>, right: &Map<String, Value>) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .all(|(key, value)| right.get(key).is_some_and(|other| other == value))
}

fn strip_trailing_commas(bytes: &mut [u8]) {
    let mut in_string = false;
    let mut escaped = false;
    let mut cursor = 0;
    while cursor < bytes.len() {
        if in_string {
            if escaped {
                escaped = false;
            } else if bytes[cursor] == b'\\' {
                escaped = true;
            } else if bytes[cursor] == b'"' {
                in_string = false;
            }
            cursor += 1;
            continue;
        }
        if bytes[cursor] == b'"' {
            in_string = true;
            cursor += 1;
            continue;
        }
        if bytes[cursor] == b',' {
            let mut next = cursor + 1;
            while bytes.get(next).is_some_and(u8::is_ascii_whitespace) {
                next += 1;
            }
            if matches!(bytes.get(next), Some(b'}') | Some(b']')) {
                bytes[cursor] = b' ';
            }
        }
        cursor += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::{
        apply_jsonc_updates, parse_jsonc_object, strip_json_comments, update_jsonc_content,
        update_jsonc_object_content,
    };
    use serde_json::{Map, Value, json};

    fn object(entries: &[(&str, Value)]) -> Map<String, Value> {
        entries
            .iter()
            .map(|(key, value)| ((*key).to_owned(), value.clone()))
            .collect()
    }

    #[test]
    fn strips_comments_but_keeps_comment_markers_in_strings() {
        let source = r#"{
  // line comment
  "url": "https://example.test/a/*b*/",
  "text": "escaped quote: \" // still inside string",
  /* block
     comment */
  "ok": true
}"#;
        let cleaned = strip_json_comments(source);
        let parsed: serde_json::Value = serde_json::from_str(&cleaned).unwrap();
        assert_eq!(parsed["url"], "https://example.test/a/*b*/");
        assert_eq!(parsed["ok"], true);
        assert_eq!(cleaned.lines().count(), source.lines().count());
    }

    #[test]
    fn unterminated_block_comment_is_consumed_to_end() {
        assert_eq!(strip_json_comments("{} /* ignored"), "{}           ");
    }

    #[test]
    fn parses_bom_comments_and_trailing_commas() {
        let parsed =
            parse_jsonc_object("\u{feff}{\n // project\n \"/repo\": \"TRUST_FOLDER\",\n}").unwrap();
        assert_eq!(parsed["/repo"], "TRUST_FOLDER");
    }

    #[test]
    fn appends_a_rule_and_preserves_existing_comments_and_trailing_comma() {
        let original = "{\n  // work repos\n  \"/existing\": \"TRUST_FOLDER\",\n}\n";
        let updated = update_jsonc_object_content(
            original,
            &object(&[
                ("/existing", json!("TRUST_FOLDER")),
                ("/new", json!("DO_NOT_TRUST")),
            ]),
        )
        .unwrap();
        assert!(updated.contains("// work repos"));
        assert!(updated.contains("\"/existing\": \"TRUST_FOLDER\""));
        assert!(updated.contains("\"/new\": \"DO_NOT_TRUST\""));
        assert!(updated.ends_with("}\n"));
        let parsed = parse_jsonc_object(&updated).unwrap();
        assert_eq!(parsed["/new"], "DO_NOT_TRUST");
    }

    #[test]
    fn appends_multiple_properties_with_valid_commas() {
        let original = "{\n  \"existing\": true\n}\n";
        let updated = update_jsonc_object_content(
            original,
            &object(&[
                ("existing", json!(true)),
                ("first", json!(1)),
                ("second", json!(2)),
            ]),
        )
        .unwrap();

        assert_eq!(
            parse_jsonc_object(&updated).unwrap(),
            object(&[
                ("existing", json!(true)),
                ("first", json!(1)),
                ("second", json!(2)),
            ])
        );
    }

    #[test]
    fn sync_removes_deleted_rule_and_its_leading_and_inline_comments() {
        let original = "{\n  // remove this rule\n  \"/stale\": \"TRUST_FOLDER\", // stale inline\n  // retain this rule\n  \"/keep\": \"DO_NOT_TRUST\"\n}";
        let updated =
            update_jsonc_object_content(original, &object(&[("/keep", json!("DO_NOT_TRUST"))]))
                .unwrap();
        assert!(!updated.contains("/stale"));
        assert!(!updated.contains("remove this rule"));
        assert!(!updated.contains("stale inline"));
        assert!(updated.contains("retain this rule"));
        assert_eq!(
            parse_jsonc_object(&updated).unwrap()["/keep"],
            "DO_NOT_TRUST"
        );
    }

    #[test]
    fn retains_crlf_tabs_and_bom_when_adding_a_rule() {
        let original = "\u{feff}{\r\n\t// workspace\r\n\t\"/one\": \"TRUST_FOLDER\"\r\n}\r\n";
        let updated = update_jsonc_object_content(
            original,
            &object(&[
                ("/one", json!("TRUST_FOLDER")),
                ("/two", json!("TRUST_PARENT")),
            ]),
        )
        .unwrap();
        assert!(updated.starts_with('\u{feff}'));
        assert!(updated.contains("\r\n\t\"/two\": \"TRUST_PARENT\"\r\n"));
        assert!(updated.ends_with("\r\n"));
    }

    #[test]
    fn duplicate_root_keys_are_normalized_to_the_effective_value() {
        let original = "{\n  // old\n  \"/repo\": \"TRUST_FOLDER\", // old inline\n  // effective\n  \"/repo\": \"DO_NOT_TRUST\"\n}";
        let updated =
            update_jsonc_object_content(original, &object(&[("/repo", json!("TRUST_PARENT"))]))
                .unwrap();
        assert_eq!(updated.matches("/repo").count(), 1);
        assert!(!updated.contains("old inline"));
        assert!(updated.contains("effective"));
        assert_eq!(
            parse_jsonc_object(&updated).unwrap()["/repo"],
            "TRUST_PARENT"
        );
    }

    #[test]
    fn a_semantic_noop_leaves_jsonc_byte_for_byte_unchanged() {
        let original = "{\n  // untouched\n  \"/repo\": \"TRUST_FOLDER\",\n}\n";
        let updated =
            update_jsonc_object_content(original, &object(&[("/repo", json!("TRUST_FOLDER"))]))
                .unwrap();
        assert_eq!(updated, original);
    }

    #[test]
    fn deep_merge_updates_nested_values_and_keeps_nested_comments() {
        let original = r#"{
  "ui": {
    // retained comment
    "theme": "dark",
    "showLineNumbers": true
  }
}"#;
        let updates = object(&[("ui", json!({"theme": "light"}))]);
        let updated = update_jsonc_content(original, &updates, false, &[]).unwrap();
        assert!(updated.contains("// retained comment"));
        assert!(updated.contains("\"theme\": \"light\""));
        assert!(updated.contains("\"showLineNumbers\": true"));
        assert_eq!(
            parse_jsonc_object(&updated).unwrap()["ui"]["theme"],
            "light"
        );
    }

    #[test]
    fn sync_recursively_removes_nested_keys_and_keeps_requested_keys() {
        let original = r#"{
  "general": {
    "disableAutoUpdate": true,
    "keep": 7
  }
}"#;
        let updates = object(&[("general", json!({"enableAutoUpdate": false, "keep": 7}))]);
        let updated = update_jsonc_content(original, &updates, true, &[]).unwrap();
        assert!(!updated.contains("disableAutoUpdate"));
        assert!(updated.contains("enableAutoUpdate"));
        assert!(updated.contains("\"keep\": 7"));
        assert_eq!(
            parse_jsonc_object(&updated).unwrap()["general"]["enableAutoUpdate"],
            false
        );
    }

    #[test]
    fn replace_path_replaces_one_nested_object_without_syncing_siblings() {
        let current =
            parse_jsonc_object(r#"{"ui":{"theme":"dark","fontSize":14},"model":"m"}"#).unwrap();
        let updates = parse_jsonc_object(r#"{"ui":{"theme":{"color":"blue"}}}"#).unwrap();
        let target = apply_jsonc_updates(
            current,
            &updates,
            false,
            &["ui".to_owned(), "theme".to_owned()],
        );
        assert_eq!(
            target["ui"],
            json!({"theme":{"color":"blue"}, "fontSize":14})
        );
        assert_eq!(target["model"], "m");
    }

    #[test]
    fn updates_ignore_prototype_pollution_keys_at_every_depth() {
        let current = Map::new();
        let updates: Map<String, Value> = serde_json::from_str(
            r#"{"safe":true,"__proto__":{"polluted":true},"nested":{"prototype":{"polluted":true},"keep":1}}"#,
        )
        .unwrap();
        let target = apply_jsonc_updates(current, &updates, false, &[]);
        assert_eq!(target["safe"], true);
        assert_eq!(target["nested"], json!({"keep":1}));
        assert!(!target.contains_key("__proto__"));
    }

    #[test]
    fn rejects_non_object_jsonc_roots() {
        assert_eq!(
            parse_jsonc_object("[]").unwrap_err(),
            super::JsoncError::RootNotObject
        );
    }
}
