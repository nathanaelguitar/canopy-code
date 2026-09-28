//! Strict versioned channel-memory JSON documents and legacy Markdown
//! migration. Port of `packages/core/src/memory/channel-memory-document.ts`.

use serde::Serialize;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashSet};
use std::error::Error;
use std::fmt;
use unicode_normalization::UnicodeNormalization;

pub const CHANNEL_MEMORY_DOCUMENT_VERSION: u8 = 1;
pub const MAX_CHANNEL_MEMORY_ENTRIES: usize = 500;
pub const MAX_CHANNEL_MEMORY_ENTRIES_PER_REQUEST: usize = 10;
pub const MAX_CHANNEL_MEMORY_ENTRY_CODE_POINTS: usize = 2_000;
pub const CHANNEL_MEMORY_ID_PATTERN: &str = r"^m-[a-f0-9]{12}$";

#[derive(Clone, Debug, Eq, PartialEq, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelMemoryEntry {
    pub id: String,
    pub text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_by: Option<String>,
    /// Allows the serializer to reject JavaScript-side unknown properties
    /// rather than silently losing them at the typed Rust boundary.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelMemoryMigration {
    pub legacy_sha256: String,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelMemoryDocument {
    pub version: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub migration: Option<ChannelMemoryMigration>,
    pub entries: Vec<ChannelMemoryEntry>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

impl Default for ChannelMemoryDocument {
    fn default() -> Self {
        Self {
            version: CHANNEL_MEMORY_DOCUMENT_VERSION,
            migration: None,
            entries: Vec::new(),
            extra: Map::new(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewChannelMemoryEntry<'a> {
    pub text: &'a str,
    pub created_by: Option<&'a str>,
    pub now: &'a str,
    pub random_hex: &'a str,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChannelMemoryDocumentError(String);

impl ChannelMemoryDocumentError {
    fn invalid_document() -> Self {
        Self("Invalid channel memory document".to_owned())
    }

    fn invalid_entry() -> Self {
        Self("Invalid channel memory entry".to_owned())
    }

    fn with_message(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for ChannelMemoryDocumentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for ChannelMemoryDocumentError {}

#[derive(Clone, Debug, PartialEq)]
enum StrictJsonValue {
    Null,
    Bool,
    Number(f64),
    String(String),
    Array(Vec<Self>),
    Object(BTreeMap<String, Self>),
}

/// Return whether an ID satisfies `CHANNEL_MEMORY_ID_RE`.
pub fn is_channel_memory_id(id: &str) -> bool {
    id.strip_prefix("m-").is_some_and(|hex| {
        hex.len() == 12
            && hex
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

/// NFKC-normalize, ECMAScript-trim, collapse ECMAScript whitespace, and
/// lowercase one legacy channel-memory line.
pub fn normalize_channel_memory_text(text: &str) -> String {
    let nfkc = text.nfkc().collect::<String>();
    let normalized = trim_ecmascript_whitespace(&nfkc)
        .split(is_ecmascript_whitespace)
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join(" ");
    normalized.to_lowercase()
}

/// Parse and fully validate a strict version-1 channel-memory document.
/// Duplicate object keys anywhere in the JSON source are rejected before
/// document validation so duplicate occurrences can never be overwritten.
pub fn parse_channel_memory_document(
    raw: &str,
) -> Result<ChannelMemoryDocument, ChannelMemoryDocumentError> {
    let value = StrictJsonParser::new(raw)
        .parse()
        .map_err(|_| ChannelMemoryDocumentError::invalid_document())?;
    parse_document_value(&value)
}

/// Convert a legacy Markdown memory file into a version-1 document. Entry IDs
/// are tied to normalized content and its original zero-based source line;
/// the migration marker hashes the exact original bytes.
pub fn parse_legacy_channel_memory(
    raw: &[u8],
) -> Result<ChannelMemoryDocument, ChannelMemoryDocumentError> {
    let decoded = std::str::from_utf8(raw).map_err(|_| {
        ChannelMemoryDocumentError::with_message(
            "The encoded data was not valid for encoding utf-8",
        )
    })?;
    // TextDecoder strips one leading UTF-8 BOM unless `ignoreBOM` is enabled.
    let decoded = decoded.strip_prefix('\u{feff}').unwrap_or(decoded);
    let mut entries = Vec::new();
    let mut normalized_texts = HashSet::new();
    let mut ids = HashSet::new();

    for (source_line_index, line) in split_legacy_lines(decoded).into_iter().enumerate() {
        let normalized_text = normalize_channel_memory_text(line);
        if normalized_text.is_empty() || !normalized_texts.insert(normalized_text.clone()) {
            continue;
        }

        let id = legacy_entry_id(&normalized_text, source_line_index);
        if !ids.insert(id.clone()) {
            return Err(ChannelMemoryDocumentError::with_message(
                "Channel memory legacy ID collision",
            ));
        }
        let entry = validate_entry(ChannelMemoryEntry {
            id,
            text: line.to_owned(),
            ..ChannelMemoryEntry::default()
        })?;
        entries.push(entry);
        if entries.len() > MAX_CHANNEL_MEMORY_ENTRIES {
            return Err(ChannelMemoryDocumentError::with_message(
                "Channel memory exceeds maximum number of entries",
            ));
        }
    }

    Ok(ChannelMemoryDocument {
        version: CHANNEL_MEMORY_DOCUMENT_VERSION,
        migration: Some(ChannelMemoryMigration {
            legacy_sha256: sha256_hex(raw),
            extra: Map::new(),
        }),
        entries,
        extra: Map::new(),
    })
}

/// Create a timestamped entry with a twelve-hex-digit random ID suffix.
pub fn create_channel_memory_entry(
    input: NewChannelMemoryEntry<'_>,
) -> Result<ChannelMemoryEntry, ChannelMemoryDocumentError> {
    if input.random_hex.len() != 12
        || !input
            .random_hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ChannelMemoryDocumentError::with_message(
            "Invalid randomHex for channel memory entry",
        ));
    }

    let text = trim_ecmascript_whitespace(input.text).to_owned();
    let mut entry = validate_entry(ChannelMemoryEntry {
        id: format!("m-{}", input.random_hex),
        text,
        ..ChannelMemoryEntry::default()
    })?;
    entry.created_at = Some(input.now.to_owned());
    entry.updated_at = Some(input.now.to_owned());
    if let Some(created_by) = input.created_by {
        entry.created_by = Some(created_by.to_owned());
    }
    Ok(entry)
}

/// Render memory entries as a newline-terminated recall block.
pub fn render_channel_memory_recall(entries: &[ChannelMemoryEntry]) -> String {
    if entries.is_empty() {
        String::new()
    } else {
        format!(
            "{}\n",
            entries
                .iter()
                .map(|entry| entry.text.as_str())
                .collect::<Vec<_>>()
                .join("\n")
        )
    }
}

/// Validate and pretty-print a memory document with two-space indentation and
/// a final newline. Unknown properties carried in `extra` are rejected.
pub fn serialize_channel_memory_document(
    document: &ChannelMemoryDocument,
) -> Result<String, ChannelMemoryDocumentError> {
    let raw = serde_json::to_string(document)
        .map_err(|_| ChannelMemoryDocumentError::invalid_document())?;
    let value = StrictJsonParser::new(&raw)
        .parse()
        .map_err(|_| ChannelMemoryDocumentError::invalid_document())?;
    let validated = parse_document_value(&value)?;
    let mut serialized = serde_json::to_string_pretty(&validated)
        .map_err(|_| ChannelMemoryDocumentError::invalid_document())?;
    serialized.push('\n');
    Ok(serialized)
}

fn parse_document_value(
    value: &StrictJsonValue,
) -> Result<ChannelMemoryDocument, ChannelMemoryDocumentError> {
    let StrictJsonValue::Object(object) = value else {
        return Err(ChannelMemoryDocumentError::invalid_document());
    };
    validate_keys(object, &["version", "migration", "entries"])
        .map_err(|_| ChannelMemoryDocumentError::invalid_document())?;

    let Some(StrictJsonValue::Number(version)) = object.get("version") else {
        return Err(ChannelMemoryDocumentError::invalid_document());
    };
    if *version != f64::from(CHANNEL_MEMORY_DOCUMENT_VERSION) {
        return Err(ChannelMemoryDocumentError::with_message(
            "Unsupported channel memory version",
        ));
    }

    let Some(StrictJsonValue::Array(raw_entries)) = object.get("entries") else {
        return Err(ChannelMemoryDocumentError::invalid_document());
    };
    if raw_entries.len() > MAX_CHANNEL_MEMORY_ENTRIES {
        return Err(ChannelMemoryDocumentError::with_message(
            "Channel memory exceeds maximum number of entries",
        ));
    }

    let mut ids = HashSet::new();
    let mut entries = Vec::with_capacity(raw_entries.len());
    for raw_entry in raw_entries {
        let entry = parse_entry(raw_entry)?;
        if !ids.insert(entry.id.clone()) {
            return Err(ChannelMemoryDocumentError::invalid_entry());
        }
        entries.push(entry);
    }

    let migration = match object.get("migration") {
        None => None,
        Some(StrictJsonValue::Object(migration)) => {
            let Some(StrictJsonValue::String(legacy_sha256)) = migration.get("legacySha256") else {
                return Err(ChannelMemoryDocumentError::invalid_document());
            };
            if !is_sha256_hex(legacy_sha256) {
                return Err(ChannelMemoryDocumentError::invalid_document());
            }
            validate_keys(migration, &["legacySha256"])
                .map_err(|_| ChannelMemoryDocumentError::invalid_document())?;
            Some(ChannelMemoryMigration {
                legacy_sha256: legacy_sha256.clone(),
                extra: Map::new(),
            })
        }
        Some(_) => return Err(ChannelMemoryDocumentError::invalid_document()),
    };

    Ok(ChannelMemoryDocument {
        version: CHANNEL_MEMORY_DOCUMENT_VERSION,
        migration,
        entries,
        extra: Map::new(),
    })
}

fn parse_entry(value: &StrictJsonValue) -> Result<ChannelMemoryEntry, ChannelMemoryDocumentError> {
    let StrictJsonValue::Object(object) = value else {
        return Err(ChannelMemoryDocumentError::invalid_entry());
    };
    validate_keys(
        object,
        &["id", "text", "createdAt", "updatedAt", "createdBy"],
    )
    .map_err(|_| ChannelMemoryDocumentError::invalid_entry())?;

    let Some(StrictJsonValue::String(id)) = object.get("id") else {
        return Err(ChannelMemoryDocumentError::invalid_entry());
    };
    let Some(StrictJsonValue::String(text)) = object.get("text") else {
        return Err(ChannelMemoryDocumentError::invalid_entry());
    };
    if !is_channel_memory_id(id)
        || trim_ecmascript_whitespace(text).is_empty()
        || text.chars().count() > MAX_CHANNEL_MEMORY_ENTRY_CODE_POINTS
    {
        return Err(ChannelMemoryDocumentError::invalid_entry());
    }

    Ok(ChannelMemoryEntry {
        id: id.clone(),
        text: text.clone(),
        created_at: optional_string(object, "createdAt")?,
        updated_at: optional_string(object, "updatedAt")?,
        created_by: optional_string(object, "createdBy")?,
        extra: Map::new(),
    })
}

fn optional_string(
    object: &BTreeMap<String, StrictJsonValue>,
    key: &str,
) -> Result<Option<String>, ChannelMemoryDocumentError> {
    match object.get(key) {
        None => Ok(None),
        Some(StrictJsonValue::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(ChannelMemoryDocumentError::invalid_entry()),
    }
}

fn validate_entry(
    entry: ChannelMemoryEntry,
) -> Result<ChannelMemoryEntry, ChannelMemoryDocumentError> {
    if !entry.extra.is_empty()
        || !is_channel_memory_id(&entry.id)
        || trim_ecmascript_whitespace(&entry.text).is_empty()
        || entry.text.chars().count() > MAX_CHANNEL_MEMORY_ENTRY_CODE_POINTS
    {
        return Err(ChannelMemoryDocumentError::invalid_entry());
    }
    Ok(entry)
}

fn validate_keys(object: &BTreeMap<String, StrictJsonValue>, allowed: &[&str]) -> Result<(), ()> {
    if object.keys().any(|key| !allowed.contains(&key.as_str())) {
        Err(())
    } else {
        Ok(())
    }
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn split_legacy_lines(text: &str) -> Vec<&str> {
    let bytes = text.as_bytes();
    let mut lines = Vec::new();
    let mut start = 0;
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'\r' || bytes[index] == b'\n' {
            lines.push(&text[start..index]);
            if bytes[index] == b'\r' && bytes.get(index + 1) == Some(&b'\n') {
                index += 1;
            }
            index += 1;
            start = index;
        } else {
            index += 1;
        }
    }
    lines.push(&text[start..]);
    lines
}

fn legacy_entry_id(normalized_text: &str, source_line_index: usize) -> String {
    let mut hasher = Sha256::new();
    hasher.update(normalized_text.as_bytes());
    hasher.update([0]);
    hasher.update(source_line_index.to_string().as_bytes());
    let digest = hasher.finalize();
    let mut hex = String::with_capacity(64);
    for byte in digest {
        use fmt::Write as _;
        let _ = write!(hex, "{byte:02x}");
    }
    format!("m-{}", &hex[..12])
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut hex = String::with_capacity(64);
    for byte in digest {
        use fmt::Write as _;
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

fn trim_ecmascript_whitespace(text: &str) -> &str {
    text.trim_matches(is_ecmascript_whitespace)
}

fn is_ecmascript_whitespace(character: char) -> bool {
    matches!(
        character,
        '\u{0009}'..='\u{000d}'
            | '\u{0020}'
            | '\u{00a0}'
            | '\u{1680}'
            | '\u{2000}'..='\u{200a}'
            | '\u{2028}'
            | '\u{2029}'
            | '\u{202f}'
            | '\u{205f}'
            | '\u{3000}'
            | '\u{feff}'
    )
}

struct StrictJsonParser<'a> {
    input: &'a str,
    index: usize,
}

impl<'a> StrictJsonParser<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, index: 0 }
    }

    fn parse(mut self) -> Result<StrictJsonValue, ()> {
        let value = self.parse_value()?;
        self.skip_whitespace();
        if self.index != self.input.len() {
            return Err(());
        }
        Ok(value)
    }

    fn parse_value(&mut self) -> Result<StrictJsonValue, ()> {
        self.skip_whitespace();
        match self.input.as_bytes().get(self.index).copied() {
            Some(b'{') => self.parse_object(),
            Some(b'[') => self.parse_array(),
            Some(b'"') => self.parse_string().map(StrictJsonValue::String),
            Some(b'-' | b'0'..=b'9') => self.parse_number().map(StrictJsonValue::Number),
            Some(b't') => self.parse_literal(b"true", StrictJsonValue::Bool),
            Some(b'f') => self.parse_literal(b"false", StrictJsonValue::Bool),
            Some(b'n') => self.parse_literal(b"null", StrictJsonValue::Null),
            _ => Err(()),
        }
    }

    fn parse_object(&mut self) -> Result<StrictJsonValue, ()> {
        self.index += 1;
        let mut object = BTreeMap::new();
        self.skip_whitespace();
        if self.consume(b'}') {
            return Ok(StrictJsonValue::Object(object));
        }
        loop {
            self.skip_whitespace();
            if self.input.as_bytes().get(self.index) != Some(&b'"') {
                return Err(());
            }
            let key = self.parse_string()?;
            if object.contains_key(&key) {
                return Err(());
            }
            self.skip_whitespace();
            if !self.consume(b':') {
                return Err(());
            }
            let value = self.parse_value()?;
            object.insert(key, value);
            self.skip_whitespace();
            if self.consume(b'}') {
                return Ok(StrictJsonValue::Object(object));
            }
            if !self.consume(b',') {
                return Err(());
            }
        }
    }

    fn parse_array(&mut self) -> Result<StrictJsonValue, ()> {
        self.index += 1;
        let mut array = Vec::new();
        self.skip_whitespace();
        if self.consume(b']') {
            return Ok(StrictJsonValue::Array(array));
        }
        loop {
            array.push(self.parse_value()?);
            self.skip_whitespace();
            if self.consume(b']') {
                return Ok(StrictJsonValue::Array(array));
            }
            if !self.consume(b',') {
                return Err(());
            }
        }
    }

    fn parse_string(&mut self) -> Result<String, ()> {
        let start = self.index;
        self.index += 1;
        while self.index < self.input.len() {
            let byte = self.input.as_bytes()[self.index];
            match byte {
                b'"' => {
                    self.index += 1;
                    return serde_json::from_str(&self.input[start..self.index]).map_err(|_| ());
                }
                b'\\' => {
                    self.index += 1;
                    let escape = *self.input.as_bytes().get(self.index).ok_or(())?;
                    if escape == b'u' {
                        let end = self.index.checked_add(5).ok_or(())?;
                        if end > self.input.len()
                            || !self.input.as_bytes()[self.index + 1..end]
                                .iter()
                                .all(u8::is_ascii_hexdigit)
                        {
                            return Err(());
                        }
                        self.index = end;
                    } else if matches!(
                        escape,
                        b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't'
                    ) {
                        self.index += 1;
                    } else {
                        return Err(());
                    }
                }
                0x00..=0x1f => return Err(()),
                _ => {
                    let character = self.input[self.index..].chars().next().ok_or(())?;
                    self.index += character.len_utf8();
                }
            }
        }
        Err(())
    }

    fn parse_number(&mut self) -> Result<f64, ()> {
        let bytes = self.input.as_bytes();
        let start = self.index;
        if bytes.get(self.index) == Some(&b'-') {
            self.index += 1;
        }
        match bytes.get(self.index).copied() {
            Some(b'0') => self.index += 1,
            Some(b'1'..=b'9') => {
                self.index += 1;
                while bytes.get(self.index).is_some_and(u8::is_ascii_digit) {
                    self.index += 1;
                }
            }
            _ => return Err(()),
        }
        if bytes.get(self.index) == Some(&b'.') {
            self.index += 1;
            let fractional_start = self.index;
            while bytes.get(self.index).is_some_and(u8::is_ascii_digit) {
                self.index += 1;
            }
            if self.index == fractional_start {
                return Err(());
            }
        }
        if matches!(bytes.get(self.index), Some(b'e' | b'E')) {
            self.index += 1;
            if matches!(bytes.get(self.index), Some(b'+' | b'-')) {
                self.index += 1;
            }
            let exponent_start = self.index;
            while bytes.get(self.index).is_some_and(u8::is_ascii_digit) {
                self.index += 1;
            }
            if self.index == exponent_start {
                return Err(());
            }
        }
        self.input[start..self.index].parse::<f64>().map_err(|_| ())
    }

    fn parse_literal(
        &mut self,
        literal: &[u8],
        value: StrictJsonValue,
    ) -> Result<StrictJsonValue, ()> {
        let end = self.index.checked_add(literal.len()).ok_or(())?;
        if self.input.as_bytes().get(self.index..end) != Some(literal) {
            return Err(());
        }
        self.index = end;
        Ok(value)
    }

    fn skip_whitespace(&mut self) {
        while matches!(
            self.input.as_bytes().get(self.index),
            Some(b' ' | b'\t' | b'\r' | b'\n')
        ) {
            self.index += 1;
        }
    }

    fn consume(&mut self, byte: u8) -> bool {
        if self.input.as_bytes().get(self.index) == Some(&byte) {
            self.index += 1;
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MEMORY_ID: &str = "m-123456789abc";

    fn entry(text: &str) -> ChannelMemoryEntry {
        ChannelMemoryEntry {
            id: MEMORY_ID.into(),
            text: text.into(),
            ..ChannelMemoryEntry::default()
        }
    }

    fn document_json(entries: &str) -> String {
        format!("{{\"version\":1,\"entries\":{entries}}}")
    }

    fn assert_error(raw: &str, expected: &str) {
        let error = parse_channel_memory_document(raw).unwrap_err();
        assert_eq!(error.to_string(), expected);
    }

    #[test]
    fn exported_limits_and_id_pattern_match_the_source_contract() {
        assert_eq!(CHANNEL_MEMORY_DOCUMENT_VERSION, 1);
        assert_eq!(MAX_CHANNEL_MEMORY_ENTRIES, 500);
        assert_eq!(MAX_CHANNEL_MEMORY_ENTRIES_PER_REQUEST, 10);
        assert_eq!(MAX_CHANNEL_MEMORY_ENTRY_CODE_POINTS, 2_000);
        assert_eq!(CHANNEL_MEMORY_ID_PATTERN, r"^m-[a-f0-9]{12}$");
        assert!(is_channel_memory_id(MEMORY_ID));
        assert!(!is_channel_memory_id("m-123456789abC"));
        assert!(!is_channel_memory_id("m-123456789ab"));
        assert!(!is_channel_memory_id("m-123456789abcd"));
    }

    #[test]
    fn normalization_matches_nfkc_trim_whitespace_collapse_and_lowercase_order() {
        assert_eq!(
            normalize_channel_memory_text("  USE\u{00a0}staging  "),
            "use staging"
        );
        assert_eq!(
            normalize_channel_memory_text("\u{feff}Ｆｕｌｌｗｉｄｔｈ　ＯＫ\u{feff}"),
            "fullwidth ok"
        );
        assert_eq!(
            normalize_channel_memory_text("\u{0085}a\u{0085}"),
            "\u{0085}a\u{0085}"
        );
        assert_eq!(
            normalize_channel_memory_text("\u{2028} A\nB \u{2029}"),
            "a b"
        );
        assert_eq!(normalize_channel_memory_text("\u{feff} \u{00a0}"), "");
    }

    #[test]
    fn parses_complete_v1_shape_and_omits_absent_optional_fields() {
        let parsed = parse_channel_memory_document(
            r#"{"version":1,"migration":{"legacySha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"entries":[{"id":"m-123456789abc","text":"Use staging","createdAt":"2026-07-14T00:00:00.000Z","updatedAt":"2026-07-14T00:01:00.000Z","createdBy":"alice"}]}"#,
        )
        .unwrap();
        assert_eq!(parsed.version, 1);
        assert_eq!(
            parsed.migration.as_ref().unwrap().legacy_sha256,
            "a".repeat(64)
        );
        assert_eq!(parsed.entries[0].created_by.as_deref(), Some("alice"));
        assert_eq!(parsed.entries[0].extra, Map::new());

        let no_migration = parse_channel_memory_document(&document_json("[]")).unwrap();
        assert!(no_migration.migration.is_none());
        assert!(no_migration.entries.is_empty());
    }

    #[test]
    fn distinguishes_unsupported_numeric_version_from_invalid_version_type() {
        assert_error(
            r#"{"version":2,"entries":[]}"#,
            "Unsupported channel memory version",
        );
        assert_eq!(
            parse_channel_memory_document(r#"{"version":1.0,"entries":[]}"#)
                .unwrap()
                .version,
            1
        );
        assert_eq!(
            parse_channel_memory_document(r#"{"version":1e0,"entries":[]}"#)
                .unwrap()
                .version,
            1
        );
        assert_error(
            r#"{"version":"1","entries":[]}"#,
            "Invalid channel memory document",
        );
        assert_error(
            r#"{"version":1e999,"entries":[]}"#,
            "Unsupported channel memory version",
        );
    }

    #[test]
    fn rejects_invalid_json_root_and_duplicate_keys_at_any_depth() {
        for raw in ["null", "[]", "", "{", "{\"version\":1,}"] {
            assert_error(raw, "Invalid channel memory document");
        }
        assert_error(
            r#"{"version":1,"entries":[],"entries":[]}"#,
            "Invalid channel memory document",
        );
        assert_error(
            r#"{"version":1,"migration":{"legacySha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","legacySha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},"entries":[]}"#,
            "Invalid channel memory document",
        );
        assert_error(
            r#"{"version":1,"entries":[{"id":"m-123456789abc","text":"x","text":"y"}]}"#,
            "Invalid channel memory document",
        );
    }

    #[test]
    fn rejects_missing_or_non_array_entries_and_unknown_keys_at_every_level() {
        assert_error(r#"{"version":1}"#, "Invalid channel memory document");
        assert_error(
            r#"{"version":1,"entries":{}}"#,
            "Invalid channel memory document",
        );
        assert_error(
            r#"{"version":1,"entries":[],"futureMetadata":true}"#,
            "Invalid channel memory document",
        );
        assert_error(
            r#"{"version":1,"migration":{"legacySha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","extra":true},"entries":[]}"#,
            "Invalid channel memory document",
        );
        assert_error(
            r#"{"version":1,"entries":[{"id":"m-123456789abc","text":"x","extra":true}]}"#,
            "Invalid channel memory entry",
        );
    }

    #[test]
    fn validates_entry_id_text_size_optional_types_and_unique_ids() {
        assert_error(
            &document_json(r#"[{"id":"bad","text":"x"}]"#),
            "Invalid channel memory entry",
        );
        assert_error(
            &document_json(r#"[{"id":"m-123456789abc","text":" \uFEFF "}]"#),
            "Invalid channel memory entry",
        );
        assert_error(
            &document_json(r#"[{"id":"m-123456789abc","text":"x","createdAt":null}]"#),
            "Invalid channel memory entry",
        );
        assert_error(
            &document_json(
                r#"[{"id":"m-123456789abc","text":"x"},{"id":"m-123456789abc","text":"y"}]"#,
            ),
            "Invalid channel memory entry",
        );
        let astral = "😀".repeat(MAX_CHANNEL_MEMORY_ENTRY_CODE_POINTS);
        let raw =
            serde_json::json!({"version":1,"entries":[{"id":MEMORY_ID,"text":astral}]}).to_string();
        assert_eq!(
            parse_channel_memory_document(&raw).unwrap().entries[0]
                .text
                .chars()
                .count(),
            MAX_CHANNEL_MEMORY_ENTRY_CODE_POINTS
        );
        let too_long = "😀".repeat(MAX_CHANNEL_MEMORY_ENTRY_CODE_POINTS + 1);
        let raw = serde_json::json!({"version":1,"entries":[{"id":MEMORY_ID,"text":too_long}]})
            .to_string();
        assert_error(&raw, "Invalid channel memory entry");
    }

    #[test]
    fn checks_entry_limit_before_parsing_entries() {
        let entries = (0..=MAX_CHANNEL_MEMORY_ENTRIES)
            .map(|index| serde_json::json!({"id":format!("m-{index:012x}"),"text":"x"}))
            .collect::<Vec<_>>();
        let raw = serde_json::json!({"version":1,"entries":entries}).to_string();
        assert_error(&raw, "Channel memory exceeds maximum number of entries");
    }

    #[test]
    fn parses_legacy_lines_with_stable_ids_and_exact_byte_migration_hash() {
        let raw = b"Use staging\n\n use   STAGING \nRun tests\n";
        let first = parse_legacy_channel_memory(raw).unwrap();
        let second = parse_legacy_channel_memory(raw).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.entries.len(), 2);
        assert_eq!(
            first
                .entries
                .iter()
                .map(|entry| entry.text.as_str())
                .collect::<Vec<_>>(),
            vec!["Use staging", "Run tests"]
        );
        assert_eq!(first.entries[0].id, "m-5c1888e97dc2");
        assert_eq!(first.entries[1].id, legacy_entry_id("run tests", 3));
        assert!(
            first
                .entries
                .iter()
                .all(|entry| is_channel_memory_id(&entry.id))
        );
        assert!(first.entries.iter().all(|entry| entry.created_at.is_none()));
        assert_eq!(first.migration.unwrap().legacy_sha256, sha256_hex(raw));
        assert_eq!(
            sha256_hex(raw),
            "158cc7b8d474c25beea5528ef4feabe855c4730b7fac483f0fce8062a7d10736"
        );
    }

    #[test]
    fn legacy_line_splitting_treats_crlf_as_one_separator_and_preserves_line_indexes() {
        let raw = b"\r\nUse staging\r\n\rRun tests\n";
        let parsed = parse_legacy_channel_memory(raw).unwrap();
        assert_eq!(parsed.entries.len(), 2);
        assert_eq!(parsed.entries[0].id, legacy_entry_id("use staging", 1));
        assert_eq!(parsed.entries[1].id, legacy_entry_id("run tests", 3));
    }

    #[test]
    fn legacy_entries_preserve_original_whitespace_and_reject_invalid_utf8() {
        let parsed = parse_legacy_channel_memory(b"  Keep surrounding whitespace  \n").unwrap();
        assert_eq!(parsed.entries[0].text, "  Keep surrounding whitespace  ");
        assert_eq!(
            parse_legacy_channel_memory(&[0xff])
                .unwrap_err()
                .to_string(),
            "The encoded data was not valid for encoding utf-8"
        );
    }

    #[test]
    fn legacy_decode_strips_one_leading_utf8_bom_but_hashes_all_original_bytes() {
        let raw = b"\xef\xbb\xbfUse staging\n";
        let parsed = parse_legacy_channel_memory(raw).unwrap();
        assert_eq!(parsed.entries[0].text, "Use staging");
        assert_eq!(parsed.entries[0].id, legacy_entry_id("use staging", 0));
        assert_eq!(parsed.migration.unwrap().legacy_sha256, sha256_hex(raw));
    }

    #[test]
    fn legacy_conversion_enforces_entry_size_and_total_limit() {
        let too_long = format!("{}\n", "x".repeat(MAX_CHANNEL_MEMORY_ENTRY_CODE_POINTS + 1));
        assert_eq!(
            parse_legacy_channel_memory(too_long.as_bytes())
                .unwrap_err()
                .to_string(),
            "Invalid channel memory entry"
        );
        let too_many = (0..=MAX_CHANNEL_MEMORY_ENTRIES)
            .map(|index| format!("entry-{index}\n"))
            .collect::<String>();
        assert_eq!(
            parse_legacy_channel_memory(too_many.as_bytes())
                .unwrap_err()
                .to_string(),
            "Channel memory exceeds maximum number of entries"
        );
    }

    #[test]
    fn creates_valid_entries_and_rejects_invalid_random_hex_or_text() {
        let created = create_channel_memory_entry(NewChannelMemoryEntry {
            text: " Use staging ",
            created_by: Some("alice"),
            now: "2026-07-14T00:00:00.000Z",
            random_hex: "abcdef012345",
        })
        .unwrap();
        assert_eq!(created.id, "m-abcdef012345");
        assert_eq!(created.text, "Use staging");
        assert_eq!(
            created.created_at.as_deref(),
            Some("2026-07-14T00:00:00.000Z")
        );
        assert_eq!(created.updated_at, created.created_at);
        assert_eq!(created.created_by.as_deref(), Some("alice"));

        for random_hex in ["ABCDEF012345", "abcdef01234", "abcdef0123450"] {
            let error = create_channel_memory_entry(NewChannelMemoryEntry {
                text: "valid",
                created_by: None,
                now: "now",
                random_hex,
            })
            .unwrap_err();
            assert_eq!(
                error.to_string(),
                "Invalid randomHex for channel memory entry"
            );
        }
        assert_eq!(
            create_channel_memory_entry(NewChannelMemoryEntry {
                text: " \u{feff} ",
                created_by: None,
                now: "now",
                random_hex: "abcdef012345",
            })
            .unwrap_err()
            .to_string(),
            "Invalid channel memory entry"
        );
    }

    #[test]
    fn renders_recall_text_and_serializes_in_stable_pretty_form() {
        assert_eq!(render_channel_memory_recall(&[]), "");
        assert_eq!(
            render_channel_memory_recall(&[
                ChannelMemoryEntry {
                    text: "Use staging".into(),
                    created_by: Some("alice".into()),
                    ..entry("Use staging")
                },
                ChannelMemoryEntry {
                    text: "Run tests".into(),
                    updated_at: Some("now".into()),
                    ..entry("Run tests")
                },
            ]),
            "Use staging\nRun tests\n"
        );
        assert_eq!(
            serialize_channel_memory_document(&ChannelMemoryDocument::default()).unwrap(),
            "{\n  \"version\": 1,\n  \"entries\": []\n}\n"
        );
        let with_migration = ChannelMemoryDocument {
            migration: Some(ChannelMemoryMigration {
                legacy_sha256: "a".repeat(64),
                extra: Map::new(),
            }),
            entries: vec![entry("Use staging")],
            ..ChannelMemoryDocument::default()
        };
        assert_eq!(
            serialize_channel_memory_document(&with_migration).unwrap(),
            format!(
                "{{\n  \"version\": 1,\n  \"migration\": {{\n    \"legacySha256\": \"{}\"\n  }},\n  \"entries\": [\n    {{\n      \"id\": \"{MEMORY_ID}\",\n      \"text\": \"Use staging\"\n    }}\n  ]\n}}\n",
                "a".repeat(64)
            )
        );
    }

    #[test]
    fn serialization_does_not_silently_discard_unknown_keys() {
        let mut document = ChannelMemoryDocument::default();
        document
            .extra
            .insert("futureMetadata".into(), Value::Bool(true));
        assert_eq!(
            serialize_channel_memory_document(&document)
                .unwrap_err()
                .to_string(),
            "Invalid channel memory document"
        );

        let mut document = ChannelMemoryDocument::default();
        let mut invalid_entry = entry("Use staging");
        invalid_entry
            .extra
            .insert("futureMetadata".into(), Value::Bool(true));
        document.entries.push(invalid_entry);
        assert_eq!(
            serialize_channel_memory_document(&document)
                .unwrap_err()
                .to_string(),
            "Invalid channel memory entry"
        );

        let document = ChannelMemoryDocument {
            migration: Some(ChannelMemoryMigration {
                legacy_sha256: "a".repeat(64),
                extra: Map::from_iter([("futureMetadata".into(), Value::Bool(true))]),
            }),
            ..ChannelMemoryDocument::default()
        };
        assert_eq!(
            serialize_channel_memory_document(&document)
                .unwrap_err()
                .to_string(),
            "Invalid channel memory document"
        );
    }
}
