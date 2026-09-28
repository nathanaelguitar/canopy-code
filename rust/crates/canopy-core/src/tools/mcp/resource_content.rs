//! Formatting helpers for untrusted MCP resource content.
//!
//! The wire response is accepted as JSON so malformed server payloads can be
//! handled explicitly. Text and blob payloads are capped before being framed
//! as model-facing content parts.

use serde_json::{Value, json};
use thiserror::Error;
use uuid::Uuid;

/// Maximum cumulative text payload, measured in JavaScript UTF-16 code units.
pub const MAX_MCP_RESOURCE_TEXT_CHARS: usize = 100_000;

/// Maximum cumulative base64 blob payload, measured in UTF-16 code units.
pub const MAX_MCP_RESOURCE_BLOB_CHARS: usize = 8_000_000;

const TRUNCATION_NOTICE: &str =
    "\n[Content truncated — part of this resource exceeded size limits and was omitted.]";

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FormattedMcpResource {
    /// Gemini-compatible parts: text parts use `text`; blob parts use
    /// `inlineData: { mimeType, data }`.
    pub parts: Vec<Value>,
    /// Number of text UTF-16 code units actually injected, excluding framing.
    pub text_chars: usize,
    /// Number of blob attachments actually injected.
    pub blob_count: usize,
    /// Number of blob UTF-16 code units actually injected.
    pub blob_chars: usize,
    /// True if text was sliced or any text/blob payload was skipped.
    pub truncated: bool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FormatMcpResourceOptions {
    /// Per-call cumulative blob budget. Defaults to
    /// [`MAX_MCP_RESOURCE_BLOB_CHARS`].
    pub max_blob_chars: Option<usize>,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum MalformedMcpResourceError {
    #[error("MCP resource result cannot be null")]
    ResultIsNull,
    #[error("MCP resource contents must be an array or null")]
    ContentsNotArray,
    #[error("MCP resource content item must be an object")]
    ContentItemNotObject,
}

/// Convert a raw MCP `resources/read` result into model-ready parts.
///
/// Missing or null `contents` produces an empty result. A null result,
/// non-array `contents`, and primitive array entries are rejected: the
/// TypeScript source throws for those shapes when reading / iterating / using
/// the `in` operator. Array entries and objects with neither a string `text`
/// nor a string `blob` are silently ignored.
pub fn format_mcp_resource_contents(
    result: &Value,
    label: &str,
    options: Option<FormatMcpResourceOptions>,
) -> Result<FormattedMcpResource, MalformedMcpResourceError> {
    if result.is_null() {
        return Err(MalformedMcpResourceError::ResultIsNull);
    }
    let max_blob_chars = options
        .and_then(|options| options.max_blob_chars)
        .unwrap_or(MAX_MCP_RESOURCE_BLOB_CHARS);

    let contents = match result.get("contents") {
        None | Some(Value::Null) => &[][..],
        Some(Value::Array(contents)) => contents.as_slice(),
        Some(_) => return Err(MalformedMcpResourceError::ContentsNotArray),
    };

    let mut content_parts = Vec::new();
    let mut text_chars = 0usize;
    let mut blob_chars = 0usize;
    let mut blob_count = 0usize;
    let mut truncated = false;

    for content in contents {
        let Some(content) = content.as_object() else {
            // JSON arrays are objects in JavaScript, so the source's `in`
            // checks succeed and find no named `text` or `blob` properties.
            if content.is_array() {
                continue;
            }
            return Err(MalformedMcpResourceError::ContentItemNotObject);
        };

        if let Some(text) = content.get("text").and_then(Value::as_str) {
            let text_length = utf16_len(text);
            let remaining = MAX_MCP_RESOURCE_TEXT_CHARS.saturating_sub(text_chars);
            if remaining == 0 {
                truncated |= text_length > 0;
                continue;
            }

            let (injected_text, injected_length) = prefix_by_utf16_units(text, remaining);
            if injected_length < text_length {
                truncated = true;
            }
            if injected_length > 0 {
                content_parts.push(json!({"text": injected_text}));
                text_chars += injected_length;
            }
        } else if let Some(blob) = content.get("blob").and_then(Value::as_str) {
            let length = utf16_len(blob);
            if blob_chars.saturating_add(length) > max_blob_chars {
                truncated = true;
                continue;
            }

            blob_chars += length;
            let mime_type = content
                .get("mimeType")
                .and_then(Value::as_str)
                .unwrap_or("application/octet-stream");
            content_parts.push(json!({
                "inlineData": {
                    "mimeType": mime_type,
                    "data": blob,
                }
            }));
            blob_count += 1;
        }
    }

    let parts = if content_parts.is_empty() {
        Vec::new()
    } else {
        // Keep the closing delimiter unpredictable to content supplied by an
        // untrusted MCP server.
        let nonce = Uuid::new_v4().simple().to_string();
        let nonce = &nonce[..8];
        let mut parts = Vec::with_capacity(content_parts.len() + 2);
        parts.push(json!({
            "text": format!("\n--- Content from MCP resource {label} [{nonce}] ---\n")
        }));
        parts.extend(content_parts);
        let truncation_notice = if truncated { TRUNCATION_NOTICE } else { "" };
        parts.push(json!({
            "text": format!(
                "{truncation_notice}\n--- End of MCP resource {label} [{nonce}] ---\n"
            )
        }));
        parts
    };

    Ok(FormattedMcpResource {
        parts,
        text_chars,
        blob_count,
        blob_chars,
        truncated,
    })
}

/// Return a model-facing diagnostic when formatting yielded no parts.
pub fn empty_mcp_resource_text(formatted: &FormattedMcpResource, label: &str) -> String {
    format!(
        "\n--- MCP resource {label}: {} ---\n",
        summarize_mcp_resource(formatted)
    )
}

/// Summarize the payload that was actually injected.
pub fn summarize_mcp_resource(formatted: &FormattedMcpResource) -> String {
    let mut summary = Vec::new();
    if formatted.text_chars > 0 {
        summary.push(format!("{} chars", formatted.text_chars));
    }
    if formatted.blob_count > 0 {
        let suffix = if formatted.blob_count == 1 {
            "attachment"
        } else {
            "attachments"
        };
        summary.push(format!("{} {suffix}", formatted.blob_count));
    }

    if !summary.is_empty() {
        return format!(
            "Injected {}{}",
            summary.join(" + "),
            if formatted.truncated {
                " (truncated)"
            } else {
                ""
            }
        );
    }
    if formatted.truncated {
        "(content too large — skipped)".to_owned()
    } else {
        "(no readable content)".to_owned()
    }
}

/// Concatenate the text fields of content parts, ignoring inline data parts.
pub fn extract_text_parts(parts: &[Value]) -> String {
    parts
        .iter()
        .filter_map(|part| part.get("text").and_then(Value::as_str))
        .collect()
}

fn utf16_len(value: &str) -> usize {
    value.encode_utf16().count()
}

/// Slice at a UTF-16 code-unit boundary while retaining valid Rust UTF-8.
///
/// If the requested boundary falls between the two code units of a non-BMP
/// scalar, the scalar is omitted in full. JavaScript can retain a lone
/// surrogate at that boundary; Rust `String` cannot represent one.
fn prefix_by_utf16_units(value: &str, limit: usize) -> (String, usize) {
    let mut output = String::new();
    let mut used = 0;
    for character in value.chars() {
        let units = character.len_utf16();
        if used + units > limit {
            break;
        }
        output.push(character);
        used += units;
    }
    (output, used)
}

#[cfg(test)]
mod tests {
    use super::{
        FormatMcpResourceOptions, MAX_MCP_RESOURCE_BLOB_CHARS, MAX_MCP_RESOURCE_TEXT_CHARS,
        MalformedMcpResourceError, empty_mcp_resource_text, extract_text_parts,
        format_mcp_resource_contents, summarize_mcp_resource,
    };
    use serde_json::{Value, json};

    fn format(result: &Value, label: &str) -> super::FormattedMcpResource {
        format_mcp_resource_contents(result, label, None).unwrap()
    }

    fn nonce_headers(joined: &str) -> Vec<&str> {
        let marker = "[";
        let mut values = Vec::new();
        let mut rest = joined;
        while let Some(start) = rest.find(marker) {
            let after = &rest[start + 1..];
            let Some(end) = after.find(']') else {
                break;
            };
            let value = &after[..end];
            if value.len() == 8 && value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                values.push(value);
            }
            rest = &after[end + 1..];
        }
        values
    }

    #[test]
    fn frames_text_with_a_shared_eight_digit_nonce() {
        let out = format(
            &json!({"contents":[{"uri":"x://a","text":"hello"}]}),
            "srv:x://a",
        );
        assert!(!out.truncated);
        assert_eq!(out.text_chars, 5);
        let joined = extract_text_parts(&out.parts);
        assert!(joined.contains("--- Content from MCP resource srv:x://a ["));
        assert!(joined.contains("hello"));
        assert!(joined.contains("--- End of MCP resource srv:x://a ["));
        let nonces = nonce_headers(&joined);
        assert_eq!(nonces.len(), 2);
        assert_eq!(nonces[0], nonces[1]);
    }

    #[test]
    fn caps_cumulative_text_and_marks_the_frame() {
        let out = format(
            &json!({"contents":[{"text":"a".repeat(MAX_MCP_RESOURCE_TEXT_CHARS + 100)}]}),
            "srv",
        );
        assert!(out.truncated);
        assert_eq!(out.text_chars, MAX_MCP_RESOURCE_TEXT_CHARS);
        assert!(extract_text_parts(&out.parts).contains("[Content truncated"));
    }

    #[test]
    fn exact_text_cap_is_not_truncated() {
        let out = format(
            &json!({"contents":[{"text":"a".repeat(MAX_MCP_RESOURCE_TEXT_CHARS)}]}),
            "srv",
        );
        assert!(!out.truncated);
        assert_eq!(out.text_chars, MAX_MCP_RESOURCE_TEXT_CHARS);
        assert!(!extract_text_parts(&out.parts).contains("[Content truncated"));
    }

    #[test]
    fn accumulates_text_budget_across_items() {
        let half = MAX_MCP_RESOURCE_TEXT_CHARS.div_ceil(2) + 100;
        let out = format(
            &json!({"contents":[{"text":"a".repeat(half)},{"text":"b".repeat(half)}]}),
            "srv",
        );
        assert_eq!(out.text_chars, MAX_MCP_RESOURCE_TEXT_CHARS);
        assert!(out.truncated);
    }

    #[test]
    fn skips_blobs_when_cumulative_budget_would_be_exceeded() {
        let half = MAX_MCP_RESOURCE_BLOB_CHARS.div_ceil(2) + 1;
        let blob = "b".repeat(half);
        let out = format(
            &json!({"contents":[
                {"blob":blob,"mimeType":"image/png"},
                {"blob":blob,"mimeType":"image/png"}
            ]}),
            "srv",
        );
        assert_eq!(out.blob_count, 1);
        assert!(out.truncated);
        assert_eq!(
            out.parts
                .iter()
                .filter(|part| part.get("inlineData").is_some())
                .count(),
            1
        );
    }

    #[test]
    fn missing_null_and_empty_contents_yield_no_parts() {
        for result in [json!({}), json!({"contents":null}), json!({"contents":[]})] {
            let out = format(&result, "srv");
            assert!(out.parts.is_empty());
            assert_eq!(summarize_mcp_resource(&out), "(no readable content)");
        }
        assert_eq!(
            empty_mcp_resource_text(&format(&json!({"contents":[]}), "srv"), "srv"),
            "\n--- MCP resource srv: (no readable content) ---\n"
        );
    }

    #[test]
    fn summarizes_text_and_attachment_counts() {
        let out = format(
            &json!({"contents":[
                {"text":"hi"},
                {"blob":"aGk=","mimeType":"image/png"}
            ]}),
            "srv",
        );
        assert_eq!(
            summarize_mcp_resource(&out),
            "Injected 2 chars + 1 attachment"
        );

        let two_blobs = format(
            &json!({"contents":[{"blob":"aGk="},{"blob":"aGk="}]}),
            "srv",
        );
        assert_eq!(two_blobs.blob_count, 2);
        assert_eq!(summarize_mcp_resource(&two_blobs), "Injected 2 attachments");
    }

    #[test]
    fn defaults_blob_mime_type_and_reports_blob_chars() {
        let out = format(&json!({"contents":[{"blob":"aGk="}]}), "srv");
        let inline = out
            .parts
            .iter()
            .find_map(|part| part.get("inlineData"))
            .unwrap();
        assert_eq!(inline["mimeType"], "application/octet-stream");
        assert_eq!(inline["data"], "aGk=");
        assert_eq!(out.blob_chars, 4);
    }

    #[test]
    fn skips_object_items_without_text_or_blob() {
        let out = format(&json!({"contents":[{"uri":"x://link"}, []]}), "srv");
        assert!(out.parts.is_empty());
        assert!(!out.truncated);
        assert_eq!(summarize_mcp_resource(&out), "(no readable content)");
    }

    #[test]
    fn reports_oversized_blob_when_nothing_was_injected() {
        let out = format(
            &json!({"contents":[{
                "blob":"A".repeat(MAX_MCP_RESOURCE_BLOB_CHARS + 1),
                "mimeType":"image/png"
            }]}),
            "srv",
        );
        assert_eq!(out.blob_count, 0);
        assert_eq!(out.blob_chars, 0);
        assert!(out.truncated);
        assert_eq!(
            summarize_mcp_resource(&out),
            "(content too large — skipped)"
        );
    }

    #[test]
    fn honors_a_per_call_blob_budget_and_skips_only_oversized_entries() {
        let out = format_mcp_resource_contents(
            &json!({"contents":[
                {"blob":"a".repeat(100)},
                {"blob":"b".repeat(50)}
            ]}),
            "srv",
            Some(FormatMcpResourceOptions {
                max_blob_chars: Some(50),
            }),
        )
        .unwrap();
        assert_eq!(out.blob_count, 1);
        assert_eq!(out.blob_chars, 50);
        assert!(out.truncated);
    }

    #[test]
    fn counts_non_bmp_text_as_two_utf16_units() {
        let out = format(&json!({"contents":[{"text":"a😀b"}]}), "srv");
        assert_eq!(out.text_chars, 4);
        assert!(extract_text_parts(&out.parts).contains("a😀b"));
    }

    #[test]
    fn malformed_iterable_shapes_return_errors() {
        assert_eq!(
            format_mcp_resource_contents(&Value::Null, "srv", None).unwrap_err(),
            MalformedMcpResourceError::ResultIsNull
        );
        assert_eq!(
            format_mcp_resource_contents(&json!({"contents":{}}), "srv", None).unwrap_err(),
            MalformedMcpResourceError::ContentsNotArray
        );
        assert_eq!(
            format_mcp_resource_contents(&json!({"contents":[null]}), "srv", None).unwrap_err(),
            MalformedMcpResourceError::ContentItemNotObject
        );
    }
}
