use std::sync::LazyLock;

use regex::Regex;
use serde_json::Value;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PermissionErrorKind {
    None,
    Other,
    Accessibility,
    ScreenRecording,
    UnknownPermission,
}

impl PermissionErrorKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Other => "other",
            Self::Accessibility => "accessibility",
            Self::ScreenRecording => "screenRecording",
            Self::UnknownPermission => "unknown_permission",
        }
    }
}

static PATTERNS: LazyLock<[(PermissionErrorKind, Regex); 5]> = LazyLock::new(|| {
    [
        (
            PermissionErrorKind::Accessibility,
            Regex::new(r"(?i)accessibility:?\s*(missing|denied|not granted)").unwrap(),
        ),
        (
            PermissionErrorKind::Accessibility,
            Regex::new(r"(?i)accessibility permission").unwrap(),
        ),
        (
            PermissionErrorKind::ScreenRecording,
            Regex::new(r"(?i)screen recording:?\s*(missing|denied|not granted)").unwrap(),
        ),
        (
            PermissionErrorKind::ScreenRecording,
            Regex::new(r"(?i)screen recording permission").unwrap(),
        ),
        (
            PermissionErrorKind::UnknownPermission,
            Regex::new(r"(?i)missing tcc grant|needs your permission").unwrap(),
        ),
    ]
});

/// Classify an MCP `CallToolResult` represented as JSON. Only text content is
/// inspected; permission patterns are checked in the same precedence order as
/// the TypeScript implementation.
pub fn detect_permission_error(result: &Value) -> PermissionErrorKind {
    if result.get("isError") != Some(&Value::Bool(true)) {
        return PermissionErrorKind::None;
    }
    let text = result
        .get("content")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|part| part.get("type").and_then(Value::as_str) == Some("text"))
        .filter_map(|part| part.get("text").and_then(Value::as_str))
        .collect::<Vec<_>>()
        .join("\n");

    PATTERNS
        .iter()
        .find_map(|(kind, pattern)| pattern.is_match(&text).then_some(*kind))
        .unwrap_or(PermissionErrorKind::Other)
}
