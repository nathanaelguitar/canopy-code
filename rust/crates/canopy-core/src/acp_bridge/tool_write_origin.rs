//! Tool write provenance carried in ACP metadata.

use serde_json::{Map, Number, Value};

/// Metadata key used to mark which tool initiated a file write.
pub const TOOL_WRITE_ORIGIN_META_KEY: &str = "canopy-code/tool-write-origin";

/// The write tools whose provenance may be attached to an ACP write request.
pub const TOOL_WRITE_ORIGINS: [ToolWriteOrigin; 4] = [
    ToolWriteOrigin::WriteFile,
    ToolWriteOrigin::Edit,
    ToolWriteOrigin::NotebookEdit,
    ToolWriteOrigin::ShellSedEdit,
];

/// A supported tool that initiated a file write.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ToolWriteOrigin {
    WriteFile,
    Edit,
    NotebookEdit,
    ShellSedEdit,
}

impl ToolWriteOrigin {
    /// Returns the stable metadata spelling for this origin.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::WriteFile => "write_file",
            Self::Edit => "edit",
            Self::NotebookEdit => "notebook_edit",
            Self::ShellSedEdit => "shell_sed_edit",
        }
    }

    fn from_str(value: &str) -> Option<Self> {
        match value {
            "write_file" => Some(Self::WriteFile),
            "edit" => Some(Self::Edit),
            "notebook_edit" => Some(Self::NotebookEdit),
            "shell_sed_edit" => Some(Self::ShellSedEdit),
            _ => None,
        }
    }
}

/// Returns a shallow copy of `meta` with the caller-supplied marker replaced
/// by `source`. Passing no source removes the marker. Empty results are omitted.
pub fn build_tool_write_origin_meta(
    meta: Option<&Map<String, Value>>,
    source: Option<ToolWriteOrigin>,
) -> Option<Map<String, Value>> {
    let mut sanitized = meta.cloned().unwrap_or_default();
    sanitized.remove(TOOL_WRITE_ORIGIN_META_KEY);

    if let Some(source) = source {
        let marker = Value::Object(Map::from_iter([
            ("version".to_owned(), Value::Number(Number::from(1))),
            (
                "source".to_owned(),
                Value::String(source.as_str().to_owned()),
            ),
        ]));
        sanitized.insert(TOOL_WRITE_ORIGIN_META_KEY.to_owned(), marker);
    }

    (!sanitized.is_empty()).then_some(sanitized)
}

/// Parses a valid tool write provenance marker from ACP metadata.
pub fn parse_tool_write_origin_meta(meta: Option<&Map<String, Value>>) -> Option<ToolWriteOrigin> {
    let marker = meta?.get(TOOL_WRITE_ORIGIN_META_KEY)?.as_object()?;
    if marker.len() != 2 {
        return None;
    }

    let version = marker.get("version")?.as_number()?;
    // JavaScript's `=== 1` also accepts JSON's `1.0`, since both become the
    // same Number value. Comparing through f64 keeps that behavior in Rust.
    if version.as_f64()? != 1.0 {
        return None;
    }

    ToolWriteOrigin::from_str(marker.get("source")?.as_str()?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn metadata(value: Value) -> Map<String, Value> {
        value.as_object().expect("test metadata object").clone()
    }

    #[test]
    fn round_trips_each_supported_origin_and_preserves_other_metadata() {
        let input = metadata(json!({"bom": true, "nested": {"keep": [1, 2]}}));

        for source in TOOL_WRITE_ORIGINS {
            let built = build_tool_write_origin_meta(Some(&input), Some(source)).unwrap();
            assert_eq!(parse_tool_write_origin_meta(Some(&built)), Some(source));
            assert_eq!(built.get("bom"), Some(&Value::Bool(true)));
            assert_eq!(built.get("nested"), input.get("nested"));
            assert_eq!(
                built.get(TOOL_WRITE_ORIGIN_META_KEY),
                Some(&json!({"version": 1, "source": source.as_str()}))
            );
        }
    }

    #[test]
    fn replaces_caller_supplied_marker() {
        let input = metadata(json!({
            "canopy-code/tool-write-origin": {"version": 1, "source": "write_file"},
            "other": "preserved"
        }));

        let built =
            build_tool_write_origin_meta(Some(&input), Some(ToolWriteOrigin::Edit)).unwrap();

        assert_eq!(
            parse_tool_write_origin_meta(Some(&built)),
            Some(ToolWriteOrigin::Edit)
        );
        assert_eq!(built.get("other"), Some(&json!("preserved")));
    }

    #[test]
    fn removes_marker_and_omits_empty_result() {
        let with_marker = metadata(json!({
            "canopy-code/tool-write-origin": {"version": 1, "source": "edit"}
        }));
        assert_eq!(build_tool_write_origin_meta(Some(&with_marker), None), None);

        let with_other_fields = metadata(json!({
            "canopy-code/tool-write-origin": {"version": 1, "source": "edit"},
            "bom": true
        }));
        let built = build_tool_write_origin_meta(Some(&with_other_fields), None).unwrap();
        assert_eq!(built, metadata(json!({"bom": true})));
        assert_eq!(build_tool_write_origin_meta(None, None), None);
    }

    #[test]
    fn parses_javascript_equivalent_numeric_version_one() {
        let meta = metadata(json!({
            "canopy-code/tool-write-origin": {"version": 1.0, "source": "notebook_edit"}
        }));

        assert_eq!(
            parse_tool_write_origin_meta(Some(&meta)),
            Some(ToolWriteOrigin::NotebookEdit)
        );
    }

    #[test]
    fn rejects_invalid_markers() {
        let invalid_markers = [
            Value::Null,
            json!([]),
            json!("write_file"),
            json!({"version": 2, "source": "write_file"}),
            json!({"version": "1", "source": "write_file"}),
            json!({"version": true, "source": "write_file"}),
            json!({"version": 1, "source": "unknown"}),
            json!({"version": 1, "source": "write_file", "extra": true}),
            json!({"source": "write_file"}),
        ];

        for marker in invalid_markers {
            let meta = Map::from_iter([(TOOL_WRITE_ORIGIN_META_KEY.to_owned(), marker)]);
            assert_eq!(parse_tool_write_origin_meta(Some(&meta)), None);
        }

        assert_eq!(parse_tool_write_origin_meta(None), None);
        assert_eq!(parse_tool_write_origin_meta(Some(&Map::new())), None);
    }
}
