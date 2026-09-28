//! MCP result projection for Canopy's Computer Use tool responses.
//!
//! This ports `buildLlmContent`, `buildDisplayText`, and
//! `stringifyStructured` from `packages/core/src/tools/computer-use/tool.ts`.

use serde::ser::SerializeMap;
use serde::{Serialize, Serializer};
use serde_json::{Map, Value, json};

/// Project MCP content blocks to the Gemini-compatible string-or-parts shape.
/// Text-only results stay strings; image and audio payloads remain inline so
/// the model can inspect screenshots and recordings.
pub fn build_llm_content(
    content: Vec<Value>,
    tool_name: &str,
    structured_content: Option<&Value>,
) -> Value {
    let mut parts = Vec::new();

    for block in content {
        let Value::Object(mut block) = block else {
            continue;
        };
        let kind = block
            .remove("type")
            .and_then(|value| value.as_str().map(str::to_owned));
        match kind.as_deref() {
            Some("text") => {
                if let Some(Value::String(text)) = block.remove("text")
                    && !text.is_empty()
                {
                    parts.push(text_part(text));
                }
            }
            Some(kind @ ("image" | "audio")) => {
                let mime_type = block.remove("mimeType");
                let data = block.remove("data");
                if let (Some(Value::String(mime_type)), Some(Value::String(data))) =
                    (mime_type, data)
                    && !mime_type.is_empty()
                    && !data.is_empty()
                {
                    parts.push(json!({
                        "text": format!(
                            "[Tool '{tool_name}' provided the following {kind} data with mime-type: {mime_type}]"
                        )
                    }));
                    let mut inline_data = Map::new();
                    inline_data.insert("mimeType".to_owned(), Value::String(mime_type));
                    inline_data.insert("data".to_owned(), Value::String(data));
                    let mut inline_part = Map::new();
                    inline_part.insert("inlineData".to_owned(), Value::Object(inline_data));
                    parts.push(Value::Object(inline_part));
                }
            }
            _ => {}
        }
    }

    if let Some(structured_text) = stringify_structured(structured_content) {
        parts.push(json!({"text": format!("Structured result: {structured_text}")}));
    }

    if !parts.iter().any(|part| part.get("inlineData").is_some()) {
        let text = parts
            .iter()
            .filter_map(|part| part.get("text").and_then(Value::as_str))
            .filter(|text| !text.is_empty())
            .collect::<Vec<_>>()
            .join("\n");
        return Value::String(text);
    }

    Value::Array(parts)
}

fn text_part(text: String) -> Value {
    let mut part = Map::new();
    part.insert("text".to_owned(), Value::String(text));
    Value::Object(part)
}

/// Return only text content, omitting image/audio bytes and other block types.
pub fn build_display_text(content: &[Value]) -> String {
    content
        .iter()
        .filter(|block| block.get("type").and_then(Value::as_str) == Some("text"))
        .filter_map(|block| block.get("text").and_then(Value::as_str))
        .filter(|text| !text.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

/// Compactly serialize useful structured content, excluding `tree_markdown`
/// because the same accessibility tree already appears in the text block.
pub fn stringify_structured(structured: Option<&Value>) -> Option<String> {
    let Value::Object(object) = structured? else {
        return None;
    };
    let included_fields = object
        .keys()
        .filter(|key| key.as_str() != "tree_markdown")
        .count();
    if included_fields == 0 {
        return None;
    }
    serde_json::to_string(&StructuredContentWithoutTreeMarkdown(object)).ok()
}

/// Serialize the structured result without cloning potentially large element
/// arrays. The source TypeScript path filters into a new object, but Rust can
/// preserve the exact JSON projection by serializing borrowed fields directly.
struct StructuredContentWithoutTreeMarkdown<'a>(&'a Map<String, Value>);

impl Serialize for StructuredContentWithoutTreeMarkdown<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let included_fields = self
            .0
            .keys()
            .filter(|key| key.as_str() != "tree_markdown")
            .count();
        let mut map = serializer.serialize_map(Some(included_fields))?;
        for (key, value) in self.0 {
            if key != "tree_markdown" {
                map.serialize_entry(key, value)?;
            }
        }
        map.end()
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::{build_display_text, build_llm_content, stringify_structured};

    #[test]
    fn text_only_content_collapses_to_a_plain_string() {
        assert_eq!(
            build_llm_content(
                vec![
                    json!({"type":"text","text":"hello"}),
                    json!({"type":"text","text":"world"})
                ],
                "get_window_state",
                None,
            ),
            Value::String("hello\nworld".to_owned())
        );
        assert_eq!(
            build_llm_content(Vec::new(), "noop", None),
            Value::String(String::new())
        );
    }

    #[test]
    fn image_and_audio_content_keep_inline_payloads_and_context_labels() {
        let content = vec![
            json!({"type":"text","text":"screenshot below"}),
            json!({"type":"image","mimeType":"image/png","data":"base64data=="}),
            json!({"type":"audio","mimeType":"audio/wav","data":"wave=="}),
        ];
        let result = build_llm_content(content, "get_window_state", None);
        let parts = result.as_array().unwrap();
        assert_eq!(parts[0]["text"], "screenshot below");
        assert_eq!(
            parts[1]["text"],
            "[Tool 'get_window_state' provided the following image data with mime-type: image/png]"
        );
        assert_eq!(parts[2]["inlineData"]["mimeType"], "image/png");
        assert_eq!(parts[2]["inlineData"]["data"], "base64data==");
        assert_eq!(parts[4]["inlineData"]["mimeType"], "audio/wav");
        assert_eq!(parts[4]["inlineData"]["data"], "wave==");
    }

    #[test]
    fn structured_content_is_forwarded_without_duplicated_tree_markdown() {
        let result = build_llm_content(
            vec![json!({"type":"text","text":"window_id=358"})],
            "get_window_state",
            Some(&json!({
                "window_id": 358,
                "tree_markdown": "X".repeat(5000)
            })),
        );
        let text = result.as_str().unwrap();
        assert!(text.contains("Structured result: {\"window_id\":358}"));
        assert!(!text.contains('X'));
    }

    #[test]
    fn structured_values_that_are_not_objects_or_have_no_useful_keys_are_omitted() {
        assert_eq!(stringify_structured(Some(&json!(null))), None);
        assert_eq!(
            stringify_structured(Some(&json!({"tree_markdown":"already rendered"}))),
            None
        );
    }

    #[test]
    fn display_projection_contains_text_only() {
        assert_eq!(
            build_display_text(&[
                json!({"type":"text","text":"line 1"}),
                json!({"type":"image","mimeType":"image/png","data":"secret"}),
                json!({"type":"text","text":"line 2"}),
            ]),
            "line 1\nline 2"
        );
        assert_eq!(
            build_display_text(&[json!({"type":"image","data":"secret"})]),
            ""
        );
    }
}
