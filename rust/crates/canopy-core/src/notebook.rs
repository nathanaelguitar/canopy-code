use std::collections::HashSet;
use std::sync::OnceLock;

use regex::Regex;
use serde::Serialize;
use serde_json::{Map, Value};

const MAX_CELL_OUTPUT_CHARS: usize = 10_000;
const MAX_NOTEBOOK_OUTPUT_CHARS: usize = 100_000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NotebookReadResult {
    pub content: String,
    pub is_truncated: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NotebookEditParams {
    pub notebook_path: String,
    pub cell_id: Option<String>,
    pub new_source: Option<String>,
    pub cell_type: Option<String>,
    pub edit_mode: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NotebookEditResult {
    pub updated_content: String,
    pub edited_cell_id: String,
    pub edited_cell_type: Option<String>,
    pub language: String,
    pub mode: String,
    pub requires_read_after_write: bool,
}

pub fn render_notebook(raw: &str) -> Result<NotebookReadResult, String> {
    let notebook = parse_notebook(raw)?;
    let cells = notebook
        .get("cells")
        .and_then(Value::as_array)
        .ok_or_else(|| "Invalid notebook: missing cells array".to_owned())?;
    if cells.is_empty() {
        return Ok(NotebookReadResult {
            content: "(empty notebook)".to_owned(),
            is_truncated: false,
        });
    }
    let language = notebook_language(&notebook);
    let header = format!("Jupyter Notebook ({language}, {} cells)", cells.len());
    let mut total = utf16_len(&header);
    let mut rendered = Vec::new();
    let mut is_truncated = false;
    for (index, cell) in cells.iter().enumerate() {
        let text = render_cell(cell, index, &language);
        total = total.saturating_add(utf16_len(&text)).saturating_add(2);
        if total > MAX_NOTEBOOK_OUTPUT_CHARS {
            is_truncated = true;
            rendered.push(format!(
                "... [{} remaining cells truncated, total {} cells. Use notebook_edit with cell IDs shown by a full read.]",
                cells.len() - index,
                cells.len()
            ));
            break;
        }
        rendered.push(text);
    }
    Ok(NotebookReadResult {
        content: format!("{header}\n\n{}", rendered.join("\n\n")),
        is_truncated,
    })
}

pub fn apply_notebook_edit(
    raw: &str,
    params: &NotebookEditParams,
) -> Result<NotebookEditResult, String> {
    let mut notebook = parse_notebook(raw)?;
    let mode = params.edit_mode.as_deref().unwrap_or("replace");
    if !matches!(mode, "replace" | "insert" | "delete") {
        return Err(format!("Unsupported notebook edit mode: {mode}"));
    }
    let source = match mode {
        "delete" => String::new(),
        _ => params
            .new_source
            .clone()
            .ok_or_else(|| format!("new_source is required when edit_mode is \"{mode}\"."))?,
    };
    let cell_type = params.cell_type.as_deref();
    if cell_type.is_some_and(|value| !matches!(value, "code" | "markdown")) {
        return Err("cell_type must be 'code' or 'markdown'.".to_owned());
    }
    let language = notebook_language(&notebook);
    let generate_cell_ids = should_generate_cell_ids(&notebook);
    let cells = notebook
        .get_mut("cells")
        .and_then(Value::as_array_mut)
        .ok_or_else(|| "Invalid notebook: missing cells array".to_owned())?;
    let original_stable_ids = has_stable_cell_ids(cells);
    let target_index = resolve_target_index(cells, params.cell_id.as_deref(), mode)?;
    let indentation = infer_indentation(raw);
    let trailing_newline = raw.ends_with('\n');

    let (edited_cell_id, edited_cell_type) = match mode {
        "insert" => {
            let insert_at = if target_index < 0 {
                0
            } else {
                target_index as usize + 1
            };
            let actual_type = cell_type.unwrap_or("code");
            let prefer_array = infer_inserted_source_array_style(cells, insert_at);
            let mut cell = Map::new();
            cell.insert(
                "cell_type".to_owned(),
                Value::String(actual_type.to_owned()),
            );
            cell.insert("metadata".to_owned(), Value::Object(Map::new()));
            cell.insert(
                "source".to_owned(),
                to_notebook_source(&source, prefer_array),
            );
            if generate_cell_ids {
                cell.insert("id".to_owned(), Value::String(make_cell_id(cells)));
            }
            normalize_edited_cell(&mut cell, actual_type);
            let display_id = cell
                .get("id")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .unwrap_or_else(|| format!("cell-{insert_at}"));
            cells.insert(insert_at, Value::Object(cell));
            (display_id, Some(actual_type.to_owned()))
        }
        "delete" => {
            let removed = cells.remove(target_index as usize);
            let display_id = display_cell_id(&removed, target_index as usize);
            let cell_type = removed
                .get("cell_type")
                .and_then(Value::as_str)
                .map(str::to_owned);
            (display_id, cell_type)
        }
        "replace" => {
            let target = cells
                .get_mut(target_index as usize)
                .and_then(Value::as_object_mut)
                .ok_or_else(|| format!("Cell index {target_index} is out of range."))?;
            let id = display_cell_id(&Value::Object(target.clone()), target_index as usize);
            let actual_type = cell_type
                .map(str::to_owned)
                .or_else(|| {
                    target
                        .get("cell_type")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                })
                .unwrap_or_else(|| "code".to_owned());
            let source_array = target.get("source").is_some_and(Value::is_array);
            target.insert(
                "source".to_owned(),
                to_notebook_source(&source, source_array),
            );
            normalize_edited_cell(target, &actual_type);
            (id, Some(actual_type))
        }
        _ => unreachable!("mode validated above"),
    };

    let requires_read_after_write =
        matches!(mode, "insert" | "delete") && !(original_stable_ids && has_stable_cell_ids(cells));
    let updated_content = serialize_notebook(&notebook, indentation.as_deref(), trailing_newline)?;
    Ok(NotebookEditResult {
        updated_content,
        edited_cell_id,
        edited_cell_type,
        language,
        mode: mode.to_owned(),
        requires_read_after_write,
    })
}

fn parse_notebook(raw: &str) -> Result<Value, String> {
    let raw = raw.strip_prefix('\u{feff}').unwrap_or(raw);
    let value: Value =
        serde_json::from_str(raw).map_err(|error| format!("Invalid notebook JSON: {error}"))?;
    let object = value
        .as_object()
        .ok_or_else(|| "Invalid notebook: expected a JSON object".to_owned())?;
    let cells = object
        .get("cells")
        .and_then(Value::as_array)
        .ok_or_else(|| "Invalid notebook: missing cells array".to_owned())?;
    for (index, cell) in cells.iter().enumerate() {
        if !cell.is_object() {
            return Err(format!(
                "Invalid notebook: cell at index {index} is not an object"
            ));
        }
    }
    Ok(value)
}

fn render_cell(cell: &Value, index: usize, language: &str) -> String {
    let cell_id = display_cell_id(cell, index);
    let source = normalize_source(cell.get("source"));
    let mut parts = Vec::new();
    match cell.get("cell_type").and_then(Value::as_str).unwrap_or("") {
        "code" => {
            let execution = cell
                .get("execution_count")
                .filter(|value| !value.is_null())
                .map(|value| format!(" [{}]", value));
            parts.push(format!(
                "--- Code Cell {cell_id}{} ---",
                execution.unwrap_or_default()
            ));
            parts.push(format!("```{language}\n{source}\n```"));
            if let Some(outputs) = cell.get("outputs").and_then(Value::as_array) {
                let texts = outputs
                    .iter()
                    .map(process_output)
                    .filter(|text| !text.is_empty())
                    .collect::<Vec<_>>();
                if !texts.is_empty() {
                    let combined = texts.join("\n");
                    let total_chars = utf16_len(&combined);
                    let displayed = if total_chars > MAX_CELL_OUTPUT_CHARS {
                        format!(
                            "{}\n... [output truncated, total {} chars]",
                            utf16_prefix(&combined, MAX_CELL_OUTPUT_CHARS),
                            total_chars
                        )
                    } else {
                        combined
                    };
                    parts.push(format!("Output:\n{displayed}"));
                }
            }
        }
        "markdown" => {
            parts.push(format!("--- Markdown Cell {cell_id} ---"));
            parts.push(source);
        }
        "raw" => {
            parts.push(format!("--- Raw Cell {cell_id} ---"));
            parts.push(source);
        }
        _ => {
            parts.push(format!("--- Cell {cell_id} ---"));
            parts.push(source);
        }
    }
    parts.join("\n")
}

fn process_output(output: &Value) -> String {
    match output
        .get("output_type")
        .and_then(Value::as_str)
        .unwrap_or("")
    {
        "stream" => strip_ansi(&normalize_source(output.get("text"))),
        "execute_result" | "display_data" => {
            let data = output.get("data").and_then(Value::as_object);
            if let Some(text) = data.and_then(|data| data.get("text/plain")) {
                let rendered = normalize_source(Some(text));
                if !rendered.is_empty() {
                    return strip_ansi(&rendered);
                }
            }
            let mime_types = data
                .into_iter()
                .flat_map(|data| data.keys())
                .filter(|key| valid_mime_type(key))
                .cloned()
                .collect::<Vec<_>>();
            if mime_types.is_empty() {
                String::new()
            } else {
                format!("[non-text output: {}]", mime_types.join(", "))
            }
        }
        "error" => {
            let mut pieces = Vec::new();
            if let Some(name) = output.get("ename").and_then(Value::as_str) {
                pieces.push(name.to_owned());
            }
            if let Some(value) = output.get("evalue").and_then(Value::as_str) {
                pieces.push(value.to_owned());
            }
            if let Some(traceback) = output.get("traceback").and_then(Value::as_array) {
                let lines = traceback
                    .iter()
                    .filter_map(Value::as_str)
                    .collect::<Vec<_>>();
                if !lines.is_empty() {
                    pieces.push(lines.join("\n"));
                }
            }
            strip_ansi(&pieces.join(": "))
        }
        _ => String::new(),
    }
}

fn valid_mime_type(value: &str) -> bool {
    let Some((major, minor)) = value.split_once('/') else {
        return false;
    };
    if major.is_empty() || minor.is_empty() || minor.matches('+').count() > 1 {
        return false;
    }
    value.bytes().all(|byte| {
        byte.is_ascii_alphanumeric()
            || matches!(
                byte,
                b'!' | b'#' | b'$' | b'&' | b'^' | b'_' | b'.' | b'+' | b'-' | b'/'
            )
    })
}

fn strip_ansi(input: &str) -> String {
    static ANSI: OnceLock<Regex> = OnceLock::new();
    ANSI.get_or_init(|| {
        Regex::new(r"\x1B(?:\[[0-?]*[ -/]*[@-~]|\][^\x07\x1B]*(?:\x07|\x1B\\)|[P_^X][^\x1B]*\x1B\\|[@-Z\\-_])")
            .expect("ANSI escape regex is valid")
    })
    .replace_all(input, "")
    .into_owned()
}

fn normalize_source(source: Option<&Value>) -> String {
    match source {
        Some(Value::String(text)) => text.clone(),
        Some(Value::Array(lines)) => lines.iter().filter_map(Value::as_str).collect(),
        _ => String::new(),
    }
}

fn display_cell_id(cell: &Value, index: usize) -> String {
    cell.get("id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| format!("cell-{index}"))
}

fn notebook_language(notebook: &Value) -> String {
    notebook
        .pointer("/metadata/language_info/name")
        .and_then(Value::as_str)
        .or_else(|| {
            notebook
                .pointer("/metadata/kernelspec/language")
                .and_then(Value::as_str)
        })
        .unwrap_or("python")
        .to_owned()
}

fn resolve_target_index(
    cells: &[Value],
    cell_id: Option<&str>,
    mode: &str,
) -> Result<isize, String> {
    let Some(cell_id) = cell_id else {
        if mode == "insert" {
            return Ok(-1);
        }
        return Err("cell_id is required for replace and delete operations.".to_owned());
    };
    let matches = cells
        .iter()
        .enumerate()
        .filter(|(index, cell)| display_cell_id(cell, *index) == cell_id)
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    if matches.len() > 1 {
        return Err(format!(
            "Cell ID \"{cell_id}\" is ambiguous in the rendered notebook. Re-read the notebook and target a stable real cell ID before editing."
        ));
    }
    matches
        .first()
        .map(|index| *index as isize)
        .ok_or_else(|| format!("Cell with ID \"{cell_id}\" not found in notebook."))
}

fn has_stable_cell_ids(cells: &[Value]) -> bool {
    cells.iter().all(|cell| {
        cell.get("id")
            .and_then(Value::as_str)
            .is_some_and(|id| !id.is_empty())
    })
}

fn should_generate_cell_ids(notebook: &Value) -> bool {
    let major = notebook
        .get("nbformat")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let minor = notebook
        .get("nbformat_minor")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    major > 4 || (major == 4 && minor >= 5)
}

fn make_cell_id(cells: &[Value]) -> String {
    let existing = cells
        .iter()
        .enumerate()
        .map(|(index, cell)| display_cell_id(cell, index))
        .collect::<HashSet<_>>();
    let mut index = 1u64;
    loop {
        let candidate = format!("canopy-cell-{index}");
        if !existing.contains(&candidate) {
            return candidate;
        }
        index += 1;
    }
}

fn infer_inserted_source_array_style(cells: &[Value], index: usize) -> bool {
    index
        .checked_sub(1)
        .and_then(|previous| cells.get(previous))
        .and_then(|cell| cell.get("source"))
        .or_else(|| cells.get(index).and_then(|cell| cell.get("source")))
        .map(Value::is_array)
        .unwrap_or_else(|| {
            cells
                .iter()
                .find_map(|cell| cell.get("source"))
                .map(Value::is_array)
                .unwrap_or(true)
        })
}

fn to_notebook_source(source: &str, prefer_array: bool) -> Value {
    if !prefer_array {
        return Value::String(source.to_owned());
    }
    let mut lines = Vec::new();
    let mut start = 0;
    for (index, character) in source.char_indices() {
        if character == '\n' {
            let end = index + character.len_utf8();
            lines.push(Value::String(source[start..end].to_owned()));
            start = end;
        }
    }
    if start < source.len() {
        lines.push(Value::String(source[start..].to_owned()));
    }
    Value::Array(lines)
}

fn normalize_edited_cell(cell: &mut Map<String, Value>, final_type: &str) {
    cell.insert("cell_type".to_owned(), Value::String(final_type.to_owned()));
    if !cell.get("metadata").is_some_and(Value::is_object) {
        cell.insert("metadata".to_owned(), Value::Object(Map::new()));
    }
    if final_type == "code" {
        cell.insert("execution_count".to_owned(), Value::Null);
        cell.insert("outputs".to_owned(), Value::Array(Vec::new()));
    } else {
        cell.remove("execution_count");
        cell.remove("outputs");
    }
}

fn serialize_notebook(
    notebook: &Value,
    indent: Option<&str>,
    trailing_newline: bool,
) -> Result<String, String> {
    let mut bytes = Vec::new();
    if let Some(indent) = indent {
        let formatter = serde_json::ser::PrettyFormatter::with_indent(indent.as_bytes());
        let mut serializer = serde_json::Serializer::with_formatter(&mut bytes, formatter);
        notebook
            .serialize(&mut serializer)
            .map_err(|error| format!("could not serialize notebook: {error}"))?;
    } else {
        serde_json::to_writer(&mut bytes, notebook)
            .map_err(|error| format!("could not serialize notebook: {error}"))?;
    }
    let mut serialized = String::from_utf8(bytes)
        .map_err(|error| format!("notebook serialization returned invalid UTF-8: {error}"))?;
    if trailing_newline {
        serialized.push('\n');
    }
    Ok(serialized)
}

fn infer_indentation(raw: &str) -> Option<String> {
    raw.match_indices('\n').find_map(|(index, _)| {
        let rest = &raw[index + 1..];
        let indent = rest
            .chars()
            .take_while(|character| matches!(character, ' ' | '\t'))
            .collect::<String>();
        if !indent.is_empty() && rest[indent.len()..].starts_with('"') {
            Some(indent)
        } else {
            None
        }
    })
}

fn utf16_len(value: &str) -> usize {
    value.encode_utf16().count()
}

fn utf16_prefix(value: &str, max_units: usize) -> &str {
    let mut units = 0usize;
    for (byte_index, character) in value.char_indices() {
        let next_units = units.saturating_add(character.len_utf16());
        if next_units > max_units {
            return &value[..byte_index];
        }
        units = next_units;
    }
    value
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn params(mode: &str, cell_id: Option<&str>, source: Option<&str>) -> NotebookEditParams {
        NotebookEditParams {
            notebook_path: "/tmp/demo.ipynb".to_owned(),
            cell_id: cell_id.map(str::to_owned),
            new_source: source.map(str::to_owned),
            cell_type: None,
            edit_mode: Some(mode.to_owned()),
        }
    }

    #[test]
    fn renders_cells_with_ids_outputs_and_ansi_removed() {
        let raw = r#"{"nbformat":4,"metadata":{"language_info":{"name":"rust"}},"cells":[{"cell_type":"code","id":"stable","execution_count":3,"source":["println!(1);"],"outputs":[{"output_type":"stream","text":"\u001b[31mred\u001b[0m\n"},{"output_type":"display_data","data":{"image/png":"base64","text/html":"<b>x</b>"}}]}]}"#;
        let result = render_notebook(raw).unwrap();
        assert!(result.content.contains("Jupyter Notebook (rust, 1 cells)"));
        assert!(result.content.contains("Code Cell stable [3]"));
        assert!(result.content.contains("red\n"));
        assert!(
            result
                .content
                .contains("[non-text output: image/png, text/html]")
        );
        assert!(!result.content.contains('\u{1b}'));
    }

    #[test]
    fn replaces_cell_and_preserves_ids_and_json_format_while_clearing_outputs() {
        let raw = "{\n  \"nbformat\": 4,\n  \"nbformat_minor\": 5,\n  \"cells\": [{\n    \"cell_type\": \"code\",\n    \"id\": \"a\",\n    \"source\": [\"old\\n\"],\n    \"outputs\": [{\"output_type\":\"stream\",\"text\":\"x\"}],\n    \"execution_count\": 9,\n    \"metadata\": {}\n  }],\n  \"metadata\": {}\n}\n";
        let result =
            apply_notebook_edit(raw, &params("replace", Some("a"), Some("new\nline"))).unwrap();
        let notebook: Value = serde_json::from_str(&result.updated_content).unwrap();
        let cell = &notebook["cells"][0];
        assert_eq!(cell["id"], "a");
        assert_eq!(cell["source"], json!(["new\n", "line"]));
        assert_eq!(cell["execution_count"], Value::Null);
        assert_eq!(cell["outputs"], json!([]));
        assert!(result.updated_content.starts_with("{\n  \"nbformat\""));
        assert!(result.updated_content.ends_with('\n'));
    }

    #[test]
    fn inserts_with_stable_ids_and_deletes_with_fallback_refresh_requirement() {
        let raw = r#"{"nbformat":4,"nbformat_minor":5,"cells":[{"cell_type":"markdown","id":"a","source":["A"],"metadata":{}}],"metadata":{}}"#;
        let inserted = apply_notebook_edit(raw, &params("insert", Some("a"), Some("B"))).unwrap();
        let notebook: Value = serde_json::from_str(&inserted.updated_content).unwrap();
        assert_eq!(notebook["cells"][1]["id"], "canopy-cell-1");
        assert!(!inserted.requires_read_after_write);

        let raw_fallback = r#"{"nbformat":4,"nbformat_minor":5,"cells":[{"cell_type":"markdown","source":["A"],"metadata":{}},{"cell_type":"markdown","source":["B"],"metadata":{}}],"metadata":{}}"#;
        let deleted =
            apply_notebook_edit(raw_fallback, &params("delete", Some("cell-1"), None)).unwrap();
        assert!(deleted.requires_read_after_write);
        assert_eq!(
            serde_json::from_str::<Value>(&deleted.updated_content).unwrap()["cells"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn rejects_ambiguous_cell_ids_and_invalid_notebook_json() {
        let raw = r#"{"nbformat":4,"nbformat_minor":5,"cells":[{"cell_type":"markdown","id":"cell-1","source":"A"},{"cell_type":"markdown","source":"B"}],"metadata":{}}"#;
        assert!(
            apply_notebook_edit(raw, &params("replace", Some("cell-1"), Some("C")))
                .unwrap_err()
                .contains("ambiguous")
        );
        assert!(
            render_notebook("no json")
                .unwrap_err()
                .contains("Invalid notebook JSON")
        );
    }

    #[test]
    fn caps_notebook_cell_output_and_total_rendered_size() {
        let long = "x".repeat(MAX_NOTEBOOK_OUTPUT_CHARS + 100);
        let raw = serde_json::json!({
            "cells":[{"cell_type":"markdown","id":"a","source":long,"metadata":{}}],
            "metadata":{}
        })
        .to_string();
        let result = render_notebook(&raw).unwrap();
        assert!(result.is_truncated);
        assert!(result.content.chars().count() <= MAX_NOTEBOOK_OUTPUT_CHARS + 250);
    }

    #[test]
    fn applies_utf16_output_limits_without_splitting_astral_characters() {
        let output = "😀".repeat(6_000);
        let raw = serde_json::json!({
            "cells":[{"cell_type":"code","source":"","outputs":[{"output_type":"stream","text":output}]}],
            "metadata":{}
        })
        .to_string();
        let rendered = render_notebook(&raw).unwrap();
        let prefix = "😀".repeat(MAX_CELL_OUTPUT_CHARS / 2);
        assert!(rendered.content.contains(&prefix));
        assert!(
            rendered
                .content
                .contains("output truncated, total 12000 chars")
        );
        assert!(!rendered.content.contains('\u{fffd}'));

        let oversized_cell = "😀".repeat(MAX_NOTEBOOK_OUTPUT_CHARS / 2 + 1);
        let raw = serde_json::json!({
            "cells":[{"cell_type":"markdown","source":oversized_cell}],
            "metadata":{}
        })
        .to_string();
        assert!(render_notebook(&raw).unwrap().is_truncated);
    }
}
