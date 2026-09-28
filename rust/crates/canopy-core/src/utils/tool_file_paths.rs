//! Extract filesystem paths from core-tool arguments.
//!
//! Port of the path helpers in `packages/core/src/core/coreToolScheduler.ts`.
//! The allowlist is intentionally closed so unrelated tools cannot feed
//! path-like fields into path-conditional skill/rule activation.

use reqwest::Url;
use serde_json::Value;

use crate::tool_utils::canonical_tool_name;

/// Canonical tool names whose inputs contain project filesystem paths.
pub const FS_PATH_TOOL_NAMES: &[&str] = &[
    "read_file",
    "zoom_image",
    "edit",
    "write_file",
    "grep_search",
    "glob",
    "super_search",
    "list_directory",
    "lsp",
    "notebook_edit",
    "display_image",
];

/// Whether a tool resolves to a filesystem path-bearing core tool.
pub fn is_filesystem_path_tool(tool_name: &str) -> bool {
    let canonical = canonical_tool_name(tool_name);
    FS_PATH_TOOL_NAMES.contains(&canonical.as_str())
}

/// Extract filesystem path candidates from a core tool's JSON input.
///
/// Legacy tool aliases are canonicalized before checking the allowlist. The
/// function preserves source order and duplicates, and returns an empty vector
/// for non-filesystem tools or non-object inputs.
pub fn extract_tool_file_paths(tool_name: &str, tool_input: &Value) -> Vec<String> {
    let canonical = canonical_tool_name(tool_name);
    if !FS_PATH_TOOL_NAMES.contains(&canonical.as_str()) {
        return Vec::new();
    }
    let Some(object) = tool_input.as_object() else {
        return Vec::new();
    };
    let mut output = Vec::new();
    let push_string = |output: &mut Vec<String>, value: Option<&Value>| {
        if let Some(value) = value.and_then(Value::as_str) {
            if !value.is_empty() {
                output.push(value.to_owned());
            }
        }
    };

    match canonical.as_str() {
        "lsp" => {
            push_lsp_path_candidate(&mut output, object.get("filePath"));
            if let Some(call_hierarchy_item) = object.get("callHierarchyItem") {
                if let Some(item) = call_hierarchy_item.as_object() {
                    push_lsp_path_candidate(&mut output, item.get("uri"));
                }
            }
        }
        "glob" => {
            let path = object.get("path");
            push_string(&mut output, path);
            if let Some(pattern) = object.get("pattern").and_then(Value::as_str) {
                if !pattern.is_empty() {
                    let root = path.and_then(Value::as_str);
                    output.push(join_search_root_and_glob(root, pattern));
                }
            }
        }
        "grep_search" => {
            let path = object.get("path");
            push_string(&mut output, path);
            if let Some(glob) = object.get("glob").and_then(Value::as_str) {
                if !glob.is_empty() {
                    let root = path.and_then(Value::as_str);
                    output.push(join_search_root_and_glob(root, glob));
                }
            }
        }
        "super_search" | "list_directory" => {
            push_string(&mut output, object.get("path"));
        }
        "read_file" | "zoom_image" | "edit" | "write_file" | "display_image" => {
            push_string(&mut output, object.get("file_path"));
        }
        "notebook_edit" => {
            push_string(&mut output, object.get("notebook_path"));
        }
        // This fallback mirrors the source switch's file_path case. Every
        // current allowlisted name has a branch above, but keeping it avoids
        // changing behavior when a path-bearing core tool is added here.
        _ => push_string(&mut output, object.get("file_path")),
    }

    output
}

/// Join a search root and path-shaped glob without normalizing `..` segments
/// or introducing platform-specific separators.
pub fn join_search_root_and_glob(search_root: Option<&str>, glob: &str) -> String {
    let Some(search_root) = search_root.filter(|root| !root.is_empty()) else {
        return glob.to_owned();
    };
    format!("{}/{glob}", trim_trailing_slashes(search_root))
}

fn trim_trailing_slashes(path: &str) -> &str {
    let mut end = path.len();
    while end > 0 {
        let character = path[..end].chars().next_back().expect("non-empty suffix");
        if character == '/' || character == '\\' {
            end -= character.len_utf8();
        } else {
            break;
        }
    }
    &path[..end]
}

fn push_lsp_path_candidate(output: &mut Vec<String>, value: Option<&Value>) {
    let Some(value) = value
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
    else {
        return;
    };
    if value.starts_with("file://") {
        let Ok(url) = Url::parse(value) else {
            return;
        };
        if url.scheme() != "file" {
            return;
        }
        if let Ok(path) = url.to_file_path() {
            output.push(path.to_string_lossy().into_owned());
        }
        return;
    }
    if value.contains("://") {
        return;
    }
    output.push(value.to_owned());
}
