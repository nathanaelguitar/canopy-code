use std::path::{Path, PathBuf};

use regex::Regex;
use serde_json::{Value, json};

use crate::file_discovery::{FileDiscoveryService, FileFilteringOptions};

const MAX_ENTRY_COUNT: usize = 100;
const DEFAULT_TRUNCATE_TOOL_OUTPUT_LINES: usize = 1_000;
const SHELL_SPECIAL_CHARS: &str = " \t()[]{};|*?$`'\"#&<>!~,";

#[derive(Clone, Debug)]
struct FileEntry {
    name: String,
    is_directory: bool,
}

pub struct ListDirectoryTool {
    workspace_root: PathBuf,
    file_discovery: FileDiscoveryService,
    truncate_tool_output_lines: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListDirectoryError {
    pub message: String,
    pub display_message: String,
    pub error_type: &'static str,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListDirectoryResult {
    pub llm_content: String,
    pub return_display: String,
    pub error: Option<ListDirectoryError>,
}

impl ListDirectoryTool {
    pub fn new(
        workspace_root: impl AsRef<Path>,
        truncate_tool_output_lines: Option<usize>,
    ) -> Result<Self, String> {
        let workspace_root = std::fs::canonicalize(workspace_root.as_ref())
            .map_err(|error| format!("could not resolve workspace root: {error}"))?;
        if !workspace_root.is_dir() {
            return Err("workspace root is not a directory".to_owned());
        }
        let file_discovery = FileDiscoveryService::new(&workspace_root, None)?;
        Ok(Self {
            workspace_root,
            file_discovery,
            truncate_tool_output_lines: truncate_tool_output_lines
                .unwrap_or(DEFAULT_TRUNCATE_TOOL_OUTPUT_LINES),
        })
    }

    pub fn execute(&self, args: &Value) -> Result<ListDirectoryResult, String> {
        let requested_path = args
            .get("path")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|path| !path.is_empty())
            .ok_or_else(|| "The 'path' parameter must be non-empty.".to_owned())?;
        let requested_path = unescape_path(requested_path);
        let requested_path = Path::new(&requested_path);
        if !requested_path.is_absolute() {
            return Err(format!(
                "Path must be absolute: {}",
                requested_path.display()
            ));
        }

        let canonical_directory = match std::fs::canonicalize(requested_path) {
            Ok(path) => path,
            Err(error) => {
                return Ok(tool_error(
                    format!("Error listing directory: {error}"),
                    "Error: Failed to list directory.",
                    "ls_execution_error",
                ));
            }
        };
        if !canonical_directory.starts_with(&self.workspace_root) {
            return Ok(tool_error(
                "Directory is outside the workspace boundary.".to_owned(),
                "Error: Directory is outside the workspace.",
                "path_not_in_workspace",
            ));
        }
        let metadata = match std::fs::metadata(requested_path) {
            Ok(metadata) => metadata,
            Err(error) => {
                return Ok(tool_error(
                    format!("Error listing directory: {error}"),
                    "Error: Failed to list directory.",
                    "ls_execution_error",
                ));
            }
        };
        if !metadata.is_dir() {
            return Ok(tool_error(
                format!(
                    "Error: Path is not a directory: {}",
                    requested_path.display()
                ),
                "Error: Path is not a directory.",
                "path_is_not_a_directory",
            ));
        }

        let directory = match std::fs::read_dir(&canonical_directory) {
            Ok(directory) => directory,
            Err(error) => {
                return Ok(tool_error(
                    format!("Error listing directory: {error}"),
                    "Error: Failed to list directory.",
                    "ls_execution_error",
                ));
            }
        };
        let mut raw_entries = Vec::new();
        for entry in directory {
            let entry = match entry {
                Ok(entry) => entry,
                Err(error) => {
                    return Ok(tool_error(
                        format!("Error listing directory: {error}"),
                        "Error: Failed to list directory.",
                        "ls_execution_error",
                    ));
                }
            };
            let name = entry.file_name().to_string_lossy().into_owned();
            let entry_path = canonical_directory.join(&name);
            raw_entries.push((name, entry_path));
        }

        if raw_entries.is_empty() {
            return Ok(ListDirectoryResult {
                llm_content: format!("Directory {} is empty.", requested_path.display()),
                return_display: "Directory is empty.".to_owned(),
                error: None,
            });
        }

        let relative_paths = raw_entries
            .iter()
            .map(|(_, path)| self.workspace_relative_path(path))
            .collect::<Vec<_>>();
        let file_filtering_options = filtering_options(args)?;
        let report = self
            .file_discovery
            .filter_files_with_report(&relative_paths, &file_filtering_options)?;
        let filtered_paths = report
            .filtered_paths
            .into_iter()
            .collect::<std::collections::HashSet<_>>();
        let ignore_patterns = ignore_patterns(args)?;

        let mut entries = Vec::new();
        for (name, path) in raw_entries {
            let relative_path = self.workspace_relative_path(&path);
            if !filtered_paths.contains(&relative_path)
                || matches_any_ignore_pattern(&name, &ignore_patterns)?
            {
                continue;
            }
            match std::fs::metadata(&path) {
                Ok(metadata) => entries.push(FileEntry {
                    name,
                    is_directory: metadata.is_dir(),
                }),
                Err(_) => {
                    // Upstream skips entries whose individual stat fails; the
                    // rest of the directory remains useful to the model.
                }
            }
        }

        entries.sort_by(|left, right| {
            right
                .is_directory
                .cmp(&left.is_directory)
                .then_with(|| left.name.cmp(&right.name))
        });

        let total_entry_count = entries.len();
        let configured_limit = if self.truncate_tool_output_lines == 0 {
            usize::MAX
        } else {
            self.truncate_tool_output_lines
        };
        let entry_limit = MAX_ENTRY_COUNT.min(configured_limit);
        let truncated = total_entry_count > entry_limit;
        let visible_entries = entries.iter().take(entry_limit).map(|entry| {
            if entry.is_directory {
                format!("[DIR] {}", entry.name)
            } else {
                entry.name.clone()
            }
        });

        let mut result = format!(
            "Listed {total_entry_count} item(s) in {}:\n---\n{}",
            requested_path.display(),
            visible_entries.collect::<Vec<_>>().join("\n")
        );
        if truncated {
            let omitted = total_entry_count - entry_limit;
            let noun = if omitted == 1 { "item" } else { "items" };
            result.push_str(&format!("\n---\n[{omitted} {noun} truncated] ..."));
        }

        let mut ignored_messages = Vec::new();
        if report.git_ignored_count > 0 {
            ignored_messages.push(format!("{} git-ignored", report.git_ignored_count));
        }
        if report.canopy_ignored_count > 0 {
            ignored_messages.push(format!("{} canopy-ignored", report.canopy_ignored_count));
        }
        if !ignored_messages.is_empty() {
            result.push_str(&format!("\n\n({})", ignored_messages.join(", ")));
        }

        let mut display = format!("Listed {total_entry_count} item(s)");
        if !ignored_messages.is_empty() {
            display.push_str(&format!(" ({})", ignored_messages.join(", ")));
        }
        if truncated {
            display.push_str(" (truncated)");
        }
        Ok(ListDirectoryResult {
            llm_content: result,
            return_display: display,
            error: None,
        })
    }

    fn workspace_relative_path(&self, path: &Path) -> String {
        let relative = path.strip_prefix(&self.workspace_root).unwrap_or(path);
        relative
            .components()
            .map(|component| component.as_os_str().to_string_lossy())
            .collect::<Vec<_>>()
            .join("/")
    }
}

pub fn function_declaration() -> Value {
    json!({
        "name":"list_directory",
        "description":"Lists the names of files and subdirectories directly within a specified directory path. Can optionally ignore entries matching provided glob patterns.",
        "parameters":{
            "type":"OBJECT",
            "properties":{
                "path":{
                    "description":"The absolute path to the directory to list (must be absolute, not relative)",
                    "type":"STRING"
                },
                "ignore":{
                    "description":"List of glob patterns to ignore",
                    "items":{"type":"STRING"},
                    "type":"ARRAY"
                },
                "file_filtering_options":{
                    "description":"Optional: Whether to respect ignore patterns from .gitignore, .canopyignore, and configured custom Canopy ignore files",
                    "type":"OBJECT",
                    "properties":{
                        "respect_git_ignore":{
                            "description":"Optional: Whether to respect .gitignore patterns when listing files. Only available in git repositories. Defaults to true.",
                            "type":"BOOLEAN"
                        },
                        "respect_canopy_ignore":{
                            "description":"Optional: Whether to respect .canopyignore and configured custom Canopy ignore file patterns when listing files. Defaults to true.",
                            "type":"BOOLEAN"
                        }
                    }
                }
            },
            "required":["path"]
        }
    })
}

fn tool_error(
    message: String,
    display_message: &str,
    error_type: &'static str,
) -> ListDirectoryResult {
    ListDirectoryResult {
        llm_content: message.clone(),
        return_display: display_message.to_owned(),
        error: Some(ListDirectoryError {
            message,
            display_message: display_message.to_owned(),
            error_type,
        }),
    }
}

fn filtering_options(args: &Value) -> Result<FileFilteringOptions, String> {
    let mut options = FileFilteringOptions::default();
    let Some(value) = args.get("file_filtering_options") else {
        return Ok(options);
    };
    let object = value
        .as_object()
        .ok_or_else(|| "file_filtering_options must be an object".to_owned())?;
    if let Some(value) = object.get("respect_git_ignore") {
        options.respect_git_ignore = value
            .as_bool()
            .ok_or_else(|| "respect_git_ignore must be a boolean".to_owned())?;
    }
    if let Some(value) = object.get("respect_canopy_ignore") {
        options.respect_canopy_ignore = value
            .as_bool()
            .ok_or_else(|| "respect_canopy_ignore must be a boolean".to_owned())?;
    }
    Ok(options)
}

fn ignore_patterns(args: &Value) -> Result<Vec<String>, String> {
    let Some(value) = args.get("ignore") else {
        return Ok(Vec::new());
    };
    let values = value
        .as_array()
        .ok_or_else(|| "ignore must be an array of strings".to_owned())?;
    values
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| "ignore must be an array of strings".to_owned())
        })
        .collect()
}

fn matches_any_ignore_pattern(name: &str, patterns: &[String]) -> Result<bool, String> {
    for pattern in patterns {
        let expression = wildcard_pattern_to_regex(pattern);
        let regex = Regex::new(&expression)
            .map_err(|error| format!("could not compile ignore pattern {pattern:?}: {error}"))?;
        if regex.is_match(name) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn wildcard_pattern_to_regex(pattern: &str) -> String {
    let mut expression = String::from("^");
    for character in pattern.chars() {
        match character {
            '*' => expression.push_str(".*"),
            '?' => expression.push('.'),
            '\\' | '.' | '+' | '^' | '$' | '{' | '}' | '(' | ')' | '[' | ']' | '|' => {
                expression.push('\\');
                expression.push(character);
            }
            _ => expression.push(character),
        }
    }
    expression.push('$');
    expression
}

fn unescape_path(path: &str) -> String {
    #[cfg(windows)]
    {
        path.to_owned()
    }
    #[cfg(not(windows))]
    {
        let mut result = String::with_capacity(path.len());
        let mut characters = path.chars().peekable();
        while let Some(character) = characters.next() {
            if character == '\\'
                && characters
                    .peek()
                    .is_some_and(|next| SHELL_SPECIAL_CHARS.contains(*next))
            {
                if let Some(escaped) = characters.next() {
                    result.push(escaped);
                }
            } else {
                result.push(character);
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use uuid::Uuid;

    struct TempWorkspace(PathBuf);

    impl TempWorkspace {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!("canopy-ls-{}", Uuid::new_v4()));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TempWorkspace {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn tool(workspace: &TempWorkspace, limit: Option<usize>) -> ListDirectoryTool {
        ListDirectoryTool::new(&workspace.0, limit).unwrap()
    }

    #[test]
    fn lists_directories_before_files_and_sorts_each_group() {
        let workspace = TempWorkspace::new();
        std::fs::write(workspace.0.join("b.txt"), "b").unwrap();
        std::fs::write(workspace.0.join("a.txt"), "a").unwrap();
        std::fs::create_dir(workspace.0.join("z-dir")).unwrap();
        std::fs::create_dir(workspace.0.join("a-dir")).unwrap();

        let result = tool(&workspace, None)
            .execute(&json!({"path":workspace.0}))
            .unwrap();

        assert!(result.llm_content.contains("Listed 4 item(s)"));
        assert!(
            result.llm_content.find("[DIR] a-dir").unwrap()
                < result.llm_content.find("[DIR] z-dir").unwrap()
        );
        assert!(
            result.llm_content.find("[DIR] z-dir").unwrap()
                < result.llm_content.find("a.txt").unwrap()
        );
        assert!(
            result.llm_content.find("a.txt").unwrap() < result.llm_content.find("b.txt").unwrap()
        );
    }

    #[test]
    fn filters_git_canopy_and_custom_glob_patterns_and_reports_counts() {
        let workspace = TempWorkspace::new();
        std::fs::write(workspace.0.join(".git"), "worktree marker").unwrap();
        std::fs::write(workspace.0.join(".gitignore"), "*.log\n").unwrap();
        std::fs::write(workspace.0.join(".canopyignore"), "private.txt\n").unwrap();
        std::fs::write(workspace.0.join("keep.txt"), "keep").unwrap();
        std::fs::write(workspace.0.join("trace.log"), "trace").unwrap();
        std::fs::write(workspace.0.join("private.txt"), "private").unwrap();
        std::fs::write(workspace.0.join("notes.md"), "notes").unwrap();

        let result = tool(&workspace, None)
            .execute(&json!({"path":workspace.0,"ignore":["*.md"]}))
            .unwrap();

        assert!(result.llm_content.contains("keep.txt"));
        assert!(!result.llm_content.contains("trace.log"));
        assert!(!result.llm_content.contains("private.txt"));
        assert!(!result.llm_content.contains("notes.md"));
        assert!(
            result
                .llm_content
                .contains("(2 git-ignored, 1 canopy-ignored)")
        );
    }

    #[test]
    fn preserves_the_distinction_between_empty_and_all_filtered_directories() {
        let workspace = TempWorkspace::new();
        let empty = workspace.0.join("empty");
        std::fs::create_dir(&empty).unwrap();
        assert_eq!(
            tool(&workspace, None)
                .execute(&json!({"path":empty}))
                .unwrap()
                .llm_content,
            format!("Directory {} is empty.", empty.display())
        );

        std::fs::write(workspace.0.join(".canopyignore"), "hidden.txt\n").unwrap();
        std::fs::write(empty.join("hidden.txt"), "hidden").unwrap();
        let result = tool(&workspace, None)
            .execute(&json!({"path":empty}))
            .unwrap();
        assert!(
            result
                .llm_content
                .starts_with(&format!("Listed 0 item(s) in {}", empty.display()))
        );
        assert!(result.llm_content.contains("1 canopy-ignored"));
    }

    #[test]
    fn enforces_workspace_boundary_after_resolving_symlinks() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;
            let workspace = TempWorkspace::new();
            let outside =
                std::env::temp_dir().join(format!("canopy-ls-outside-{}", Uuid::new_v4()));
            std::fs::create_dir(&outside).unwrap();
            let link = workspace.0.join("outside");
            symlink(&outside, &link).unwrap();

            let error = tool(&workspace, None)
                .execute(&json!({"path":link}))
                .unwrap()
                .error
                .unwrap()
                .message;
            let _ = std::fs::remove_dir_all(outside);
            assert!(error.contains("outside the workspace"));
        }
    }

    #[test]
    fn respects_configured_output_limit_and_hard_entry_cap() {
        let workspace = TempWorkspace::new();
        for index in 0..105 {
            std::fs::write(workspace.0.join(format!("f{index:03}.txt")), "x").unwrap();
        }

        let limited = tool(&workspace, Some(3))
            .execute(&json!({"path":workspace.0}))
            .unwrap();
        assert!(limited.llm_content.contains("[102 items truncated]"));

        let capped = tool(&workspace, None)
            .execute(&json!({"path":workspace.0}))
            .unwrap();
        assert!(capped.llm_content.contains("[5 items truncated]"));
    }

    #[test]
    fn rejects_relative_paths_and_non_directory_targets() {
        let workspace = TempWorkspace::new();
        assert!(
            tool(&workspace, None)
                .execute(&json!({"path":"relative"}))
                .unwrap_err()
                .contains("Path must be absolute")
        );
        let file = workspace.0.join("file.txt");
        std::fs::write(&file, "x").unwrap();
        assert!(
            tool(&workspace, None)
                .execute(&json!({"path":file}))
                .unwrap()
                .error
                .unwrap()
                .message
                .contains("Path is not a directory")
        );
    }
}
