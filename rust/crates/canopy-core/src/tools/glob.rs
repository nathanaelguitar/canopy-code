use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use globset::{GlobBuilder, escape};
use serde_json::{Value, json};
use walkdir::{DirEntry, WalkDir};

use crate::file_discovery::{FileDiscoveryService, FileFilteringOptions};

const MAX_FILE_COUNT: usize = 100;
const MAX_GLOB_COLLECTED_ENTRIES: usize = MAX_FILE_COUNT * 10;
const DEFAULT_TRUNCATE_TOOL_OUTPUT_LINES: usize = 1_000;
const RECENCY_WINDOW: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Clone, Debug)]
struct GlobEntry {
    path: PathBuf,
    modified: Option<SystemTime>,
}

pub struct GlobTool {
    workspace_root: PathBuf,
    file_discovery: FileDiscoveryService,
    truncate_tool_output_lines: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GlobResult {
    pub llm_content: String,
    pub return_display: String,
    pub result_file_paths: Vec<PathBuf>,
}

impl GlobTool {
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
        let configured_limit =
            truncate_tool_output_lines.unwrap_or(DEFAULT_TRUNCATE_TOOL_OUTPUT_LINES);
        Ok(Self {
            workspace_root,
            file_discovery,
            truncate_tool_output_lines: if configured_limit == 0 {
                usize::MAX
            } else {
                configured_limit
            },
        })
    }

    pub fn execute(&self, args: &Value) -> Result<GlobResult, String> {
        let pattern = args
            .get("pattern")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|pattern| !pattern.is_empty())
            .ok_or_else(|| "The 'pattern' parameter must be non-empty.".to_owned())?;
        let search_directory = self.search_directory(args)?;
        let location = if args.get("path").is_some() {
            format!("within {}", search_directory.display())
        } else {
            "in the workspace directory".to_owned()
        };

        let effective_pattern = if search_directory.join(pattern).exists() {
            escape(pattern)
        } else {
            pattern.to_owned()
        };
        let matcher = GlobBuilder::new(&effective_pattern)
            .case_insensitive(cfg!(target_os = "macos") || cfg!(windows))
            .literal_separator(true)
            .backslash_escape(true)
            .build()
            .map_err(|error| format!("invalid glob pattern {pattern:?}: {error}"))?
            .compile_matcher();

        let filtering_options = FileFilteringOptions::default();
        let mut entries = Vec::new();
        let mut hit_collection_limit = false;
        let walker = WalkDir::new(&search_directory)
            .follow_links(false)
            .into_iter()
            .filter_entry(|entry| {
                if entry.depth() == 0 {
                    return true;
                }
                !self.is_ignored_entry(entry, &filtering_options)
            });

        for entry in walker {
            let entry = entry.map_err(|error| format!("glob traversal failed: {error}"))?;
            if !entry.file_type().is_file() {
                continue;
            }
            let relative_path = entry
                .path()
                .strip_prefix(&search_directory)
                .map_err(|error| format!("could not relativize glob result: {error}"))?;
            let relative_for_match = relative_path.to_string_lossy().replace('\\', "/");
            if !matcher.is_match(&relative_for_match) {
                continue;
            }

            if entries.len() >= MAX_GLOB_COLLECTED_ENTRIES {
                hit_collection_limit = true;
                break;
            }
            let modified = entry
                .metadata()
                .ok()
                .and_then(|metadata| metadata.modified().ok());
            entries.push(GlobEntry {
                path: entry.path().to_path_buf(),
                modified,
            });
        }

        if entries.is_empty() {
            return Ok(GlobResult {
                llm_content: format!("No files found matching pattern \"{pattern}\" {location}"),
                return_display: "No files found".to_owned(),
                result_file_paths: Vec::new(),
            });
        }

        sort_file_entries(&mut entries, SystemTime::now());
        let total_file_count = entries.len();
        let file_limit = MAX_FILE_COUNT.min(self.truncate_tool_output_lines);
        let truncated = hit_collection_limit || total_file_count > file_limit;
        let entries_to_show = entries.iter().take(file_limit);
        let result_file_paths = entries_to_show
            .map(|entry| entry.path.clone())
            .collect::<Vec<_>>();
        let file_list = result_file_paths
            .iter()
            .map(|path| path.display().to_string())
            .collect::<Vec<_>>()
            .join("\n");

        let count_qualifier = if hit_collection_limit {
            "at least "
        } else {
            ""
        };
        let mut llm_content = format!(
            "Found {count_qualifier}{total_file_count} file(s) matching \"{pattern}\" {location}, sorted by modification time (newest first):\n---\n{file_list}"
        );
        if hit_collection_limit {
            llm_content.push_str(&format!(
                "\n---\n[Results truncated after scanning {total_file_count} matching files. Narrow the pattern or path.]"
            ));
        } else if truncated {
            let omitted = total_file_count - file_limit;
            let noun = if omitted == 1 { "file" } else { "files" };
            llm_content.push_str(&format!("\n---\n[{omitted} {noun} truncated] ..."));
        }

        let qualifier = if hit_collection_limit {
            "at least "
        } else {
            ""
        };
        let truncation_label = if truncated { " (truncated)" } else { "" };
        Ok(GlobResult {
            llm_content,
            return_display: format!(
                "Found {qualifier}{total_file_count} matching file(s){truncation_label}"
            ),
            result_file_paths,
        })
    }

    fn search_directory(&self, args: &Value) -> Result<PathBuf, String> {
        let requested_path = match args.get("path") {
            None | Some(Value::Null) => self.workspace_root.clone(),
            Some(Value::String(path)) if !path.trim().is_empty() => {
                let path = Path::new(path.trim());
                if path.is_absolute() {
                    path.to_path_buf()
                } else {
                    self.workspace_root.join(path)
                }
            }
            Some(_) => return Err("The 'path' parameter must be a path string.".to_owned()),
        };
        let canonical_path = std::fs::canonicalize(&requested_path)
            .map_err(|error| format!("could not resolve search path: {error}"))?;
        if !canonical_path.starts_with(&self.workspace_root) {
            return Err("glob search is restricted to directories inside the workspace".to_owned());
        }
        if !canonical_path.is_dir() {
            return Err(format!(
                "glob search path is not a directory: {}",
                canonical_path.display()
            ));
        }
        Ok(canonical_path)
    }

    fn is_ignored_entry(&self, entry: &DirEntry, filtering_options: &FileFilteringOptions) -> bool {
        let Ok(relative_path) = entry.path().strip_prefix(&self.workspace_root) else {
            return false;
        };
        let mut ignore_path = relative_path.to_string_lossy().replace('\\', "/");
        if entry.file_type().is_dir() {
            ignore_path.push('/');
        }
        self.file_discovery
            .should_ignore_file(&ignore_path, filtering_options)
            .unwrap_or(false)
    }
}

fn sort_file_entries(entries: &mut [GlobEntry], now: SystemTime) {
    entries.sort_by(|left, right| {
        let left_modified = left.modified.unwrap_or(SystemTime::UNIX_EPOCH);
        let right_modified = right.modified.unwrap_or(SystemTime::UNIX_EPOCH);
        let left_recent =
            now.duration_since(left_modified).unwrap_or(Duration::ZERO) < RECENCY_WINDOW;
        let right_recent =
            now.duration_since(right_modified).unwrap_or(Duration::ZERO) < RECENCY_WINDOW;
        match (left_recent, right_recent) {
            (true, true) => right_modified.cmp(&left_modified),
            (true, false) => std::cmp::Ordering::Less,
            (false, true) => std::cmp::Ordering::Greater,
            (false, false) => left.path.cmp(&right.path),
        }
    });
}

pub fn function_declaration() -> Value {
    json!({
        "name":"glob",
        "description":"Find files matching a glob pattern. Search is recursive, includes hidden files, and respects .gitignore and Canopy agent-ignore files.",
        "parameters":{
            "type":"OBJECT",
            "properties":{
                "pattern":{"type":"STRING","description":"Glob pattern to match files, such as **/*.rs."},
                "path":{"type":"STRING","description":"Optional directory to search. Defaults to the current workspace and must remain inside it."}
            },
            "required":["pattern"]
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use uuid::Uuid;

    struct TempWorkspace(PathBuf);

    impl TempWorkspace {
        fn new() -> Self {
            let root = std::env::temp_dir().join(format!("canopy-glob-{}", Uuid::new_v4()));
            std::fs::create_dir_all(&root).unwrap();
            Self(root)
        }
    }

    impl Drop for TempWorkspace {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn tool(workspace: &TempWorkspace, limit: Option<usize>) -> GlobTool {
        GlobTool::new(&workspace.0, limit).unwrap()
    }

    #[test]
    fn searches_recursively_including_hidden_files_and_respects_ignore_rules() {
        let workspace = TempWorkspace::new();
        std::fs::write(workspace.0.join(".git"), "worktree marker").unwrap();
        std::fs::write(workspace.0.join(".gitignore"), "target/\n*.log\n").unwrap();
        std::fs::write(workspace.0.join(".canopyignore"), "private.rs\n").unwrap();
        std::fs::create_dir_all(workspace.0.join("src/.hidden")).unwrap();
        std::fs::create_dir_all(workspace.0.join("target")).unwrap();
        std::fs::write(workspace.0.join("src/main.rs"), "main").unwrap();
        std::fs::write(workspace.0.join("src/.hidden/extra.rs"), "hidden").unwrap();
        std::fs::write(workspace.0.join("target/build.rs"), "ignored").unwrap();
        std::fs::write(workspace.0.join("private.rs"), "ignored").unwrap();
        std::fs::write(workspace.0.join("debug.log"), "ignored").unwrap();

        let result = tool(&workspace, None)
            .execute(&json!({"pattern":"**/*.rs"}))
            .unwrap();
        assert!(result.llm_content.contains("src/main.rs"));
        assert!(result.llm_content.contains("src/.hidden/extra.rs"));
        assert!(!result.llm_content.contains("target/build.rs"));
        assert!(!result.llm_content.contains("private.rs"));
        assert!(!result.llm_content.contains("debug.log"));
    }

    #[test]
    fn treats_an_existing_special_character_filename_as_literal() {
        let workspace = TempWorkspace::new();
        std::fs::write(workspace.0.join("literal[1].rs"), "literal").unwrap();

        let result = tool(&workspace, None)
            .execute(&json!({"pattern":"literal[1].rs"}))
            .unwrap();
        assert!(result.llm_content.contains("literal[1].rs"));
        assert_eq!(result.result_file_paths.len(), 1);
    }

    #[test]
    fn applies_output_limit_and_reports_truncation() {
        let workspace = TempWorkspace::new();
        for index in 0..5 {
            std::fs::write(workspace.0.join(format!("file-{index}.rs")), "file").unwrap();
        }

        let result = tool(&workspace, Some(2))
            .execute(&json!({"pattern":"*.rs"}))
            .unwrap();
        assert_eq!(result.result_file_paths.len(), 2);
        assert!(result.llm_content.contains("[3 files truncated]"));
        assert!(result.return_display.contains("(truncated)"));
    }

    #[test]
    fn rejects_paths_outside_the_workspace_and_invalid_patterns() {
        let workspace = TempWorkspace::new();
        let outside = TempWorkspace::new();
        assert!(
            tool(&workspace, None)
                .execute(&json!({
                    "pattern":"*",
                    "path":outside.0.to_string_lossy()
                }))
                .unwrap_err()
                .contains("inside the workspace")
        );
        assert!(
            tool(&workspace, None)
                .execute(&json!({"pattern":"["}))
                .unwrap_err()
                .contains("invalid glob pattern")
        );
    }
}
