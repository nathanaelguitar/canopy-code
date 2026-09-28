use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader};
use std::path::{Path, PathBuf};

use globset::{GlobBuilder, GlobMatcher};
use regex::{Regex, RegexBuilder};
use serde_json::{Value, json};
use walkdir::{DirEntry, WalkDir};

use crate::file_discovery::{FileDiscoveryService, FileFilteringOptions};
use crate::file_read_cache::FileReadCache;

const DEFAULT_TRUNCATE_TOOL_OUTPUT_LINES: usize = 1_000;
const DEFAULT_TRUNCATE_TOOL_OUTPUT_CHARS: usize = 25_000;
const MAX_LINE_BYTES: usize = 2 * 1024 * 1024;
const MAX_STORED_LINE_BYTES: usize = 24 * 1024;
const MAX_REGEX_BYTES: usize = 2 * 1024 * 1024;
const MAX_RETAINED_MATCHES: usize = 1_000;
const NON_CACHEABLE_GREP_EXTENSIONS: [&str; 1] = ["ipynb"];
const COMMON_IGNORED_DIRECTORIES: [&str; 5] =
    [".git", "node_modules", "bower_components", ".svn", ".hg"];

#[derive(Clone, Debug)]
struct GrepMatch {
    file_path: String,
    absolute_file_path: PathBuf,
    line_number: usize,
    line: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GrepResult {
    pub llm_content: String,
    pub return_display: String,
    pub result_file_paths: Vec<PathBuf>,
}

pub struct GrepTool {
    workspace_root: PathBuf,
    file_discovery: FileDiscoveryService,
    file_read_cache: FileReadCache,
    truncate_tool_output_lines: usize,
    truncate_tool_output_chars: usize,
}

impl GrepTool {
    pub fn new(
        workspace_root: impl AsRef<Path>,
        truncate_tool_output_lines: Option<usize>,
        truncate_tool_output_chars: Option<usize>,
    ) -> Result<Self, String> {
        Self::new_with_cache(
            workspace_root,
            truncate_tool_output_lines,
            truncate_tool_output_chars,
            FileReadCache::default(),
        )
    }

    pub fn new_with_cache(
        workspace_root: impl AsRef<Path>,
        truncate_tool_output_lines: Option<usize>,
        truncate_tool_output_chars: Option<usize>,
        file_read_cache: FileReadCache,
    ) -> Result<Self, String> {
        let workspace_root = std::fs::canonicalize(workspace_root.as_ref())
            .map_err(|error| format!("could not resolve workspace root: {error}"))?;
        if !workspace_root.is_dir() {
            return Err("workspace root is not a directory".to_owned());
        }
        let file_discovery = FileDiscoveryService::new(&workspace_root, None)?;
        let configured_lines =
            truncate_tool_output_lines.unwrap_or(DEFAULT_TRUNCATE_TOOL_OUTPUT_LINES);
        let configured_chars =
            truncate_tool_output_chars.unwrap_or(DEFAULT_TRUNCATE_TOOL_OUTPUT_CHARS);
        Ok(Self {
            workspace_root,
            file_discovery,
            file_read_cache,
            truncate_tool_output_lines: if configured_lines == 0 {
                usize::MAX
            } else {
                configured_lines
            },
            truncate_tool_output_chars: if configured_chars == 0 {
                DEFAULT_TRUNCATE_TOOL_OUTPUT_CHARS
            } else {
                configured_chars
            },
        })
    }

    pub fn execute(&self, args: &Value) -> Result<GrepResult, String> {
        let pattern = args
            .get("pattern")
            .and_then(Value::as_str)
            .ok_or_else(|| "The 'pattern' parameter must be a string.".to_owned())?;
        let regex = RegexBuilder::new(pattern)
            .case_insensitive(true)
            .size_limit(MAX_REGEX_BYTES)
            .build()
            .map_err(|error| format!("Invalid regular expression pattern: {error}"))?;
        let search_path = self.search_path(args)?;
        let search_location = if args.get("path").and_then(Value::as_str).is_some() {
            format!("in path \"{}\"", args["path"].as_str().unwrap_or_default())
        } else {
            "in the workspace directory".to_owned()
        };
        let file_glob = optional_glob_matcher(args)?;
        let parameter_limit = optional_limit(args)?;
        let line_limit = MAX_RETAINED_MATCHES.min(
            self.truncate_tool_output_lines
                .min(parameter_limit.unwrap_or(usize::MAX)),
        );
        let filtering_options = FileFilteringOptions::default();
        let mut matches = Vec::with_capacity(line_limit.min(128));
        let mut total_matches = 0usize;
        let mut skipped_oversized_lines = 0usize;

        if search_path.is_file() {
            if self.is_file_allowed(&search_path, &filtering_options)
                && file_matches_glob(&search_path, &search_path, file_glob.as_ref())
            {
                let (count, skipped) =
                    self.scan_file(&search_path, &search_path, &regex, line_limit, &mut matches);
                total_matches = total_matches.saturating_add(count);
                skipped_oversized_lines = skipped_oversized_lines.saturating_add(skipped);
            }
        } else {
            let walker = WalkDir::new(&search_path)
                .follow_links(false)
                .into_iter()
                .filter_entry(|entry| {
                    if entry.depth() == 0 {
                        return true;
                    }
                    !self.is_ignored_entry(entry, &filtering_options)
                });

            for entry in walker {
                let entry = entry.map_err(|error| format!("grep traversal failed: {error}"))?;
                if !entry.file_type().is_file() {
                    continue;
                }
                let path = entry.path();
                if !self.is_file_allowed(path, &filtering_options)
                    || !file_matches_glob(&search_path, path, file_glob.as_ref())
                {
                    continue;
                }
                let (count, skipped) =
                    self.scan_file(path, &search_path, &regex, line_limit, &mut matches);
                total_matches = total_matches.saturating_add(count);
                skipped_oversized_lines = skipped_oversized_lines.saturating_add(skipped);
            }
        }

        if total_matches == 0 {
            let filter_description = args
                .get("glob")
                .and_then(Value::as_str)
                .map(|glob| format!(" (filter: \"{glob}\")"))
                .unwrap_or_default();
            let mut message = format!(
                "No matches found for pattern \"{pattern}\" {search_location}{filter_description}."
            );
            append_skipped_line_notice(&mut message, skipped_oversized_lines);
            return Ok(GrepResult {
                llm_content: message,
                return_display: "No matches found".to_owned(),
                result_file_paths: Vec::new(),
            });
        }

        let truncated_by_line_limit = total_matches > matches.len();
        let match_term = if total_matches == 1 {
            "match"
        } else {
            "matches"
        };
        let filter_description = args
            .get("glob")
            .and_then(Value::as_str)
            .map(|glob| format!(" (filter: \"{glob}\")"))
            .unwrap_or_default();
        let header = format!(
            "Found {total_matches} {match_term} for pattern \"{pattern}\" {search_location}{filter_description}:\n---\n"
        );
        let (grep_output, visible_matches, truncated_by_char_limit) =
            render_matches(&matches, self.truncate_tool_output_chars);
        let mut llm_content = format!("{header}{}", grep_output.trim());
        let truncated = truncated_by_line_limit || truncated_by_char_limit;
        if truncated {
            let omitted = total_matches.saturating_sub(visible_matches.len());
            let noun = if omitted == 1 { "line" } else { "lines" };
            llm_content.push_str(&format!(" [{omitted} {noun} truncated] ..."));
        }
        append_skipped_line_notice(&mut llm_content, skipped_oversized_lines);

        let mut result_file_paths = Vec::new();
        for matched in &visible_matches {
            if !result_file_paths.contains(&matched.absolute_file_path) {
                result_file_paths.push(matched.absolute_file_path.clone());
            }
        }
        for path in &result_file_paths {
            let Ok(metadata) = std::fs::metadata(path) else {
                continue;
            };
            if !metadata.is_file() {
                continue;
            }
            let extension = path
                .extension()
                .map(|extension| extension.to_string_lossy().to_ascii_lowercase());
            let cacheable = extension
                .as_deref()
                .is_none_or(|extension| !NON_CACHEABLE_GREP_EXTENSIONS.contains(&extension));
            self.file_read_cache
                .record_read(path, &metadata, false, cacheable);
        }
        let truncation_label = if truncated { " (truncated)" } else { "" };
        Ok(GrepResult {
            llm_content,
            return_display: format!("Found {total_matches} {match_term}{truncation_label}"),
            result_file_paths,
        })
    }

    fn search_path(&self, args: &Value) -> Result<PathBuf, String> {
        let requested = match args.get("path") {
            None | Some(Value::Null) => self.workspace_root.clone(),
            Some(Value::String(value)) if !value.trim().is_empty() => {
                let path = Path::new(value.trim());
                if path.is_absolute() {
                    path.to_path_buf()
                } else {
                    self.workspace_root.join(path)
                }
            }
            Some(_) => return Err("The 'path' parameter must be a path string.".to_owned()),
        };
        let canonical = std::fs::canonicalize(&requested)
            .map_err(|error| format!("Path does not exist or cannot be accessed: {error}"))?;
        if !canonical.starts_with(&self.workspace_root) {
            return Err("grep search is restricted to files inside the workspace".to_owned());
        }
        if !canonical.is_dir() && !canonical.is_file() {
            return Err(format!(
                "Path is not a file or directory: {}",
                canonical.display()
            ));
        }
        Ok(canonical)
    }

    fn is_ignored_entry(&self, entry: &DirEntry, filtering_options: &FileFilteringOptions) -> bool {
        let path = entry.path();
        if entry.file_type().is_dir()
            && path.file_name().is_some_and(|name| {
                COMMON_IGNORED_DIRECTORIES.contains(&name.to_string_lossy().as_ref())
            })
        {
            return true;
        }
        !self.is_file_allowed_with_options(path, filtering_options)
    }

    fn is_file_allowed(&self, path: &Path, options: &FileFilteringOptions) -> bool {
        self.is_file_allowed_with_options(path, options)
    }

    fn is_file_allowed_with_options(&self, path: &Path, options: &FileFilteringOptions) -> bool {
        let Ok(relative) = path.strip_prefix(&self.workspace_root) else {
            return false;
        };
        if relative.components().any(|component| {
            COMMON_IGNORED_DIRECTORIES.contains(&component.as_os_str().to_string_lossy().as_ref())
        }) {
            return false;
        }
        let mut ignore_path = relative.to_string_lossy().replace('\\', "/");
        if path.is_dir() {
            ignore_path.push('/');
        }
        self.file_discovery
            .should_ignore_file(&ignore_path, options)
            .map(|ignored| !ignored)
            .unwrap_or(true)
    }

    fn scan_file(
        &self,
        path: &Path,
        search_root: &Path,
        regex: &Regex,
        retain_limit: usize,
        matches: &mut Vec<GrepMatch>,
    ) -> (usize, usize) {
        let file = match open_regular_file(path) {
            Ok(file) => file,
            Err(_) => return (0, 0),
        };
        if !file
            .metadata()
            .map(|metadata| metadata.is_file())
            .unwrap_or(false)
        {
            return (0, 0);
        }
        let mut reader = BufReader::new(file);
        let mut line_number = 0usize;
        let mut match_count = 0usize;
        let mut skipped_oversized = 0usize;
        while let Ok(Some(line)) = read_bounded_line(&mut reader) {
            line_number = line_number.saturating_add(1);
            if line.too_long {
                skipped_oversized = skipped_oversized.saturating_add(1);
                continue;
            }
            let text = String::from_utf8_lossy(&line.bytes);
            if !regex.is_match(&text) {
                continue;
            }
            match_count = match_count.saturating_add(1);
            if matches.len() >= retain_limit {
                continue;
            }
            let relative = if path == search_root {
                path.file_name().map(PathBuf::from).unwrap_or_default()
            } else {
                path.strip_prefix(search_root)
                    .map(Path::to_path_buf)
                    .unwrap_or_else(|_| path.to_path_buf())
            };
            let line = truncate_utf8(&text, MAX_STORED_LINE_BYTES);
            matches.push(GrepMatch {
                file_path: relative.to_string_lossy().replace('\\', "/"),
                absolute_file_path: path.to_path_buf(),
                line_number,
                line,
            });
        }
        (match_count, skipped_oversized)
    }
}

fn optional_glob_matcher(args: &Value) -> Result<Option<GlobMatcher>, String> {
    let Some(value) = args.get("glob") else {
        return Ok(None);
    };
    let pattern = value
        .as_str()
        .ok_or_else(|| "glob must be a string".to_owned())?;
    if pattern.is_empty() {
        return Ok(None);
    }
    let matcher = GlobBuilder::new(pattern)
        .literal_separator(true)
        .case_insensitive(cfg!(target_os = "macos") || cfg!(windows))
        .backslash_escape(true)
        .build()
        .map_err(|error| format!("invalid file glob {pattern:?}: {error}"))?
        .compile_matcher();
    Ok(Some(matcher))
}

fn optional_limit(args: &Value) -> Result<Option<usize>, String> {
    match args.get("limit") {
        None => Ok(None),
        Some(value) => {
            let limit = value
                .as_u64()
                .and_then(|value| usize::try_from(value).ok())
                .filter(|limit| *limit > 0)
                .ok_or_else(|| "limit must be a positive integer".to_owned())?;
            Ok(Some(limit))
        }
    }
}

fn file_matches_glob(search_root: &Path, path: &Path, matcher: Option<&GlobMatcher>) -> bool {
    let Some(matcher) = matcher else {
        return true;
    };
    let relative = path
        .strip_prefix(search_root)
        .ok()
        .unwrap_or_else(|| path.file_name().map(Path::new).unwrap_or(path));
    matcher.is_match(relative.to_string_lossy().replace('\\', "/"))
}

fn render_matches(matches: &[GrepMatch], char_limit: usize) -> (String, Vec<GrepMatch>, bool) {
    let mut groups = Vec::<(String, Vec<GrepMatch>)>::new();
    let mut group_indices = HashMap::<String, usize>::new();
    for matched in matches {
        let index = if let Some(index) = group_indices.get(&matched.file_path) {
            *index
        } else {
            let index = groups.len();
            group_indices.insert(matched.file_path.clone(), index);
            groups.push((matched.file_path.clone(), Vec::new()));
            index
        };
        groups[index].1.push(matched.clone());
    }

    let mut output = String::new();
    let mut visible = Vec::new();
    let mut truncated = false;
    'groups: for (path, file_matches) in groups {
        if !append_limited(
            &mut output,
            &format!("File: {path}\n"),
            char_limit,
            None,
            &mut visible,
        ) {
            truncated = true;
            break;
        }
        for matched in file_matches {
            let line = matched.line.trim();
            let chunk = format!("L{}: {line}\n", matched.line_number);
            if !append_limited(
                &mut output,
                &chunk,
                char_limit,
                Some(matched.clone()),
                &mut visible,
            ) {
                truncated = true;
                break 'groups;
            }
        }
        if !append_limited(&mut output, "---\n", char_limit, None, &mut visible) {
            truncated = true;
            break;
        }
    }
    (output, visible, truncated)
}

fn append_limited(
    output: &mut String,
    chunk: &str,
    char_limit: usize,
    matched: Option<GrepMatch>,
    visible: &mut Vec<GrepMatch>,
) -> bool {
    let current_units = output.encode_utf16().count();
    let chunk_units = chunk.encode_utf16().count();
    if current_units.saturating_add(chunk_units) > char_limit {
        let remaining = char_limit.saturating_sub(current_units);
        output.push_str(utf16_prefix(chunk, remaining));
        output.push_str("...");
        if let Some(matched) = matched {
            visible.push(matched);
        }
        return false;
    }
    output.push_str(chunk);
    if let Some(matched) = matched {
        visible.push(matched);
    }
    true
}

fn utf16_prefix(value: &str, max_units: usize) -> &str {
    let mut used = 0usize;
    for (index, character) in value.char_indices() {
        let next = used.saturating_add(character.len_utf16());
        if next > max_units {
            return &value[..index];
        }
        used = next;
    }
    value
}

fn truncate_utf8(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_owned();
    }
    let mut end = max_bytes.min(value.len());
    while !value.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    format!("{}...", &value[..end])
}

fn append_skipped_line_notice(message: &mut String, skipped: usize) {
    if skipped > 0 {
        message.push_str(&format!(
            "\n[{skipped} line(s) longer than {MAX_LINE_BYTES} bytes skipped]"
        ));
    }
}

struct BoundedLine {
    bytes: Vec<u8>,
    too_long: bool,
}

fn read_bounded_line(reader: &mut BufReader<File>) -> io::Result<Option<BoundedLine>> {
    let mut bytes = Vec::new();
    let mut too_long = false;
    let mut saw_bytes = false;
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return Ok(saw_bytes.then_some(BoundedLine { bytes, too_long }));
        }
        let consumed = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(available.len(), |index| index + 1);
        saw_bytes = true;
        if !too_long {
            if bytes.len().saturating_add(consumed) > MAX_LINE_BYTES {
                too_long = true;
                bytes.clear();
            } else {
                bytes.extend_from_slice(&available[..consumed]);
            }
        }
        let has_newline = available[consumed - 1] == b'\n';
        reader.consume(consumed);
        if has_newline {
            break;
        }
    }
    if bytes.last() == Some(&b'\n') {
        bytes.pop();
        if bytes.last() == Some(&b'\r') {
            bytes.pop();
        }
    }
    Ok(Some(BoundedLine { bytes, too_long }))
}

fn open_regular_file(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    options.open(path)
}

pub fn function_declaration() -> Value {
    json!({
        "name":"grep",
        "description":"Search workspace text files for a case-insensitive regular expression. Results include file names and line numbers and respect Canopy and common directory ignore rules.",
        "parameters":{
            "type":"OBJECT",
            "properties":{
                "pattern":{"type":"STRING","description":"Regular expression to search for."},
                "path":{"type":"STRING","description":"Optional file or directory. Defaults to the workspace and must remain inside it."},
                "glob":{"type":"STRING","description":"Optional glob to filter searched files, such as **/*.rs."},
                "limit":{"type":"INTEGER","description":"Optional positive maximum number of matching lines to return."}
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
            let root = std::env::temp_dir().join(format!("canopy-grep-{}", Uuid::new_v4()));
            std::fs::create_dir_all(&root).unwrap();
            Self(root)
        }
    }

    impl Drop for TempWorkspace {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn tool(workspace: &TempWorkspace, line_limit: usize, char_limit: usize) -> GrepTool {
        GrepTool::new(&workspace.0, Some(line_limit), Some(char_limit)).unwrap()
    }

    #[test]
    fn searches_case_insensitively_and_applies_file_globs() {
        let workspace = TempWorkspace::new();
        std::fs::create_dir(workspace.0.join("src")).unwrap();
        std::fs::write(
            workspace.0.join("src/main.rs"),
            "first needle\nsecond NEEDLE\nthird other\n",
        )
        .unwrap();
        std::fs::create_dir(workspace.0.join("node_modules")).unwrap();
        std::fs::write(
            workspace.0.join("node_modules/vendor.rs"),
            "needle in dependency\n",
        )
        .unwrap();
        std::fs::write(workspace.0.join("notes.md"), "needle in markdown\n").unwrap();

        let cache = FileReadCache::default();
        let grep =
            GrepTool::new_with_cache(&workspace.0, Some(100), Some(25_000), cache.clone()).unwrap();
        let result = grep
            .execute(&json!({"pattern":"needle","glob":"**/*.rs"}))
            .unwrap();
        assert!(result.llm_content.contains("Found 2 matches"));
        assert!(result.llm_content.contains("L1: first needle"));
        assert!(result.llm_content.contains("L2: second NEEDLE"));
        assert!(!result.llm_content.contains("markdown"));
        assert!(!result.llm_content.contains("node_modules"));
        assert_eq!(result.result_file_paths.len(), 1);
        cache
            .ensure_prior_read(workspace.0.join("src/main.rs"), "editing")
            .unwrap();
    }

    #[test]
    fn line_and_character_limits_keep_memory_and_output_bounded() {
        let workspace = TempWorkspace::new();
        std::fs::write(
            workspace.0.join("matches.txt"),
            (0..10)
                .map(|index| format!("match-{index} {}\n", "x".repeat(100)))
                .collect::<String>(),
        )
        .unwrap();

        let line_limited = tool(&workspace, 2, 25_000)
            .execute(&json!({"pattern":"match"}))
            .unwrap();
        assert!(line_limited.llm_content.contains("Found 10 matches"));
        assert!(line_limited.llm_content.contains("[8 lines truncated]"));
        assert!(line_limited.return_display.contains("(truncated)"));

        let char_limited = tool(&workspace, 10, 50)
            .execute(&json!({"pattern":"match"}))
            .unwrap();
        assert!(char_limited.llm_content.contains("..."));
        assert!(char_limited.llm_content.contains("truncated"));
    }

    #[test]
    fn rejects_external_paths_and_invalid_parameters() {
        let workspace = TempWorkspace::new();
        let outside = TempWorkspace::new();
        assert!(
            tool(&workspace, 100, 25_000)
                .execute(&json!({"pattern":"needle","path":outside.0.to_string_lossy()}))
                .unwrap_err()
                .contains("inside the workspace")
        );
        assert!(
            tool(&workspace, 100, 25_000)
                .execute(&json!({"pattern":"["}))
                .unwrap_err()
                .contains("Invalid regular expression")
        );
        assert!(
            tool(&workspace, 100, 25_000)
                .execute(&json!({"pattern":"x","limit":0}))
                .unwrap_err()
                .contains("positive integer")
        );
    }

    #[test]
    fn skips_and_reports_lines_above_the_read_bound() {
        let workspace = TempWorkspace::new();
        let mut content = "x".repeat(MAX_LINE_BYTES + 1);
        content.push_str("\nneedle after long line\n");
        std::fs::write(workspace.0.join("large.txt"), content).unwrap();

        let result = tool(&workspace, 100, 25_000)
            .execute(&json!({"pattern":"needle"}))
            .unwrap();
        assert!(result.llm_content.contains("Found 1 match"));
        assert!(result.llm_content.contains("1 line(s) longer than"));
        assert!(result.llm_content.contains("L2: needle after long line"));
    }
}
