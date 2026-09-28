use std::fs::OpenOptions;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde_json::{Value, json};

use crate::file_read_cache::FileReadCache;
use crate::secret_scanner::check_team_memory_secrets;
use crate::services::commit_attribution::CommitAttributionService;
use crate::services::file_history::FileHistoryService;
use crate::tools::prior_read_enforcement::{PriorReadVerb, check_prior_read};
use crate::tools::write_file::{WriteFilePreview, WriteFileTool};
use tokio::sync::Mutex;

const MAX_FILE_BYTES: u64 = 8 * 1024 * 1024;

pub struct EditFileTool {
    workspace_root: PathBuf,
    file_read_cache: FileReadCache,
    writer: WriteFileTool,
}

impl EditFileTool {
    pub fn new(
        workspace_root: impl AsRef<Path>,
        file_read_cache: FileReadCache,
    ) -> Result<Self, String> {
        let workspace_root = std::fs::canonicalize(workspace_root.as_ref())
            .map_err(|error| format!("could not resolve workspace root: {error}"))?;
        let writer = WriteFileTool::new(&workspace_root, file_read_cache.clone())?;
        Ok(Self {
            workspace_root,
            file_read_cache,
            writer,
        })
    }

    /// Attach the session's shared file-history service to this edit tool.
    pub fn with_file_history(mut self, file_history: Arc<Mutex<FileHistoryService>>) -> Self {
        self.writer = self.writer.with_file_history(file_history);
        self
    }

    /// Replace the session's shared file-history service after construction.
    pub fn set_file_history(&mut self, file_history: Arc<Mutex<FileHistoryService>>) {
        self.writer.set_file_history(file_history);
    }

    /// Attach the host's session-scoped commit-attribution service.
    pub fn with_commit_attribution(
        mut self,
        service: Arc<std::sync::Mutex<CommitAttributionService>>,
    ) -> Self {
        self.writer = self.writer.with_commit_attribution(service);
        self
    }

    /// Replace the session's shared commit-attribution service after
    /// construction.
    pub fn set_commit_attribution(
        &mut self,
        service: Arc<std::sync::Mutex<CommitAttributionService>>,
    ) {
        self.writer.set_commit_attribution(service);
    }

    pub fn preview(&self, args: &Value) -> Result<WriteFilePreview, String> {
        let (_path, requested_path, new_content) = self.calculate(args)?;
        self.writer
            .preview_edit(&json!({"file_path":requested_path,"content":new_content}))
    }

    pub fn execute(&self, args: &Value, approved: bool) -> Result<String, String> {
        if !approved {
            return Err("edit_file was not approved; no file was changed.".to_owned());
        }
        let (_path, requested_path, new_content) = self.calculate(args)?;
        self.writer
            .execute_edit(&json!({"file_path":requested_path,"content":new_content}))
    }

    fn calculate(&self, args: &Value) -> Result<(PathBuf, PathBuf, String), String> {
        let (requested_path, old_string, mut new_string, replace_all) = parse_args(args)?;
        if !requested_path.is_absolute() {
            return Err(format!(
                "File path must be absolute: {}",
                requested_path.display()
            ));
        }

        match std::fs::symlink_metadata(&requested_path) {
            Ok(_) => {
                let path = std::fs::canonicalize(&requested_path).map_err(|error| {
                    format!(
                        "could not resolve destination file {}: {error}",
                        requested_path.display()
                    )
                })?;
                if !path.starts_with(&self.workspace_root) {
                    return Err("edit_file is restricted to files inside the workspace".to_owned());
                }
                enforce_prior_read(&self.file_read_cache, &path, &requested_path, false)?;

                let raw_content = read_text_file(&path)?;
                enforce_prior_read(&self.file_read_cache, &path, &requested_path, true)?;
                let mut current_content = normalize_line_endings(&raw_content);
                let normalized = normalize_edit_strings(&current_content, &old_string, &new_string);
                let mut final_old_string = normalized.old_string;
                new_string = normalized.new_string;

                if old_string.is_empty() {
                    return Err(
                        "Failed to edit. Attempted to create a file that already exists."
                            .to_owned(),
                    );
                }
                final_old_string = maybe_augment_old_string_for_deletion(
                    &current_content,
                    &final_old_string,
                    &new_string,
                );
                let occurrences = count_occurrences(&current_content, &final_old_string);
                if occurrences == 0 {
                    return Err(format!(
                        "Failed to edit, 0 occurrences found for old_string in {}. No edits made. Ensure whitespace, indentation, and context match; read the file again before retrying.",
                        path.display()
                    ));
                }
                if !replace_all && occurrences > 1 {
                    return Err(format!(
                        "Failed to edit. Found {occurrences} occurrences for old_string in {} but replace_all was not enabled.",
                        path.display()
                    ));
                }
                if final_old_string == new_string {
                    return Err(
                        "No changes to apply. The old_string and new_string are identical."
                            .to_owned(),
                    );
                }

                current_content = current_content.replace(&final_old_string, &new_string);
                if current_content == raw_content {
                    return Err(
                        "No changes to apply. The new content is identical to the current content."
                            .to_owned(),
                    );
                }
                if let Some(error) =
                    check_team_memory_secrets(&path, &current_content, &self.workspace_root)
                {
                    return Err(error);
                }
                Ok((path, requested_path, current_content))
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let path = canonicalize_new_target(&requested_path, &self.workspace_root)?;
                enforce_prior_read(&self.file_read_cache, &path, &requested_path, false)?;
                if !old_string.is_empty() {
                    return Err(format!(
                        "File not found: {}. Use an empty old_string to create a new file.",
                        requested_path.display()
                    ));
                }
                if let Some(error) =
                    check_team_memory_secrets(&path, &new_string, &self.workspace_root)
                {
                    return Err(error);
                }
                Ok((path, requested_path, new_string))
            }
            Err(error) => Err(format!(
                "could not inspect destination {}: {error}",
                requested_path.display()
            )),
        }
    }
}

fn enforce_prior_read(
    cache: &FileReadCache,
    path: &Path,
    display_path: &Path,
    expect_existing: bool,
) -> Result<(), String> {
    let decision = check_prior_read(cache, path, PriorReadVerb::Editing, expect_existing);
    if decision.ok {
        Ok(())
    } else {
        Err(decision
            .raw_message
            .unwrap_or_else(|| "prior read check failed".to_owned())
            .replace(
                &path.display().to_string(),
                &display_path.display().to_string(),
            ))
    }
}

fn parse_args(args: &Value) -> Result<(PathBuf, String, String, bool), String> {
    let path = args
        .get("file_path")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|path| !path.is_empty())
        .ok_or_else(|| "The 'file_path' parameter must be non-empty.".to_owned())?;
    let old_string = args
        .get("old_string")
        .and_then(Value::as_str)
        .ok_or_else(|| "The 'old_string' parameter must be a string.".to_owned())?;
    let new_string = args
        .get("new_string")
        .and_then(Value::as_str)
        .ok_or_else(|| "The 'new_string' parameter must be a string.".to_owned())?;
    Ok((
        PathBuf::from(path),
        old_string.to_owned(),
        new_string.to_owned(),
        args.get("replace_all")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    ))
}

fn canonicalize_new_target(path: &Path, workspace_root: &Path) -> Result<PathBuf, String> {
    let file_name = path
        .file_name()
        .ok_or_else(|| "file_path must name a file".to_owned())?;
    let mut cursor = path
        .parent()
        .ok_or_else(|| "file path has no parent directory".to_owned())?
        .to_path_buf();
    let mut missing = Vec::new();
    loop {
        match std::fs::metadata(&cursor) {
            Ok(metadata) if metadata.is_dir() => break,
            Ok(_) => {
                return Err(format!(
                    "Parent path is not a directory: {}",
                    cursor.display()
                ));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let name = cursor
                    .file_name()
                    .ok_or_else(|| "could not find an existing parent directory".to_owned())?;
                missing.push(name.to_os_string());
                cursor = cursor
                    .parent()
                    .ok_or_else(|| "could not find an existing parent directory".to_owned())?
                    .to_path_buf();
            }
            Err(error) => {
                return Err(format!(
                    "could not inspect parent directory {}: {error}",
                    cursor.display()
                ));
            }
        }
    }
    let canonical_parent = std::fs::canonicalize(&cursor)
        .map_err(|error| format!("could not resolve parent directory: {error}"))?;
    if !canonical_parent.starts_with(workspace_root) {
        return Err("edit_file is restricted to files inside the workspace".to_owned());
    }
    let mut destination = canonical_parent;
    for part in missing.into_iter().rev() {
        destination.push(part);
    }
    destination.push(file_name);
    Ok(destination)
}

fn read_text_file(path: &Path) -> Result<String, String> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = options
        .open(path)
        .map_err(|error| format!("could not read {}: {error}", path.display()))?;
    let metadata = file
        .metadata()
        .map_err(|error| format!("could not inspect {}: {error}", path.display()))?;
    if !metadata.is_file() {
        return Err(format!("Path is not a regular file: {}", path.display()));
    }
    if metadata.len() > MAX_FILE_BYTES {
        return Err(format!(
            "{} exceeds the {MAX_FILE_BYTES}-byte edit limit",
            path.display()
        ));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(MAX_FILE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| format!("could not read {}: {error}", path.display()))?;
    if bytes.len() as u64 > MAX_FILE_BYTES {
        return Err(format!(
            "{} exceeds the {MAX_FILE_BYTES}-byte edit limit",
            path.display()
        ));
    }
    let text = bytes.strip_prefix(&[0xef, 0xbb, 0xbf]).unwrap_or(&bytes);
    let content = String::from_utf8(text.to_vec())
        .map_err(|_| "Existing file is not valid UTF-8 text.".to_owned())?;
    Ok(content)
}

struct NormalizedEdit {
    old_string: String,
    new_string: String,
}

fn normalize_line_endings(content: &str) -> String {
    content.replace("\r\n", "\n")
}

fn normalize_edit_strings(content: &str, old_string: &str, new_string: &str) -> NormalizedEdit {
    if old_string.is_empty() {
        return NormalizedEdit {
            old_string: old_string.to_owned(),
            new_string: new_string.to_owned(),
        };
    }
    if let Some(range) = content.find(old_string) {
        return NormalizedEdit {
            old_string: content[range..range + old_string.len()].to_owned(),
            new_string: new_string.to_owned(),
        };
    }
    if let Some(range) = find_normalized_character_match(content, old_string) {
        return NormalizedEdit {
            old_string: content[range].to_owned(),
            new_string: new_string.to_owned(),
        };
    }
    if let Some(found) = find_line_based_match(content, old_string) {
        return NormalizedEdit {
            old_string: found.slice,
            new_string: if found.removed_trailing_final_empty_line {
                remove_trailing_newline(new_string).to_owned()
            } else {
                new_string.to_owned()
            },
        };
    }
    NormalizedEdit {
        old_string: old_string.to_owned(),
        new_string: new_string.to_owned(),
    }
}

fn find_normalized_character_match(content: &str, needle: &str) -> Option<std::ops::Range<usize>> {
    let haystack_chars = content.char_indices().collect::<Vec<_>>();
    let haystack = haystack_chars
        .iter()
        .map(|(_, ch)| normalize_character(*ch))
        .collect::<Vec<_>>();
    let needle = needle.chars().map(normalize_character).collect::<Vec<_>>();
    if needle.is_empty() || needle.len() > haystack.len() {
        return None;
    }
    (0..=haystack.len() - needle.len())
        .find(|start| haystack[*start..*start + needle.len()] == needle)
        .map(|start| {
            let byte_start = haystack_chars[start].0;
            let end_char = start + needle.len();
            let byte_end = haystack_chars
                .get(end_char)
                .map(|(index, _)| *index)
                .unwrap_or(content.len());
            byte_start..byte_end
        })
}

fn normalize_character(character: char) -> char {
    match character {
        '\u{2010}'..='\u{2015}' | '\u{2212}' => '-',
        '\u{2018}'..='\u{201b}' => '\'',
        '\u{201c}'..='\u{201f}' => '"',
        '\u{00a0}' | '\u{2002}'..='\u{200a}' | '\u{202f}' | '\u{205f}' | '\u{3000}' => ' ',
        character => character,
    }
}

struct LineMatch {
    slice: String,
    removed_trailing_final_empty_line: bool,
}

fn find_line_based_match(content: &str, needle: &str) -> Option<LineMatch> {
    let lines = content.split('\n').collect::<Vec<_>>();
    let mut offsets = Vec::with_capacity(lines.len() + 1);
    let mut cursor = 0;
    for (index, line) in lines.iter().enumerate() {
        offsets.push(cursor);
        cursor += line.len();
        if index + 1 < lines.len() {
            cursor += 1;
        }
    }
    offsets.push(content.len());
    let pattern = needle.split('\n').collect::<Vec<_>>();
    let attempt = |pattern: &[&str]| {
        for mode in 0..3 {
            if let Some(start) = find_line_sequence(&lines, pattern, mode) {
                return Some(start);
            }
        }
        None
    };
    if let Some(start) = attempt(&pattern) {
        return Some(LineMatch {
            slice: slice_lines(
                content,
                &lines,
                &offsets,
                start,
                pattern.len(),
                needle.ends_with('\n'),
            ),
            removed_trailing_final_empty_line: false,
        });
    }
    if pattern.last() == Some(&"") && pattern.len() > 1 {
        let shorter = &pattern[..pattern.len() - 1];
        if let Some(start) = attempt(shorter) {
            return Some(LineMatch {
                slice: slice_lines(content, &lines, &offsets, start, shorter.len(), false),
                removed_trailing_final_empty_line: true,
            });
        }
    }
    None
}

fn find_line_sequence(lines: &[&str], pattern: &[&str], mode: u8) -> Option<usize> {
    if pattern.is_empty() || pattern.len() > lines.len() {
        return None;
    }
    (0..=lines.len() - pattern.len()).find(|start| {
        lines[*start..*start + pattern.len()]
            .iter()
            .zip(pattern)
            .all(|(actual, expected)| match mode {
                0 => actual == expected,
                1 => actual.trim_end() == expected.trim_end(),
                _ => normalize_line(actual) == normalize_line(expected),
            })
    })
}

fn normalize_line(line: &str) -> String {
    line.chars()
        .map(normalize_character)
        .collect::<String>()
        .trim_end()
        .to_owned()
}

fn slice_lines(
    content: &str,
    lines: &[&str],
    offsets: &[usize],
    start: usize,
    count: usize,
    include_trailing_newline: bool,
) -> String {
    let last = start + count - 1;
    let mut end = offsets[last] + lines[last].len();
    if include_trailing_newline {
        if let Some(next_line) = offsets.get(start + count) {
            end = *next_line;
        } else if content.ends_with('\n') {
            end = content.len();
        }
    }
    content[offsets[start]..end].to_owned()
}

fn remove_trailing_newline(value: &str) -> &str {
    value
        .strip_suffix("\r\n")
        .or_else(|| value.strip_suffix('\n'))
        .or_else(|| value.strip_suffix('\r'))
        .unwrap_or(value)
}

fn maybe_augment_old_string_for_deletion(
    content: &str,
    old_string: &str,
    new_string: &str,
) -> String {
    if old_string.is_empty() || !new_string.is_empty() || old_string.ends_with('\n') {
        return old_string.to_owned();
    }
    let with_newline = format!("{old_string}\n");
    if content.contains(&with_newline) {
        with_newline
    } else {
        old_string.to_owned()
    }
}

fn count_occurrences(source: &str, needle: &str) -> usize {
    if needle.is_empty() {
        0
    } else {
        source.match_indices(needle).count()
    }
}

pub fn function_declaration() -> Value {
    json!({
        "name":"edit_file",
        "description":"Replace a literal text span inside a workspace file. Existing files must have been read first. The CLI shows the full diff and requires approval before writing.",
        "parameters":{
            "type":"OBJECT",
            "properties":{
                "file_path":{"type":"STRING","description":"Absolute destination path inside the current workspace."},
                "old_string":{"type":"STRING","description":"Exact text to replace. Include enough context to identify one location. Use an empty string only to create a new file."},
                "new_string":{"type":"STRING","description":"Replacement text."},
                "replace_all":{"type":"BOOLEAN","description":"Replace every occurrence instead of requiring a unique match."}
            },
            "required":["file_path","old_string","new_string"]
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    struct TempWorkspace(PathBuf);

    impl TempWorkspace {
        fn new() -> Self {
            let root = std::env::temp_dir().join(format!("canopy-edit-file-{}", Uuid::new_v4()));
            std::fs::create_dir_all(&root).unwrap();
            Self(root)
        }
    }

    impl Drop for TempWorkspace {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn record_read(cache: &FileReadCache, path: &Path) {
        cache.record_read(path, &std::fs::metadata(path).unwrap(), true, true);
    }

    #[test]
    fn rejects_content_oracle_edits_with_the_structured_prior_read_message() {
        let workspace = TempWorkspace::new();
        let path = workspace.0.join("secret.txt");
        std::fs::write(&path, "needle and private bytes").unwrap();
        let tool = EditFileTool::new(&workspace.0, FileReadCache::default()).unwrap();
        let args = json!({"file_path":path,"old_string":"needle","new_string":"safe"});

        assert_eq!(
            tool.preview(&args).unwrap_err(),
            format!(
                "File {} has not been read in this session. Use the read_file tool first to load the current content (a partial read with offset / limit is fine — you only need to have seen the bytes you intend to edit) before editing it.",
                path.display()
            )
        );
        assert_eq!(
            std::fs::read_to_string(path).unwrap(),
            "needle and private bytes"
        );
    }

    #[test]
    fn requires_unique_match_and_supports_replace_all() {
        let workspace = TempWorkspace::new();
        let path = workspace.0.join("notes.txt");
        std::fs::write(&path, "red red").unwrap();
        let cache = FileReadCache::default();
        record_read(&cache, &path);
        let tool = EditFileTool::new(&workspace.0, cache.clone()).unwrap();
        let args = json!({"file_path":path,"old_string":"red","new_string":"blue"});
        assert!(
            tool.preview(&args)
                .unwrap_err()
                .contains("Found 2 occurrences")
        );
        let replace_all =
            json!({"file_path":path,"old_string":"red","new_string":"blue","replace_all":true});
        assert!(
            tool.preview(&replace_all)
                .unwrap()
                .diff
                .contains("+blue blue")
        );
        assert!(
            tool.execute(&replace_all, false)
                .unwrap_err()
                .contains("not approved")
        );
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "red red");
        tool.execute(&replace_all, true).unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "blue blue");
    }

    #[test]
    fn creates_files_and_uses_line_normalization_for_matches() {
        let workspace = TempWorkspace::new();
        let path = workspace.0.join("nested/notes.txt");
        let cache = FileReadCache::default();
        let tool = EditFileTool::new(&workspace.0, cache.clone()).unwrap();
        let create = json!({"file_path":path,"old_string":"","new_string":"first\nsecond\n"});
        assert!(tool.preview(&create).unwrap().is_new_file);
        tool.execute(&create, true).unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "first\nsecond\n");

        record_read(&cache, &path);
        let edit = json!({"file_path":path,"old_string":"second  \n","new_string":"third\n"});
        assert!(tool.preview(&edit).unwrap().diff.contains("+third"));
        tool.execute(&edit, true).unwrap();
        assert_eq!(std::fs::read_to_string(path).unwrap(), "first\nthird\n");
    }

    #[test]
    fn unicode_normalization_and_deletion_preserve_literal_edit_intent() {
        let workspace = TempWorkspace::new();
        let path = workspace.0.join("quotes.txt");
        std::fs::write(&path, "‘hello’\nremove me\nkeep\n").unwrap();
        let cache = FileReadCache::default();
        record_read(&cache, &path);
        let tool = EditFileTool::new(&workspace.0, cache).unwrap();
        let edit = json!({"file_path":path,"old_string":"'hello'","new_string":"\"hello\""});
        tool.execute(&edit, true).unwrap();
        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.starts_with("\"hello\"\n"));

        let cache = FileReadCache::default();
        record_read(&cache, &path);
        let tool = EditFileTool::new(&workspace.0, cache).unwrap();
        let delete = json!({"file_path":path,"old_string":"remove me","new_string":""});
        tool.execute(&delete, true).unwrap();
        assert_eq!(std::fs::read_to_string(path).unwrap(), "\"hello\"\nkeep\n");
    }

    #[test]
    fn scans_the_full_result_before_editing_team_memory() {
        let workspace = TempWorkspace::new();
        std::fs::create_dir_all(workspace.0.join(".git")).unwrap();
        let path = workspace.0.join(".canopy/team-memory/notes.md");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, "token = placeholder\n").unwrap();
        let cache = FileReadCache::default();
        record_read(&cache, &path);
        let tool = EditFileTool::new(&workspace.0, cache).unwrap();
        let secret = format!("ghp_{}", "b".repeat(36));
        let args = json!({"file_path":path,"old_string":"placeholder","new_string":secret});
        let error = tool.preview(&args).unwrap_err();
        assert!(error.contains("GitHub PAT"));
        assert!(!error.contains(&secret));
    }
}
