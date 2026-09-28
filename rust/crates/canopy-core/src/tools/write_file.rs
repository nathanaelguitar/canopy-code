use std::ffi::OsString;
use std::fs::{File, Metadata, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use serde_json::{Value, json};
use similar::TextDiff;
use uuid::Uuid;

use crate::file_read_cache::FileReadCache;
use crate::secret_scanner::check_team_memory_secrets;
use crate::services::commit_attribution::CommitAttributionService;
use crate::services::file_history::FileHistoryService;
use crate::tools::prior_read_enforcement::{PriorReadVerb, check_prior_read};
use tokio::sync::Mutex;

const MAX_FILE_BYTES: u64 = 8 * 1024 * 1024;
const MAX_DIFF_UTF16_UNITS: usize = 50_000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteFilePreview {
    pub path: PathBuf,
    pub is_new_file: bool,
    pub diff: String,
}

impl WriteFilePreview {
    pub fn confirmation_text(&self) -> String {
        let action = if self.is_new_file {
            "Create"
        } else {
            "Overwrite"
        };
        format!(
            "{action} {}?\n{}",
            self.path.display(),
            if self.diff.is_empty() {
                "No content changes.".to_owned()
            } else {
                self.diff.clone()
            }
        )
    }
}

pub struct WriteFileTool {
    workspace_root: PathBuf,
    file_read_cache: FileReadCache,
    file_history: Option<Arc<Mutex<FileHistoryService>>>,
    commit_attribution: Option<Arc<std::sync::Mutex<CommitAttributionService>>>,
}

impl WriteFileTool {
    pub fn new(
        workspace_root: impl AsRef<Path>,
        file_read_cache: FileReadCache,
    ) -> Result<Self, String> {
        let workspace_root = std::fs::canonicalize(workspace_root.as_ref())
            .map_err(|error| format!("could not resolve workspace root: {error}"))?;
        if !workspace_root.is_dir() {
            return Err("workspace root is not a directory".to_owned());
        }
        Ok(Self {
            workspace_root,
            file_read_cache,
            file_history: None,
            commit_attribution: None,
        })
    }

    /// Attach the session's shared file-history service to this writer.
    ///
    /// Callers that also construct edit or notebook tools should pass clones
    /// of the same handle to each writer. The session owner remains
    /// responsible for calling `make_snapshot` at each turn boundary and
    /// serializing that async operation with file-tool execution.
    pub fn with_file_history(mut self, file_history: Arc<Mutex<FileHistoryService>>) -> Self {
        self.file_history = Some(file_history);
        self
    }

    /// Replace the session's shared file-history service after construction.
    pub fn set_file_history(&mut self, file_history: Arc<Mutex<FileHistoryService>>) {
        self.file_history = Some(file_history);
    }

    /// Attach the host's session-scoped commit-attribution service. Successful
    /// writes update it on a best-effort basis after the file has been written.
    pub fn with_commit_attribution(
        mut self,
        service: Arc<std::sync::Mutex<CommitAttributionService>>,
    ) -> Self {
        self.commit_attribution = Some(service);
        self
    }

    /// Replace the session's shared commit-attribution service after
    /// construction.
    pub fn set_commit_attribution(
        &mut self,
        service: Arc<std::sync::Mutex<CommitAttributionService>>,
    ) {
        self.commit_attribution = Some(service);
    }

    pub fn preview(&self, args: &Value) -> Result<WriteFilePreview, String> {
        self.preview_internal(args, false, PriorReadVerb::Overwriting)
    }

    pub(crate) fn preview_edit(&self, args: &Value) -> Result<WriteFilePreview, String> {
        self.preview_internal(args, false, PriorReadVerb::Editing)
    }

    pub(crate) fn preview_notebook(&self, args: &Value) -> Result<WriteFilePreview, String> {
        require_notebook_path(args)?;
        self.preview_internal(args, true, PriorReadVerb::Overwriting)
    }

    fn preview_internal(
        &self,
        args: &Value,
        notebook_mode: bool,
        verb: PriorReadVerb,
    ) -> Result<WriteFilePreview, String> {
        let (requested_path, proposed_content) = parse_args(args)?;
        let target = self.resolve_target(&requested_path)?;
        self.ensure_prior_read(
            &target.path,
            &requested_path,
            verb,
            notebook_mode,
            notebook_mode,
        )?;
        if notebook_mode && !target.exists {
            return Err(format!(
                "Notebook file not found: {}",
                target.path.display()
            ));
        }
        if let Some(error) =
            check_team_memory_secrets(&target.path, &proposed_content, &self.workspace_root)
        {
            return Err(error);
        }
        let original_content = if target.exists {
            let snapshot = read_text_snapshot(&target.path)?;
            self.ensure_prior_read(&target.path, &requested_path, verb, notebook_mode, true)?;
            normalize_line_endings(&snapshot.content)
        } else {
            String::new()
        };
        let proposed_content = normalize_line_endings(&proposed_content);
        let diff = make_diff(&target.path, &original_content, &proposed_content)?;
        Ok(WriteFilePreview {
            path: target.path,
            is_new_file: !target.exists,
            diff,
        })
    }

    /// Write a file only after the caller has obtained explicit approval.
    pub fn execute(&self, args: &Value, approved: bool) -> Result<String, String> {
        self.execute_internal(args, approved, false, false, PriorReadVerb::Overwriting)
    }

    pub(crate) fn execute_edit(&self, args: &Value) -> Result<String, String> {
        self.execute_internal(args, true, false, false, PriorReadVerb::Editing)
    }

    pub(crate) fn execute_notebook(
        &self,
        args: &Value,
        approved: bool,
        requires_read_after_write: bool,
    ) -> Result<String, String> {
        require_notebook_path(args)?;
        self.execute_internal(
            args,
            approved,
            true,
            requires_read_after_write,
            PriorReadVerb::Overwriting,
        )
    }

    fn execute_internal(
        &self,
        args: &Value,
        approved: bool,
        notebook_mode: bool,
        requires_read_after_write: bool,
        verb: PriorReadVerb,
    ) -> Result<String, String> {
        if !approved {
            return Err(if notebook_mode {
                "notebook_edit was not approved; no file was changed.".to_owned()
            } else {
                "write_file was not approved; no file was changed.".to_owned()
            });
        }
        let (requested_path, proposed_content) = parse_args(args)?;
        let mut target = self.resolve_target(&requested_path)?;
        self.ensure_prior_read(
            &target.path,
            &requested_path,
            verb,
            notebook_mode,
            notebook_mode,
        )?;
        if notebook_mode && !target.exists {
            return Err(format!(
                "Notebook file not found: {}",
                target.path.display()
            ));
        }
        if let Some(error) =
            check_team_memory_secrets(&target.path, &proposed_content, &self.workspace_root)
        {
            return Err(error);
        }
        let snapshot = if target.exists {
            let snapshot = read_text_snapshot(&target.path)?;
            self.ensure_prior_read(&target.path, &requested_path, verb, notebook_mode, true)?;
            Some(snapshot)
        } else {
            None
        };

        let original_content = snapshot
            .as_ref()
            .map(|snapshot| normalize_line_endings(&snapshot.content))
            .unwrap_or_default();
        let proposed_preview_content = normalize_line_endings(&proposed_content);
        make_diff(&target.path, &original_content, &proposed_preview_content)?;

        if !target.exists {
            let parent = target
                .path
                .parent()
                .ok_or_else(|| "file path has no parent directory".to_owned())?;
            std::fs::create_dir_all(parent)
                .map_err(|error| format!("could not create parent directories: {error}"))?;
            let canonical_parent = std::fs::canonicalize(parent)
                .map_err(|error| format!("could not resolve destination directory: {error}"))?;
            if !canonical_parent.starts_with(&self.workspace_root) {
                return Err("write_file is restricted to files inside the workspace".to_owned());
            }
            let file_name = target
                .path
                .file_name()
                .ok_or_else(|| "file path must name a file".to_owned())?;
            target.path = canonical_parent.join(file_name);
        }

        let is_new_file = snapshot.is_none();
        let line_ending = snapshot
            .as_ref()
            .map(|snapshot| snapshot.line_ending)
            .unwrap_or("\n");
        let bom = snapshot.as_ref().is_some_and(|snapshot| snapshot.bom);
        let permissions = snapshot
            .as_ref()
            .map(|snapshot| snapshot.metadata.permissions());
        let bytes = encode_content(&proposed_content, line_ending, bom)?;
        if bytes.len() as u64 > MAX_FILE_BYTES {
            return Err(format!(
                "proposed content exceeds the {MAX_FILE_BYTES}-byte write limit"
            ));
        }

        if !is_new_file {
            if let Err(error) = verify_existing_writable(&target.path) {
                self.ensure_prior_read(&target.path, &requested_path, verb, notebook_mode, true)?;
                return Err(error);
            }
        }
        self.ensure_prior_read(
            &target.path,
            &requested_path,
            verb,
            notebook_mode,
            !is_new_file,
        )?;
        self.track_file_history_edit(&target.path);
        write_atomically(&target.path, &bytes, permissions, is_new_file)
            .map_err(|error| format!("could not write {}: {error}", target.path.display()))?;
        self.track_commit_attribution_edit(
            &target.path,
            (!is_new_file).then_some(original_content.as_str()),
            &proposed_preview_content,
        );
        if let Ok(metadata) = std::fs::metadata(&target.path) {
            if notebook_mode && requires_read_after_write {
                self.file_read_cache.invalidate(&metadata);
            } else {
                self.file_read_cache
                    .record_write(&target.path, &metadata, !notebook_mode);
            }
        }

        if is_new_file {
            Ok(format!(
                "Successfully created and wrote to new file: {}.",
                target.path.display()
            ))
        } else {
            Ok(format!(
                "Successfully overwrote file: {}.",
                target.path.display()
            ))
        }
    }

    fn track_file_history_edit(&self, path: &Path) {
        let (Some(file_history), Some(path)) = (&self.file_history, path.to_str()) else {
            return;
        };
        if let Ok(mut file_history) = file_history.try_lock() {
            // File history is best effort; it must never prevent a validated
            // write from proceeding if its shared state is unavailable.
            file_history.track_edit(path);
        }
    }

    fn track_commit_attribution_edit(
        &self,
        path: &Path,
        old_content: Option<&str>,
        new_content: &str,
    ) {
        let Some(service) = &self.commit_attribution else {
            return;
        };
        if let Ok(mut service) = service.lock() {
            service.record_edit(path, old_content, new_content);
        }
    }

    fn ensure_prior_read(
        &self,
        path: &Path,
        display_path: &Path,
        verb: PriorReadVerb,
        notebook_mode: bool,
        expect_existing: bool,
    ) -> Result<(), String> {
        if notebook_mode {
            self.ensure_notebook_prior_read(path, display_path, expect_existing)
        } else {
            let decision = check_prior_read(&self.file_read_cache, path, verb, expect_existing);
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
    }

    pub(crate) fn ensure_notebook_prior_read(
        &self,
        path: &Path,
        display_path: &Path,
        expect_existing: bool,
    ) -> Result<(), String> {
        use crate::file_read_cache::FileReadCheckResult;

        let metadata = match std::fs::metadata(path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                if !expect_existing {
                    return Ok(());
                }
                return Err(format!(
                    "Notebook {} disappeared after it was read. Re-read it with the read_file tool before editing it.",
                    display_path.display()
                ));
            }
            Err(error) => {
                let code = stat_error_code(&error).unwrap_or_else(|| "unknown error".to_owned());
                return Err(format!(
                    "Could not stat {} to verify prior notebook read ({code}). Re-read it with the read_file tool before editing it.",
                    display_path.display()
                ));
            }
        };
        if metadata.is_dir() {
            return Err(format!(
                "{} is a directory. The NotebookEdit tool only operates on .ipynb files.",
                display_path.display()
            ));
        }
        if !metadata.is_file() {
            return Err(format!(
                "{} is not a regular file. The NotebookEdit tool only operates on .ipynb files.",
                display_path.display()
            ));
        }

        match self.file_read_cache.check(&metadata) {
            FileReadCheckResult::Fresh(entry)
                if entry.last_read_at.is_some() && entry.last_read_was_full =>
            {
                Ok(())
            }
            FileReadCheckResult::Fresh(entry)
                if entry.last_read_at.is_some() && !entry.last_read_was_full =>
            {
                Err(format!(
                    "Notebook {} is too large for cell-level editing because its rendered output was truncated when read. Reduce the notebook output size or split the notebook before editing cells.",
                    display_path.display()
                ))
            }
            FileReadCheckResult::Stale(_) => Err(format!(
                "Notebook {} has been modified since you last read it. Re-read it with the read_file tool before editing it.",
                display_path.display()
            )),
            FileReadCheckResult::Unverifiable => Err(format!(
                "Notebook {} is on a filesystem that does not provide a verifiable inode identity (ino=0), so NotebookEdit cannot safely confirm a prior read. Use a different mechanism to edit this notebook.",
                display_path.display()
            )),
            FileReadCheckResult::Unknown | FileReadCheckResult::Fresh(_) => Err(format!(
                "Notebook {} has not been fully read in this session. Use the read_file tool first, without offset or limit, before editing cells.",
                display_path.display()
            )),
        }
    }

    fn resolve_target(&self, requested_path: &Path) -> Result<PreparedTarget, String> {
        if !requested_path.is_absolute() {
            return Err(format!(
                "File path must be absolute: {}",
                requested_path.display()
            ));
        }
        let normalized = lexical_normalize(requested_path);
        if normalized.file_name().is_none() {
            return Err("file_path must name a file".to_owned());
        }

        match std::fs::symlink_metadata(&normalized) {
            Ok(_) => {
                let canonical = std::fs::canonicalize(&normalized).map_err(|error| {
                    format!(
                        "could not resolve destination file {}: {error}",
                        normalized.display()
                    )
                })?;
                if !canonical.starts_with(&self.workspace_root) {
                    return Err("write_file is restricted to files inside the workspace".to_owned());
                }
                Ok(PreparedTarget {
                    path: canonical,
                    exists: true,
                })
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let file_name = normalized
                    .file_name()
                    .ok_or_else(|| "file_path must name a file".to_owned())?;
                let mut cursor = normalized
                    .parent()
                    .ok_or_else(|| "file path has no parent directory".to_owned())?
                    .to_path_buf();
                let mut missing = Vec::<OsString>::new();
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
                            let name = cursor.file_name().ok_or_else(|| {
                                "could not find an existing parent directory".to_owned()
                            })?;
                            missing.push(name.to_os_string());
                            cursor = cursor
                                .parent()
                                .ok_or_else(|| {
                                    "could not find an existing parent directory".to_owned()
                                })?
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
                if !canonical_parent.starts_with(&self.workspace_root) {
                    return Err("write_file is restricted to files inside the workspace".to_owned());
                }
                let mut destination = canonical_parent;
                for part in missing.into_iter().rev() {
                    destination.push(part);
                }
                destination.push(file_name);
                if !destination.starts_with(&self.workspace_root) {
                    return Err("write_file is restricted to files inside the workspace".to_owned());
                }
                Ok(PreparedTarget {
                    path: destination,
                    exists: false,
                })
            }
            Err(error) => Err(format!(
                "could not inspect destination {}: {error}",
                normalized.display()
            )),
        }
    }
}

fn require_notebook_path(args: &Value) -> Result<(), String> {
    let path = args
        .get("file_path")
        .and_then(Value::as_str)
        .ok_or_else(|| "The 'file_path' parameter must be non-empty.".to_owned())?;
    if Path::new(path)
        .extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| extension.eq_ignore_ascii_case("ipynb"))
    {
        Ok(())
    } else {
        Err("notebook_edit only operates on .ipynb files.".to_owned())
    }
}

struct PreparedTarget {
    path: PathBuf,
    exists: bool,
}

struct TextSnapshot {
    content: String,
    bom: bool,
    line_ending: &'static str,
    metadata: Metadata,
}

fn parse_args(args: &Value) -> Result<(PathBuf, String), String> {
    let file_path = args
        .get("file_path")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|path| !path.is_empty())
        .ok_or_else(|| "The 'file_path' parameter must be non-empty.".to_owned())?;
    let content = args
        .get("content")
        .and_then(Value::as_str)
        .ok_or_else(|| "The 'content' parameter must be a string.".to_owned())?;
    if content.len() as u64 > MAX_FILE_BYTES {
        return Err(format!(
            "proposed content exceeds the {MAX_FILE_BYTES}-byte write limit"
        ));
    }
    Ok((PathBuf::from(file_path), content.to_owned()))
}

fn read_text_snapshot(path: &Path) -> Result<TextSnapshot, String> {
    let file = open_regular_file(path)
        .map_err(|error| format!("could not read existing file {}: {error}", path.display()))?;
    let metadata = file
        .metadata()
        .map_err(|error| format!("could not inspect existing file: {error}"))?;
    if !metadata.is_file() {
        return Err(format!("Path is not a regular file: {}", path.display()));
    }
    if metadata.len() > MAX_FILE_BYTES {
        return Err(format!(
            "existing file exceeds the {MAX_FILE_BYTES}-byte write limit"
        ));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(MAX_FILE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| format!("could not read existing file: {error}"))?;
    if bytes.len() as u64 > MAX_FILE_BYTES {
        return Err(format!(
            "existing file exceeds the {MAX_FILE_BYTES}-byte write limit"
        ));
    }
    let bom = bytes.starts_with(&[0xef, 0xbb, 0xbf]);
    let text_bytes = if bom { &bytes[3..] } else { &bytes[..] };
    let content = String::from_utf8(text_bytes.to_vec())
        .map_err(|_| "Existing file is not valid UTF-8 text.".to_owned())?;
    let line_ending = detect_line_ending(&content);
    Ok(TextSnapshot {
        content,
        bom,
        line_ending,
        metadata,
    })
}

fn detect_line_ending(content: &str) -> &'static str {
    let crlf_count = content.matches("\r\n").count();
    let lf_count = content.matches('\n').count().saturating_sub(crlf_count);
    if crlf_count > lf_count { "\r\n" } else { "\n" }
}

fn stat_error_code(error: &io::Error) -> Option<String> {
    let errno = error.raw_os_error()?;
    #[cfg(unix)]
    {
        match nix::errno::Errno::from_raw(errno) {
            nix::errno::Errno::UnknownErrno => None,
            errno => Some(format!("{errno:?}")),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = errno;
        let name = match error.kind() {
            io::ErrorKind::NotFound => "ENOENT",
            io::ErrorKind::PermissionDenied => "EACCES",
            io::ErrorKind::AlreadyExists => "EEXIST",
            io::ErrorKind::InvalidInput | io::ErrorKind::InvalidData => "EINVAL",
            io::ErrorKind::TimedOut => "ETIMEDOUT",
            io::ErrorKind::WouldBlock => "EAGAIN",
            _ => return None,
        };
        Some(name.to_owned())
    }
}

fn normalize_line_endings(content: &str) -> String {
    content.replace("\r\n", "\n")
}

fn encode_content(content: &str, line_ending: &str, bom: bool) -> Result<Vec<u8>, String> {
    let normalized = normalize_line_endings(content);
    let disk_content = if line_ending == "\r\n" {
        normalized.replace('\n', "\r\n")
    } else {
        normalized
    };
    let mut bytes = Vec::with_capacity(disk_content.len() + usize::from(bom) * 3);
    if bom {
        bytes.extend_from_slice(&[0xef, 0xbb, 0xbf]);
    }
    bytes.extend_from_slice(disk_content.as_bytes());
    if bytes.len() as u64 > MAX_FILE_BYTES {
        return Err(format!(
            "proposed content exceeds the {MAX_FILE_BYTES}-byte write limit"
        ));
    }
    Ok(bytes)
}

fn make_diff(path: &Path, old_content: &str, new_content: &str) -> Result<String, String> {
    let file_name = path.file_name().unwrap_or_default().to_string_lossy();
    let diff = TextDiff::from_lines(old_content, new_content)
        .unified_diff()
        .context_radius(3)
        .header(
            &format!("{file_name} (Current)"),
            &format!("{file_name} (Proposed)"),
        )
        .to_string();
    if diff.encode_utf16().count() > MAX_DIFF_UTF16_UNITS {
        return Err(format!(
            "The proposed change to {} is too large to review safely ({MAX_DIFF_UTF16_UNITS} UTF-16 character preview limit). No changes were made.",
            path.display()
        ));
    }
    Ok(diff)
}

fn lexical_normalize(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            Component::Normal(part) => normalized.push(part),
        }
    }
    normalized
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

struct TempFile(PathBuf);

impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn write_atomically(
    path: &Path,
    bytes: &[u8],
    permissions: Option<std::fs::Permissions>,
    is_new_file: bool,
) -> io::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "file has no parent"))?;
    let file_name = path.file_name().unwrap_or_default().to_string_lossy();
    let temp_path = parent.join(format!(".{file_name}.canopy-{}.tmp", Uuid::new_v4()));
    let temp_guard = TempFile(temp_path.clone());
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o666);
    }
    let mut file = options.open(&temp_path)?;
    file.write_all(bytes)?;
    if let Some(permissions) = permissions {
        file.set_permissions(permissions)?;
    }
    file.sync_all()?;
    drop(file);

    if is_new_file {
        std::fs::hard_link(&temp_path, path)?;
        let _ = std::fs::remove_file(&temp_path);
    } else {
        std::fs::rename(&temp_path, path)?;
    }
    drop(temp_guard);
    if let Ok(directory) = File::open(parent) {
        directory.sync_all()?;
    }
    Ok(())
}

fn verify_existing_writable(path: &Path) -> Result<(), String> {
    let mut options = OpenOptions::new();
    options.write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    options
        .open(path)
        .map(drop)
        .map_err(|error| format!("could not open {} for writing: {error}", path.display()))
}

pub fn function_declaration() -> Value {
    json!({
        "name":"write_file",
        "description":"Write UTF-8 text to a file inside the current workspace. Existing files must have been read first. The CLI shows a diff and asks for approval before writing.",
        "parameters":{
            "type":"OBJECT",
            "properties":{
                "file_path":{"type":"STRING","description":"Absolute destination path inside the workspace."},
                "content":{"type":"STRING","description":"Complete UTF-8 text to write."}
            },
            "required":["file_path","content"]
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::file_history::resolve_backup_path;
    use crate::tools::read_file::ReadFileTool;
    use serde_json::json;
    use uuid::Uuid;

    struct TempWorkspace(PathBuf);

    impl TempWorkspace {
        fn new() -> Self {
            let root = std::env::temp_dir().join(format!("canopy-write-file-{}", Uuid::new_v4()));
            std::fs::create_dir_all(&root).unwrap();
            Self(root)
        }
    }

    impl Drop for TempWorkspace {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[tokio::test]
    async fn asks_for_approval_and_preserves_bom_and_crlf_on_replacement() {
        let workspace = TempWorkspace::new();
        let path = workspace.0.join("notes.txt");
        std::fs::write(&path, b"\xef\xbb\xbfold\r\nline\r\n").unwrap();
        let cache = FileReadCache::default();
        ReadFileTool::new_with_cache(&workspace.0, cache.clone())
            .unwrap()
            .execute(&json!({"file_path":path}))
            .await
            .unwrap();
        let tool = WriteFileTool::new(&workspace.0, cache.clone()).unwrap();
        let args = json!({"file_path":path,"content":"new\nline\n"});

        let preview = tool.preview(&args).unwrap();
        assert!(!preview.is_new_file);
        assert!(preview.diff.contains("-old"));
        assert!(preview.diff.contains("+new"));
        assert!(
            tool.execute(&args, false)
                .unwrap_err()
                .contains("not approved")
        );
        assert_eq!(
            std::fs::read(&path).unwrap(),
            b"\xef\xbb\xbfold\r\nline\r\n"
        );

        assert!(
            tool.execute(&args, true)
                .unwrap()
                .contains("Successfully overwrote")
        );
        assert_eq!(
            std::fs::read(&path).unwrap(),
            b"\xef\xbb\xbfnew\r\nline\r\n"
        );
        cache.ensure_prior_read(&path, "editing").unwrap();
    }

    #[tokio::test]
    async fn tracks_pre_edit_state_for_write_edit_notebook_and_new_file_paths() {
        let workspace = TempWorkspace::new();
        let write_path = workspace.0.join("write.txt");
        let edit_path = workspace.0.join("edit.txt");
        let notebook_path = workspace.0.join("notebook.ipynb");
        let notebook_before = r#"{"nbformat":4,"nbformat_minor":5,"metadata":{},"cells":[]}"#;
        std::fs::write(&write_path, "before write").unwrap();
        std::fs::write(&edit_path, "before edit").unwrap();
        std::fs::write(&notebook_path, notebook_before).unwrap();

        let cache = FileReadCache::default();
        let reader = ReadFileTool::new_with_cache(&workspace.0, cache.clone()).unwrap();
        for path in [&write_path, &edit_path, &notebook_path] {
            reader.execute(&json!({"file_path":path})).await.unwrap();
        }

        let history_root = workspace.0.join("history");
        let mut history_service =
            FileHistoryService::new(&history_root, "session-a", &workspace.0, true);
        history_service.make_snapshot("turn-1").await;
        let history = Arc::new(Mutex::new(history_service));
        let tool = WriteFileTool::new(&workspace.0, cache)
            .unwrap()
            .with_file_history(history.clone());

        tool.execute(
            &json!({"file_path":write_path,"content":"after write"}),
            true,
        )
        .unwrap();
        tool.execute_edit(&json!({"file_path":edit_path,"content":"after edit"}))
            .unwrap();
        tool.execute_notebook(
            &json!({"file_path":notebook_path,"content":r#"{"nbformat":4,"nbformat_minor":5,"metadata":{},"cells":[{"cell_type":"markdown","source":["after edit"],"metadata":{}}]}"#}),
            true,
            false,
        )
        .unwrap();

        let new_path = workspace.0.join("created.txt");
        tool.execute(&json!({"file_path":new_path,"content":"new file"}), true)
            .unwrap();

        let history = history.try_lock().unwrap();
        let backups = &history.get_snapshots()[0].tracked_file_backups;
        for (path, expected) in [
            ("write.txt", "before write"),
            ("edit.txt", "before edit"),
            ("notebook.ipynb", notebook_before),
        ] {
            let backup = backups.get(path).unwrap_or_else(|| {
                panic!(
                    "pre-edit backup for {path} was not recorded; tracked paths: {:?}",
                    backups.keys().collect::<Vec<_>>()
                )
            });
            let backup_name = backup
                .backup_file_name
                .as_deref()
                .expect("existing file has backup content");
            let backup_path = resolve_backup_path(&history_root, "session-a", backup_name)
                .expect("valid backup path");
            assert_eq!(std::fs::read_to_string(backup_path).unwrap(), expected);
        }
        assert_eq!(
            backups
                .get("created.txt")
                .expect("new file creation marker recorded")
                .backup_file_name,
            None
        );
    }

    #[test]
    fn refuses_unread_existing_files_but_creates_new_nested_files() {
        let workspace = TempWorkspace::new();
        let existing = workspace.0.join("existing.txt");
        std::fs::write(&existing, "old").unwrap();
        let tool = WriteFileTool::new(&workspace.0, FileReadCache::default()).unwrap();
        assert_eq!(
            tool.preview(&json!({"file_path":existing,"content":"new"}))
                .unwrap_err(),
            format!(
                "File {} has not been read in this session. Use the read_file tool first to load the current content (read the full file — overwriting replaces every byte, so any unseen bytes would be discarded) before overwriting it.",
                existing.display()
            )
        );

        let new_file = workspace.0.join("nested/dir/new.txt");
        let args = json!({"file_path":new_file,"content":"created"});
        assert!(tool.preview(&args).unwrap().is_new_file);
        assert!(
            tool.execute(&args, true)
                .unwrap()
                .contains("Successfully created")
        );
        assert_eq!(std::fs::read_to_string(new_file).unwrap(), "created");
    }

    #[test]
    fn rejects_external_symlink_targets_and_oversized_contents() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;
            let workspace = TempWorkspace::new();
            let outside = TempWorkspace::new();
            let target = outside.0.join("outside.txt");
            std::fs::write(&target, "private").unwrap();
            let link = workspace.0.join("link.txt");
            symlink(&target, &link).unwrap();
            let tool = WriteFileTool::new(&workspace.0, FileReadCache::default()).unwrap();
            assert!(
                tool.preview(&json!({"file_path":link,"content":"new"}))
                    .unwrap_err()
                    .contains("inside the workspace")
            );
            assert!(tool
                .preview(&json!({"file_path":workspace.0.join("big.txt"),"content":"x".repeat(MAX_FILE_BYTES as usize + 1)}))
                .unwrap_err()
                .contains("write limit"));
        }
    }

    #[test]
    fn refuses_a_change_when_the_full_diff_cannot_fit_in_the_approval_prompt() {
        let workspace = TempWorkspace::new();
        let path = workspace.0.join("large.txt");
        let tool = WriteFileTool::new(&workspace.0, FileReadCache::default()).unwrap();
        let args = json!({"file_path":path,"content":"new\n".repeat(MAX_DIFF_UTF16_UNITS)});

        assert!(tool.preview(&args).unwrap_err().contains("review safely"));
        assert!(
            tool.execute(&args, true)
                .unwrap_err()
                .contains("review safely")
        );
        assert!(!path.exists());
    }

    #[test]
    fn blocks_potential_secrets_only_when_the_destination_is_team_memory() {
        let workspace = TempWorkspace::new();
        std::fs::create_dir_all(workspace.0.join(".git")).unwrap();
        let cache = FileReadCache::default();
        let tool = WriteFileTool::new(&workspace.0, cache).unwrap();
        let secret = format!("ghp_{}", "a".repeat(36));
        let team_file = workspace.0.join(".canopy/team-memory/notes.md");
        let error = tool
            .preview(&json!({"file_path":team_file,"content":secret}))
            .unwrap_err();
        assert!(error.contains("GitHub PAT"));
        assert!(!error.contains(&format!("ghp_{}", "a".repeat(36))));

        let regular_file = workspace.0.join("notes.md");
        assert!(
            tool.preview(&json!({"file_path":regular_file,"content":secret}))
                .is_ok()
        );
    }
}
