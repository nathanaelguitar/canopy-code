//! Serialized writes to workspace or global context files.
//!
//! Runtime-specific settings are passed as paths and a configured filename so
//! callers can connect native settings without this module reading process
//! globals. The implementation mirrors
//! `packages/core/src/memory/writeContextFile.ts`.

use std::collections::HashMap;
use std::io;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use futures_util::lock::{Mutex as AsyncMutex, MutexGuard as AsyncMutexGuard};
use thiserror::Error;
use tokio::io::AsyncWriteExt;

pub const MEMORY_SECTION_HEADER: &str = "## Canopy Added Memories";
pub const FILE_LOCK_TIMEOUT_MS: u64 = 30_000;
pub const MAX_EXISTING_FILE_BYTES: u64 = 16 * 1024 * 1024;

static FILE_LOCKS: OnceLock<Mutex<HashMap<PathBuf, Arc<AsyncMutex<()>>>>> = OnceLock::new();

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriteContextFileScope {
    Workspace,
    Global,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriteContextFileMode {
    Append,
    Replace,
}

/// Result from the guard is kept as a message because the caller owns the
/// generation or cancellation error type that the guard represents.
pub type ContextFileCommitGuard<'a> = dyn Fn() -> Result<(), String> + Send + Sync + 'a;

/// Explicit path inputs and write behavior. `context_filename` is the
/// configured filename (for example, `CANOPY.md` or `AGENTS.md`); the same
/// value is used for either scope.
pub struct WriteContextFileOptions<'a> {
    pub scope: WriteContextFileScope,
    pub mode: WriteContextFileMode,
    pub content: &'a str,
    pub project_root: &'a Path,
    pub global_dir: &'a Path,
    pub context_filename: &'a str,
    pub assert_can_commit: Option<&'a ContextFileCommitGuard<'a>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteContextFileResult {
    pub file_path: PathBuf,
    /// UTF-8 bytes written by this call; zero for an append no-op.
    pub bytes_written: usize,
    pub changed: bool,
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error(
    "Workspace memory write at {file_path:?} did not acquire the per-file lock within {timeout_ms}ms"
)]
pub struct WorkspaceMemoryWriteTimeoutError {
    pub file_path: PathBuf,
    pub timeout_ms: u64,
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error(
    "Existing memory file at {file_path:?} is {bytes} bytes, exceeds the {limit}-byte cap for safe append. Trim the file or use mode=replace to overwrite it."
)]
pub struct WorkspaceMemoryFileTooLargeError {
    pub file_path: PathBuf,
    pub bytes: u64,
    pub limit: u64,
}

#[derive(Debug, Error)]
pub enum WriteContextFileError {
    #[error("writeWorkspaceContextFile: projectRoot must be absolute, got {project_root:?}")]
    ProjectRootMustBeAbsolute { project_root: PathBuf },
    #[error(transparent)]
    LockTimeout(#[from] WorkspaceMemoryWriteTimeoutError),
    #[error(transparent)]
    FileTooLarge(#[from] WorkspaceMemoryFileTooLargeError),
    #[error("workspace memory commit guard failed: {0}")]
    CommitGuard(String),
    #[error(transparent)]
    Io(#[from] io::Error),
}

/// Write or append context content while holding a mutex keyed by the resolved
/// file path. The 30-second deadline applies only while acquiring the mutex;
/// once acquired, the read-compose-write transaction runs to completion.
pub async fn write_context_file(
    options: WriteContextFileOptions<'_>,
) -> Result<WriteContextFileResult, WriteContextFileError> {
    if !options.project_root.is_absolute() {
        return Err(WriteContextFileError::ProjectRootMustBeAbsolute {
            project_root: options.project_root.to_path_buf(),
        });
    }

    let file_path = resolve_context_file_path(
        options.scope,
        options.project_root,
        options.global_dir,
        options.context_filename,
    );
    let file_lock = get_file_lock(&file_path);
    let _guard = acquire_file_lock(
        &file_lock,
        &file_path,
        Duration::from_millis(FILE_LOCK_TIMEOUT_MS),
    )
    .await?;
    run_write(&file_path, &options).await
}

fn resolve_context_file_path(
    scope: WriteContextFileScope,
    project_root: &Path,
    global_dir: &Path,
    context_filename: &str,
) -> PathBuf {
    let directory = match scope {
        WriteContextFileScope::Workspace => project_root,
        WriteContextFileScope::Global => global_dir,
    };
    join_like_node(directory, Path::new(context_filename))
}

fn join_like_node(directory: &Path, filename: &Path) -> PathBuf {
    // Node's path.join normalizes a joined path but does not let a later
    // absolute component discard the earlier directory. PathBuf::push does,
    // so append components explicitly and normalize once at the end.
    let mut joined = directory.to_path_buf();
    for component in filename.components() {
        match component {
            Component::Prefix(prefix) => joined.push(prefix.as_os_str()),
            Component::RootDir | Component::CurDir => {}
            Component::ParentDir => joined.push(".."),
            Component::Normal(part) => joined.push(part),
        }
    }
    normalize_path(&joined)
}

fn normalize_path(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                if normalized.file_name().is_some() {
                    normalized.pop();
                } else if !normalized.has_root() {
                    normalized.push("..");
                }
            }
            Component::Normal(part) => normalized.push(part),
        }
    }
    normalized
}

async fn acquire_file_lock<'a>(
    lock: &'a AsyncMutex<()>,
    file_path: &Path,
    timeout: Duration,
) -> Result<AsyncMutexGuard<'a, ()>, WorkspaceMemoryWriteTimeoutError> {
    tokio::time::timeout(timeout, lock.lock())
        .await
        .map_err(|_| WorkspaceMemoryWriteTimeoutError {
            file_path: file_path.to_path_buf(),
            timeout_ms: u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
        })
}

fn get_file_lock(file_path: &Path) -> Arc<AsyncMutex<()>> {
    let locks = FILE_LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut locks = locks
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    locks
        .entry(file_path.to_path_buf())
        .or_insert_with(|| Arc::new(AsyncMutex::new(())))
        .clone()
}

async fn run_write(
    file_path: &Path,
    options: &WriteContextFileOptions<'_>,
) -> Result<WriteContextFileResult, WriteContextFileError> {
    if options.mode == WriteContextFileMode::Append && is_whitespace_only(options.content) {
        return Ok(WriteContextFileResult {
            file_path: file_path.to_path_buf(),
            bytes_written: 0,
            changed: false,
        });
    }

    assert_can_commit(options)?;
    if let Some(parent) = file_path.parent() {
        let parent = if parent.as_os_str().is_empty() {
            Path::new(".")
        } else {
            parent
        };
        tokio::fs::create_dir_all(parent).await?;
    }

    if options.mode == WriteContextFileMode::Replace {
        assert_can_commit(options)?;
        write_file(file_path, options.content.as_bytes()).await?;
        return Ok(WriteContextFileResult {
            file_path: file_path.to_path_buf(),
            bytes_written: options.content.len(),
            changed: true,
        });
    }

    let next = compose_appended_content(file_path, options.content, MEMORY_SECTION_HEADER).await?;
    assert_can_commit(options)?;
    write_file(file_path, next.as_bytes()).await?;
    Ok(WriteContextFileResult {
        file_path: file_path.to_path_buf(),
        bytes_written: next.len(),
        changed: true,
    })
}

fn assert_can_commit(options: &WriteContextFileOptions<'_>) -> Result<(), WriteContextFileError> {
    if let Some(assert_can_commit) = options.assert_can_commit {
        assert_can_commit().map_err(WriteContextFileError::CommitGuard)?;
    }
    Ok(())
}

async fn write_file(path: &Path, contents: &[u8]) -> io::Result<()> {
    let mut options = tokio::fs::OpenOptions::new();
    options.create(true).write(true).truncate(true);
    #[cfg(unix)]
    options.mode(0o644);
    let mut file = options.open(path).await?;
    file.write_all(contents).await
}

async fn compose_appended_content(
    file_path: &Path,
    new_content: &str,
    section_header: &str,
) -> Result<String, WriteContextFileError> {
    let existing = match tokio::fs::metadata(file_path).await {
        Ok(metadata) => {
            if metadata.len() > MAX_EXISTING_FILE_BYTES {
                return Err(WorkspaceMemoryFileTooLargeError {
                    file_path: file_path.to_path_buf(),
                    bytes: metadata.len(),
                    limit: MAX_EXISTING_FILE_BYTES,
                }
                .into());
            }
            match tokio::fs::read(file_path).await {
                Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
                Err(error) if error.kind() == io::ErrorKind::NotFound => String::new(),
                Err(error) => return Err(error.into()),
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => String::new(),
        Err(error) => return Err(error.into()),
    };

    let trimmed = trim_newlines(new_content);
    if trimmed.is_empty() {
        return Ok(existing);
    }

    if existing.is_empty() {
        return Ok(format!("{section_header}\n{trimmed}\n"));
    }

    let existing_utf16 = existing.encode_utf16().collect::<Vec<_>>();
    let header_utf16 = section_header.encode_utf16().collect::<Vec<_>>();
    let Some(section_idx) = find_subslice(&existing_utf16, &header_utf16) else {
        let sep = if existing.ends_with('\n') { "" } else { "\n" };
        return Ok(format!("{existing}{sep}\n{section_header}\n{trimmed}\n"));
    };

    // All positions from this point onward are UTF-16 code-unit offsets to
    // match String.indexOf, charCodeAt, and String.slice in the TypeScript
    // implementation, including when earlier prose contains astral symbols.
    let after_header_idx = section_idx + header_utf16.len();
    let next_header_rel = find_next_top_level_heading(&existing_utf16, after_header_idx);
    let Some(next_header_rel) = next_header_rel else {
        let sep = if existing.ends_with('\n') { "" } else { "\n" };
        return Ok(format!("{existing}{sep}{trimmed}\n"));
    };

    let insert_at = after_header_idx + next_header_rel;
    let before = &existing_utf16[..insert_at];
    let after = &existing_utf16[insert_at..];
    let mut composed =
        Vec::with_capacity(existing_utf16.len() + trimmed.encode_utf16().count() + 1);
    composed.extend_from_slice(before);
    if before.last() != Some(&(b'\n' as u16)) {
        composed.push(b'\n' as u16);
    }
    composed.extend(trimmed.encode_utf16());
    composed.push(b'\n' as u16);
    composed.extend_from_slice(after);
    Ok(String::from_utf16_lossy(&composed))
}

fn find_next_top_level_heading(text: &[u16], start: usize) -> Option<usize> {
    let mut in_fence = false;
    let mut line_start = start;
    for index in start..text.len() {
        if text[index] != b'\n' as u16 {
            continue;
        }
        let next_line_start = index + 1;
        if starts_with_at(text, line_start, b"```") || starts_with_at(text, line_start, b"~~~") {
            in_fence = !in_fence;
        }
        if !in_fence && starts_with_at(text, next_line_start, b"## ") {
            return Some(index - start);
        }
        line_start = next_line_start;
    }
    None
}

fn starts_with_at(text: &[u16], index: usize, ascii_prefix: &[u8]) -> bool {
    let end = index.saturating_add(ascii_prefix.len());
    end <= text.len()
        && text[index..end]
            .iter()
            .zip(ascii_prefix)
            .all(|(left, right)| *left == u16::from(*right))
}

fn find_subslice(haystack: &[u16], needle: &[u16]) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn is_whitespace_only(text: &str) -> bool {
    text.encode_utf16().all(|character| {
        matches!(
            character,
            0x0020 | 0x0009 | 0x000a | 0x000d | 0x000c | 0x000b
        )
    })
}

fn trim_newlines(text: &str) -> &str {
    let bytes = text.as_bytes();
    let mut start = 0;
    let mut end = bytes.len();
    while start < end && bytes[start] == b'\n' {
        start += 1;
    }
    while end > start && bytes[end - 1] == b'\n' {
        end -= 1;
    }
    // Newline is one byte, so trimming the ASCII boundary preserves valid
    // UTF-8 character boundaries for Rust's str slicing.
    &text[start..end]
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn test_root(label: &str) -> PathBuf {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "canopy-write-context-{}-{}-{label}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    fn options<'a>(
        scope: WriteContextFileScope,
        mode: WriteContextFileMode,
        content: &'a str,
        project_root: &'a Path,
        global_dir: &'a Path,
        filename: &'a str,
    ) -> WriteContextFileOptions<'a> {
        WriteContextFileOptions {
            scope,
            mode,
            content,
            project_root,
            global_dir,
            context_filename: filename,
            assert_can_commit: None,
        }
    }

    #[tokio::test]
    async fn creates_context_file_and_uses_explicit_filename() {
        let root = test_root("create");
        let global = root.join("global");
        let result = write_context_file(options(
            WriteContextFileScope::Workspace,
            WriteContextFileMode::Append,
            "- first entry",
            &root,
            &global,
            "AGENTS.md",
        ))
        .await
        .unwrap();
        assert_eq!(result.file_path, root.join("AGENTS.md"));
        assert_eq!(
            tokio::fs::read_to_string(&result.file_path).await.unwrap(),
            format!("{MEMORY_SECTION_HEADER}\n- first entry\n")
        );
        assert_eq!(
            result.bytes_written,
            result.file_path.metadata().unwrap().len() as usize
        );
        assert!(result.changed);
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn appends_inside_existing_section_and_before_the_next_heading() {
        let root = test_root("heading");
        let path = root.join("CANOPY.md");
        let initial = format!("# pre\n\n{MEMORY_SECTION_HEADER}\n- first\n\n## post\nstuff\n");
        tokio::fs::write(&path, &initial).await.unwrap();
        write_context_file(options(
            WriteContextFileScope::Workspace,
            WriteContextFileMode::Append,
            "- second",
            &root,
            &root,
            "CANOPY.md",
        ))
        .await
        .unwrap();
        assert_eq!(
            tokio::fs::read_to_string(&path).await.unwrap(),
            format!("# pre\n\n{MEMORY_SECTION_HEADER}\n- first\n- second\n\n## post\nstuff\n")
        );
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn inserts_section_when_existing_file_has_no_memory_header() {
        let root = test_root("insert-section");
        let path = root.join("CANOPY.md");
        tokio::fs::write(&path, "# project notes\n").await.unwrap();
        write_context_file(options(
            WriteContextFileScope::Workspace,
            WriteContextFileMode::Append,
            "\n\t- entry\n\n",
            &root,
            &root,
            "CANOPY.md",
        ))
        .await
        .unwrap();
        assert_eq!(
            tokio::fs::read_to_string(&path).await.unwrap(),
            format!("# project notes\n\n{MEMORY_SECTION_HEADER}\n\t- entry\n")
        );
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn slices_after_memory_header_using_javascript_utf16_offsets() {
        let root = test_root("astral-offset");
        let path = root.join("CANOPY.md");
        let initial = format!("😀 heading\n{MEMORY_SECTION_HEADER}\n- item\n\n## next\n");
        tokio::fs::write(&path, initial).await.unwrap();
        write_context_file(options(
            WriteContextFileScope::Workspace,
            WriteContextFileMode::Append,
            "- added",
            &root,
            &root,
            "CANOPY.md",
        ))
        .await
        .unwrap();
        assert_eq!(
            tokio::fs::read_to_string(&path).await.unwrap(),
            format!("😀 heading\n{MEMORY_SECTION_HEADER}\n- item\n- added\n\n## next\n")
        );
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn ignores_headings_inside_backtick_and_tilde_fences() {
        let root = test_root("fences");
        let path = root.join("CANOPY.md");
        let initial = format!(
            "{MEMORY_SECTION_HEADER}\n- backticks\n```markdown\n## fake backtick\n```\n- tildes\n~~~markdown\n## fake tilde\n~~~\n\n## real heading\ntail\n"
        );
        tokio::fs::write(&path, initial).await.unwrap();
        write_context_file(options(
            WriteContextFileScope::Workspace,
            WriteContextFileMode::Append,
            "- added",
            &root,
            &root,
            "CANOPY.md",
        ))
        .await
        .unwrap();
        let written = tokio::fs::read_to_string(&path).await.unwrap();
        assert!(written.contains("~~~markdown\n## fake tilde\n~~~\n- added\n\n## real heading"));
        assert!(!written.contains("## fake backtick\n- added"));
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn fences_with_indentation_do_not_toggle_and_tail_sections_append_to_eof() {
        let root = test_root("fence-indent");
        let path = root.join("CANOPY.md");
        let initial = format!("{MEMORY_SECTION_HEADER}\n- item\n    ```\n\n## real heading\n");
        tokio::fs::write(&path, initial).await.unwrap();
        write_context_file(options(
            WriteContextFileScope::Workspace,
            WriteContextFileMode::Append,
            "- added",
            &root,
            &root,
            "CANOPY.md",
        ))
        .await
        .unwrap();
        let written = tokio::fs::read_to_string(&path).await.unwrap();
        assert!(written.contains("- item\n    ```\n- added\n\n## real heading\n"));

        let tail = format!("# pre\n\n{MEMORY_SECTION_HEADER}\n- a\n");
        tokio::fs::write(&path, tail).await.unwrap();
        write_context_file(options(
            WriteContextFileScope::Workspace,
            WriteContextFileMode::Append,
            "\n- b\n\n",
            &root,
            &root,
            "CANOPY.md",
        ))
        .await
        .unwrap();
        assert_eq!(
            tokio::fs::read_to_string(&path).await.unwrap(),
            format!("# pre\n\n{MEMORY_SECTION_HEADER}\n- a\n- b\n")
        );
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn replace_is_verbatim_and_reports_utf8_bytes() {
        let root = test_root("replace");
        let result = write_context_file(options(
            WriteContextFileScope::Workspace,
            WriteContextFileMode::Replace,
            "💚\n",
            &root,
            &root,
            "CANOPY.md",
        ))
        .await
        .unwrap();
        assert_eq!(result.bytes_written, 5);
        assert_eq!(
            tokio::fs::read_to_string(&result.file_path).await.unwrap(),
            "💚\n"
        );
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn global_scope_uses_global_directory_and_noop_does_not_create_anything() {
        let root = test_root("global-noop");
        let global = root.join("global");
        let result = write_context_file(options(
            WriteContextFileScope::Global,
            WriteContextFileMode::Append,
            "\n\t \r\u{000c}\u{000b}",
            &root,
            &global,
            "AGENTS.md",
        ))
        .await
        .unwrap();
        assert_eq!(result.file_path, global.join("AGENTS.md"));
        assert!(!result.changed);
        assert_eq!(result.bytes_written, 0);
        assert!(!global.exists());

        let result = write_context_file(options(
            WriteContextFileScope::Global,
            WriteContextFileMode::Append,
            "- global",
            &root,
            &global,
            "AGENTS.md",
        ))
        .await
        .unwrap();
        assert_eq!(result.file_path, global.join("AGENTS.md"));
        assert_eq!(
            tokio::fs::read_to_string(result.file_path).await.unwrap(),
            format!("{MEMORY_SECTION_HEADER}\n- global\n")
        );
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn whitespace_only_append_skips_commit_guard_and_directory_creation() {
        let root = test_root("noop-guard");
        let nested = root.join("missing").join("deep");
        let calls = AtomicUsize::new(0);
        let guard = || {
            calls.fetch_add(1, Ordering::Relaxed);
            Err("must not run".to_owned())
        };
        let mut options = options(
            WriteContextFileScope::Workspace,
            WriteContextFileMode::Append,
            "\n\n\t",
            &nested,
            &root,
            "CANOPY.md",
        );
        options.assert_can_commit = Some(&guard);
        let result = write_context_file(options).await.unwrap();
        assert!(!result.changed);
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        assert!(!nested.exists());
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn concurrent_appends_do_not_lose_entries_or_duplicate_the_header() {
        let root = test_root("concurrent");
        let global = root.join("global");
        let contents = (0..10)
            .map(|index| format!("- entry {index}"))
            .collect::<Vec<_>>();
        let writes = contents.iter().map(|content| {
            write_context_file(options(
                WriteContextFileScope::Workspace,
                WriteContextFileMode::Append,
                content,
                &root,
                &global,
                "CANOPY.md",
            ))
        });
        let results = futures_util::future::join_all(writes).await;
        assert!(
            results
                .iter()
                .all(|result| result.as_ref().unwrap().changed)
        );
        let path = root.join("CANOPY.md");
        let written = tokio::fs::read_to_string(&path).await.unwrap();
        for index in 0..10 {
            assert!(written.contains(&format!("- entry {index}")));
        }
        assert_eq!(written.matches(MEMORY_SECTION_HEADER).count(), 1);
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn existing_file_size_guard_fires_before_append() {
        let root = test_root("large");
        let path = root.join("CANOPY.md");
        tokio::fs::write(&path, vec![b'x'; MAX_EXISTING_FILE_BYTES as usize + 1])
            .await
            .unwrap();
        let error = write_context_file(options(
            WriteContextFileScope::Workspace,
            WriteContextFileMode::Append,
            "- entry",
            &root,
            &root,
            "CANOPY.md",
        ))
        .await
        .unwrap_err();
        match error {
            WriteContextFileError::FileTooLarge(error) => {
                assert_eq!(error.bytes, MAX_EXISTING_FILE_BYTES + 1);
                assert_eq!(error.limit, MAX_EXISTING_FILE_BYTES);
            }
            other => panic!("expected size error, got {other:?}"),
        }
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn commit_guard_runs_before_mutation_and_again_immediately_before_write() {
        let root = test_root("commit-guard");
        let calls = AtomicUsize::new(0);
        let guard = || {
            if calls.fetch_add(1, Ordering::Relaxed) == 1 {
                Err("generation closed".to_owned())
            } else {
                Ok(())
            }
        };
        let mut options = options(
            WriteContextFileScope::Workspace,
            WriteContextFileMode::Replace,
            "replacement\n",
            &root,
            &root,
            "CANOPY.md",
        );
        options.assert_can_commit = Some(&guard);
        let error = write_context_file(options).await.unwrap_err();
        assert!(
            matches!(error, WriteContextFileError::CommitGuard(message) if message == "generation closed")
        );
        assert_eq!(calls.load(Ordering::Relaxed), 2);
        assert!(!root.join("CANOPY.md").exists());
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn append_commit_guard_runs_again_after_composition_before_write() {
        let root = test_root("append-commit-guard");
        let path = root.join("CANOPY.md");
        let original = format!("{MEMORY_SECTION_HEADER}\n- original\n");
        tokio::fs::write(&path, &original).await.unwrap();
        let calls = AtomicUsize::new(0);
        let guard = || {
            if calls.fetch_add(1, Ordering::Relaxed) == 1 {
                Err("generation closed".to_owned())
            } else {
                Ok(())
            }
        };
        let mut options = options(
            WriteContextFileScope::Workspace,
            WriteContextFileMode::Append,
            "- appended",
            &root,
            &root,
            "CANOPY.md",
        );
        options.assert_can_commit = Some(&guard);
        let error = write_context_file(options).await.unwrap_err();
        assert!(
            matches!(error, WriteContextFileError::CommitGuard(message) if message == "generation closed")
        );
        assert_eq!(calls.load(Ordering::Relaxed), 2);
        assert_eq!(tokio::fs::read_to_string(path).await.unwrap(), original);
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn lock_acquisition_uses_typed_timeout_error() {
        let root = test_root("timeout");
        let path = root.join("CANOPY.md");
        let lock = get_file_lock(&path);
        let held = lock.lock().await;
        let error = acquire_file_lock(&lock, &path, Duration::from_millis(1))
            .await
            .err()
            .unwrap();
        assert_eq!(error.file_path, path);
        assert_eq!(error.timeout_ms, 1);
        drop(held);
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn rejects_relative_project_root_even_for_global_scope() {
        let root = test_root("relative");
        let error = write_context_file(options(
            WriteContextFileScope::Global,
            WriteContextFileMode::Append,
            "x",
            Path::new("relative/root"),
            &root,
            "CANOPY.md",
        ))
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            WriteContextFileError::ProjectRootMustBeAbsolute { .. }
        ));
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[test]
    fn javascript_utf16_helpers_find_heading_after_astral_prefix() {
        let text = format!("😀{MEMORY_SECTION_HEADER}\nitem\n## next\n");
        let units = text.encode_utf16().collect::<Vec<_>>();
        let header = MEMORY_SECTION_HEADER.encode_utf16().collect::<Vec<_>>();
        let header_at = find_subslice(&units, &header).unwrap();
        assert_eq!(header_at, 2);
        assert_eq!(
            find_next_top_level_heading(&units, header_at + header.len()),
            Some("\nitem".encode_utf16().count())
        );
    }
}
