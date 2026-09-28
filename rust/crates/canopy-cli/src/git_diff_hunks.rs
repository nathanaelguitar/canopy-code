//! Bounded, read-only retrieval of one workspace file's diff hunks.
//!
//! This is the native counterpart of `fetchGitDiffHunksForFile`. It is kept
//! separate from the HTTP transport so other Rust surfaces can reuse it.

use std::ffi::CString;
use std::fs::{self, File, OpenOptions};
use std::io::Read;
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use serde::Serialize;

const GIT_PROCESS_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_GIT_OUTPUT_BYTES: usize = 64 * 1024 * 1024;
const MAX_DIFF_SIZE_BYTES: usize = 1_000_000;
const MAX_LINES_PER_FILE: usize = 400;
const BINARY_SNIFF_BYTES: usize = 8 * 1024;

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GitDiffHunk {
    pub(crate) old_start: usize,
    pub(crate) old_lines: usize,
    pub(crate) new_start: usize,
    pub(crate) new_lines: usize,
    pub(crate) lines: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct GitDiffFileHunks {
    pub(crate) hunks: Vec<GitDiffHunk>,
    pub(crate) truncated: bool,
}

/// Fetch changed-file hunks relative to `HEAD`.
///
/// `file_path` and `old_path` may be repository-relative paths or absolute
/// paths inside the worktree. Paths with traversal, a drive prefix, or an
/// absolute path outside the repository are rejected. An invalid optional
/// `old_path` is ignored, matching the TypeScript helper's rename fallback.
/// Returns `None` for non-repositories, transient Git operations, failed or
/// oversized Git commands, unchanged/binary tracked files, and unreadable or
/// binary untracked files. Untracked files are synthesized as one all-added
/// hunk and are opened without following any path-component symlink on Unix.
pub(crate) fn fetch_git_diff_hunks_for_file(
    cwd: &Path,
    file_path: &str,
    old_path: Option<&str>,
) -> Option<GitDiffFileHunks> {
    let git_root = find_git_root(cwd)?;
    let relative_path = to_repo_relative_path(&git_root, file_path)?;
    if is_in_transient_git_state(&git_root) {
        return None;
    }

    let old_relative_path = old_path.and_then(|path| to_repo_relative_path(&git_root, path));
    let mut args = vec![
        "--no-optional-locks".to_owned(),
        "diff".to_owned(),
        "--no-ext-diff".to_owned(),
        "--no-textconv".to_owned(),
    ];
    if old_relative_path.is_some() {
        args.push("-M".to_owned());
    }
    args.push("HEAD".to_owned());
    args.push("--".to_owned());
    if let Some(old_path) = &old_relative_path {
        args.push(literal_pathspec(old_path)?);
    }
    args.push(literal_pathspec(&relative_path)?);

    let diff_output = run_git(&git_root, &args)?;
    if let Some(parsed) = parse_single_file_diff(&diff_output) {
        return Some(parsed);
    }

    // `git diff HEAD` omits untracked files. Ask Git for the exact literal
    // path with standard excludes so ignored files are not synthesized.
    let untracked_args = vec![
        "--no-optional-locks".to_owned(),
        "ls-files".to_owned(),
        "--others".to_owned(),
        "--exclude-standard".to_owned(),
        "-z".to_owned(),
        "--".to_owned(),
        literal_pathspec(&relative_path)?,
    ];
    let untracked_output = run_git(&git_root, &untracked_args)?;
    if untracked_output.is_empty() {
        return None;
    }
    synthesize_untracked_hunk(&git_root, &relative_path)
}

fn find_git_root(cwd: &Path) -> Option<PathBuf> {
    let mut current = fs::canonicalize(cwd).ok()?;
    for _ in 0..1024 {
        if fs::metadata(current.join(".git")).is_ok() {
            return Some(current);
        }
        let parent = current.parent()?;
        if parent == current {
            return None;
        }
        current = parent.to_path_buf();
    }
    None
}

fn to_repo_relative_path(git_root: &Path, input: &str) -> Option<PathBuf> {
    if input.is_empty() || input.contains('\0') {
        return None;
    }
    if !Path::new(input).is_absolute() {
        if input.starts_with('/')
            || input.starts_with('\\')
            || (input
                .as_bytes()
                .first()
                .is_some_and(u8::is_ascii_alphabetic)
                && input.as_bytes().get(1) == Some(&b':'))
            || input.split(['/', '\\']).any(|segment| segment == "..")
        {
            return None;
        }
        let relative = PathBuf::from(input);
        if relative.as_os_str().is_empty()
            || relative
                .components()
                .any(|component| matches!(component, Component::ParentDir | Component::RootDir))
        {
            return None;
        }
        return Some(relative);
    }

    let absolute = Path::new(input);
    let relative = absolute.strip_prefix(git_root).ok()?;
    if relative.as_os_str().is_empty()
        || relative
            .components()
            .any(|component| matches!(component, Component::ParentDir | Component::RootDir))
    {
        return None;
    }
    Some(relative.to_path_buf())
}

fn literal_pathspec(path: &Path) -> Option<String> {
    Some(format!(":(literal){}", path.to_str()?))
}

fn is_in_transient_git_state(git_root: &Path) -> bool {
    let dot_git = git_root.join(".git");
    let metadata = match fs::metadata(&dot_git) {
        Ok(metadata) => metadata,
        Err(_) => return false,
    };
    let git_dir = if metadata.is_dir() {
        dot_git
    } else if metadata.is_file() {
        let Some(line) = read_bounded_metadata_line(&dot_git) else {
            return false;
        };
        let Some(raw_path) = line.strip_prefix("gitdir:") else {
            return false;
        };
        let raw_path = raw_path.trim();
        if raw_path.is_empty() {
            return false;
        }
        let path = PathBuf::from(raw_path);
        if path.is_absolute() {
            path
        } else {
            git_root.join(path)
        }
    } else {
        return false;
    };

    [
        "MERGE_HEAD",
        "CHERRY_PICK_HEAD",
        "REVERT_HEAD",
        "rebase-merge",
        "rebase-apply",
    ]
    .iter()
    .any(|name| fs::metadata(git_dir.join(name)).is_ok())
}

fn read_bounded_metadata_line(path: &Path) -> Option<String> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_NONBLOCK);
    }
    let file = options.open(path).ok()?;
    if !file.metadata().ok()?.is_file() {
        return None;
    }
    let mut bytes = Vec::with_capacity(256);
    file.take(4097).read_to_end(&mut bytes).ok()?;
    if bytes.len() > 4096 {
        return None;
    }
    let line = bytes.split(|byte| *byte == b'\n').next()?;
    Some(String::from_utf8_lossy(line).into_owned())
}

fn run_git(git_root: &Path, args: &[String]) -> Option<Vec<u8>> {
    let mut child = Command::new("git")
        .arg("-c")
        .arg("core.quotepath=false")
        .args(args)
        .current_dir(git_root)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    let stdout = child.stdout.take()?;
    let exceeded_limit = Arc::new(AtomicBool::new(false));
    let reader_exceeded_limit = Arc::clone(&exceeded_limit);
    let reader = match thread::Builder::new()
        .name("canopy-git-diff-reader".to_owned())
        .spawn(move || {
            let mut stdout = stdout;
            let mut output = Vec::with_capacity(64 * 1024);
            let mut buffer = [0_u8; 16 * 1024];
            loop {
                let read = match stdout.read(&mut buffer) {
                    Ok(read) => read,
                    Err(_) => return None,
                };
                if read == 0 {
                    return Some(output);
                }
                if output.len().saturating_add(read) > MAX_GIT_OUTPUT_BYTES {
                    reader_exceeded_limit.store(true, Ordering::Release);
                    return None;
                }
                output.extend_from_slice(&buffer[..read]);
            }
        }) {
        Ok(reader) => reader,
        Err(_) => {
            let _ = child.kill();
            let _ = child.wait();
            return None;
        }
    };

    let deadline = Instant::now() + GIT_PROCESS_TIMEOUT;
    let status = loop {
        if exceeded_limit.load(Ordering::Acquire) {
            let _ = child.kill();
            let _ = child.wait();
            break None;
        }
        match child.try_wait() {
            Ok(Some(status)) => break Some(status),
            Ok(None) if Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(10));
            }
            Ok(None) | Err(_) => {
                let _ = child.kill();
                let _ = child.wait();
                break None;
            }
        }
    };
    let output = reader.join().ok().flatten()?;
    if !status?.success() || exceeded_limit.load(Ordering::Acquire) {
        return None;
    }
    Some(output)
}

fn parse_single_file_diff(output: &[u8]) -> Option<GitDiffFileHunks> {
    if output.is_empty() {
        return None;
    }
    let text = String::from_utf8_lossy(output);
    // The pathspec is literal and names one path, so Git can return at most
    // one file block. Scan it without collecting every line into a Vec: a
    // diff containing millions of short lines must not multiply memory use.
    let start = text.match_indices("diff --git ").find_map(|(index, _)| {
        (index == 0 || text.as_bytes()[index - 1] == b'\n').then_some(index)
    })?;
    let tail = &text[start..];
    let end = tail
        .match_indices("\ndiff --git ")
        .next()
        .map_or(tail.len(), |(index, _)| index + 1);
    let block = &tail[..end];
    if block.len().saturating_sub("diff --git ".len()) > MAX_DIFF_SIZE_BYTES {
        return None;
    }
    parse_hunk_lines(block.split('\n').skip(1))
}

fn parse_hunk_lines<'a>(lines: impl IntoIterator<Item = &'a str>) -> Option<GitDiffFileHunks> {
    let mut hunks = Vec::new();
    let mut current_hunk: Option<GitDiffHunk> = None;
    let mut content_line_count = 0_usize;
    let mut truncated = false;

    for line in lines {
        if let Some((old_start, old_lines, new_start, new_lines)) = parse_hunk_header(line) {
            if let Some(previous) = current_hunk.take() {
                hunks.push(previous);
            }
            current_hunk = Some(GitDiffHunk {
                old_start,
                old_lines,
                new_start,
                new_lines,
                lines: Vec::new(),
            });
            continue;
        }
        let Some(hunk) = current_hunk.as_mut() else {
            continue;
        };
        if line.starts_with('+') || line.starts_with('-') || line.starts_with(' ') {
            if content_line_count >= MAX_LINES_PER_FILE {
                truncated = true;
                break;
            }
            hunk.lines.push((*line).to_owned());
            content_line_count += 1;
        } else if line.starts_with('\\') {
            // Git's no-final-newline marker is rendered by the viewer and does
            // not count against the content-line cap.
            hunk.lines.push((*line).to_owned());
        }
    }
    if let Some(hunk) = current_hunk {
        hunks.push(hunk);
    }
    (!hunks.is_empty()).then_some(GitDiffFileHunks { hunks, truncated })
}

fn parse_hunk_header(line: &str) -> Option<(usize, usize, usize, usize)> {
    let header = line.strip_prefix("@@ -")?;
    let (old_range, rest) = header.split_once(" +")?;
    let (new_range, _) = rest.split_once(" @@")?;
    let (old_start, old_lines) = parse_range(old_range)?;
    let (new_start, new_lines) = parse_range(new_range)?;
    Some((old_start, old_lines, new_start, new_lines))
}

fn parse_range(range: &str) -> Option<(usize, usize)> {
    let (start, lines) = match range.split_once(',') {
        Some((start, lines)) => (start.parse().ok()?, lines.parse().ok()?),
        None => (range.parse().ok()?, 1),
    };
    Some((start, lines))
}

fn synthesize_untracked_hunk(git_root: &Path, relative_path: &Path) -> Option<GitDiffFileHunks> {
    let file = open_untracked_file(git_root, relative_path)?;
    let metadata = file.metadata().ok()?;
    if !metadata.is_file() {
        return None;
    }

    // Read one extra byte to catch growth after metadata was sampled, while
    // keeping allocation and work bounded to the documented 1 MB cap.
    let mut bytes = Vec::with_capacity(MAX_DIFF_SIZE_BYTES.min(metadata.len() as usize));
    file.take((MAX_DIFF_SIZE_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .ok()?;
    if bytes[..bytes.len().min(BINARY_SNIFF_BYTES)].contains(&0) {
        return None;
    }
    let was_over_byte_cap =
        metadata.len() > MAX_DIFF_SIZE_BYTES as u64 || bytes.len() > MAX_DIFF_SIZE_BYTES;
    bytes.truncate(MAX_DIFF_SIZE_BYTES);
    let text = String::from_utf8_lossy(&bytes);
    let mut lines = text.split_terminator('\n');
    let mut added_lines = Vec::with_capacity(MAX_LINES_PER_FILE);
    for _ in 0..MAX_LINES_PER_FILE {
        let Some(line) = lines.next() else {
            break;
        };
        added_lines.push(format!("+{line}"));
    }
    let was_over_line_cap = lines.next().is_some();
    let truncated = was_over_byte_cap || was_over_line_cap;
    if added_lines.is_empty() {
        return Some(GitDiffFileHunks {
            hunks: Vec::new(),
            truncated,
        });
    }
    let line_count = added_lines.len();
    Some(GitDiffFileHunks {
        hunks: vec![GitDiffHunk {
            old_start: 0,
            old_lines: 0,
            new_start: 1,
            new_lines: line_count,
            lines: added_lines,
        }],
        truncated,
    })
}

#[cfg(unix)]
fn open_untracked_file(git_root: &Path, relative_path: &Path) -> Option<File> {
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::OpenOptionsExt;

    let mut root_options = OpenOptions::new();
    root_options
        .read(true)
        .custom_flags(nix::libc::O_DIRECTORY | nix::libc::O_NOFOLLOW | nix::libc::O_CLOEXEC);
    let mut directory = root_options.open(git_root).ok()?;
    let components: Vec<_> = relative_path
        .components()
        .filter_map(|component| match component {
            Component::Normal(part) => Some(part.to_os_string()),
            Component::CurDir => None,
            _ => Some(PathBuf::from("..").into_os_string()),
        })
        .collect();
    if components.is_empty() {
        return None;
    }

    for (index, component) in components.iter().enumerate() {
        if component == ".." {
            return None;
        }
        let name = CString::new(component.as_bytes()).ok()?;
        let is_file = index + 1 == components.len();
        let mut flags = nix::libc::O_RDONLY | nix::libc::O_NOFOLLOW | nix::libc::O_CLOEXEC;
        if is_file {
            flags |= nix::libc::O_NONBLOCK;
        } else {
            flags |= nix::libc::O_DIRECTORY;
        }
        // Each directory is opened relative to the prior verified directory
        // descriptor, so symlink replacement cannot redirect the file read.
        let descriptor = unsafe { nix::libc::openat(directory.as_raw_fd(), name.as_ptr(), flags) };
        if descriptor < 0 {
            return None;
        }
        let opened = unsafe { File::from_raw_fd(descriptor) };
        if is_file {
            return opened.metadata().ok()?.is_file().then_some(opened);
        }
        directory = opened;
    }
    None
}

#[cfg(not(unix))]
fn open_untracked_file(git_root: &Path, relative_path: &Path) -> Option<File> {
    let mut current = git_root.to_path_buf();
    let components: Vec<_> = relative_path.components().collect();
    for (index, component) in components.iter().enumerate() {
        let Component::Normal(part) = component else {
            return None;
        };
        current.push(part);
        let metadata = fs::symlink_metadata(&current).ok()?;
        if metadata.file_type().is_symlink() {
            return None;
        }
        if index + 1 < components.len() && !metadata.is_dir() {
            return None;
        }
        if index + 1 == components.len() && !metadata.is_file() {
            return None;
        }
    }
    let parent = current.parent()?;
    if !fs::canonicalize(parent).ok()?.starts_with(git_root) {
        return None;
    }
    let mut options = OpenOptions::new();
    options.read(true);
    options.open(current).ok()
}
