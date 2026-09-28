//! Locate the nearest Git worktree or repository root.
//!
//! Port of `packages/core/src/utils/projectRoot.ts`. The walk is lexical and
//! checks `.git` with `symlink_metadata`, so it accepts directories and files
//! (including worktree markers) without following symlinks.

use std::path::{Component, Path, PathBuf};

/// Walk upward from `start_dir` and return the nearest ancestor whose `.git`
/// entry is a directory or regular file.
///
/// Relative paths are resolved against the process current directory and
/// normalized without filesystem canonicalization. Returns `None` after
/// checking the filesystem root. Filesystem lookup errors are treated like a
/// missing `.git` entry, matching the source helper's continue-up behavior.
pub async fn find_project_root(start_dir: impl AsRef<Path>) -> Option<PathBuf> {
    let mut current = resolve_path(start_dir.as_ref());
    loop {
        let git_path = current.join(".git");
        if let Ok(metadata) = tokio::fs::symlink_metadata(&git_path).await {
            let file_type = metadata.file_type();
            if file_type.is_dir() || file_type.is_file() {
                return Some(current);
            }
        }

        let parent = current.parent()?.to_path_buf();
        if parent == current {
            return None;
        }
        current = parent;
    }
}

fn resolve_path(path: &Path) -> PathBuf {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(path)
    };
    lexical_normalize(&absolute)
}

fn lexical_normalize(path: &Path) -> PathBuf {
    let mut output = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => output.push(prefix.as_os_str()),
            Component::RootDir => output.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                if !output.pop() && !output.is_absolute() {
                    output.push(component.as_os_str());
                }
            }
            Component::Normal(part) => output.push(part),
        }
    }
    output
}
