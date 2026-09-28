//! Canonical workspace paths used by bridge ownership checks.

use std::io;
use std::path::{Path, PathBuf};

use crate::storage::normalize_absolute;

pub const MAX_WORKSPACE_PATH_LENGTH: usize = 4096;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkspacePlatform {
    Posix,
    Windows,
}

/// Translate a host Windows path to `/c/...` only for an existing bind mount
/// inside a POSIX container. The caller supplies the existence probe so tests
/// can exercise the mount seam without creating paths at filesystem root.
pub fn translate_windows_workspace_for_posix_sandbox(
    value: &str,
    platform: WorkspacePlatform,
    sandbox_env: Option<&str>,
    exists: impl Fn(&Path) -> bool,
) -> String {
    if platform == WorkspacePlatform::Windows
        || sandbox_env.is_none_or(|value| value.is_empty() || value == "sandbox-exec")
    {
        return value.to_owned();
    }
    let bytes = value.as_bytes();
    if bytes.len() < 3
        || !bytes[0].is_ascii_alphabetic()
        || bytes[1] != b':'
        || !matches!(bytes[2], b'/' | b'\\')
    {
        return value.to_owned();
    }
    let drive = (bytes[0] as char).to_ascii_lowercase();
    let tail = value[3..].replace('\\', "/");
    let translated = PathBuf::from(format!("/{drive}/{tail}"));
    let candidate = normalize_absolute(&translated, Path::new("/"));
    let root = PathBuf::from(format!("/{drive}"));
    if candidate != root && !candidate.starts_with(root.join("")) {
        return value.to_owned();
    }
    if exists(&candidate) {
        format!("/{drive}/{tail}")
    } else {
        value.to_owned()
    }
}

pub fn translate_and_check_absolute_workspace_path(
    raw: &str,
    platform: WorkspacePlatform,
    sandbox_env: Option<&str>,
    exists: impl Fn(&Path) -> bool,
) -> Option<String> {
    let translated =
        translate_windows_workspace_for_posix_sandbox(raw, platform, sandbox_env, exists);
    Path::new(&translated).is_absolute().then_some(translated)
}

pub fn canonicalize_workspace(path: &str) -> io::Result<PathBuf> {
    let platform = if cfg!(windows) {
        WorkspacePlatform::Windows
    } else {
        WorkspacePlatform::Posix
    };
    let translated = translate_windows_workspace_for_posix_sandbox(
        path,
        platform,
        std::env::var("SANDBOX").ok().as_deref(),
        Path::exists,
    );
    let cwd = std::env::current_dir()?;
    let resolved = normalize_absolute(Path::new(&translated), &cwd);
    match std::fs::canonicalize(&resolved) {
        Ok(path) => Ok(path),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(resolved),
        Err(error) => Err(error),
    }
}

pub fn canonicalize_workspaces(paths: &[String]) -> io::Result<Vec<PathBuf>> {
    let mut seen = std::collections::HashSet::new();
    let mut output = Vec::new();
    for path in paths {
        let canonical = canonicalize_workspace(path)?;
        if seen.insert(canonical.clone()) {
            output.push(canonical);
        }
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn windows_host_paths_translate_only_for_existing_safe_mounts() {
        let translated = translate_windows_workspace_for_posix_sandbox(
            r"C:\work\project",
            WorkspacePlatform::Posix,
            Some("docker"),
            |candidate| candidate == Path::new("/c/work/project"),
        );
        assert_eq!(translated, "/c/work/project");

        let missing = translate_windows_workspace_for_posix_sandbox(
            r"C:\missing",
            WorkspacePlatform::Posix,
            Some("docker"),
            |_| false,
        );
        assert_eq!(missing, r"C:\missing");
        let escape = translate_windows_workspace_for_posix_sandbox(
            r"C:\..\etc",
            WorkspacePlatform::Posix,
            Some("docker"),
            |_| true,
        );
        assert_eq!(escape, r"C:\..\etc");
    }

    #[test]
    fn translates_before_absolute_check_and_canonicalizes_missing_paths() {
        assert_eq!(
            translate_and_check_absolute_workspace_path(
                r"D:\work",
                WorkspacePlatform::Posix,
                Some("docker"),
                |_| true,
            ),
            Some("/d/work".to_owned())
        );
        assert!(
            translate_and_check_absolute_workspace_path(
                "relative",
                WorkspacePlatform::Posix,
                None,
                |_| false,
            )
            .is_none()
        );
        let canonical = canonicalize_workspace("/definitely/not/a/real/acp-workspace").unwrap();
        assert_eq!(
            canonical,
            PathBuf::from("/definitely/not/a/real/acp-workspace")
        );
    }
}
