//! Prevent credentials from being written to repository-shared team memory.
//!
//! Port of `packages/core/src/memory/team-memory-secret-guard.ts`. The path
//! check intentionally runs before content scanning, and diagnostics contain
//! only scanner labels; detected secret values are never returned or logged.

use std::path::Path;

use super::paths::AutoMemoryPaths;
use super::secret_scanner::scan_for_secrets;

/// Return a blocking warning when `file_path` targets team memory and
/// `content` contains a detected secret. Non-team paths return immediately
/// without scanning their contents.
pub fn check_team_memory_secrets(
    file_path: impl AsRef<Path>,
    content: &str,
    paths: &AutoMemoryPaths,
) -> Option<String> {
    if !paths.is_team_auto_memory_path(file_path) {
        return None;
    }

    let matches = scan_for_secrets(content);
    if matches.is_empty() {
        return None;
    }

    let labels = matches
        .iter()
        .map(|found| found.label.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    Some(format!(
        "Content contains potential secrets ({labels}) and cannot be written to team memory. Team memory is shared with all repository collaborators. Remove the sensitive content and try again."
    ))
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::check_team_memory_secrets;
    use crate::memory::paths::{AutoMemoryPaths, MemoryProjectScope};

    fn temp_dir(label: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "canopy-team-memory-secret-guard-{label}-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    fn project_paths(root: &Path) -> AutoMemoryPaths {
        std::fs::create_dir_all(root.join(".git")).unwrap();
        AutoMemoryPaths::new(root, root.join("state"), false, MemoryProjectScope::GitRoot)
    }

    #[test]
    fn blocks_secrets_on_team_paths_without_echoing_secret_values() {
        let temp = temp_dir("block");
        let paths = project_paths(&temp);
        let team_file = paths.team_auto_memory_root().join("feedback/x.md");
        let secret = format!("ghp_{}", "a".repeat(36));

        let warning = check_team_memory_secrets(&team_file, &format!("token={secret}"), &paths)
            .expect("team memory write containing a token must be blocked");
        assert_eq!(
            warning,
            "Content contains potential secrets (GitHub PAT) and cannot be written to team memory. Team memory is shared with all repository collaborators. Remove the sensitive content and try again."
        );
        assert!(!warning.contains(&secret));

        std::fs::remove_dir_all(temp).unwrap();
    }

    #[test]
    fn allows_clean_content_on_team_paths() {
        let temp = temp_dir("clean");
        let paths = project_paths(&temp);
        let team_file = paths.team_auto_memory_root().join("feedback/x.md");

        assert_eq!(
            check_team_memory_secrets(
                &team_file,
                "Use real databases in integration tests.",
                &paths
            ),
            None
        );

        std::fs::remove_dir_all(temp).unwrap();
    }

    #[test]
    fn ignores_secret_content_outside_team_memory() {
        let temp = temp_dir("outside");
        let paths = project_paths(&temp);
        let outside_file = temp.join("src/config.ts");
        let secret = format!("ghp_{}", "a".repeat(36));

        assert_eq!(
            check_team_memory_secrets(&outside_file, &format!("token={secret}"), &paths),
            None
        );

        std::fs::remove_dir_all(temp).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn blocks_secrets_written_through_a_symlink_into_team_memory() {
        let temp = temp_dir("symlink");
        let paths = project_paths(&temp);
        let team_root = paths.team_auto_memory_root();
        std::fs::create_dir_all(&team_root).unwrap();
        let alias = temp.join("alias");
        std::os::unix::fs::symlink(&team_root, &alias).unwrap();
        let secret = format!("ghp_{}", "a".repeat(36));

        assert!(
            check_team_memory_secrets(alias.join("leak.md"), &format!("token={secret}"), &paths)
                .is_some()
        );

        std::fs::remove_dir_all(temp).unwrap();
    }
}
