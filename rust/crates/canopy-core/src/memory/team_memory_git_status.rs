//! Checks whether shared team-memory files can be tracked by Git.
//!
//! Port of `packages/core/src/memory/team-memory-git-status.ts`. The probe is
//! injectable so warning behavior can be tested independently of Git, while
//! the default adapter uses `git check-ignore` with the source's five-second
//! timeout.

use std::path::{Component, Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const GIT_TIMEOUT: Duration = Duration::from_secs(5);
const TEAM_DIRECTORY: &str = ".canopy/team-memory";
const TEAM_INDEX: &str = "MEMORY.md";
const REPRESENTATIVE_TOPIC: &str = "feedback.md";

/// The Git ignore check used to assess a representative team-memory file.
///
/// Errors and timeouts should be treated as not ignored, matching the
/// TypeScript helper's best-effort behavior.
pub trait GitIgnoreProbe: Send + Sync {
    fn is_ignored(&self, git_root: &Path, file_path: &Path) -> bool;
}

/// Runs `git check-ignore --quiet -- <path>` and treats every failure as
/// "not ignored". Git's exit code 0 means ignored; exit code 1 means not
/// ignored. The timeout bounds hangs caused by a broken Git installation or
/// repository configuration.
#[derive(Clone, Copy, Debug, Default)]
pub struct ProcessGitIgnoreProbe;

impl GitIgnoreProbe for ProcessGitIgnoreProbe {
    fn is_ignored(&self, git_root: &Path, file_path: &Path) -> bool {
        let Ok(relative_path) = file_path.strip_prefix(git_root) else {
            return false;
        };
        let relative_path = if relative_path.as_os_str().is_empty() {
            Path::new(".")
        } else {
            relative_path
        };

        let Ok(mut child) = Command::new("git")
            .arg("check-ignore")
            .arg("--quiet")
            .arg("--")
            .arg(relative_path)
            .current_dir(git_root)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
        else {
            return false;
        };

        wait_until_timeout(&mut child)
    }
}

fn wait_until_timeout(child: &mut Child) -> bool {
    let deadline = Instant::now() + GIT_TIMEOUT;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return status.success(),
            Ok(None) if Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(10));
            }
            Ok(None) | Err(_) => {
                let _ = child.kill();
                let _ = child.wait();
                return false;
            }
        }
    }
}

/// Returns a warning when team memory is outside a Git repository or when
/// either its index or a representative topic document is ignored.
///
/// Call this only when the team-memory tier is active, as the caller does in
/// the TypeScript runtime.
pub fn get_team_memory_shareability_warning(project_root: &Path) -> Option<String> {
    get_team_memory_shareability_warning_with_probe(project_root, &ProcessGitIgnoreProbe)
}

/// Testable variant of [`get_team_memory_shareability_warning`] that accepts
/// an injected Git ignore probe.
pub fn get_team_memory_shareability_warning_with_probe(
    project_root: &Path,
    probe: &dyn GitIgnoreProbe,
) -> Option<String> {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let project_root = absolute_normalized(project_root, &cwd);
    let Some(git_root) = find_git_root(&project_root) else {
        let team_root = project_root.join(TEAM_DIRECTORY);
        return Some(format!(
            "Team memory is enabled, but {} is not inside a git repository, so saved memories stay local and are not shared with collaborators.",
            team_root.display()
        ));
    };

    let team_root = git_root.join(TEAM_DIRECTORY);
    let index_ignored = probe.is_ignored(&git_root, &team_root.join(TEAM_INDEX));
    let topic_ignored = probe.is_ignored(&git_root, &team_root.join(REPRESENTATIVE_TOPIC));
    if index_ignored || topic_ignored {
        return Some(format!(
            "Team memory is enabled, but {} is git-ignored, so saved memories are not shared. If your .gitignore excludes '.canopy/' (directory form), change it to '.canopy/*' and re-include '.canopy/team-memory/' (and its contents, e.g. '!.canopy/team-memory/**').",
            team_root.display()
        ));
    }

    None
}

fn find_git_root(start_path: &Path) -> Option<PathBuf> {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let mut current = absolute_normalized(start_path, &cwd);
    loop {
        // `.git` may be a directory or a file (for a linked worktree).
        if current.join(".git").exists() {
            return Some(current);
        }
        let parent = current.parent()?;
        if parent == current {
            return None;
        }
        current = parent.to_path_buf();
    }
}

fn absolute_normalized(path: &Path, cwd: &Path) -> PathBuf {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        cwd.join(path)
    };
    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                let popped =
                    normalized.file_name().is_some_and(|name| name != "..") && normalized.pop();
                if !popped && !normalized.has_root() {
                    normalized.push("..");
                }
            }
            Component::Normal(part) => normalized.push(part),
        }
    }
    normalized
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

    struct TempDirectory(PathBuf);

    impl TempDirectory {
        fn new(label: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "canopy-team-git-status-{label}-{}-{}",
                std::process::id(),
                NEXT_TEMP.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[derive(Default)]
    struct RecordingProbe {
        ignored_paths: Vec<PathBuf>,
        checked: std::sync::Mutex<Vec<PathBuf>>,
    }

    impl GitIgnoreProbe for RecordingProbe {
        fn is_ignored(&self, git_root: &Path, file_path: &Path) -> bool {
            self.checked.lock().unwrap().push(file_path.to_path_buf());
            self.ignored_paths
                .iter()
                .any(|ignored| git_root.join(ignored) == file_path)
        }
    }

    #[test]
    fn warns_when_there_is_no_git_repository() {
        let dir = TempDirectory::new("nogit");
        let probe = RecordingProbe::default();
        let warning = get_team_memory_shareability_warning_with_probe(dir.path(), &probe).unwrap();
        assert!(warning.contains("not inside a git repository"));
        assert!(warning.contains(&dir.path().join(TEAM_DIRECTORY).display().to_string()));
        assert!(probe.checked.lock().unwrap().is_empty());
    }

    #[test]
    fn probes_both_index_and_topic_and_warns_if_either_is_ignored() {
        let repo = TempDirectory::new("ignored");
        fs::create_dir(repo.path().join(".git")).unwrap();
        let probe = RecordingProbe {
            ignored_paths: vec![PathBuf::from(TEAM_DIRECTORY).join(REPRESENTATIVE_TOPIC)],
            ..RecordingProbe::default()
        };

        let warning = get_team_memory_shareability_warning_with_probe(repo.path(), &probe).unwrap();
        let checked = probe.checked.lock().unwrap();
        assert!(warning.contains("git-ignored"));
        assert_eq!(checked.len(), 2);
        assert_eq!(
            checked[0],
            repo.path().join(TEAM_DIRECTORY).join(TEAM_INDEX)
        );
        assert_eq!(
            checked[1],
            repo.path().join(TEAM_DIRECTORY).join(REPRESENTATIVE_TOPIC)
        );
    }

    #[test]
    fn returns_none_when_both_paths_are_shareable() {
        let repo = TempDirectory::new("shareable");
        fs::create_dir(repo.path().join(".git")).unwrap();
        assert!(
            get_team_memory_shareability_warning_with_probe(
                repo.path(),
                &RecordingProbe::default()
            )
            .is_none()
        );
    }

    #[test]
    fn git_probe_distinguishes_ignored_and_not_ignored_paths() {
        let repo = TempDirectory::new("real-git");
        let initialized = Command::new("git")
            .arg("init")
            .current_dir(repo.path())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        if !initialized.is_ok_and(|status| status.success()) {
            // Git is optional in some build/test environments.
            return;
        }
        fs::write(repo.path().join(".gitignore"), ".canopy/\n").unwrap();
        let team_root = repo.path().join(TEAM_DIRECTORY);
        let probe = ProcessGitIgnoreProbe;
        assert!(probe.is_ignored(&repo.0, &team_root.join(TEAM_INDEX)));

        fs::write(
            repo.path().join(".gitignore"),
            ".canopy/*\n!.canopy/team-memory/\n!.canopy/team-memory/**\n",
        )
        .unwrap();
        assert!(!probe.is_ignored(&repo.0, &team_root.join(TEAM_INDEX)));
    }
}
