//! Validation for caller-owned Git worktrees used as agent working directories.
//!
//! Port of `packages/core/src/agents/worktree-pin.ts`. Git access is injected
//! through [`WorktreePinRuntime`], keeping this resolver independent from a
//! particular process runner and letting parity tests run without Git.

use std::io;
use std::path::{Component, Path, PathBuf};

/// The successful result of validating a caller-owned linked worktree.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedWorktreePin {
    /// Canonical absolute path when it exists, otherwise the resolved path.
    pub path: PathBuf,
    /// Best-effort branch label. Empty for detached HEAD or unknown branches.
    pub branch: String,
    /// Last component of `path`.
    pub slug: String,
    /// Repository's main worktree path, with top-level and parent fallbacks.
    pub repo_root: PathBuf,
}

/// Result of the runtime's Git availability check.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GitAvailability {
    pub available: bool,
    pub error: Option<String>,
}

/// Narrow asynchronous seam for the Git and filesystem operations used by
/// worktree pin validation.
///
/// `is_registered_linked_worktree` must fail closed and accept only paths that
/// Git reports as registered linked worktrees of `repo_root`; it must reject
/// the main worktree, stale entries, and worktrees belonging to another
/// repository. `get_registered_worktree_branch` is informational only and
/// returns `None` for detached HEAD. Runtime methods should represent Git
/// failures as false/`None`, matching the TypeScript service's behavior.
#[allow(async_fn_in_trait)]
pub trait WorktreePinRuntime: Send + Sync {
    async fn check_git_available(&self, parent_cwd: &Path) -> GitAvailability;
    async fn is_git_repository(&self, parent_cwd: &Path) -> bool;
    async fn get_main_worktree_path(&self, parent_cwd: &Path) -> Option<PathBuf>;
    async fn get_repo_top_level(&self, parent_cwd: &Path) -> Option<PathBuf>;

    /// Equivalent to `fs.realpath`; on failure the resolver keeps its lexical
    /// absolute path and lets the registry gate produce the user-facing error.
    async fn canonicalize(&self, path: &Path) -> io::Result<PathBuf>;

    async fn is_registered_linked_worktree(&self, repo_root: &Path, path: &Path) -> bool;
    async fn get_registered_worktree_branch(&self, repo_root: &Path, path: &Path)
    -> Option<String>;
}

/// Resolve a model-supplied directory relative to the parent's configured
/// working directory and require it to be a registered linked worktree of
/// the parent's repository. The same canonical path is passed to both Git
/// checks and returned to the caller.
pub async fn resolve_external_worktree_dir<R: WorktreePinRuntime>(
    runtime: &R,
    parent_cwd: &Path,
    working_dir: &str,
    label: &str,
) -> Result<ResolvedWorktreePin, String> {
    let resolved_path = resolve_from_parent(parent_cwd, Path::new(working_dir));

    let git_check = runtime.check_git_available(parent_cwd).await;
    if !git_check.available {
        return Err(format!(
            "Cannot use {label}: {}.",
            git_check.error.as_deref().unwrap_or("git is not available")
        ));
    }

    if !runtime.is_git_repository(parent_cwd).await {
        return Err(format!(
            "Cannot use {label}: {} is not a git repository.",
            parent_cwd.display()
        ));
    }

    // A linked worktree's own top-level is not the main repository root. Use
    // the main tree when available, then mirror the source's top-level and
    // parent-cwd fallbacks.
    let main_tree_path = runtime.get_main_worktree_path(parent_cwd).await;
    let repo_root = match main_tree_path {
        Some(path) => path,
        None => runtime
            .get_repo_top_level(parent_cwd)
            .await
            .unwrap_or_else(|| parent_cwd.to_path_buf()),
    };

    // Thread one resolution through the registry gate, branch lookup, and
    // result. If realpath fails (for example, a missing target), use the
    // resolved spelling as TypeScript does.
    let pinned_path = runtime
        .canonicalize(&resolved_path)
        .await
        .unwrap_or(resolved_path.clone());

    if !runtime
        .is_registered_linked_worktree(&repo_root, &pinned_path)
        .await
    {
        return Err(format!(
            "{label} \"{}\" is not a registered linked worktree of this repository (it is the main working tree, is absent from `git worktree list`, or its git metadata could not be read) — pinning a sub-agent there would not isolate it. Pass a worktree created via `git worktree add`.",
            resolved_path.display()
        ));
    }

    let branch = runtime
        .get_registered_worktree_branch(&repo_root, &pinned_path)
        .await
        .unwrap_or_default();
    let slug = pinned_path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();

    Ok(ResolvedWorktreePin {
        path: pinned_path,
        branch,
        slug,
        repo_root,
    })
}

fn resolve_from_parent(parent_cwd: &Path, working_dir: &Path) -> PathBuf {
    let joined = if working_dir.is_absolute() {
        working_dir.to_path_buf()
    } else {
        parent_cwd.join(working_dir)
    };

    let absolute = if joined.is_absolute() {
        joined
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(joined)
    };
    normalize_absolute(&absolute)
}

fn normalize_absolute(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                // Absolute paths cannot climb above their root. On Windows,
                // `pop` also leaves the drive prefix intact.
                if !normalized.pop() && !normalized.has_root() {
                    normalized.push(component.as_os_str());
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
    use std::sync::Mutex;

    #[derive(Clone, Debug, Eq, PartialEq)]
    enum Call {
        GitAvailable(PathBuf),
        IsRepository(PathBuf),
        MainWorktree(PathBuf),
        TopLevel(PathBuf),
        Canonicalize(PathBuf),
        IsRegistered(PathBuf, PathBuf),
        Branch(PathBuf, PathBuf),
    }

    #[derive(Debug)]
    struct FakeRuntime {
        available: GitAvailability,
        is_repo: bool,
        main_worktree: Option<PathBuf>,
        top_level: Option<PathBuf>,
        canonical_path: Option<PathBuf>,
        registered: bool,
        branch: Option<String>,
        calls: Mutex<Vec<Call>>,
    }

    impl Default for FakeRuntime {
        fn default() -> Self {
            Self {
                available: GitAvailability {
                    available: true,
                    error: None,
                },
                is_repo: true,
                main_worktree: Some(PathBuf::from("/repo")),
                top_level: Some(PathBuf::from("/repo")),
                canonical_path: None,
                registered: true,
                branch: Some("pr-7".into()),
                calls: Mutex::new(Vec::new()),
            }
        }
    }

    impl FakeRuntime {
        fn calls(&self) -> Vec<Call> {
            self.calls.lock().unwrap().clone()
        }

        fn record(&self, call: Call) {
            self.calls.lock().unwrap().push(call);
        }
    }

    impl WorktreePinRuntime for FakeRuntime {
        async fn check_git_available(&self, parent_cwd: &Path) -> GitAvailability {
            self.record(Call::GitAvailable(parent_cwd.to_path_buf()));
            self.available.clone()
        }

        async fn is_git_repository(&self, parent_cwd: &Path) -> bool {
            self.record(Call::IsRepository(parent_cwd.to_path_buf()));
            self.is_repo
        }

        async fn get_main_worktree_path(&self, parent_cwd: &Path) -> Option<PathBuf> {
            self.record(Call::MainWorktree(parent_cwd.to_path_buf()));
            self.main_worktree.clone()
        }

        async fn get_repo_top_level(&self, parent_cwd: &Path) -> Option<PathBuf> {
            self.record(Call::TopLevel(parent_cwd.to_path_buf()));
            self.top_level.clone()
        }

        async fn canonicalize(&self, path: &Path) -> io::Result<PathBuf> {
            self.record(Call::Canonicalize(path.to_path_buf()));
            self.canonical_path.clone().ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, "fake target does not exist")
            })
        }

        async fn is_registered_linked_worktree(&self, repo_root: &Path, path: &Path) -> bool {
            self.record(Call::IsRegistered(
                repo_root.to_path_buf(),
                path.to_path_buf(),
            ));
            self.registered
        }

        async fn get_registered_worktree_branch(
            &self,
            repo_root: &Path,
            path: &Path,
        ) -> Option<String> {
            self.record(Call::Branch(repo_root.to_path_buf(), path.to_path_buf()));
            self.branch.clone()
        }
    }

    #[tokio::test]
    async fn resolves_relative_target_from_parent_and_returns_labels() {
        let runtime = FakeRuntime::default();
        let result = resolve_external_worktree_dir(
            &runtime,
            Path::new("/repo"),
            ".canopy/tmp/review-pr-7",
            "working_dir",
        )
        .await
        .unwrap();

        assert_eq!(
            result,
            ResolvedWorktreePin {
                path: PathBuf::from("/repo/.canopy/tmp/review-pr-7"),
                branch: "pr-7".into(),
                slug: "review-pr-7".into(),
                repo_root: PathBuf::from("/repo"),
            }
        );
    }

    #[tokio::test]
    async fn absolute_target_is_allowed_outside_repository() {
        let runtime = FakeRuntime::default();
        let result = resolve_external_worktree_dir(
            &runtime,
            Path::new("/repo"),
            "/elsewhere/wt",
            "working_dir",
        )
        .await
        .unwrap();

        assert_eq!(result.path, PathBuf::from("/elsewhere/wt"));
        assert_eq!(result.slug, "wt");
        assert_eq!(result.repo_root, PathBuf::from("/repo"));
    }

    #[tokio::test]
    async fn anchors_registry_queries_at_main_tree_from_a_linked_parent() {
        let runtime = FakeRuntime {
            main_worktree: Some(PathBuf::from("/repo")),
            top_level: Some(PathBuf::from("/repo/.canopy/wt-parent")),
            ..FakeRuntime::default()
        };
        let result = resolve_external_worktree_dir(
            &runtime,
            Path::new("/repo/.canopy/wt-parent"),
            "../review-pr-1-base",
            "working_dir",
        )
        .await
        .unwrap();

        assert_eq!(result.path, PathBuf::from("/repo/.canopy/review-pr-1-base"));
        assert!(runtime.calls().contains(&Call::IsRegistered(
            PathBuf::from("/repo"),
            PathBuf::from("/repo/.canopy/review-pr-1-base"),
        )));
    }

    #[tokio::test]
    async fn falls_back_from_main_tree_to_top_level_then_parent() {
        let runtime = FakeRuntime {
            main_worktree: None,
            top_level: Some(PathBuf::from("/repo/top")),
            ..FakeRuntime::default()
        };
        let result =
            resolve_external_worktree_dir(&runtime, Path::new("/repo"), "wt", "working_dir")
                .await
                .unwrap();
        assert_eq!(result.repo_root, PathBuf::from("/repo/top"));
        assert!(
            runtime
                .calls()
                .iter()
                .any(|call| matches!(call, Call::TopLevel(_)))
        );

        let runtime = FakeRuntime {
            main_worktree: None,
            top_level: None,
            ..FakeRuntime::default()
        };
        let result =
            resolve_external_worktree_dir(&runtime, Path::new("/repo"), "wt", "working_dir")
                .await
                .unwrap();
        assert_eq!(result.repo_root, PathBuf::from("/repo"));
    }

    #[tokio::test]
    async fn uses_one_canonical_path_for_both_gates_and_return_value() {
        let canonical = PathBuf::from("/real/repo/wt");
        let runtime = FakeRuntime {
            canonical_path: Some(canonical.clone()),
            ..FakeRuntime::default()
        };
        let result =
            resolve_external_worktree_dir(&runtime, Path::new("/repo"), "link", "working_dir")
                .await
                .unwrap();

        assert_eq!(result.path, canonical);
        assert_eq!(result.slug, "wt");
        let calls = runtime.calls();
        assert_eq!(
            calls
                .iter()
                .filter(|call| matches!(call, Call::Canonicalize(_)))
                .count(),
            1
        );
        assert!(calls.contains(&Call::Canonicalize(PathBuf::from("/repo/link"))));
        assert!(calls.contains(&Call::IsRegistered(
            PathBuf::from("/repo"),
            canonical.clone(),
        )));
        assert!(calls.contains(&Call::Branch(PathBuf::from("/repo"), canonical)));
    }

    #[tokio::test]
    async fn canonicalization_failure_uses_resolved_path_for_registration_gate() {
        let runtime = FakeRuntime {
            registered: false,
            ..FakeRuntime::default()
        };
        let error =
            resolve_external_worktree_dir(&runtime, Path::new("/repo"), "missing", "working_dir")
                .await
                .unwrap_err();

        assert!(error.contains("working_dir \"/repo/missing\""));
        assert!(error.contains("is not a registered linked worktree"));
        assert!(runtime.calls().contains(&Call::IsRegistered(
            PathBuf::from("/repo"),
            PathBuf::from("/repo/missing"),
        )));
    }

    #[tokio::test]
    async fn preflight_errors_name_git_and_parent_repository() {
        let unavailable = FakeRuntime {
            available: GitAvailability {
                available: false,
                error: Some("git not found on PATH".into()),
            },
            ..FakeRuntime::default()
        };
        assert_eq!(
            resolve_external_worktree_dir(&unavailable, Path::new("/repo"), "wt", "workingDir")
                .await
                .unwrap_err(),
            "Cannot use workingDir: git not found on PATH."
        );
        assert_eq!(unavailable.calls().len(), 1);

        let no_repo = FakeRuntime {
            is_repo: false,
            ..FakeRuntime::default()
        };
        assert_eq!(
            resolve_external_worktree_dir(&no_repo, Path::new("/repo"), "wt", "working_dir")
                .await
                .unwrap_err(),
            "Cannot use working_dir: /repo is not a git repository."
        );
        assert_eq!(no_repo.calls().len(), 2);
    }

    #[tokio::test]
    async fn rejects_unregistered_directory_with_source_error_text() {
        let runtime = FakeRuntime {
            registered: false,
            ..FakeRuntime::default()
        };
        assert_eq!(
            resolve_external_worktree_dir(
                &runtime,
                Path::new("/repo"),
                "plain-subdir",
                "working_dir"
            )
            .await
            .unwrap_err(),
            "working_dir \"/repo/plain-subdir\" is not a registered linked worktree of this repository (it is the main working tree, is absent from `git worktree list`, or its git metadata could not be read) — pinning a sub-agent there would not isolate it. Pass a worktree created via `git worktree add`."
        );
    }

    #[tokio::test]
    async fn detached_head_has_no_branch_label_but_still_passes() {
        let runtime = FakeRuntime {
            branch: None,
            ..FakeRuntime::default()
        };
        let result =
            resolve_external_worktree_dir(&runtime, Path::new("/repo"), "wt", "working_dir")
                .await
                .unwrap();
        assert_eq!(result.branch, "");
    }

    #[tokio::test]
    async fn caller_controls_error_parameter_label() {
        let runtime = FakeRuntime {
            registered: false,
            ..FakeRuntime::default()
        };
        let error = resolve_external_worktree_dir(&runtime, Path::new("/repo"), "wt", "workingDir")
            .await
            .unwrap_err();
        assert!(error.starts_with("workingDir \""));
    }

    #[test]
    fn resolves_dot_segments_like_path_resolve() {
        assert_eq!(
            resolve_from_parent(Path::new("/repo/sub"), Path::new("../wt/./review")),
            PathBuf::from("/repo/wt/review")
        );
    }
}
