use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use crate::storage::Storage;

pub const AUTO_MEMORY_DIRNAME: &str = "memory";
pub const AUTO_MEMORY_INDEX_FILENAME: &str = "MEMORY.md";
pub const AUTO_MEMORY_PINNED_DIRNAME: &str = "pinned";
pub const AUTO_MEMORY_METADATA_FILENAME: &str = "meta.json";
pub const AUTO_MEMORY_EXTRACT_CURSOR_FILENAME: &str = "extract-cursor.json";
pub const AUTO_MEMORY_CONSOLIDATION_LOCK_FILENAME: &str = "consolidation.lock";
pub const USER_AUTO_MEMORY_DIRNAME: &str = "memories";
pub const TEAM_AUTO_MEMORY_DIRNAME: &str = "team-memory";
pub const MEMORY_PROJECT_SCOPES: [&str; 2] = ["git-root", "workspace"];

static WARNED_UNKNOWN_PROJECT_SCOPE: AtomicBool = AtomicBool::new(false);

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum MemoryProjectScope {
    #[default]
    GitRoot,
    Workspace,
}

impl MemoryProjectScope {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::GitRoot => "git-root",
            Self::Workspace => "workspace",
        }
    }
}

/// Inputs that used to come from `process.env`, `Storage`, and the process cwd.
/// `from_process` connects these to the native runtime; `from_inputs` keeps
/// path semantics testable without mutating process-global environment state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemoryPathInputs {
    pub project_root: PathBuf,
    pub runtime_base_dir: PathBuf,
    pub memory_base_dir_override: Option<String>,
    pub memory_local: bool,
    pub project_scope: Option<String>,
    pub cwd: PathBuf,
    pub home_dir: Option<PathBuf>,
}

impl MemoryPathInputs {
    pub fn from_process(project_root: impl Into<PathBuf>) -> Self {
        Self {
            project_root: project_root.into(),
            runtime_base_dir: Storage::get_runtime_base_dir(),
            memory_base_dir_override: std::env::var("CANOPY_CODE_MEMORY_BASE_DIR").ok(),
            memory_local: std::env::var("CANOPY_CODE_MEMORY_LOCAL").as_deref() == Ok("1"),
            project_scope: std::env::var("CANOPY_CODE_MEMORY_PROJECT_SCOPE").ok(),
            cwd: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            home_dir: std::env::var_os(if cfg!(windows) { "USERPROFILE" } else { "HOME" })
                .map(PathBuf::from),
        }
    }

    pub fn paths(self) -> AutoMemoryPaths {
        let memory_base_dir = self
            .memory_base_dir_override
            .as_deref()
            .filter(|value| !value.is_empty())
            .map(|value| resolve_configured_path(value, &self.cwd, self.home_dir.as_deref()))
            .unwrap_or_else(|| absolute_normalized(&self.runtime_base_dir, &self.cwd));

        let (project_scope, warn_value) =
            resolve_memory_project_scope(self.project_scope.as_deref());
        if let Some(raw) = warn_value {
            if !WARNED_UNKNOWN_PROJECT_SCOPE.swap(true, Ordering::Relaxed) {
                eprintln!(
                    "[canopy-code] Ignoring unrecognized CANOPY_CODE_MEMORY_PROJECT_SCOPE=\"{raw}\"; falling back to \"git-root\". Expected \"git-root\" or \"workspace\"."
                );
            }
        }

        AutoMemoryPaths {
            project_root: self.project_root,
            memory_base_dir,
            memory_local: self.memory_local,
            project_scope,
            cwd: self.cwd,
        }
    }
}

/// Resolve the project partition. Only `workspace` opts into workspace
/// partitioning. The optional second result is the unrecognized, nonempty
/// value that the process-backed adapter should warn about.
pub fn resolve_memory_project_scope(value: Option<&str>) -> (MemoryProjectScope, Option<String>) {
    let Some(value) = value else {
        return (MemoryProjectScope::GitRoot, None);
    };
    let normalized = value.trim_matches(is_js_whitespace).to_lowercase();
    match normalized.as_str() {
        "workspace" => (MemoryProjectScope::Workspace, None),
        "git-root" | "" => (MemoryProjectScope::GitRoot, None),
        _ => (MemoryProjectScope::GitRoot, Some(value.to_owned())),
    }
}

/// Reset the source-compatible one-time warning state. There is no root cache
/// in Rust: path resolution is scoped to this immutable settings value.
pub fn clear_auto_memory_root_cache() {
    WARNED_UNKNOWN_PROJECT_SCOPE.store(false, Ordering::Relaxed);
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AutoMemoryPaths {
    project_root: PathBuf,
    memory_base_dir: PathBuf,
    memory_local: bool,
    project_scope: MemoryProjectScope,
    cwd: PathBuf,
}

impl AutoMemoryPaths {
    pub fn new(
        project_root: impl Into<PathBuf>,
        memory_base_dir: impl Into<PathBuf>,
        memory_local: bool,
        project_scope: MemoryProjectScope,
    ) -> Self {
        let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
        Self {
            project_root: project_root.into(),
            memory_base_dir: absolute_normalized(&memory_base_dir.into(), &cwd),
            memory_local,
            project_scope,
            cwd,
        }
    }

    pub fn from_inputs(inputs: MemoryPathInputs) -> Self {
        inputs.paths()
    }

    pub fn from_process(project_root: impl Into<PathBuf>) -> Self {
        MemoryPathInputs::from_process(project_root).paths()
    }

    pub fn project_root(&self) -> &Path {
        &self.project_root
    }

    pub fn memory_base_dir(&self) -> &Path {
        &self.memory_base_dir
    }

    pub fn project_scope(&self) -> MemoryProjectScope {
        self.project_scope
    }

    pub fn is_local(&self) -> bool {
        self.memory_local
    }

    pub fn auto_memory_root(&self) -> PathBuf {
        if self.memory_local {
            return join_like_node(
                &self.project_root,
                Path::new(&format!(".canopy/{AUTO_MEMORY_DIRNAME}")),
            );
        }

        let resolved_project = absolute_normalized(&self.project_root, &self.cwd);
        let project_key = match self.project_scope {
            MemoryProjectScope::Workspace => resolved_project,
            MemoryProjectScope::GitRoot => {
                find_git_root(&resolved_project).unwrap_or(resolved_project)
            }
        };
        self.memory_base_dir
            .join("projects")
            .join(sanitize_memory_project_key(&project_key, cfg!(windows)))
            .join(AUTO_MEMORY_DIRNAME)
    }

    /// User-wide memory is shared across every project and lives below the
    /// resolved Canopy runtime/memory base.
    pub fn user_auto_memory_root(&self) -> PathBuf {
        self.memory_base_dir.join(USER_AUTO_MEMORY_DIRNAME)
    }

    /// Shared team memory lives in the active worktree, regardless of private
    /// project memory's local/shared setting.
    pub fn team_auto_memory_root(&self) -> PathBuf {
        let project_root = absolute_normalized(&self.project_root, &self.cwd);
        find_git_root(&project_root)
            .unwrap_or(project_root)
            .join(".canopy")
            .join(TEAM_AUTO_MEMORY_DIRNAME)
    }

    /// Trusted anchor for writes into project memory. The repo-controlled
    /// `.canopy/memory` suffix remains outside the canonicalized anchor.
    pub fn auto_memory_trusted_anchor(&self) -> &Path {
        if self.memory_local {
            &self.project_root
        } else {
            &self.memory_base_dir
        }
    }

    pub fn auto_memory_project_state_dir(&self) -> PathBuf {
        self.auto_memory_root()
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf()
    }

    pub fn auto_memory_index_path(&self) -> PathBuf {
        self.auto_memory_root().join(AUTO_MEMORY_INDEX_FILENAME)
    }

    pub fn auto_memory_metadata_path(&self) -> PathBuf {
        self.auto_memory_project_state_dir()
            .join(AUTO_MEMORY_METADATA_FILENAME)
    }

    pub fn auto_memory_extract_cursor_path(&self) -> PathBuf {
        self.auto_memory_project_state_dir()
            .join(AUTO_MEMORY_EXTRACT_CURSOR_FILENAME)
    }

    pub fn auto_memory_consolidation_lock_path(&self) -> PathBuf {
        self.auto_memory_project_state_dir()
            .join(AUTO_MEMORY_CONSOLIDATION_LOCK_FILENAME)
    }

    pub fn auto_memory_topic_path(&self, memory_type: super::AutoMemoryType) -> PathBuf {
        self.auto_memory_root()
            .join(get_auto_memory_topic_filename(memory_type))
    }

    /// Mirrors `path.join(root, relativePath)`; Node's path.join treats a
    /// leading slash in a later segment as root-relative to the first segment.
    pub fn auto_memory_file_path(&self, relative_path: impl AsRef<Path>) -> PathBuf {
        join_like_node(&self.auto_memory_root(), relative_path.as_ref())
    }

    pub fn user_auto_memory_index_path(&self) -> PathBuf {
        self.user_auto_memory_root()
            .join(AUTO_MEMORY_INDEX_FILENAME)
    }

    pub fn user_auto_memory_topic_path(&self, memory_type: super::AutoMemoryType) -> PathBuf {
        self.user_auto_memory_root()
            .join(get_auto_memory_topic_filename(memory_type))
    }

    pub fn team_auto_memory_index_path(&self) -> PathBuf {
        self.team_auto_memory_root()
            .join(AUTO_MEMORY_INDEX_FILENAME)
    }

    pub fn is_auto_memory_path(&self, absolute_path: impl AsRef<Path>) -> bool {
        is_inside_memory_root_at(absolute_path.as_ref(), &self.auto_memory_root(), &self.cwd)
    }

    pub fn is_user_auto_memory_path(&self, absolute_path: impl AsRef<Path>) -> bool {
        is_inside_memory_root_at(
            absolute_path.as_ref(),
            &self.user_auto_memory_root(),
            &self.cwd,
        )
    }

    pub fn is_team_auto_memory_path(&self, absolute_path: impl AsRef<Path>) -> bool {
        let path = realpath_nearest_existing(absolute_path.as_ref(), &self.cwd);
        let root = realpath_nearest_existing(&self.team_auto_memory_root(), &self.cwd);
        is_inside_memory_root(&path, &root)
    }

    /// Includes every layer for read retention. This is not a write permission
    /// check; use `is_any_auto_memory_path` for the two private auto-approved
    /// layers.
    pub fn is_managed_memory_path(
        &self,
        file_path: impl AsRef<Path>,
        base_dir: impl AsRef<Path>,
    ) -> bool {
        let file_path = file_path.as_ref();
        let resolved_input = if file_path.is_absolute() {
            file_path.to_path_buf()
        } else {
            base_dir.as_ref().join(file_path)
        };
        let absolute_path = absolute_normalized(&resolved_input, &self.cwd);
        let resolved_path = realpath_nearest_existing(&absolute_path, &self.cwd);
        [
            self.auto_memory_root(),
            self.user_auto_memory_root(),
            self.team_auto_memory_root(),
        ]
        .iter()
        .any(|root| {
            is_inside_memory_root(&resolved_path, &realpath_nearest_existing(root, &self.cwd))
        })
    }

    /// Team memory is deliberately excluded because its tracked writes require
    /// review. This matches the extraction sandbox's permission boundary.
    pub fn is_any_auto_memory_path(&self, absolute_path: impl AsRef<Path>) -> bool {
        self.is_auto_memory_path(absolute_path.as_ref())
            || self.is_user_auto_memory_path(absolute_path)
    }
}

pub fn get_auto_memory_topic_filename(memory_type: super::AutoMemoryType) -> String {
    format!("{}.md", memory_type.as_str())
}

/// Lexical containment equivalent to the memory helpers' normalized
/// `path.relative` check, including its rejection of a child name beginning
/// with two dots.
pub fn is_inside_memory_root(path: &Path, root: &Path) -> bool {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    is_inside_memory_root_at(path, root, &cwd)
}

fn is_inside_memory_root_at(path: &Path, root: &Path, cwd: &Path) -> bool {
    let path = absolute_normalized(path, cwd);
    let root = absolute_normalized(root, cwd);
    let Some(remainder) = strip_path_prefix_platform(&path, &root) else {
        return false;
    };
    if remainder.as_os_str().is_empty() {
        return true;
    }
    !remainder.to_string_lossy().starts_with("..")
}

fn strip_path_prefix_platform<'a>(path: &'a Path, root: &'a Path) -> Option<PathBuf> {
    if !cfg!(windows) {
        return path.strip_prefix(root).ok().map(Path::to_path_buf);
    }
    let path_components = path.components().collect::<Vec<_>>();
    let root_components = root.components().collect::<Vec<_>>();
    if path_components.len() < root_components.len()
        || !path_components
            .iter()
            .zip(root_components.iter())
            .all(|(path, root)| component_eq_ignore_case(*path, *root))
    {
        return None;
    }
    Some(path_components[root_components.len()..].iter().fold(
        PathBuf::new(),
        |mut remainder, component| {
            remainder.push(component.as_os_str());
            remainder
        },
    ))
}

fn component_eq_ignore_case(left: Component<'_>, right: Component<'_>) -> bool {
    left.as_os_str().to_string_lossy().to_lowercase()
        == right.as_os_str().to_string_lossy().to_lowercase()
}

fn is_js_whitespace(character: char) -> bool {
    matches!(
        character,
        '\u{0009}'
            | '\u{000a}'
            | '\u{000b}'
            | '\u{000c}'
            | '\u{000d}'
            | '\u{0020}'
            | '\u{00a0}'
            | '\u{1680}'
            | '\u{2000}'
            ..='\u{200a}'
                | '\u{2028}'
                | '\u{2029}'
                | '\u{202f}'
                | '\u{205f}'
                | '\u{3000}'
                | '\u{feff}'
    )
}

fn sanitize_memory_project_key(project_root: &Path, windows: bool) -> String {
    let value = project_root.to_string_lossy();
    let normalized = if windows {
        value.to_lowercase()
    } else {
        value.into_owned()
    };
    normalized
        .encode_utf16()
        .map(|unit| {
            if (u16::from(b'a')..=u16::from(b'z')).contains(&unit)
                || (u16::from(b'A')..=u16::from(b'Z')).contains(&unit)
                || (u16::from(b'0')..=u16::from(b'9')).contains(&unit)
            {
                char::from_u32(u32::from(unit)).unwrap_or('-')
            } else {
                '-'
            }
        })
        .collect()
}

fn find_git_root(start_path: &Path) -> Option<PathBuf> {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let mut current = absolute_normalized(start_path, &cwd);
    loop {
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

fn join_like_node(base: &Path, child: &Path) -> PathBuf {
    let mut joined = base.to_path_buf();
    for component in child.components() {
        match component {
            Component::Prefix(_) | Component::RootDir | Component::CurDir => {}
            Component::ParentDir => joined.push(".."),
            Component::Normal(part) => joined.push(part),
        }
    }
    normalize_lexical(&joined)
}

fn normalize_lexical(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
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

fn resolve_configured_path(value: &str, cwd: &Path, home: Option<&Path>) -> PathBuf {
    let expanded = if value == "~" {
        home.unwrap_or_else(|| Path::new("~")).to_path_buf()
    } else if let Some(suffix) = value
        .strip_prefix("~/")
        .or_else(|| value.strip_prefix("~\\"))
    {
        suffix
            .split(['/', '\\'])
            .filter(|segment| !segment.is_empty())
            .fold(
                home.unwrap_or_else(|| Path::new("~")).to_path_buf(),
                |mut path, segment| {
                    path.push(segment);
                    path
                },
            )
    } else {
        PathBuf::from(value)
    };
    absolute_normalized(&expanded, cwd)
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
                if !normalized.pop() && !normalized.has_root() {
                    normalized.push("..");
                }
            }
            Component::Normal(part) => normalized.push(part),
        }
    }
    normalized
}

/// Follow the final dangling-symlink chain, then canonicalize the nearest
/// existing ancestor while retaining any missing suffix. Errors degrade to
/// the lexical absolute path, matching the JavaScript helper's fail-closed
/// path classification behavior.
fn realpath_nearest_existing(path: &Path, cwd: &Path) -> PathBuf {
    let initial = absolute_normalized(path, cwd);
    let mut current = initial.clone();
    for _ in 0..40 {
        let Ok(metadata) = std::fs::symlink_metadata(&current) else {
            break;
        };
        if !metadata.file_type().is_symlink() {
            break;
        }
        let Ok(target) = std::fs::read_link(&current) else {
            return current;
        };
        if target.is_absolute() {
            current = absolute_normalized(&target, cwd);
        } else {
            let parent = current.parent().unwrap_or(cwd);
            let resolved_parent =
                std::fs::canonicalize(parent).unwrap_or_else(|_| parent.to_path_buf());
            current = absolute_normalized(&resolved_parent.join(target), cwd);
        }
    }

    let mut missing = Vec::new();
    let mut ancestor = current.clone();
    while !ancestor.exists() {
        let Some(name) = ancestor.file_name() else {
            return initial;
        };
        missing.push(name.to_os_string());
        let Some(parent) = ancestor.parent() else {
            return initial;
        };
        if parent == ancestor {
            return initial;
        }
        ancestor = parent.to_path_buf();
    }
    let Ok(mut resolved) = std::fs::canonicalize(&ancestor) else {
        return initial;
    };
    for part in missing.into_iter().rev() {
        resolved.push(part);
    }
    resolved
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::AutoMemoryType;

    fn temp_dir(label: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("canopy-memory-{label}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn resolves_local_shared_and_workspace_roots_with_trusted_anchors() {
        let temp = temp_dir("roots");
        let repo = temp.join("repo");
        let nested = repo.join("packages/a");
        std::fs::create_dir_all(repo.join(".git")).unwrap();
        std::fs::create_dir_all(&nested).unwrap();
        let base = temp.join("state");

        let git = AutoMemoryPaths::new(&nested, &base, false, MemoryProjectScope::GitRoot);
        assert_eq!(
            git.auto_memory_root(),
            base.join("projects")
                .join(sanitize_memory_project_key(&repo, cfg!(windows)))
                .join("memory")
        );
        assert_eq!(git.auto_memory_trusted_anchor(), git.memory_base_dir());

        let workspace = AutoMemoryPaths::new(&nested, &base, false, MemoryProjectScope::Workspace);
        assert_eq!(
            workspace.auto_memory_root(),
            base.join("projects")
                .join(sanitize_memory_project_key(&nested, cfg!(windows)))
                .join("memory")
        );

        let local = AutoMemoryPaths::new(&nested, &base, true, MemoryProjectScope::GitRoot);
        assert_eq!(local.auto_memory_root(), nested.join(".canopy/memory"));
        assert_eq!(local.auto_memory_trusted_anchor(), nested);
        assert_eq!(
            local.auto_memory_metadata_path(),
            nested.join(".canopy/meta.json")
        );
        assert_eq!(
            local.auto_memory_topic_path(AutoMemoryType::Feedback),
            nested.join(".canopy/memory/feedback.md")
        );
        let non_normalized_local = AutoMemoryPaths::new(
            repo.join("packages/../packages/a"),
            &base,
            true,
            MemoryProjectScope::GitRoot,
        );
        assert_eq!(
            non_normalized_local.auto_memory_root(),
            nested.join(".canopy/memory")
        );
        std::fs::remove_dir_all(temp).unwrap();
    }

    #[test]
    fn runtime_override_scope_normalization_and_linked_worktree_paths_match_contract() {
        let temp = temp_dir("inputs");
        let cwd = temp.join("cwd");
        let home = temp.join("home");
        std::fs::create_dir_all(&cwd).unwrap();
        std::fs::create_dir_all(&home).unwrap();
        let inputs = MemoryPathInputs {
            project_root: PathBuf::from("project"),
            runtime_base_dir: temp.join("runtime"),
            memory_base_dir_override: Some("~/custom/../memory".into()),
            memory_local: false,
            project_scope: Some("  WoRkSpAcE  ".into()),
            cwd: cwd.clone(),
            home_dir: Some(home.clone()),
        };
        let paths = inputs.paths();
        assert_eq!(paths.memory_base_dir(), home.join("memory"));
        assert_eq!(paths.project_scope(), MemoryProjectScope::Workspace);
        assert_eq!(
            paths.auto_memory_root(),
            home.join("memory/projects")
                .join(sanitize_memory_project_key(
                    &cwd.join("project"),
                    cfg!(windows)
                ))
                .join("memory")
        );

        let runtime_only = MemoryPathInputs {
            project_root: PathBuf::from("project"),
            runtime_base_dir: PathBuf::from("runtime"),
            memory_base_dir_override: None,
            memory_local: false,
            project_scope: None,
            cwd: cwd.clone(),
            home_dir: Some(home.clone()),
        }
        .paths();
        assert_eq!(runtime_only.memory_base_dir(), cwd.join("runtime"));

        let workspace_paths =
            AutoMemoryPaths::new(&cwd, &home, false, MemoryProjectScope::Workspace);
        assert_eq!(
            workspace_paths.auto_memory_file_path("/nested/../file.md"),
            workspace_paths.auto_memory_root().join("file.md")
        );
        assert_eq!(
            sanitize_memory_project_key(Path::new("/tmp/🦀"), false),
            "-tmp---"
        );

        assert_eq!(
            resolve_memory_project_scope(Some(" exact ")),
            (MemoryProjectScope::GitRoot, Some(" exact ".to_owned()))
        );
        assert_eq!(
            resolve_memory_project_scope(Some("GIT-ROOT")),
            (MemoryProjectScope::GitRoot, None)
        );
        assert_eq!(
            resolve_memory_project_scope(Some("\u{feff} Workspace \u{feff}")),
            (MemoryProjectScope::Workspace, None)
        );

        let main = temp.join("main");
        let worktree = temp.join("worktree");
        let worktree_gitdir = main.join(".git/worktrees/wt");
        std::fs::create_dir_all(&worktree_gitdir).unwrap();
        std::fs::create_dir_all(&worktree).unwrap();
        std::fs::write(
            worktree.join(".git"),
            format!("gitdir: {}", worktree_gitdir.display()),
        )
        .unwrap();
        std::fs::write(worktree_gitdir.join("commondir"), "../..").unwrap();
        let linked = AutoMemoryPaths::new(&worktree, &temp, false, MemoryProjectScope::GitRoot);
        assert_eq!(
            linked.auto_memory_root(),
            temp.join("projects")
                .join(sanitize_memory_project_key(&worktree, cfg!(windows)))
                .join("memory")
        );
        std::fs::remove_dir_all(temp).unwrap();
    }

    #[test]
    fn containment_distinguishes_private_team_and_symlinked_managed_paths() {
        let temp = temp_dir("scope");
        let repo = temp.join("repo");
        std::fs::create_dir_all(repo.join(".git")).unwrap();
        let base = temp.join("state");
        let paths = AutoMemoryPaths::new(&repo, &base, false, MemoryProjectScope::GitRoot);
        let private_file = paths.auto_memory_root().join("topic.md");
        let user_file = paths.user_auto_memory_root().join("topic.md");
        let team_file = paths.team_auto_memory_root().join("topic.md");
        assert!(paths.is_any_auto_memory_path(&private_file));
        assert!(paths.is_any_auto_memory_path(&user_file));
        assert!(!paths.is_any_auto_memory_path(&team_file));
        assert!(paths.is_team_auto_memory_path(&team_file));
        assert!(paths.is_managed_memory_path(&team_file, &repo));
        assert!(!paths.is_auto_memory_path(paths.auto_memory_root().join("..secret")));

        std::fs::create_dir_all(paths.auto_memory_root()).unwrap();
        let outside = temp.join("outside.md");
        std::fs::write(&outside, "outside").unwrap();
        #[cfg(unix)]
        {
            let escaped = paths.auto_memory_root().join("escaped.md");
            std::os::unix::fs::symlink(&outside, &escaped).unwrap();
            assert!(!paths.is_managed_memory_path(&escaped, &repo));

            let dangling_inside = temp.join("dangling-inside.md");
            std::os::unix::fs::symlink(
                paths.team_auto_memory_root().join("reference/new.md"),
                &dangling_inside,
            )
            .unwrap();
            assert!(paths.is_team_auto_memory_path(&dangling_inside));

            let dangling_outside = temp.join("dangling-outside.md");
            std::os::unix::fs::symlink(temp.join("not-created/outside.md"), &dangling_outside)
                .unwrap();
            assert!(!paths.is_team_auto_memory_path(&dangling_outside));
        }
        std::fs::remove_dir_all(temp).unwrap();
    }
}
