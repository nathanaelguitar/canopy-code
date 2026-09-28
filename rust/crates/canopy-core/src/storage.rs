//! Canopy global and per-project storage paths.
//!
//! This mirrors `packages/core/src/config/storage.ts`. Instances capture the
//! runtime root at construction time so later configuration changes cannot
//! move an active session's files.

use std::future::Future;
use std::path::{Component, Path, PathBuf};
use std::sync::{OnceLock, RwLock};

use thiserror::Error;

pub const CANOPY_DIR: &str = ".canopy";
pub const GOOGLE_ACCOUNTS_FILENAME: &str = "google_accounts.json";
pub const OAUTH_FILE: &str = "oauth_creds.json";
pub const SKILL_PROVIDER_CONFIG_DIRS: [&str; 2] = [".canopy", ".agents"];

const TMP_DIR_NAME: &str = "tmp";
const PROJECT_DIR_NAME: &str = "projects";
const IDE_DIR_NAME: &str = "ide";
const PLANS_DIR_NAME: &str = "plans";
const DEBUG_DIR_NAME: &str = "debug";
const BIN_DIR_NAME: &str = "bin";
const ARENA_DIR_NAME: &str = "arena";

static CONFIGURED_RUNTIME_BASE_DIR: OnceLock<RwLock<Option<PathBuf>>> = OnceLock::new();

tokio::task_local! {
    static RUNTIME_BASE_DIR_CONTEXT: RuntimeBaseDirContext;
}

#[derive(Clone, Debug)]
struct RuntimeBaseDirContext {
    dir: Option<PathBuf>,
    pinned: bool,
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("projectRoot is required when plansDirectory is configured.")]
    ProjectRootRequired,
    #[error("plansDirectory must resolve within the project root.")]
    PlansDirectoryOutsideProject,
    #[error("could not resolve storage path: {0}")]
    Io(#[from] std::io::Error),
}

/// Resolve `.` and `..` without consulting the filesystem.
pub(crate) fn normalize_absolute(path: &Path, cwd: &Path) -> PathBuf {
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
                    normalized.push(component.as_os_str());
                }
            }
            Component::Normal(part) => normalized.push(part),
        }
    }
    normalized
}

fn current_dir() -> PathBuf {
    std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
}

fn home_dir() -> Option<PathBuf> {
    ["HOME", "USERPROFILE"]
        .into_iter()
        .filter_map(std::env::var_os)
        .find(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn temp_dir() -> PathBuf {
    std::env::temp_dir()
}

fn expand_home(value: &str, home: Option<&Path>) -> PathBuf {
    if value == "~" {
        return home.unwrap_or_else(|| Path::new("~")).to_path_buf();
    }
    if value.starts_with("~/") || value.starts_with("~\\") {
        let tail = value[2..]
            .split(['/', '\\'])
            .filter(|part| !part.is_empty());
        return tail.fold(
            home.unwrap_or_else(|| Path::new("~")).to_path_buf(),
            |mut path, part| {
                path.push(part);
                path
            },
        );
    }
    PathBuf::from(value)
}

fn resolve_path(value: &str, cwd: Option<&Path>) -> PathBuf {
    let cwd = cwd.map(Path::to_path_buf).unwrap_or_else(current_dir);
    normalize_absolute(&expand_home(value, home_dir().as_deref()), &cwd)
}

fn configured_runtime_base_dir() -> &'static RwLock<Option<PathBuf>> {
    CONFIGURED_RUNTIME_BASE_DIR.get_or_init(|| RwLock::new(None))
}

fn global_canopy_dir_with(
    qwen_home: Option<&str>,
    home: Option<&Path>,
    cwd: &Path,
    temp: &Path,
) -> PathBuf {
    if let Some(directory) = qwen_home.filter(|value| !value.is_empty()) {
        return normalize_absolute(&expand_home(directory, home), cwd);
    }
    let base = home.unwrap_or(temp).join(CANOPY_DIR);
    normalize_absolute(&base, cwd)
}

/// Resolve the environment-driven runtime path using the process environment.
///
/// `QWEN_HOME` selects the global Canopy directory, which is also the runtime
/// fallback when no separate runtime directory is configured.
pub fn resolve_runtime_base_dir(
    canopy_runtime_dir: Option<&str>,
    qwen_home: Option<&str>,
    home: Option<&Path>,
    cwd: &Path,
    temp: &Path,
) -> PathBuf {
    if let Some(directory) = canopy_runtime_dir.filter(|value| !value.is_empty()) {
        return normalize_absolute(&expand_home(directory, home), cwd);
    }
    global_canopy_dir_with(qwen_home, home, cwd, temp)
}

/// A set of paths rooted at one project and one captured runtime directory.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Storage {
    target_dir: PathBuf,
    runtime_base_dir: PathBuf,
}

impl Storage {
    pub fn new(target_dir: impl Into<PathBuf>) -> Self {
        Self::with_runtime_base_dir(target_dir, Self::get_runtime_base_dir())
    }

    pub fn with_runtime_base_dir(
        target_dir: impl Into<PathBuf>,
        runtime_base_dir: impl AsRef<Path>,
    ) -> Self {
        let cwd = current_dir();
        Self {
            target_dir: target_dir.into(),
            runtime_base_dir: normalize_absolute(runtime_base_dir.as_ref(), &cwd),
        }
    }

    /// Set the settings-selected runtime root. Empty input resets it.
    pub fn set_runtime_base_dir(dir: Option<&str>, cwd: Option<&Path>) {
        let resolved = dir
            .filter(|value| !value.is_empty())
            .map(|value| resolve_path(value, cwd));
        *configured_runtime_base_dir()
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = resolved;
    }

    pub fn get_runtime_base_dir() -> PathBuf {
        if let Ok(context) = RUNTIME_BASE_DIR_CONTEXT.try_with(Clone::clone) {
            if context.pinned {
                return context.dir.unwrap_or_else(Self::get_global_canopy_dir);
            }
        }

        let cwd = current_dir();
        if let Some(directory) = std::env::var("CANOPY_RUNTIME_DIR")
            .ok()
            .filter(|value| !value.is_empty())
        {
            return resolve_path(&directory, Some(&cwd));
        }

        if let Ok(context) = RUNTIME_BASE_DIR_CONTEXT.try_with(Clone::clone) {
            if let Some(directory) = context.dir {
                return directory;
            }
            return Self::get_global_canopy_dir();
        }

        if let Some(directory) = configured_runtime_base_dir()
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
        {
            return directory.clone();
        }
        Self::get_global_canopy_dir()
    }

    /// Runs an async operation in a configurable runtime path context. A
    /// resolved outer pin cannot be replaced by a nested configurable context.
    pub async fn run_with_runtime_base_dir<F, Fut, T>(
        dir: Option<&str>,
        cwd: Option<&Path>,
        operation: F,
    ) -> T
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = T>,
    {
        if RUNTIME_BASE_DIR_CONTEXT
            .try_with(|context| context.pinned)
            .unwrap_or(false)
        {
            return operation().await;
        }
        let dir = dir
            .filter(|value| !value.is_empty())
            .map(|value| resolve_path(value, cwd));
        RUNTIME_BASE_DIR_CONTEXT
            .scope(RuntimeBaseDirContext { dir, pinned: false }, async move {
                operation().await
            })
            .await
    }

    /// Runs an async operation with a runtime root owned by a managed
    /// workspace for its full lifetime.
    pub async fn run_with_resolved_runtime_base_dir<F, Fut, T>(dir: &Path, operation: F) -> T
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = T>,
    {
        let dir = normalize_absolute(dir, &current_dir());
        RUNTIME_BASE_DIR_CONTEXT
            .scope(
                RuntimeBaseDirContext {
                    dir: Some(dir),
                    pinned: true,
                },
                async move { operation().await },
            )
            .await
    }

    pub fn has_runtime_base_dir_context() -> bool {
        RUNTIME_BASE_DIR_CONTEXT.try_with(|_| ()).is_ok()
    }

    /// Spawn a child task that inherits the current runtime path context.
    /// Tokio task-local values do not automatically cross `tokio::spawn`, so
    /// callers that create child tasks inside a storage context use this helper.
    pub fn spawn_with_current_runtime_base_dir<F>(future: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        match RUNTIME_BASE_DIR_CONTEXT.try_with(Clone::clone) {
            Ok(context) => tokio::spawn(RUNTIME_BASE_DIR_CONTEXT.scope(context, future)),
            Err(_) => tokio::spawn(future),
        }
    }

    pub fn get_global_canopy_dir() -> PathBuf {
        let cwd = current_dir();
        global_canopy_dir_with(
            std::env::var("QWEN_HOME").ok().as_deref(),
            home_dir().as_deref(),
            &cwd,
            &temp_dir(),
        )
    }

    pub fn get_mcp_oauth_tokens_path() -> PathBuf {
        Self::get_global_canopy_dir().join("mcp-oauth-tokens.json")
    }

    pub fn get_global_settings_path() -> PathBuf {
        Self::get_global_canopy_dir().join("settings.json")
    }

    pub fn get_installation_id_path() -> PathBuf {
        Self::get_global_canopy_dir().join("installation_id")
    }

    pub fn get_google_accounts_path() -> PathBuf {
        Self::get_global_canopy_dir().join(GOOGLE_ACCOUNTS_FILENAME)
    }

    pub fn get_user_commands_dir() -> PathBuf {
        Self::get_global_canopy_dir().join("commands")
    }

    pub fn get_global_memory_file_path() -> PathBuf {
        Self::get_global_canopy_dir().join("memory.md")
    }

    pub fn get_global_temp_dir() -> PathBuf {
        Self::get_runtime_base_dir().join(TMP_DIR_NAME)
    }

    pub fn get_global_debug_dir() -> PathBuf {
        Self::get_runtime_base_dir().join(DEBUG_DIR_NAME)
    }

    pub fn get_debug_log_path(session_id: &str) -> PathBuf {
        Self::get_global_debug_dir().join(format!("{session_id}.txt"))
    }

    /// The IDE lock-file location stays under global config even when runtime
    /// output has a separate root.
    pub fn get_global_ide_dir() -> PathBuf {
        Self::get_global_canopy_dir().join(IDE_DIR_NAME)
    }

    pub fn assert_path_within_directory(
        child_path: &Path,
        parent_path: &Path,
    ) -> Result<(), StorageError> {
        let parent = resolve_through_existing_ancestor(parent_path)?;
        let child = resolve_through_existing_ancestor(child_path)?;
        if child.starts_with(parent) {
            Ok(())
        } else {
            Err(StorageError::PlansDirectoryOutsideProject)
        }
    }

    pub fn get_plans_dir(
        project_root: Option<&Path>,
        plans_directory: Option<&str>,
    ) -> Result<PathBuf, StorageError> {
        if let Some(configured) = plans_directory.map(str::trim).filter(|s| !s.is_empty()) {
            let project_root = project_root.ok_or(StorageError::ProjectRootRequired)?;
            let cwd = current_dir();
            let resolved_project_root = normalize_absolute(project_root, &cwd);
            let resolved_plans_dir = normalize_absolute(
                &expand_home(configured, home_dir().as_deref()),
                &resolved_project_root,
            );
            Self::assert_path_within_directory(&resolved_plans_dir, &resolved_project_root)?;
            return Ok(resolved_plans_dir);
        }
        Ok(Self::get_global_canopy_dir().join(PLANS_DIR_NAME))
    }

    pub fn sanitize_plan_session_id(session_id: &str) -> String {
        let normalized = session_id.replace('\\', "/");
        let basename = normalized.rsplit('/').next().unwrap_or_default();
        let mut safe_name = String::with_capacity(basename.len());
        let basename = if basename.starts_with('.') {
            safe_name.push('_');
            basename.trim_start_matches('.')
        } else {
            basename
        };
        for character in basename.chars() {
            if matches!(character, '<' | '>' | ':' | '"' | '|' | '?' | '*')
                || (character as u32) <= 0x1f
            {
                safe_name.push('_');
            } else {
                safe_name.push(character);
            }
        }
        if safe_name.is_empty() {
            "_".to_owned()
        } else {
            safe_name
        }
    }

    pub fn get_plan_file_path(
        session_id: &str,
        project_root: Option<&Path>,
        plans_directory: Option<&str>,
    ) -> Result<PathBuf, StorageError> {
        Ok(Self::get_plans_dir(project_root, plans_directory)?
            .join(format!("{}.md", Self::sanitize_plan_session_id(session_id))))
    }

    pub fn get_global_bin_dir() -> PathBuf {
        Self::get_global_canopy_dir().join(BIN_DIR_NAME)
    }

    pub fn get_global_arena_dir() -> PathBuf {
        Self::get_global_canopy_dir().join(ARENA_DIR_NAME)
    }

    pub fn target_dir(&self) -> &Path {
        &self.target_dir
    }

    pub fn get_canopy_dir(&self) -> PathBuf {
        self.target_dir.join(CANOPY_DIR)
    }

    pub fn runtime_base_dir(&self) -> &Path {
        &self.runtime_base_dir
    }

    pub fn get_project_dir(&self) -> PathBuf {
        self.get_project_dir_for_platform(cfg!(windows))
    }

    pub fn get_project_dir_for_platform(&self, windows: bool) -> PathBuf {
        self.runtime_base_dir
            .join(PROJECT_DIR_NAME)
            .join(crate::session_paths::sanitize_cwd(
                &self.target_dir,
                windows,
            ))
    }

    pub fn get_project_temp_dir(&self) -> PathBuf {
        self.runtime_base_dir
            .join(TMP_DIR_NAME)
            .join(crate::session_paths::get_project_hash(&self.target_dir))
    }

    pub fn get_tool_results_dir(&self) -> PathBuf {
        self.get_project_temp_dir().join("tool-results")
    }

    pub fn ensure_project_temp_dir_exists(&self) -> std::io::Result<()> {
        std::fs::create_dir_all(self.get_project_temp_dir())
    }

    pub fn get_oauth_creds_path() -> PathBuf {
        Self::get_global_canopy_dir().join(OAUTH_FILE)
    }

    pub fn get_project_root(&self) -> &Path {
        &self.target_dir
    }

    pub fn get_workspace_settings_path(&self) -> PathBuf {
        self.get_canopy_dir().join("settings.json")
    }

    pub fn get_project_commands_dir(&self) -> PathBuf {
        self.get_canopy_dir().join("commands")
    }

    pub fn get_project_workflows_dir(&self) -> PathBuf {
        self.get_canopy_dir().join("workflows")
    }

    pub fn get_user_workflows_dir() -> PathBuf {
        Self::get_global_canopy_dir().join("workflows")
    }

    pub fn get_workflow_runs_dir(&self) -> PathBuf {
        self.get_project_dir().join("workflows")
    }

    pub fn get_workflow_run_snapshot_path(&self, run_id: &str) -> PathBuf {
        self.get_workflow_runs_dir().join(format!("{run_id}.json"))
    }

    pub fn get_workflow_run_journal_path(&self, run_id: &str) -> PathBuf {
        self.get_workflow_runs_dir()
            .join(run_id)
            .join("journal.jsonl")
    }

    pub fn get_runtime_status_path(&self, session_id: &str) -> PathBuf {
        self.get_project_dir()
            .join("chats")
            .join(format!("{session_id}.runtime.json"))
    }

    pub fn get_project_temp_checkpoints_dir(&self) -> PathBuf {
        self.get_project_temp_dir().join("checkpoints")
    }

    pub fn get_extensions_dir(&self) -> PathBuf {
        self.get_canopy_dir().join("extensions")
    }

    pub fn get_extensions_config_path(&self) -> PathBuf {
        self.get_extensions_dir().join("canopy-extension.json")
    }

    pub fn get_user_skills_dirs() -> [PathBuf; 2] {
        let home = home_dir().unwrap_or_else(temp_dir);
        [
            Self::get_global_canopy_dir().join("skills"),
            home.join(".agents").join("skills"),
        ]
    }

    pub fn get_user_extensions_dir() -> PathBuf {
        Self::get_global_canopy_dir().join("extensions")
    }

    pub fn get_history_file_path(&self) -> PathBuf {
        self.get_project_temp_dir().join("shell_history")
    }
}

fn resolve_through_existing_ancestor(path: &Path) -> Result<PathBuf, std::io::Error> {
    let cwd = current_dir();
    let path = normalize_absolute(path, &cwd);
    let mut candidate = path.as_path();
    loop {
        match std::fs::canonicalize(candidate) {
            Ok(real_candidate) => {
                let remainder = path
                    .strip_prefix(candidate)
                    .unwrap_or_else(|_| Path::new(""));
                return Ok(normalize_absolute(&real_candidate.join(remainder), &cwd));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let Some(parent) = candidate.parent() else {
                    return Ok(path);
                };
                if parent == candidate {
                    return Ok(path);
                }
                candidate = parent;
            }
            Err(error) => return Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn runtime_and_global_directories_keep_their_separate_roles() {
        let home = Path::new("/Users/example");
        let cwd = Path::new("/workspace");
        let temp = Path::new("/tmp");
        assert_eq!(
            resolve_runtime_base_dir(Some("~/runtime"), Some("/config"), Some(home), cwd, temp),
            PathBuf::from("/Users/example/runtime")
        );
        assert_eq!(
            resolve_runtime_base_dir(None, Some("~/config"), Some(home), cwd, temp),
            PathBuf::from("/Users/example/config")
        );
        assert_eq!(
            resolve_runtime_base_dir(None, None, Some(home), cwd, temp),
            PathBuf::from("/Users/example/.canopy")
        );
        assert_eq!(
            resolve_runtime_base_dir(None, Some("relative/config"), Some(home), cwd, temp),
            PathBuf::from("/workspace/relative/config")
        );
        assert_eq!(
            global_canopy_dir_with(Some("~/config"), Some(home), cwd, temp),
            PathBuf::from("/Users/example/config")
        );
    }

    #[test]
    fn global_config_paths_and_project_paths_use_their_own_roots() {
        let storage = Storage::with_runtime_base_dir("/work/project", "/tmp/runtime");
        assert_eq!(
            storage.get_workspace_settings_path(),
            PathBuf::from("/work/project/.canopy/settings.json")
        );
        assert_eq!(
            storage.get_project_temp_dir(),
            PathBuf::from(format!(
                "/tmp/runtime/tmp/{}",
                crate::session_paths::get_project_hash(Path::new("/work/project"))
            ))
        );
    }

    #[test]
    fn plan_paths_reject_traversal_and_sanitize_session_ids() {
        let root = Path::new("/tmp/work/project");
        assert_eq!(
            Storage::get_plans_dir(Some(root), Some("./plans")).unwrap(),
            PathBuf::from("/tmp/work/project/plans")
        );
        assert!(matches!(
            Storage::get_plans_dir(Some(root), Some("../plans")),
            Err(StorageError::PlansDirectoryOutsideProject)
        ));
        assert!(matches!(
            Storage::get_plans_dir(None, Some("./plans")),
            Err(StorageError::ProjectRootRequired)
        ));
        assert_eq!(
            Storage::sanitize_plan_session_id("../../.bad:<id>"),
            "_bad__id_"
        );
        assert_eq!(
            Storage::get_plan_file_path("../../../escape", Some(root), Some("./plans")).unwrap(),
            PathBuf::from("/tmp/work/project/plans/escape.md")
        );
        assert_eq!(Storage::sanitize_plan_session_id("...hidden"), "_hidden");
    }

    #[cfg(unix)]
    #[test]
    fn plan_path_validation_resolves_symlink_escapes() {
        use std::os::unix::fs::symlink;

        let scratch = std::env::temp_dir().join(format!("canopy-storage-{}", uuid::Uuid::new_v4()));
        let project = scratch.join("project");
        let outside = scratch.join("outside");
        std::fs::create_dir_all(&project).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        symlink(&outside, project.join("escape")).unwrap();
        assert!(matches!(
            Storage::get_plans_dir(Some(&project), Some("escape/plans")),
            Err(StorageError::PlansDirectoryOutsideProject)
        ));
        std::fs::remove_dir_all(scratch).unwrap();
    }

    #[tokio::test]
    async fn runtime_contexts_are_task_local_and_pins_survive_nested_contexts() {
        let cwd_a = Path::new("/workspace/a");
        let cwd_b = Path::new("/workspace/b");
        let process_runtime_override = std::env::var("CANOPY_RUNTIME_DIR")
            .ok()
            .filter(|value| !value.is_empty())
            .map(|directory| resolve_path(&directory, Some(&current_dir())));
        let (a, b) = tokio::join!(
            Storage::run_with_runtime_base_dir(Some(".canopy-a"), Some(cwd_a), || async {
                tokio::task::yield_now().await;
                Storage::get_runtime_base_dir()
            }),
            Storage::run_with_runtime_base_dir(Some(".canopy-b"), Some(cwd_b), || async {
                tokio::task::yield_now().await;
                Storage::get_runtime_base_dir()
            })
        );
        assert_eq!(
            a,
            process_runtime_override
                .clone()
                .unwrap_or_else(|| PathBuf::from("/workspace/a/.canopy-a"))
        );
        assert_eq!(
            b,
            process_runtime_override
                .clone()
                .unwrap_or_else(|| PathBuf::from("/workspace/b/.canopy-b"))
        );

        let child =
            Storage::run_with_runtime_base_dir(Some(".canopy-child"), Some(cwd_a), || async {
                Storage::spawn_with_current_runtime_base_dir(async {
                    tokio::task::yield_now().await;
                    Storage::get_runtime_base_dir()
                })
                .await
                .unwrap()
            })
            .await;
        assert_eq!(
            child,
            process_runtime_override
                .clone()
                .unwrap_or_else(|| PathBuf::from("/workspace/a/.canopy-child"))
        );

        let pinned = Path::new("/workspace/pinned");
        let actual = Storage::run_with_resolved_runtime_base_dir(pinned, || async {
            Storage::run_with_runtime_base_dir(Some("/nested"), None, || async {
                tokio::task::yield_now().await;
                Storage::get_runtime_base_dir()
            })
            .await
        })
        .await;
        assert_eq!(actual, pinned);
    }
}
