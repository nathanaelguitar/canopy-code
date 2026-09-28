//! Managed-memory "remember" prompts and agent orchestration boundary.
//!
//! Port of `packages/core/src/memory/remember.ts`. The native runtime owns
//! model execution and permission enforcement; this module passes it an
//! explicit request describing the source tool allowlist, limits, history
//! policy, hooks/recording policy, and scoped memory roots.

use std::future::Future;
use std::io;
use std::path::{Component, Path, PathBuf};
use std::pin::Pin;

use thiserror::Error;
use tokio::sync::watch;

use super::indexer::{rebuild_managed_auto_memory_index, rebuild_user_auto_memory_index};
use super::paths::AutoMemoryPaths;
use super::prompt::{
    BuildMemoryPromptOptions, UserAutoMemorySection, build_managed_auto_memory_prompt,
};
use super::store::{
    ensure_auto_memory_scaffold, ensure_user_auto_memory_scaffold, read_auto_memory_index,
    read_user_auto_memory_index,
};

pub const MANAGED_REMEMBER_AGENT_NAME: &str = "managed-auto-memory-remember";
pub const REMEMBER_AGENT_TOOLS: [&str; 5] = [
    "read_file",
    "grep_search",
    "list_directory",
    "write_file",
    "edit",
];
pub const DEFAULT_REMEMBER_MAX_TURNS: u32 = 6;
pub const DEFAULT_REMEMBER_TIMEOUT_MINUTES: u32 = 5;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkspaceRememberContextMode {
    Workspace,
    Clean,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum WorkspaceRememberScope {
    Project,
    User,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagedRememberResult {
    pub summary: String,
    pub files_touched: Vec<String>,
    pub touched_scopes: Vec<WorkspaceRememberScope>,
}

/// A child agent's initial-history behavior. Neither option injects parent
/// chat history. Workspace mode retains the normal workspace bootstrap;
/// clean mode preserves an explicitly empty history to suppress that context.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RememberHistoryPolicy {
    WorkspaceBootstrapWithoutParentHistory,
    ExplicitlyEmpty,
}

/// Permission settings requested from the runtime's memory-scoped config.
/// These mirror the source wrapper options and are data, not an imitation of
/// Canopy's permission manager.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RememberScopedPaths {
    pub project_root: PathBuf,
    pub trusted_project_anchor: PathBuf,
    pub project_memory_root: PathBuf,
    pub user_memory_root: PathBuf,
    pub bypass_base_ask_for_scoped_paths: bool,
    pub restrict_reads_to_memory_paths: bool,
    pub include_user_memory: bool,
    pub allow_shell: bool,
    pub protect_pinned_memory: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RememberAgentRequest {
    pub name: &'static str,
    pub task_prompt: String,
    pub system_prompt: String,
    pub max_turns: u32,
    pub max_time_minutes: u32,
    pub tools: &'static [&'static str],
    pub history_policy: RememberHistoryPolicy,
    /// The full managed-memory protocol is already embedded in `system_prompt`.
    pub append_runtime_auto_memory_prompt: bool,
    /// Clean context mode blanks the parent's user-memory prompt.
    pub include_config_user_memory: bool,
    pub disable_hooks: bool,
    pub suppress_chat_recording: bool,
    pub scoped_paths: RememberScopedPaths,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RememberAgentStatus {
    Completed,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RememberAgentRunResult {
    pub status: RememberAgentStatus,
    pub terminate_reason: Option<String>,
    pub files_written: Vec<String>,
}

pub type RememberAgentFuture<'a> =
    Pin<Box<dyn Future<Output = Result<RememberAgentRunResult, String>> + Send + 'a>>;

/// Runtime injection point for settings and native agent execution. A runtime
/// implementation must enforce `request.scoped_paths`, history policy, tool
/// allowlist, hook suppression, and recording suppression; this port does not
/// fabricate a model/tool loop.
pub trait RememberAgentRuntime: Send + Sync {
    fn managed_memory_available(&self) -> bool;

    fn max_turns(&self) -> Option<u32> {
        None
    }

    fn timeout_minutes(&self) -> Option<u32> {
        None
    }

    fn execute_remember_agent<'a>(
        &'a self,
        request: RememberAgentRequest,
        abort_signal: Option<watch::Receiver<bool>>,
    ) -> RememberAgentFuture<'a>;
}

#[derive(Debug, Error)]
pub enum RememberError {
    #[error("Managed memory is unavailable")]
    ManagedMemoryUnavailable,
    #[error("Failed to scaffold project memory: {0}")]
    ProjectScaffold(#[source] io::Error),
    #[error("Failed to read project memory index: {0}")]
    ProjectIndexRead(#[source] io::Error),
    #[error("Remember agent execution failed: {0}")]
    AgentExecution(String),
    #[error("{0}")]
    AgentFailed(String),
    #[error("{0}")]
    AgentCancelled(String),
    #[error("Remember agent touched a non-memory path: {0}")]
    PathEscape(String),
    #[error("Failed to rebuild project memory index: {0}")]
    ProjectIndexRebuild(#[source] io::Error),
}

impl RememberError {
    /// Stable source-compatible error codes where the TypeScript API exposes
    /// one. Other variants are ordinary runtime/filesystem failures.
    pub fn code(&self) -> Option<&'static str> {
        match self {
            Self::ManagedMemoryUnavailable => Some("managed_memory_unavailable"),
            Self::PathEscape(_) => Some("remember_path_escape"),
            _ => None,
        }
    }
}

/// Construct the managed remember task prompt, including project and user
/// destination hints when a project is available.
pub fn build_managed_remember_prompt(
    fact: &str,
    project_memory_root: Option<&Path>,
    user_memory_root: &Path,
    wrap_user_content: bool,
) -> String {
    let trimmed = trim_ecmascript_whitespace(fact);
    let dir_hint = project_memory_root
        .map(|memory_root| {
            format!(
                " Choose the destination directory by the type's `<scope>`: USER memory at `{}` for cross-project facts, PROJECT memory at `{}` for this-project-only facts.",
                user_memory_root.to_string_lossy(),
                memory_root.to_string_lossy()
            )
        })
        .unwrap_or_default();
    let content = if wrap_user_content {
        format!("<user-content>\n{trimmed}\n</user-content>")
    } else {
        trimmed.to_owned()
    };
    format!(
        "Please save the following to your memory system.{dir_hint} Choose the most appropriate memory type (user, feedback, project, or reference) based on the content:\n\n{content}"
    )
}

/// Construct the legacy, unmanaged-memory task prompt.
pub fn build_bare_remember_prompt(fact: &str) -> String {
    format!(
        "Please save the following fact to memory (e.g. append to CANOPY.md in the project root):\n\n{}",
        trim_ecmascript_whitespace(fact)
    )
}

/// Build the remember agent's full system prompt. The managed protocol is
/// forced even when both memory indexes are empty.
pub async fn build_clean_memory_system_prompt(
    paths: &AutoMemoryPaths,
) -> Result<String, RememberError> {
    ensure_auto_memory_scaffold(paths)
        .await
        .map_err(RememberError::ProjectScaffold)?;
    // User memory is optional. An inaccessible home directory must not prevent
    // project-scoped memory from working.
    let _ = ensure_user_auto_memory_scaffold(paths).await;

    let (project_index, user_index) = tokio::join!(
        read_auto_memory_index(paths),
        read_user_auto_memory_index(paths),
    );
    let project_index = project_index.map_err(RememberError::ProjectIndexRead)?;
    let user_index = user_index.unwrap_or(None);
    let project_dir = paths.auto_memory_root().to_string_lossy().into_owned();
    let user_dir = paths.user_auto_memory_root().to_string_lossy().into_owned();
    let user_section = UserAutoMemorySection {
        memory_dir: &user_dir,
        index_content: user_index.as_deref(),
    };
    let memory_prompt = build_managed_auto_memory_prompt(
        &project_dir,
        project_index.as_deref(),
        Some(&user_section),
        None,
        BuildMemoryPromptOptions {
            force_full_protocol: true,
        },
    );
    Ok(build_remember_system_prompt(&memory_prompt))
}

pub fn build_remember_system_prompt(memory_prompt: &str) -> String {
    [
        "You are saving one explicit durable memory for Canopy Code.",
        "",
        "Rules:",
        "- Save only information provided in the task prompt.",
        "- Use the managed auto-memory system only; do not write CANOPY.md or AGENTS.md.",
        "- Do not inspect or depend on any user-visible chat session history.",
        "- Use read/list/search/write/edit tools only inside the managed memory directories.",
        "- When finished, report only whether the memory update completed; do not quote or summarize memory content.",
        "",
        memory_prompt,
    ]
    .join("\n")
}

pub async fn run_managed_remember_by_agent<R: RememberAgentRuntime>(
    runtime: &R,
    paths: &AutoMemoryPaths,
    content: &str,
    context_mode: WorkspaceRememberContextMode,
    abort_signal: Option<watch::Receiver<bool>>,
) -> Result<ManagedRememberResult, RememberError> {
    if !runtime.managed_memory_available() {
        return Err(RememberError::ManagedMemoryUnavailable);
    }

    let system_prompt = build_clean_memory_system_prompt(paths).await?;
    let clean = context_mode == WorkspaceRememberContextMode::Clean;
    let request = RememberAgentRequest {
        name: MANAGED_REMEMBER_AGENT_NAME,
        task_prompt: build_managed_remember_prompt(
            content,
            Some(&paths.auto_memory_root()),
            &paths.user_auto_memory_root(),
            true,
        ),
        system_prompt,
        max_turns: runtime.max_turns().unwrap_or(DEFAULT_REMEMBER_MAX_TURNS),
        max_time_minutes: runtime
            .timeout_minutes()
            .unwrap_or(DEFAULT_REMEMBER_TIMEOUT_MINUTES),
        tools: &REMEMBER_AGENT_TOOLS,
        history_policy: if clean {
            RememberHistoryPolicy::ExplicitlyEmpty
        } else {
            RememberHistoryPolicy::WorkspaceBootstrapWithoutParentHistory
        },
        // The system prompt already embeds this once, in every context mode.
        append_runtime_auto_memory_prompt: false,
        include_config_user_memory: !clean,
        disable_hooks: clean,
        suppress_chat_recording: true,
        scoped_paths: RememberScopedPaths {
            project_root: paths.project_root().to_path_buf(),
            trusted_project_anchor: paths.auto_memory_trusted_anchor().to_path_buf(),
            project_memory_root: paths.auto_memory_root(),
            user_memory_root: paths.user_auto_memory_root(),
            bypass_base_ask_for_scoped_paths: true,
            restrict_reads_to_memory_paths: true,
            include_user_memory: true,
            allow_shell: false,
            protect_pinned_memory: false,
        },
    };

    let result = runtime
        .execute_remember_agent(request, abort_signal)
        .await
        .map_err(RememberError::AgentExecution)?;
    match result.status {
        RememberAgentStatus::Failed => {
            return Err(RememberError::AgentFailed(
                result
                    .terminate_reason
                    .unwrap_or_else(|| "Remember agent failed".to_owned()),
            ));
        }
        RememberAgentStatus::Cancelled => {
            return Err(RememberError::AgentCancelled(
                result
                    .terminate_reason
                    .unwrap_or_else(|| "Remember agent cancelled".to_owned()),
            ));
        }
        RememberAgentStatus::Completed => {}
    }

    let touched_scopes = classify_touched_scopes(&result.files_written, paths)?;
    let project_touched = touched_scopes.contains(&WorkspaceRememberScope::Project);
    let user_touched = touched_scopes.contains(&WorkspaceRememberScope::User);
    let (project_rebuild, user_rebuild) = tokio::join!(
        async {
            if project_touched {
                rebuild_managed_auto_memory_index(paths).await.map(|_| ())
            } else {
                Ok(())
            }
        },
        async {
            if user_touched {
                rebuild_user_auto_memory_index(paths).await.map(|_| ())
            } else {
                Ok(())
            }
        },
    );
    project_rebuild.map_err(RememberError::ProjectIndexRebuild)?;
    if let Err(error) = user_rebuild {
        eprintln!("[AUTO_MEMORY_REMEMBER] User memory index rebuild failed: {error}");
    }

    Ok(ManagedRememberResult {
        summary: if result.files_written.is_empty() {
            "No memory files updated.".to_owned()
        } else {
            "Memory update completed.".to_owned()
        },
        files_touched: result.files_written,
        touched_scopes,
    })
}

/// Check all successfully written paths before indexing either scope. A
/// single escape aborts the operation, matching the source's fail-closed
/// validation.
pub fn classify_touched_scopes(
    files_touched: &[String],
    paths: &AutoMemoryPaths,
) -> Result<Vec<WorkspaceRememberScope>, RememberError> {
    let project_root = trusted_project_memory_root(paths);
    let user_root = resolve_realpath_or_resolved(&paths.user_auto_memory_root());
    let mut scopes = Vec::new();
    for file_path in files_touched {
        let Some(candidate) = resolve_realpath_existing_or_new(Path::new(file_path)) else {
            return Err(RememberError::PathEscape(file_path.clone()));
        };
        if is_within_root(&candidate, &project_root) {
            scopes.push(WorkspaceRememberScope::Project);
        } else if is_within_root(&candidate, &user_root) {
            scopes.push(WorkspaceRememberScope::User);
        } else {
            return Err(RememberError::PathEscape(file_path.clone()));
        }
    }
    scopes.sort_unstable();
    scopes.dedup();
    Ok(scopes)
}

fn trusted_project_memory_root(paths: &AutoMemoryPaths) -> PathBuf {
    let literal_root = absolute_normalized(&paths.auto_memory_root());
    let anchor = absolute_normalized(paths.auto_memory_trusted_anchor());
    if let Ok(suffix) = literal_root.strip_prefix(&anchor)
        && !suffix.as_os_str().is_empty()
        && !suffix
            .components()
            .any(|component| matches!(component, Component::ParentDir | Component::RootDir))
    {
        return absolute_normalized(&resolve_realpath_or_resolved(&anchor).join(suffix));
    }
    resolve_realpath_or_resolved(&literal_root)
}

fn resolve_realpath_existing_or_new(path: &Path) -> Option<PathBuf> {
    let path = absolute_normalized(path);
    match std::fs::canonicalize(&path) {
        Ok(resolved) => Some(resolved),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            if std::fs::symlink_metadata(&path)
                .is_ok_and(|metadata| metadata.file_type().is_symlink())
            {
                return None;
            }
            resolve_new_path(&path)
        }
        Err(_) => None,
    }
}

fn resolve_realpath_or_resolved(path: &Path) -> PathBuf {
    resolve_realpath_existing_or_new(path)
        .or_else(|| resolve_new_path(path))
        .unwrap_or_else(|| absolute_normalized(path))
}

fn resolve_new_path(path: &Path) -> Option<PathBuf> {
    let path = absolute_normalized(path);
    let mut current = path.parent()?.to_path_buf();
    let mut remainder = PathBuf::from(path.file_name()?);
    loop {
        match std::fs::canonicalize(&current) {
            Ok(parent) => return Some(absolute_normalized(&parent.join(remainder))),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let name = current.file_name()?.to_os_string();
                remainder = PathBuf::from(name).join(remainder);
                let parent = current.parent()?.to_path_buf();
                if parent == current {
                    return None;
                }
                current = parent;
            }
            Err(_) => return None,
        }
    }
}

fn absolute_normalized(path: &Path) -> PathBuf {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        cwd.join(path)
    };
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

fn is_within_root(path: &Path, root: &Path) -> bool {
    let path = absolute_normalized(path);
    let root = absolute_normalized(root);
    if !cfg!(windows) {
        return path.strip_prefix(root).is_ok();
    }
    let path_components = path.components().collect::<Vec<_>>();
    let root_components = root.components().collect::<Vec<_>>();
    path_components.len() >= root_components.len()
        && path_components
            .iter()
            .zip(root_components.iter())
            .all(|(candidate, root)| {
                candidate
                    .as_os_str()
                    .to_string_lossy()
                    .eq_ignore_ascii_case(&root.as_os_str().to_string_lossy())
            })
}

fn trim_ecmascript_whitespace(value: &str) -> &str {
    value.trim_matches(|character| {
        matches!(
            character,
            '\u{0009}'..='\u{000d}'
                | '\u{0020}'
                | '\u{00a0}'
                | '\u{1680}'
                | '\u{2000}'..='\u{200a}'
                | '\u{2028}'
                | '\u{2029}'
                | '\u{202f}'
                | '\u{205f}'
                | '\u{3000}'
                | '\u{feff}'
        )
    })
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::memory::paths::MemoryPathInputs;

    fn temp(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!("canopy-remember-{label}-{}", uuid::Uuid::new_v4()))
    }

    fn paths(root: &Path) -> AutoMemoryPaths {
        AutoMemoryPaths::from_inputs(MemoryPathInputs {
            project_root: root.join("project"),
            runtime_base_dir: root.join("runtime"),
            memory_base_dir_override: None,
            memory_local: true,
            project_scope: Some("workspace".to_owned()),
            cwd: root.to_path_buf(),
            home_dir: Some(root.join("home")),
        })
    }

    #[test]
    fn remember_task_prompts_match_source_format_and_ecmascript_trim() {
        let project = Path::new("/tmp/project/.canopy/memory");
        let user = Path::new("/tmp/home/.canopy/memories");
        assert_eq!(
            build_managed_remember_prompt(
                "\u{feff}  Keep this durable. \n",
                Some(project),
                user,
                true
            ),
            "Please save the following to your memory system. Choose the destination directory by the type's `<scope>`: USER memory at `/tmp/home/.canopy/memories` for cross-project facts, PROJECT memory at `/tmp/project/.canopy/memory` for this-project-only facts. Choose the most appropriate memory type (user, feedback, project, or reference) based on the content:\n\n<user-content>\nKeep this durable.\n</user-content>"
        );
        assert_eq!(
            build_bare_remember_prompt("  Fact.\n"),
            "Please save the following fact to memory (e.g. append to CANOPY.md in the project root):\n\nFact."
        );
        assert!(
            build_managed_remember_prompt("fact", None, user, false).contains(
                "Please save the following to your memory system. Choose the most appropriate"
            )
        );
    }

    #[test]
    fn system_prompt_embeds_full_protocol_wrapper_verbatim() {
        let wrapped = build_remember_system_prompt("# auto memory\nprotocol");
        assert!(
            wrapped.starts_with(
                "You are saving one explicit durable memory for Canopy Code.\n\nRules:"
            )
        );
        assert!(
            wrapped
                .contains("- Do not inspect or depend on any user-visible chat session history.")
        );
        assert!(wrapped.ends_with("# auto memory\nprotocol"));
    }

    #[tokio::test]
    async fn clean_prompt_forces_full_protocol_with_scaffolded_empty_indexes() {
        let root = temp("prompt");
        let memory_paths = paths(&root);
        let prompt = build_clean_memory_system_prompt(&memory_paths)
            .await
            .unwrap();
        assert!(prompt.contains("# auto memory"));
        assert!(prompt.contains("## Types of memory"));
        assert!(prompt.contains("USER memory (cross-project"));
        assert!(prompt.contains("PROJECT memory (this project only"));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn touched_paths_are_classified_and_outside_paths_fail_closed() {
        let root = temp("classify");
        let memory_paths = paths(&root);
        let project_file = memory_paths.auto_memory_root().join("project.md");
        let user_file = memory_paths.user_auto_memory_root().join("user.md");
        std::fs::create_dir_all(project_file.parent().unwrap()).unwrap();
        std::fs::create_dir_all(user_file.parent().unwrap()).unwrap();
        std::fs::write(&project_file, "project").unwrap();
        std::fs::write(&user_file, "user").unwrap();
        assert_eq!(
            classify_touched_scopes(
                &[
                    project_file.to_string_lossy().into_owned(),
                    user_file.to_string_lossy().into_owned()
                ],
                &memory_paths,
            )
            .unwrap(),
            vec![
                WorkspaceRememberScope::Project,
                WorkspaceRememberScope::User
            ]
        );
        let escape = classify_touched_scopes(
            &[root.join("outside.md").to_string_lossy().into_owned()],
            &memory_paths,
        )
        .unwrap_err();
        assert_eq!(escape.code(), Some("remember_path_escape"));
        assert!(
            escape
                .to_string()
                .contains("Remember agent touched a non-memory path:")
        );
        let _ = std::fs::remove_dir_all(root);
    }

    struct TestRuntime {
        available: bool,
        max_turns: Option<u32>,
        timeout: Option<u32>,
        result: RememberAgentRunResult,
        request: Arc<Mutex<Option<RememberAgentRequest>>>,
    }

    impl RememberAgentRuntime for TestRuntime {
        fn managed_memory_available(&self) -> bool {
            self.available
        }

        fn max_turns(&self) -> Option<u32> {
            self.max_turns
        }

        fn timeout_minutes(&self) -> Option<u32> {
            self.timeout
        }

        fn execute_remember_agent<'a>(
            &'a self,
            request: RememberAgentRequest,
            _abort_signal: Option<watch::Receiver<bool>>,
        ) -> RememberAgentFuture<'a> {
            *self.request.lock().unwrap() = Some(request);
            let result = self.result.clone();
            Box::pin(async move { Ok(result) })
        }
    }

    fn test_runtime(
        result: RememberAgentRunResult,
        max_turns: Option<u32>,
        timeout: Option<u32>,
    ) -> TestRuntime {
        TestRuntime {
            available: true,
            max_turns,
            timeout,
            result,
            request: Arc::new(Mutex::new(None)),
        }
    }

    #[tokio::test]
    async fn workspace_and_clean_modes_build_explicit_runtime_policies() {
        for (mode, history, hooks, user_memory) in [
            (
                WorkspaceRememberContextMode::Workspace,
                RememberHistoryPolicy::WorkspaceBootstrapWithoutParentHistory,
                false,
                true,
            ),
            (
                WorkspaceRememberContextMode::Clean,
                RememberHistoryPolicy::ExplicitlyEmpty,
                true,
                false,
            ),
        ] {
            let root = temp("policy");
            let memory_paths = paths(&root);
            let runtime = test_runtime(
                RememberAgentRunResult {
                    status: RememberAgentStatus::Completed,
                    terminate_reason: None,
                    files_written: Vec::new(),
                },
                Some(3),
                Some(2),
            );
            let outcome =
                run_managed_remember_by_agent(&runtime, &memory_paths, "fact", mode, None)
                    .await
                    .unwrap();
            assert_eq!(outcome.summary, "No memory files updated.");
            let request = runtime.request.lock().unwrap().clone().unwrap();
            assert_eq!(request.name, MANAGED_REMEMBER_AGENT_NAME);
            assert_eq!(request.tools, &REMEMBER_AGENT_TOOLS);
            assert_eq!((request.max_turns, request.max_time_minutes), (3, 2));
            assert_eq!(request.history_policy, history);
            assert_eq!(request.disable_hooks, hooks);
            assert_eq!(request.include_config_user_memory, user_memory);
            assert!(!request.append_runtime_auto_memory_prompt);
            assert!(request.suppress_chat_recording);
            assert!(request.scoped_paths.bypass_base_ask_for_scoped_paths);
            assert!(request.scoped_paths.restrict_reads_to_memory_paths);
            assert!(!request.scoped_paths.allow_shell);
            let _ = std::fs::remove_dir_all(root);
        }
    }

    #[tokio::test]
    async fn agent_status_and_memory_scope_index_rebuild_follow_source_policy() {
        let root = temp("result");
        let memory_paths = paths(&root);
        let project_file = memory_paths.auto_memory_root().join("project/topic.md");
        std::fs::create_dir_all(project_file.parent().unwrap()).unwrap();
        std::fs::write(
            &project_file,
            "---\nname: Topic\ndescription: Project topic\ntype: project\n---\nBody\n",
        )
        .unwrap();
        let runtime = test_runtime(
            RememberAgentRunResult {
                status: RememberAgentStatus::Completed,
                terminate_reason: None,
                files_written: vec![project_file.to_string_lossy().into_owned()],
            },
            None,
            None,
        );
        let result = run_managed_remember_by_agent(
            &runtime,
            &memory_paths,
            "fact",
            WorkspaceRememberContextMode::Workspace,
            None,
        )
        .await
        .unwrap();
        assert_eq!(result.summary, "Memory update completed.");
        assert_eq!(result.touched_scopes, vec![WorkspaceRememberScope::Project]);
        let index = tokio::fs::read_to_string(memory_paths.auto_memory_index_path())
            .await
            .unwrap();
        assert!(index.contains("Topic"));

        for (status, fallback, expected) in [
            (
                RememberAgentStatus::Failed,
                "Remember agent failed",
                "Remember agent failed",
            ),
            (
                RememberAgentStatus::Cancelled,
                "Remember agent cancelled",
                "Remember agent cancelled",
            ),
        ] {
            let failed_runtime = test_runtime(
                RememberAgentRunResult {
                    status,
                    terminate_reason: None,
                    files_written: Vec::new(),
                },
                None,
                None,
            );
            let error = run_managed_remember_by_agent(
                &failed_runtime,
                &memory_paths,
                "fact",
                WorkspaceRememberContextMode::Workspace,
                None,
            )
            .await
            .unwrap_err();
            assert_eq!(error.to_string(), expected);
            assert_eq!(fallback, expected);
        }
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn user_index_read_and_rebuild_failures_are_best_effort() {
        let root = temp("user-best-effort");
        let memory_paths = paths(&root);
        let user_root = memory_paths.user_auto_memory_root();
        let user_index = memory_paths.user_auto_memory_index_path();
        std::fs::create_dir_all(&user_index).unwrap();
        let user_file = user_root.join("feedback/preference.md");
        std::fs::create_dir_all(user_file.parent().unwrap()).unwrap();
        std::fs::write(
            &user_file,
            "---\nname: Preference\ndescription: Durable preference\ntype: feedback\n---\nBody\n",
        )
        .unwrap();

        let prompt = build_clean_memory_system_prompt(&memory_paths)
            .await
            .unwrap();
        assert!(prompt.contains("# auto memory"));
        assert!(prompt.contains("Your MEMORY.md is currently empty."));

        let runtime = test_runtime(
            RememberAgentRunResult {
                status: RememberAgentStatus::Completed,
                terminate_reason: None,
                files_written: vec![user_file.to_string_lossy().into_owned()],
            },
            None,
            None,
        );
        let result = run_managed_remember_by_agent(
            &runtime,
            &memory_paths,
            "fact",
            WorkspaceRememberContextMode::Workspace,
            None,
        )
        .await
        .unwrap();
        assert_eq!(result.summary, "Memory update completed.");
        assert_eq!(result.touched_scopes, vec![WorkspaceRememberScope::User]);
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn unavailable_runtime_fails_before_scaffolding() {
        let root = temp("unavailable");
        let memory_paths = paths(&root);
        let mut runtime = test_runtime(
            RememberAgentRunResult {
                status: RememberAgentStatus::Completed,
                terminate_reason: None,
                files_written: Vec::new(),
            },
            None,
            None,
        );
        runtime.available = false;
        let error = run_managed_remember_by_agent(
            &runtime,
            &memory_paths,
            "fact",
            WorkspaceRememberContextMode::Workspace,
            None,
        )
        .await
        .unwrap_err();
        assert_eq!(error.code(), Some("managed_memory_unavailable"));
        assert!(!memory_paths.auto_memory_root().exists());
        let _ = std::fs::remove_dir_all(root);
    }
}
