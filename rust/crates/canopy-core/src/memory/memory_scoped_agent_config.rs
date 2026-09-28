//! Memory-scoped permission policy for managed-memory agents.
//!
//! This mirrors `packages/core/src/memory/memory-scoped-agent-config.ts`.
//! Project and user memory roots come from [`AutoMemoryPaths`]; shell parsing
//! and the configured base permission manager are injected by the caller.

use std::future::Future;
use std::path::{Component, Path, PathBuf};
use std::pin::Pin;

use crate::memory::paths::{AUTO_MEMORY_PINNED_DIRNAME, AutoMemoryPaths};
use crate::permissions::{PermissionCheckContext, PermissionDecision};

const TOOL_READ_FILE: &str = "read_file";
const TOOL_GREP: &str = "grep_search";
const TOOL_LS: &str = "list_directory";
const TOOL_EDIT: &str = "edit";
const TOOL_WRITE_FILE: &str = "write_file";
const TOOL_SHELL: &str = "run_shell_command";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MemoryScopedAgentConfigOptions {
    pub allow_shell: bool,
    pub bypass_base_ask_for_scoped_paths: bool,
    pub include_user_memory: bool,
    pub protect_pinned_memory: bool,
    pub restrict_reads_to_memory_paths: bool,
}

impl Default for MemoryScopedAgentConfigOptions {
    fn default() -> Self {
        Self {
            allow_shell: false,
            bypass_base_ask_for_scoped_paths: false,
            include_user_memory: true,
            protect_pinned_memory: false,
            restrict_reads_to_memory_paths: false,
        }
    }
}

/// Narrow adapter over the active permission manager. Scoped decisions are
/// merged with these base decisions using the same deny/ask/allow/default
/// priority as the TypeScript permission manager wrapper.
pub trait MemoryScopedBasePermissionManager: Send + Sync {
    fn has_relevant_rules(&self, context: &PermissionCheckContext<'_>) -> bool;
    fn has_matching_ask_rule(&self, context: &PermissionCheckContext<'_>) -> bool;
    fn find_matching_deny_rule(&self, context: &PermissionCheckContext<'_>) -> Option<String>;
    fn evaluate(&self, context: &PermissionCheckContext<'_>) -> PermissionDecision;
    fn is_tool_enabled(&self, tool_name: &str) -> bool;
}

pub type ShellReadOnlyFuture<'a> = Pin<Box<dyn Future<Output = bool> + Send + 'a>>;

/// Adapter for the shell utility layer. Implementations should apply the
/// source `stripShellWrapper` behavior in `strip_shell_wrapper`, then classify
/// the resulting command as read-only in the requested directory. A failed or
/// unavailable parser should return `false` unless its own safe fallback can
/// establish that the command is read-only.
pub trait MemoryShellReadOnlyChecker: Send + Sync {
    fn strip_shell_wrapper(&self, command: &str) -> String;

    fn is_read_only_ast_in_directory<'a>(
        &'a self,
        command: &'a str,
        directory: &'a Path,
    ) -> ShellReadOnlyFuture<'a>;
}

#[derive(Clone, Debug)]
struct PinnedMemoryRoot {
    literal_path: PathBuf,
    resolved_path: Option<PathBuf>,
}

/// Snapshot of a memory agent's path and permission policy.
///
/// Construct with [`from_process`](Self::from_process) for the active runtime
/// paths, or [`new`](Self::new) when a caller already has `AutoMemoryPaths`.
#[derive(Clone, Debug)]
pub struct MemoryScopedAgentConfig {
    project_root: PathBuf,
    project_memory_root: PathBuf,
    project_memory_trusted_anchor: PathBuf,
    user_memory_root: PathBuf,
    options: MemoryScopedAgentConfigOptions,
    pinned_roots: Vec<PinnedMemoryRoot>,
}

impl MemoryScopedAgentConfig {
    pub fn from_process(
        project_root: impl Into<PathBuf>,
        options: MemoryScopedAgentConfigOptions,
    ) -> Self {
        Self::new(AutoMemoryPaths::from_process(project_root), options)
    }

    pub fn new(paths: AutoMemoryPaths, options: MemoryScopedAgentConfigOptions) -> Self {
        let project_root = absolute_lexical(paths.project_root());
        let project_memory_root = absolute_lexical(&paths.auto_memory_root());
        let project_memory_trusted_anchor = absolute_lexical(paths.auto_memory_trusted_anchor());
        let user_memory_root = absolute_lexical(&paths.user_auto_memory_root());
        let pinned_roots = if options.protect_pinned_memory {
            create_pinned_memory_roots(
                &project_memory_root,
                &user_memory_root,
                options.include_user_memory,
            )
        } else {
            Vec::new()
        };

        Self {
            project_root,
            project_memory_root,
            project_memory_trusted_anchor,
            user_memory_root,
            options,
            pinned_roots,
        }
    }

    pub fn options(&self) -> MemoryScopedAgentConfigOptions {
        self.options
    }

    pub fn project_root(&self) -> &Path {
        &self.project_root
    }

    /// Resolve an existing path through symlinks, or resolve a missing path
    /// against its nearest existing parent. Dangling symlink leaves fail
    /// closed, matching the TypeScript `realpathExistingOrNew` helper.
    pub fn is_allowed_memory_path(&self, file_path: Option<&Path>) -> bool {
        let Some(file_path) = file_path else {
            return false;
        };
        let Some(candidate) = realpath_existing_or_new(file_path) else {
            return false;
        };
        self.is_allowed_resolved_memory_path(&candidate)
    }

    pub fn has_relevant_rules(
        &self,
        context: &PermissionCheckContext<'_>,
        base: Option<&dyn MemoryScopedBasePermissionManager>,
    ) -> bool {
        is_scoped_tool(context.tool_name, self.options)
            || base.is_some_and(|base| base.has_relevant_rules(context))
    }

    pub fn has_matching_ask_rule(
        &self,
        context: &PermissionCheckContext<'_>,
        base: Option<&dyn MemoryScopedBasePermissionManager>,
    ) -> bool {
        base.is_some_and(|base| base.has_matching_ask_rule(context))
    }

    pub fn find_matching_deny_rule(
        &self,
        context: &PermissionCheckContext<'_>,
        base: Option<&dyn MemoryScopedBasePermissionManager>,
    ) -> Option<String> {
        self.scoped_deny_rule(context)
            .or_else(|| base.and_then(|base| base.find_matching_deny_rule(context)))
    }

    pub async fn evaluate(
        &self,
        context: &PermissionCheckContext<'_>,
        base: Option<&dyn MemoryScopedBasePermissionManager>,
        shell_checker: &dyn MemoryShellReadOnlyChecker,
    ) -> PermissionDecision {
        let scoped = self.evaluate_scoped_decision(context, shell_checker).await;
        let Some(base) = base else {
            return scoped;
        };
        let base_decision = if base.has_relevant_rules(context) {
            base.evaluate(context)
        } else {
            PermissionDecision::Default
        };
        merge_permission_decision(scoped, base_decision, self.options)
    }

    pub fn is_tool_enabled(
        &self,
        tool_name: &str,
        base: Option<&dyn MemoryScopedBasePermissionManager>,
    ) -> bool {
        if tool_name == TOOL_SHELL {
            return self.options.allow_shell;
        }
        if is_scoped_tool(tool_name, self.options) {
            return true;
        }
        base.is_none_or(|base| base.is_tool_enabled(tool_name))
    }

    fn is_allowed_resolved_memory_path(&self, candidate: &Path) -> bool {
        let project_root = resolve_trusted_memory_root(
            &self.project_memory_root,
            &self.project_memory_trusted_anchor,
        );
        is_within_root(candidate, &project_root)
            || (self.options.include_user_memory
                && is_within_root(candidate, &realpath_or_resolved(&self.user_memory_root)))
    }

    async fn evaluate_scoped_decision(
        &self,
        context: &PermissionCheckContext<'_>,
        shell_checker: &dyn MemoryShellReadOnlyChecker,
    ) -> PermissionDecision {
        match context.tool_name {
            TOOL_SHELL => {
                let Some(command) = context.command.filter(|_| self.options.allow_shell) else {
                    return PermissionDecision::Deny;
                };
                let command = shell_checker.strip_shell_wrapper(command);
                if shell_checker
                    .is_read_only_ast_in_directory(&command, context.cwd)
                    .await
                {
                    PermissionDecision::Allow
                } else {
                    PermissionDecision::Deny
                }
            }
            TOOL_READ_FILE | TOOL_GREP | TOOL_LS => {
                if !self.options.restrict_reads_to_memory_paths {
                    return PermissionDecision::Default;
                }
                if self.is_allowed_memory_path(context.file_path) {
                    PermissionDecision::Allow
                } else {
                    PermissionDecision::Deny
                }
            }
            TOOL_EDIT | TOOL_WRITE_FILE => {
                let resolved_candidate = context.file_path.and_then(realpath_existing_or_new);
                let is_pinned = self.options.protect_pinned_memory
                    && is_protected_pinned_memory_path(
                        context.file_path,
                        &self.pinned_roots,
                        resolved_candidate.as_deref(),
                    );
                if is_pinned {
                    return PermissionDecision::Deny;
                }
                if resolved_candidate
                    .as_deref()
                    .is_some_and(|candidate| self.is_allowed_resolved_memory_path(candidate))
                {
                    PermissionDecision::Allow
                } else {
                    PermissionDecision::Deny
                }
            }
            _ => PermissionDecision::Default,
        }
    }

    fn scoped_deny_rule(&self, context: &PermissionCheckContext<'_>) -> Option<String> {
        let allowed_roots = if self.options.include_user_memory {
            format!(
                "{} or {}",
                self.user_memory_root.display(),
                self.project_memory_root.display()
            )
        } else {
            self.project_memory_root.display().to_string()
        };

        match context.tool_name {
            TOOL_SHELL => Some(if self.options.allow_shell {
                "ManagedAutoMemory(run_shell_command: read-only only)".to_owned()
            } else {
                "ManagedAutoMemory(run_shell_command: disabled)".to_owned()
            }),
            TOOL_READ_FILE if self.options.restrict_reads_to_memory_paths => Some(format!(
                "ManagedAutoMemory(read_file: only within {allowed_roots})"
            )),
            TOOL_GREP if self.options.restrict_reads_to_memory_paths => Some(format!(
                "ManagedAutoMemory(grep_search: only within {allowed_roots})"
            )),
            TOOL_LS if self.options.restrict_reads_to_memory_paths => Some(format!(
                "ManagedAutoMemory(list_directory: only within {allowed_roots})"
            )),
            TOOL_EDIT | TOOL_WRITE_FILE => {
                let resolved_candidate = context.file_path.and_then(realpath_existing_or_new);
                let is_allowed = resolved_candidate
                    .as_deref()
                    .is_some_and(|candidate| self.is_allowed_resolved_memory_path(candidate));
                if is_allowed
                    && self.options.protect_pinned_memory
                    && is_protected_pinned_memory_path(
                        context.file_path,
                        &self.pinned_roots,
                        resolved_candidate.as_deref(),
                    )
                {
                    Some(format!(
                        "ManagedAutoMemory({}: pinned memory is read-only)",
                        context.tool_name
                    ))
                } else {
                    Some(format!(
                        "ManagedAutoMemory({}: only within {allowed_roots})",
                        context.tool_name
                    ))
                }
            }
            _ => None,
        }
    }
}

fn is_scoped_tool(tool_name: &str, options: MemoryScopedAgentConfigOptions) -> bool {
    (options.restrict_reads_to_memory_paths
        && matches!(tool_name, TOOL_READ_FILE | TOOL_GREP | TOOL_LS))
        || matches!(tool_name, TOOL_EDIT | TOOL_WRITE_FILE | TOOL_SHELL)
}

fn merge_permission_decision(
    scoped: PermissionDecision,
    base: PermissionDecision,
    options: MemoryScopedAgentConfigOptions,
) -> PermissionDecision {
    if options.bypass_base_ask_for_scoped_paths
        && scoped == PermissionDecision::Allow
        && base == PermissionDecision::Ask
    {
        return PermissionDecision::Allow;
    }
    fn priority(decision: PermissionDecision) -> u8 {
        match decision {
            PermissionDecision::Deny => 4,
            PermissionDecision::Ask => 3,
            PermissionDecision::Allow => 2,
            PermissionDecision::Default => 1,
        }
    }
    if priority(base) > priority(scoped) {
        base
    } else {
        scoped
    }
}

fn create_pinned_memory_roots(
    project_memory_root: &Path,
    user_memory_root: &Path,
    include_user_memory: bool,
) -> Vec<PinnedMemoryRoot> {
    let mut roots = vec![project_memory_root.to_path_buf()];
    if include_user_memory {
        roots.push(user_memory_root.to_path_buf());
    }
    roots
        .into_iter()
        .map(|memory_root| {
            let literal_path = absolute_lexical(&memory_root.join(AUTO_MEMORY_PINNED_DIRNAME));
            let resolved_path = realpath_existing_or_new(&literal_path);
            PinnedMemoryRoot {
                literal_path,
                resolved_path,
            }
        })
        .collect()
}

fn is_protected_pinned_memory_path(
    file_path: Option<&Path>,
    pinned_roots: &[PinnedMemoryRoot],
    resolved_candidate: Option<&Path>,
) -> bool {
    let Some(file_path) = file_path else {
        return false;
    };
    let literal_candidate = absolute_lexical(file_path);
    pinned_roots.iter().any(|pinned_root| {
        is_within_root_case_insensitive(&literal_candidate, &pinned_root.literal_path)
            || resolved_candidate.is_some_and(|candidate| {
                pinned_root
                    .resolved_path
                    .as_deref()
                    .is_some_and(|root| is_within_root_case_insensitive(candidate, root))
            })
    })
}

/// Resolve a candidate under the same rules used by the scoped write checks.
fn realpath_existing_or_new(file_path: &Path) -> Option<PathBuf> {
    let absolute = absolute_lexical(file_path);
    match std::fs::canonicalize(&absolute) {
        Ok(path) => Some(path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if std::fs::symlink_metadata(&absolute)
                .is_ok_and(|metadata| metadata.file_type().is_symlink())
            {
                return None;
            }
            realpath_new_path(&absolute)
        }
        Err(_) => None,
    }
}

fn realpath_new_path(file_path: &Path) -> Option<PathBuf> {
    let absolute = absolute_lexical(file_path);
    let mut current = absolute.parent()?.to_path_buf();
    let mut missing = vec![absolute.file_name()?.to_os_string()];

    loop {
        match std::fs::canonicalize(&current) {
            Ok(mut resolved) => {
                for component in missing.iter().rev() {
                    resolved.push(component);
                }
                return Some(resolved);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let name = current.file_name()?.to_os_string();
                let parent = current.parent()?.to_path_buf();
                if parent == current {
                    return None;
                }
                missing.push(name);
                current = parent;
            }
            Err(_) => return None,
        }
    }
}

fn realpath_or_resolved(file_path: &Path) -> PathBuf {
    let absolute = absolute_lexical(file_path);
    match std::fs::canonicalize(&absolute) {
        Ok(resolved) => resolved,
        Err(_) => realpath_new_path(&absolute).unwrap_or(absolute),
    }
}

/// Canonicalize only the trusted anchor, then append the managed suffix
/// literally. This prevents a repo-controlled `.canopy` or `memory` symlink
/// from moving the allowed write root outside its configured anchor.
fn resolve_trusted_memory_root(literal_root: &Path, anchor: &Path) -> PathBuf {
    let literal_root = absolute_lexical(literal_root);
    let anchor = absolute_lexical(anchor);
    let Some(suffix) = relative_suffix_if_beneath(&literal_root, &anchor) else {
        return realpath_or_resolved(&literal_root);
    };
    if suffix.as_os_str().is_empty() || suffix.is_absolute() {
        return realpath_or_resolved(&literal_root);
    }
    if suffix
        .components()
        .any(|component| matches!(component, Component::ParentDir) || component.as_os_str() == "..")
    {
        return realpath_or_resolved(&literal_root);
    }
    realpath_or_resolved(&anchor).join(suffix)
}

fn relative_suffix_if_beneath(path: &Path, root: &Path) -> Option<PathBuf> {
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
        |mut suffix, component| {
            suffix.push(component.as_os_str());
            suffix
        },
    ))
}

fn is_within_root(path: &Path, root: &Path) -> bool {
    let path = absolute_lexical(path);
    let root = absolute_lexical(root);
    if !cfg!(windows) {
        return path.starts_with(root);
    }
    let path_components = path.components().collect::<Vec<_>>();
    let root_components = root.components().collect::<Vec<_>>();
    path_components.len() >= root_components.len()
        && path_components
            .iter()
            .zip(root_components.iter())
            .all(|(path, root)| component_eq_ignore_case(*path, *root))
}

fn is_within_root_case_insensitive(path: &Path, root: &Path) -> bool {
    let path = absolute_lexical(path).to_string_lossy().to_lowercase();
    let root = absolute_lexical(root).to_string_lossy().to_lowercase();
    is_within_root(Path::new(&path), Path::new(&root))
}

fn component_eq_ignore_case(left: Component<'_>, right: Component<'_>) -> bool {
    left.as_os_str().to_string_lossy().to_lowercase()
        == right.as_os_str().to_string_lossy().to_lowercase()
}

fn absolute_lexical(path: &Path) -> PathBuf {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        cwd.join(path)
    };
    let mut result = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::Prefix(prefix) => result.push(prefix.as_os_str()),
            Component::RootDir => result.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                if !result.pop() && !result.has_root() {
                    result.push(component.as_os_str());
                }
            }
            Component::Normal(part) => result.push(part),
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::paths::{AutoMemoryPaths, MemoryProjectScope};
    use serde_json::Value;
    use std::sync::{Arc, Mutex};

    fn temp_dir(label: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "canopy-memory-scoped-{label}-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    fn make_policy(
        project_root: &Path,
        memory_base: &Path,
        local: bool,
        options: MemoryScopedAgentConfigOptions,
    ) -> MemoryScopedAgentConfig {
        MemoryScopedAgentConfig::new(
            AutoMemoryPaths::new(
                project_root,
                memory_base,
                local,
                MemoryProjectScope::Workspace,
            ),
            options,
        )
    }

    fn context<'a>(
        tool_name: &'a str,
        project_root: &'a Path,
        cwd: &'a Path,
        file_path: Option<&'a Path>,
        command: Option<&'a str>,
    ) -> PermissionCheckContext<'a> {
        PermissionCheckContext {
            tool_name,
            command,
            file_path,
            domain: None,
            specifier: None,
            tool_params: None::<&'a Value>,
            project_root,
            cwd,
        }
    }

    struct TestShellChecker {
        saw_command: Arc<Mutex<Option<String>>>,
        read_only: bool,
    }

    impl MemoryShellReadOnlyChecker for TestShellChecker {
        fn strip_shell_wrapper(&self, command: &str) -> String {
            command
                .strip_prefix("bash -c '")
                .and_then(|command| command.strip_suffix('\''))
                .unwrap_or(command)
                .to_owned()
        }

        fn is_read_only_ast_in_directory<'a>(
            &'a self,
            command: &'a str,
            _directory: &'a Path,
        ) -> ShellReadOnlyFuture<'a> {
            *self.saw_command.lock().unwrap() = Some(command.to_owned());
            let read_only = self.read_only;
            Box::pin(async move { read_only })
        }
    }

    #[derive(Clone, Copy)]
    struct TestBaseManager {
        relevant: bool,
        decision: PermissionDecision,
        enabled: bool,
    }

    impl MemoryScopedBasePermissionManager for TestBaseManager {
        fn has_relevant_rules(&self, _context: &PermissionCheckContext<'_>) -> bool {
            self.relevant
        }

        fn has_matching_ask_rule(&self, _context: &PermissionCheckContext<'_>) -> bool {
            self.decision == PermissionDecision::Ask
        }

        fn find_matching_deny_rule(&self, _context: &PermissionCheckContext<'_>) -> Option<String> {
            (self.decision == PermissionDecision::Deny).then(|| "base deny".to_owned())
        }

        fn evaluate(&self, _context: &PermissionCheckContext<'_>) -> PermissionDecision {
            self.decision
        }

        fn is_tool_enabled(&self, _tool_name: &str) -> bool {
            self.enabled
        }
    }

    fn shell_checker(read_only: bool) -> TestShellChecker {
        TestShellChecker {
            saw_command: Arc::new(Mutex::new(None)),
            read_only,
        }
    }

    #[tokio::test]
    async fn defaults_scope_reads_and_writes_to_managed_memory() {
        let temp = temp_dir("defaults");
        let project = temp.join("project");
        let base = temp.join("base");
        std::fs::create_dir_all(&project).unwrap();
        let policy = make_policy(
            &project,
            &base,
            false,
            MemoryScopedAgentConfigOptions::default(),
        );
        let project_memory = policy.project_memory_root.clone();
        let user_memory = policy.user_memory_root.clone();
        let cwd = project.as_path();
        let checker = shell_checker(false);

        assert!(policy.options().include_user_memory);
        assert!(!policy.options().restrict_reads_to_memory_paths);
        assert!(!policy.options().allow_shell);
        assert_eq!(
            policy
                .evaluate(
                    &context(
                        TOOL_READ_FILE,
                        &project,
                        cwd,
                        Some(&project.join("transcripts/latest.jsonl")),
                        None
                    ),
                    None,
                    &checker,
                )
                .await,
            PermissionDecision::Default
        );
        assert_eq!(
            policy
                .evaluate(
                    &context(
                        TOOL_WRITE_FILE,
                        &project,
                        cwd,
                        Some(&project_memory.join("topic.md")),
                        None
                    ),
                    None,
                    &checker,
                )
                .await,
            PermissionDecision::Allow
        );
        assert_eq!(
            policy
                .evaluate(
                    &context(
                        TOOL_EDIT,
                        &project,
                        cwd,
                        Some(&user_memory.join("topic.md")),
                        None
                    ),
                    None,
                    &checker,
                )
                .await,
            PermissionDecision::Allow
        );
        assert_eq!(
            policy
                .evaluate(
                    &context(
                        TOOL_WRITE_FILE,
                        &project,
                        cwd,
                        Some(&project.join("README.md")),
                        None
                    ),
                    None,
                    &checker,
                )
                .await,
            PermissionDecision::Deny
        );
        std::fs::remove_dir_all(temp).unwrap();
    }

    #[tokio::test]
    async fn restricts_reads_and_can_exclude_user_memory() {
        let temp = temp_dir("read-boundary");
        let project = temp.join("project");
        let base = temp.join("base");
        std::fs::create_dir_all(&project).unwrap();
        let options = MemoryScopedAgentConfigOptions {
            include_user_memory: false,
            restrict_reads_to_memory_paths: true,
            ..MemoryScopedAgentConfigOptions::default()
        };
        let policy = make_policy(&project, &base, false, options);
        let checker = shell_checker(false);
        let project_file = policy.project_memory_root.join("topic.md");
        let user_file = policy.user_memory_root.join("topic.md");
        let outside = project.join("README.md");

        assert!(policy.is_allowed_memory_path(Some(&project_file)));
        assert!(!policy.is_allowed_memory_path(Some(&user_file)));
        assert!(!policy.is_allowed_memory_path(None));
        for (path, expected) in [
            (Some(project_file.as_path()), PermissionDecision::Allow),
            (Some(user_file.as_path()), PermissionDecision::Deny),
            (Some(outside.as_path()), PermissionDecision::Deny),
            (None, PermissionDecision::Deny),
        ] {
            assert_eq!(
                policy
                    .evaluate(
                        &context(TOOL_GREP, &project, &project, path, None),
                        None,
                        &checker,
                    )
                    .await,
                expected
            );
        }
        assert!(policy.has_relevant_rules(&context(TOOL_LS, &project, &project, None, None), None));
        assert!(
            !policy.has_relevant_rules(&context("web_fetch", &project, &project, None, None), None)
        );
        std::fs::remove_dir_all(temp).unwrap();
    }

    #[tokio::test]
    async fn pinned_paths_and_symlink_aliases_are_read_only() {
        let temp = temp_dir("pinned");
        let project = temp.join("project");
        let base = temp.join("base");
        std::fs::create_dir_all(&project).unwrap();
        let options = MemoryScopedAgentConfigOptions {
            protect_pinned_memory: true,
            ..MemoryScopedAgentConfigOptions::default()
        };
        let policy = make_policy(&project, &base, false, options);
        let pinned = policy.project_memory_root.join(AUTO_MEMORY_PINNED_DIRNAME);
        let pinned_file = pinned.join("architecture.md");
        let alias = policy.project_memory_root.join("topic/pinned-alias");
        let alias_file = alias.join("architecture.md");
        let target_file = policy.project_memory_root.join("topic/shared.md");
        std::fs::create_dir_all(&pinned).unwrap();
        std::fs::create_dir_all(alias.parent().unwrap()).unwrap();
        std::fs::write(&pinned_file, "pinned").unwrap();
        std::fs::write(&target_file, "shared").unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(&pinned, &alias).unwrap();
        #[cfg(windows)]
        std::os::windows::fs::symlink_dir(&pinned, &alias).unwrap();

        let checker = shell_checker(false);
        for file_path in [pinned_file.as_path(), alias_file.as_path()] {
            let result = policy
                .evaluate(
                    &context(TOOL_EDIT, &project, &project, Some(file_path), None),
                    None,
                    &checker,
                )
                .await;
            assert_eq!(result, PermissionDecision::Deny);
        }
        assert_eq!(
            policy
                .evaluate(
                    &context(
                        TOOL_WRITE_FILE,
                        &project,
                        &project,
                        Some(&policy.project_memory_root.join("..topic/new.md")),
                        None
                    ),
                    None,
                    &checker,
                )
                .await,
            PermissionDecision::Allow
        );
        let case_variant_pinned = policy
            .project_memory_root
            .join(AUTO_MEMORY_PINNED_DIRNAME.to_uppercase())
            .join("future.md");
        assert_eq!(
            policy
                .evaluate(
                    &context(
                        TOOL_WRITE_FILE,
                        &project,
                        &project,
                        Some(&case_variant_pinned),
                        None
                    ),
                    None,
                    &checker,
                )
                .await,
            PermissionDecision::Deny
        );
        let user_pinned = policy
            .user_memory_root
            .join(AUTO_MEMORY_PINNED_DIRNAME)
            .join("preferences.md");
        std::fs::create_dir_all(user_pinned.parent().unwrap()).unwrap();
        std::fs::write(&user_pinned, "pinned user preference").unwrap();
        assert_eq!(
            policy
                .evaluate(
                    &context(TOOL_EDIT, &project, &project, Some(&user_pinned), None),
                    None,
                    &checker,
                )
                .await,
            PermissionDecision::Deny
        );
        assert_eq!(
            policy.find_matching_deny_rule(
                &context(TOOL_EDIT, &project, &project, Some(&pinned_file), None),
                None
            ),
            Some("ManagedAutoMemory(edit: pinned memory is read-only)".to_owned())
        );
        assert_eq!(
            policy.find_matching_deny_rule(
                &context(
                    TOOL_EDIT,
                    &project,
                    &project,
                    Some(&pinned.join("subdir/../new.md")),
                    None
                ),
                None
            ),
            Some("ManagedAutoMemory(edit: pinned memory is read-only)".to_owned())
        );
        std::fs::remove_dir_all(temp).unwrap();
    }

    #[tokio::test]
    async fn pinned_symlink_snapshot_protects_its_memory_target() {
        let temp = temp_dir("pinned-target");
        let project = temp.join("project");
        let base = temp.join("base");
        std::fs::create_dir_all(&project).unwrap();
        let paths = AutoMemoryPaths::new(&project, &base, false, MemoryProjectScope::Workspace);
        let memory_root = paths.auto_memory_root();
        let target = memory_root.join("topic/shared");
        let pinned = memory_root.join(AUTO_MEMORY_PINNED_DIRNAME);
        let target_file = target.join("fact.md");
        std::fs::create_dir_all(&target).unwrap();
        std::fs::write(&target_file, "shared target").unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(&target, &pinned).unwrap();
        #[cfg(windows)]
        std::os::windows::fs::symlink_dir(&target, &pinned).unwrap();
        let policy = MemoryScopedAgentConfig::new(
            paths,
            MemoryScopedAgentConfigOptions {
                protect_pinned_memory: true,
                ..MemoryScopedAgentConfigOptions::default()
            },
        );
        assert_eq!(
            policy
                .evaluate(
                    &context(TOOL_EDIT, &project, &project, Some(&target_file), None),
                    None,
                    &shell_checker(false),
                )
                .await,
            PermissionDecision::Deny
        );
        std::fs::remove_dir_all(temp).unwrap();
    }

    #[tokio::test]
    async fn trusted_anchor_rejects_managed_suffix_symlink_escape() {
        let temp = temp_dir("suffix-symlink");
        let project = temp.join("project");
        let base = temp.join("base");
        let outside = temp.join("outside");
        std::fs::create_dir_all(&project).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        let policy = make_policy(
            &project,
            &base,
            true,
            MemoryScopedAgentConfigOptions::default(),
        );
        #[cfg(unix)]
        std::os::unix::fs::symlink(&outside, project.join(".canopy")).unwrap();
        #[cfg(windows)]
        std::os::windows::fs::symlink_dir(&outside, project.join(".canopy")).unwrap();
        let escaped_file = policy.project_memory_root.join("topic.md");
        assert!(!policy.is_allowed_memory_path(Some(&escaped_file)));
        assert!(!policy.is_allowed_memory_path(Some(&outside.join("memory/topic.md"))));
        std::fs::remove_dir_all(temp).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn candidate_symlinks_cannot_escape_or_use_dangling_leaves() {
        let temp = temp_dir("candidate-symlink");
        let project = temp.join("project");
        let base = temp.join("base");
        let outside = temp.join("outside");
        std::fs::create_dir_all(&project).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        let policy = make_policy(
            &project,
            &base,
            false,
            MemoryScopedAgentConfigOptions::default(),
        );
        let memory_dir = policy.project_memory_root.join("topic");
        std::fs::create_dir_all(&memory_dir).unwrap();
        let outside_file = outside.join("secret.md");
        let external_link = memory_dir.join("external.md");
        let dangling_link = memory_dir.join("dangling.md");
        std::fs::write(&outside_file, "outside").unwrap();
        std::os::unix::fs::symlink(&outside_file, &external_link).unwrap();
        std::os::unix::fs::symlink(outside.join("missing.md"), &dangling_link).unwrap();

        assert!(!policy.is_allowed_memory_path(Some(&external_link)));
        assert!(!policy.is_allowed_memory_path(Some(&dangling_link)));
        std::fs::remove_dir_all(temp).unwrap();
    }

    #[test]
    fn symlinked_project_root_resolves_symmetrically() {
        let temp = temp_dir("project-symlink");
        let real_project = temp.join("real-project");
        let linked_project = temp.join("linked-project");
        let base = temp.join("base");
        std::fs::create_dir_all(&real_project).unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(&real_project, &linked_project).unwrap();
        #[cfg(windows)]
        std::os::windows::fs::symlink_dir(&real_project, &linked_project).unwrap();
        let policy = make_policy(
            &linked_project,
            &base,
            true,
            MemoryScopedAgentConfigOptions::default(),
        );
        assert!(
            policy.is_allowed_memory_path(Some(&policy.project_memory_root.join("new-topic.md")))
        );
        assert!(!policy.is_allowed_memory_path(Some(&linked_project.join("notes.md"))));
        std::fs::remove_dir_all(temp).unwrap();
    }

    #[tokio::test]
    async fn shell_checks_are_injected_fail_closed_and_respect_tool_toggle() {
        let temp = temp_dir("shell");
        let project = temp.join("project");
        std::fs::create_dir_all(&project).unwrap();
        let options = MemoryScopedAgentConfigOptions {
            allow_shell: true,
            ..MemoryScopedAgentConfigOptions::default()
        };
        let policy = make_policy(&project, &temp.join("base"), false, options);
        let checker = shell_checker(true);
        let ctx = context(
            TOOL_SHELL,
            &project,
            &project,
            None,
            Some("bash -c 'ls -la'"),
        );
        assert!(policy.is_tool_enabled(TOOL_SHELL, None));
        assert_eq!(
            policy.evaluate(&ctx, None, &checker).await,
            PermissionDecision::Allow
        );
        assert_eq!(
            *checker.saw_command.lock().unwrap(),
            Some("ls -la".to_owned())
        );
        let unsafe_checker = shell_checker(false);
        assert_eq!(
            policy.evaluate(&ctx, None, &unsafe_checker).await,
            PermissionDecision::Deny
        );
        assert_eq!(
            policy
                .evaluate(
                    &context(TOOL_SHELL, &project, &project, None, None),
                    None,
                    &checker,
                )
                .await,
            PermissionDecision::Deny
        );
        let disabled = make_policy(
            &project,
            &temp.join("base"),
            false,
            MemoryScopedAgentConfigOptions::default(),
        );
        assert!(!disabled.is_tool_enabled(TOOL_SHELL, None));
        assert_eq!(
            disabled.evaluate(&ctx, None, &checker).await,
            PermissionDecision::Deny
        );
        std::fs::remove_dir_all(temp).unwrap();
    }

    #[tokio::test]
    async fn base_permissions_merge_and_ask_bypass_only_applies_to_allow() {
        let temp = temp_dir("merge");
        let project = temp.join("project");
        std::fs::create_dir_all(&project).unwrap();
        let checker = shell_checker(false);
        let ask_base = TestBaseManager {
            relevant: true,
            decision: PermissionDecision::Ask,
            enabled: true,
        };
        let bypass = make_policy(
            &project,
            &temp.join("base"),
            false,
            MemoryScopedAgentConfigOptions {
                bypass_base_ask_for_scoped_paths: true,
                ..MemoryScopedAgentConfigOptions::default()
            },
        );
        let file_path = bypass.project_memory_root.join("topic.md");
        let path_ctx = context(TOOL_WRITE_FILE, &project, &project, Some(&file_path), None);
        assert_eq!(
            bypass.evaluate(&path_ctx, Some(&ask_base), &checker).await,
            PermissionDecision::Allow
        );
        assert!(bypass.has_matching_ask_rule(&path_ctx, Some(&ask_base)));

        let deny_base = TestBaseManager {
            decision: PermissionDecision::Deny,
            ..ask_base
        };
        assert_eq!(
            bypass.evaluate(&path_ctx, Some(&deny_base), &checker).await,
            PermissionDecision::Deny
        );
        assert_eq!(
            bypass.find_matching_deny_rule(&path_ctx, Some(&deny_base)),
            Some(format!(
                "ManagedAutoMemory(write_file: only within {} or {})",
                bypass.user_memory_root.display(),
                bypass.project_memory_root.display()
            ))
        );
        let unrelated_ctx = context("web_fetch", &project, &project, None, None);
        assert_eq!(
            bypass.find_matching_deny_rule(&unrelated_ctx, Some(&deny_base)),
            Some("base deny".to_owned())
        );
        let outside_file = project.join("README.md");
        let outside_ctx = context(
            TOOL_WRITE_FILE,
            &project,
            &project,
            Some(&outside_file),
            None,
        );
        assert_eq!(
            bypass
                .evaluate(&outside_ctx, Some(&ask_base), &checker)
                .await,
            PermissionDecision::Deny
        );
        assert!(bypass.is_tool_enabled("web_fetch", Some(&ask_base)));
        std::fs::remove_dir_all(temp).unwrap();
    }
}
