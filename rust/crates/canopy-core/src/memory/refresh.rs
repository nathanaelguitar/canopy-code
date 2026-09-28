//! Managed memory refresh classification and orchestration.
//!
//! Port of `packages/core/src/memory/refresh.ts`. Tool names, configured
//! context filenames, availability, and live instruction refresh callbacks
//! are supplied by the runtime adapter.

use std::future::Future;
use std::path::{Component, Path, PathBuf};
use std::pin::Pin;

use futures_util::future::join;
use serde_json::{Map, Value};

use super::indexer::{rebuild_managed_auto_memory_index, rebuild_user_auto_memory_index};
use super::paths::{AutoMemoryPaths, is_inside_memory_root};

pub const WRITE_FILE_TOOL_NAME: &str = "write_file";
pub const EDIT_TOOL_NAME: &str = "edit";
pub const LEGACY_EDIT_TOOL_NAME: &str = "replace";

#[derive(Clone, Debug, Default, PartialEq)]
pub struct MemoryWriteCandidate {
    pub tool_name: String,
    pub args: Option<Map<String, Value>>,
    /// `None` mirrors an absent `status`; only the exact string `success`
    /// counts as a successful write when a status is present.
    pub status: Option<Value>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WrittenMemoryScope {
    Project,
    User,
}

pub type RefreshFuture<'a> = Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;
pub type RefreshCallback<'a> = dyn Fn() -> RefreshFuture<'a> + Send + Sync + 'a;
pub type RefreshWarningCallback = dyn Fn(&str) + Send + Sync;

/// Runtime-owned Config behavior. The hierarchical callback is required;
/// system-instruction refresh is optional when no live model client exists.
pub struct RefreshRuntimeCallbacks<'a> {
    pub managed_memory_available: bool,
    pub refresh_hierarchical_memory: &'a RefreshCallback<'a>,
    pub refresh_system_instruction: Option<&'a RefreshCallback<'a>>,
    /// Receives source-compatible warning text including the optional log
    /// context. If omitted, warnings are written to stderr.
    pub warn: Option<&'a RefreshWarningCallback>,
}

pub struct RefreshMemoryAfterWriteOptions<'a> {
    pub context_filenames: &'a [String],
    pub log_context: Option<&'a str>,
    pub callbacks: &'a RefreshRuntimeCallbacks<'a>,
}

/// Resolve the write tool aliases used by `canonicalToolName` that can affect
/// memory classification. Other aliases canonicalize to non-write tools.
pub fn canonical_write_tool_name(tool_name: &str) -> &str {
    match tool_name {
        LEGACY_EDIT_TOOL_NAME => EDIT_TOOL_NAME,
        other => other,
    }
}

/// Select the source path argument using JavaScript nullish precedence:
/// `file_path ?? path ?? target_file`. A present but non-string or empty first
/// value prevents falling through to a later key.
pub fn candidate_file_path(args: Option<&Map<String, Value>>) -> Option<&str> {
    let args = args?;
    let value = ["file_path", "path", "target_file"]
        .into_iter()
        .filter_map(|key| args.get(key))
        .find(|value| !value.is_null())?;
    value.as_str().filter(|value| !value.is_empty())
}

pub fn is_successful_write(candidate: &MemoryWriteCandidate) -> bool {
    candidate
        .status
        .as_ref()
        .is_none_or(|status| status.as_str() == Some("success"))
        && matches!(
            canonical_write_tool_name(&candidate.tool_name),
            WRITE_FILE_TOOL_NAME | EDIT_TOOL_NAME
        )
}

/// Classify a successful write into private project or user memory. Team
/// memory is deliberately excluded because it is tracked and requires review.
pub fn classify_written_memory_scope(
    paths: &AutoMemoryPaths,
    candidate: &MemoryWriteCandidate,
) -> Option<WrittenMemoryScope> {
    if !is_successful_write(candidate) {
        return None;
    }
    let file_path = candidate_file_path(candidate.args.as_ref())?;
    let resolved_path = resolve_candidate_path(file_path, paths.project_root());

    // This permission-level check rejects paths outside all managed roots and
    // resolves ordinary symlinked files before the scope-specific comparison.
    if !paths.is_managed_memory_path(&resolved_path, paths.project_root()) {
        return None;
    }
    let resolved_candidate = realpath_existing_or_new(&resolved_path)?;
    let project_root = trusted_project_memory_root(paths);
    if is_inside_memory_root(&resolved_candidate, &project_root) {
        return Some(WrittenMemoryScope::Project);
    }
    let user_root = realpath_or_resolved(&paths.user_auto_memory_root());
    if is_inside_memory_root(&resolved_candidate, &user_root) {
        return Some(WrittenMemoryScope::User);
    }
    None
}

pub fn did_write_managed_memory(
    candidates: &[MemoryWriteCandidate],
    paths: &AutoMemoryPaths,
) -> bool {
    candidates
        .iter()
        .any(|candidate| classify_written_memory_scope(paths, candidate).is_some())
}

/// Detect writes to a configured project context file. This is intentionally
/// independent of managed-memory permission classification.
pub fn did_write_project_context_file(
    candidates: &[MemoryWriteCandidate],
    project_root: &Path,
    context_filenames: &[String],
) -> bool {
    let context_paths = context_filenames
        .iter()
        .map(|name| trim_ecmascript_whitespace(name))
        .filter(|name| !name.is_empty())
        .map(|name| resolve_candidate_path(name, project_root))
        .collect::<Vec<_>>();

    candidates.iter().any(|candidate| {
        if !is_successful_write(candidate) {
            return false;
        }
        let Some(file_path) = candidate_file_path(candidate.args.as_ref()) else {
            return false;
        };
        let resolved = resolve_candidate_path(file_path, project_root);
        context_paths.contains(&resolved)
    })
}

/// Best-effort instruction refresh. A failure in the hierarchy callback does
/// not prevent refreshing the live client's system instruction.
pub async fn refresh_memory_instruction(
    callbacks: &RefreshRuntimeCallbacks<'_>,
    log_context: Option<&str>,
) {
    if let Err(error) = (callbacks.refresh_hierarchical_memory)().await {
        warn(
            callbacks,
            log_context,
            &format!("refreshHierarchicalMemory failed: {error}"),
        );
    }
    if let Some(refresh_system_instruction) = callbacks.refresh_system_instruction
        && let Err(error) = refresh_system_instruction().await
    {
        warn(
            callbacks,
            log_context,
            &format!("refreshSystemInstruction failed: {error}"),
        );
    }
}

/// Rebuild indexes for successfully written private memory scopes, then
/// refresh injected runtime instructions. Index and callback failures are
/// warnings; the boolean stays true once a managed-memory write qualified.
pub async fn refresh_memory_after_managed_write(
    paths: &AutoMemoryPaths,
    candidates: &[MemoryWriteCandidate],
    options: RefreshMemoryAfterWriteOptions<'_>,
) -> bool {
    if !options.callbacks.managed_memory_available {
        return false;
    }

    let mut wrote_project_memory = false;
    let mut wrote_user_memory = false;
    for candidate in candidates {
        match classify_written_memory_scope(paths, candidate) {
            Some(WrittenMemoryScope::Project) => wrote_project_memory = true,
            Some(WrittenMemoryScope::User) => wrote_user_memory = true,
            None => {}
        }
    }
    if !wrote_project_memory && !wrote_user_memory {
        return false;
    }

    let project_rebuild = async {
        if wrote_project_memory && let Err(error) = rebuild_managed_auto_memory_index(paths).await {
            warn(
                options.callbacks,
                options.log_context,
                &format!("rebuildManagedAutoMemoryIndex failed: {error}"),
            );
        }
    };
    let user_rebuild = async {
        if wrote_user_memory && let Err(error) = rebuild_user_auto_memory_index(paths).await {
            warn(
                options.callbacks,
                options.log_context,
                &format!("rebuildUserAutoMemoryIndex failed: {error}"),
            );
        }
    };
    join(project_rebuild, user_rebuild).await;

    refresh_memory_instruction(options.callbacks, options.log_context).await;
    true
}

fn warn(callbacks: &RefreshRuntimeCallbacks<'_>, log_context: Option<&str>, message: &str) {
    let prefix = log_context
        .filter(|context| !context.is_empty())
        .map(|context| format!("{context}: "))
        .unwrap_or_default();
    let message = format!("{prefix}{message}");
    if let Some(warn) = callbacks.warn {
        warn(&message);
    } else {
        eprintln!("[AUTO_MEMORY_REFRESH] {message}");
    }
}

fn resolve_candidate_path(file_path: &str, project_root: &Path) -> PathBuf {
    let path = Path::new(file_path);
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let root = absolute_normalized(project_root, &cwd);
    let candidate = if path.is_absolute() {
        path.to_path_buf()
    } else {
        root.join(path)
    };
    absolute_normalized(&candidate, &cwd)
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

fn trusted_project_memory_root(paths: &AutoMemoryPaths) -> PathBuf {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let literal_root = absolute_normalized(&paths.auto_memory_root(), &cwd);
    let anchor = absolute_normalized(paths.auto_memory_trusted_anchor(), &cwd);
    if let Ok(suffix) = literal_root.strip_prefix(&anchor)
        && !suffix.as_os_str().is_empty()
        && !suffix
            .components()
            .any(|component| matches!(component, Component::ParentDir | Component::RootDir))
    {
        let mut resolved = realpath_or_resolved(&anchor);
        resolved.push(suffix);
        return absolute_normalized(&resolved, &cwd);
    }
    realpath_or_resolved(&literal_root)
}

fn realpath_existing_or_new(path: &Path) -> Option<PathBuf> {
    match std::fs::canonicalize(path) {
        Ok(path) => Some(path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if std::fs::symlink_metadata(path)
                .is_ok_and(|metadata| metadata.file_type().is_symlink())
            {
                return None;
            }
            realpath_new_path(path)
        }
        Err(_) => None,
    }
}

fn realpath_or_resolved(path: &Path) -> PathBuf {
    realpath_existing_or_new(path)
        .or_else(|| realpath_new_path(path))
        .unwrap_or_else(|| {
            let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
            absolute_normalized(path, &cwd)
        })
}

fn realpath_new_path(path: &Path) -> Option<PathBuf> {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let initial = absolute_normalized(path, &cwd);
    let mut current = initial.parent()?.to_path_buf();
    let mut remainder = PathBuf::from(initial.file_name()?);
    loop {
        match std::fs::canonicalize(&current) {
            Ok(parent) => {
                return Some(absolute_normalized(&parent.join(remainder), &cwd));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
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

fn trim_ecmascript_whitespace(text: &str) -> &str {
    text.trim_matches(is_ecmascript_whitespace)
}

fn is_ecmascript_whitespace(character: char) -> bool {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::{AutoMemoryType, MemoryProjectScope};
    use std::sync::{Arc, Mutex};

    fn temp_dir(label: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "canopy-memory-refresh-{label}-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    fn paths(root: &Path) -> AutoMemoryPaths {
        AutoMemoryPaths::new(
            root,
            root.join("memory-base"),
            false,
            MemoryProjectScope::Workspace,
        )
    }

    fn candidate(
        tool_name: &str,
        args: Map<String, Value>,
        status: Option<Value>,
    ) -> MemoryWriteCandidate {
        MemoryWriteCandidate {
            tool_name: tool_name.to_owned(),
            args: Some(args),
            status,
        }
    }

    fn args(fields: impl IntoIterator<Item = (&'static str, Value)>) -> Map<String, Value> {
        fields
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value))
            .collect()
    }

    fn file_candidate(tool: &str, path: &Path, status: Option<&str>) -> MemoryWriteCandidate {
        candidate(
            tool,
            args([(
                "file_path",
                Value::String(path.to_string_lossy().into_owned()),
            )]),
            status.map(|status| Value::String(status.to_owned())),
        )
    }

    fn project_topic(paths: &AutoMemoryPaths, name: &str) -> PathBuf {
        let path = paths.auto_memory_topic_path(AutoMemoryType::Project);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(
            &path,
            format!("---\ntype: project\nname: {name}\ndescription: fixture\n---\nbody\n"),
        )
        .unwrap();
        path
    }

    fn user_topic(paths: &AutoMemoryPaths, name: &str) -> PathBuf {
        let path = paths.user_auto_memory_topic_path(AutoMemoryType::User);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(
            &path,
            format!("---\ntype: user\nname: {name}\ndescription: fixture\n---\nbody\n"),
        )
        .unwrap();
        path
    }

    fn callbacks<'a>(
        available: bool,
        hierarchy: &'a RefreshCallback<'a>,
        system: Option<&'a RefreshCallback<'a>>,
        warn: Option<&'a RefreshWarningCallback>,
    ) -> RefreshRuntimeCallbacks<'a> {
        RefreshRuntimeCallbacks {
            managed_memory_available: available,
            refresh_hierarchical_memory: hierarchy,
            refresh_system_instruction: system,
            warn,
        }
    }

    #[test]
    fn candidate_alias_status_and_path_field_precedence_match_source() {
        let root = temp_dir("classification");
        let paths = paths(&root);
        let topic = project_topic(&paths, "fixture");

        let precedence = candidate(
            "replace",
            args([
                (
                    "file_path",
                    Value::String(root.join("outside.txt").to_string_lossy().into_owned()),
                ),
                ("path", Value::String(topic.to_string_lossy().into_owned())),
                (
                    "target_file",
                    Value::String(topic.to_string_lossy().into_owned()),
                ),
            ]),
            Some(Value::String("success".into())),
        );
        assert_eq!(
            candidate_file_path(precedence.args.as_ref()),
            Some(root.join("outside.txt").to_string_lossy().as_ref())
        );
        assert_eq!(classify_written_memory_scope(&paths, &precedence), None);

        let nullish_fallback = candidate(
            "replace",
            args([
                ("file_path", Value::Null),
                ("path", Value::String(topic.to_string_lossy().into_owned())),
            ]),
            Some(Value::String("success".into())),
        );
        assert_eq!(
            candidate_file_path(nullish_fallback.args.as_ref()),
            Some(topic.to_string_lossy().as_ref())
        );
        assert_eq!(
            classify_written_memory_scope(&paths, &nullish_fallback),
            Some(WrittenMemoryScope::Project)
        );

        for status in ["error", "cancelled", "partial"] {
            let failed = file_candidate("write_file", &topic, Some(status));
            assert_eq!(classify_written_memory_scope(&paths, &failed), None);
        }
        let unsupported_alias = file_candidate("WriteFile", &topic, Some("success"));
        assert_eq!(
            classify_written_memory_scope(&paths, &unsupported_alias),
            None
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn classifies_project_and_user_but_never_team_memory() {
        let root = temp_dir("scopes");
        let paths = paths(&root);
        let project = project_topic(&paths, "project fact");
        let user = user_topic(&paths, "user fact");
        let team = paths.team_auto_memory_root().join("shared.md");
        std::fs::create_dir_all(team.parent().unwrap()).unwrap();
        std::fs::write(&team, "team").unwrap();

        assert_eq!(
            classify_written_memory_scope(&paths, &file_candidate("write_file", &project, None)),
            Some(WrittenMemoryScope::Project)
        );
        assert_eq!(
            classify_written_memory_scope(&paths, &file_candidate("edit", &user, Some("success"))),
            Some(WrittenMemoryScope::User)
        );
        assert_eq!(
            classify_written_memory_scope(
                &paths,
                &file_candidate("write_file", &team, Some("success"))
            ),
            None
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn detects_only_configured_successful_project_context_filenames() {
        let root = temp_dir("context-files");
        let names = vec!["CANOPY.md".to_owned(), " AGENTS.md ".to_owned()];
        let canopy = file_candidate("write_file", &root.join("CANOPY.md"), Some("success"));
        let agents = candidate(
            "replace",
            args([("target_file", Value::String("AGENTS.md".into()))]),
            None,
        );
        let default_unconfigured = file_candidate("edit", &root.join("README.md"), Some("success"));
        let failed = file_candidate("write_file", &root.join("AGENTS.md"), Some("error"));
        assert!(did_write_project_context_file(
            &[canopy, agents],
            &root,
            &names
        ));
        assert!(!did_write_project_context_file(
            &[default_unconfigured],
            &root,
            &names
        ));
        assert!(!did_write_project_context_file(&[failed], &root, &names));
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn refresh_rebuilds_only_written_project_or_user_indexes() {
        let root = temp_dir("rebuild-scopes");
        let paths = paths(&root);
        let project = project_topic(&paths, "project fact");
        let user = user_topic(&paths, "user fact");
        let names = vec!["CANOPY.md".to_owned()];
        let hierarchy_calls = Arc::new(Mutex::new(0));
        let hierarchy_calls_clone = Arc::clone(&hierarchy_calls);
        let hierarchy = move || {
            let calls = Arc::clone(&hierarchy_calls_clone);
            Box::pin(async move {
                *calls.lock().unwrap() += 1;
                Ok(())
            }) as RefreshFuture<'_>
        };
        let system = || Box::pin(async { Ok(()) }) as RefreshFuture<'_>;
        let callback_set = callbacks(true, &hierarchy, Some(&system), None);
        let options = RefreshMemoryAfterWriteOptions {
            context_filenames: &names,
            log_context: None,
            callbacks: &callback_set,
        };

        assert!(
            refresh_memory_after_managed_write(
                &paths,
                &[file_candidate("write_file", &project, Some("success"))],
                options,
            )
            .await
        );
        assert!(paths.auto_memory_index_path().exists());
        assert!(!paths.user_auto_memory_index_path().exists());
        assert!(
            std::fs::read_to_string(paths.auto_memory_index_path())
                .unwrap()
                .contains("project fact")
        );

        let options = RefreshMemoryAfterWriteOptions {
            context_filenames: &names,
            log_context: None,
            callbacks: &callback_set,
        };
        assert!(
            refresh_memory_after_managed_write(
                &paths,
                &[file_candidate("edit", &user, Some("success"))],
                options,
            )
            .await
        );
        assert!(paths.user_auto_memory_index_path().exists());
        assert!(
            std::fs::read_to_string(paths.user_auto_memory_index_path())
                .unwrap()
                .contains("user fact")
        );
        assert_eq!(*hierarchy_calls.lock().unwrap(), 2);
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn unavailable_memory_short_circuits_without_rebuild_or_callbacks() {
        let root = temp_dir("unavailable");
        let paths = paths(&root);
        let topic = project_topic(&paths, "project fact");
        let calls = Arc::new(Mutex::new(0));
        let calls_clone = Arc::clone(&calls);
        let hierarchy = move || {
            let calls = Arc::clone(&calls_clone);
            Box::pin(async move {
                *calls.lock().unwrap() += 1;
                Ok(())
            }) as RefreshFuture<'_>
        };
        let callback_set = callbacks(false, &hierarchy, None, None);
        let options = RefreshMemoryAfterWriteOptions {
            context_filenames: &[],
            log_context: None,
            callbacks: &callback_set,
        };
        assert!(
            !refresh_memory_after_managed_write(
                &paths,
                &[file_candidate("write_file", &topic, Some("success"))],
                options
            )
            .await
        );
        assert_eq!(*calls.lock().unwrap(), 0);
        assert!(!paths.auto_memory_index_path().exists());
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn callback_failures_are_best_effort_and_keep_warning_context() {
        let root = temp_dir("callback-warnings");
        let paths = paths(&root);
        let topic = project_topic(&paths, "project fact");
        let hierarchy =
            || Box::pin(async { Err("hierarchy failed".to_owned()) }) as RefreshFuture<'_>;
        let system = || Box::pin(async { Err("system failed".to_owned()) }) as RefreshFuture<'_>;
        let warnings = Arc::new(Mutex::new(Vec::new()));
        let warnings_clone = Arc::clone(&warnings);
        let warn = move |message: &str| warnings_clone.lock().unwrap().push(message.to_owned());
        let callback_set = callbacks(true, &hierarchy, Some(&system), Some(&warn));
        let options = RefreshMemoryAfterWriteOptions {
            context_filenames: &[],
            log_context: Some("session-42"),
            callbacks: &callback_set,
        };

        assert!(
            refresh_memory_after_managed_write(
                &paths,
                &[file_candidate("write_file", &topic, Some("success"))],
                options
            )
            .await
        );
        let warnings = warnings.lock().unwrap();
        assert!(warnings.contains(
            &"session-42: refreshHierarchicalMemory failed: hierarchy failed".to_owned()
        ));
        assert!(
            warnings
                .contains(&"session-42: refreshSystemInstruction failed: system failed".to_owned())
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn project_scope_type_remains_workspace_derived_from_supplied_paths() {
        let root = temp_dir("scope-kind");
        let paths = paths(&root);
        assert_eq!(paths.project_scope(), MemoryProjectScope::Workspace);
        let _ = std::fs::remove_dir_all(root);
    }
}
