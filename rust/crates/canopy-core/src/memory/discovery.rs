//! Hierarchical instruction-file discovery and import expansion.
//!
//! Port of the instruction-file path, loading, and concatenation portion of
//! `packages/core/src/utils/memoryDiscovery.ts`. Host notification delivery is
//! injected through [`MemoryDiscoveryCallbacks`].

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::path::{Component, Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use futures_util::future::join_all;
use serde::Serialize;

use super::context_filenames::{LOCAL_CONTEXT_FILENAME, get_all_gemini_md_filenames};
use super::rules_discovery::{RuleFile, load_rules_with_global_dir};
use crate::utils::project_root::find_project_root;

const DIRECTORY_CONCURRENCY: usize = 10;
const FILE_READ_CONCURRENCY: usize = 20;
const MAX_IMPORT_DEPTH: usize = 5;
const CANOPY_DIR: &str = ".canopy";

/// Whether imported instruction content is expanded inline or flattened into
/// unique file sections.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum MemoryImportFormat {
    Flat,
    #[default]
    Tree,
}

/// Memory scope attached to an InstructionsLoaded notification.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryFileScope {
    User,
    Project,
    Local,
    Extension,
}

/// Reason the host loaded an instruction file.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryFileLoadReason {
    SessionStart,
    Refresh,
    Include,
}

/// Memory-discovery notification data, distinct from the hooks dispatcher
/// payload while retaining the same closure-friendly fields.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryFileLoadedNotification {
    pub file_path: String,
    pub memory_type: MemoryFileScope,
    pub load_reason: MemoryFileLoadReason,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trigger_file_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_file_path: Option<String>,
}

pub type MemoryFileLoadedFuture =
    Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'static>>;
pub type MemoryFileLoadedCallback =
    Arc<dyn Fn(MemoryFileLoadedNotification) -> MemoryFileLoadedFuture + Send + Sync>;
pub type MemoryNotificationFailureCallback =
    Arc<dyn Fn(&MemoryFileLoadedNotification, &str) + Send + Sync>;

/// Optional host notification hooks. Delivery errors are reported through
/// `on_notification_failure` and never fail memory loading.
#[derive(Clone, Default)]
pub struct MemoryDiscoveryCallbacks {
    pub on_instructions_loaded: Option<MemoryFileLoadedCallback>,
    pub on_notification_failure: Option<MemoryNotificationFailureCallback>,
}

/// Inputs owned by the host for a single hierarchical memory load.
#[derive(Clone, Debug)]
pub struct MemoryDiscoveryRequest {
    pub current_working_directory: PathBuf,
    pub include_directories: Vec<PathBuf>,
    pub user_home_path: PathBuf,
    pub global_canopy_directory: PathBuf,
    pub extension_context_file_paths: Vec<PathBuf>,
    pub folder_trust: bool,
    pub explicit_only: bool,
    pub import_format: MemoryImportFormat,
    pub load_reason: MemoryFileLoadReason,
    /// Glob patterns that exclude rule files by their absolute path.
    pub context_rule_excludes: Vec<String>,
}

impl MemoryDiscoveryRequest {
    /// Build a request using the process home and Canopy global directory.
    /// Hosts with a configured runtime home should supply those paths directly
    /// with the struct fields instead.
    pub fn from_process(current_working_directory: impl Into<PathBuf>) -> Self {
        let user_home_path = std::env::var_os("HOME")
            .or_else(|| std::env::var_os("USERPROFILE"))
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."));
        let global_canopy_directory = crate::storage::Storage::get_global_canopy_dir();
        Self {
            current_working_directory: current_working_directory.into(),
            include_directories: Vec::new(),
            user_home_path,
            global_canopy_directory,
            extension_context_file_paths: Vec::new(),
            folder_trust: false,
            explicit_only: false,
            import_format: MemoryImportFormat::Tree,
            load_reason: MemoryFileLoadReason::SessionStart,
            context_rule_excludes: Vec::new(),
        }
    }
}

/// Result of loading hierarchical instruction files.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MemoryDiscoveryResult {
    pub memory_content: String,
    /// Counts discovered configured context files, including unreadable files,
    /// matching the TypeScript loader's file-count behavior.
    pub file_count: usize,
    pub project_root: PathBuf,
    /// Number of baseline rules appended to `memory_content`.
    pub rule_count: usize,
    /// Path-conditional rules for turn-level lazy injection.
    pub conditional_rules: Vec<RuleFile>,
}

/// Discover, read, expand imports, and concatenate hierarchical instruction
/// files. Individual discovery/read failures are isolated and skipped or
/// represented as empty content, as in the TypeScript implementation.
pub async fn load_server_hierarchical_memory(
    request: &MemoryDiscoveryRequest,
    callbacks: &MemoryDiscoveryCallbacks,
) -> Result<MemoryDiscoveryResult, String> {
    let cwd = resolve_path(&request.current_working_directory);
    let home = resolve_path(&request.user_home_path);
    let global_canopy = resolve_path(&request.global_canopy_directory);
    let filenames = get_all_gemini_md_filenames();
    let implicit_discovery_enabled = !request.explicit_only;

    let mut file_paths = discover_file_paths(
        request,
        &home,
        &global_canopy,
        &filenames,
        implicit_discovery_enabled,
    )
    .await;

    let found_root = find_project_root(&cwd).await;
    let effective_root = found_root.clone().unwrap_or_else(|| cwd.clone());

    // The local context slot supplements hierarchy only for trusted projects
    // with an actual .git root; it is intentionally not synthesized from CWD.
    if implicit_discovery_enabled && request.folder_trust {
        if let Some(project_root) = &found_root {
            let local_context = project_root.join(CANOPY_DIR).join(LOCAL_CONTEXT_FILENAME);
            let local_context_key = local_context.to_string_lossy();
            if is_readable(&local_context).await
                && !file_paths
                    .iter()
                    .any(|path| path.to_string_lossy() == local_context_key)
            {
                file_paths.push(local_context);
            }
        }
    }

    let memory_filenames = filenames
        .iter()
        .map(String::as_str)
        .chain(std::iter::once(LOCAL_CONTEXT_FILENAME))
        .collect::<HashSet<_>>();
    let file_count = file_paths
        .iter()
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| memory_filenames.contains(name))
        })
        .count();

    let mut loaded_files = Vec::with_capacity(file_paths.len());
    for batch in file_paths.chunks(FILE_READ_CONCURRENCY) {
        let reads = batch.iter().cloned().map(|file_path| {
            read_instruction_file(
                file_path,
                request,
                &home,
                &global_canopy,
                found_root.as_deref(),
                callbacks,
            )
        });
        loaded_files.extend(join_all(reads).await);
    }

    let mut blocks = Vec::new();
    for (file_path, content) in loaded_files {
        let Some(content) = content else {
            continue;
        };
        let trimmed = trim_javascript_whitespace(&content);
        if trimmed.is_empty() {
            continue;
        }
        let display_path = if file_path.is_absolute() {
            relative_path(&cwd, &file_path)
                .to_string_lossy()
                .into_owned()
        } else {
            file_path.to_string_lossy().into_owned()
        };
        blocks.push(format!(
            "--- Context from: {display_path} ---\n{trimmed}\n--- End of Context from: {display_path} ---"
        ));
    }

    let mut memory_content = blocks.join("\n\n");
    let (rule_count, conditional_rules) = if request.explicit_only {
        (0, Vec::new())
    } else {
        let rules = load_rules_with_global_dir(
            &effective_root,
            &global_canopy,
            request.folder_trust,
            &request.context_rule_excludes,
        )
        .await?;
        if !rules.content.is_empty() {
            memory_content = if memory_content.is_empty() {
                rules.content
            } else {
                format!("{memory_content}\n\n{}", rules.content)
            };
        }
        (rules.rule_count, rules.conditional_rules)
    };

    Ok(MemoryDiscoveryResult {
        memory_content,
        file_count,
        project_root: effective_root,
        rule_count,
        conditional_rules,
    })
}

async fn discover_file_paths(
    request: &MemoryDiscoveryRequest,
    home: &Path,
    global_canopy: &Path,
    filenames: &[String],
    implicit_discovery_enabled: bool,
) -> Vec<PathBuf> {
    let mut dirs = Vec::<PathBuf>::new();
    let mut seen_dirs = HashSet::<String>::new();
    for dir in &request.include_directories {
        push_unique_path(&mut dirs, &mut seen_dirs, dir.clone());
    }
    if implicit_discovery_enabled {
        push_unique_path(
            &mut dirs,
            &mut seen_dirs,
            request.current_working_directory.clone(),
        );
    }

    let mut ordered_paths = Vec::new();
    let mut seen_paths = HashSet::<String>::new();
    for batch in dirs.chunks(DIRECTORY_CONCURRENCY) {
        let jobs = batch.iter().cloned().map(|dir| {
            discover_paths_for_directory(
                dir,
                request.folder_trust,
                implicit_discovery_enabled,
                home.to_path_buf(),
                global_canopy.to_path_buf(),
                filenames.to_vec(),
                request.extension_context_file_paths.clone(),
            )
        });
        // join_all preserves the batch input order, like Promise.allSettled.
        for paths in join_all(jobs).await {
            for path in paths {
                push_unique_path(&mut ordered_paths, &mut seen_paths, path);
            }
        }
    }
    ordered_paths
}

async fn discover_paths_for_directory(
    dir: PathBuf,
    folder_trust: bool,
    implicit_discovery_enabled: bool,
    home: PathBuf,
    global_canopy: PathBuf,
    filenames: Vec<String>,
    extension_paths: Vec<PathBuf>,
) -> Vec<PathBuf> {
    let resolved_dir = if dir.as_os_str().is_empty() {
        home.clone()
    } else {
        resolve_path(&dir)
    };
    let is_home_directory = resolved_dir == home;
    let mut paths = Vec::new();
    let mut seen = HashSet::<String>::new();

    for filename in filenames {
        let global_path = global_canopy.join(&filename);
        if !implicit_discovery_enabled {
            let explicit_path = resolved_dir.join(&filename);
            if is_readable(&explicit_path).await {
                push_unique_path(&mut paths, &mut seen, explicit_path);
            }
            continue;
        }

        if is_readable(&global_path).await {
            push_unique_path(&mut paths, &mut seen, global_path.clone());
        }

        if is_home_directory {
            let home_path = home.join(&filename);
            if home_path != global_path && is_readable(&home_path).await {
                push_unique_path(&mut paths, &mut seen, home_path);
            }
        } else if !dir.as_os_str().is_empty() && folder_trust {
            let mut current_dir = resolved_dir.clone();
            let project_root = find_project_root(&resolved_dir).await;
            let stop_dir = project_root
                .as_deref()
                .and_then(Path::parent)
                .or_else(|| home.parent())
                .map(Path::to_path_buf);
            let mut upward_paths = Vec::new();

            while let Some(parent) = current_dir.parent().map(Path::to_path_buf) {
                if current_dir == global_canopy || current_dir == home.join(CANOPY_DIR) {
                    break;
                }

                let potential = current_dir.join(&filename);
                if potential != global_path && is_readable(&potential).await {
                    upward_paths.push(potential);
                }

                if stop_dir.as_deref() == Some(current_dir.as_path()) || parent == current_dir {
                    break;
                }
                current_dir = parent;
            }
            upward_paths.reverse();
            for path in upward_paths {
                push_unique_path(&mut paths, &mut seen, path);
            }
        }
    }

    for extension_path in extension_paths {
        push_unique_path(&mut paths, &mut seen, extension_path);
    }
    paths
}

async fn read_instruction_file(
    file_path: PathBuf,
    request: &MemoryDiscoveryRequest,
    home: &Path,
    global_canopy: &Path,
    found_root: Option<&Path>,
    callbacks: &MemoryDiscoveryCallbacks,
) -> (PathBuf, Option<String>) {
    let bytes = match tokio::fs::read(&file_path).await {
        Ok(bytes) => bytes,
        Err(_) => return (file_path, None),
    };
    let content = String::from_utf8_lossy(&bytes).into_owned();
    let root_file_path = resolve_path(&file_path);
    let trigger_file_path = file_path.to_string_lossy().into_owned();
    let memory_type = classify_memory_type(
        &file_path,
        home,
        global_canopy,
        found_root,
        &request.extension_context_file_paths,
    );
    let project_root = find_project_root(file_path.parent().unwrap_or_else(|| Path::new(".")))
        .await
        .unwrap_or_else(|| resolve_path(file_path.parent().unwrap_or_else(|| Path::new("."))));

    let expanded = match request.import_format {
        MemoryImportFormat::Tree => {
            process_tree_imports(
                &content,
                file_path.parent().unwrap_or_else(|| Path::new(".")),
                &root_file_path,
                &project_root,
                0,
                HashSet::from([root_file_path.clone()]),
                memory_type,
                &trigger_file_path,
                callbacks,
            )
            .await
            .content
        }
        MemoryImportFormat::Flat => {
            process_flat_imports(
                &content,
                &root_file_path,
                &project_root,
                memory_type,
                &trigger_file_path,
                callbacks,
            )
            .await
        }
    };

    notify_loaded(
        callbacks,
        MemoryFileLoadedNotification {
            file_path: file_path.to_string_lossy().into_owned(),
            memory_type,
            load_reason: request.load_reason,
            trigger_file_path: None,
            parent_file_path: None,
        },
    )
    .await;
    (file_path, Some(expanded))
}

async fn notify_loaded(
    callbacks: &MemoryDiscoveryCallbacks,
    notification: MemoryFileLoadedNotification,
) {
    let Some(on_loaded) = &callbacks.on_instructions_loaded else {
        return;
    };
    if let Err(error) = on_loaded(notification.clone()).await {
        if let Some(on_failure) = &callbacks.on_notification_failure {
            on_failure(&notification, &error);
        }
    }
}

fn classify_memory_type(
    file_path: &Path,
    home: &Path,
    global_canopy: &Path,
    found_root: Option<&Path>,
    extension_paths: &[PathBuf],
) -> MemoryFileScope {
    let resolved_path = resolve_path(file_path);
    let resolved_global = resolve_path(global_canopy);
    let resolved_home = resolve_path(home);
    let resolved_extensions = extension_paths
        .iter()
        .map(|path| resolve_path(path))
        .collect::<Vec<_>>();

    if resolved_extensions.iter().any(|extension| {
        extension == &resolved_path
            || extension
                .parent()
                .is_some_and(|root| is_subpath(root, &resolved_path))
    }) {
        return MemoryFileScope::Extension;
    }
    if resolved_path != resolved_global && is_subpath(&resolved_global, &resolved_path) {
        return MemoryFileScope::User;
    }
    if let Some(root) = found_root.map(resolve_path) {
        if resolved_path == root.join(CANOPY_DIR).join(LOCAL_CONTEXT_FILENAME) {
            return MemoryFileScope::Local;
        }
        if is_subpath(&root, &resolved_path) {
            return MemoryFileScope::Project;
        }
    }
    if resolved_path.parent() == Some(resolved_home.as_path()) {
        return MemoryFileScope::User;
    }
    MemoryFileScope::Project
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemoryImportFile {
    pub path: PathBuf,
    pub imports: Vec<MemoryImportFile>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemoryImportResult {
    pub content: String,
    pub import_tree: MemoryImportFile,
}

fn process_tree_imports<'a>(
    content: &'a str,
    base_path: &'a Path,
    current_file: &'a Path,
    project_root: &'a Path,
    current_depth: usize,
    processed_files: HashSet<PathBuf>,
    memory_type: MemoryFileScope,
    trigger_file_path: &'a str,
    callbacks: &'a MemoryDiscoveryCallbacks,
) -> Pin<Box<dyn Future<Output = MemoryImportResult> + Send + 'a>> {
    Box::pin(async move {
        if current_depth >= MAX_IMPORT_DEPTH {
            return MemoryImportResult {
                content: content.to_owned(),
                import_tree: MemoryImportFile {
                    path: current_file.to_path_buf(),
                    imports: Vec::new(),
                },
            };
        }

        let imports = find_imports(content);
        let code_regions = find_code_regions(content);
        let mut output = String::with_capacity(content.len());
        let mut last_index = 0;
        let mut import_tree = Vec::new();

        for import in imports {
            output.push_str(&content[last_index..import.start]);
            last_index = import.end;
            if is_inside_regions(import.start, &code_regions) {
                output.push_str(&content[import.start..import.end]);
                continue;
            }
            if !validate_import_path(&import.path, base_path, project_root) {
                output.push_str(&format!(
                    "<!-- Import failed: {} - Path traversal attempt -->",
                    import.path
                ));
                continue;
            }

            let full_path = resolve_path(&base_path.join(&import.path));
            if processed_files.contains(&full_path) {
                output.push_str(&format!("<!-- File already processed: {} -->", import.path));
                continue;
            }
            let imported_content = match tokio::fs::read(&full_path).await {
                Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    output.push_str(&content[import.start..import.end]);
                    continue;
                }
                Err(error) => {
                    output.push_str(&format!(
                        "<!-- Import failed: {} - {} -->",
                        import.path, error
                    ));
                    continue;
                }
            };

            let mut child_processed = processed_files.clone();
            child_processed.insert(full_path.clone());
            let child_base = full_path.parent().unwrap_or_else(|| Path::new("."));
            let child = process_tree_imports(
                &imported_content,
                child_base,
                &full_path,
                project_root,
                current_depth + 1,
                child_processed,
                memory_type,
                trigger_file_path,
                callbacks,
            )
            .await;
            output.push_str(&format!(
                "<!-- Imported from: {} -->\n{}\n<!-- End of import from: {} -->",
                import.path, child.content, import.path
            ));
            import_tree.push(child.import_tree);
            notify_loaded(
                callbacks,
                MemoryFileLoadedNotification {
                    file_path: full_path.to_string_lossy().into_owned(),
                    memory_type,
                    load_reason: MemoryFileLoadReason::Include,
                    trigger_file_path: Some(trigger_file_path.to_owned()),
                    parent_file_path: Some(current_file.to_string_lossy().into_owned()),
                },
            )
            .await;
        }
        output.push_str(&content[last_index..]);
        MemoryImportResult {
            content: output,
            import_tree: MemoryImportFile {
                path: current_file.to_path_buf(),
                imports: import_tree,
            },
        }
    })
}

#[derive(Clone, Debug)]
struct FlatFile {
    path: PathBuf,
    content: String,
}

fn process_flat_imports<'a>(
    content: &'a str,
    root_file: &'a Path,
    project_root: &'a Path,
    memory_type: MemoryFileScope,
    trigger_file_path: &'a str,
    callbacks: &'a MemoryDiscoveryCallbacks,
) -> Pin<Box<dyn Future<Output = String> + Send + 'a>> {
    Box::pin(async move {
        let mut files = Vec::new();
        let mut expanded_at = HashMap::new();
        process_flat_file(
            content,
            root_file.parent().unwrap_or_else(|| Path::new(".")),
            root_file,
            0,
            project_root,
            memory_type,
            trigger_file_path,
            &mut files,
            &mut expanded_at,
            callbacks,
        )
        .await;
        files
            .iter()
            .map(|file| {
                let path = file.path.to_string_lossy();
                format!(
                    "--- File: {path} ---\n{}\n--- End of File: {path} ---",
                    trim_javascript_whitespace(&file.content)
                )
            })
            .collect::<Vec<_>>()
            .join("\n\n")
    })
}

#[allow(clippy::too_many_arguments)]
fn process_flat_file<'a>(
    content: &'a str,
    base_path: &'a Path,
    file_path: &'a Path,
    depth: usize,
    project_root: &'a Path,
    memory_type: MemoryFileScope,
    trigger_file_path: &'a str,
    files: &'a mut Vec<FlatFile>,
    expanded_at: &'a mut HashMap<PathBuf, usize>,
    callbacks: &'a MemoryDiscoveryCallbacks,
) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
    Box::pin(async move {
        let normalized_path = resolve_path(file_path);
        if expanded_at
            .get(&normalized_path)
            .is_some_and(|seen_depth| *seen_depth <= depth)
        {
            return;
        }
        let seen_depth = expanded_at.get(&normalized_path).copied();
        if seen_depth.is_none() {
            files.push(FlatFile {
                path: normalized_path.clone(),
                content: content.to_owned(),
            });
        }
        expanded_at.insert(normalized_path.clone(), depth);
        if depth >= MAX_IMPORT_DEPTH {
            return;
        }

        let imports = find_imports(content);
        let code_regions = find_code_regions(content);
        for import in imports.into_iter().rev() {
            if is_inside_regions(import.start, &code_regions)
                || !validate_import_path(&import.path, base_path, project_root)
            {
                continue;
            }
            let full_path = resolve_path(&base_path.join(&import.path));
            let child_seen_depth = expanded_at.get(&full_path).copied();
            if child_seen_depth.is_some_and(|seen| seen <= depth + 1) {
                continue;
            }
            let bytes = match tokio::fs::read(&full_path).await {
                Ok(bytes) => bytes,
                Err(_) => continue,
            };
            let imported_content = String::from_utf8_lossy(&bytes).into_owned();
            process_flat_file(
                &imported_content,
                full_path.parent().unwrap_or_else(|| Path::new(".")),
                &full_path,
                depth + 1,
                project_root,
                memory_type,
                trigger_file_path,
                files,
                expanded_at,
                callbacks,
            )
            .await;
            if child_seen_depth.is_none() {
                notify_loaded(
                    callbacks,
                    MemoryFileLoadedNotification {
                        file_path: full_path.to_string_lossy().into_owned(),
                        memory_type,
                        load_reason: MemoryFileLoadReason::Include,
                        trigger_file_path: Some(trigger_file_path.to_owned()),
                        parent_file_path: Some(normalized_path.to_string_lossy().into_owned()),
                    },
                )
                .await;
            }
        }
    })
}

#[derive(Clone, Debug)]
struct ImportReference {
    start: usize,
    end: usize,
    path: String,
}

fn find_imports(content: &str) -> Vec<ImportReference> {
    let bytes = content.as_bytes();
    let mut imports = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        let Some(relative) = bytes[index..].iter().position(|byte| *byte == b'@') else {
            break;
        };
        let start = index + relative;
        if start > 0 && !is_source_whitespace(bytes[start - 1]) {
            index = start + 1;
            continue;
        }
        let mut end = start + 1;
        while end < bytes.len() && !is_source_whitespace(bytes[end]) {
            end += 1;
        }
        let path = &content[start + 1..end];
        if path
            .as_bytes()
            .first()
            .is_some_and(|first| *first == b'.' || *first == b'/' || first.is_ascii_alphabetic())
        {
            imports.push(ImportReference {
                start,
                end,
                path: path.to_owned(),
            });
        }
        index = end.saturating_add(1);
    }
    imports
}

fn is_source_whitespace(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\n' | b'\r')
}

fn find_code_regions(content: &str) -> Vec<(usize, usize)> {
    let bytes = content.as_bytes();
    let mut regions = Vec::new();
    let mut line_start = 0;
    let mut open_fence: Option<(u8, usize, usize)> = None;
    while line_start < bytes.len() {
        let line_end = bytes[line_start..]
            .iter()
            .position(|byte| *byte == b'\n')
            .map(|offset| line_start + offset + 1)
            .unwrap_or(bytes.len());
        let without_newline = if line_end > line_start && bytes[line_end - 1] == b'\n' {
            line_end - 1
        } else {
            line_end
        };
        let mut marker_start = line_start;
        while marker_start < without_newline
            && marker_start - line_start < 4
            && bytes[marker_start] == b' '
        {
            marker_start += 1;
        }
        let indentation = marker_start - line_start;

        if let Some((marker, minimum, fence_start)) = open_fence {
            let count = bytes[marker_start..without_newline]
                .iter()
                .take_while(|byte| **byte == marker)
                .count();
            let rest_is_whitespace = bytes[marker_start + count..without_newline]
                .iter()
                .all(|byte| matches!(byte, b' ' | b'\t'));
            if indentation <= 3 && count >= minimum && rest_is_whitespace {
                regions.push((fence_start, line_end));
                open_fence = None;
            }
        } else {
            let marker = bytes.get(marker_start).copied();
            let count = marker.map_or(0, |marker| {
                bytes[marker_start..without_newline]
                    .iter()
                    .take_while(|byte| **byte == marker)
                    .count()
            });
            if indentation <= 3
                && marker.is_some_and(|marker| matches!(marker, b'`' | b'~'))
                && count >= 3
            {
                open_fence = Some((marker.unwrap(), count, line_start));
            } else if indentation >= 4 || bytes.get(line_start) == Some(&b'\t') {
                regions.push((line_start, line_end));
            }
        }
        line_start = line_end;
    }
    if let Some((_, _, fence_start)) = open_fence {
        regions.push((fence_start, bytes.len()));
    }

    let mut index = 0;
    while index < bytes.len() {
        if is_inside_regions(index, &regions) || bytes[index] != b'`' {
            index += 1;
            continue;
        }
        let escaped = bytes[..index]
            .iter()
            .rev()
            .take_while(|byte| **byte == b'\\')
            .count()
            % 2
            == 1;
        if escaped {
            index += 1;
            continue;
        }
        let run = bytes[index..]
            .iter()
            .take_while(|byte| **byte == b'`')
            .count();
        let mut cursor = index + run;
        let mut close = None;
        while cursor < bytes.len() {
            if bytes[cursor] == b'`' {
                let close_run = bytes[cursor..]
                    .iter()
                    .take_while(|byte| **byte == b'`')
                    .count();
                if close_run == run {
                    close = Some(cursor + close_run);
                    break;
                }
                cursor += close_run;
            } else {
                cursor += 1;
            }
        }
        if let Some(end) = close {
            regions.push((index, end));
            index = end;
        } else {
            index += run;
        }
    }
    regions.sort_unstable();
    regions
}

fn is_inside_regions(index: usize, regions: &[(usize, usize)]) -> bool {
    regions
        .iter()
        .any(|(start, end)| index >= *start && index < *end)
}

fn validate_import_path(import_path: &str, base_path: &Path, project_root: &Path) -> bool {
    if import_path.starts_with("file://")
        || import_path.starts_with("http://")
        || import_path.starts_with("https://")
    {
        return false;
    }
    let resolved = resolve_path(&base_path.join(import_path));
    is_subpath(project_root, &resolved)
}

fn is_subpath(parent: &Path, child: &Path) -> bool {
    resolve_path(child)
        .strip_prefix(resolve_path(parent))
        .is_ok()
}

async fn is_readable(path: &Path) -> bool {
    tokio::fs::File::open(path).await.is_ok()
}

fn push_unique_path(paths: &mut Vec<PathBuf>, seen: &mut HashSet<String>, path: PathBuf) {
    let key = path.to_string_lossy().into_owned();
    if seen.insert(key) {
        paths.push(path);
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
                    output.push("..");
                }
            }
            Component::Normal(part) => output.push(part),
        }
    }
    output
}

fn relative_path(from: &Path, to: &Path) -> PathBuf {
    let from_components = from.components().collect::<Vec<_>>();
    let to_components = to.components().collect::<Vec<_>>();
    let shared = from_components
        .iter()
        .zip(&to_components)
        .take_while(|(left, right)| left == right)
        .count();
    let mut relative = PathBuf::new();
    for component in &from_components[shared..] {
        if matches!(component, Component::Normal(_) | Component::ParentDir) {
            relative.push("..");
        }
    }
    for component in &to_components[shared..] {
        relative.push(component.as_os_str());
    }
    relative
}

fn trim_javascript_whitespace(value: &str) -> &str {
    value.trim_matches(|character: char| {
        matches!(
            character,
            '\u{0009}'..='\u{000D}'
                | '\u{0020}'
                | '\u{00A0}'
                | '\u{1680}'
                | '\u{2000}'..='\u{200A}'
                | '\u{2028}'..='\u{2029}'
                | '\u{202F}'
                | '\u{205F}'
                | '\u{3000}'
                | '\u{FEFF}'
        )
    })
}
