//! Workspace-safe file-reference resolution for native prompt hosts.
//!
//! This service covers the two filesystem forms used by the TypeScript CLI:
//! ordinary `@path` references (whose content is appended after the query)
//! and custom-command `@{path}` injections (whose content replaces the token).
//! A host remains responsible for parsing mixed `@` references and should pass
//! only filesystem paths here after handling session, extension, and MCP refs.

use std::io;
use std::path::{Path, PathBuf};

use serde_json::{Value, json};
use walkdir::WalkDir;

use crate::file_discovery::{FileDiscoveryService, FileFilteringOptions};
use crate::providers::openai_request::InputModalities;
use crate::tool_response_finalizer::ToolExecutionOutput;
use crate::tools::read_file::ReadFileTool;
use crate::utils::folder_structure::{FolderStructureOptions, get_folder_structure};

const AT_FILE_TRIGGER: &str = "@{";
const MAX_INJECTIONS_PER_PROMPT: usize = 64;
const MAX_DIRECTORY_FILES: usize = 128;
const MAX_DIRECTORY_ENTRIES_SCANNED: usize = 10_000;
/// Bound total context added by one processor call, including inline media.
pub const MAX_AT_FILE_CONTEXT_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AtFileDiagnosticKind {
    Info,
    Error,
    Warning,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AtFileDiagnostic {
    pub kind: AtFileDiagnosticKind,
    pub message: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AtFileProcessResult {
    /// Gemini-shaped model parts. Text content is represented as `{"text": ...}`.
    pub parts: Vec<Value>,
    /// Host-visible paths whose content was included.
    pub files_read: Vec<String>,
    /// Per-path summary for native prompt hosts to display as success/error
    /// cards after the content has been prepared.
    pub file_displays: Vec<AtFileReadDisplay>,
    /// Diagnostics for ignored paths, read failures, and bounded truncation.
    pub diagnostics: Vec<AtFileDiagnostic>,
    pub changed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AtFileReadDisplay {
    pub path: String,
    pub is_directory: bool,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AtFileMention {
    /// The path portion, without the leading `@`.
    pub path: String,
    /// Optional label to show in the content block, typically the spelling
    /// that appeared in the user's prompt.
    pub display_path: Option<String>,
}

pub struct AtFileProcessor {
    workspace_root: PathBuf,
    allowed_roots: Vec<PathBuf>,
    lexical_allowed_roots: Vec<PathBuf>,
    file_discovery: FileDiscoveryService,
    reader: ReadFileTool,
}

impl AtFileProcessor {
    pub fn new(workspace_root: impl AsRef<Path>) -> Result<Self, String> {
        Self::new_with_custom_ignore_files(workspace_root, None)
    }

    /// Construct a processor with the same project-local ignore service used
    /// for resolution. `custom_ignore_files` follows `FileDiscoveryService`
    /// semantics and can be supplied from host settings during CLI wiring.
    pub fn new_with_custom_ignore_files(
        workspace_root: impl AsRef<Path>,
        custom_ignore_files: Option<&[String]>,
    ) -> Result<Self, String> {
        Self::new_with_roots_and_custom_ignore_files(workspace_root, &[], custom_ignore_files)
    }

    /// Construct a processor that also accepts references under additional
    /// explicitly trusted roots, such as Canopy's global temporary directory.
    pub fn new_with_additional_allowed_roots(
        workspace_root: impl AsRef<Path>,
        additional_roots: &[PathBuf],
    ) -> Result<Self, String> {
        Self::new_with_roots_and_custom_ignore_files(workspace_root, additional_roots, None)
    }

    fn new_with_roots_and_custom_ignore_files(
        workspace_root: impl AsRef<Path>,
        additional_roots: &[PathBuf],
        custom_ignore_files: Option<&[String]>,
    ) -> Result<Self, String> {
        let requested_workspace_root = workspace_root.as_ref();
        let lexical_workspace_root = if requested_workspace_root.is_absolute() {
            lexical_normalize(requested_workspace_root)
        } else {
            let current_dir = std::env::current_dir()
                .map_err(|error| format!("could not resolve workspace root: {error}"))?;
            lexical_normalize(&current_dir.join(requested_workspace_root))
        };
        let workspace_root = std::fs::canonicalize(workspace_root.as_ref())
            .map_err(|error| format!("could not resolve workspace root: {error}"))?;
        if !workspace_root.is_dir() {
            return Err("workspace root is not a directory".to_owned());
        }
        let mut allowed_roots = vec![workspace_root.clone()];
        let mut lexical_allowed_roots = vec![lexical_workspace_root];
        for root in additional_roots {
            let lexical_root = if root.is_absolute() {
                lexical_normalize(root)
            } else {
                let current_dir = std::env::current_dir().map_err(|error| {
                    format!("could not resolve additional allowed root: {error}")
                })?;
                lexical_normalize(&current_dir.join(root))
            };
            let Ok(root) = std::fs::canonicalize(root) else {
                continue;
            };
            if root.is_dir() {
                if !allowed_roots.contains(&root) {
                    allowed_roots.push(root);
                }
                if !lexical_allowed_roots.contains(&lexical_root) {
                    lexical_allowed_roots.push(lexical_root);
                }
            }
        }
        Ok(Self {
            file_discovery: FileDiscoveryService::new(&workspace_root, custom_ignore_files)?,
            reader: ReadFileTool::new(&workspace_root)?,
            workspace_root,
            allowed_roots,
            lexical_allowed_roots,
        })
    }

    /// Expand `@{path}` placeholders inline, matching the custom-command
    /// prompt processor. Failed reads keep the original placeholder; ignored
    /// files are omitted and produce an informational diagnostic. An unclosed
    /// brace is a syntax error and returns `Err`, matching the TypeScript
    /// injection parser.
    pub async fn process_braced_injections(
        &self,
        text: &str,
        command_name: Option<&str>,
        modalities: InputModalities,
    ) -> Result<AtFileProcessResult, String> {
        let injections = parse_braced_injections(text, command_name)?;
        if injections.is_empty() {
            return Ok(AtFileProcessResult {
                parts: vec![json!({"text": text})],
                ..AtFileProcessResult::default()
            });
        }

        let mut result = AtFileProcessResult {
            changed: true,
            ..AtFileProcessResult::default()
        };
        let mut added_bytes = 0usize;
        let mut last_index = 0usize;

        for injection in injections {
            let prefix = &text[last_index..injection.start_index];
            if !prefix.is_empty() {
                result.parts.push(json!({"text": prefix}));
            }

            let display_token = &text[injection.start_index..injection.end_index];
            let read_result = self
                .read_path_contents(&injection.path, &injection.path, modalities, added_bytes)
                .await;
            match read_result {
                Ok(outcome) if outcome.ignored => {
                    result.diagnostics.push(AtFileDiagnostic {
                        kind: AtFileDiagnosticKind::Info,
                        message: format!(
                            "File '{display_token}' was ignored by .gitignore or configured Canopy ignore files and was not included in the prompt."
                        ),
                    });
                }
                Ok(outcome) => {
                    let bytes = parts_size(&outcome.parts);
                    if added_bytes.saturating_add(bytes) > MAX_AT_FILE_CONTEXT_BYTES {
                        result.parts.push(json!({"text": display_token}));
                        result.diagnostics.push(AtFileDiagnostic {
                            kind: AtFileDiagnosticKind::Error,
                            message: format!(
                                "Failed to inject content for '{display_token}': injected file content exceeded the {} byte prompt limit",
                                MAX_AT_FILE_CONTEXT_BYTES
                            ),
                        });
                    } else {
                        added_bytes = added_bytes.saturating_add(bytes);
                        result.parts.extend(outcome.parts);
                        if outcome.truncated {
                            result.diagnostics.push(AtFileDiagnostic {
                                kind: AtFileDiagnosticKind::Warning,
                                message: format!(
                                    "Content for '{display_token}' was truncated to keep file injection bounded."
                                ),
                            });
                        }
                        if !outcome.files_read.is_empty() {
                            result.files_read.extend(outcome.files_read);
                        }
                    }
                }
                Err(error) => {
                    result.parts.push(json!({"text": display_token}));
                    result.diagnostics.push(AtFileDiagnostic {
                        kind: AtFileDiagnosticKind::Error,
                        message: format!("Failed to inject content for '{display_token}': {error}"),
                    });
                }
            }

            last_index = injection.end_index;
        }

        let suffix = &text[last_index..];
        if !suffix.is_empty() {
            result.parts.push(json!({"text": suffix}));
        }
        Ok(result)
    }

    /// Resolve filesystem-only `@path` mentions parsed by the host's mixed
    /// reference parser. The original query is intentionally not returned or
    /// rewritten here: native `atCommandProcessor` keeps those tokens in the
    /// query and appends the returned file-context parts after it.
    pub async fn resolve_file_mentions(
        &self,
        mentions: &[AtFileMention],
        modalities: InputModalities,
    ) -> AtFileProcessResult {
        let mut result = AtFileProcessResult::default();
        let wrapper_parts = [
            json!({"text": "\n--- Content from referenced files ---"}),
            json!({"text": "\n--- End of content ---"}),
        ];
        let mut added_bytes = parts_size(&wrapper_parts);
        let mut accepted_paths = Vec::new();
        let mut file_parts = Vec::new();
        let mut file_displays = Vec::new();
        let mut seen_file_paths = std::collections::HashSet::new();
        let mut ignored_git = Vec::new();
        let mut ignored_canopy = Vec::new();
        let mut ignored_both = Vec::new();

        for mention in mentions {
            let display_path = mention
                .display_path
                .as_deref()
                .unwrap_or(mention.path.as_str());
            let token = format!("@{}", mention.path);
            let requested_path = self.requested_path(&mention.path);
            if !self.is_lexically_allowed(&requested_path) {
                result.diagnostics.push(AtFileDiagnostic {
                    kind: AtFileDiagnosticKind::Info,
                    message: format!(
                        "Path {} is not in the workspace and will be skipped.",
                        mention.path
                    ),
                });
                continue;
            }
            let canonical_path = match self.resolve_path(&mention.path) {
                Ok(path) => path,
                Err(error) => {
                    let message = if error.contains("outside of the allowed workspace") {
                        format!(
                            "Path {} is not in the workspace and will be skipped.",
                            mention.path
                        )
                    } else if error.contains("No such file or directory")
                        || error.contains("not found")
                    {
                        format!(
                            "Path {} not found. Path {} will be skipped.",
                            mention.path, mention.path
                        )
                    } else {
                        format!(
                            "Error stating path {}: {error}. Path {} will be skipped.",
                            mention.path, mention.path
                        )
                    };
                    result.diagnostics.push(AtFileDiagnostic {
                        kind: AtFileDiagnosticKind::Info,
                        message,
                    });
                    continue;
                }
            };
            let metadata = match std::fs::metadata(&canonical_path) {
                Ok(metadata) => metadata,
                Err(error) => {
                    result.diagnostics.push(AtFileDiagnostic {
                        kind: AtFileDiagnosticKind::Info,
                        message: format!("Path {} will be skipped: {error}", mention.path),
                    });
                    continue;
                }
            };
            if !metadata.is_file() && !metadata.is_dir() {
                result.diagnostics.push(AtFileDiagnostic {
                    kind: AtFileDiagnosticKind::Info,
                    message: format!(
                        "Path {} is not a file or directory and will be skipped.",
                        mention.path
                    ),
                });
                continue;
            }

            let mut ignore_reason = None;
            let mut ignore_check_failed = false;
            for candidate in [&requested_path, &canonical_path] {
                match self.ignore_reason(candidate, metadata.is_dir()) {
                    Ok(Some(reason)) => {
                        ignore_reason = Some(reason);
                        break;
                    }
                    Ok(None) => {}
                    Err(error) => {
                        result.diagnostics.push(AtFileDiagnostic {
                            kind: AtFileDiagnosticKind::Error,
                            message: format!(
                                "Could not check ignore rules for {}: {error}",
                                mention.path
                            ),
                        });
                        ignore_check_failed = true;
                        break;
                    }
                }
            }
            if ignore_check_failed {
                continue;
            }
            if let Some(reason) = ignore_reason {
                let reason_text = match reason {
                    "git" => "git-ignored",
                    "canopy" => "canopy-ignored",
                    _ => "ignored by both git and canopy",
                };
                match reason {
                    "git" => ignored_git.push(mention.path.clone()),
                    "canopy" => ignored_canopy.push(mention.path.clone()),
                    _ => ignored_both.push(mention.path.clone()),
                }
                result.diagnostics.push(AtFileDiagnostic {
                    kind: AtFileDiagnosticKind::Info,
                    message: format!(
                        "Path {} is {reason_text} and will be skipped.",
                        mention.path
                    ),
                });
                continue;
            }

            if metadata.is_file() && !seen_file_paths.insert(canonical_path.clone()) {
                continue;
            }

            let mut file_error = None;
            let content = if metadata.is_dir() {
                let tree = get_folder_structure(
                    &canonical_path,
                    FolderStructureOptions {
                        file_service: Some(&self.file_discovery),
                        file_filtering_options: Some(&FileFilteringOptions::default()),
                        ..FolderStructureOptions::default()
                    },
                );
                vec![
                    json!({"text": format!("\nContent from {display_path}:\n")}),
                    json!({"text": tree}),
                ]
            } else {
                let header = json!({"text": format!("\nContent from {display_path}:\n")});
                match self.read_file_parts(&canonical_path, modalities).await {
                    Ok(parts) => {
                        let mut content = vec![header];
                        content.extend(parts);
                        content
                    }
                    Err(error) => {
                        file_error = Some(error.clone());
                        result.diagnostics.push(AtFileDiagnostic {
                            kind: AtFileDiagnosticKind::Error,
                            message: format!("Failed to read {display_path}: {error}"),
                        });
                        vec![
                            header,
                            json!({"text": format!("Error reading {display_path}: {error}")}),
                        ]
                    }
                }
            };
            let next_bytes = parts_size(&content);
            if added_bytes.saturating_add(next_bytes) > MAX_AT_FILE_CONTEXT_BYTES {
                let error = format!(
                    "Referenced content exceeded the {} byte prompt limit.",
                    MAX_AT_FILE_CONTEXT_BYTES
                );
                result.diagnostics.push(AtFileDiagnostic {
                    kind: AtFileDiagnosticKind::Warning,
                    message: format!(
                        "Stopped reading file references after reaching the {} byte prompt limit at {token}.",
                        MAX_AT_FILE_CONTEXT_BYTES
                    ),
                });
                file_displays.push(AtFileReadDisplay {
                    path: display_path.to_owned(),
                    is_directory: metadata.is_dir(),
                    error: Some(error),
                });
                break;
            }
            added_bytes = added_bytes.saturating_add(next_bytes);
            file_parts.extend(content);
            accepted_paths.push(display_path.to_owned());
            file_displays.push(AtFileReadDisplay {
                path: display_path.to_owned(),
                is_directory: metadata.is_dir(),
                error: file_error,
            });
        }

        let ignored_count = ignored_git.len() + ignored_canopy.len() + ignored_both.len();
        if ignored_count > 0 {
            let mut groups = Vec::new();
            if !ignored_git.is_empty() {
                groups.push(format!("Git-ignored: {}", ignored_git.join(", ")));
            }
            if !ignored_canopy.is_empty() {
                groups.push(format!("Canopy-ignored: {}", ignored_canopy.join(", ")));
            }
            if !ignored_both.is_empty() {
                groups.push(format!("Ignored by both: {}", ignored_both.join(", ")));
            }
            result.diagnostics.push(AtFileDiagnostic {
                kind: AtFileDiagnosticKind::Info,
                message: format!("Ignored {ignored_count} files:\n{}", groups.join("\n")),
            });
        }

        if !file_parts.is_empty() {
            let mut parts = vec![json!({"text": "\n--- Content from referenced files ---"})];
            parts.extend(file_parts);
            parts.push(json!({"text": "\n--- End of content ---"}));
            result.parts = parts;
        }
        result.files_read = accepted_paths;
        result.file_displays = file_displays;
        result
    }

    async fn read_path_contents(
        &self,
        raw_path: &str,
        display_path: &str,
        modalities: InputModalities,
        used_bytes: usize,
    ) -> Result<ReadPathOutcome, String> {
        if raw_path.trim().is_empty() {
            return Err("path must not be empty".to_owned());
        }
        let path = self.resolve_path(raw_path)?;
        let metadata =
            std::fs::metadata(&path).map_err(|error| format!("could not inspect path: {error}"))?;
        if metadata.is_dir() {
            self.read_directory_contents(&path, display_path, modalities, used_bytes)
                .await
        } else if metadata.is_file() {
            if self.is_ignored(&path, false)? {
                return Ok(ReadPathOutcome::ignored());
            }
            let parts = self.read_file_parts(&path, modalities).await?;
            Ok(ReadPathOutcome {
                parts,
                files_read: vec![display_path.to_owned()],
                ignored: false,
                truncated: false,
            })
        } else {
            Err("path is not a regular file or directory".to_owned())
        }
    }

    async fn read_directory_contents(
        &self,
        directory: &Path,
        display_path: &str,
        modalities: InputModalities,
        used_bytes: usize,
    ) -> Result<ReadPathOutcome, String> {
        let mut result = ReadPathOutcome {
            parts: Vec::new(),
            files_read: Vec::new(),
            ignored: false,
            truncated: false,
        };
        let mut budget_remaining = MAX_AT_FILE_CONTEXT_BYTES.saturating_sub(used_bytes);
        if !push_text_part(
            &mut result.parts,
            &mut budget_remaining,
            format!("--- Start of content for directory: {display_path} ---\n"),
        ) {
            result.truncated = true;
            return Ok(result);
        }

        let mut walker = WalkDir::new(directory)
            .follow_links(false)
            .max_open(32)
            .into_iter();
        let mut scanned_entries = 0usize;
        let mut files_read = 0usize;
        while let Some(entry) = walker.next() {
            scanned_entries = scanned_entries.saturating_add(1);
            if scanned_entries > MAX_DIRECTORY_ENTRIES_SCANNED {
                result.truncated = true;
                break;
            }
            let entry = entry.map_err(|error| format!("could not scan directory: {error}"))?;
            if entry.path() == directory {
                continue;
            }
            if entry.file_type().is_symlink() {
                continue;
            }
            if entry.file_type().is_dir() {
                if self.is_ignored(entry.path(), true)? {
                    walker.skip_current_dir();
                }
                continue;
            }
            if !entry.file_type().is_file() || self.is_ignored(entry.path(), false)? {
                continue;
            }
            if files_read >= MAX_DIRECTORY_FILES {
                result.truncated = true;
                break;
            }

            let relative_path = entry
                .path()
                .strip_prefix(directory)
                .unwrap_or(entry.path())
                .to_string_lossy()
                .replace('\\', "/");
            if !push_text_part(
                &mut result.parts,
                &mut budget_remaining,
                format!("--- {relative_path} ---\n"),
            ) {
                result.truncated = true;
                break;
            }

            match self.read_file_parts(entry.path(), modalities).await {
                Ok(parts) => {
                    let size = parts_size(&parts);
                    if size > budget_remaining {
                        result.truncated = true;
                        break;
                    }
                    budget_remaining = budget_remaining.saturating_sub(size);
                    result.parts.extend(parts);
                    result.files_read.push(relative_path);
                }
                Err(error) => {
                    let error_text = format!("Error reading file {relative_path}: {error}");
                    if !push_text_part(&mut result.parts, &mut budget_remaining, error_text) {
                        result.truncated = true;
                        break;
                    }
                }
            }
            if !push_text_part(&mut result.parts, &mut budget_remaining, "\n".to_owned()) {
                result.truncated = true;
                break;
            }
            files_read = files_read.saturating_add(1);
        }
        let _ = push_text_part(
            &mut result.parts,
            &mut budget_remaining,
            format!("--- End of content for directory: {display_path} ---"),
        );
        Ok(result)
    }

    async fn read_file_parts(
        &self,
        path: &Path,
        modalities: InputModalities,
    ) -> Result<Vec<Value>, String> {
        let args = json!({"file_path": path});
        let output: ToolExecutionOutput = if path.starts_with(&self.workspace_root) {
            self.reader
                .execute_with_modalities(&args, modalities)
                .await?
        } else {
            let root = self
                .allowed_roots
                .iter()
                .find(|root| path.starts_with(root))
                .ok_or_else(|| "path is outside the allowed roots".to_owned())?;
            ReadFileTool::new(root)?
                .execute_with_modalities(&args, modalities)
                .await?
        };
        if !output.parts.is_empty() {
            Ok(output.parts)
        } else {
            Ok(vec![json!({"text": output.output})])
        }
    }

    fn requested_path(&self, raw_path: &str) -> PathBuf {
        let requested = Path::new(raw_path);
        let absolute = if requested.is_absolute() {
            requested.to_path_buf()
        } else {
            self.workspace_root.join(requested)
        };
        lexical_normalize(&absolute)
    }

    fn is_lexically_allowed(&self, path: &Path) -> bool {
        self.allowed_roots
            .iter()
            .chain(self.lexical_allowed_roots.iter())
            .any(|root| path.starts_with(root))
    }

    fn resolve_path(&self, raw_path: &str) -> Result<PathBuf, String> {
        if raw_path.trim().is_empty() {
            return Err("path must not be empty".to_owned());
        }
        let candidate = self.requested_path(raw_path);
        if !self.is_lexically_allowed(&candidate) {
            return Err(format!(
                "Path is outside of the allowed workspace: {raw_path}"
            ));
        }
        let canonical = std::fs::canonicalize(&candidate)
            .map_err(|error| format!("Path not found in workspace: {raw_path} ({error})"))?;
        if !self
            .allowed_roots
            .iter()
            .any(|root| canonical.starts_with(root))
        {
            return Err(format!(
                "Absolute path is outside of the allowed workspace: {raw_path}"
            ));
        }
        Ok(canonical)
    }

    fn ignore_reason(
        &self,
        path: &Path,
        is_directory: bool,
    ) -> Result<Option<&'static str>, String> {
        let mut path = path.to_string_lossy().into_owned();
        if is_directory && !path.ends_with('/') && !path.ends_with('\\') {
            path.push('/');
        }
        let git_ignored = self.file_discovery.should_git_ignore_file(&path)?;
        let canopy_ignored = self.file_discovery.should_canopy_ignore_file(&path);
        Ok(match (git_ignored, canopy_ignored) {
            (true, true) => Some("both"),
            (true, false) => Some("git"),
            (false, true) => Some("canopy"),
            (false, false) => None,
        })
    }

    fn is_ignored(&self, path: &Path, is_directory: bool) -> Result<bool, String> {
        Ok(self.ignore_reason(path, is_directory)?.is_some())
    }
}

fn lexical_normalize(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            std::path::Component::RootDir => normalized.push(component.as_os_str()),
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                if !normalized.pop() {
                    normalized.push("..");
                }
            }
            std::path::Component::Normal(part) => normalized.push(part),
        }
    }
    normalized
}

#[derive(Default)]
struct ReadPathOutcome {
    parts: Vec<Value>,
    files_read: Vec<String>,
    ignored: bool,
    truncated: bool,
}

impl ReadPathOutcome {
    fn ignored() -> Self {
        Self {
            ignored: true,
            ..Self::default()
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct BracedInjection<'a> {
    path: &'a str,
    start_index: usize,
    end_index: usize,
}

fn parse_braced_injections<'a>(
    text: &'a str,
    command_name: Option<&str>,
) -> Result<Vec<BracedInjection<'a>>, String> {
    let mut injections = Vec::new();
    let mut index = 0usize;
    while index < text.len() {
        let Some(offset) = text[index..].find(AT_FILE_TRIGGER) else {
            break;
        };
        let start_index = index + offset;
        if injections.len() >= MAX_INJECTIONS_PER_PROMPT {
            return Err(format!(
                "Prompt contains more than {MAX_INJECTIONS_PER_PROMPT} file injections."
            ));
        }
        let content_start = start_index + AT_FILE_TRIGGER.len();
        let mut brace_count = 1usize;
        let mut close_index = None;
        for (relative, character) in text[content_start..].char_indices() {
            match character {
                '{' => brace_count = brace_count.saturating_add(1),
                '}' => {
                    brace_count = brace_count.saturating_sub(1);
                    if brace_count == 0 {
                        close_index = Some(content_start + relative);
                        break;
                    }
                }
                _ => {}
            }
        }
        let Some(close_index) = close_index else {
            let context = command_name
                .map(|name| format!(" in command '{name}'"))
                .unwrap_or_default();
            return Err(format!(
                "Invalid syntax{context}: Unclosed injection starting at index {start_index} ('{AT_FILE_TRIGGER}'). Ensure braces are balanced. Paths with unbalanced braces are not supported directly."
            ));
        };
        let path = trim_ecmascript_whitespace(&text[content_start..close_index]);
        injections.push(BracedInjection {
            path,
            start_index,
            end_index: close_index + 1,
        });
        index = close_index + 1;
    }
    Ok(injections)
}

fn trim_ecmascript_whitespace(value: &str) -> &str {
    value.trim_matches(|character: char| {
        matches!(
            character,
            '\u{0009}'
                | '\u{000A}'
                | '\u{000B}'
                | '\u{000C}'
                | '\u{000D}'
                | '\u{0020}'
                | '\u{00A0}'
                | '\u{1680}'
                | '\u{2000}'
                ..='\u{200A}'
                    | '\u{2028}'
                    | '\u{2029}'
                    | '\u{202F}'
                    | '\u{205F}'
                    | '\u{3000}'
                    | '\u{FEFF}'
        )
    })
}

fn push_text_part(parts: &mut Vec<Value>, remaining: &mut usize, text: String) -> bool {
    let part = json!({"text": text});
    let length = value_json_size(&part);
    if length > *remaining {
        return false;
    }
    *remaining = (*remaining).saturating_sub(length);
    parts.push(part);
    true
}

fn parts_size(parts: &[Value]) -> usize {
    parts
        .iter()
        .map(value_json_size)
        .fold(0usize, usize::saturating_add)
}

fn value_json_size(value: &Value) -> usize {
    struct Counter(usize);
    impl io::Write for Counter {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            self.0 = self.0.saturating_add(buffer.len());
            Ok(buffer.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter(0);
    serde_json::to_writer(&mut counter, value).map_or(usize::MAX, |_| counter.0)
}
