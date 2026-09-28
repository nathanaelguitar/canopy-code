//! Native resolution for active extension and configured MCP prompt references.
//!
//! Hosts parse mixed `@` syntax and pass the path portion (without `@`) to
//! this service. The service only uses active local extension descriptors and
//! already-discovered, already-connected MCP resources; it never installs an
//! extension, discovers a server, or initiates a new connection.

use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};

use serde_json::{Value, json};
use uuid::Uuid;

use crate::extension_activation::{ExtensionActivation, ExtensionActivationResult};
use crate::extension_preferences::ExtensionScope;
use crate::resources::resource_registry::ResourceRegistry;
use crate::tools::mcp::client_manager::McpClientManager;
use crate::tools::mcp::client_runtime::McpRequestOptions;
use crate::tools::mcp::prompt_registry::PromptRegistry;
use crate::tools::mcp::resource_content::{
    FormatMcpResourceOptions, empty_mcp_resource_text, format_mcp_resource_contents,
};
use crate::utils::terminal_safe::strip_terminal_control_sequences;

pub const EXTENSION_CONTEXT_BUDGET: usize = 200_000;
pub const EXTENSION_CONTEXT_FILE_CAP: usize = 50_000;
const EXTENSION_CONTEXT_FILE_COUNT_CAP: usize = 64;
const EXTENSION_METADATA_BUDGET: usize = 16_000;
const MAX_REFERENCE_LABEL_CHARS: usize = 1_024;
const MAX_DIAGNOSTIC_CHARS: usize = 4_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AtResourceReferenceKind {
    Extension,
    McpServer,
    McpResource,
}

/// The host supplies only extensions already selected as active by its config.
/// Context paths may be absolute or relative to `path`, but canonical paths
/// must remain inside that extension directory after symlink resolution.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LocalExtensionReference {
    pub name: String,
    pub config_name: Option<String>,
    pub display_name: Option<String>,
    pub description: Option<String>,
    pub path: PathBuf,
    pub context_files: Vec<PathBuf>,
    pub skills: Vec<String>,
    pub mcp_servers: Vec<String>,
    pub agents: Vec<String>,
}

/// Runtime gates needed before a host can expose a validated local extension
/// candidate to prompt-reference resolution.
///
/// `activation` must already be resolved for the current workspace using the
/// extension identity and activation snapshot. Explicit CLI name overrides
/// take precedence over it, matching TypeScript `ExtensionManager.isEnabled`.
/// The scope preference is not an activation source; it is used only to keep
/// project-scoped candidates out of untrusted workspaces.
#[derive(Clone, Copy, Debug)]
pub struct LocalExtensionReferencePolicy<'a> {
    pub activation: ExtensionActivationResult,
    pub scope: Option<ExtensionScope>,
    pub enabled_extension_overrides: &'a [String],
    pub workspace_trusted: bool,
    pub safe_mode: bool,
    pub bare_mode: bool,
}

/// Project a validated local extension descriptor only when the current host
/// policy allows it to participate in prompt-reference resolution.
///
/// This function does not discover or parse installed extensions. Hosts must
/// provide metadata from a successfully loaded local extension, then supply
/// its resolved activation state and runtime gates here. The normal resolver
/// still applies canonical path containment and output-size limits when it
/// reads context files.
pub fn project_active_local_extension_reference(
    candidate: &LocalExtensionReference,
    policy: LocalExtensionReferencePolicy<'_>,
) -> Option<LocalExtensionReference> {
    if policy.safe_mode || candidate.name.trim().is_empty() || candidate.path.as_os_str().is_empty()
    {
        return None;
    }

    let overrides = policy.enabled_extension_overrides;
    if policy.bare_mode && overrides.is_empty() {
        return None;
    }

    let enabled_by_override = if overrides.is_empty() {
        None
    } else if overrides.len() == 1 && overrides[0].eq_ignore_ascii_case("none") {
        Some(false)
    } else {
        Some(
            overrides
                .iter()
                .any(|name| name.eq_ignore_ascii_case(&candidate.name)),
        )
    };
    let enabled =
        enabled_by_override.unwrap_or(policy.activation.effective == ExtensionActivation::Enabled);
    if !enabled {
        return None;
    }

    if !policy.workspace_trusted && policy.scope == Some(ExtensionScope::Project) {
        return None;
    }

    Some(candidate.clone())
}

/// A host-ready result. An absent `canonical_reference` means resolution
/// failed and the host should retain the original token as literal prompt text.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AtResourceReferenceResult {
    pub kind: Option<AtResourceReferenceKind>,
    pub canonical_reference: Option<String>,
    pub label: Option<String>,
    /// Gemini-compatible content parts (`{"text": ...}` / `inlineData`).
    pub parts: Vec<Value>,
    pub diagnostics: Vec<String>,
}

/// Resolves extension and MCP references against a host's current session view.
/// The extension list and configured server names are intentionally supplied by
/// the host: this core service does not infer activation or read config files.
pub struct AtResourceReferenceResolver<'a> {
    extensions: &'a [LocalExtensionReference],
    mcp_server_names: &'a [String],
    resources: &'a ResourceRegistry,
    prompts: &'a PromptRegistry,
    mcp: &'a McpClientManager,
    request_options: McpRequestOptions,
    extension_budget_remaining: usize,
    max_mcp_blob_chars: usize,
}

impl<'a> AtResourceReferenceResolver<'a> {
    pub fn new(
        extensions: &'a [LocalExtensionReference],
        mcp_server_names: &'a [String],
        resources: &'a ResourceRegistry,
        prompts: &'a PromptRegistry,
        mcp: &'a McpClientManager,
        request_options: McpRequestOptions,
    ) -> Self {
        Self {
            extensions,
            mcp_server_names,
            resources,
            prompts,
            mcp,
            request_options,
            extension_budget_remaining: EXTENSION_CONTEXT_BUDGET,
            max_mcp_blob_chars: crate::tools::mcp::resource_content::MAX_MCP_RESOURCE_BLOB_CHARS,
        }
    }

    /// Optionally resolve one parsed `@` token without its leading `@`.
    /// `None` means the token is not one of the supported reference forms and
    /// the caller should continue its ordinary path/session parsing.
    pub async fn resolve(&mut self, path_name: &str) -> Option<AtResourceReferenceResult> {
        if let Some(name) = parse_prefixed_name(path_name, "ext:") {
            return Some(self.resolve_extension(name));
        }

        // Match TypeScript precedence: a configured server resource such as
        // `mcp:resource://x` is a resource ref before `mcp:<server>` is treated
        // as an advisory server mention.
        if let Some((server_name, uri)) = match_mcp_server_prefix(path_name, self.mcp_server_names)
        {
            if !uri.is_empty() {
                return Some(self.resolve_mcp_resource(server_name, uri).await);
            }
        }

        if let Some(name) = parse_prefixed_name(path_name, "mcp:") {
            return Some(self.resolve_mcp_server(name));
        }
        None
    }

    fn resolve_extension(&mut self, requested_name: &str) -> AtResourceReferenceResult {
        let Some(extension) = self.extensions.iter().find(|extension| {
            extension.name.eq_ignore_ascii_case(requested_name)
                || extension
                    .config_name
                    .as_ref()
                    .is_some_and(|name| name.eq_ignore_ascii_case(requested_name))
        }) else {
            return AtResourceReferenceResult {
                kind: Some(AtResourceReferenceKind::Extension),
                diagnostics: vec![format!(
                    "Extension `{}` was not found among active local extensions.",
                    bounded_display(requested_name, MAX_DIAGNOSTIC_CHARS)
                )],
                ..Default::default()
            };
        };

        let display_name = sanitize_display(
            extension
                .display_name
                .as_deref()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or(&extension.name),
            MAX_REFERENCE_LABEL_CHARS,
        );
        let mut diagnostics = Vec::new();
        let context = build_extension_context(
            extension,
            &display_name,
            &mut self.extension_budget_remaining,
            &mut diagnostics,
        );
        let canonical_reference = format!("ext:{}", extension.name);
        AtResourceReferenceResult {
            kind: Some(AtResourceReferenceKind::Extension),
            canonical_reference: Some(canonical_reference.clone()),
            label: Some(canonical_reference),
            parts: vec![json!({"text": context})],
            diagnostics,
        }
    }

    fn resolve_mcp_server(&self, requested_name: &str) -> AtResourceReferenceResult {
        let Some(server_name) = self
            .mcp_server_names
            .iter()
            .find(|name| name.eq_ignore_ascii_case(requested_name))
        else {
            return AtResourceReferenceResult {
                kind: Some(AtResourceReferenceKind::McpServer),
                diagnostics: vec![format!(
                    "MCP server `{}` was not found among configured servers.",
                    bounded_display(requested_name, MAX_DIAGNOSTIC_CHARS)
                )],
                ..Default::default()
            };
        };

        let display_name = sanitize_display(server_name, MAX_REFERENCE_LABEL_CHARS);
        let resource_count = self.resources.get_resources_by_server(server_name).len();
        let prompt_count = self.prompts.get_prompts_by_server(server_name).len();
        let mut lines = vec![
            format!("--- MCP Server: {display_name} ---"),
            "The user explicitly mentioned this MCP server. Prefer using tools and resources from this server when relevant for this turn. This is advisory context, not a hard restriction.".to_owned(),
        ];
        let mut details = Vec::new();
        if resource_count > 0 {
            details.push(format!("- Resources: {resource_count}"));
        }
        if prompt_count > 0 {
            details.push(format!("- Prompts: {prompt_count}"));
        }
        if !details.is_empty() {
            lines.push("Available capabilities from this MCP server:".to_owned());
            lines.extend(details);
        }
        lines.push(format!("--- End MCP Server: {display_name} ---"));

        AtResourceReferenceResult {
            kind: Some(AtResourceReferenceKind::McpServer),
            canonical_reference: Some(format!("mcp:{server_name}")),
            label: Some(format!("mcp:{server_name}")),
            parts: vec![json!({"text": lines.join("\n")})],
            diagnostics: Vec::new(),
        }
    }

    async fn resolve_mcp_resource(
        &self,
        server_name: &str,
        uri: &str,
    ) -> AtResourceReferenceResult {
        let display_label =
            sanitize_display(&format!("{server_name}:{uri}"), MAX_REFERENCE_LABEL_CHARS);
        let canonical_reference = format!("{server_name}:{uri}");
        let registered = self.resources.get_resource(server_name, uri);
        if registered.is_none() {
            return AtResourceReferenceResult {
                kind: Some(AtResourceReferenceKind::McpResource),
                canonical_reference: Some(canonical_reference),
                label: Some(display_label.clone()),
                parts: Vec::new(),
                diagnostics: vec![format!(
                    "MCP resource `{display_label}` is not in the current discovered resource list."
                )],
            };
        }

        let response = match self
            .mcp
            .read_resource(server_name, uri, self.request_options.clone())
            .await
        {
            Ok(response) => response,
            Err(error) => {
                return AtResourceReferenceResult {
                    kind: Some(AtResourceReferenceKind::McpResource),
                    canonical_reference: Some(canonical_reference),
                    label: Some(display_label.clone()),
                    parts: Vec::new(),
                    diagnostics: vec![format!(
                        "Failed to read MCP resource `{display_label}`: {}",
                        bounded_display(&error.to_string(), MAX_DIAGNOSTIC_CHARS)
                    )],
                };
            }
        };

        let formatted = match format_mcp_resource_contents(
            &response,
            &display_label,
            Some(FormatMcpResourceOptions {
                max_blob_chars: Some(self.max_mcp_blob_chars),
            }),
        ) {
            Ok(formatted) => formatted,
            Err(error) => {
                return AtResourceReferenceResult {
                    kind: Some(AtResourceReferenceKind::McpResource),
                    canonical_reference: Some(canonical_reference),
                    label: Some(display_label.clone()),
                    parts: Vec::new(),
                    diagnostics: vec![format!(
                        "MCP resource `{display_label}` returned invalid content: {}",
                        bounded_display(&error.to_string(), MAX_DIAGNOSTIC_CHARS)
                    )],
                };
            }
        };

        let parts = if formatted.parts.is_empty() {
            vec![json!({
                "text": empty_mcp_resource_text(&formatted, &display_label)
            })]
        } else {
            formatted.parts
        };
        let diagnostics = formatted
            .truncated
            .then(|| {
                format!("MCP resource `{display_label}` was truncated to configured output limits.")
            })
            .into_iter()
            .collect();
        AtResourceReferenceResult {
            kind: Some(AtResourceReferenceKind::McpResource),
            canonical_reference: Some(canonical_reference),
            label: Some(display_label),
            parts,
            diagnostics,
        }
    }
}

fn parse_prefixed_name<'a>(path_name: &'a str, prefix: &str) -> Option<&'a str> {
    path_name
        .strip_prefix(prefix)
        .filter(|name| !name.is_empty())
}

fn match_mcp_server_prefix<'a>(
    path_name: &'a str,
    server_names: &[String],
) -> Option<(&'a str, &'a str)> {
    let server_name = server_names
        .iter()
        .filter(|name| path_name.starts_with(&format!("{}:", name)))
        .max_by_key(|name| name.len())?;
    let remainder = &path_name[server_name.len() + 1..];
    Some((&path_name[..server_name.len()], remainder))
}

fn build_extension_context(
    extension: &LocalExtensionReference,
    display_name: &str,
    remaining_budget: &mut usize,
    diagnostics: &mut Vec<String>,
) -> String {
    let initial_budget = *remaining_budget;
    let end_frame = format!("\n--- End Extension: {display_name} [00000000] ---");
    if initial_budget <= utf16_len(&end_frame).saturating_add(128) {
        diagnostics
            .push("Extension context budget exhausted; extension context was skipped.".to_owned());
        return String::new();
    }

    let extension_root = match fs::canonicalize(&extension.path) {
        Ok(path) if path.is_dir() => Some(path),
        Ok(_) => {
            diagnostics.push(format!(
                "Skipping extension context for `{}` because its local path is not a directory.",
                bounded_display(&extension.name, MAX_DIAGNOSTIC_CHARS)
            ));
            None
        }
        Err(error) => {
            diagnostics.push(format!(
                "Could not resolve local extension `{}`: {}",
                bounded_display(&extension.name, MAX_DIAGNOSTIC_CHARS),
                bounded_display(&error.to_string(), MAX_DIAGNOSTIC_CHARS)
            ));
            None
        }
    };

    let nonce = Uuid::new_v4().simple().to_string();
    let nonce = &nonce[..8];
    let mut lines = vec![format!(
        "--- Extension: {display_name} (untrusted third-party content) [{nonce}] ---"
    )];
    if let Some(description) = extension.description.as_deref() {
        let description = sanitize_display(description, 4_000);
        if !description.is_empty() {
            lines.push(description);
            lines.push(String::new());
        }
    }

    let mut capabilities = Vec::new();
    push_capability(&mut capabilities, "Skills", &extension.skills);
    push_capability(&mut capabilities, "MCP Servers", &extension.mcp_servers);
    push_capability(&mut capabilities, "Agents", &extension.agents);
    if !capabilities.is_empty() {
        lines.push("Available capabilities from this extension:".to_owned());
        lines.extend(capabilities);
        lines.push(String::new());
    }

    let metadata = lines.join("\n");
    let metadata_budget =
        EXTENSION_METADATA_BUDGET.min(initial_budget.saturating_sub(utf16_len(&end_frame)));
    let (metadata, metadata_truncated) = take_utf16_prefix(&metadata, metadata_budget);
    if metadata_truncated {
        diagnostics.push(format!(
            "Extension `{}` metadata was truncated to its context limit.",
            bounded_display(&extension.name, MAX_DIAGNOSTIC_CHARS)
        ));
    }
    let mut context = metadata;

    if let Some(extension_root) = extension_root {
        for (index, context_path) in extension.context_files.iter().enumerate() {
            if index >= EXTENSION_CONTEXT_FILE_COUNT_CAP {
                diagnostics.push(format!(
                    "Extension `{}` has more than {EXTENSION_CONTEXT_FILE_COUNT_CAP} context files; remaining files were skipped.",
                    bounded_display(&extension.name, MAX_DIAGNOSTIC_CHARS)
                ));
                break;
            }
            if *remaining_budget == 0 {
                diagnostics.push(
                    "Extension context budget exhausted; remaining files were skipped.".to_owned(),
                );
                break;
            }

            let candidate = if context_path.is_absolute() {
                context_path.clone()
            } else {
                extension_root.join(context_path)
            };
            let canonical_path = match fs::canonicalize(&candidate) {
                Ok(path) => path,
                Err(error) => {
                    diagnostics.push(format!(
                        "Could not read extension context file `{}`: {}",
                        bounded_display(&candidate.display().to_string(), MAX_DIAGNOSTIC_CHARS),
                        bounded_display(&error.to_string(), MAX_DIAGNOSTIC_CHARS)
                    ));
                    continue;
                }
            };
            if !canonical_path.starts_with(&extension_root) {
                diagnostics.push(format!(
                    "Skipped extension context file `{}` because it resolves outside its extension directory.",
                    bounded_display(&candidate.display().to_string(), MAX_DIAGNOSTIC_CHARS)
                ));
                continue;
            }
            if !canonical_path.is_file() {
                diagnostics.push(format!(
                    "Skipped extension context path `{}` because it is not a file.",
                    bounded_display(&candidate.display().to_string(), MAX_DIAGNOSTIC_CHARS)
                ));
                continue;
            }

            let framing_budget = utf16_len(&end_frame).saturating_add(2);
            let file_budget = initial_budget
                .saturating_sub(utf16_len(&context))
                .saturating_sub(framing_budget)
                .min(EXTENSION_CONTEXT_FILE_CAP);
            if file_budget == 0 {
                diagnostics.push(
                    "Extension context budget exhausted; remaining files were skipped.".to_owned(),
                );
                break;
            }
            match read_bounded_text(&canonical_path, file_budget) {
                Ok((content, truncated)) => {
                    if content.trim().is_empty() {
                        continue;
                    }
                    if truncated {
                        diagnostics.push(format!(
                            "Extension context file `{}` was truncated to its per-file limit.",
                            bounded_display(&candidate.display().to_string(), MAX_DIAGNOSTIC_CHARS)
                        ));
                    }
                    context.push_str("\n\n");
                    context.push_str(&content);
                }
                Err(error) => diagnostics.push(format!(
                    "Could not read extension context file `{}`: {}",
                    bounded_display(&candidate.display().to_string(), MAX_DIAGNOSTIC_CHARS),
                    bounded_display(&error.to_string(), MAX_DIAGNOSTIC_CHARS)
                )),
            }
        }
    }

    context.push_str(&format!(
        "\n--- End Extension: {display_name} [{nonce}] ---"
    ));
    *remaining_budget = initial_budget.saturating_sub(utf16_len(&context));
    context
}

fn push_capability(lines: &mut Vec<String>, label: &str, values: &[String]) {
    if values.is_empty() {
        return;
    }
    let mut sanitized = values
        .iter()
        .take(128)
        .map(|value| sanitize_display(value, 512))
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    if sanitized.is_empty() {
        return;
    }
    if values.len() > 128 {
        sanitized.push("…".to_owned());
    }
    lines.push(format!("- {label}: {}", sanitized.join(", ")));
}

fn read_bounded_text(path: &Path, max_utf16_units: usize) -> std::io::Result<(String, bool)> {
    let max_bytes = max_utf16_units.saturating_mul(3).saturating_add(4);
    let mut file = File::open(path)?;
    let mut bytes = Vec::with_capacity(max_bytes.min(64 * 1024));
    file.by_ref()
        .take(max_bytes as u64)
        .read_to_end(&mut bytes)?;
    let file_is_larger = fs::metadata(path)
        .map(|metadata| metadata.len() > bytes.len() as u64)
        .unwrap_or(false);
    let lossy = String::from_utf8_lossy(&bytes);
    let (text, text_truncated) = take_utf16_prefix(&lossy, max_utf16_units);
    Ok((text, file_is_larger || text_truncated))
}

fn take_utf16_prefix(value: &str, max_units: usize) -> (String, bool) {
    let mut output = String::new();
    let mut units: usize = 0;
    let mut truncated = false;
    for character in value.chars() {
        let character_units = character.len_utf16();
        if units.saturating_add(character_units) > max_units {
            truncated = true;
            break;
        }
        output.push(character);
        units += character_units;
    }
    (output, truncated)
}

fn utf16_len(value: &str) -> usize {
    value.encode_utf16().count()
}

fn sanitize_display(value: &str, max_chars: usize) -> String {
    let stripped = strip_terminal_control_sequences(value);
    let without_bidi = stripped
        .chars()
        .filter(|character| {
            !matches!(
                character,
                '\u{061c}'
                    | '\u{200e}'
                    | '\u{200f}'
                    | '\u{202a}'..='\u{202e}'
                    | '\u{2066}'..='\u{2069}'
            )
        })
        .collect::<String>();
    let compact = without_bidi
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    truncate_display(&compact, max_chars)
}

fn bounded_display(value: &str, max_chars: usize) -> String {
    sanitize_display(value, max_chars)
}

fn truncate_display(value: &str, max_chars: usize) -> String {
    let (mut output, truncated) = take_utf16_prefix(value, max_chars);
    if truncated {
        output.push('…');
    }
    output
}
