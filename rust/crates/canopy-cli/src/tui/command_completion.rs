//! Slash-command suggestions for the commands implemented by the native host.

use std::collections::HashSet;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

use canopy_core::services::at_resource_references::LocalExtensionReference;
use walkdir::WalkDir;

const COMMANDS: [Command; 6] = [
    Command {
        name: "help",
        aliases: &[],
        description: "For help on Canopy Code.",
    },
    Command {
        name: "quit",
        aliases: &["exit"],
        description: "Exit the CLI.",
    },
    Command {
        name: "memory",
        aliases: &[],
        description: "Show managed-memory status for this workspace.",
    },
    Command {
        name: "hooks",
        aliases: &[],
        description: "Browse configured hooks.",
    },
    Command {
        name: "doctor",
        aliases: &[],
        description: "Check the native runtime or show memory diagnostics.",
    },
    Command {
        name: "stats",
        aliases: &["usage"],
        description: "Show usage statistics dashboard.",
    },
];

const DOCTOR_SUBCOMMANDS: [Command; 2] = [
    Command {
        name: "memory",
        aliases: &[],
        description: "Show process and system memory measurements.",
    },
    Command {
        name: "rollback",
        aliases: &[],
        description: "Restore the previous standalone installation.",
    },
];

const DOCTOR_MEMORY_FLAGS: [Command; 2] = [
    Command {
        name: "--json",
        aliases: &[],
        description: "Write machine-readable diagnostics.",
    },
    Command {
        name: "--sample",
        aliases: &[],
        description: "Collect three RSS samples one second apart.",
    },
];

const STATS_SUBCOMMANDS: [Command; 6] = [
    Command {
        name: "model",
        aliases: &[],
        description: "Show per-model requests, errors, latency, and tokens.",
    },
    Command {
        name: "tools",
        aliases: &[],
        description: "Show tool-call counts and durations.",
    },
    Command {
        name: "skills",
        aliases: &[],
        description: "Show skill-call counts and success rates.",
    },
    Command {
        name: "daily",
        aliases: &["day"],
        description: "Show daily token usage, optionally for YYYY-MM-DD.",
    },
    Command {
        name: "monthly",
        aliases: &["month"],
        description: "Show monthly token usage, optionally for YYYY-MM.",
    },
    Command {
        name: "export",
        aliases: &[],
        description: "Export daily or monthly token usage.",
    },
];

const EXPORT_PERIODS: [Command; 4] = [
    Command {
        name: "daily",
        aliases: &["day"],
        description: "Export daily token usage.",
    },
    Command {
        name: "monthly",
        aliases: &["month"],
        description: "Export monthly token usage.",
    },
    Command {
        name: "daily",
        aliases: &[],
        description: "Export daily token usage.",
    },
    Command {
        name: "monthly",
        aliases: &[],
        description: "Export monthly token usage.",
    },
];

const EXPORT_FORMATS: [Command; 2] = [
    Command {
        name: "csv",
        aliases: &[],
        description: "Write comma-separated values.",
    },
    Command {
        name: "json",
        aliases: &[],
        description: "Write JSON.",
    },
];

const EXPORT_FLAGS: [Command; 2] = [
    Command {
        name: "--format",
        aliases: &["-f"],
        description: "Choose csv or json output.",
    },
    Command {
        name: "--output",
        aliases: &["-o"],
        description: "Write to a file path.",
    },
];

const MAX_VISIBLE_SUGGESTIONS: usize = 5;
const MAX_STACKED_SKILLS: usize = 5;
const MAX_PROMPT_COMMANDS: usize = 256;
const MAX_PROMPT_COMMAND_FILES: usize = 4096;
const MAX_EXTENSION_COMMAND_ROOTS: usize = 64;
const MAX_EXTENSION_COMMAND_EXTENSIONS: usize = 256;
const MAX_EXTENSION_COMMAND_PATH_BYTES: usize = 4096;
const MAX_EXTENSION_COMMAND_FILE_BYTES: u64 = 1024 * 1024;
const MAX_EXTENSION_COMMAND_TOTAL_BYTES: u64 = 16 * 1024 * 1024;
const MAX_EXTENSION_COMMAND_NAME_BYTES: usize = 256;
const MAX_COMPLETION_INPUT_CHARS: usize = 64 * 1024;
const MAX_EXPANDED_PROMPT_COMMAND_BYTES: usize = 2 * 1024 * 1024;

#[derive(Clone, Copy)]
struct Command {
    name: &'static str,
    aliases: &'static [&'static str],
    description: &'static str,
}

pub(super) struct Suggestion {
    pub(super) name: String,
    pub(super) description: String,
    matched_alias: Option<String>,
}

impl Suggestion {
    pub(super) fn label(&self, include_aliases: bool, is_root_command: bool) -> String {
        if let Some(alias) = self.matched_alias.as_deref() {
            if is_root_command {
                format!("/{} (alias: {alias})", self.name)
            } else {
                format!("{} (alias: {alias})", self.name)
            }
        } else if include_aliases && is_root_command {
            match self.name.as_str() {
                "quit" => "/quit (exit)".to_owned(),
                "memory" => "/memory".to_owned(),
                "hooks" => "/hooks".to_owned(),
                "doctor" => "/doctor".to_owned(),
                "stats" => "/stats (usage)".to_owned(),
                name => format!("/{name}"),
            }
        } else if is_root_command {
            format!("/{}", self.name)
        } else {
            self.name.to_owned()
        }
    }
}

pub(super) struct Completion {
    pub(super) start: usize,
    pub(super) end: usize,
    pub(super) query: String,
    pub(super) suggestions: Vec<Suggestion>,
    pub(super) is_root_command: bool,
    perfect_match: bool,
}

/// A prompt-only command loaded from a user, workspace, or extension directory.
/// Commands with unsupported processors remain visible but fail explicitly.
#[derive(Clone, Debug)]
pub(super) struct PromptCommand {
    pub(super) name: String,
    prompt: String,
    unsupported_processor: Option<&'static str>,
}

impl Completion {
    pub(super) fn is_perfect_match(&self) -> bool {
        self.perfect_match
    }
}

pub(super) fn for_input(
    input: &[char],
    cursor: usize,
    skill_commands: &[String],
    file_commands: &[String],
) -> Option<Completion> {
    if input.len() > MAX_COMPLETION_INPUT_CHARS || input.contains(&'\n') {
        return None;
    }

    let command_start = input
        .iter()
        .position(|character| !character.is_whitespace())?;
    if input.get(command_start) != Some(&'/') || cursor <= command_start {
        return None;
    }

    let command_end = (command_start + 1..input.len())
        .find(|index| input[*index].is_whitespace())
        .unwrap_or(input.len());
    if cursor <= command_end {
        let query = input[command_start + 1..command_end]
            .iter()
            .collect::<String>();
        let mut completion =
            completion_for_candidates(command_start, command_end, query, &COMMANDS, true);
        add_skill_suggestions(&mut completion, skill_commands);
        add_file_command_suggestions(&mut completion, file_commands);
        completion.suggestions.truncate(MAX_VISIBLE_SUGGESTIONS);
        return Some(completion);
    }

    let command = input[command_start + 1..command_end]
        .iter()
        .collect::<String>();
    let cursor = cursor.min(input.len());
    let current_start = (command_end..cursor)
        .rev()
        .find(|index| input[*index].is_whitespace())
        .map_or(command_end, |index| index + 1);
    let current_end = (cursor..input.len())
        .find(|index| input[*index].is_whitespace())
        .unwrap_or(input.len());
    let previous_tokens = input[command_end..current_start]
        .iter()
        .collect::<String>()
        .split_whitespace()
        .map(str::to_ascii_lowercase)
        .collect::<Vec<_>>();
    let query = input[current_start..current_end].iter().collect::<String>();
    let candidates = if !file_command_shadows_native_name(file_commands, &command)
        && (command.eq_ignore_ascii_case("stats") || command.eq_ignore_ascii_case("usage"))
    {
        stats_argument_candidates(&previous_tokens)
    } else if !file_command_shadows_native_name(file_commands, &command)
        && command.eq_ignore_ascii_case("doctor")
    {
        doctor_argument_candidates(&previous_tokens)
    } else {
        None
    };
    if let Some(candidates) = candidates {
        let mut completion =
            completion_for_candidates(current_start, current_end, query, candidates, false);
        completion.suggestions.truncate(MAX_VISIBLE_SUGGESTIONS);
        return Some(completion);
    }
    if command.eq_ignore_ascii_case("stats")
        || command.eq_ignore_ascii_case("usage")
        || command.eq_ignore_ascii_case("doctor")
    {
        return None;
    }

    let Some(skill_query) = query.strip_prefix('/') else {
        return None;
    };
    let prefix = input[command_start + 1..current_start]
        .iter()
        .collect::<String>();
    if !is_valid_skill_stack_prefix(&prefix, skill_commands) {
        return None;
    }
    let mut completion = Completion {
        start: current_start,
        end: current_end,
        query: skill_query.to_owned(),
        suggestions: Vec::new(),
        is_root_command: false,
        perfect_match: false,
    };
    add_skill_suggestions(&mut completion, skill_commands);
    completion.suggestions.truncate(MAX_VISIBLE_SUGGESTIONS);
    (!completion.suggestions.is_empty()).then_some(completion)
}

/// Discover user and workspace commands before extension commands so project
/// files override user files and extensions receive namespaced conflict names.
pub(super) fn load_prompt_commands(
    user_command_dir: &Path,
    workspace_command_dir: &Path,
    active_extensions: &[LocalExtensionReference],
    safe_mode: bool,
    bare_mode: bool,
    workspace_trusted: bool,
    disabled_names: &HashSet<String>,
    skill_commands: &[String],
) -> Vec<PromptCommand> {
    if bare_mode || !workspace_trusted {
        return Vec::new();
    }

    let mut occupied = COMMANDS
        .iter()
        .flat_map(|command| std::iter::once(command.name).chain(command.aliases.iter().copied()))
        .map(str::to_ascii_lowercase)
        .chain(skill_commands.iter().map(|name| name.to_ascii_lowercase()))
        .collect::<HashSet<_>>();
    let mut output = Vec::new();
    let mut visited_entries = 0usize;
    let mut loaded_bytes = 0u64;

    for command_dir in [user_command_dir, workspace_command_dir] {
        load_local_prompt_commands(
            command_dir,
            &mut output,
            &mut occupied,
            &mut visited_entries,
            &mut loaded_bytes,
        );
    }

    // FileCommandLoader runs in safe mode, but extension activation does not;
    // the native extension prompt path continues to skip extensions there.
    if !safe_mode {
        let mut extensions = active_extensions.iter().collect::<Vec<_>>();
        extensions.sort_by(|left, right| left.name.cmp(&right.name));

        for extension in extensions
            .into_iter()
            .take(MAX_EXTENSION_COMMAND_EXTENSIONS)
        {
            if output.len() >= MAX_PROMPT_COMMANDS
                || visited_entries >= MAX_PROMPT_COMMAND_FILES
                || loaded_bytes >= MAX_EXTENSION_COMMAND_TOTAL_BYTES
            {
                break;
            }
            let Ok(extension_root) = fs::canonicalize(&extension.path) else {
                continue;
            };
            if !extension_root.is_dir()
                || canopy_core::agent_plugins::get_agent_plugin_schema_status(
                    &extension_root.to_string_lossy(),
                ) != canopy_core::agent_plugins::AgentPluginSchemaStatus::Unrelated
            {
                continue;
            }

            let display_name = extension
                .display_name
                .as_deref()
                .filter(|name| {
                    !name.trim().is_empty()
                        && name.len() <= 128
                        && !name.chars().any(char::is_whitespace)
                        && !name.chars().any(char::is_control)
                })
                .unwrap_or(&extension.name);
            let command_roots = extension_command_roots(&extension_root);
            for command_root in command_roots {
                if output.len() >= MAX_PROMPT_COMMANDS
                    || visited_entries >= MAX_PROMPT_COMMAND_FILES
                    || loaded_bytes >= MAX_EXTENSION_COMMAND_TOTAL_BYTES
                {
                    break;
                }
                let Ok(command_root) = fs::canonicalize(command_root) else {
                    continue;
                };
                if !command_root.is_dir() {
                    continue;
                }
                let mut files = Vec::new();
                for result in WalkDir::new(&command_root)
                    .follow_links(true)
                    .max_depth(32)
                    .max_open(8)
                    .into_iter()
                    .take(MAX_PROMPT_COMMAND_FILES.saturating_sub(visited_entries))
                {
                    visited_entries = visited_entries.saturating_add(1);
                    let Ok(entry) = result else {
                        continue;
                    };
                    if entry.file_type().is_file()
                        && matches!(
                            entry
                                .path()
                                .extension()
                                .and_then(|extension| extension.to_str()),
                            Some("md" | "toml")
                        )
                    {
                        files.push(entry);
                    }
                }
                files.sort_by(|left, right| left.path().cmp(right.path()));

                for entry in files {
                    if output.len() >= MAX_PROMPT_COMMANDS
                        || loaded_bytes >= MAX_EXTENSION_COMMAND_TOTAL_BYTES
                    {
                        break;
                    }
                    let Ok(entry_path) = fs::canonicalize(entry.path()) else {
                        continue;
                    };
                    if !entry_path.starts_with(&command_root) {
                        continue;
                    }
                    let remaining_bytes = MAX_EXTENSION_COMMAND_TOTAL_BYTES - loaded_bytes;
                    let max_file_bytes = MAX_EXTENSION_COMMAND_FILE_BYTES.min(remaining_bytes);
                    let Some(contents) = read_command_file(&entry_path, max_file_bytes) else {
                        continue;
                    };
                    loaded_bytes = loaded_bytes.saturating_add(contents.len() as u64);
                    let Ok(contents) = std::str::from_utf8(&contents) else {
                        continue;
                    };
                    let Some((prompt, unsupported_processor)) =
                        parse_prompt_command(&entry_path, contents)
                    else {
                        continue;
                    };
                    let Some(relative) = entry_path.strip_prefix(&command_root).ok() else {
                        continue;
                    };
                    let Some(command_name) = prompt_command_name(relative) else {
                        continue;
                    };

                    let mut final_name = command_name.clone();
                    if occupied.contains(&final_name.to_ascii_lowercase()) {
                        let prefixed_name = format!("{display_name}.{command_name}");
                        final_name = prefixed_name.clone();
                        let mut suffix = 1usize;
                        while occupied.contains(&final_name.to_ascii_lowercase()) {
                            final_name = format!("{prefixed_name}{suffix}");
                            suffix = suffix.saturating_add(1);
                        }
                    }
                    if !is_valid_prompt_command_name(&final_name) {
                        continue;
                    }
                    occupied.insert(final_name.to_ascii_lowercase());
                    output.push(PromptCommand {
                        name: final_name,
                        prompt,
                        unsupported_processor,
                    });
                }
            }
        }
    }

    let disabled = disabled_names
        .iter()
        .map(|name| name.trim().to_ascii_lowercase())
        .collect::<HashSet<_>>();
    output.retain(|command| !disabled.contains(&command.name.to_ascii_lowercase()));
    output
}

fn load_local_prompt_commands(
    command_dir: &Path,
    output: &mut Vec<PromptCommand>,
    occupied: &mut HashSet<String>,
    visited_entries: &mut usize,
    loaded_bytes: &mut u64,
) {
    if *visited_entries >= MAX_PROMPT_COMMAND_FILES
        || *loaded_bytes >= MAX_EXTENSION_COMMAND_TOTAL_BYTES
    {
        return;
    }
    let Ok(command_root) = fs::canonicalize(command_dir) else {
        return;
    };
    if !command_root.is_dir() {
        return;
    }

    let mut toml_files = Vec::new();
    let mut markdown_files = Vec::new();
    for result in WalkDir::new(&command_root)
        .follow_links(true)
        .max_depth(32)
        .max_open(8)
        .into_iter()
        .take(MAX_PROMPT_COMMAND_FILES.saturating_sub(*visited_entries))
    {
        *visited_entries = visited_entries.saturating_add(1);
        let Ok(entry) = result else {
            continue;
        };
        if !entry.file_type().is_file() {
            continue;
        }
        match entry
            .path()
            .extension()
            .and_then(|extension| extension.to_str())
        {
            Some("toml") => toml_files.push(entry),
            Some("md") => markdown_files.push(entry),
            _ => {}
        }
    }
    toml_files.sort_by(|left, right| left.path().cmp(right.path()));
    markdown_files.sort_by(|left, right| left.path().cmp(right.path()));

    for entry in toml_files.into_iter().chain(markdown_files) {
        if *loaded_bytes >= MAX_EXTENSION_COMMAND_TOTAL_BYTES {
            break;
        }
        let Ok(entry_path) = fs::canonicalize(entry.path()) else {
            continue;
        };
        if !entry_path.starts_with(&command_root) {
            continue;
        }
        let remaining_bytes = MAX_EXTENSION_COMMAND_TOTAL_BYTES - *loaded_bytes;
        let max_file_bytes = MAX_EXTENSION_COMMAND_FILE_BYTES.min(remaining_bytes);
        let Some(contents) = read_command_file(&entry_path, max_file_bytes) else {
            continue;
        };
        *loaded_bytes = loaded_bytes.saturating_add(contents.len() as u64);
        let Ok(contents) = std::str::from_utf8(&contents) else {
            continue;
        };
        let Some((prompt, unsupported_processor)) = parse_prompt_command(&entry_path, contents)
        else {
            continue;
        };
        let Some(relative) = entry_path.strip_prefix(&command_root).ok() else {
            continue;
        };
        let Some(name) = prompt_command_name(relative) else {
            continue;
        };
        if output.len() >= MAX_PROMPT_COMMANDS
            && !output.iter().any(|existing| existing.name == name)
        {
            continue;
        }

        let normalized = name.to_ascii_lowercase();
        occupied.insert(normalized.clone());
        let command = PromptCommand {
            name,
            prompt,
            unsupported_processor,
        };
        if let Some(existing) = output
            .iter_mut()
            .find(|existing| existing.name == command.name)
        {
            // Workspace commands are loaded second and override user commands.
            *existing = command;
        } else {
            output.push(command);
        }
    }
}

fn prompt_command_name(relative_path: &Path) -> Option<String> {
    let name = relative_path
        .with_extension("")
        .components()
        .map(|component| component.as_os_str().to_string_lossy().replace(':', "_"))
        .collect::<Vec<_>>()
        .join(":");
    is_valid_prompt_command_name(&name).then_some(name)
}

fn is_valid_prompt_command_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_EXTENSION_COMMAND_NAME_BYTES
        && !name.chars().any(char::is_whitespace)
        && !name.chars().any(char::is_control)
}

/// Load only the static prompt definition used by the native TUI. This keeps
/// the custom-command path bounded and avoids evaluating shell or file
/// processors as part of prompt submission.
fn parse_prompt_command(path: &Path, content: &str) -> Option<(String, Option<&'static str>)> {
    let content = canopy_core::skills::normalize_content(content);
    let prompt = match path.extension().and_then(|extension| extension.to_str())? {
        "toml" => {
            let parsed = toml::from_str::<toml::Value>(&content).ok()?;
            if parsed
                .get("description")
                .is_some_and(|description| !description.is_str())
            {
                return None;
            }
            parsed.get("prompt")?.as_str()?.to_owned()
        }
        "md" => parse_markdown_prompt(&content)?,
        _ => return None,
    };
    // File injections are expanded by the workspace-confined native
    // AtFileProcessor after invocation. Shell injections still require the
    // TypeScript confirmation and permission lifecycle and remain fail-closed.
    let unsupported_processor = prompt.contains("!{").then_some("shell command injection");
    Some((prompt, unsupported_processor))
}

fn parse_markdown_prompt(content: &str) -> Option<String> {
    let Some(frontmatter_and_body) = content.strip_prefix("---\n") else {
        return Some(content.trim().to_owned());
    };

    let (frontmatter, body) = if frontmatter_and_body.starts_with("---\n") {
        ("", &frontmatter_and_body[4..])
    } else if frontmatter_and_body == "---" {
        ("", "")
    } else {
        let delimiter = frontmatter_and_body
            .match_indices("\n---")
            .find(|(offset, _)| {
                let after = *offset + 4;
                after == frontmatter_and_body.len()
                    || frontmatter_and_body.as_bytes().get(after) == Some(&b'\n')
            });
        let Some((delimiter, _)) = delimiter else {
            return Some(content.trim().to_owned());
        };
        let body_start = delimiter + 4;
        let body = frontmatter_and_body[body_start..]
            .strip_prefix('\n')
            .unwrap_or(&frontmatter_and_body[body_start..]);
        (&frontmatter_and_body[..delimiter], body)
    };

    if !frontmatter.trim().is_empty() {
        let metadata = canopy_core::utils::yaml::parse(frontmatter);
        if metadata
            .get("prompt")
            .is_some_and(|value| !value.is_string())
        {
            return None;
        }
        if metadata
            .get("description")
            .is_some_and(|value| !value.is_string())
            || metadata
                .get("argument-hint")
                .is_some_and(|value| !value.is_string())
            || metadata
                .get("when_to_use")
                .is_some_and(|value| !value.is_string())
            || metadata
                .get("disable-model-invocation")
                .is_some_and(|value| !value.is_boolean())
        {
            return None;
        }
    }
    Some(body.trim().to_owned())
}

/// Resolve a typed custom slash invocation. `None` means the input is not a
/// loaded file command. Errors must be shown instead of submitting the slash
/// text as a model turn.
pub(super) fn expand_prompt_command(
    commands: &[PromptCommand],
    input: &str,
) -> Option<Result<String, String>> {
    if input.len() > MAX_COMPLETION_INPUT_CHARS {
        return None;
    }
    let invocation = input.trim();
    let command_text = invocation.strip_prefix('/')?.trim();
    let name_end = command_text
        .find(char::is_whitespace)
        .unwrap_or(command_text.len());
    let name = &command_text[..name_end];
    if name.is_empty() {
        return None;
    }
    let command = commands.iter().find(|command| command.name == name)?;
    if let Some(processor) = command.unsupported_processor {
        return Some(Err(format!(
            "Custom command `/{}` uses unsupported {processor}; it was not run.",
            command.name
        )));
    }

    let args = command_text[name_end..].trim();
    let mut expanded = String::with_capacity(command.prompt.len().min(4096));
    if command.prompt.contains("{{args}}") {
        let mut remaining = command.prompt.as_str();
        while let Some(index) = remaining.find("{{args}}") {
            for value in [&remaining[..index], args] {
                if append_bounded(&mut expanded, value).is_none() {
                    return Some(Err(
                        "Expanded custom command prompt exceeds the 2 MiB limit.".to_owned(),
                    ));
                }
            }
            remaining = &remaining[index + "{{args}}".len()..];
        }
        if append_bounded(&mut expanded, remaining).is_none() {
            return Some(Err(
                "Expanded custom command prompt exceeds the 2 MiB limit.".to_owned(),
            ));
        }
    } else {
        if append_bounded(&mut expanded, &command.prompt).is_none() {
            return Some(Err(
                "Expanded custom command prompt exceeds the 2 MiB limit.".to_owned(),
            ));
        }
        if !args.is_empty() {
            for value in ["\n\n", invocation] {
                if append_bounded(&mut expanded, value).is_none() {
                    return Some(Err(
                        "Expanded custom command prompt exceeds the 2 MiB limit.".to_owned(),
                    ));
                }
            }
        }
    }
    Some(Ok(expanded))
}

/// Return whether a file command replaced the native command that owns the
/// typed alias. CommandService replaces the complete command definition, so
/// its built-in aliases disappear unless a file command defines that name.
pub(super) fn shadows_native_alias(commands: &[PromptCommand], input: &str) -> bool {
    let Some(name) = input
        .trim_start()
        .strip_prefix('/')
        .and_then(|rest| rest.split_whitespace().next())
    else {
        return false;
    };
    let Some(native) = COMMANDS
        .iter()
        .find(|command| command.aliases.contains(&name))
    else {
        return false;
    };
    commands.iter().any(|command| command.name == native.name)
}

fn file_command_shadows_native_name(file_commands: &[String], name: &str) -> bool {
    if file_commands.iter().any(|file_name| file_name == name) {
        return true;
    }
    COMMANDS
        .iter()
        .find(|command| command.aliases.contains(&name))
        .is_some_and(|native| {
            file_commands
                .iter()
                .any(|file_name| file_name == native.name)
        })
}

fn append_bounded(output: &mut String, value: &str) -> Option<()> {
    if output.len().saturating_add(value.len()) > MAX_EXPANDED_PROMPT_COMMAND_BYTES {
        return None;
    }
    output.push_str(value);
    Some(())
}

fn extension_command_roots(extension_root: &Path) -> Vec<PathBuf> {
    let manifest_path = extension_root.join("canopy-extension.json");
    let manifest_path = fs::canonicalize(&manifest_path)
        .ok()
        .filter(|path| path.starts_with(extension_root));
    let manifest = manifest_path
        .as_deref()
        .and_then(|path| read_command_file(path, 1024 * 1024))
        .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok());
    let configured = manifest
        .as_ref()
        .and_then(|value| value.get("commands"))
        .filter(|value| !value.is_null() && value.as_str().is_none_or(|path| !path.is_empty()));

    if let Some(configured) = configured {
        let paths = match configured {
            serde_json::Value::String(path) => vec![path.as_str()],
            serde_json::Value::Array(paths) => {
                let parsed = paths
                    .iter()
                    .map(serde_json::Value::as_str)
                    .collect::<Option<Vec<_>>>();
                let Some(parsed) = parsed else {
                    return vec![extension_root.join("commands")];
                };
                return parsed
                    .into_iter()
                    .take(MAX_EXTENSION_COMMAND_ROOTS)
                    .filter(|path| path.len() <= MAX_EXTENSION_COMMAND_PATH_BYTES)
                    .map(|path| extension_command_path(extension_root, path))
                    .collect();
            }
            _ => return vec![extension_root.join("commands")],
        };
        return paths
            .into_iter()
            .filter(|path| path.len() <= MAX_EXTENSION_COMMAND_PATH_BYTES)
            .map(|path| extension_command_path(extension_root, path))
            .collect();
    }

    vec![extension_root.join("commands")]
}

fn read_command_file(path: &Path, max_bytes: u64) -> Option<Vec<u8>> {
    let mut file = fs::File::open(path).ok()?;
    let metadata = file.metadata().ok()?;
    if !metadata.is_file() || metadata.len() > max_bytes {
        return None;
    }
    let mut contents = Vec::with_capacity(metadata.len() as usize);
    file.by_ref()
        .take(max_bytes.saturating_add(1))
        .read_to_end(&mut contents)
        .ok()?;
    (contents.len() as u64 <= max_bytes).then_some(contents)
}

fn extension_command_path(extension_root: &Path, command_path: &str) -> PathBuf {
    let path = Path::new(command_path);
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        extension_root.join(path)
    }
}

fn add_file_command_suggestions(completion: &mut Completion, file_commands: &[String]) {
    let mut ranked = Vec::new();
    for (index, suggestion) in completion.suggestions.drain(..).enumerate() {
        if file_command_shadows_native_name(file_commands, &suggestion.name)
            || suggestion
                .matched_alias
                .as_ref()
                .is_some_and(|alias| file_command_shadows_native_name(file_commands, alias))
        {
            continue;
        }
        let strength = suggestion
            .matched_alias
            .as_deref()
            .and_then(|alias| match_strength(&completion.query, alias))
            .or_else(|| match_strength(&completion.query, suggestion.name.trim_start_matches('/')));
        if let Some(strength) = strength {
            ranked.push((strength, index, suggestion));
        }
    }
    let existing = ranked.len();
    for (index, name) in file_commands.iter().enumerate() {
        let Some(strength) = match_strength(&completion.query, name) else {
            continue;
        };
        if completion.query.eq_ignore_ascii_case(name) {
            completion.perfect_match = true;
        }
        ranked.push((
            strength,
            existing + index,
            Suggestion {
                name: name.clone(),
                description: "Run this custom command".to_owned(),
                matched_alias: None,
            },
        ));
    }
    ranked.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1)));
    completion.suggestions = ranked
        .into_iter()
        .map(|(_, _, suggestion)| suggestion)
        .collect();
}

fn doctor_argument_candidates(previous_tokens: &[String]) -> Option<&'static [Command]> {
    match previous_tokens {
        [] => Some(&DOCTOR_SUBCOMMANDS),
        [subcommand] if subcommand == "memory" => Some(&DOCTOR_MEMORY_FLAGS),
        _ => None,
    }
}

fn is_valid_skill_stack_prefix(prefix: &str, skill_commands: &[String]) -> bool {
    let tokens = prefix.split_whitespace().collect::<Vec<_>>();
    if tokens.is_empty() || tokens.len() >= MAX_STACKED_SKILLS {
        return false;
    }
    tokens.iter().enumerate().all(|(index, token)| {
        let name = if index == 0 {
            *token
        } else if let Some(name) = token.strip_prefix('/') {
            name
        } else {
            return false;
        };
        skill_commands
            .iter()
            .any(|skill| skill.eq_ignore_ascii_case(name))
    })
}

fn add_skill_suggestions(completion: &mut Completion, skill_commands: &[String]) {
    let mut ranked = Vec::new();
    for (index, suggestion) in completion.suggestions.drain(..).enumerate() {
        let strength = suggestion
            .matched_alias
            .as_deref()
            .and_then(|alias| match_strength(&completion.query, alias))
            .or_else(|| match_strength(&completion.query, suggestion.name.trim_start_matches('/')));
        if let Some(strength) = strength {
            ranked.push((strength, index, suggestion));
        }
    }
    let static_count = ranked.len();
    for (index, name) in skill_commands.iter().enumerate() {
        if COMMANDS.iter().any(|command| {
            command.name.eq_ignore_ascii_case(name)
                || command
                    .aliases
                    .iter()
                    .any(|alias| alias.eq_ignore_ascii_case(name))
        }) {
            continue;
        }
        let Some(strength) = match_strength(&completion.query, name) else {
            continue;
        };
        if completion.query.eq_ignore_ascii_case(name) {
            completion.perfect_match = true;
        }
        ranked.push((
            strength,
            static_count + index,
            Suggestion {
                name: if completion.is_root_command {
                    name.clone()
                } else {
                    format!("/{name}")
                },
                description: "Run this skill command".to_owned(),
                matched_alias: None,
            },
        ));
    }
    ranked.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1)));
    completion.suggestions = ranked
        .into_iter()
        .map(|(_, _, suggestion)| suggestion)
        .collect();
}

fn stats_argument_candidates(previous_tokens: &[String]) -> Option<&'static [Command]> {
    if previous_tokens
        .last()
        .is_some_and(|token| matches!(token.as_str(), "--format" | "-f"))
    {
        return Some(&EXPORT_FORMATS);
    }
    if previous_tokens
        .last()
        .is_some_and(|token| matches!(token.as_str(), "--output" | "-o"))
    {
        return None;
    }

    match previous_tokens.first().map(String::as_str) {
        None => Some(&STATS_SUBCOMMANDS),
        Some("export") if previous_tokens.len() == 1 => Some(&EXPORT_PERIODS),
        Some("export") => {
            let has_period = previous_tokens.get(1).is_some_and(|token| {
                matches!(token.as_str(), "daily" | "day" | "monthly" | "month")
            });
            if !has_period
                || previous_tokens
                    .iter()
                    .any(|token| token == "--output" || token == "-o")
            {
                return None;
            }
            Some(
                if previous_tokens
                    .iter()
                    .any(|token| token == "--format" || token == "-f")
                {
                    &EXPORT_FLAGS[1..]
                } else {
                    &EXPORT_FLAGS
                },
            )
        }
        _ => None,
    }
}

fn completion_for_candidates(
    start: usize,
    end: usize,
    query: String,
    candidates: &'static [Command],
    is_root_command: bool,
) -> Completion {
    let mut ranked = Vec::new();
    for (candidate_index, candidate) in candidates.iter().enumerate() {
        let mut best_match: Option<(u8, Option<&'static str>)> = None;
        for value in std::iter::once(candidate.name).chain(candidate.aliases.iter().copied()) {
            let Some(strength) = match_strength(&query, value) else {
                continue;
            };
            let matched_alias = (value != candidate.name).then_some(value);
            let rank = (strength, matched_alias.is_none());
            if best_match.is_none_or(|(current_strength, current_alias)| {
                rank > (current_strength, current_alias.is_none())
            }) {
                best_match = Some((strength, matched_alias));
            }
        }
        if let Some((strength, matched_alias)) = best_match {
            ranked.push((
                strength,
                matched_alias.is_some(),
                candidate_index,
                Suggestion {
                    name: candidate.name.to_owned(),
                    description: candidate.description.to_owned(),
                    matched_alias: matched_alias.map(str::to_owned),
                },
            ));
        }
    }

    ranked.sort_by(|left, right| {
        right
            .0
            .cmp(&left.0)
            .then_with(|| left.1.cmp(&right.1))
            .then_with(|| left.2.cmp(&right.2))
    });
    let perfect_match = candidates.iter().any(|candidate| {
        !candidate.name.starts_with('-')
            && (query.eq_ignore_ascii_case(candidate.name)
                || candidate
                    .aliases
                    .iter()
                    .any(|alias| query.eq_ignore_ascii_case(alias)))
    });

    Completion {
        start,
        end,
        query,
        suggestions: ranked
            .into_iter()
            .map(|(_, _, _, suggestion)| suggestion)
            .collect(),
        is_root_command,
        perfect_match,
    }
}

pub(super) fn max_visible_suggestions() -> usize {
    MAX_VISIBLE_SUGGESTIONS
}

fn match_strength(query: &str, candidate: &str) -> Option<u8> {
    if query.is_empty() {
        return Some(0);
    }
    if query.eq_ignore_ascii_case(candidate) {
        return Some(3);
    }
    if candidate
        .get(..query.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(query))
    {
        return Some(2);
    }

    let mut candidate_chars = candidate.chars();
    query
        .chars()
        .all(|query_char| {
            candidate_chars.any(|candidate_char| candidate_char.eq_ignore_ascii_case(&query_char))
        })
        .then_some(1)
}
