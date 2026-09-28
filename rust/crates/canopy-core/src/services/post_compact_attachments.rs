//! Post-compaction attachment builders, ported from
//! `packages/core/src/services/postCompactAttachments.ts`.
//!
//! History and summary text are passed in by the session/compression caller.
//! File access is injected through [`PostCompactFileSystem`], keeping this
//! module independent of Canopy's TypeScript `Config` and provider clients.
//! The default Tokio adapter is provided for callers that want local files.

use std::collections::HashSet;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;

use serde_json::{Value, json};

use crate::services::compaction_input_slimming::get_function_response_parts;
use crate::utils::cancellation::CancellationToken;
use crate::utils::xml::escape_xml;

pub const POST_COMPACT_MAX_FILES_TO_RESTORE: i64 = 5;
pub const POST_COMPACT_MAX_TOKENS_PER_FILE: usize = 5_000;
pub const POST_COMPACT_TOKEN_BUDGET: usize = 50_000;
pub const POST_COMPACT_MAX_IMAGES_TO_RESTORE: i64 = 3;
pub const CHARS_PER_TOKEN: usize = 4;

const BINARY_DETECT_SAMPLE: usize = 512;
const BINARY_NONPRINTABLE_THRESHOLD: f64 = 0.3;
const MAX_SUBAGENT_DESC_CHARS: usize = 200;
const MAX_SUBAGENT_SNAPSHOT_COUNT: usize = 30;

const FILE_TOUCHING_TOOLS: &[&str] = &["read_file", "write_file", "edit", "replace"];
const RESUME_TRAILER: &str = "Resume the prior task using the summary above. Continue from the last in-flight step; do not acknowledge the summary, do not re-introduce, do not greet the user again.";
const PLAN_MODE_REMINDER_TEXT: &str = "<plan-mode-active>\nYou are currently in PLAN mode. You may research, read files, and propose plans, but you may not execute modification tools (write_file, edit, run_shell_command, etc.) until the user exits plan mode. The summary above may not reflect this constraint — honor plan mode regardless.\n</plan-mode-active>";
const FILE_REFERENCE_INTRO: &str = "The following files were recently accessed before context was compacted. They are listed as reference only because they are large. Use `read_file` to view current content for any file you need:";
const IMAGE_RESTORATION_INTRO: &str = "Recent visual snapshots preserved from before context was compacted (most recent last). Each image corresponds to a tool result or user-pasted image earlier in the conversation:";
const BACKGROUND_TASKS_INTRO: &str = "The following background subagent tasks were active at compaction. The summary above does not include their per-task state. Use `task_stop` / `send_message` to interact; do not assume they completed.";

pub type AttachmentFsFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Minimal async file-system surface needed for restoration. Adapters should
/// return an error for any failed stat/read; the compaction flow deliberately
/// treats those errors as a missing attachment and continues.
pub trait PostCompactFileSystem: Send + Sync {
    fn metadata_size<'a>(&'a self, path: &'a str) -> AttachmentFsFuture<'a, Result<u64, String>>;

    /// The adapter should stop waiting for a read when the token is cancelled.
    fn read_file<'a>(
        &'a self,
        path: &'a str,
        cancellation: &'a CancellationToken,
    ) -> AttachmentFsFuture<'a, Result<Vec<u8>, String>>;

    /// Resolve symlinks when possible. `Err` triggers a lexical absolute-path
    /// fallback, matching the source helper's handling of nonexistent files.
    fn canonicalize(&self, path: &str) -> Result<String, String>;
}

/// Local asynchronous file-system adapter. All read/stat failures are still
/// collapsed to `missing` by the public restoration helpers.
#[derive(Clone, Copy, Debug, Default)]
pub struct TokioPostCompactFileSystem;

impl PostCompactFileSystem for TokioPostCompactFileSystem {
    fn metadata_size<'a>(&'a self, path: &'a str) -> AttachmentFsFuture<'a, Result<u64, String>> {
        let path = PathBuf::from(path);
        Box::pin(async move {
            tokio::fs::metadata(path)
                .await
                .map(|metadata| metadata.len())
                .map_err(|error| error.to_string())
        })
    }

    fn read_file<'a>(
        &'a self,
        path: &'a str,
        cancellation: &'a CancellationToken,
    ) -> AttachmentFsFuture<'a, Result<Vec<u8>, String>> {
        let path = PathBuf::from(path);
        Box::pin(async move {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => Err("file read cancelled".to_owned()),
                result = tokio::fs::read(path) => result.map_err(|error| error.to_string()),
            }
        })
    }

    fn canonicalize(&self, path: &str) -> Result<String, String> {
        std::fs::canonicalize(path)
            .map(|path| path.to_string_lossy().into_owned())
            .map_err(|error| error.to_string())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FileEmbedResult {
    Embed { content: String },
    Reference,
    Missing,
    Binary,
}

/// One recent inline image and its best-effort source-tool attribution.
#[derive(Clone, Debug, PartialEq)]
pub struct ExtractedImage {
    /// The original inlineData part, ready to reinsert unchanged.
    pub part: Value,
    /// Zero-based position in the original history.
    pub turn_index: usize,
    pub source_tool_name: Option<String>,
    pub source_tool_args: Option<Value>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubagentSnapshot {
    pub id: String,
    pub description: String,
    pub status: SubagentStatus,
    pub start_time: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SubagentStatus {
    Running,
    Paused,
}

impl SubagentStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Paused => "paused",
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ComposePostCompactOptions {
    pub workspace_root: Option<String>,
    pub max_files: Option<i64>,
    pub max_images: Option<i64>,
    pub plan_mode_active: bool,
    pub running_subagents: Vec<SubagentSnapshot>,
}

/// Walk newest-first and collect the latest distinct file paths touched by
/// successful read/write/edit calls. Denied or failed calls are excluded so
/// compaction cannot bypass a denied file-read permission by reading directly.
pub fn extract_recent_file_paths(history: &[Value], max_files: i64) -> Vec<String> {
    if max_files <= 0 {
        return Vec::new();
    }

    let mut failed_call_ids = HashSet::<String>::new();
    for content in history {
        for part in parts(content) {
            let Some(response) = part.get("functionResponse") else {
                continue;
            };
            let Some(id) = response.get("id").and_then(Value::as_str) else {
                continue;
            };
            if id.is_empty() {
                continue;
            }
            if response
                .get("response")
                .and_then(Value::as_object)
                .is_some_and(|value| value.contains_key("error"))
            {
                failed_call_ids.insert(id.to_owned());
            }
        }
    }

    let mut seen = HashSet::<String>::new();
    let mut result = Vec::new();
    for content in history.iter().rev() {
        if content.get("role").and_then(Value::as_str) != Some("model") {
            continue;
        }
        for part in parts(content).iter().rev() {
            let Some(call) = part.get("functionCall") else {
                continue;
            };
            let name = call.get("name").and_then(Value::as_str).unwrap_or_default();
            if !FILE_TOUCHING_TOOLS.contains(&name) {
                continue;
            }
            if call
                .get("id")
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty())
                .is_some_and(|id| failed_call_ids.contains(id))
            {
                continue;
            }
            let Some(path) = call
                .get("args")
                .and_then(|args| args.get("file_path"))
                .and_then(Value::as_str)
                .filter(|path| !path.is_empty())
            else {
                continue;
            };
            if seen.insert(path.to_owned()) {
                result.push(path.to_owned());
                if result.len() >= max_files as usize {
                    return result;
                }
            }
        }
    }
    result
}

/// Extract the newest top-level or tool-returned images, returned in
/// chronological order so the latest visual state appears last.
pub fn extract_recent_images(history: &[Value], max_images: i64) -> Vec<ExtractedImage> {
    if max_images <= 0 {
        return Vec::new();
    }
    let mut collected = Vec::new();
    'history: for index in (0..history.len()).rev() {
        let image_parts = image_parts_in_content_reverse(&history[index]);
        if image_parts.is_empty() {
            continue;
        }
        let mut source_tool_name = None;
        let mut source_tool_args = None;
        if index > 0 && history[index - 1].get("role").and_then(Value::as_str) == Some("model") {
            if let Some(call) = parts(&history[index - 1])
                .iter()
                .find_map(|part| part.get("functionCall").filter(|call| js_truthy(call)))
            {
                source_tool_name = call.get("name").and_then(Value::as_str).map(str::to_owned);
                source_tool_args = call.get("args").cloned();
                if source_tool_args.as_ref().is_some_and(Value::is_null) {
                    source_tool_args = None;
                }
            }
        }
        for part in image_parts {
            collected.push(ExtractedImage {
                part,
                turn_index: index,
                source_tool_name: source_tool_name.clone(),
                source_tool_args: source_tool_args.clone(),
            });
            if collected.len() >= max_images as usize {
                break 'history;
            }
        }
    }
    collected.reverse();
    collected
}

/// Count only images nested under `functionResponse.parts`. User-pasted
/// top-level images intentionally do not contribute to the screenshot trigger.
pub fn count_tool_response_images(history: &[Value]) -> usize {
    history
        .iter()
        .flat_map(parts)
        .filter_map(get_function_response_parts)
        .flatten()
        .filter(|part| is_image_part(part))
        .count()
}

/// Read one file using a byte precheck and then a character-length cap. Any
/// stat/read error, including permission errors and cancellation, is treated
/// as `Missing`, matching the source's best-effort compaction behavior.
pub async fn read_file_size_adaptive(
    file_system: &impl PostCompactFileSystem,
    file_path: &str,
    max_tokens: usize,
    cancellation: &CancellationToken,
) -> FileEmbedResult {
    if cancellation.is_cancelled() {
        return FileEmbedResult::Missing;
    }
    let max_chars = max_tokens.saturating_mul(CHARS_PER_TOKEN);
    let size = match file_system.metadata_size(file_path).await {
        Ok(size) => size,
        Err(_) => return FileEmbedResult::Missing,
    };
    if size > max_chars.saturating_mul(4) as u64 {
        return FileEmbedResult::Reference;
    }

    let buffer = match file_system.read_file(file_path, cancellation).await {
        Ok(buffer) => buffer,
        Err(_) => return FileEmbedResult::Missing,
    };
    let sample = &buffer[..buffer.len().min(BINARY_DETECT_SAMPLE)];
    if !sample.is_empty() {
        let non_printable = sample
            .iter()
            .filter(|&&byte| {
                !(matches!(byte, 0x20..=0x7e | 0x80..=0xff) || matches!(byte, 0x09 | 0x0a | 0x0d))
            })
            .count();
        if non_printable as f64 / sample.len() as f64 > BINARY_NONPRINTABLE_THRESHOLD {
            return FileEmbedResult::Binary;
        }
    }

    let decoded = String::from_utf8_lossy(&buffer).into_owned();
    if js_string_len(&decoded) > max_chars {
        FileEmbedResult::Reference
    } else {
        FileEmbedResult::Embed { content: decoded }
    }
}

/// Build one large-file reference block followed by one block per embedded
/// file. The aggregate embedded character budget is enforced in input order;
/// files that exceed it are downgraded to references.
pub async fn build_file_restoration_blocks(
    file_system: &impl PostCompactFileSystem,
    file_paths: &[String],
    cancellation: &CancellationToken,
) -> Vec<Value> {
    let mut references = Vec::<String>::new();
    let mut embeds = Vec::<(String, String)>::new();
    let mut used_chars = 0usize;
    let budget_chars = POST_COMPACT_TOKEN_BUDGET.saturating_mul(CHARS_PER_TOKEN);

    for file_path in file_paths {
        if cancellation.is_cancelled() {
            break;
        }
        match read_file_size_adaptive(
            file_system,
            file_path,
            POST_COMPACT_MAX_TOKENS_PER_FILE,
            cancellation,
        )
        .await
        {
            FileEmbedResult::Missing | FileEmbedResult::Binary => {}
            FileEmbedResult::Reference => references.push(file_path.clone()),
            FileEmbedResult::Embed { content } => {
                let chars = js_string_len(&content);
                if used_chars.saturating_add(chars) > budget_chars {
                    references.push(file_path.clone());
                } else {
                    embeds.push((file_path.clone(), content));
                    used_chars += chars;
                }
            }
        }
    }

    let mut blocks = Vec::new();
    if !references.is_empty() {
        let mut lines = vec![FILE_REFERENCE_INTRO.to_owned(), String::new()];
        lines.extend(
            references
                .iter()
                .map(|path| format!("- {}", sanitize_path_for_display(path))),
        );
        blocks.push(user_text_content(lines.join("\n")));
    }

    for (path, content) in embeds {
        let run = longest_backtick_run(&content).saturating_add(1);
        let fence = if run >= 3 {
            "`".repeat(run)
        } else {
            "```".to_owned()
        };
        let text = format!(
            "Recently accessed file (full current content embedded):\n\n## {}\n\n{}\n{}\n{}",
            sanitize_path_for_display(&path),
            fence,
            content,
            fence
        );
        blocks.push(user_text_content(text));
    }
    blocks
}

/// Build one user content with a textual source header and the unchanged
/// inline image parts. Returns `None` for an empty image list.
pub fn build_image_restoration_block(images: &[ExtractedImage]) -> Option<Value> {
    if images.is_empty() {
        return None;
    }
    let mut lines = vec![IMAGE_RESTORATION_INTRO.to_owned(), String::new()];
    for image in images {
        if let Some(tool_name) = image
            .source_tool_name
            .as_deref()
            .filter(|tool_name| !tool_name.is_empty())
        {
            let args = image.source_tool_args.as_ref().map_or_else(
                || "{}".to_owned(),
                |args| serde_json::to_string(args).unwrap_or_else(|_| "{}".to_owned()),
            );
            lines.push(format!(
                "- turn {}: {} args={}",
                image.turn_index, tool_name, args
            ));
        } else {
            lines.push(format!("- turn {}: user-provided image", image.turn_index));
        }
    }
    let mut output_parts = vec![json!({ "text": lines.join("\n") })];
    output_parts.extend(images.iter().map(|image| image.part.clone()));
    Some(json!({ "role": "user", "parts": output_parts }))
}

/// Strip exact `<analysis>...</analysis>` blocks and an optional unclosed
/// trailing block. If stripping leaves no summary body, the source sentinel
/// is used. The fixed resume guidance is appended in all cases.
pub fn post_process_summary(raw_summary: &str) -> String {
    let stripped = strip_analysis_block(raw_summary);
    let body = if stripped.is_empty() {
        "[Summary unavailable]"
    } else {
        &stripped
    };
    format!("{body}\n\n{RESUME_TRAILER}")
}

pub fn strip_analysis_block(raw_summary: &str) -> String {
    let mut remaining = raw_summary;
    let mut output = String::with_capacity(raw_summary.len());
    while let Some(open) = remaining.find("<analysis>") {
        output.push_str(&remaining[..open]);
        let body_start = open + "<analysis>".len();
        let Some(close_offset) = remaining[body_start..].find("</analysis>") else {
            remaining = "";
            break;
        };
        let after_close = body_start + close_offset + "</analysis>".len();
        let rest = &remaining[after_close..];
        let whitespace = rest
            .char_indices()
            .take_while(|(_, character)| is_js_whitespace(*character))
            .map(|(index, character)| index + character.len_utf8())
            .last()
            .unwrap_or(0);
        remaining = &rest[whitespace..];
    }
    // The closed-tag pass has removed every closed block; any remaining open
    // tag is the source's unclosed-analysis fallback and truncates the tail.
    if !remaining.is_empty() {
        output.push_str(remaining);
    }
    trim_js(&output).to_owned()
}

/// Produce mid-session reminders in their required priority order: plan mode,
/// then a bounded background-subagent snapshot.
pub fn build_state_reminder_parts(options: &ComposePostCompactOptions) -> Vec<Value> {
    let mut parts = Vec::new();
    if options.plan_mode_active {
        parts.push(json!({ "text": PLAN_MODE_REMINDER_TEXT }));
    }
    if !options.running_subagents.is_empty() {
        parts.push(json!({ "text": build_subagent_snapshot(&options.running_subagents) }));
    }
    parts
}

/// Assemble summary, ack, merged attachments, and any pending trailing tool
/// call while preserving API role alternation. History/session retrieval is
/// deliberately the caller's responsibility.
pub async fn compose_post_compact_history(
    file_system: &impl PostCompactFileSystem,
    history: &[Value],
    summary: &str,
    options: &ComposePostCompactOptions,
    cancellation: &CancellationToken,
) -> Vec<Value> {
    let max_files = options
        .max_files
        .unwrap_or(POST_COMPACT_MAX_FILES_TO_RESTORE);
    let max_images = options
        .max_images
        .unwrap_or(POST_COMPACT_MAX_IMAGES_TO_RESTORE);
    let file_paths = extract_recent_file_paths(history, max_files)
        .into_iter()
        .filter(|path| {
            options
                .workspace_root
                .as_deref()
                .is_none_or(|root| is_inside_workspace(file_system, path, root))
        })
        .collect::<Vec<_>>();
    let file_blocks = build_file_restoration_blocks(file_system, &file_paths, cancellation).await;
    let images = extract_recent_images(history, max_images);
    let image_block = build_image_restoration_block(&images);

    let mut attachment_parts = build_state_reminder_parts(options);
    for block in &file_blocks {
        attachment_parts.extend(parts(block).iter().cloned());
    }
    if let Some(image_block) = image_block {
        attachment_parts.extend(parts(&image_block).iter().cloned());
    }

    let trailing_call = trailing_function_call_content(history);
    let mut output = vec![user_text_content(post_process_summary(summary))];
    if !attachment_parts.is_empty() {
        output.push(json!({
            "role": "model",
            "parts": [{ "text": "Got it. Thanks for the additional context!" }]
        }));
        output.push(json!({ "role": "user", "parts": attachment_parts }));
        if let Some(trailing_call) = trailing_call {
            output.push(trailing_call);
        }
    } else if let Some(trailing_call) = trailing_call {
        let mut ack_parts = vec![json!({ "text": "Got it. Thanks for the additional context!" })];
        ack_parts.extend(
            parts(&trailing_call)
                .iter()
                .filter(|part| part.get("functionCall").is_some_and(js_truthy))
                .cloned(),
        );
        output.push(json!({ "role": "model", "parts": ack_parts }));
    } else {
        output.push(json!({
            "role": "model",
            "parts": [{ "text": "Got it. Thanks for the additional context!" }]
        }));
    }
    output
}

fn image_parts_in_content_reverse(content: &Value) -> Vec<Value> {
    let mut result = Vec::new();
    for part in parts(content).iter().rev() {
        if is_image_part(part) {
            result.push(part.clone());
            continue;
        }
        if let Some(nested) = get_function_response_parts(part) {
            result.extend(
                nested
                    .iter()
                    .rev()
                    .filter(|inner| is_image_part(inner))
                    .cloned(),
            );
        }
    }
    result
}

fn is_image_part(part: &Value) -> bool {
    part.get("inlineData")
        .and_then(|data| data.get("mimeType"))
        .and_then(Value::as_str)
        .is_some_and(|mime| mime.starts_with("image/"))
}

fn parts(content: &Value) -> &[Value] {
    content
        .get("parts")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default()
}

fn user_text_content(text: String) -> Value {
    json!({ "role": "user", "parts": [{ "text": text }] })
}

fn sanitize_path_for_display(path: &str) -> String {
    path.chars()
        .filter(|character| !matches!(character, '\r' | '\n' | '\t'))
        .collect()
}

fn longest_backtick_run(text: &str) -> usize {
    let mut longest = 0;
    let mut current = 0;
    for character in text.chars() {
        if character == '`' {
            current += 1;
            longest = longest.max(current);
        } else {
            current = 0;
        }
    }
    longest
}

fn js_string_len(text: &str) -> usize {
    text.encode_utf16().count()
}

fn js_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_f64().is_some_and(|number| number != 0.0),
        Value::String(value) => !value.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}

fn trim_js(text: &str) -> &str {
    text.trim_matches(is_js_whitespace)
}

fn is_js_whitespace(character: char) -> bool {
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

fn trailing_function_call_content(history: &[Value]) -> Option<Value> {
    let last = history.last()?;
    if last.get("role").and_then(Value::as_str) != Some("model")
        || !parts(last)
            .iter()
            .any(|part| part.get("functionCall").is_some_and(js_truthy))
    {
        return None;
    }
    Some(last.clone())
}

fn is_inside_workspace(
    file_system: &impl PostCompactFileSystem,
    file_path: &str,
    workspace_root: &str,
) -> bool {
    if workspace_root.is_empty() {
        return true;
    }
    let resolved_file = safe_realpath(file_system, file_path);
    let resolved_root = safe_realpath(file_system, workspace_root);
    let file_path = Path::new(&resolved_file);
    let root_path = Path::new(&resolved_root);
    file_path == root_path || file_path.starts_with(root_path)
}

fn safe_realpath(file_system: &impl PostCompactFileSystem, path: &str) -> PathBuf {
    match file_system.canonicalize(path) {
        Ok(canonical) => PathBuf::from(canonical),
        Err(_) => lexical_absolute_path(path),
    }
}

fn lexical_absolute_path(path: &str) -> PathBuf {
    let path = Path::new(path);
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(path)
    };
    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                let _ = normalized.pop();
            }
            other => normalized.push(other.as_os_str()),
        }
    }
    normalized
}

fn build_subagent_snapshot(snapshots: &[SubagentSnapshot]) -> String {
    let mut sorted = snapshots.to_vec();
    sorted.sort_by_key(|snapshot| snapshot.start_time);
    let overflow = sorted.len().saturating_sub(MAX_SUBAGENT_SNAPSHOT_COUNT);
    let shown = &sorted[overflow..];
    let mut lines = shown
        .iter()
        .map(|snapshot| {
            let flattened = flatten_whitespace_for_bullet(&snapshot.description);
            let description = truncate_utf16(&flattened, MAX_SUBAGENT_DESC_CHARS);
            format!(
                "- [{}] {}: {}",
                escape_xml(snapshot.status.as_str()),
                escape_xml(&snapshot.id),
                escape_xml(&description)
            )
        })
        .collect::<Vec<_>>();
    if overflow > 0 {
        lines.push(format!(
            "- (… and {overflow} older task{} not shown)",
            if overflow == 1 { "" } else { "s" }
        ));
    }
    format!(
        "<background-tasks>\n{BACKGROUND_TASKS_INTRO}\n{}\n</background-tasks>",
        lines.join("\n")
    )
}

fn flatten_whitespace_for_bullet(text: &str) -> String {
    let mut output = String::with_capacity(text.len());
    let mut in_collapsible_run = false;
    for character in text.chars() {
        if matches!(character, '\r' | '\n' | '\t') {
            if !in_collapsible_run {
                output.push(' ');
            }
            in_collapsible_run = true;
        } else {
            in_collapsible_run = false;
            output.push(character);
        }
    }
    output
}

fn truncate_utf16(text: &str, max_code_units: usize) -> String {
    let mut output = String::new();
    let mut used = 0usize;
    let mut truncated = false;
    for character in text.chars() {
        let units = character.len_utf16();
        if used + units > max_code_units {
            truncated = true;
            break;
        }
        output.push(character);
        used += units;
    }
    if truncated {
        output.push('…');
    }
    output
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use serde_json::{Value, json};

    use crate::utils::cancellation::CancellationToken;

    use super::{
        AttachmentFsFuture, ComposePostCompactOptions, FileEmbedResult, PostCompactFileSystem,
        SubagentSnapshot, SubagentStatus, build_file_restoration_blocks,
        build_image_restoration_block, build_state_reminder_parts, compose_post_compact_history,
        count_tool_response_images, extract_recent_file_paths, extract_recent_images,
        is_inside_workspace, post_process_summary, read_file_size_adaptive,
    };

    #[derive(Default)]
    struct MemoryFileSystem {
        files: Mutex<HashMap<String, Vec<u8>>>,
        canonical_paths: Mutex<HashMap<String, String>>,
        reads: AtomicUsize,
        stat_errors: Mutex<HashMap<String, String>>,
    }

    impl MemoryFileSystem {
        fn add(&self, path: &str, bytes: impl Into<Vec<u8>>) {
            self.files
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .insert(path.to_owned(), bytes.into());
        }

        fn read_count(&self) -> usize {
            self.reads.load(Ordering::SeqCst)
        }
    }

    impl PostCompactFileSystem for MemoryFileSystem {
        fn metadata_size<'a>(
            &'a self,
            path: &'a str,
        ) -> AttachmentFsFuture<'a, Result<u64, String>> {
            Box::pin(async move {
                if let Some(error) = self
                    .stat_errors
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .get(path)
                    .cloned()
                {
                    return Err(error);
                }
                self.files
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .get(path)
                    .map(|bytes| bytes.len() as u64)
                    .ok_or_else(|| "missing".to_owned())
            })
        }

        fn read_file<'a>(
            &'a self,
            path: &'a str,
            cancellation: &'a CancellationToken,
        ) -> AttachmentFsFuture<'a, Result<Vec<u8>, String>> {
            Box::pin(async move {
                if cancellation.is_cancelled() {
                    return Err("cancelled".to_owned());
                }
                self.reads.fetch_add(1, Ordering::SeqCst);
                self.files
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .get(path)
                    .cloned()
                    .ok_or_else(|| "missing".to_owned())
            })
        }

        fn canonicalize(&self, path: &str) -> Result<String, String> {
            self.canonical_paths
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .get(path)
                .cloned()
                .ok_or_else(|| "not mapped".to_owned())
        }
    }

    fn call(name: &str, path: &str) -> Value {
        json!({
            "role":"model",
            "parts":[{"functionCall":{"name":name,"args":{"file_path":path}}}]
        })
    }

    fn tool_image(data: &str) -> Value {
        json!({
            "role":"user",
            "parts":[{"functionResponse":{
                "name":"computer_use__get_app_state",
                "response":{"output":"screenshot"},
                "parts":[{"inlineData":{"mimeType":"image/png","data":data}}]
            }}]
        })
    }

    fn cancellation() -> CancellationToken {
        CancellationToken::new()
    }

    #[test]
    fn recent_file_paths_are_newest_first_deduped_and_part_ordered() {
        let history = vec![
            call("read_file", "/a"),
            call("read_file", "/b"),
            json!({"role":"model","parts":[
                {"functionCall":{"name":"read_file","args":{"file_path":"/c"}}},
                {"functionCall":{"name":"write_file","args":{"file_path":"/a"}}}
            ]}),
        ];
        assert_eq!(extract_recent_file_paths(&history, 5), ["/a", "/c", "/b"]);
        assert_eq!(extract_recent_file_paths(&history, 0), Vec::<String>::new());
        assert_eq!(extract_recent_file_paths(&history, 1), ["/a"]);
    }

    #[test]
    fn denied_or_errored_file_calls_are_never_restored() {
        let history = vec![
            json!({"role":"model","parts":[
                {"functionCall":{"id":"ok","name":"read_file","args":{"file_path":"/ok"}}},
                {"functionCall":{"id":"denied","name":"read_file","args":{"file_path":"/secret"}}}
            ]}),
            json!({"role":"user","parts":[
                {"functionResponse":{"id":"ok","response":{"output":"safe"}}},
                {"functionResponse":{"id":"denied","response":{"error":"Permission denied"}}}
            ]}),
        ];
        assert_eq!(extract_recent_file_paths(&history, 5), ["/ok"]);
    }

    #[test]
    fn workspace_boundary_uses_canonical_components_not_string_prefixes() {
        let fs = MemoryFileSystem::default();
        fs.canonical_paths
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .extend([
                ("/workspace".to_owned(), "/real/workspace".to_owned()),
                (
                    "/workspace-sibling/file.rs".to_owned(),
                    "/real/workspace-sibling/file.rs".to_owned(),
                ),
            ]);
        assert!(!is_inside_workspace(
            &fs,
            "/workspace-sibling/file.rs",
            "/workspace"
        ));
        assert!(is_inside_workspace(&fs, "/anything", ""));
    }

    #[test]
    fn empty_call_ids_do_not_trigger_failed_call_filtering() {
        let history = vec![
            json!({"role":"model","parts":[{"functionCall":{"id":"","name":"read_file","args":{"file_path":"/empty-id"}}}]}),
            json!({"role":"user","parts":[{"functionResponse":{"id":"","response":{"error":"failure"}}}]}),
        ];
        assert_eq!(extract_recent_file_paths(&history, 5), ["/empty-id"]);
    }

    #[test]
    fn recent_images_include_nested_tool_media_and_user_pastes_in_time_order() {
        let history = vec![
            json!({"role":"model","parts":[{"functionCall":{"name":"computer_use__get_app_state","args":{"app":"Safari"}}}]}),
            tool_image("one"),
            json!({"role":"user","parts":[{"inlineData":{"mimeType":"image/jpeg","data":"paste"}}]}),
            json!({"role":"model","parts":[{"functionCall":{"name":"computer_use__get_app_state","args":{"app":"Safari"}}}]}),
            tool_image("two"),
        ];
        let images = extract_recent_images(&history, 2);
        assert_eq!(images.len(), 2);
        assert_eq!(images[0].part["inlineData"]["data"], "paste");
        assert_eq!(images[0].turn_index, 2);
        assert_eq!(images[1].part["inlineData"]["data"], "two");
        assert_eq!(
            images[1].source_tool_name.as_deref(),
            Some("computer_use__get_app_state")
        );
        assert_eq!(
            images[1].source_tool_args.as_ref().unwrap()["app"],
            "Safari"
        );
        assert_eq!(count_tool_response_images(&history), 2);
        assert_eq!(count_tool_response_images(&images_to_history(&images)), 0);
    }

    #[test]
    fn images_in_one_tool_result_keep_nested_part_order_when_capped() {
        let history = vec![json!({
            "role":"user",
            "parts":[{"functionResponse":{
                "response":{"output":"two images"},
                "parts":[
                    {"inlineData":{"mimeType":"image/png","data":"first"}},
                    {"inlineData":{"mimeType":"image/jpeg","data":"second"}},
                    {"inlineData":{"mimeType":"application/pdf","data":"not-image"}}
                ]
            }}]
        })];
        let images = extract_recent_images(&history, 1);
        assert_eq!(images.len(), 1);
        assert_eq!(images[0].part["inlineData"]["data"], "second");
        assert_eq!(count_tool_response_images(&history), 2);
    }

    fn images_to_history(images: &[super::ExtractedImage]) -> Vec<Value> {
        vec![json!({
            "role":"user",
            "parts":images.iter().map(|image| image.part.clone()).collect::<Vec<_>>()
        })]
    }

    #[test]
    fn image_block_has_json_metadata_and_preserves_original_parts() {
        let images = extract_recent_images(
            &[
                json!({"role":"model","parts":[{"functionCall":{"name":"capture","args":{"app":"Safari"}}}]}),
                tool_image("png-data"),
            ],
            3,
        );
        let block = build_image_restoration_block(&images).unwrap();
        assert_eq!(block["role"], "user");
        assert_eq!(block["parts"].as_array().unwrap().len(), 2);
        assert!(
            block["parts"][0]["text"]
                .as_str()
                .unwrap()
                .contains("args={\"app\":\"Safari\"}")
        );
        assert_eq!(block["parts"][1], images[0].part);
        assert!(build_image_restoration_block(&[]).is_none());
    }

    #[tokio::test]
    async fn adaptive_file_read_covers_embed_reference_binary_missing_and_utf8() {
        let fs = MemoryFileSystem::default();
        let token = cancellation();
        fs.add("small", b"hello".to_vec());
        fs.add("cjk", "中".repeat(10_000).into_bytes());
        fs.add(
            "binary",
            (0_u8..100).map(|byte| byte % 32).collect::<Vec<_>>(),
        );
        fs.add("large", vec![b'x'; 30_000]);

        assert_eq!(
            read_file_size_adaptive(&fs, "small", 5_000, &token).await,
            FileEmbedResult::Embed {
                content: "hello".to_owned()
            }
        );
        assert_eq!(
            read_file_size_adaptive(&fs, "cjk", 5_000, &token).await,
            FileEmbedResult::Embed {
                content: "中".repeat(10_000)
            }
        );
        assert_eq!(
            read_file_size_adaptive(&fs, "binary", 5_000, &token).await,
            FileEmbedResult::Binary
        );
        assert_eq!(
            read_file_size_adaptive(&fs, "large", 5_000, &token).await,
            FileEmbedResult::Reference
        );
        assert_eq!(
            read_file_size_adaptive(&fs, "absent", 5_000, &token).await,
            FileEmbedResult::Missing
        );
    }

    #[tokio::test]
    async fn byte_size_precheck_skips_read_and_cancellation_prevents_io() {
        let fs = MemoryFileSystem::default();
        fs.add("huge", vec![0; 4_096]);
        let token = cancellation();
        assert_eq!(
            read_file_size_adaptive(&fs, "huge", 10, &token).await,
            FileEmbedResult::Reference
        );
        assert_eq!(fs.read_count(), 0);
        token.cancel();
        assert_eq!(
            read_file_size_adaptive(&fs, "huge", 10, &token).await,
            FileEmbedResult::Missing
        );
        assert_eq!(fs.read_count(), 0);
    }

    #[tokio::test]
    async fn file_restoration_batches_references_and_uses_safe_fences() {
        let fs = MemoryFileSystem::default();
        fs.add("large", vec![b'x'; 30_000]);
        fs.add("markdown", b"# Example\n```rust\nlet x = 1;\n```".to_vec());
        let blocks = build_file_restoration_blocks(
            &fs,
            &["large".to_owned(), "markdown".to_owned()],
            &cancellation(),
        )
        .await;
        assert_eq!(blocks.len(), 2);
        assert!(
            blocks[0]["parts"][0]["text"]
                .as_str()
                .unwrap()
                .contains("reference only")
        );
        let embed = blocks[1]["parts"][0]["text"].as_str().unwrap();
        assert!(embed.contains("````\n# Example"));
        assert!(embed.contains("\n````"));
    }

    #[tokio::test]
    async fn aggregate_file_budget_downgrades_overflow_to_reference() {
        let fs = MemoryFileSystem::default();
        let mut paths = Vec::new();
        for index in 0..11 {
            let path = format!("file-{index}");
            fs.add(&path, vec![b'a' + index as u8; 20_000]);
            paths.push(path);
        }
        let blocks = build_file_restoration_blocks(&fs, &paths, &cancellation()).await;
        assert_eq!(blocks.len(), 11);
        assert!(
            blocks[0]["parts"][0]["text"]
                .as_str()
                .unwrap()
                .contains("file-10")
        );
        assert!(blocks[1..11].iter().all(|block| {
            block["parts"][0]["text"]
                .as_str()
                .unwrap()
                .contains("Recently accessed file")
        }));
    }

    #[test]
    fn post_process_summary_strips_multiple_and_unclosed_analysis_blocks() {
        let processed = post_process_summary(
            "<analysis>hidden</analysis>\n<state_snapshot>kept</state_snapshot><analysis>tail",
        );
        assert!(!processed.contains("hidden"));
        assert!(!processed.contains("tail"));
        assert!(processed.contains("<state_snapshot>kept</state_snapshot>"));
        assert!(processed.contains("Resume the prior task"));
        let empty = post_process_summary("<analysis>only scratchpad</analysis>");
        assert!(empty.starts_with("[Summary unavailable]"));
    }

    #[test]
    fn state_reminders_escape_fields_flatten_and_cap_snapshot_rows() {
        let options = ComposePostCompactOptions {
            plan_mode_active: true,
            running_subagents: (0..31)
                .map(|index| SubagentSnapshot {
                    id: if index == 30 {
                        "agent<&".to_owned()
                    } else {
                        format!("agent-{index}")
                    },
                    description: if index == 30 {
                        "line one\nline two & <x>".to_owned()
                    } else {
                        format!("task-{index}")
                    },
                    status: if index == 30 {
                        SubagentStatus::Paused
                    } else {
                        SubagentStatus::Running
                    },
                    start_time: index,
                })
                .collect(),
            ..ComposePostCompactOptions::default()
        };
        let parts = build_state_reminder_parts(&options);
        assert_eq!(parts.len(), 2);
        assert!(
            parts[0]["text"]
                .as_str()
                .unwrap()
                .contains("<plan-mode-active>")
        );
        let snapshot = parts[1]["text"].as_str().unwrap();
        assert!(snapshot.contains("agent&lt;&amp;"));
        assert!(snapshot.contains("line one line two &amp; &lt;x&gt;"));
        assert!(snapshot.contains("and 1 older task not shown"));
        assert!(!snapshot.contains("agent-0:"));
    }

    #[test]
    fn subagent_descriptions_are_truncated_at_two_hundred_utf16_units() {
        let options = ComposePostCompactOptions {
            running_subagents: vec![SubagentSnapshot {
                id: "long".to_owned(),
                description: "x".repeat(201),
                status: SubagentStatus::Running,
                start_time: 1,
            }],
            ..ComposePostCompactOptions::default()
        };
        let parts = build_state_reminder_parts(&options);
        let snapshot = parts[0]["text"].as_str().unwrap();
        assert!(snapshot.contains(&format!("{}…", "x".repeat(200))));
        assert!(!snapshot.contains(&"x".repeat(201)));
    }

    #[tokio::test]
    async fn composition_merges_attachments_and_preserves_pending_tool_call() {
        let fs = MemoryFileSystem::default();
        fs.add("/workspace/file.rs", b"fn main() {}".to_vec());
        fs.add("/outside", b"OUTSIDE_SECRET".to_vec());
        fs.add("/workspace/link", b"SYMLINK_SECRET".to_vec());
        fs.canonical_paths
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .extend([
                ("/workspace".to_owned(), "/real/workspace".to_owned()),
                (
                    "/workspace/file.rs".to_owned(),
                    "/real/workspace/file.rs".to_owned(),
                ),
                ("/workspace/link".to_owned(), "/outside/secret".to_owned()),
                ("/outside".to_owned(), "/outside".to_owned()),
            ]);
        let history = vec![
            json!({"role":"model","parts":[
                {"functionCall":{"name":"read_file","args":{"file_path":"/outside"}}},
                {"functionCall":{"name":"read_file","args":{"file_path":"/workspace/file.rs"}}},
                {"functionCall":{"name":"read_file","args":{"file_path":"/workspace/link"}}}
            ]}),
            json!({"role":"user","parts":[{"inlineData":{"mimeType":"image/png","data":"image"}}]}),
            json!({"role":"model","parts":[{"functionCall":{"name":"edit","args":{"file_path":"/workspace/file.rs"}}}]}),
        ];
        let options = ComposePostCompactOptions {
            workspace_root: Some("/workspace".to_owned()),
            plan_mode_active: true,
            ..ComposePostCompactOptions::default()
        };
        let output = compose_post_compact_history(
            &fs,
            &history,
            "<analysis>x</analysis>SUM",
            &options,
            &cancellation(),
        )
        .await;
        assert_eq!(
            output
                .iter()
                .map(|content| content["role"].as_str().unwrap())
                .collect::<Vec<_>>(),
            ["user", "model", "user", "model"]
        );
        assert!(
            output[0]["parts"][0]["text"]
                .as_str()
                .unwrap()
                .contains("SUM")
        );
        assert!(output[2]["parts"].as_array().unwrap().iter().any(|part| {
            part["text"]
                .as_str()
                .is_some_and(|text| text.contains("<plan-mode-active>"))
        }));
        assert!(output[2]["parts"].as_array().unwrap().iter().any(|part| {
            part["text"]
                .as_str()
                .is_some_and(|text| text.contains("fn main() {}"))
        }));
        assert!(
            output[2]["parts"]
                .as_array()
                .unwrap()
                .iter()
                .any(|part| part["inlineData"]["data"] == "image")
        );
        let attachment_text = output[2]["parts"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|part| part["text"].as_str())
            .collect::<Vec<_>>();
        let plan_index = attachment_text
            .iter()
            .position(|text| text.contains("<plan-mode-active>"))
            .unwrap();
        let file_index = attachment_text
            .iter()
            .position(|text| text.contains("fn main() {}"))
            .unwrap();
        let image_index = attachment_text
            .iter()
            .position(|text| text.contains("Recent visual snapshots"))
            .unwrap();
        assert!(plan_index < file_index && file_index < image_index);
        assert!(!output[2].to_string().contains("OUTSIDE_SECRET"));
        assert!(!output[2].to_string().contains("SYMLINK_SECRET"));
        assert_eq!(output[3], history[2]);
    }

    #[tokio::test]
    async fn composition_folds_function_call_into_ack_when_no_attachments_exist() {
        let history = vec![
            json!({"role":"user","parts":[{"text":"request"}]}),
            json!({"role":"model","parts":[{"text":"discarded tail"},{"functionCall":{"name":"shell","args":{"command":"true"}}}]}),
        ];
        let output = compose_post_compact_history(
            &MemoryFileSystem::default(),
            &history,
            "summary",
            &ComposePostCompactOptions {
                max_files: Some(0),
                max_images: Some(0),
                ..ComposePostCompactOptions::default()
            },
            &cancellation(),
        )
        .await;
        assert_eq!(output.len(), 2);
        assert_eq!(output[1]["parts"].as_array().unwrap().len(), 2);
        assert!(output[1]["parts"][1].get("functionCall").is_some());
        assert!(!output[1].to_string().contains("discarded tail"));
    }

    #[tokio::test]
    async fn null_function_call_does_not_count_as_pending_trailing_call() {
        let history = vec![
            json!({"role":"user","parts":[{"text":"request"}]}),
            json!({"role":"model","parts":[{"functionCall":null}]}),
        ];
        let output = compose_post_compact_history(
            &MemoryFileSystem::default(),
            &history,
            "summary",
            &ComposePostCompactOptions {
                max_files: Some(0),
                max_images: Some(0),
                ..ComposePostCompactOptions::default()
            },
            &cancellation(),
        )
        .await;
        assert_eq!(output.len(), 2);
        assert!(!output[1].to_string().contains("functionCall"));
    }

    #[tokio::test]
    async fn stat_errors_are_best_effort_missing_attachments() {
        let fs = MemoryFileSystem::default();
        fs.stat_errors
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert("private".to_owned(), "permission denied".to_owned());
        assert_eq!(
            read_file_size_adaptive(&fs, "private", 10, &cancellation()).await,
            FileEmbedResult::Missing
        );
        assert_eq!(fs.read_count(), 0);
    }
}
