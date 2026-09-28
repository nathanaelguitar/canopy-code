//! Deterministic auto-memory relevance selection and prompt formatting.
//!
//! Ported from `packages/core/src/memory/recall.ts` and `memoryAge.ts`.
//! The query resolver combines project and user scans, applies exclusions,
//! invokes the injected model selector, falls back to deterministic ranking,
//! and reports optional telemetry. Model request construction and response
//! validation live in `relevance_selector.rs`.

use std::collections::HashSet;
use std::io;
use std::path::PathBuf;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use super::paths::AutoMemoryPaths;
use super::relevance_selector::{
    AutoMemoryRecallSelector, select_relevant_auto_memory_documents_by_model,
};
use super::scan::{
    ScannedAutoMemoryDocument, scan_auto_memory_topic_documents,
    scan_user_auto_memory_topic_documents,
};
use super::store::AutoMemoryType;
use crate::utils::cancellation::CancellationToken;

pub const MAX_RELEVANT_DOCS: usize = 5;
pub const MAX_DOC_BODY_CODE_UNITS: usize = 1_200;
const MILLISECONDS_PER_DAY: f64 = 86_400_000.0;
const EMPTY_MEMORY_BODY: &str = "_No entries yet._";
const TRUNCATED_MEMORY_NOTE: &str = "> NOTE: Relevant memory truncated for prompt budget.";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AutoMemoryRecallStrategy {
    None,
    Heuristic,
    Model,
}

impl AutoMemoryRecallStrategy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Heuristic => "heuristic",
            Self::Model => "model",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AutoMemoryRecallTelemetryEvent {
    /// JavaScript-compatible UTF-16 code-unit length.
    pub query_length: usize,
    pub docs_scanned: usize,
    pub docs_selected: usize,
    pub strategy: AutoMemoryRecallStrategy,
    pub duration_ms: u64,
}

/// Optional host adapter for memory-recall telemetry.
pub trait AutoMemoryRecallTelemetry: Send + Sync {
    fn record_memory_recall(&self, _event: AutoMemoryRecallTelemetryEvent) {}
}

#[derive(Clone, Debug, PartialEq)]
pub struct RelevantAutoMemoryPromptResult {
    pub prompt: String,
    pub selected_docs: Vec<ScannedAutoMemoryDocument>,
    pub strategy: AutoMemoryRecallStrategy,
}

#[derive(Default)]
pub struct ResolveRelevantAutoMemoryPromptOptions<'a> {
    /// Exact absolute paths to omit before either selector sees the manifest.
    pub excluded_file_paths: &'a [PathBuf],
    /// Defaults to the source resolver's five-document cap.
    pub limit: Option<i64>,
    pub recent_tools: &'a [String],
    pub selector: Option<&'a dyn AutoMemoryRecallSelector>,
    pub cancellation: Option<&'a CancellationToken>,
    /// Omit this adapter to suppress telemetry, as the TypeScript resolver does
    /// when no Config is supplied.
    pub telemetry: Option<&'a dyn AutoMemoryRecallTelemetry>,
}

#[derive(Debug, thiserror::Error)]
pub enum AutoMemoryRecallResolveError {
    #[error("Project-level auto-memory scan failed: {0}")]
    ProjectScan(#[source] io::Error),
}

const ACTIVE_TOOL_USAGE_MEMORY_MARKERS: &[&str] = &[
    "api docs",
    "api documentation",
    "failed call",
    "failed tool call",
    "failed tool-call",
    "field mapping",
    "field mappings",
    "guessed call",
    "guessed tool",
    "mcp tool",
    "parameter schema",
    "parameter schemas",
    "tool schema",
    "tool schemas",
    "tool usage",
    "usage reference",
];

const DURABLE_ACTIVE_TOOL_MEMORY_MARKERS: &[&str] = &[
    "credential",
    "credentials",
    "escalation",
    "gotcha",
    "gotchas",
    "known issue",
    "known issues",
    "owner",
    "ownership",
    "warning",
    "warnings",
    "workaround",
    "workarounds",
];

impl AsRef<ScannedAutoMemoryDocument> for ScannedAutoMemoryDocument {
    fn as_ref(&self) -> &ScannedAutoMemoryDocument {
        self
    }
}

/// Days since the memory's mtime, floor-rounded and clamped at zero.
/// `now_ms` is injectable so boundary behavior can be tested deterministically.
pub fn memory_age_days_at(mtime_ms: f64, now_ms: f64) -> f64 {
    let days = ((now_ms - mtime_ms) / MILLISECONDS_PER_DAY).floor();
    if days.is_nan() {
        f64::NAN
    } else if days < 0.0 {
        0.0
    } else {
        days
    }
}

/// Current-time wrapper around [`memory_age_days_at`].
pub fn memory_age_days(mtime_ms: f64) -> f64 {
    memory_age_days_at(mtime_ms, now_ms())
}

/// Return the source's human-readable age label using an injected clock.
pub fn memory_age_at(mtime_ms: f64, now_ms: f64) -> String {
    let days = memory_age_days_at(mtime_ms, now_ms);
    if days == 0.0 {
        "today".to_owned()
    } else if days == 1.0 {
        "yesterday".to_owned()
    } else {
        format!("{} days ago", js_number_string(days))
    }
}

/// Return the current-time memory age label.
pub fn memory_age(mtime_ms: f64) -> String {
    memory_age_at(mtime_ms, now_ms())
}

/// Produce the freshness warning for memories older than one full day.
pub fn memory_freshness_text_at(mtime_ms: f64, now_ms: f64) -> String {
    let days = memory_age_days_at(mtime_ms, now_ms);
    if days <= 1.0 {
        return String::new();
    }
    format!(
        "This memory is {} days old. Memories are point-in-time observations, not live state — claims about code behavior or file:line citations may be outdated. Verify against current code before asserting as fact.",
        js_number_string(days)
    )
}

/// Current-time wrapper around [`memory_freshness_text_at`].
pub fn memory_freshness_text(mtime_ms: f64) -> String {
    memory_freshness_text_at(mtime_ms, now_ms())
}

/// Format the system-reminder wrapper used for stale memories.
pub fn memory_freshness_note_at(mtime_ms: f64, now_ms: f64) -> String {
    let text = memory_freshness_text_at(mtime_ms, now_ms);
    if text.is_empty() {
        String::new()
    } else {
        format!("<system-reminder>{text}</system-reminder>\n")
    }
}

/// Current-time wrapper around [`memory_freshness_note_at`].
pub fn memory_freshness_note(mtime_ms: f64) -> String {
    memory_freshness_note_at(mtime_ms, now_ms())
}

/// Select by deterministic token overlap, then type. Equal scores and equal
/// types retain input order, matching stable JavaScript `Array.sort` behavior.
/// A negative limit has JavaScript `slice(0, limit)` semantics (drop items from
/// the end); callers in the query resolver normally short-circuit nonpositive
/// limits before reaching this selector.
pub fn select_relevant_auto_memory_documents<'a>(
    query: &str,
    docs: &'a [ScannedAutoMemoryDocument],
    limit: i64,
) -> Vec<&'a ScannedAutoMemoryDocument> {
    select_ranked_documents(query, docs.iter(), limit)
}

fn select_ranked_documents<'a>(
    query: &str,
    docs: impl Iterator<Item = &'a ScannedAutoMemoryDocument>,
    limit: i64,
) -> Vec<&'a ScannedAutoMemoryDocument> {
    let query_tokens = tokenize(query);
    if query_tokens.is_empty() {
        return Vec::new();
    }

    let mut ranked = docs
        .map(|doc| (doc, score_document(&query_tokens, doc)))
        .filter(|(_, score)| *score > 0)
        .collect::<Vec<_>>();
    ranked.sort_by(|(left_doc, left_score), (right_doc, right_score)| {
        right_score.cmp(left_score).then_with(|| {
            left_doc
                .memory_type
                .as_str()
                .cmp(right_doc.memory_type.as_str())
        })
    });

    let end = slice_end(ranked.len(), limit);
    ranked.into_iter().take(end).map(|(doc, _)| doc).collect()
}

/// Select deterministic fallback memories while suppressing active-tool usage
/// references. Durable ownership, credentials, gotchas, warnings, and
/// workarounds remain eligible even when they mention a recently used tool.
pub fn select_fallback_auto_memory_documents<'a>(
    query: &str,
    docs: &'a [ScannedAutoMemoryDocument],
    limit: i64,
    recent_tools: &[String],
) -> Vec<&'a ScannedAutoMemoryDocument> {
    if recent_tools.is_empty() {
        return select_relevant_auto_memory_documents(query, docs, limit);
    }
    select_ranked_documents(
        query,
        docs.iter()
            .filter(|doc| !is_active_tool_usage_memory(doc, recent_tools)),
        limit,
    )
}

/// Source-default selector with the default five-document cap.
pub fn select_relevant_auto_memory_documents_default<'a>(
    query: &str,
    docs: &'a [ScannedAutoMemoryDocument],
) -> Vec<&'a ScannedAutoMemoryDocument> {
    select_relevant_auto_memory_documents(query, docs, MAX_RELEVANT_DOCS as i64)
}

/// Format selected memories as the prompt block used by the current runtime.
pub fn build_relevant_auto_memory_prompt<T>(docs: &[T]) -> String
where
    T: AsRef<ScannedAutoMemoryDocument>,
{
    build_relevant_auto_memory_prompt_at(docs, now_ms())
}

/// Clock-injected prompt formatter used for deterministic tests and replay.
pub fn build_relevant_auto_memory_prompt_at<T>(docs: &[T], now_ms: f64) -> String
where
    T: AsRef<ScannedAutoMemoryDocument>,
{
    if docs.is_empty() {
        return String::new();
    }

    let mut lines = vec![
        "## Relevant memory".to_owned(),
        String::new(),
        "Use the following memories only when they are directly relevant to the current request. Verify file/function claims before relying on them.".to_owned(),
        String::new(),
    ];

    for item in docs {
        let doc = item.as_ref();
        let body = truncate_body(&doc.body);
        let file_path = doc.file_path.to_string_lossy();
        let path = if doc.relative_path.is_empty() {
            path_basename(&file_path)
        } else {
            doc.relative_path.as_str()
        };
        lines.push(format!("### {} ({path})", doc.title));
        lines.push(format!("Saved {}.", memory_age_at(doc.mtime_ms, now_ms)));
        lines.push(doc.description.clone());
        lines.push(String::new());
        lines.push(if body.is_empty() {
            "_No detailed entries yet._".to_owned()
        } else {
            body
        });

        let staleness = memory_freshness_text_at(doc.mtime_ms, now_ms);
        if !staleness.is_empty() {
            lines.push(String::new());
            lines.push(format!("> NOTE: {staleness}"));
        }
        lines.push(String::new());
    }

    lines.join("\n")
}

/// Resolve project and user memory for one query.
///
/// Project scan errors are returned; user-memory scan errors are isolated so
/// they cannot suppress project recall. Exclusions are applied before the
/// model manifest and heuristic selector are built. A successful model
/// selection, including an empty selection, is authoritative and keeps the
/// model's returned order. Model failures fall back to the deterministic
/// selector unless the caller cancelled the operation.
pub async fn resolve_relevant_auto_memory_prompt_for_query(
    paths: &AutoMemoryPaths,
    query: &str,
    options: ResolveRelevantAutoMemoryPromptOptions<'_>,
) -> Result<RelevantAutoMemoryPromptResult, AutoMemoryRecallResolveError> {
    let started = Instant::now();
    let (project_result, user_result) = tokio::join!(
        scan_auto_memory_topic_documents(paths),
        scan_user_auto_memory_topic_documents(paths)
    );
    let project_docs = project_result.map_err(AutoMemoryRecallResolveError::ProjectScan)?;
    let user_docs = user_result.unwrap_or_default();
    let excluded = options.excluded_file_paths.iter().collect::<HashSet<_>>();
    let docs = project_docs
        .into_iter()
        .chain(user_docs)
        .filter(|doc| !excluded.contains(&doc.file_path))
        .collect::<Vec<_>>();
    let limit = options.limit.unwrap_or(MAX_RELEVANT_DOCS as i64);

    if trim_js_whitespace(query).is_empty() || docs.is_empty() || limit <= 0 {
        return Ok(finish_recall(
            query,
            docs.len(),
            Vec::new(),
            AutoMemoryRecallStrategy::None,
            started,
            options.cancellation,
            options.telemetry,
        ));
    }

    if options
        .cancellation
        .is_some_and(CancellationToken::is_cancelled)
    {
        return Ok(finish_recall(
            query,
            docs.len(),
            Vec::new(),
            AutoMemoryRecallStrategy::None,
            started,
            options.cancellation,
            options.telemetry,
        ));
    }

    if let Some(selector) = options.selector {
        match select_relevant_auto_memory_documents_by_model(
            query,
            &docs,
            limit,
            options.recent_tools,
            selector,
            options.cancellation,
        )
        .await
        {
            Ok(selected) => {
                let strategy = if selected.is_empty() {
                    AutoMemoryRecallStrategy::None
                } else {
                    AutoMemoryRecallStrategy::Model
                };
                return Ok(finish_recall(
                    query,
                    docs.len(),
                    selected.into_iter().cloned().collect(),
                    strategy,
                    started,
                    options.cancellation,
                    options.telemetry,
                ));
            }
            Err(_)
                if options
                    .cancellation
                    .is_some_and(CancellationToken::is_cancelled) =>
            {
                return Ok(finish_recall(
                    query,
                    docs.len(),
                    Vec::new(),
                    AutoMemoryRecallStrategy::None,
                    started,
                    options.cancellation,
                    options.telemetry,
                ));
            }
            Err(_) => {}
        }
    }

    let selected = select_fallback_auto_memory_documents(query, &docs, limit, options.recent_tools)
        .into_iter()
        .cloned()
        .collect::<Vec<_>>();
    let strategy = if selected.is_empty() {
        AutoMemoryRecallStrategy::None
    } else {
        AutoMemoryRecallStrategy::Heuristic
    };
    Ok(finish_recall(
        query,
        docs.len(),
        selected,
        strategy,
        started,
        options.cancellation,
        options.telemetry,
    ))
}

fn finish_recall(
    query: &str,
    docs_scanned: usize,
    selected_docs: Vec<ScannedAutoMemoryDocument>,
    strategy: AutoMemoryRecallStrategy,
    started: Instant,
    cancellation: Option<&CancellationToken>,
    telemetry: Option<&dyn AutoMemoryRecallTelemetry>,
) -> RelevantAutoMemoryPromptResult {
    if !cancellation.is_some_and(CancellationToken::is_cancelled)
        && let Some(telemetry) = telemetry
    {
        telemetry.record_memory_recall(AutoMemoryRecallTelemetryEvent {
            query_length: query.encode_utf16().count(),
            docs_scanned,
            docs_selected: selected_docs.len(),
            strategy,
            duration_ms: started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
        });
    }

    let prompt = build_relevant_auto_memory_prompt(&selected_docs);
    RelevantAutoMemoryPromptResult {
        prompt,
        selected_docs,
        strategy,
    }
}

fn now_ms() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as f64)
        .unwrap_or_else(|error| -(error.duration().as_millis() as f64))
}

fn js_number_string(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_owned()
    } else if value == f64::INFINITY {
        "Infinity".to_owned()
    } else if value == f64::NEG_INFINITY {
        "-Infinity".to_owned()
    } else if value == 0.0 {
        "0".to_owned()
    } else if value.fract() == 0.0 {
        format!("{value:.0}")
    } else {
        value.to_string()
    }
}

fn tokenize(text: &str) -> Vec<String> {
    let lowercase = text.to_lowercase();
    let mut tokens = Vec::new();
    let mut seen = HashSet::new();
    for token in lowercase.split(|character: char| !character.is_ascii_alphanumeric()) {
        if token.len() >= 3 && seen.insert(token) {
            tokens.push(token.to_owned());
        }
    }
    tokens
}

fn normalize_body(body: &str) -> &str {
    let trimmed = trim_js_whitespace(body);
    if trimmed == EMPTY_MEMORY_BODY {
        ""
    } else {
        trimmed
    }
}

fn score_document(query_tokens: &[String], doc: &ScannedAutoMemoryDocument) -> usize {
    let normalized_body = normalize_body(&doc.body);
    // The source joins and lowercases the full document haystack. Lowercase
    // each field separately: ASCII query tokens cannot cross the inserted
    // spaces, and this avoids another full-body joined-string allocation.
    let haystack = [
        doc.memory_type.as_str().to_lowercase(),
        doc.title.to_lowercase(),
        doc.description.to_lowercase(),
        normalized_body.to_lowercase(),
    ];

    let type_keywords = type_keywords(doc.memory_type);
    let mut score = 0_usize;
    for token in query_tokens {
        if haystack.iter().any(|field| field.contains(token)) {
            score = score.saturating_add(2);
        }
        if type_keywords.contains(&token.as_str()) {
            score = score.saturating_add(1);
        }
    }
    if !normalized_body.is_empty() {
        score = score.saturating_add(1);
    }
    score
}

fn type_keywords(memory_type: AutoMemoryType) -> &'static [&'static str] {
    match memory_type {
        AutoMemoryType::User => &[
            "user",
            "preference",
            "preferences",
            "background",
            "role",
            "terse",
        ],
        AutoMemoryType::Feedback => &["feedback", "rule", "rules", "avoid", "style", "summary"],
        AutoMemoryType::Project => &[
            "project", "goal", "goals", "incident", "deadline", "release",
        ],
        AutoMemoryType::Reference => &["reference", "dashboard", "ticket", "docs", "doc", "link"],
    }
}

fn is_active_tool_usage_memory(doc: &ScannedAutoMemoryDocument, recent_tools: &[String]) -> bool {
    if recent_tools.is_empty() {
        return false;
    }

    let haystack = [
        doc.title.to_lowercase(),
        doc.description.to_lowercase(),
        normalize_body(&doc.body).to_lowercase(),
    ];
    let names_active_tool = recent_tools.iter().any(|tool_name| {
        tool_aliases(tool_name)
            .iter()
            .any(|alias| joined_fields_contain(&haystack, alias))
    });
    if !names_active_tool {
        return false;
    }
    if DURABLE_ACTIVE_TOOL_MEMORY_MARKERS
        .iter()
        .any(|marker| joined_fields_contain(&haystack, marker))
    {
        return false;
    }
    ACTIVE_TOOL_USAGE_MEMORY_MARKERS
        .iter()
        .any(|marker| joined_fields_contain(&haystack, marker))
}

/// Search the source's single-space-joined fields without allocating another
/// copy of a possibly large memory body.
fn joined_fields_contain(fields: &[String], needle: &str) -> bool {
    let pattern = needle.as_bytes();
    if pattern.is_empty() {
        return true;
    }

    let mut prefix = vec![0; pattern.len()];
    let mut matched = 0;
    for index in 1..pattern.len() {
        while matched > 0 && pattern[index] != pattern[matched] {
            matched = prefix[matched - 1];
        }
        if pattern[index] == pattern[matched] {
            matched += 1;
            prefix[index] = matched;
        }
    }

    let mut matched = 0;
    for (field_index, field) in fields.iter().enumerate() {
        for byte in field
            .bytes()
            .chain((field_index + 1 < fields.len()).then_some(b' '))
        {
            while matched > 0 && byte != pattern[matched] {
                matched = prefix[matched - 1];
            }
            if byte == pattern[matched] {
                matched += 1;
                if matched == pattern.len() {
                    return true;
                }
            }
        }
    }
    false
}

fn tool_aliases(tool_name: &str) -> Vec<String> {
    let normalized = trim_js_whitespace(tool_name).to_lowercase();
    let mut aliases = Vec::new();
    push_alias(&mut aliases, &normalized);

    if let Some((_, suffix)) = normalized.rsplit_once("::") {
        push_alias(&mut aliases, suffix);
    }

    if normalized.starts_with("mcp__") {
        let parts = normalized.split("__").collect::<Vec<_>>();
        if parts.len() >= 3 {
            push_alias(&mut aliases, &parts[2..].join("__"));
            if let Some(last) = parts.last() {
                push_alias(&mut aliases, last);
            }
        }
    }
    aliases
}

fn push_alias(aliases: &mut Vec<String>, alias: &str) {
    let alias = trim_js_whitespace(alias);
    if !alias.is_empty() && !aliases.iter().any(|existing| existing == alias) {
        aliases.push(alias.to_owned());
    }
}

fn truncate_body(body: &str) -> String {
    let normalized = normalize_body(body);
    let code_units = normalized
        .encode_utf16()
        .take(MAX_DOC_BODY_CODE_UNITS + 1)
        .count();
    if code_units <= MAX_DOC_BODY_CODE_UNITS {
        return normalized.to_owned();
    }

    // JavaScript slices UTF-16 code units and may leave a lone surrogate at
    // this boundary. Rust strings cannot represent that value, so lossy UTF-16
    // decoding emits U+FFFD for the unmatched half while preserving the exact
    // 1,200-code-unit cut and subsequent prompt layout.
    let prefix = normalized
        .encode_utf16()
        .take(MAX_DOC_BODY_CODE_UNITS)
        .collect::<Vec<_>>();
    format!(
        "{}\n\n{TRUNCATED_MEMORY_NOTE}",
        String::from_utf16_lossy(&prefix).trim_end_matches(is_js_whitespace)
    )
}

fn path_basename(path: &str) -> &str {
    path.rsplit('/')
        .find(|component| !component.is_empty())
        .unwrap_or(path)
}

fn trim_js_whitespace(value: &str) -> &str {
    value.trim_matches(is_js_whitespace)
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

fn slice_end(length: usize, limit: i64) -> usize {
    if limit >= 0 {
        length.min(limit as usize)
    } else {
        length.saturating_sub(limit.unsigned_abs().min(usize::MAX as u64) as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::paths::MemoryProjectScope;
    use crate::memory::relevance_selector::{AutoMemoryRecallRequest, AutoMemoryRecallSelector};
    use crate::utils::cancellation::CancellationToken;
    use serde_json::{Value, json};
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Mutex;

    struct TempRecallMemory {
        root: PathBuf,
        paths: AutoMemoryPaths,
    }

    impl TempRecallMemory {
        fn new() -> Self {
            let root =
                std::env::temp_dir().join(format!("canopy-memory-recall-{}", uuid::Uuid::new_v4()));
            let project_root = root.join("project");
            let memory_base = root.join("memory-base");
            std::fs::create_dir_all(&project_root).unwrap();
            let paths = AutoMemoryPaths::new(
                &project_root,
                &memory_base,
                false,
                MemoryProjectScope::Workspace,
            );
            Self { root, paths }
        }

        async fn write_project_topic(&self, filename: &str, content: &str) -> PathBuf {
            let path = self.paths.auto_memory_root().join(filename);
            tokio::fs::create_dir_all(path.parent().unwrap())
                .await
                .unwrap();
            tokio::fs::write(&path, content).await.unwrap();
            path
        }

        async fn write_user_topic(&self, filename: &str, content: &str) -> PathBuf {
            let path = self.paths.user_auto_memory_root().join(filename);
            tokio::fs::create_dir_all(path.parent().unwrap())
                .await
                .unwrap();
            tokio::fs::write(&path, content).await.unwrap();
            path
        }
    }

    impl Drop for TempRecallMemory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }

    struct FixedRecallSelector {
        response: Result<Value, String>,
        request: Mutex<Option<AutoMemoryRecallRequest>>,
    }

    impl FixedRecallSelector {
        fn new(response: Result<Value, String>) -> Self {
            Self {
                response,
                request: Mutex::new(None),
            }
        }
    }

    impl AutoMemoryRecallSelector for FixedRecallSelector {
        fn select_json<'a>(
            &'a self,
            request: &'a AutoMemoryRecallRequest,
            _cancellation: &'a CancellationToken,
        ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
            *self.request.lock().unwrap() = Some(request.clone());
            let response = self.response.clone();
            Box::pin(async move { response })
        }
    }

    #[derive(Default)]
    struct RecallTelemetryRecorder(Mutex<Vec<AutoMemoryRecallTelemetryEvent>>);

    impl AutoMemoryRecallTelemetry for RecallTelemetryRecorder {
        fn record_memory_recall(&self, event: AutoMemoryRecallTelemetryEvent) {
            self.0.lock().unwrap().push(event);
        }
    }

    fn topic(type_name: &str, title: &str, description: &str, body: &str) -> String {
        format!("---\ntype: {type_name}\nname: {title}\ndescription: {description}\n---\n{body}\n")
    }

    fn doc(
        memory_type: AutoMemoryType,
        file_path: &str,
        relative_path: &str,
        title: &str,
        description: &str,
        body: &str,
        mtime_ms: f64,
    ) -> ScannedAutoMemoryDocument {
        ScannedAutoMemoryDocument {
            memory_type,
            file_path: file_path.into(),
            relative_path: relative_path.to_owned(),
            filename: path_basename(file_path).to_owned(),
            title: title.to_owned(),
            description: description.to_owned(),
            body: body.to_owned(),
            mtime_ms,
        }
    }

    #[test]
    fn tokenizes_unique_ascii_terms_and_ranks_body_and_type_matches() {
        let docs = vec![
            doc(
                AutoMemoryType::Project,
                "/project.md",
                "project.md",
                "Build context",
                "",
                "Current latency dashboard guide",
                0.0,
            ),
            doc(
                AutoMemoryType::Reference,
                "/reference.md",
                "reference.md",
                "Dashboard reference",
                "",
                "Latency details",
                0.0,
            ),
            doc(
                AutoMemoryType::User,
                "/unrelated.md",
                "unrelated.md",
                "Preferences",
                "",
                "Short replies",
                0.0,
            ),
        ];

        let selected =
            select_relevant_auto_memory_documents("DASHBOARD dashboard, latency!", &docs, 5);
        assert_eq!(selected[0].memory_type, AutoMemoryType::Reference);
        assert_eq!(selected[1].memory_type, AutoMemoryType::Project);
        // A non-empty body itself contributes one point, so the unrelated
        // user document is retained with the minimum positive score.
        assert_eq!(selected.len(), 3);
        assert!(build_relevant_auto_memory_prompt(&selected).contains("Dashboard reference"));
        assert!(select_relevant_auto_memory_documents("---", &docs, 5).is_empty());
    }

    #[test]
    fn ties_sort_by_type_then_preserve_source_order_within_type() {
        let docs = vec![
            doc(
                AutoMemoryType::User,
                "/user.md",
                "user.md",
                "Alpha",
                "",
                "body",
                0.0,
            ),
            doc(
                AutoMemoryType::Feedback,
                "/feedback.md",
                "feedback.md",
                "Alpha",
                "",
                "body",
                0.0,
            ),
            doc(
                AutoMemoryType::Reference,
                "/ref-first.md",
                "ref-first.md",
                "Alpha",
                "",
                "body",
                0.0,
            ),
            doc(
                AutoMemoryType::Reference,
                "/ref-second.md",
                "ref-second.md",
                "Alpha",
                "",
                "body",
                0.0,
            ),
            doc(
                AutoMemoryType::Project,
                "/project.md",
                "project.md",
                "Alpha",
                "",
                "body",
                0.0,
            ),
        ];

        let selected = select_relevant_auto_memory_documents("alpha", &docs, 5);
        assert_eq!(
            selected
                .iter()
                .map(|doc| doc.file_path.to_string_lossy().into_owned())
                .collect::<Vec<_>>(),
            [
                "/feedback.md".to_owned(),
                "/project.md".to_owned(),
                "/ref-first.md".to_owned(),
                "/ref-second.md".to_owned(),
                "/user.md".to_owned()
            ]
        );
    }

    #[test]
    fn preserves_javascript_slice_limit_edges() {
        let docs = vec![
            doc(AutoMemoryType::Project, "/a", "a", "alpha", "", "body", 0.0),
            doc(AutoMemoryType::Project, "/b", "b", "alpha", "", "body", 0.0),
            doc(AutoMemoryType::Project, "/c", "c", "alpha", "", "body", 0.0),
        ];
        assert_eq!(
            select_relevant_auto_memory_documents("alpha", &docs, 0).len(),
            0
        );
        assert_eq!(
            select_relevant_auto_memory_documents("alpha", &docs, -1).len(),
            2
        );
    }

    #[test]
    fn repeated_query_tokens_are_counted_once_and_short_tokens_are_ignored() {
        let docs = vec![
            doc(
                AutoMemoryType::Project,
                "/beta.md",
                "beta.md",
                "Beta",
                "",
                "beta",
                0.0,
            ),
            doc(
                AutoMemoryType::Project,
                "/alpha.md",
                "alpha.md",
                "Alpha",
                "",
                "alpha",
                0.0,
            ),
        ];

        let selected = select_relevant_auto_memory_documents("alpha alpha alpha beta", &docs, 5);
        assert_eq!(selected[0].file_path, std::path::Path::new("/beta.md"));
        assert_eq!(selected[1].file_path, std::path::Path::new("/alpha.md"));
        assert!(select_relevant_auto_memory_documents("go to me", &docs, 5).is_empty());
    }

    #[test]
    fn suppresses_active_tool_reference_but_keeps_durable_operational_memory() {
        let docs = vec![
            doc(
                AutoMemoryType::Reference,
                "/schema.md",
                "schema.md",
                "ATA schema",
                "article-list-query parameter schema",
                "Failed tool-call field mappings",
                0.0,
            ),
            doc(
                AutoMemoryType::Reference,
                "/gotcha.md",
                "gotcha.md",
                "ATA known",
                "issue article-list-query parameter schema",
                "Check with the ATA oncall before retrying.",
                0.0,
            ),
        ];
        let tools = vec!["mcp__ata__article-list-query".to_owned()];

        let selected = select_fallback_auto_memory_documents("article query", &docs, 5, &tools);
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].file_path, std::path::Path::new("/gotcha.md"));
    }

    #[test]
    fn age_rounding_clamps_future_times_and_warns_only_after_yesterday() {
        let now = 3.0 * MILLISECONDS_PER_DAY;
        assert_eq!(memory_age_days_at(now, now), 0.0);
        assert_eq!(memory_age_at(now, now), "today");
        assert_eq!(memory_age_at(now - MILLISECONDS_PER_DAY, now), "yesterday");
        assert_eq!(
            memory_age_days_at(now - 1.9 * MILLISECONDS_PER_DAY, now),
            1.0
        );
        assert_eq!(memory_age_days_at(now + MILLISECONDS_PER_DAY, now), 0.0);
        assert_eq!(
            memory_freshness_text_at(now - MILLISECONDS_PER_DAY, now),
            ""
        );
        assert!(
            memory_freshness_text_at(now - 2.0 * MILLISECONDS_PER_DAY, now)
                .starts_with("This memory is 2 days old.")
        );
        assert_eq!(memory_freshness_note_at(now, now), "");
        assert_eq!(
            memory_freshness_note_at(now - 2.0 * MILLISECONDS_PER_DAY, now),
            format!(
                "<system-reminder>{}</system-reminder>\n",
                memory_freshness_text_at(now - 2.0 * MILLISECONDS_PER_DAY, now)
            )
        );
    }

    #[test]
    fn prompt_matches_headers_body_placeholders_age_notes_and_trailing_newline() {
        let docs = vec![
            doc(
                AutoMemoryType::Reference,
                "/tmp/reference.md",
                "",
                "Reference Memory",
                "Dashboard constraints",
                "  _No entries yet._  ",
                0.0,
            ),
            doc(
                AutoMemoryType::User,
                "/tmp/user.md",
                "user.md",
                "User Memory",
                "User preferences",
                "Prefers short answers.",
                0.0,
            ),
        ];

        let prompt = build_relevant_auto_memory_prompt_at(&docs, 2.0 * MILLISECONDS_PER_DAY);
        let stale_note = "This memory is 2 days old. Memories are point-in-time observations, not live state — claims about code behavior or file:line citations may be outdated. Verify against current code before asserting as fact.";
        let expected = format!(
            "## Relevant memory\n\nUse the following memories only when they are directly relevant to the current request. Verify file/function claims before relying on them.\n\n### Reference Memory (reference.md)\nSaved 2 days ago.\nDashboard constraints\n\n_No detailed entries yet._\n\n> NOTE: {stale_note}\n\n### User Memory (user.md)\nSaved 2 days ago.\nUser preferences\n\nPrefers short answers.\n\n> NOTE: {stale_note}\n"
        );
        assert_eq!(prompt, expected);
        assert!(
            build_relevant_auto_memory_prompt_at(&[] as &[ScannedAutoMemoryDocument], 0.0)
                .is_empty()
        );
    }

    #[test]
    fn body_truncation_counts_utf16_code_units_and_trims_javascript_whitespace() {
        let astral_doc = doc(
            AutoMemoryType::Project,
            "/long.md",
            "long.md",
            "Long memory",
            "",
            &format!("{}😀tail", "a".repeat(1_199)),
            0.0,
        );
        let prompt = build_relevant_auto_memory_prompt_at(&[astral_doc], 0.0);
        let selected_body = prompt.contains(&format!(
            "{}�\n\n{TRUNCATED_MEMORY_NOTE}",
            "a".repeat(1_199)
        ));
        assert!(
            selected_body,
            "UTF-16 cut preserves 1,199 ASCII units + replacement"
        );

        let trailing_space = doc(
            AutoMemoryType::Project,
            "/space.md",
            "space.md",
            "Space",
            "",
            &format!("{}\u{feff}tail", "x".repeat(MAX_DOC_BODY_CODE_UNITS - 1)),
            0.0,
        );
        let prompt = build_relevant_auto_memory_prompt_at(&[trailing_space], 0.0);
        assert!(prompt.contains(&format!("{}\n\n{TRUNCATED_MEMORY_NOTE}", "x".repeat(1_199))));
    }

    #[tokio::test]
    async fn resolver_filters_exclusions_before_model_selection_and_keeps_model_order() {
        let memory = TempRecallMemory::new();
        let project_path = memory
            .write_project_topic(
                "project.md",
                &topic(
                    "project",
                    "Project memory",
                    "project query",
                    "Project body.",
                ),
            )
            .await;
        let excluded_path = memory
            .write_project_topic(
                "excluded.md",
                &topic(
                    "reference",
                    "Excluded memory",
                    "query",
                    "Should not be selected.",
                ),
            )
            .await;
        let user_path = memory
            .write_user_topic(
                "user.md",
                &topic("user", "User memory", "user query", "User body."),
            )
            .await;
        let selector = FixedRecallSelector::new(Ok(json!({
            "selected_memories": [
                user_path.to_string_lossy(),
                project_path.to_string_lossy()
            ]
        })));
        let telemetry = RecallTelemetryRecorder::default();
        let excluded = [excluded_path.clone()];

        let result = resolve_relevant_auto_memory_prompt_for_query(
            &memory.paths,
            "🦊 query",
            ResolveRelevantAutoMemoryPromptOptions {
                excluded_file_paths: &excluded,
                selector: Some(&selector),
                telemetry: Some(&telemetry),
                ..ResolveRelevantAutoMemoryPromptOptions::default()
            },
        )
        .await
        .unwrap();

        assert_eq!(result.strategy, AutoMemoryRecallStrategy::Model);
        assert_eq!(
            result
                .selected_docs
                .iter()
                .map(|doc| doc.file_path.clone())
                .collect::<Vec<_>>(),
            [user_path, project_path]
        );
        assert!(
            result.prompt.find("User memory").unwrap()
                < result.prompt.find("Project memory").unwrap()
        );
        let request = selector.request.lock().unwrap().clone().unwrap();
        assert!(
            !request.contents[0]
                .text
                .contains(&excluded_path.to_string_lossy().to_string())
        );
        let events = telemetry.0.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].query_length, 8);
        assert_eq!(events[0].docs_scanned, 2);
        assert_eq!(events[0].docs_selected, 2);
        assert_eq!(events[0].strategy, AutoMemoryRecallStrategy::Model);
    }

    #[tokio::test]
    async fn resolver_falls_back_with_tool_usage_suppression_and_reports_strategy() {
        let memory = TempRecallMemory::new();
        let tool_path = memory
            .write_project_topic(
                "tool.md",
                &topic(
                    "reference",
                    "ATA tool schema",
                    "article-list-query parameter schema",
                    "Failed tool-call field mappings.",
                ),
            )
            .await;
        let durable_path = memory
            .write_project_topic(
                "gotcha.md",
                &topic(
                    "reference",
                    "ATA known workaround",
                    "article-list-query known issue",
                    "Ask the ATA oncall before retrying.",
                ),
            )
            .await;
        let selector = FixedRecallSelector::new(Err("side query failed".to_owned()));
        let telemetry = RecallTelemetryRecorder::default();
        let recent_tools = vec!["mcp__ata__article-list-query".to_owned()];

        let result = resolve_relevant_auto_memory_prompt_for_query(
            &memory.paths,
            "article query",
            ResolveRelevantAutoMemoryPromptOptions {
                recent_tools: &recent_tools,
                selector: Some(&selector),
                telemetry: Some(&telemetry),
                ..ResolveRelevantAutoMemoryPromptOptions::default()
            },
        )
        .await
        .unwrap();

        assert_eq!(result.strategy, AutoMemoryRecallStrategy::Heuristic);
        assert_eq!(
            result
                .selected_docs
                .iter()
                .map(|doc| doc.file_path.clone())
                .collect::<Vec<_>>(),
            [durable_path]
        );
        assert_ne!(result.selected_docs[0].file_path, tool_path);
        let events = telemetry.0.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].strategy, AutoMemoryRecallStrategy::Heuristic);
        assert_eq!(events[0].docs_scanned, 2);
        assert_eq!(events[0].docs_selected, 1);
    }

    #[tokio::test]
    async fn resolver_suppresses_telemetry_and_results_after_caller_cancellation() {
        let memory = TempRecallMemory::new();
        memory
            .write_project_topic(
                "project.md",
                &topic("project", "Project memory", "query", "Project body."),
            )
            .await;
        let selector = FixedRecallSelector::new(Ok(json!({"selected_memories": []})));
        let telemetry = RecallTelemetryRecorder::default();
        let cancellation = CancellationToken::new();
        cancellation.cancel();

        let result = resolve_relevant_auto_memory_prompt_for_query(
            &memory.paths,
            "query",
            ResolveRelevantAutoMemoryPromptOptions {
                selector: Some(&selector),
                cancellation: Some(&cancellation),
                telemetry: Some(&telemetry),
                ..ResolveRelevantAutoMemoryPromptOptions::default()
            },
        )
        .await
        .unwrap();

        assert_eq!(result.strategy, AutoMemoryRecallStrategy::None);
        assert!(result.selected_docs.is_empty());
        assert!(selector.request.lock().unwrap().is_none());
        assert!(telemetry.0.lock().unwrap().is_empty());
    }
}
