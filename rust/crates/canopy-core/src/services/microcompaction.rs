//! Clear old tool outputs and attached media from provider history.
//!
//! Source: `packages/core/src/services/microcompaction/microcompact.ts`.
//! This operates on Gemini-shaped `serde_json::Value` content records and
//! leaves caller-owned history untouched. Unchanged results borrow the input;
//! changed histories are returned as an owned copy.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};

use serde::Serialize;
use serde_json::{Map, Value, json};

use crate::services::compaction_input_slimming::sanitize_mime_for_placeholder;

pub const MICROCOMPACT_CLEARED_MESSAGE: &str = "[Old tool result content cleared]";
pub const MICROCOMPACT_CLEARED_IMAGE_PREFIX: &str = "[Old inline media cleared:";
pub const DEFAULT_TOOL_RESULTS_TOTAL_CHARS_THRESHOLD: f64 = 500_000.0;
pub const DEFAULT_TOOL_RESULTS_NUM_TO_KEEP: usize = 5;
pub const MEDIA_PART_TOKEN_ESTIMATE: f64 = 1_600.0;

const COMPACTABLE_TOOLS: &[&str] = &[
    "read_file",
    "run_shell_command",
    "grep_search",
    "glob",
    "super_search",
    "web_fetch",
    "web_search",
    "read_mcp_resource",
    "edit",
    "write_file",
    "skill",
];
const FILE_PATH_TOOLS: &[&str] = &["read_file", "edit", "write_file"];
const MAX_SAFE_INTEGER: f64 = 9_007_199_254_740_991.0;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum MicrocompactTriggerReason {
    Force,
    Idle,
    Size,
}

impl MicrocompactTriggerReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Force => "force",
            Self::Idle => "idle",
            Self::Size => "size",
        }
    }
}

/// Settings accepted by the source `ClearContextOnIdleSettings` interface.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct ClearContextOnIdleSettings {
    pub tool_results_threshold_minutes: Option<f64>,
    pub tool_results_num_to_keep: Option<f64>,
    pub tool_results_total_chars_threshold: Option<f64>,
}

/// Optional controls for one microcompaction pass.
#[derive(Default)]
pub struct MicrocompactOptions<'a> {
    pub force: bool,
    pub size_only: bool,
    /// Pending tool-result content is appended virtually for size accounting,
    /// but is never modified or used to consume recent-result protection.
    pub pending_content: Option<&'a [Value]>,
    /// Returning true protects the result of a read_file call for this path.
    pub preserve_read_file_result: Option<&'a dyn Fn(&str) -> bool>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TimeBasedTrigger {
    pub gap_ms: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MicrocompactMeta {
    pub trigger_reason: MicrocompactTriggerReason,
    pub gap_minutes: f64,
    pub threshold_minutes: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_result_chars_before: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_result_chars_after: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pending_tool_result_chars: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_results_total_chars_threshold: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_results_low_watermark: Option<f64>,
    pub tools_cleared: usize,
    pub media_cleared: usize,
    pub tools_kept: usize,
    pub media_kept: usize,
    pub keep_recent: usize,
    pub tokens_saved: f64,
    pub evicted_read_paths: Vec<String>,
    pub unresolved_evicted_reads: usize,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MicrocompactResult<'a> {
    pub history: Cow<'a, [Value]>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<MicrocompactMeta>,
}

/// Check whether the configured idle threshold has elapsed.
pub fn evaluate_time_based_trigger(
    last_api_completion_timestamp: Option<f64>,
    settings: &ClearContextOnIdleSettings,
    now_ms: f64,
) -> Option<TimeBasedTrigger> {
    let threshold_minutes = settings.tool_results_threshold_minutes.unwrap_or(60.0);
    if threshold_minutes < 0.0 {
        return None;
    }
    let last_api_completion_timestamp = last_api_completion_timestamp?;
    let gap_ms = now_ms - last_api_completion_timestamp;
    if !gap_ms.is_finite() || gap_ms < threshold_minutes * 60_000.0 {
        return None;
    }
    Some(TimeBasedTrigger { gap_ms })
}

/// Microcompact with process environment configuration and the current clock.
pub fn microcompact_history<'a>(
    history: &'a [Value],
    last_api_completion_timestamp: Option<f64>,
    settings: &ClearContextOnIdleSettings,
    options: MicrocompactOptions<'_>,
) -> MicrocompactResult<'a> {
    let keep_recent_env = std::env::var("CANOPY_MC_KEEP_RECENT").ok();
    microcompact_history_with_env(
        history,
        last_api_completion_timestamp,
        settings,
        options,
        keep_recent_env.as_deref(),
        current_time_millis(),
    )
}

/// Deterministic variant for hosts and tests that provide environment and time.
pub fn microcompact_history_with_env<'a>(
    history: &'a [Value],
    last_api_completion_timestamp: Option<f64>,
    settings: &ClearContextOnIdleSettings,
    options: MicrocompactOptions<'_>,
    keep_recent_env: Option<&str>,
    now_ms: f64,
) -> MicrocompactResult<'a> {
    let keep_recent = resolve_keep_recent(keep_recent_env, settings.tool_results_num_to_keep);
    let mut trigger_reason = None;
    let mut gap_ms = 0.0;

    if options.force {
        trigger_reason = Some(MicrocompactTriggerReason::Force);
    } else if !options.size_only {
        if let Some(time_trigger) =
            evaluate_time_based_trigger(last_api_completion_timestamp, settings, now_ms)
        {
            trigger_reason = Some(MicrocompactTriggerReason::Idle);
            gap_ms = time_trigger.gap_ms;
        }
    }

    let (
        tool,
        media,
        nested_media,
        keep_refs,
        clear_refs,
        kept_path_history,
        kept_path_refs,
        tool_result_chars_before,
        tool_result_chars_after,
        pending_tool_result_chars,
        tool_results_total_chars_threshold,
        tool_results_low_watermark,
    ) = if matches!(
        trigger_reason,
        Some(MicrocompactTriggerReason::Force | MicrocompactTriggerReason::Idle)
    ) {
        let path_history = HistoryView::new(history, &[]);
        let collected =
            collect_compactable_part_refs(path_history, options.preserve_read_file_result);
        let tool = collected.tool;
        let media = collected.media;
        let nested_media = collected.nested_media;

        let keep_tool_refs = build_keep_refs(
            tool.iter()
                .filter(|reference| {
                    let part = get_part(path_history, reference);
                    get_tool_output_chars(part) > 0.0 || part.is_some_and(has_nested_media)
                })
                .cloned()
                .collect(),
            keep_recent,
        );
        let mut keep_refs = HashSet::new();
        keep_refs.extend(keep_tool_refs);
        keep_refs.extend(media.iter().rev().take(keep_recent).map(PartRef::key));
        keep_refs.extend(
            nested_media
                .iter()
                .rev()
                .take(keep_recent)
                .map(PartRef::key),
        );

        let tool_keys = tool.iter().map(PartRef::key).collect::<HashSet<_>>();
        let all_refs = tool.iter().chain(media.iter()).chain(nested_media.iter());
        let clear_refs = all_refs
            .filter(|reference| {
                let key = reference.key();
                if keep_refs.contains(&key) {
                    return false;
                }
                if tool_keys.contains(&key) {
                    let part = get_part(path_history, reference);
                    if get_tool_output_chars(part) == 0.0 && !part.is_some_and(has_nested_media) {
                        return false;
                    }
                }
                true
            })
            .cloned()
            .collect();
        let kept_path_refs = tool.clone();
        (
            tool,
            media,
            nested_media,
            keep_refs,
            clear_refs,
            path_history,
            kept_path_refs,
            None,
            None,
            None,
            None,
            None,
        )
    } else {
        let pending = options.pending_content.unwrap_or_default();
        let path_history = HistoryView::new(history, pending);
        let size_plan = plan_size_based_clearing(
            path_history,
            settings,
            keep_recent,
            options.preserve_read_file_result,
        );
        let Some(size_plan) = size_plan else {
            return unchanged(history);
        };
        trigger_reason = Some(MicrocompactTriggerReason::Size);
        let kept_path_refs = size_plan.tool_refs;
        let tool = kept_path_refs
            .iter()
            .filter(|reference| reference.content_index < history.len())
            .cloned()
            .collect();
        (
            tool,
            Vec::new(),
            Vec::new(),
            size_plan.keep_tool_refs,
            size_plan.clear_refs,
            path_history,
            kept_path_refs,
            Some(size_plan.tool_result_chars_before),
            Some(size_plan.tool_result_chars_after),
            Some(size_plan.pending_tool_result_chars),
            Some(size_plan.threshold),
            Some(size_plan.low_watermark),
        )
    };

    let trigger_reason = trigger_reason.expect("trigger set before compaction planning");
    if clear_refs.is_empty() && trigger_reason != MicrocompactTriggerReason::Size {
        return unchanged(history);
    }

    let mut tokens_saved = 0.0;
    let mut tools_cleared = 0;
    let mut media_cleared = 0;
    let mut evicted_read_paths = OrderedPaths::default();
    let mut unresolved_evicted_reads = 0;
    let result = if clear_refs.is_empty() {
        Cow::Borrowed(history)
    } else {
        let clear_map = build_clear_map(&clear_refs);
        let call_id_to_file_path = build_call_id_to_file_path(kept_path_history);
        let kept_file_paths = build_kept_file_paths(
            kept_path_history,
            &kept_path_refs,
            &keep_refs,
            &call_id_to_file_path,
        );
        let mut changed_contents = HashMap::new();
        for (content_index, content) in history.iter().enumerate() {
            let Some(parts_to_clean) = clear_map.get(&content_index) else {
                continue;
            };
            let Some(parts) = content.get("parts").and_then(Value::as_array) else {
                continue;
            };

            // Delay cloning unchanged parts until the first real replacement.
            // Some defensive refs can resolve to already-cleared content;
            // those messages should require only their one output clone.
            let mut new_parts: Option<Vec<Value>> = None;
            for (part_index, part) in parts.iter().enumerate() {
                let replacement = if let Some(kind) = parts_to_clean.get(&part_index) {
                    if is_already_cleared(part) {
                        None
                    } else {
                        match kind {
                            PartKind::Tool
                                if function_response_name(part)
                                    .is_some_and(is_compactable_tool)
                                    && !is_error_response(part) =>
                            {
                                tokens_saved += estimate_part_tokens(part);
                                tools_cleared += 1;
                                if function_response_name(part).is_some_and(is_file_path_tool) {
                                    if let Some(paths) = get_file_paths_for_response(
                                        Some(part),
                                        &call_id_to_file_path,
                                    ) {
                                        for path in paths {
                                            if !kept_file_paths.contains(&path) {
                                                evicted_read_paths.insert(path);
                                            }
                                        }
                                    } else {
                                        unresolved_evicted_reads += 1;
                                    }
                                }
                                Some(clear_tool_result(part))
                            }
                            PartKind::NestedMedia
                                if part.get("functionResponse").is_some()
                                    && !is_error_response(part) =>
                            {
                                tokens_saved += estimate_part_tokens(part);
                                media_cleared += 1;
                                Some(strip_nested_media_from_part(part))
                            }
                            PartKind::Media if has_top_level_media(part) => {
                                let mime =
                                    media_mime_type(part).unwrap_or("application/octet-stream");
                                tokens_saved += estimate_part_tokens(part);
                                media_cleared += 1;
                                Some(json!({
                                    "text": format!(
                                        "{MICROCOMPACT_CLEARED_IMAGE_PREFIX} {}]",
                                        sanitize_mime_for_placeholder(mime)
                                    )
                                }))
                            }
                            _ => None,
                        }
                    }
                } else {
                    None
                };

                if let Some(replacement) = replacement {
                    let new_parts = new_parts.get_or_insert_with(|| {
                        let mut new_parts = Vec::with_capacity(parts.len());
                        new_parts.extend(parts[..part_index].iter().cloned());
                        new_parts
                    });
                    new_parts.push(replacement);
                } else if let Some(new_parts) = &mut new_parts {
                    new_parts.push(part.clone());
                }
            }
            let Some(new_parts) = new_parts else {
                continue;
            };
            let mut new_content = Map::new();
            if let Some(object) = content.as_object() {
                for (key, value) in object {
                    if key != "parts" {
                        new_content.insert(key.clone(), value.clone());
                    }
                }
            }
            new_content.insert("parts".to_owned(), Value::Array(new_parts));
            changed_contents.insert(content_index, Value::Object(new_content));
        }
        if tokens_saved == 0.0 && trigger_reason != MicrocompactTriggerReason::Size {
            return unchanged(history);
        }
        let changed_history = history
            .iter()
            .enumerate()
            .map(|(content_index, content)| {
                changed_contents
                    .remove(&content_index)
                    .unwrap_or_else(|| content.clone())
            })
            .collect::<Vec<_>>();
        Cow::Owned(changed_history)
    };

    if tokens_saved == 0.0 && trigger_reason != MicrocompactTriggerReason::Size {
        return unchanged(history);
    }

    let threshold_minutes = settings.tool_results_threshold_minutes.unwrap_or(60.0);
    let tools_kept = tool
        .iter()
        .filter(|reference| keep_refs.contains(&reference.key()))
        .count();
    let media_kept = if trigger_reason == MicrocompactTriggerReason::Size {
        0
    } else {
        (media.len() + nested_media.len()).min(keep_recent)
    };

    MicrocompactResult {
        history: result,
        meta: Some(MicrocompactMeta {
            trigger_reason,
            gap_minutes: (gap_ms / 60_000.0).round(),
            threshold_minutes,
            tool_result_chars_before,
            tool_result_chars_after,
            pending_tool_result_chars,
            tool_results_total_chars_threshold,
            tool_results_low_watermark,
            tools_cleared,
            media_cleared,
            tools_kept,
            media_kept,
            keep_recent,
            tokens_saved,
            evicted_read_paths: evicted_read_paths.ordered,
            unresolved_evicted_reads,
        }),
    }
}

fn unchanged(history: &[Value]) -> MicrocompactResult<'_> {
    MicrocompactResult {
        history: Cow::Borrowed(history),
        meta: None,
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum PartKind {
    Tool,
    Media,
    NestedMedia,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PartRef {
    content_index: usize,
    part_index: usize,
    kind: PartKind,
}

impl PartRef {
    fn key(&self) -> PartKey {
        (self.content_index, self.part_index)
    }
}

type PartKey = (usize, usize);

/// A borrowed logical history with optional pending content at its tail.
/// This matches the source's virtual concatenation without deep-cloning each
/// `serde_json::Value` before it knows whether a size-triggered clear is due.
#[derive(Clone, Copy)]
struct HistoryView<'a> {
    committed: &'a [Value],
    pending: &'a [Value],
}

impl<'a> HistoryView<'a> {
    fn new(committed: &'a [Value], pending: &'a [Value]) -> Self {
        Self { committed, pending }
    }

    fn committed_len(self) -> usize {
        self.committed.len()
    }

    fn get(self, index: usize) -> Option<&'a Value> {
        if index < self.committed.len() {
            self.committed.get(index)
        } else {
            self.pending.get(index - self.committed.len())
        }
    }

    fn iter(self) -> impl Iterator<Item = (usize, &'a Value)> {
        self.committed.iter().chain(self.pending.iter()).enumerate()
    }
}

#[derive(Default)]
struct CollectedRefs {
    tool: Vec<PartRef>,
    media: Vec<PartRef>,
    nested_media: Vec<PartRef>,
}

fn collect_compactable_part_refs(
    history: HistoryView<'_>,
    preserve_read_file_result: Option<&dyn Fn(&str) -> bool>,
) -> CollectedRefs {
    let mut collected = CollectedRefs::default();
    for (content_index, content) in history.iter() {
        if content.get("role").and_then(Value::as_str) != Some("user") {
            continue;
        }
        let Some(parts) = content.get("parts").and_then(Value::as_array) else {
            continue;
        };
        for (part_index, part) in parts.iter().enumerate() {
            if function_response_name(part).is_some_and(is_compactable_tool) {
                collected.tool.push(PartRef {
                    content_index,
                    part_index,
                    kind: PartKind::Tool,
                });
            } else if part.get("functionResponse").is_some() && has_nested_media(part) {
                collected.nested_media.push(PartRef {
                    content_index,
                    part_index,
                    kind: PartKind::NestedMedia,
                });
            } else if has_top_level_media(part) {
                collected.media.push(PartRef {
                    content_index,
                    part_index,
                    kind: PartKind::Media,
                });
            }
        }
    }

    if let Some(preserve_read_file_result) = preserve_read_file_result {
        let preserved =
            build_preserved_read_refs(history, &collected.tool, preserve_read_file_result);
        collected
            .tool
            .retain(|reference| !preserved.contains(&reference.key()));
    }
    collected
}

fn has_nested_media(part: &Value) -> bool {
    part.get("functionResponse")
        .and_then(|response| response.get("parts"))
        .and_then(Value::as_array)
        .is_some_and(|parts| parts.iter().any(has_top_level_media))
}

fn has_top_level_media(part: &Value) -> bool {
    part.get("inlineData").is_some_and(js_truthy) || part.get("fileData").is_some_and(js_truthy)
}

fn is_error_response(part: &Value) -> bool {
    part.get("functionResponse")
        .and_then(|response| response.get("response"))
        .and_then(Value::as_object)
        .is_some_and(|response| response.contains_key("error"))
}

fn is_already_cleared(part: &Value) -> bool {
    part.pointer("/functionResponse/response/output")
        .and_then(Value::as_str)
        == Some(MICROCOMPACT_CLEARED_MESSAGE)
}

fn function_response_name(part: &Value) -> Option<&str> {
    part.get("functionResponse")?.get("name")?.as_str()
}

fn is_compactable_tool(name: &str) -> bool {
    COMPACTABLE_TOOLS.contains(&name)
}

fn is_file_path_tool(name: &str) -> bool {
    FILE_PATH_TOOLS.contains(&name)
}

fn get_tool_output_chars(part: Option<&Value>) -> f64 {
    let Some(part) = part else {
        return 0.0;
    };
    let Some(name) = function_response_name(part) else {
        return 0.0;
    };
    if !is_compactable_tool(name) || is_error_response(part) || is_already_cleared(part) {
        return 0.0;
    }
    part.pointer("/functionResponse/response/output")
        .and_then(Value::as_str)
        .map(|output| output.encode_utf16().count() as f64)
        .unwrap_or(0.0)
}

fn estimate_part_tokens(part: &Value) -> f64 {
    if let Some(response) = part
        .get("functionResponse")
        .and_then(|function_response| function_response.get("response"))
        .filter(|response| js_truthy(response))
    {
        let output_tokens = response
            .get("output")
            .and_then(Value::as_str)
            .map(|output| (output.encode_utf16().count() as f64 / 4.0).ceil())
            .unwrap_or(0.0);
        let nested_media_tokens = part
            .get("functionResponse")
            .and_then(|function_response| function_response.get("parts"))
            .and_then(Value::as_array)
            .map(|parts| {
                parts
                    .iter()
                    .filter(|inner| has_top_level_media(inner))
                    .count() as f64
                    * MEDIA_PART_TOKEN_ESTIMATE
            })
            .unwrap_or(0.0);
        return output_tokens + nested_media_tokens;
    }
    if has_top_level_media(part) {
        MEDIA_PART_TOKEN_ESTIMATE
    } else {
        0.0
    }
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

fn get_tool_results_total_chars_threshold(settings: &ClearContextOnIdleSettings) -> f64 {
    if let Some(threshold) = settings.tool_results_total_chars_threshold {
        return threshold;
    }
    if settings.tool_results_threshold_minutes.unwrap_or(0.0) < 0.0 {
        -1.0
    } else {
        DEFAULT_TOOL_RESULTS_TOTAL_CHARS_THRESHOLD
    }
}

fn build_keep_refs(refs: Vec<PartRef>, keep_recent: usize) -> HashSet<PartKey> {
    refs.iter()
        .rev()
        .take(keep_recent)
        .map(PartRef::key)
        .collect()
}

fn build_clear_map(clear_refs: &[PartRef]) -> HashMap<usize, HashMap<usize, PartKind>> {
    let mut clear_map: HashMap<usize, HashMap<usize, PartKind>> = HashMap::new();
    for reference in clear_refs {
        clear_map
            .entry(reference.content_index)
            .or_default()
            .insert(reference.part_index, reference.kind);
    }
    clear_map
}

fn get_part<'a>(history: HistoryView<'a>, reference: &PartRef) -> Option<&'a Value> {
    history
        .get(reference.content_index)?
        .get("parts")?
        .as_array()?
        .get(reference.part_index)
}

fn build_call_id_to_file_path(history: HistoryView<'_>) -> HashMap<String, Vec<String>> {
    let mut map: HashMap<String, Vec<String>> = HashMap::new();
    for (_, content) in history.iter() {
        if content.get("role").and_then(Value::as_str) != Some("model") {
            continue;
        }
        let Some(parts) = content.get("parts").and_then(Value::as_array) else {
            continue;
        };
        for part in parts {
            let Some(call) = part.get("functionCall") else {
                continue;
            };
            let Some(id) = call
                .get("id")
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty())
            else {
                continue;
            };
            let Some(name) = call
                .get("name")
                .and_then(Value::as_str)
                .filter(|name| !name.is_empty())
            else {
                continue;
            };
            if !is_file_path_tool(name) {
                continue;
            }
            let Some(file_path) = call
                .get("args")
                .and_then(|args| args.get("file_path"))
                .and_then(Value::as_str)
                .filter(|file_path| !file_path.is_empty())
            else {
                continue;
            };
            map.entry(id.to_owned())
                .or_default()
                .push(file_path.to_owned());
        }
    }
    map
}

fn get_file_paths_for_response(
    part: Option<&Value>,
    call_id_to_file_path: &HashMap<String, Vec<String>>,
) -> Option<Vec<String>> {
    let response = part?.get("functionResponse")?;
    let id = response
        .get("id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())?;
    let name = response
        .get("name")
        .and_then(Value::as_str)
        .filter(|name| !name.is_empty())?;
    if !is_file_path_tool(name) {
        return None;
    }
    let paths = call_id_to_file_path.get(id)?;
    if paths.is_empty() {
        return None;
    }
    let mut unique = Vec::new();
    let mut seen = HashSet::new();
    for path in paths {
        if seen.insert(path.as_str()) {
            unique.push(path.clone());
        }
    }
    Some(unique)
}

fn build_preserved_read_refs(
    history: HistoryView<'_>,
    refs: &[PartRef],
    preserve_read_file_result: &dyn Fn(&str) -> bool,
) -> HashSet<PartKey> {
    let call_id_to_file_path = build_call_id_to_file_path(history);
    refs.iter()
        .filter_map(|reference| {
            let part = get_part(history, reference)?;
            if function_response_name(part) != Some("read_file") || is_error_response(part) {
                return None;
            }
            let paths = get_file_paths_for_response(Some(part), &call_id_to_file_path)?;
            (!paths.is_empty() && paths.iter().all(|path| preserve_read_file_result(path)))
                .then(|| reference.key())
        })
        .collect()
}

fn build_kept_file_paths(
    history: HistoryView<'_>,
    refs: &[PartRef],
    keep_refs: &HashSet<PartKey>,
    call_id_to_file_path: &HashMap<String, Vec<String>>,
) -> HashSet<String> {
    let mut kept = HashSet::new();
    for reference in refs {
        if !keep_refs.contains(&reference.key()) {
            continue;
        }
        let Some(part) = get_part(history, reference) else {
            continue;
        };
        if is_error_response(part) || is_already_cleared(part) {
            continue;
        }
        if function_response_name(part) != Some("write_file") {
            continue;
        }
        if let Some(paths) = get_file_paths_for_response(Some(part), call_id_to_file_path) {
            if paths.len() == 1 {
                kept.insert(paths[0].clone());
            }
        }
    }
    kept
}

#[derive(Default)]
struct OrderedPaths {
    ordered: Vec<String>,
    seen: HashSet<String>,
}

impl OrderedPaths {
    fn insert(&mut self, path: String) {
        if self.seen.insert(path.clone()) {
            self.ordered.push(path);
        }
    }
}

fn clear_tool_result(part: &Value) -> Value {
    let mut function_response = Map::new();
    if let Some(source) = part.get("functionResponse").and_then(Value::as_object) {
        for (key, value) in source {
            // `parts` may contain a large image/document payload, while the
            // old response is replaced below. Do not deep-clone either one.
            if key != "parts" && key != "response" {
                function_response.insert(key.clone(), value.clone());
            }
        }
    }
    function_response.insert(
        "response".to_owned(),
        json!({"output": MICROCOMPACT_CLEARED_MESSAGE}),
    );
    json!({"functionResponse": Value::Object(function_response)})
}

fn strip_nested_media_from_part(part: &Value) -> Value {
    let Some(function_response) = part.get("functionResponse").and_then(Value::as_object) else {
        return part.clone();
    };
    let stripped = function_response
        .iter()
        .filter(|(key, _)| key.as_str() != "parts")
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    json!({"functionResponse": Value::Object(stripped)})
}

fn media_mime_type(part: &Value) -> Option<&str> {
    part.get("inlineData")
        .and_then(|data| data.get("mimeType"))
        .filter(|mime| !mime.is_null())
        .and_then(Value::as_str)
        .or_else(|| {
            part.get("fileData")
                .and_then(|data| data.get("mimeType"))
                .filter(|mime| !mime.is_null())
                .and_then(Value::as_str)
        })
}

fn plan_size_based_clearing(
    history: HistoryView<'_>,
    settings: &ClearContextOnIdleSettings,
    keep_recent: usize,
    preserve_read_file_result: Option<&dyn Fn(&str) -> bool>,
) -> Option<SizeClearPlan> {
    let threshold = get_tool_results_total_chars_threshold(settings);
    if !threshold.is_finite() || threshold < 0.0 {
        return None;
    }
    let low_watermark = (threshold / 2.0).floor();
    let collected = collect_compactable_part_refs(history, None);
    let tool = collected.tool;
    let mut chars_by_ref = HashMap::new();
    let mut total_chars = 0.0;
    let mut pending_chars = 0.0;
    for reference in &tool {
        let chars = get_tool_output_chars(get_part(history, reference));
        if chars <= 0.0 {
            continue;
        }
        chars_by_ref.insert(reference.key(), chars);
        total_chars += chars;
        if reference.content_index >= history.committed_len() {
            pending_chars += chars;
        }
    }
    if total_chars <= threshold {
        return None;
    }

    let preserved_tool_refs = preserve_read_file_result.map_or_else(HashSet::new, |callback| {
        build_preserved_read_refs(history, &tool, callback)
    });
    let compactable_tool_refs = tool
        .into_iter()
        .filter(|reference| !preserved_tool_refs.contains(&reference.key()))
        .collect::<Vec<_>>();
    // The pending tail is counted toward total size but cannot consume the
    // keepRecent slots, since it is neither committed nor eligible to clear.
    let keepable_committed_refs = compactable_tool_refs
        .iter()
        .filter(|reference| {
            reference.content_index < history.committed_len()
                && chars_by_ref.contains_key(&reference.key())
        })
        .cloned()
        .collect::<Vec<_>>();
    let keep_tool_refs = build_keep_refs(keepable_committed_refs, keep_recent);

    let mut clear_refs = Vec::new();
    let mut remaining_chars = total_chars;
    for reference in &compactable_tool_refs {
        if remaining_chars <= low_watermark {
            break;
        }
        let chars = chars_by_ref
            .get(&reference.key())
            .copied()
            .unwrap_or_default();
        if chars <= 0.0
            || reference.content_index >= history.committed_len()
            || keep_tool_refs.contains(&reference.key())
        {
            continue;
        }
        clear_refs.push(reference.clone());
        remaining_chars -= chars;
    }

    Some(SizeClearPlan {
        clear_refs,
        tool_refs: compactable_tool_refs,
        keep_tool_refs,
        tool_result_chars_before: total_chars,
        tool_result_chars_after: remaining_chars - pending_chars,
        pending_tool_result_chars: pending_chars,
        threshold,
        low_watermark,
    })
}

struct SizeClearPlan {
    clear_refs: Vec<PartRef>,
    tool_refs: Vec<PartRef>,
    keep_tool_refs: HashSet<PartKey>,
    tool_result_chars_before: f64,
    tool_result_chars_after: f64,
    pending_tool_result_chars: f64,
    threshold: f64,
    low_watermark: f64,
}

fn resolve_keep_recent(env_value: Option<&str>, settings_value: Option<f64>) -> usize {
    fn normalize(value: f64) -> Option<usize> {
        if !value.is_finite() || value.fract() != 0.0 || value.abs() > MAX_SAFE_INTEGER {
            return None;
        }
        Some(value.max(1.0) as usize)
    }

    if let Some(env_value) = env_value {
        let trimmed = env_value.trim_matches(is_ecmascript_whitespace);
        let integer_syntax = if let Some(digits) = trimmed.strip_prefix('-') {
            !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
        } else {
            !trimmed.is_empty() && trimmed.bytes().all(|byte| byte.is_ascii_digit())
        };
        if integer_syntax {
            if let Some(keep) = trimmed.parse::<f64>().ok().and_then(normalize) {
                return keep;
            }
        }
    }
    settings_value
        .and_then(normalize)
        .unwrap_or(DEFAULT_TOOL_RESULTS_NUM_TO_KEEP)
}

fn is_ecmascript_whitespace(ch: char) -> bool {
    matches!(
        ch,
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

fn current_time_millis() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn content(role: &str, parts: Vec<Value>) -> Value {
        json!({"role": role, "parts": parts})
    }

    fn tool_call(id: &str, name: &str, file_path: Option<&str>) -> Value {
        let mut call = json!({"id": id, "name": name, "args": {}});
        if let Some(file_path) = file_path {
            call["args"]["file_path"] = json!(file_path);
        }
        json!({"role":"model", "parts":[{"functionCall":call}]})
    }

    fn tool_result(id: &str, name: &str, output: &str) -> Value {
        json!({
            "role":"user",
            "parts":[{"functionResponse":{
                "id":id,
                "name":name,
                "response":{"output":output}
            }}]
        })
    }

    fn settings() -> ClearContextOnIdleSettings {
        ClearContextOnIdleSettings {
            tool_results_threshold_minutes: Some(5.0),
            tool_results_num_to_keep: Some(1.0),
            tool_results_total_chars_threshold: Some(-1.0),
        }
    }

    fn force_options() -> MicrocompactOptions<'static> {
        MicrocompactOptions {
            force: true,
            ..MicrocompactOptions::default()
        }
    }

    #[test]
    fn time_trigger_respects_disabled_missing_and_exact_boundary_cases() {
        let mut config = settings();
        assert_eq!(evaluate_time_based_trigger(None, &config, 900_000.0), None);
        assert_eq!(
            evaluate_time_based_trigger(Some(600_000.0), &config, 899_999.0),
            None
        );
        assert_eq!(
            evaluate_time_based_trigger(Some(600_000.0), &config, 900_000.0),
            Some(TimeBasedTrigger { gap_ms: 300_000.0 })
        );
        config.tool_results_threshold_minutes = Some(-1.0);
        assert_eq!(
            evaluate_time_based_trigger(Some(0.0), &config, 3_600_000.0),
            None
        );
        assert_eq!(
            evaluate_time_based_trigger(Some(f64::NAN), &settings(), 3_600_000.0),
            None
        );
    }

    #[test]
    fn forced_pass_clears_old_tool_output_and_keeps_recent_part() {
        let history = vec![
            tool_call("old-call", "read_file", None),
            tool_result("old-call", "read_file", &"old contents ".repeat(20)),
            tool_call("new-call", "read_file", None),
            tool_result("new-call", "read_file", "recent contents"),
        ];
        let result = microcompact_history_with_env(
            &history,
            Some(0.0),
            &settings(),
            force_options(),
            None,
            10_000.0,
        );
        let meta = result.meta.as_ref().unwrap();
        assert_eq!(meta.trigger_reason, MicrocompactTriggerReason::Force);
        assert_eq!(meta.tools_cleared, 1);
        assert_eq!(meta.tools_kept, 1);
        assert_eq!(
            result.history[1]["parts"][0]["functionResponse"]["response"]["output"],
            MICROCOMPACT_CLEARED_MESSAGE
        );
        assert_eq!(
            result.history[3]["parts"][0]["functionResponse"]["response"]["output"],
            "recent contents"
        );
        assert_eq!(meta.tokens_saved, 65.0);
    }

    #[test]
    fn size_trigger_counts_pending_but_only_protects_committed_recent_results() {
        let history = vec![
            tool_result("old", "run_shell_command", &"a".repeat(30)),
            tool_result("recent", "skill", &"b".repeat(30)),
        ];
        let pending = vec![tool_result("pending", "skill", &"c".repeat(30))];
        let config = ClearContextOnIdleSettings {
            tool_results_threshold_minutes: Some(60.0),
            tool_results_num_to_keep: Some(1.0),
            tool_results_total_chars_threshold: Some(60.0),
        };
        let options = MicrocompactOptions {
            size_only: true,
            pending_content: Some(&pending),
            ..MicrocompactOptions::default()
        };
        let result = microcompact_history_with_env(&history, None, &config, options, None, 0.0);
        let meta = result.meta.as_ref().unwrap();
        assert_eq!(meta.trigger_reason, MicrocompactTriggerReason::Size);
        assert_eq!(meta.tool_result_chars_before, Some(90.0));
        assert_eq!(meta.pending_tool_result_chars, Some(30.0));
        assert_eq!(meta.tool_result_chars_after, Some(30.0));
        assert_eq!(meta.tool_results_low_watermark, Some(30.0));
        assert_eq!(meta.tools_cleared, 1);
        assert_eq!(meta.tools_kept, 1);
        assert_eq!(
            result.history[0]["parts"][0]["functionResponse"]["response"]["output"],
            MICROCOMPACT_CLEARED_MESSAGE
        );
        assert_eq!(
            result.history[1]["parts"][0]["functionResponse"]["response"]["output"],
            "b".repeat(30)
        );
        assert_eq!(
            pending[0]["parts"][0]["functionResponse"]["response"]["output"],
            "c".repeat(30)
        );
    }

    #[test]
    fn media_and_nested_media_keep_context_and_have_separate_recent_budgets() {
        let nested_old = json!({"functionResponse":{
            "name":"custom_tool",
            "response":{"output":"preserve this text"},
            "parts":[{"inlineData":{"mimeType":"image/png", "data":"old"}}]
        }});
        let nested_new = json!({"functionResponse":{
            "name":"custom_tool",
            "response":{"output":"keep recent text"},
            "parts":[{"fileData":{"mimeType":"application/pdf", "fileUri":"new"}}]
        }});
        let history = vec![content(
            "user",
            vec![
                nested_old,
                json!({"inlineData":{"mimeType":"image/png", "data":"old"}}),
                nested_new,
                json!({"inlineData":{"mimeType":"image/jpeg", "data":"new"}}),
            ],
        )];
        let result =
            microcompact_history_with_env(&history, None, &settings(), force_options(), None, 0.0);
        let meta = result.meta.unwrap();
        assert_eq!(meta.media_cleared, 2);
        assert_eq!(meta.media_kept, 1);
        assert_eq!(
            result.history[0]["parts"][0]["functionResponse"]["response"]["output"],
            "preserve this text"
        );
        assert!(
            result.history[0]["parts"][0]["functionResponse"]
                .get("parts")
                .is_none()
        );
        assert_eq!(
            result.history[0]["parts"][1]["text"],
            "[Old inline media cleared: image/png]"
        );
        assert_eq!(
            result.history[0]["parts"][2]["functionResponse"]
                .get("parts")
                .unwrap()[0]["fileData"]["fileUri"],
            "new"
        );
        assert!(result.history[0]["parts"][3].get("inlineData").is_some());
    }

    #[test]
    fn cleared_tool_result_drops_large_replaced_fields_but_keeps_metadata() {
        let old_output = "large output ".repeat(20_000);
        let nested_payload = "base64 payload ".repeat(20_000);
        let old_result = json!({"functionResponse":{
            "id":"old",
            "name":"read_file",
            "providerMetadata":{"source":"cached"},
            "response":{"output":old_output},
            "parts":[{"inlineData":{"mimeType":"image/png", "data":nested_payload}}]
        }});
        let history = vec![
            content("user", vec![old_result]),
            tool_result("new", "read_file", "recent"),
        ];

        let result =
            microcompact_history_with_env(&history, None, &settings(), force_options(), None, 0.0);
        let cleared = &result.history[0]["parts"][0]["functionResponse"];
        assert_eq!(cleared["providerMetadata"]["source"], "cached");
        assert_eq!(
            cleared["response"],
            json!({"output": MICROCOMPACT_CLEARED_MESSAGE})
        );
        assert!(cleared.get("parts").is_none());
        assert_eq!(
            history[0]["parts"][0]["functionResponse"]["response"]["output"],
            old_output
        );
        assert_eq!(
            history[0]["parts"][0]["functionResponse"]["parts"][0]["inlineData"]["data"],
            nested_payload
        );
    }

    #[test]
    fn errors_placeholders_and_noncompactable_outputs_are_preserved() {
        let history = vec![content(
            "user",
            vec![
                json!({"functionResponse":{"name":"read_file", "response":{"error":"denied"}}}),
                json!({"functionResponse":{"name":"read_file", "response":{"output":MICROCOMPACT_CLEARED_MESSAGE}}}),
                json!({"functionResponse":{"name":"ask_user_question", "response":{"output":"do not clear"}}}),
            ],
        )];
        let result =
            microcompact_history_with_env(&history, None, &settings(), force_options(), None, 0.0);
        assert!(result.meta.is_none());
        assert!(matches!(&result.history, Cow::Borrowed(_)));
        assert_eq!(result.history.as_ref(), history.as_slice());
    }

    #[test]
    fn defensive_placeholder_refs_leave_history_borrowed_when_no_clear_occurs() {
        let history = vec![
            content(
                "user",
                vec![json!({"functionResponse":{
                    "name":"read_file",
                    "response":{"output": MICROCOMPACT_CLEARED_MESSAGE},
                    "parts":[{"inlineData":{"mimeType":"image/png", "data":"already removed"}}]
                }})],
            ),
            tool_result("recent", "read_file", "new result"),
        ];

        let result =
            microcompact_history_with_env(&history, None, &settings(), force_options(), None, 0.0);

        assert!(result.meta.is_none());
        assert!(matches!(&result.history, Cow::Borrowed(_)));
        assert_eq!(result.history.as_ref(), history.as_slice());
    }

    #[test]
    fn size_threshold_is_strict_and_clearing_targets_half_watermark() {
        let exact_history = vec![tool_result("a", "read_file", &"x".repeat(20))];
        let config = ClearContextOnIdleSettings {
            tool_results_threshold_minutes: Some(60.0),
            tool_results_num_to_keep: Some(1.0),
            tool_results_total_chars_threshold: Some(20.0),
        };
        let options = MicrocompactOptions {
            size_only: true,
            ..MicrocompactOptions::default()
        };
        let exact =
            microcompact_history_with_env(&exact_history, None, &config, options, None, 0.0);
        assert!(exact.meta.is_none());

        let history = vec![
            tool_result("a", "read_file", &"x".repeat(20)),
            tool_result("b", "read_file", &"y".repeat(20)),
        ];
        let over = microcompact_history_with_env(
            &history,
            None,
            &config,
            MicrocompactOptions {
                size_only: true,
                ..MicrocompactOptions::default()
            },
            None,
            0.0,
        );
        assert_eq!(
            over.meta.as_ref().unwrap().tool_results_low_watermark,
            Some(10.0)
        );
        assert_eq!(over.meta.as_ref().unwrap().tools_cleared, 1);
        assert_eq!(
            over.history[0]["parts"][0]["functionResponse"]["response"]["output"],
            MICROCOMPACT_CLEARED_MESSAGE
        );
    }

    #[test]
    fn file_eviction_is_deduplicated_and_kept_write_result_protects_residency() {
        let path = "/workspace/a.txt";
        let history = vec![
            tool_call("read-1", "read_file", Some(path)),
            tool_result("read-1", "read_file", &"file body ".repeat(20)),
            tool_call("write-1", "write_file", Some(path)),
            tool_result("write-1", "write_file", "written"),
        ];
        let result =
            microcompact_history_with_env(&history, None, &settings(), force_options(), None, 0.0);
        let meta = result.meta.unwrap();
        assert_eq!(meta.tools_cleared, 1);
        assert_eq!(meta.tools_kept, 1);
        assert!(meta.evicted_read_paths.is_empty());
        assert_eq!(meta.unresolved_evicted_reads, 0);
    }

    #[test]
    fn path_preserver_keeps_matching_read_results_and_unmapped_paths_are_unresolved() {
        let path = "/workspace/managed.txt";
        let history = vec![
            tool_call("read", "read_file", Some(path)),
            tool_result("read", "read_file", &"managed body ".repeat(10)),
            tool_call("old", "read_file", None),
            tool_result("old", "read_file", &"ordinary body ".repeat(10)),
            tool_result("latest", "read_file", "keep this latest result"),
        ];
        let preserve = |candidate: &str| candidate == path;
        let options = MicrocompactOptions {
            force: true,
            preserve_read_file_result: Some(&preserve),
            ..MicrocompactOptions::default()
        };
        let result = microcompact_history_with_env(&history, None, &settings(), options, None, 0.0);
        let meta = result.meta.unwrap();
        assert_eq!(meta.tools_cleared, 1);
        assert_eq!(meta.unresolved_evicted_reads, 1);
        assert_eq!(
            result.history[1]["parts"][0]["functionResponse"]["response"]["output"],
            "managed body ".repeat(10)
        );
    }

    #[test]
    fn keep_recent_environment_requires_safe_integer_syntax() {
        assert_eq!(resolve_keep_recent(Some("0"), Some(7.0)), 1);
        assert_eq!(resolve_keep_recent(Some(" 3 "), Some(7.0)), 3);
        assert_eq!(resolve_keep_recent(Some("3.5"), Some(7.0)), 7);
        assert_eq!(resolve_keep_recent(Some("9007199254740992"), Some(7.0)), 7);
        assert_eq!(resolve_keep_recent(None, Some(2.5)), 5);
    }

    #[test]
    fn keep_recent_environment_trims_ecmascript_bom_whitespace() {
        assert_eq!(resolve_keep_recent(Some("\u{feff}2\u{feff}"), Some(3.0)), 2);
        assert_eq!(resolve_keep_recent(Some("\u{0085}2\u{0085}"), Some(3.0)), 3);
    }

    #[test]
    fn mime_placeholder_uses_existing_sanitizer() {
        let history = vec![
            content(
                "user",
                vec![json!({"inlineData":{
                    "mimeType":"image/png]\n\n<bad>",
                    "data":"bytes"
                }})],
            ),
            content(
                "user",
                vec![json!({"inlineData":{
                    "mimeType":"image/jpeg",
                    "data":"newer bytes"
                }})],
            ),
        ];
        let result =
            microcompact_history_with_env(&history, None, &settings(), force_options(), None, 0.0);
        let meta = result.meta.unwrap();
        assert_eq!(meta.media_cleared, 1);
        assert_eq!(meta.tokens_saved, MEDIA_PART_TOKEN_ESTIMATE);
        assert_eq!(
            result.history[0]["parts"][0]["text"],
            "[Old inline media cleared: image/png <bad>]"
        );
    }
}
