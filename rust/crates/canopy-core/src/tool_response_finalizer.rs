//! Batch budgeting for model-visible tool responses.
//!
//! This ports the text-allocation and preview rules from
//! `utils/tool-response-finalizer.ts`. Persistence is injected so the caller
//! can apply Canopy's session byte budget and storage policy.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use serde_json::Value;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

use crate::utils::json_string_byte_projection::json_string_json_byte_length;

const ENTER_PLAN_MODE_TOOL: &str = "enter_plan_mode";
const MAX_TOOL_RESULT_FILE_SIZE_BYTES: usize = 50 * 1024 * 1024;
pub const MAX_SESSION_TOOL_RESULT_BYTES: u64 = 500 * 1024 * 1024;

/// Provider-neutral tool result with optional model-visible media parts.
/// `output` remains the text displayed by existing tool hosts; `parts` carries
/// inline media through the Gemini-shaped transcript without flattening it.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ToolExecutionOutput {
    pub output: String,
    pub parts: Vec<Value>,
    pub display: Option<Value>,
    /// Structured host metadata for session artifacts. This is kept out of
    /// model-visible response budgeting and copied to the transcript recorder.
    pub artifacts: Vec<Value>,
    /// Files discovered by the tool result, available to post-tool hooks.
    /// These paths are host metadata and are not included in model output
    /// budgeting or serialization.
    pub result_file_paths: Vec<String>,
}

impl ToolExecutionOutput {
    pub fn text(output: impl Into<String>) -> Self {
        Self {
            output: output.into(),
            parts: Vec::new(),
            display: None,
            artifacts: Vec::new(),
            result_file_paths: Vec::new(),
        }
    }

    pub fn with_parts(output: impl Into<String>, parts: Vec<Value>) -> Self {
        Self {
            output: output.into(),
            parts,
            display: None,
            artifacts: Vec::new(),
            result_file_paths: Vec::new(),
        }
    }

    pub fn with_display(output: impl Into<String>, display: Value) -> Self {
        Self {
            output: output.into(),
            parts: Vec::new(),
            display: Some(display),
            artifacts: Vec::new(),
            result_file_paths: Vec::new(),
        }
    }

    /// Include serialized media parts in runtime output limits. The tool
    /// executor itself caps inline file data before base64 encoding. Count
    /// JSON bytes through a streaming writer so an oversized response does
    /// not require a second allocation as large as the original payload.
    pub fn estimated_bytes(&self) -> usize {
        let parts_bytes = self.parts.iter().map(serialized_json_byte_length).fold(
            json_string_json_byte_length(&self.output),
            usize::saturating_add,
        );
        self.display.as_ref().map_or(parts_bytes, |display| {
            parts_bytes.saturating_add(serialized_json_byte_length(display))
        })
    }
}

#[derive(Default)]
struct ByteCounter(usize);

impl Write for ByteCounter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 = self.0.saturating_add(bytes.len());
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn serialized_json_byte_length(value: &Value) -> usize {
    let mut counter = ByteCounter::default();
    serde_json::to_writer(&mut counter, value).map_or(usize::MAX, |_| counter.0)
}

#[derive(Clone, Debug, PartialEq)]
pub struct ToolResponseBudgetEntry {
    pub call_id: String,
    pub tool_name: String,
    pub response_parts: Vec<Value>,
    pub persisted_output_files: Option<Vec<String>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TextField {
    Text,
    Output,
    Error,
}

#[derive(Clone, Debug)]
struct TextSlot {
    entry_index: usize,
    part_index: usize,
    field: TextField,
    protected_prefix_bytes: Option<usize>,
}

/// An implementation is responsible for its own atomic-write and session
/// storage budgets. A failed spill is tolerated because the response is still
/// bounded before it returns to the model.
pub trait ToolOutputStore {
    fn persist<'a>(
        &'a mut self,
        call_id: &'a str,
        tool_name: &'a str,
        content: &'a str,
    ) -> impl Future<Output = Result<Option<String>, String>> + 'a;
}

/// Atomic, owner-only artifact writer for the current project's tool results.
pub struct FileToolOutputStore {
    output_dir: PathBuf,
    bytes_written: u64,
    session_budget: u64,
}

impl FileToolOutputStore {
    pub fn new(output_dir: impl Into<PathBuf>) -> Self {
        Self::with_session_budget(output_dir, MAX_SESSION_TOOL_RESULT_BYTES)
    }

    pub fn with_session_budget(output_dir: impl Into<PathBuf>, session_budget: u64) -> Self {
        Self {
            output_dir: output_dir.into(),
            bytes_written: 0,
            session_budget,
        }
    }

    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }
}

impl ToolOutputStore for FileToolOutputStore {
    async fn persist(
        &mut self,
        call_id: &str,
        _tool_name: &str,
        content: &str,
    ) -> Result<Option<String>, String> {
        let byte_size = content.len() as u64;
        if byte_size > MAX_TOOL_RESULT_FILE_SIZE_BYTES as u64
            || self.bytes_written.saturating_add(byte_size) > self.session_budget
        {
            return Ok(None);
        }
        let Some(call_id) = normalize_tool_result_call_id(call_id) else {
            return Ok(None);
        };
        self.bytes_written = self.bytes_written.saturating_add(byte_size);

        let output_path = self.output_dir.join(format!("{call_id}.txt"));
        match atomic_write_tool_output(&output_path, content).await {
            Ok(()) => Ok(Some(output_path.to_string_lossy().into_owned())),
            Err(error) => {
                self.bytes_written = self.bytes_written.saturating_sub(byte_size);
                Err(error.to_string())
            }
        }
    }
}

async fn atomic_write_tool_output(path: &Path, content: &str) -> io::Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    tokio::fs::create_dir_all(parent).await?;
    let file_name = path.file_name().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "tool output path has no filename",
        )
    })?;
    let mut temporary = None;
    for _ in 0..3 {
        let candidate = parent.join(format!(
            ".{}.tmp-{}",
            file_name.to_string_lossy(),
            Uuid::new_v4().simple()
        ));
        let mut options = tokio::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
        }
        match options.open(&candidate).await {
            Ok(file) => {
                temporary = Some((candidate, file));
                break;
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
    let (temporary_path, mut temporary_file) = temporary.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::AlreadyExists,
            "could not allocate tool output temp file",
        )
    })?;
    let result = async {
        temporary_file.write_all(content.as_bytes()).await?;
        temporary_file.sync_all().await?;
        drop(temporary_file);
        tokio::fs::rename(&temporary_path, path).await?;
        #[cfg(unix)]
        tokio::fs::File::open(parent).await?.sync_all().await?;
        Ok(())
    }
    .await;
    if result.is_err() {
        let _ = tokio::fs::remove_file(&temporary_path).await;
    }
    result
}

/// Count top-level text and function-response output/error in UTF-16 code
/// units, matching JavaScript's `String.length` budget contract.
pub fn tool_response_text_length(parts: &[Value]) -> usize {
    parts
        .iter()
        .flat_map(|part| {
            [
                part.get("text").and_then(Value::as_str),
                part.pointer("/functionResponse/response/output")
                    .and_then(Value::as_str),
                part.pointer("/functionResponse/response/error")
                    .and_then(Value::as_str),
            ]
        })
        .flatten()
        .map(utf16_len)
        .fold(0usize, usize::saturating_add)
}

/// Apply the pure combined function-response budget without spilling output.
/// `plan_mode_reminders` contains the two strings emitted by Canopy's plan-mode
/// prompt helper; their lifecycle prefix is kept intact while appended hook
/// context remains subject to the shared budget.
pub fn enforce_function_response_budget(
    entries: &mut [ToolResponseBudgetEntry],
    budget: f64,
    plan_mode_reminders: &[String],
) -> bool {
    if !budget.is_finite() || budget <= 0.0 {
        return false;
    }
    let slots = collect_text_slots(entries, false, true, plan_mode_reminders);
    let total = text_total(entries, &slots);
    if total as f64 <= budget {
        return false;
    }
    let allocations = allocate_text_budget(
        &slot_lengths(entries, &slots),
        budget.floor().min(usize::MAX as f64) as usize,
    );
    replace_text_slots(entries, &slots, &allocations);
    true
}

/// Finalize a batch in source order. Oversized results are spilled once per
/// entry, then replaced with bounded head/tail previews and artifact pointers.
pub async fn finalize_tool_responses<S: ToolOutputStore>(
    entries: &mut [ToolResponseBudgetEntry],
    budget: Option<f64>,
    plan_mode_reminders: &[String],
    store: &mut S,
) {
    let budget = budget.unwrap_or(f64::INFINITY);
    if !budget.is_finite() || budget <= 0.0 {
        return;
    }
    let slots = collect_text_slots(entries, true, true, plan_mode_reminders);
    let total = text_total(entries, &slots);
    if total as f64 <= budget {
        return;
    }
    let allocations = allocate_text_budget(
        &slot_lengths(entries, &slots),
        budget.floor().min(usize::MAX as f64) as usize,
    );

    let mut entries_to_persist = BTreeSet::new();
    for (slot, allocation) in slots.iter().zip(&allocations) {
        if text_at(entries, slot).is_some_and(|text| utf16_len(text) > *allocation) {
            entries_to_persist.insert(slot.entry_index);
        }
    }

    let normalized_ids: Vec<Option<String>> = entries
        .iter()
        .map(|entry| normalize_tool_result_call_id(&entry.call_id))
        .collect();
    let mut id_counts = BTreeMap::<String, usize>::new();
    for id in normalized_ids.iter().flatten() {
        *id_counts.entry(id.clone()).or_default() += 1;
    }
    let reserved_ids: BTreeSet<String> = normalized_ids.iter().flatten().cloned().collect();
    let mut used_ids = BTreeSet::new();

    for entry_index in &entries_to_persist {
        if entries[*entry_index].persisted_output_files.is_some() {
            continue;
        }
        let slots_for_entry: Vec<&TextSlot> = slots
            .iter()
            .filter(|slot| slot.entry_index == *entry_index)
            .collect();
        let content_size = slots_for_entry
            .iter()
            .filter_map(|slot| text_at(entries, slot))
            .map(str::len)
            .fold(0usize, usize::saturating_add)
            .saturating_add(slots_for_entry.len().saturating_sub(1).saturating_mul(2));
        if content_size > MAX_TOOL_RESULT_FILE_SIZE_BYTES {
            entries[*entry_index].persisted_output_files = Some(Vec::new());
            continue;
        }
        let mut content = String::with_capacity(content_size);
        let mut has_content = false;
        for slot in slots_for_entry {
            let Some(text) = text_at(entries, slot) else {
                continue;
            };
            if has_content {
                content.push_str("\n\n");
            }
            content.push_str(text);
            has_content = true;
        }

        let persistence_id = persistence_call_id(
            &entries[*entry_index].call_id,
            normalized_ids[*entry_index].as_deref(),
            id_counts.get(normalized_ids[*entry_index].as_deref().unwrap_or_default()),
            &reserved_ids,
            &mut used_ids,
        );
        let output_file = store
            .persist(&persistence_id, &entries[*entry_index].tool_name, &content)
            .await
            .ok()
            .flatten();
        entries[*entry_index].persisted_output_files = Some(output_file.into_iter().collect());
    }

    replace_text_slots(entries, &slots, &allocations);
}

fn collect_text_slots(
    entries: &[ToolResponseBudgetEntry],
    include_top_level_text: bool,
    exclude_budget_exempt_output: bool,
    plan_mode_reminders: &[String],
) -> Vec<TextSlot> {
    let mut slots = Vec::new();
    for (entry_index, entry) in entries.iter().enumerate() {
        for (part_index, part) in entry.response_parts.iter().enumerate() {
            if include_top_level_text && part.get("text").and_then(Value::as_str).is_some() {
                slots.push(TextSlot {
                    entry_index,
                    part_index,
                    field: TextField::Text,
                    protected_prefix_bytes: None,
                });
            }
            let Some(response) = part.pointer("/functionResponse/response") else {
                continue;
            };
            if let Some(output) = response.get("output").and_then(Value::as_str) {
                let protected_prefix = if exclude_budget_exempt_output {
                    plan_mode_lifecycle_prefix(
                        part.pointer("/functionResponse/name")
                            .and_then(Value::as_str)
                            .or(Some(&entry.tool_name)),
                        output,
                        plan_mode_reminders,
                    )
                } else {
                    None
                };
                if protected_prefix.is_none_or(|prefix| prefix < output.len()) {
                    slots.push(TextSlot {
                        entry_index,
                        part_index,
                        field: TextField::Output,
                        protected_prefix_bytes: protected_prefix,
                    });
                }
            }
            if response.get("error").and_then(Value::as_str).is_some() {
                slots.push(TextSlot {
                    entry_index,
                    part_index,
                    field: TextField::Error,
                    protected_prefix_bytes: None,
                });
            }
        }
    }
    slots
}

fn text_at<'a>(entries: &'a [ToolResponseBudgetEntry], slot: &TextSlot) -> Option<&'a str> {
    let part = entries
        .get(slot.entry_index)?
        .response_parts
        .get(slot.part_index)?;
    let text = match slot.field {
        TextField::Text => part.get("text")?.as_str()?,
        TextField::Output => part
            .pointer("/functionResponse/response/output")?
            .as_str()?,
        TextField::Error => part.pointer("/functionResponse/response/error")?.as_str()?,
    };
    match slot.protected_prefix_bytes {
        Some(prefix_bytes) => text.get(prefix_bytes..),
        None => Some(text),
    }
}

fn text_total(entries: &[ToolResponseBudgetEntry], slots: &[TextSlot]) -> usize {
    slots
        .iter()
        .filter_map(|slot| text_at(entries, slot))
        .map(utf16_len)
        .fold(0usize, usize::saturating_add)
}

fn slot_lengths(entries: &[ToolResponseBudgetEntry], slots: &[TextSlot]) -> Vec<usize> {
    slots
        .iter()
        .map(|slot| text_at(entries, slot).map(utf16_len).unwrap_or_default())
        .collect()
}

fn allocate_text_budget(lengths: &[usize], budget: usize) -> Vec<usize> {
    let mut allocations = vec![0; lengths.len()];
    let mut remaining = budget;
    let mut active: Vec<usize> = (0..lengths.len()).collect();
    while !active.is_empty() {
        let share = remaining / active.len();
        let fixed: Vec<usize> = active
            .iter()
            .copied()
            .filter(|index| lengths[*index] <= share)
            .collect();
        if fixed.is_empty() {
            let remainder = remaining - share * active.len();
            for (position, index) in active.iter().enumerate() {
                allocations[*index] = share + usize::from(position < remainder);
            }
            break;
        }
        let fixed_set: BTreeSet<usize> = fixed.iter().copied().collect();
        for index in &fixed {
            allocations[*index] = lengths[*index];
            remaining = remaining.saturating_sub(lengths[*index]);
        }
        active.retain(|index| !fixed_set.contains(index));
    }
    allocations
}

fn replace_text_slots(
    entries: &mut [ToolResponseBudgetEntry],
    slots: &[TextSlot],
    allocations: &[usize],
) {
    for (slot, allocation) in slots.iter().zip(allocations) {
        let Some(text) = text_at(entries, slot).map(str::to_owned) else {
            continue;
        };
        if utf16_len(&text) <= *allocation {
            continue;
        }
        let entry_index = slot.entry_index;
        let files = entries[entry_index].persisted_output_files.clone();
        let protected_prefix = slot.protected_prefix_bytes.and_then(|prefix_bytes| {
            entries[entry_index].response_parts[slot.part_index]
                .pointer("/functionResponse/response/output")
                .and_then(Value::as_str)
                .and_then(|output| output.get(..prefix_bytes))
                .map(str::to_owned)
        });
        let replacement = fit_text(&text, *allocation, files.as_deref());
        let replacement = match protected_prefix {
            Some(prefix) => format!("{prefix}{replacement}"),
            None => replacement,
        };
        let part = &mut entries[entry_index].response_parts[slot.part_index];
        match slot.field {
            TextField::Text => {
                if let Some(object) = part.as_object_mut() {
                    object.insert("text".to_owned(), Value::String(replacement));
                }
            }
            TextField::Output | TextField::Error => {
                if let Some(response) = part
                    .pointer_mut("/functionResponse/response")
                    .and_then(Value::as_object_mut)
                {
                    let key = if slot.field == TextField::Output {
                        "output"
                    } else {
                        "error"
                    };
                    response.insert(key.to_owned(), Value::String(replacement));
                }
            }
        }
    }
}

fn fit_text(text: &str, max_chars: usize, persisted_output_files: Option<&[String]>) -> String {
    if utf16_len(text) <= max_chars {
        return text.to_owned();
    }
    if max_chars == 0 {
        return String::new();
    }
    let header = match persisted_output_files.filter(|files| !files.is_empty()) {
        Some([file]) => format!("Tool output truncated. Persisted tool-output artifact: {file}"),
        Some(files) => format!(
            "Tool output truncated. Persisted tool-output artifacts:\n{}",
            files
                .iter()
                .map(|file| format!("- {file}"))
                .collect::<Vec<_>>()
                .join("\n")
        ),
        None => "Tool output truncated.".to_owned(),
    };
    let header_length = utf16_len(&header);
    if header_length >= max_chars {
        return slice_start_without_broken_surrogate(&header, max_chars).to_owned();
    }
    let separator = "\n\n";
    let marker = "\n...\n";
    let preview_budget = max_chars.saturating_sub(header_length + utf16_len(separator));
    if preview_budget == 0 {
        return slice_start_without_broken_surrogate(&header, max_chars).to_owned();
    }
    if preview_budget <= utf16_len(marker) {
        return format!(
            "{header}{separator}{}",
            slice_start_without_broken_surrogate(text, preview_budget)
        );
    }
    let content_budget = preview_budget - utf16_len(marker);
    let head_budget = content_budget / 5;
    let tail_budget = content_budget - head_budget;
    format!(
        "{header}{separator}{}{marker}{}",
        slice_start_without_broken_surrogate(text, head_budget),
        slice_end_without_broken_surrogate(text, tail_budget)
    )
}

fn plan_mode_lifecycle_prefix(
    tool_name: Option<&str>,
    output: &str,
    reminders: &[String],
) -> Option<usize> {
    if tool_name != Some(ENTER_PLAN_MODE_TOOL) {
        return None;
    }
    reminders.iter().find_map(|reminder| {
        if output == reminder {
            Some(reminder.len())
        } else {
            let prefix = format!("{reminder}\n\n");
            output.starts_with(&prefix).then_some(prefix.len())
        }
    })
}

fn normalize_tool_result_call_id(call_id: &str) -> Option<String> {
    let basename = Path::new(call_id).file_name()?.to_str()?;
    let safe_id = basename.replace('\0', "_");
    (!safe_id.is_empty() && safe_id != "." && safe_id != "..").then_some(safe_id)
}

fn persistence_call_id(
    original: &str,
    normalized: Option<&str>,
    normalized_count: Option<&usize>,
    reserved: &BTreeSet<String>,
    used: &mut BTreeSet<String>,
) -> String {
    let Some(normalized) = normalized else {
        return original.to_owned();
    };
    let chosen = if normalized_count == Some(&1) && !used.contains(normalized) {
        normalized.to_owned()
    } else {
        let mut suffix = 1usize;
        loop {
            let candidate = format!("{normalized}-{suffix}");
            if !reserved.contains(&candidate) && !used.contains(&candidate) {
                break candidate;
            }
            suffix = suffix.saturating_add(1);
        }
    };
    used.insert(chosen.clone());
    chosen
}

fn utf16_len(text: &str) -> usize {
    text.encode_utf16().count()
}

fn slice_start_without_broken_surrogate(text: &str, length: usize) -> &str {
    &text[..byte_index_at_utf16(text, length, true)]
}

fn slice_end_without_broken_surrogate(text: &str, length: usize) -> &str {
    let start_units = utf16_len(text).saturating_sub(length);
    &text[byte_index_at_utf16(text, start_units, false)..]
}

fn byte_index_at_utf16(text: &str, target: usize, head: bool) -> usize {
    let mut units = 0usize;
    for (byte_index, character) in text.char_indices() {
        let next_units = units + character.len_utf16();
        if units == target {
            return byte_index;
        }
        if units < target && target < next_units {
            return if head {
                byte_index
            } else {
                byte_index + character.len_utf8()
            };
        }
        units = next_units;
    }
    text.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[derive(Default)]
    struct MemoryStore {
        writes: Vec<(String, String, String)>,
        fail: bool,
    }

    impl ToolOutputStore for MemoryStore {
        async fn persist(
            &mut self,
            call_id: &str,
            tool_name: &str,
            content: &str,
        ) -> Result<Option<String>, String> {
            if self.fail {
                return Err("disk unavailable".to_owned());
            }
            self.writes
                .push((call_id.to_owned(), tool_name.to_owned(), content.to_owned()));
            Ok(Some(format!("/tmp/{call_id}.txt")))
        }
    }

    fn entry(call_id: &str, output: &str) -> ToolResponseBudgetEntry {
        ToolResponseBudgetEntry {
            call_id: call_id.to_owned(),
            tool_name: "shell".to_owned(),
            response_parts: vec![json!({"functionResponse":{
                "id":call_id,"name":"shell","response":{"output":output}
            }})],
            persisted_output_files: None,
        }
    }

    #[test]
    fn counts_text_output_and_error_but_not_media() {
        let parts = vec![
            json!({"text":"abc"}),
            json!({"functionResponse":{"response":{"output":"de","error":"fg"}}}),
            json!({"inlineData":{"mimeType":"image/png","data":"hidden"}}),
        ];
        assert_eq!(tool_response_text_length(&parts), 7);
    }

    #[test]
    fn estimates_tool_payload_sizes_without_changing_serialized_byte_counts() {
        let output = ToolExecutionOutput {
            output: "raw output".to_owned(),
            parts: vec![json!({"text":"quoted \" text, newline\n, and 😀"})],
            display: Some(json!({"ansiOutput":[[{
                "text":"another \t string",
                "bold":true
            }]]})),
            artifacts: Vec::new(),
            result_file_paths: Vec::new(),
        };
        let expected = serde_json::to_string(&output.output).unwrap().len()
            + serde_json::to_vec(&output.parts[0]).unwrap().len()
            + serde_json::to_vec(output.display.as_ref().unwrap())
                .unwrap()
                .len();
        assert_eq!(output.estimated_bytes(), expected);
    }

    #[test]
    fn pure_budget_allocates_across_tool_responses_and_preserves_media() {
        let media = json!({"inlineData":{"mimeType":"image/png","data":"base64"}});
        let mut entries = vec![
            entry("one", &"a".repeat(100)),
            entry("two", &"b".repeat(100)),
        ];
        entries[1].response_parts.push(media.clone());
        assert!(enforce_function_response_budget(&mut entries, 40.0, &[]));
        assert!(
            tool_response_text_length(&entries[0].response_parts)
                + tool_response_text_length(&entries[1].response_parts)
                <= 40
        );
        assert_eq!(entries[1].response_parts[1], media);
    }

    #[tokio::test]
    async fn finalizer_persists_oversized_output_and_shows_artifact_path() {
        let mut entries = vec![entry("call-1", &"a".repeat(1000))];
        let mut store = MemoryStore::default();
        finalize_tool_responses(&mut entries, Some(100.0), &[], &mut store).await;
        assert_eq!(store.writes.len(), 1);
        assert_eq!(store.writes[0].0, "call-1");
        assert!(entries[0].persisted_output_files.is_some());
        assert!(tool_response_text_length(&entries[0].response_parts) <= 100);
        let output = entries[0].response_parts[0]
            .pointer("/functionResponse/response/output")
            .and_then(Value::as_str)
            .unwrap();
        assert!(output.contains("/tmp/call-1.txt"));
        assert!(output.contains("..."));
    }

    #[tokio::test]
    async fn duplicate_persistence_ids_do_not_overwrite_each_other() {
        let mut entries = vec![
            entry("same", &"a".repeat(1000)),
            entry("same", &"b".repeat(1000)),
        ];
        let mut store = MemoryStore::default();
        finalize_tool_responses(&mut entries, Some(100.0), &[], &mut store).await;
        assert_eq!(store.writes[0].0, "same-1");
        assert_eq!(store.writes[1].0, "same-2");
    }

    #[tokio::test]
    async fn plan_lifecycle_prefix_survives_while_hook_context_is_budgeted() {
        let reminder = "<system-reminder>plan mode active</system-reminder>".to_owned();
        let output = format!("{reminder}\n\n{}", "hook".repeat(100));
        let mut plan = entry("plan", &output);
        plan.tool_name = "enter_plan_mode".to_owned();
        plan.response_parts[0]["functionResponse"]["name"] = json!("enter_plan_mode");
        let mut entries = vec![plan];
        let mut store = MemoryStore::default();
        finalize_tool_responses(
            &mut entries,
            Some(40.0),
            std::slice::from_ref(&reminder),
            &mut store,
        )
        .await;
        let output = entries[0].response_parts[0]
            .pointer("/functionResponse/response/output")
            .and_then(Value::as_str)
            .unwrap();
        assert!(output.starts_with(&format!("{reminder}\n\n")));
        assert!(store.writes[0].2.starts_with("hook"));
    }

    #[tokio::test]
    async fn persistence_failure_still_enforces_the_model_visible_budget() {
        let mut entries = vec![entry("call-1", &"a".repeat(1000))];
        let mut store = MemoryStore {
            fail: true,
            ..MemoryStore::default()
        };
        finalize_tool_responses(&mut entries, Some(100.0), &[], &mut store).await;
        assert_eq!(entries[0].persisted_output_files, Some(Vec::new()));
        assert!(tool_response_text_length(&entries[0].response_parts) <= 100);
    }

    #[tokio::test]
    async fn file_store_writes_atomically_with_owner_only_permissions_and_budget() {
        let root = std::env::temp_dir().join(format!("canopy-tool-output-{}", Uuid::new_v4()));
        let mut store = FileToolOutputStore::with_session_budget(&root, 5);
        let output_path = store
            .persist("call-1", "shell", "hello")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            tokio::fs::read_to_string(&output_path).await.unwrap(),
            "hello"
        );
        assert_eq!(store.bytes_written(), 5);
        assert_eq!(
            store.persist("call-2", "shell", "too long").await.unwrap(),
            None
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let permissions = std::fs::metadata(&output_path).unwrap().permissions();
            assert_eq!(permissions.mode() & 0o777, 0o600);
        }
        tokio::fs::remove_dir_all(root).await.unwrap();
    }

    #[test]
    fn slicing_never_splits_an_astral_character() {
        let text = "a😀b";
        assert_eq!(slice_start_without_broken_surrogate(text, 2), "a");
        assert_eq!(slice_end_without_broken_surrogate(text, 2), "b");
    }
}
