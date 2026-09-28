//! Privacy-preserving tool-result boundary diagnostics port of
//! `packages/core/src/utils/tool-result-boundary-diagnostics.ts`.
//!
//! This module is provider-neutral and is not yet connected to the Rust
//! runtime's producer, finalizer, recorder, ACP, or headless boundaries. The
//! TypeScript source gets session and prompt identifiers from async-local
//! storage; Rust callers must inject [`ToolResultBoundaryContext`] or pass
//! identifiers explicitly. In particular, this module does not install an
//! async-local context provider.

use std::collections::{HashMap, HashSet};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use hmac::{Hmac, Mac};
use serde::Serialize;
use serde_json::Value;
use sha2::Sha256;
use uuid::Uuid;

use crate::tool_utils::canonical_tool_name;

pub const TOOL_RESULT_BOUNDARY_EVENT_NAME: &str = "canopy-code.tool_result.boundary";
pub const TOOL_RESULT_BOUNDARY_JSON_BYTE_THRESHOLD: usize = 65_536;
pub const TOOL_RESULT_BOUNDARY_LOG_LIMIT: usize = 50;
pub const TOOL_RESULT_BOUNDARY_LOG_WINDOW_MS: u64 = 60_000;

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolResultBoundaryStage {
    Producer,
    ProducerInput,
    ProducerOutput,
    FinalizerInput,
    FinalizerOutput,
    RecorderInput,
    RecorderOutput,
    AcpProjectionInput,
    AcpProjectionOutput,
    AcpWire,
    HeadlessProjectionInput,
    HeadlessProjectionOutput,
    HeadlessWire,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolResultRepresentation {
    ModelText,
    Display,
    AcpContent,
    AcpRawOutput,
    HeadlessContent,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToolResultBoundaryValue {
    pub representation: ToolResultRepresentation,
    pub value: String,
}

impl ToolResultBoundaryValue {
    pub fn new(representation: ToolResultRepresentation, value: impl Into<String>) -> Self {
        Self {
            representation,
            value: value.into(),
        }
    }
}

/// Artifact states and kinds accepted by the diagnostic schema.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolResultArtifactState {
    Undecided,
    None,
    Reusable,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolResultBoundaryArtifactKind {
    File,
    Link,
    Html,
    Image,
    Video,
    Audio,
    Pdf,
    Notebook,
    Other,
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ToolResultBoundaryArtifact {
    pub state: ToolResultArtifactState,
    pub kinds: Vec<ToolResultBoundaryArtifactKind>,
}

/// Untrusted kind metadata supplied by a tool artifact.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToolResultArtifactKindInput {
    pub kind: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct ToolResultBoundaryContext {
    pub session_id: Option<String>,
    pub prompt_id: Option<String>,
}

/// The observation's values can be produced lazily. A value closure and a
/// mutation closure are invoked at most once, and only after diagnostics are
/// enabled. A rate-limited mutated event also skips the values closure.
pub struct ToolResultBoundaryObservation<'a> {
    pub stage: ToolResultBoundaryStage,
    pub values: &'a (dyn Fn() -> Vec<ToolResultBoundaryValue> + Send + Sync),
    pub mutated: Option<&'a (dyn Fn() -> bool + Send + Sync)>,
    pub mutated_value: bool,
    pub artifacts: Option<&'a [ToolResultBoundaryArtifact]>,
    pub session_id: Option<&'a str>,
    pub prompt_id: Option<&'a str>,
    pub tool_call_id: Option<&'a str>,
    pub tool_call_ids: Option<&'a [String]>,
    pub tool_name: Option<&'a str>,
    pub wire_utf8_bytes: Option<usize>,
}

#[derive(Clone)]
pub struct ToolResultBoundaryObserverOptions {
    /// Diagnostics default to disabled. Callers should inject the actual
    /// debug-file and logger-enabled checks.
    pub enabled: Arc<dyn Fn() -> bool + Send + Sync>,
    pub log: Arc<dyn Fn(&str) + Send + Sync>,
    pub now_ms: Arc<dyn Fn() -> u64 + Send + Sync>,
    /// When absent, a process-local random key is generated on first emission.
    pub hmac_key: Option<Vec<u8>>,
    /// Optional lazy key provider, useful for a process-owned diagnostics key.
    pub hmac_key_provider: Option<Arc<dyn Fn() -> Option<Vec<u8>> + Send + Sync>>,
    pub context: Arc<dyn Fn() -> ToolResultBoundaryContext + Send + Sync>,
    pub threshold_bytes: usize,
    pub log_limit: usize,
    pub window_ms: u64,
}

impl Default for ToolResultBoundaryObserverOptions {
    fn default() -> Self {
        Self {
            enabled: Arc::new(|| false),
            log: Arc::new(|_| {}),
            now_ms: Arc::new(now_ms),
            hmac_key: None,
            hmac_key_provider: None,
            context: Arc::new(ToolResultBoundaryContext::default),
            threshold_bytes: TOOL_RESULT_BOUNDARY_JSON_BYTE_THRESHOLD,
            log_limit: TOOL_RESULT_BOUNDARY_LOG_LIMIT,
            window_ms: TOOL_RESULT_BOUNDARY_LOG_WINDOW_MS,
        }
    }
}

#[derive(Default)]
struct ObserverState {
    window_started_at: Option<u64>,
    emitted_in_window: usize,
    suppressed_count: usize,
}

pub struct ToolResultBoundaryObserver {
    options: ToolResultBoundaryObserverOptions,
    state: Mutex<ObserverState>,
    key: OnceLock<Vec<u8>>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ToolResultBoundaryValueSummary {
    representation: ToolResultRepresentation,
    slot: usize,
    code_units: usize,
    raw_utf8_bytes: usize,
    json_utf8_bytes: usize,
    hmac_sha256: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ToolResultBoundaryEvent {
    event_name: &'static str,
    stage: ToolResultBoundaryStage,
    mutated: bool,
    values: Vec<ToolResultBoundaryValueSummary>,
    #[serde(skip_serializing_if = "Option::is_none")]
    artifacts: Option<Vec<ToolResultBoundaryArtifact>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    session_hmac_sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    prompt_hmac_sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_call_hmac_sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_call_hmac_sha256s: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_name_hmac_sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    wire_utf8_bytes: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    suppressed_count: Option<usize>,
}

impl ToolResultBoundaryObserver {
    pub fn new(options: ToolResultBoundaryObserverOptions) -> Self {
        let key = OnceLock::new();
        if let Some(key_bytes) = options.hmac_key.clone() {
            let _ = key.set(key_bytes);
        }
        let observer = Self {
            options,
            state: Mutex::new(ObserverState::default()),
            key,
        };
        // Match the source's construction-time start timestamp. A panicking
        // injected clock should not make observer construction fatal.
        let initial_time = catch_unwind(AssertUnwindSafe(|| (observer.options.now_ms)()))
            .unwrap_or_else(|_| now_ms());
        if let Ok(mut state) = observer.state.lock() {
            state.window_started_at = Some(initial_time);
        }
        observer
    }

    /// Observe one boundary. Diagnostic failures, including panics from
    /// injected providers and log sinks, are contained and return `false`.
    pub fn observe(&self, observation: ToolResultBoundaryObservation<'_>) -> bool {
        catch_unwind(AssertUnwindSafe(|| self.observe_inner(observation))).unwrap_or(false)
    }

    fn observe_inner(&self, observation: ToolResultBoundaryObservation<'_>) -> bool {
        if !(self.options.enabled)() {
            return false;
        }

        let mutated = observation
            .mutated
            .map(|get| get())
            .unwrap_or(observation.mutated_value);
        let current_time = (self.options.now_ms)();
        {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            reset_window_if_needed(&mut state, current_time, self.options.window_ms);
            if mutated && state.emitted_in_window >= self.options.log_limit {
                state.suppressed_count = state.suppressed_count.saturating_add(1);
                return true;
            }
        }

        let values = (observation.values)();
        let oversized = !mutated
            && values.iter().any(|item| {
                json_string_exceeds_byte_length(&item.value, self.options.threshold_bytes)
            });
        if !mutated && !oversized {
            return false;
        }

        // Non-mutated observations must first inspect their values to decide
        // whether they cross the size threshold. Once an observation is
        // eligible, rate-limit it before calling context/key providers so a
        // capped diagnostic cannot fail on expensive or throwing providers.
        {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            reset_window_if_needed(&mut state, current_time, self.options.window_ms);
            if state.emitted_in_window >= self.options.log_limit {
                state.suppressed_count = state.suppressed_count.saturating_add(1);
                return true;
            }
        }

        let key = self.key.get_or_init(|| {
            (self
                .options
                .hmac_key_provider
                .as_ref()
                .and_then(|provider| provider()))
            .unwrap_or_else(random_process_key)
        });
        let context = if observation.session_id.is_none() || observation.prompt_id.is_none() {
            Some((self.options.context)())
        } else {
            None
        };
        let session_id = observation
            .session_id
            .or_else(|| context.as_ref().and_then(|ctx| ctx.session_id.as_deref()));
        let prompt_id = observation
            .prompt_id
            .or_else(|| context.as_ref().and_then(|ctx| ctx.prompt_id.as_deref()));

        let mut next_slots = HashMap::<ToolResultRepresentation, usize>::new();
        let mut measurements = HashMap::<&str, (usize, usize, usize)>::new();
        let mut value_hmacs = HashMap::<&str, String>::new();
        let mut summaries = Vec::with_capacity(values.len());
        for item in &values {
            let slot = next_slots.entry(item.representation).or_insert(0);
            let current_slot = *slot;
            *slot += 1;
            let (code_units, raw_utf8_bytes, json_utf8_bytes) =
                *measurements.entry(item.value.as_str()).or_insert_with(|| {
                    (
                        item.value.encode_utf16().count(),
                        item.value.len(),
                        json_string_byte_length(&item.value),
                    )
                });
            let hmac_sha256 = value_hmacs
                .entry(item.value.as_str())
                .or_insert_with(|| hmac_string(&item.value, key))
                .clone();
            summaries.push(ToolResultBoundaryValueSummary {
                representation: item.representation,
                slot: current_slot,
                code_units,
                raw_utf8_bytes,
                json_utf8_bytes,
                hmac_sha256,
            });
        }

        let artifacts = observation.artifacts.map(normalize_artifacts);
        let mut event = ToolResultBoundaryEvent {
            event_name: TOOL_RESULT_BOUNDARY_EVENT_NAME,
            stage: observation.stage,
            mutated,
            values: summaries,
            artifacts,
            session_hmac_sha256: session_id.map(|id| hmac_string(id, key)),
            prompt_hmac_sha256: prompt_id.map(|id| hmac_string(id, key)),
            tool_call_hmac_sha256: observation.tool_call_id.map(|id| hmac_string(id, key)),
            tool_call_hmac_sha256s: observation
                .tool_call_ids
                .map(|ids| ids.iter().map(|id| hmac_string(id, key)).collect()),
            tool_name_hmac_sha256: observation
                .tool_name
                .map(|name| hmac_string(&canonical_tool_name(name), key)),
            wire_utf8_bytes: observation.wire_utf8_bytes,
            suppressed_count: None,
        };

        // Only consume a log slot after all fallible providers and hashing
        // work succeeded. The source catches diagnostic failures before it
        // increments its counters; reserving here still bounds concurrent
        // Rust callers while preserving that behavior.
        let serialized = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            reset_window_if_needed(&mut state, current_time, self.options.window_ms);
            if state.emitted_in_window >= self.options.log_limit {
                state.suppressed_count = state.suppressed_count.saturating_add(1);
                return true;
            }

            state.emitted_in_window += 1;
            let suppressed_count = std::mem::take(&mut state.suppressed_count);
            event.suppressed_count = (suppressed_count > 0).then_some(suppressed_count);
            match serde_json::to_string(&event) {
                Ok(serialized) => serialized,
                Err(_) => {
                    state.emitted_in_window -= 1;
                    state.suppressed_count =
                        state.suppressed_count.saturating_add(suppressed_count);
                    return false;
                }
            }
        };
        (self.options.log)(&format!("{TOOL_RESULT_BOUNDARY_EVENT_NAME} {serialized}"));
        true
    }
}

fn reset_window_if_needed(state: &mut ObserverState, current_time: u64, window_ms: u64) {
    let should_reset = state.window_started_at.is_none_or(|started| {
        current_time < started || current_time.saturating_sub(started) >= window_ms
    });
    if should_reset {
        state.window_started_at = Some(current_time);
        state.emitted_in_window = 0;
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u64::MAX as u128) as u64)
        .unwrap_or(0)
}

fn random_process_key() -> Vec<u8> {
    // UUID v4 obtains cryptographic randomness through the existing uuid
    // dependency. Two UUIDs provide 244 random bits for this process-local key.
    let mut key = Vec::with_capacity(32);
    for _ in 0..2 {
        key.extend_from_slice(Uuid::new_v4().as_bytes());
    }
    key
}

fn hmac_string(value: &str, key: &[u8]) -> String {
    let code_units = value.encode_utf16().count();
    let byte_length = (code_units as u64).saturating_mul(2);
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts keys of any length");
    mac.update(&byte_length.to_be_bytes());
    let mut chunk = [0u8; 1024];
    let mut used = 0;
    for unit in value.encode_utf16() {
        let [lo, hi] = unit.to_le_bytes();
        chunk[used] = lo;
        chunk[used + 1] = hi;
        used += 2;
        if used == chunk.len() {
            mac.update(&chunk);
            used = 0;
        }
    }
    if used > 0 {
        mac.update(&chunk[..used]);
    }
    let digest = mac.finalize().into_bytes();
    let mut hex = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

/// JSON-string UTF-8 byte size, including the surrounding quote marks.
/// Rust strings cannot contain unpaired UTF-16 surrogates; all valid Unicode
/// scalar values otherwise match JavaScript's `JSON.stringify(string)` size.
pub fn json_string_byte_length(value: &str) -> usize {
    let mut bytes = 2usize;
    for ch in value.chars() {
        bytes = bytes.saturating_add(match ch {
            '"' | '\\' => 2,
            '\u{08}' | '\t' | '\n' | '\u{0c}' | '\r' => 2,
            c if (c as u32) <= 0x1f => 6,
            c if (c as u32) <= 0x7f => 1,
            c if (c as u32) <= 0x7ff => 2,
            c if (c as u32) <= 0xffff => 3,
            _ => 4,
        });
    }
    bytes
}

fn json_string_exceeds_byte_length(value: &str, threshold: usize) -> bool {
    let mut bytes = 2usize;
    if bytes > threshold {
        return true;
    }
    for ch in value.chars() {
        bytes = bytes.saturating_add(match ch {
            '"' | '\\' => 2,
            '\u{08}' | '\t' | '\n' | '\u{0c}' | '\r' => 2,
            c if (c as u32) <= 0x1f => 6,
            c if (c as u32) <= 0x7f => 1,
            c if (c as u32) <= 0x7ff => 2,
            c if (c as u32) <= 0xffff => 3,
            _ => 4,
        });
        if bytes > threshold {
            return true;
        }
    }
    false
}

fn normalize_kind(kind: &str) -> ToolResultBoundaryArtifactKind {
    match kind {
        "file" => ToolResultBoundaryArtifactKind::File,
        "link" => ToolResultBoundaryArtifactKind::Link,
        "html" => ToolResultBoundaryArtifactKind::Html,
        "image" => ToolResultBoundaryArtifactKind::Image,
        "video" => ToolResultBoundaryArtifactKind::Video,
        "audio" => ToolResultBoundaryArtifactKind::Audio,
        "pdf" => ToolResultBoundaryArtifactKind::Pdf,
        "notebook" => ToolResultBoundaryArtifactKind::Notebook,
        "other" => ToolResultBoundaryArtifactKind::Other,
        _ => ToolResultBoundaryArtifactKind::Unknown,
    }
}

pub fn tool_result_artifact_state(
    persisted_output_files: Option<&[String]>,
) -> ToolResultArtifactState {
    match persisted_output_files {
        None => ToolResultArtifactState::Undecided,
        Some([]) => ToolResultArtifactState::None,
        Some(_) => ToolResultArtifactState::Reusable,
    }
}

/// Build the safe artifact summary without retaining persisted paths or
/// unknown kind values. Kind order is preserved and duplicates are removed.
pub fn tool_result_boundary_artifact(
    persisted_output_files: Option<&[String]>,
    artifacts: Option<&[ToolResultArtifactKindInput]>,
) -> ToolResultBoundaryArtifact {
    let mut kinds = Vec::new();
    let mut seen = HashSet::new();
    if persisted_output_files.is_some_and(|files| !files.is_empty()) {
        push_unique_kind(&mut kinds, &mut seen, ToolResultBoundaryArtifactKind::File);
    }
    if let Some(artifacts) = artifacts {
        for artifact in artifacts {
            let kind = artifact
                .kind
                .as_deref()
                .map(normalize_kind)
                .unwrap_or(ToolResultBoundaryArtifactKind::Unknown);
            push_unique_kind(&mut kinds, &mut seen, kind);
        }
    }
    ToolResultBoundaryArtifact {
        state: tool_result_artifact_state(persisted_output_files),
        kinds,
    }
}

fn push_unique_kind(
    kinds: &mut Vec<ToolResultBoundaryArtifactKind>,
    seen: &mut HashSet<ToolResultBoundaryArtifactKind>,
    kind: ToolResultBoundaryArtifactKind,
) {
    if seen.insert(kind.clone()) {
        kinds.push(kind);
    }
}

/// Extract safe model-text diagnostic slots from a Google GenAI-like value.
/// `None` and JSON `null` match the source's empty-string fallback.
pub fn tool_result_part_diagnostic_values(value: Option<&Value>) -> Vec<ToolResultBoundaryValue> {
    let mut values = Vec::new();
    let Some(value) = value.filter(|value| !value.is_null()) else {
        return vec![ToolResultBoundaryValue::new(
            ToolResultRepresentation::ModelText,
            "",
        )];
    };
    let candidates: Vec<&Value> = value
        .as_array()
        .map(|parts| parts.iter().collect())
        .unwrap_or_else(|| vec![value]);
    for candidate in candidates {
        if let Some(text) = candidate.as_str() {
            values.push(ToolResultBoundaryValue::new(
                ToolResultRepresentation::ModelText,
                text,
            ));
            continue;
        }
        let Some(part) = candidate.as_object() else {
            continue;
        };
        if let Some(text) = part.get("text").and_then(Value::as_str) {
            values.push(ToolResultBoundaryValue::new(
                ToolResultRepresentation::ModelText,
                text,
            ));
        }
        if let Some(response) = part
            .get("functionResponse")
            .and_then(Value::as_object)
            .and_then(|function_response| function_response.get("response"))
            .and_then(Value::as_object)
        {
            for key in ["output", "error"] {
                if let Some(text) = response.get(key).and_then(Value::as_str) {
                    values.push(ToolResultBoundaryValue::new(
                        ToolResultRepresentation::ModelText,
                        text,
                    ));
                }
            }
        }
    }
    values
}

fn normalize_artifacts(
    artifacts: &[ToolResultBoundaryArtifact],
) -> Vec<ToolResultBoundaryArtifact> {
    artifacts
        .iter()
        .map(|artifact| {
            let mut seen = HashSet::new();
            let kinds = artifact
                .kinds
                .iter()
                .filter(|kind| seen.insert((**kind).clone()))
                .cloned()
                .collect();
            ToolResultBoundaryArtifact {
                state: artifact.state,
                kinds,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

    fn options(log: Arc<Mutex<Vec<String>>>) -> ToolResultBoundaryObserverOptions {
        let now = Arc::new(AtomicU64::new(0));
        ToolResultBoundaryObserverOptions {
            enabled: Arc::new(|| true),
            log: Arc::new(move |line| log.lock().unwrap().push(line.to_owned())),
            now_ms: Arc::new(move || now.load(Ordering::Relaxed)),
            hmac_key: Some(vec![7; 32]),
            threshold_bytes: 0,
            ..ToolResultBoundaryObserverOptions::default()
        }
    }

    fn observation<'a>(
        values: &'a (dyn Fn() -> Vec<ToolResultBoundaryValue> + Send + Sync),
    ) -> ToolResultBoundaryObservation<'a> {
        ToolResultBoundaryObservation {
            stage: ToolResultBoundaryStage::Producer,
            values,
            mutated: None,
            mutated_value: false,
            artifacts: None,
            session_id: None,
            prompt_id: None,
            tool_call_id: None,
            tool_call_ids: None,
            tool_name: None,
            wire_utf8_bytes: None,
        }
    }

    fn parse_event(line: &str) -> Value {
        let (_, json) = line.split_once(' ').unwrap();
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn json_byte_counts_match_json_string_encoding_for_unicode_and_controls() {
        for text in [
            "",
            "plain",
            "quote \" slash \\",
            "\0\x08\t\n\x0c\r\x1f",
            "é߿汉😀",
        ] {
            let encoded = serde_json::to_string(text).unwrap();
            assert_eq!(json_string_byte_length(text), encoded.len(), "{text:?}");
        }
        assert_eq!(json_string_byte_length(&"a".repeat(65_534)), 65_536);
        assert!(!json_string_exceeds_byte_length(
            &"a".repeat(65_534),
            65_536
        ));
        assert!(json_string_exceeds_byte_length(&"a".repeat(65_535), 65_536));
    }

    #[test]
    fn emits_only_safe_measurements_and_hmac_identifiers() {
        let lines = Arc::new(Mutex::new(Vec::new()));
        let observer = ToolResultBoundaryObserver::new(options(lines.clone()));
        let secret = "secret \"汉😀\" output".to_owned();
        let value = ToolResultBoundaryValue::new(ToolResultRepresentation::Display, secret.clone());
        let values = || vec![value.clone()];
        let session_id = "private-session";
        let prompt_id = "private-prompt";
        let tool_name = "task";
        let mut input = observation(&values);
        input.session_id = Some(session_id);
        input.prompt_id = Some(prompt_id);
        input.tool_name = Some(tool_name);
        assert!(observer.observe(input));

        let lines = lines.lock().unwrap();
        let line = &lines[0];
        assert!(!line.contains(&secret));
        assert!(!line.contains(session_id));
        assert!(!line.contains(prompt_id));
        let event = parse_event(line);
        let summary = &event["values"][0];
        assert_eq!(summary["representation"], "display");
        assert_eq!(summary["slot"], 0);
        assert_eq!(summary["codeUnits"], secret.encode_utf16().count());
        assert_eq!(summary["rawUtf8Bytes"], secret.len());
        assert_eq!(summary["jsonUtf8Bytes"], json_string_byte_length(&secret));
        assert_eq!(summary["hmacSha256"].as_str().unwrap().len(), 64);
        for field in [
            "sessionHmacSha256",
            "promptHmacSha256",
            "toolNameHmacSha256",
        ] {
            assert_eq!(event[field].as_str().unwrap().len(), 64);
        }
        assert_eq!(event["eventName"], TOOL_RESULT_BOUNDARY_EVENT_NAME);
    }

    #[test]
    fn hmac_matches_node_utf16_length_prefixed_hmac_shape_and_deduplicates_values() {
        let lines = Arc::new(Mutex::new(Vec::new()));
        let observer = ToolResultBoundaryObserver::new(options(lines.clone()));
        let same = "same value".to_owned();
        let values = || {
            vec![
                ToolResultBoundaryValue::new(ToolResultRepresentation::ModelText, same.clone()),
                ToolResultBoundaryValue::new(ToolResultRepresentation::Display, same.clone()),
            ]
        };
        assert!(observer.observe(observation(&values)));
        let event = parse_event(&lines.lock().unwrap()[0]);
        assert_eq!(
            event["values"][0]["hmacSha256"],
            event["values"][1]["hmacSha256"]
        );
        assert_eq!(
            event["values"][0]["hmacSha256"],
            "77cc10f9fba973ba64b7240855bd634f6f27c434deaf982f533e0474f20dccb5"
        );
        assert_eq!(event["values"][0]["slot"], 0);
        assert_eq!(event["values"][1]["slot"], 0);
        assert_eq!(hmac_string("", &[0; 32]).len(), 64);
    }

    #[test]
    fn lazily_avoids_value_and_mutation_work_when_disabled() {
        let calls = Arc::new(AtomicUsize::new(0));
        let opts = ToolResultBoundaryObserverOptions {
            enabled: Arc::new(|| false),
            ..ToolResultBoundaryObserverOptions::default()
        };
        let observer = ToolResultBoundaryObserver::new(opts);
        let value_calls = calls.clone();
        let values = move || {
            value_calls.fetch_add(1, Ordering::Relaxed);
            Vec::new()
        };
        let mutation_calls = calls.clone();
        let mutated = move || {
            mutation_calls.fetch_add(1, Ordering::Relaxed);
            true
        };
        let mut input = observation(&values);
        input.mutated = Some(&mutated);
        assert!(!observer.observe(input));
        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn applies_threshold_at_json_string_utf8_byte_boundary() {
        let lines = Arc::new(Mutex::new(Vec::new()));
        let mut opts = options(lines.clone());
        opts.threshold_bytes = TOOL_RESULT_BOUNDARY_JSON_BYTE_THRESHOLD;
        let observer = ToolResultBoundaryObserver::new(opts);
        let at_limit = "a".repeat(65_534);
        let values = || {
            vec![ToolResultBoundaryValue::new(
                ToolResultRepresentation::Display,
                at_limit.clone(),
            )]
        };
        assert!(!observer.observe(observation(&values)));
        let over_limit = "a".repeat(65_535);
        let values = || {
            vec![ToolResultBoundaryValue::new(
                ToolResultRepresentation::Display,
                over_limit.clone(),
            )]
        };
        assert!(observer.observe(observation(&values)));
        let event = parse_event(&lines.lock().unwrap()[0]);
        assert_eq!(event["values"][0]["jsonUtf8Bytes"], 65_537);
    }

    #[test]
    fn rate_limits_and_carries_suppression_count_to_next_window() {
        let lines = Arc::new(Mutex::new(Vec::new()));
        let now = Arc::new(AtomicU64::new(0));
        let mut opts = options(lines.clone());
        opts.now_ms = {
            let now = now.clone();
            Arc::new(move || now.load(Ordering::Relaxed))
        };
        opts.log_limit = 1;
        opts.window_ms = 100;
        let observer = ToolResultBoundaryObserver::new(opts);
        let values = || {
            vec![ToolResultBoundaryValue::new(
                ToolResultRepresentation::Display,
                "oversize",
            )]
        };
        assert!(observer.observe(observation(&values)));
        assert!(observer.observe(observation(&values)));
        assert!(observer.observe(observation(&values)));
        now.store(100, Ordering::Relaxed);
        assert!(observer.observe(observation(&values)));
        let lines = lines.lock().unwrap();
        assert_eq!(lines.len(), 2);
        assert_eq!(parse_event(&lines[1])["suppressedCount"], 2);
    }

    #[test]
    fn provider_failure_does_not_consume_log_quota_or_suppressed_count() {
        let lines = Arc::new(Mutex::new(Vec::new()));
        let now = Arc::new(AtomicU64::new(0));
        let context_calls = Arc::new(AtomicUsize::new(0));
        let mut opts = options(lines.clone());
        opts.now_ms = {
            let now = now.clone();
            Arc::new(move || now.load(Ordering::Relaxed))
        };
        opts.log_limit = 1;
        opts.window_ms = 100;
        opts.context = {
            let context_calls = context_calls.clone();
            Arc::new(move || {
                if context_calls.fetch_add(1, Ordering::Relaxed) == 0 {
                    panic!("context unavailable");
                }
                ToolResultBoundaryContext {
                    session_id: Some("context-session".to_owned()),
                    prompt_id: Some("context-prompt".to_owned()),
                }
            })
        };
        let observer = ToolResultBoundaryObserver::new(opts);
        let values = || {
            vec![ToolResultBoundaryValue::new(
                ToolResultRepresentation::Display,
                "eligible",
            )]
        };

        let mut first = observation(&values);
        first.session_id = Some("explicit-session");
        first.prompt_id = Some("explicit-prompt");
        assert!(observer.observe(first));
        assert!(observer.observe(observation(&values)));

        now.store(100, Ordering::Relaxed);
        assert!(!observer.observe(observation(&values)));
        assert!(observer.observe(observation(&values)));

        let lines = lines.lock().unwrap();
        assert_eq!(lines.len(), 2);
        let recovered_event = parse_event(&lines[1]);
        assert_eq!(recovered_event["suppressedCount"], 1);
        assert!(recovered_event["sessionHmacSha256"].is_string());
    }

    #[test]
    fn skips_values_for_mutated_events_after_rate_limit_and_handles_clock_reversal() {
        let lines = Arc::new(Mutex::new(Vec::new()));
        let now = Arc::new(AtomicU64::new(100));
        let mut opts = options(lines.clone());
        opts.now_ms = {
            let now = now.clone();
            Arc::new(move || now.load(Ordering::Relaxed))
        };
        opts.log_limit = 1;
        opts.window_ms = 100;
        let observer = ToolResultBoundaryObserver::new(opts);
        let calls = Arc::new(AtomicUsize::new(0));
        let call_count = calls.clone();
        let values = move || {
            call_count.fetch_add(1, Ordering::Relaxed);
            vec![ToolResultBoundaryValue::new(
                ToolResultRepresentation::Display,
                "x",
            )]
        };
        let mut first = observation(&values);
        first.mutated_value = true;
        assert!(observer.observe(first));
        let mut second = observation(&values);
        second.mutated_value = true;
        assert!(observer.observe(second));
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        now.store(50, Ordering::Relaxed);
        let mut third = observation(&values);
        third.mutated_value = true;
        assert!(observer.observe(third));
        assert_eq!(calls.load(Ordering::Relaxed), 2);
        assert_eq!(lines.lock().unwrap().len(), 2);
    }

    #[test]
    fn panic_safe_for_providers_values_and_logger() {
        let panic_lines = Arc::new(Mutex::new(Vec::<String>::new()));
        let mut opts = options(panic_lines.clone());
        opts.enabled = Arc::new(|| panic!("enable failed"));
        let observer = ToolResultBoundaryObserver::new(opts);
        let values = || {
            vec![ToolResultBoundaryValue::new(
                ToolResultRepresentation::Display,
                "x",
            )]
        };
        assert!(!observer.observe(observation(&values)));

        let mut opts = options(panic_lines);
        opts.log = Arc::new(|_| panic!("sink failed"));
        let observer = ToolResultBoundaryObserver::new(opts);
        let broken_values = || panic!("value extraction failed");
        assert!(!observer.observe(observation(&broken_values)));
        assert!(!observer.observe(observation(&values)));
    }

    #[test]
    fn normalizes_artifact_state_kinds_and_never_serializes_paths() {
        assert_eq!(
            tool_result_artifact_state(None),
            ToolResultArtifactState::Undecided
        );
        assert_eq!(
            tool_result_artifact_state(Some(&[])),
            ToolResultArtifactState::None
        );
        let files = vec!["/private/secret.txt".to_owned()];
        let artifact = tool_result_boundary_artifact(
            Some(&files),
            Some(&[
                ToolResultArtifactKindInput {
                    kind: Some("image".to_owned()),
                },
                ToolResultArtifactKindInput {
                    kind: Some("image".to_owned()),
                },
                ToolResultArtifactKindInput { kind: None },
                ToolResultArtifactKindInput {
                    kind: Some("/private/secret-kind".to_owned()),
                },
            ]),
        );
        assert_eq!(artifact.state, ToolResultArtifactState::Reusable);
        assert_eq!(
            artifact.kinds,
            vec![
                ToolResultBoundaryArtifactKind::File,
                ToolResultBoundaryArtifactKind::Image,
                ToolResultBoundaryArtifactKind::Unknown,
            ]
        );
        let serialized = serde_json::to_string(&artifact).unwrap();
        assert!(!serialized.contains("secret"));
    }

    #[test]
    fn supports_injected_context_and_part_value_extraction() {
        let lines = Arc::new(Mutex::new(Vec::new()));
        let mut opts = options(lines.clone());
        opts.context = Arc::new(|| ToolResultBoundaryContext {
            session_id: Some("context-session".to_owned()),
            prompt_id: Some("context-prompt".to_owned()),
        });
        let observer = ToolResultBoundaryObserver::new(opts);
        let values = || {
            vec![ToolResultBoundaryValue::new(
                ToolResultRepresentation::Display,
                "x",
            )]
        };
        assert!(observer.observe(observation(&values)));
        let event = parse_event(&lines.lock().unwrap()[0]);
        assert!(event["sessionHmacSha256"].is_string());
        assert!(event["promptHmacSha256"].is_string());

        let parts = json!([
            {"text":"top"},
            {"functionResponse":{"response":{"output":"out", "error":"err"}}},
            "plain"
        ]);
        let extracted = tool_result_part_diagnostic_values(Some(&parts));
        assert_eq!(
            extracted
                .iter()
                .map(|v| v.value.as_str())
                .collect::<Vec<_>>(),
            vec!["top", "out", "err", "plain"]
        );
        assert_eq!(tool_result_part_diagnostic_values(None)[0].value, "");
    }

    #[test]
    fn loads_injected_hmac_key_once_and_uses_it_for_later_events() {
        let lines = Arc::new(Mutex::new(Vec::new()));
        let mut opts = options(lines.clone());
        opts.hmac_key = None;
        let calls = Arc::new(AtomicUsize::new(0));
        opts.hmac_key_provider = {
            let calls = calls.clone();
            Some(Arc::new(move || {
                calls.fetch_add(1, Ordering::Relaxed);
                Some(vec![7; 32])
            }))
        };
        let observer = ToolResultBoundaryObserver::new(opts);
        let values = || {
            vec![ToolResultBoundaryValue::new(
                ToolResultRepresentation::Display,
                "x",
            )]
        };
        assert!(observer.observe(observation(&values)));
        assert!(observer.observe(observation(&values)));
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        let lines = lines.lock().unwrap();
        assert_eq!(
            parse_event(&lines[0])["values"][0]["hmacSha256"],
            parse_event(&lines[1])["values"][0]["hmacSha256"]
        );
    }
}
