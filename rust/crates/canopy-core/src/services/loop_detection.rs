//! Loop detection port of `packages/core/src/services/loopDetectionService.ts`.
//!
//! This is intentionally independent of telemetry transport: callers can
//! inspect [`LoopDetectionService::last_loop_type`] and emit the matching
//! telemetry event at their boundary. The event/state transitions and guard
//! thresholds follow the TypeScript service.
//!
//! `serde_json::Value` cannot represent JavaScript `undefined`, symbols,
//! functions, cyclic objects, or object identity; repeat-key parity therefore
//! covers JSON-compatible tool arguments (the service's normal input). The
//! caller supplies the already-resolved cap; `None` represents JavaScript
//! `Infinity` after the source config resolves a disabled cap.

use std::collections::HashMap;
use std::sync::OnceLock;

use regex::Regex;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::tool_utils::canonical_tool_name;
use crate::turn::{ThoughtSummary, ToolCallRequestInfo, TurnEvent};

const TOOL_CALL_LOOP_THRESHOLD: usize = 5;
const CONTENT_LOOP_THRESHOLD: usize = 10;
const CONTENT_CHUNK_SIZE: usize = 50;
const MAX_HISTORY_LENGTH: usize = 1000;
const THOUGHT_REPEAT_THRESHOLD: usize = 3;
const MAX_THOUGHT_HISTORY: usize = 50;
const FILE_READ_THRESHOLD: usize = 8;
const FILE_READ_WINDOW: usize = 15;
const STAGNATION_THRESHOLD: usize = 8;
const SHELL_COMMAND_STAGNATION_THRESHOLD: usize = STAGNATION_THRESHOLD;
pub const GLOBAL_DUPLICATE_THRESHOLD: usize = 6;
const ALTERNATING_PATTERN_CYCLES: usize = 3;
pub const DEFAULT_MAX_TOOL_CALLS_PER_TURN: usize = 100;
pub const UNBOUNDED_TOOL_CALLS_PER_TURN_BACKSTOP: usize = 512;
const ADAPTIVE_CAP_HARD_MULTIPLIER: usize = 10;

/// The loop type that fired most recently. String values match TypeScript's
/// telemetry enum.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum LoopType {
    ConsecutiveIdenticalToolCalls,
    ChantingIdenticalSentences,
    RepetitiveThoughts,
    ReadFileLoop,
    ActionStagnation,
    ShellCommandStagnation,
    GlobalToolCallDuplicate,
    AlternatingToolCallPattern,
    TurnToolCallCap,
    InvalidToolParamsStagnation,
    RepeatedToolExecutionFailure,
}

impl LoopType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ConsecutiveIdenticalToolCalls => "consecutive_identical_tool_calls",
            Self::ChantingIdenticalSentences => "chanting_identical_sentences",
            Self::RepetitiveThoughts => "repetitive_thoughts",
            Self::ReadFileLoop => "read_file_loop",
            Self::ActionStagnation => "action_stagnation",
            Self::ShellCommandStagnation => "shell_command_stagnation",
            Self::GlobalToolCallDuplicate => "global_tool_call_duplicate",
            Self::AlternatingToolCallPattern => "alternating_tool_call_pattern",
            Self::TurnToolCallCap => "turn_tool_call_cap",
            Self::InvalidToolParamsStagnation => "invalid_tool_params_stagnation",
            Self::RepeatedToolExecutionFailure => "repeated_tool_execution_failure",
        }
    }
}

/// Effective cap settings. `None` is the source's `Infinity`/disabled cap;
/// the unconditional 512-call backstop still applies. The default cap is
/// adaptive, while an explicit positive cap is hard.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LoopDetectionConfig {
    pub max_tool_calls_per_turn: Option<usize>,
    pub max_tool_calls_per_turn_explicit: bool,
    pub skip_loop_detection: bool,
}

impl Default for LoopDetectionConfig {
    fn default() -> Self {
        Self {
            max_tool_calls_per_turn: Some(DEFAULT_MAX_TOOL_CALLS_PER_TURN),
            max_tool_calls_per_turn_explicit: false,
            skip_loop_detection: false,
        }
    }
}

/// Halt predicate shared with the daemon turn-loop guard.
pub fn should_halt_on_turn_tool_call_cap(
    total_calls: usize,
    max_key_repeat: usize,
    cap: Option<usize>,
    is_explicit_cap: bool,
) -> bool {
    let Some(cap) = cap else {
        return false;
    };
    if total_calls <= cap {
        return false;
    }
    let hard_cap = cap.saturating_mul(ADAPTIVE_CAP_HARD_MULTIPLIER);
    let stuck = max_key_repeat >= GLOBAL_DUPLICATE_THRESHOLD;
    is_explicit_cap || total_calls > hard_cap || stuck
}

/// Stable identity for a tool call, using canonical aliases, recursively
/// sorted keys, preserved array order, and SHA-256 as in the TypeScript source.
pub fn get_tool_call_repeat_key(tool_name: &str, args: &Value) -> String {
    let mut canonical = String::new();
    write_js_json(args, &mut canonical);
    let key = format!("{}:{canonical}", canonical_tool_name(tool_name));
    let digest = Sha256::digest(key.as_bytes());
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

fn write_js_json(value: &Value, output: &mut String) {
    match value {
        Value::Null => output.push_str("null"),
        Value::Bool(value) => output.push_str(if *value { "true" } else { "false" }),
        Value::Number(number) => {
            // JSON.parse in JS represents all JSON numbers as IEEE-754 doubles;
            // using f64 here also reproduces its rounding for oversized ints.
            output.push_str(
                &number
                    .as_f64()
                    .map(js_number_string)
                    .unwrap_or_else(|| number.to_string()),
            );
        }
        Value::String(value) => {
            output.push_str(&serde_json::to_string(value).expect("string JSON encoding"))
        }
        Value::Array(values) => {
            output.push('[');
            for (index, item) in values.iter().enumerate() {
                if index != 0 {
                    output.push(',');
                }
                write_js_json(item, output);
            }
            output.push(']');
        }
        Value::Object(values) => {
            output.push('{');
            let mut keys: Vec<&String> = values
                .keys()
                .filter(|key| key.as_str() != "__proto__")
                .collect();
            keys.sort_by(
                |left, right| match (js_array_index(left), js_array_index(right)) {
                    (Some(left), Some(right)) => left.cmp(&right),
                    (Some(_), None) => std::cmp::Ordering::Less,
                    (None, Some(_)) => std::cmp::Ordering::Greater,
                    (None, None) => left.encode_utf16().cmp(right.encode_utf16()),
                },
            );
            for (index, key) in keys.into_iter().enumerate() {
                if index != 0 {
                    output.push(',');
                }
                output.push_str(&serde_json::to_string(key).expect("key JSON encoding"));
                output.push(':');
                write_js_json(&values[key], output);
            }
            output.push('}');
        }
    }
}

/// ECMAScript own-property array-index keys enumerate before ordinary keys,
/// even after the source sorts and assigns them to a fresh object.
fn js_array_index(key: &str) -> Option<u32> {
    if key.is_empty() || (key.len() > 1 && key.starts_with('0')) {
        return None;
    }
    let value = key.parse::<u32>().ok()?;
    (value != u32::MAX && value.to_string() == key).then_some(value)
}

/// Format an f64 with JavaScript's number-to-string exponent thresholds.
fn js_number_string(number: f64) -> String {
    if number == 0.0 {
        return "0".to_owned();
    }
    if !number.is_finite() {
        return "null".to_owned();
    }
    let raw = format!("{number:?}");
    let (negative, unsigned) = match raw.strip_prefix('-') {
        Some(unsigned) => (true, unsigned),
        None => (false, raw.as_str()),
    };
    let (mantissa, exponent) = raw
        .split_once('e')
        .or_else(|| raw.split_once('E'))
        .map(|(mantissa, exponent)| (mantissa.strip_prefix('-').unwrap_or(mantissa), exponent))
        .unwrap_or((unsigned, "0"));
    let exponent = exponent.parse::<i32>().unwrap_or(0);
    let decimal_position = mantissa.find('.').unwrap_or(mantissa.len()) as i32;
    let raw_digits = mantissa.replace('.', "");
    let leading_zeroes = raw_digits.bytes().take_while(|byte| *byte == b'0').count() as i32;
    let digits = raw_digits.trim_start_matches('0').trim_end_matches('0');
    if digits.is_empty() {
        return "0".to_owned();
    }
    let decimal_position = decimal_position + exponent - leading_zeroes;
    let scientific_exponent = decimal_position - 1;
    let sign = if negative { "-" } else { "" };
    if scientific_exponent >= 21 || scientific_exponent <= -7 {
        let coefficient = if digits.len() == 1 {
            digits.to_owned()
        } else {
            format!("{}.{}", &digits[..1], &digits[1..])
        };
        format!(
            "{sign}{coefficient}e{}{scientific_exponent}",
            if scientific_exponent >= 0 { "+" } else { "" }
        )
    } else if decimal_position <= 0 {
        format!(
            "{sign}0.{}{}",
            "0".repeat((-decimal_position) as usize),
            digits
        )
    } else if decimal_position as usize >= digits.len() {
        format!(
            "{sign}{digits}{}",
            "0".repeat(decimal_position as usize - digits.len())
        )
    } else {
        format!(
            "{sign}{}.{}",
            &digits[..decimal_position as usize],
            &digits[decimal_position as usize..]
        )
    }
}

#[derive(Clone, Debug)]
struct RecentToolCall {
    name: String,
}

/// Stateful port of Canopy's stream loop detector.
#[derive(Clone, Debug)]
pub struct LoopDetectionService {
    config: LoopDetectionConfig,
    prompt_id: String,
    last_tool_call_key: Option<String>,
    tool_call_repetition_count: usize,
    stream_content_history: Vec<u16>,
    content_stats: HashMap<String, Vec<usize>>,
    last_content_index: usize,
    loop_detected: bool,
    in_code_block: bool,
    disabled_for_session: bool,
    thought_history: Vec<String>,
    recent_tool_calls: Vec<RecentToolCall>,
    same_name_streak: usize,
    last_seen_tool_name: Option<String>,
    last_shell_inspection_key: Option<String>,
    shell_inspection_streak: usize,
    has_seen_non_read_tool: bool,
    global_tool_call_counts: HashMap<String, usize>,
    recent_tool_call_keys: Vec<String>,
    turn_tool_call_total: usize,
    turn_tool_call_total_committed: usize,
    cap_key_counts: HashMap<String, usize>,
    cap_max_key_repeat: usize,
    last_loop_type: Option<LoopType>,
}

impl LoopDetectionService {
    pub fn new(config: LoopDetectionConfig) -> Self {
        Self {
            config,
            prompt_id: String::new(),
            last_tool_call_key: None,
            tool_call_repetition_count: 0,
            stream_content_history: Vec::new(),
            content_stats: HashMap::new(),
            last_content_index: 0,
            loop_detected: false,
            in_code_block: false,
            disabled_for_session: false,
            thought_history: Vec::new(),
            recent_tool_calls: Vec::new(),
            same_name_streak: 0,
            last_seen_tool_name: None,
            last_shell_inspection_key: None,
            shell_inspection_streak: 0,
            has_seen_non_read_tool: false,
            global_tool_call_counts: HashMap::new(),
            recent_tool_call_keys: Vec::new(),
            turn_tool_call_total: 0,
            turn_tool_call_total_committed: 0,
            cap_key_counts: HashMap::new(),
            cap_max_key_repeat: 0,
            last_loop_type: None,
        }
    }

    pub fn last_loop_type(&self) -> Option<LoopType> {
        self.last_loop_type
    }

    pub fn consecutive_tool_call_count(&self) -> usize {
        self.tool_call_repetition_count
    }

    pub fn prompt_id(&self) -> &str {
        &self.prompt_id
    }

    pub fn disable_for_session(&mut self) {
        self.disabled_for_session = true;
    }

    /// Run always-on safeguards, then the heuristic tier, as the source's
    /// `addAndCheck` helper does.
    pub fn add_and_check(&mut self, event: &TurnEvent) -> bool {
        self.check_always_on_safeties(event) || self.add_and_check_heuristic_loops(event)
    }

    pub fn add_and_check_heuristic_loops(&mut self, event: &TurnEvent) -> bool {
        if self.loop_detected || self.disabled_for_session {
            return self.loop_detected;
        }
        match event {
            TurnEvent::ToolCallRequest { value } => {
                self.reset_content_tracking(true);
                self.thought_history.clear();
                self.track_tool_call(value);
                let key = self.key_for_call(value);
                let global_dup = self.check_global_duplicate(&key);
                let alternating = self.check_alternating_pattern(&key);
                let read_file_loop = self.check_read_file_loop();
                let action_stagnation = self.check_action_stagnation();
                self.loop_detected =
                    global_dup || alternating || read_file_loop || action_stagnation;
            }
            TurnEvent::Retry { .. } => {
                self.global_tool_call_counts.clear();
                self.recent_tool_call_keys.clear();
            }
            TurnEvent::Content { value, .. } => {
                self.loop_detected = self.check_content_loop(value);
            }
            TurnEvent::Thought { value } => {
                self.track_thought(value);
                self.loop_detected = self.check_repetitive_thoughts();
            }
            _ => {}
        }
        self.loop_detected
    }

    pub fn check_always_on_safeties(&mut self, event: &TurnEvent) -> bool {
        if self.loop_detected {
            return true;
        }
        match event {
            TurnEvent::Finished { .. } => {
                self.turn_tool_call_total_committed = self.turn_tool_call_total;
                return false;
            }
            TurnEvent::Retry { .. } => {
                self.turn_tool_call_total = self.turn_tool_call_total_committed;
                self.reset_tool_call_count();
                self.cap_key_counts.clear();
                self.cap_max_key_repeat = 0;
                return false;
            }
            TurnEvent::ToolCallRequest { value } => {
                if self.config.skip_loop_detection || self.disabled_for_session {
                    self.turn_tool_call_total = self.turn_tool_call_total.saturating_add(1);
                    if self.turn_tool_call_total > UNBOUNDED_TOOL_CALLS_PER_TURN_BACKSTOP {
                        self.last_loop_type = Some(LoopType::TurnToolCallCap);
                        return true;
                    }
                    return false;
                }
                let key = self.key_for_call(value);
                self.track_cap_key_repeat(&key);
                if self.check_tool_call_loop(&key) {
                    self.loop_detected = true;
                    return true;
                }
                if self.check_shell_command_stagnation(value) {
                    self.loop_detected = true;
                    return true;
                }
                if self.check_turn_tool_call_cap() {
                    self.loop_detected = true;
                    return true;
                }
            }
            _ => return false,
        }
        false
    }

    fn key_for_call(&self, call: &ToolCallRequestInfo) -> String {
        get_tool_call_repeat_key(&call.name, &call.args)
    }

    fn check_tool_call_loop(&mut self, key: &str) -> bool {
        if self.last_tool_call_key.as_deref() == Some(key) {
            self.tool_call_repetition_count += 1;
        } else {
            self.last_tool_call_key = Some(key.to_owned());
            self.tool_call_repetition_count = 1;
        }
        if self.tool_call_repetition_count >= TOOL_CALL_LOOP_THRESHOLD {
            self.last_loop_type = Some(LoopType::ConsecutiveIdenticalToolCalls);
            return true;
        }
        false
    }

    fn check_shell_command_stagnation(&mut self, call: &ToolCallRequestInfo) -> bool {
        let key = shell_inspection_key(call);
        let Some(key) = key else {
            self.last_shell_inspection_key = None;
            self.shell_inspection_streak = 0;
            return false;
        };
        if self.last_shell_inspection_key.as_deref() == Some(key) {
            self.shell_inspection_streak += 1;
        } else {
            self.last_shell_inspection_key = Some(key.to_owned());
            self.shell_inspection_streak = 1;
        }
        if self.shell_inspection_streak >= SHELL_COMMAND_STAGNATION_THRESHOLD {
            self.last_loop_type = Some(LoopType::ShellCommandStagnation);
            return true;
        }
        false
    }

    fn check_content_loop(&mut self, content: &str) -> bool {
        let fence_count = content.match_indices("```").count();
        let has_table = re(r"(^|\n)\s*(\|.*\||[|+-]{3,})").is_match(content);
        let has_list_item =
            re(r"(^|\n)\s*[-*+]\s").is_match(content) || re(r"(^|\n)\s*\d+\.\s").is_match(content);
        let has_heading = re(r"(^|\n)#+\s").is_match(content);
        let has_blockquote = re(r"(^|\n)>\s").is_match(content);
        let is_divider = is_divider(content);
        if fence_count > 0
            || has_table
            || has_list_item
            || has_heading
            || has_blockquote
            || is_divider
        {
            self.reset_content_tracking(true);
        }
        let was_in_code_block = self.in_code_block;
        if fence_count % 2 == 1 {
            self.in_code_block = !self.in_code_block;
        }
        if was_in_code_block || self.in_code_block || is_divider {
            return false;
        }
        self.stream_content_history.extend(content.encode_utf16());
        self.truncate_and_update();
        self.analyze_content_chunks_for_loop()
    }

    fn truncate_and_update(&mut self) {
        if self.stream_content_history.len() <= MAX_HISTORY_LENGTH {
            return;
        }
        let truncation = self.stream_content_history.len() - MAX_HISTORY_LENGTH;
        self.stream_content_history.drain(..truncation);
        self.last_content_index = self.last_content_index.saturating_sub(truncation);
        self.content_stats.retain(|_, indices| {
            indices.retain(|index| *index >= truncation);
            for index in indices.iter_mut() {
                *index -= truncation;
            }
            !indices.is_empty()
        });
    }

    fn analyze_content_chunks_for_loop(&mut self) -> bool {
        while self.last_content_index + CONTENT_CHUNK_SIZE <= self.stream_content_history.len() {
            let start = self.last_content_index;
            let chunk: Vec<u16> =
                self.stream_content_history[start..start + CONTENT_CHUNK_SIZE].to_vec();
            let digest = Sha256::digest(String::from_utf16_lossy(&chunk).as_bytes());
            let mut hash = String::with_capacity(digest.len() * 2);
            for byte in digest {
                use std::fmt::Write as _;
                let _ = write!(hash, "{byte:02x}");
            }
            let Some(indices) = self.content_stats.get_mut(&hash) else {
                self.content_stats.insert(hash, vec![start]);
                self.last_content_index += 1;
                continue;
            };
            if let Some(original_index) = indices.first().copied() {
                let original = &self.stream_content_history
                    [original_index..original_index + CONTENT_CHUNK_SIZE];
                if original == chunk.as_slice() {
                    indices.push(start);
                    if indices.len() >= CONTENT_LOOP_THRESHOLD {
                        let recent = &indices[indices.len() - CONTENT_LOOP_THRESHOLD..];
                        let distance = recent[recent.len() - 1] - recent[0];
                        let average = distance as f64 / (CONTENT_LOOP_THRESHOLD - 1) as f64;
                        if average <= CONTENT_CHUNK_SIZE as f64 * 1.5 {
                            self.last_loop_type = Some(LoopType::ChantingIdenticalSentences);
                            return true;
                        }
                    }
                }
            }
            self.last_content_index += 1;
        }
        false
    }

    fn track_thought(&mut self, summary: &ThoughtSummary) {
        let subject = summary.subject.trim().to_lowercase();
        let description = summary.description.trim().to_lowercase();
        let description: String =
            String::from_utf16_lossy(&description.encode_utf16().take(200).collect::<Vec<_>>());
        self.thought_history
            .push(format!("{subject}|{description}"));
        if self.thought_history.len() > MAX_THOUGHT_HISTORY {
            self.thought_history.remove(0);
        }
    }

    fn check_repetitive_thoughts(&mut self) -> bool {
        if self.thought_history.len() < THOUGHT_REPEAT_THRESHOLD {
            return false;
        }
        let recent = &self.thought_history[self.thought_history.len() - THOUGHT_REPEAT_THRESHOLD..];
        if recent.iter().all(|thought| thought == &recent[0]) {
            self.last_loop_type = Some(LoopType::RepetitiveThoughts);
            return true;
        }
        false
    }

    fn is_read_like_tool(name: &str) -> bool {
        matches!(
            name,
            "read_file" | "read_many_files" | "list_directory" | "zoom_image"
        ) || name.starts_with("read_")
            || name.starts_with("list_")
    }

    fn track_tool_call(&mut self, call: &ToolCallRequestInfo) {
        self.recent_tool_calls.push(RecentToolCall {
            name: call.name.clone(),
        });
        if self.recent_tool_calls.len() > FILE_READ_WINDOW {
            self.recent_tool_calls.remove(0);
        }
        if !self.has_seen_non_read_tool && !Self::is_read_like_tool(&call.name) {
            self.has_seen_non_read_tool = true;
        }
        if self.last_seen_tool_name.as_deref() == Some(&call.name) {
            self.same_name_streak += 1;
        } else {
            self.last_seen_tool_name = Some(call.name.clone());
            self.same_name_streak = 1;
        }
    }

    fn check_read_file_loop(&mut self) -> bool {
        if !self.has_seen_non_read_tool || self.recent_tool_calls.len() < FILE_READ_THRESHOLD {
            return false;
        }
        let count = self
            .recent_tool_calls
            .iter()
            .filter(|call| Self::is_read_like_tool(&call.name))
            .count();
        if count >= FILE_READ_THRESHOLD {
            self.last_loop_type = Some(LoopType::ReadFileLoop);
            return true;
        }
        false
    }

    fn check_action_stagnation(&mut self) -> bool {
        if self.same_name_streak >= STAGNATION_THRESHOLD {
            self.last_loop_type = Some(LoopType::ActionStagnation);
            return true;
        }
        false
    }

    fn track_cap_key_repeat(&mut self, key: &str) {
        let count = self.cap_key_counts.entry(key.to_owned()).or_default();
        *count += 1;
        self.cap_max_key_repeat = self.cap_max_key_repeat.max(*count);
    }

    fn check_turn_tool_call_cap(&mut self) -> bool {
        self.turn_tool_call_total = self.turn_tool_call_total.saturating_add(1);
        if should_halt_on_turn_tool_call_cap(
            self.turn_tool_call_total,
            self.cap_max_key_repeat,
            self.config.max_tool_calls_per_turn,
            self.config.max_tool_calls_per_turn_explicit,
        ) {
            self.last_loop_type = Some(LoopType::TurnToolCallCap);
            return true;
        }
        false
    }

    fn check_global_duplicate(&mut self, key: &str) -> bool {
        let count = self
            .global_tool_call_counts
            .entry(key.to_owned())
            .or_default();
        *count += 1;
        if *count >= GLOBAL_DUPLICATE_THRESHOLD {
            self.last_loop_type = Some(LoopType::GlobalToolCallDuplicate);
            return true;
        }
        false
    }

    fn check_alternating_pattern(&mut self, key: &str) -> bool {
        let max_len = 2 * ALTERNATING_PATTERN_CYCLES;
        self.recent_tool_call_keys.push(key.to_owned());
        if self.recent_tool_call_keys.len() > max_len {
            self.recent_tool_call_keys.remove(0);
        }
        if self.recent_tool_call_keys.len() < max_len {
            return false;
        }
        let a = &self.recent_tool_call_keys[0];
        let b = &self.recent_tool_call_keys[1];
        if a == b {
            return false;
        }
        for (index, item) in self.recent_tool_call_keys.iter().enumerate() {
            let expected = if index % 2 == 0 { a } else { b };
            if item != expected {
                return false;
            }
        }
        self.last_loop_type = Some(LoopType::AlternatingToolCallPattern);
        true
    }

    /// Reset all prompt-scoped state. The session disable flag is retained,
    /// matching the TypeScript implementation.
    pub fn reset(&mut self, prompt_id: impl Into<String>) {
        self.prompt_id = prompt_id.into();
        self.reset_tool_call_count();
        self.reset_content_tracking(true);
        self.loop_detected = false;
        self.thought_history.clear();
        self.recent_tool_calls.clear();
        self.same_name_streak = 0;
        self.last_seen_tool_name = None;
        self.has_seen_non_read_tool = false;
        self.last_loop_type = None;
        self.global_tool_call_counts.clear();
        self.recent_tool_call_keys.clear();
        self.turn_tool_call_total = 0;
        self.turn_tool_call_total_committed = 0;
        self.cap_key_counts.clear();
        self.cap_max_key_repeat = 0;
    }

    fn reset_tool_call_count(&mut self) {
        self.last_tool_call_key = None;
        self.tool_call_repetition_count = 0;
        self.last_shell_inspection_key = None;
        self.shell_inspection_streak = 0;
    }

    fn reset_content_tracking(&mut self, reset_history: bool) {
        if reset_history {
            self.stream_content_history.clear();
        }
        self.content_stats.clear();
        self.last_content_index = 0;
    }
}

fn re(pattern: &'static str) -> &'static Regex {
    static REGEXES: OnceLock<HashMap<&'static str, Regex>> = OnceLock::new();
    REGEXES
        .get_or_init(|| {
            [
                r"(^|\n)\s*(\|.*\||[|+-]{3,})",
                r"(^|\n)\s*[-*+]\s",
                r"(^|\n)\s*\d+\.\s",
                r"(^|\n)#+\s",
                r"(^|\n)>\s",
            ]
            .into_iter()
            .map(|pattern| (pattern, Regex::new(pattern).expect("valid content pattern")))
            .collect()
        })
        .get(pattern)
        .expect("known pattern")
}

fn is_divider(content: &str) -> bool {
    let candidate = content
        .strip_suffix('\n')
        .or_else(|| content.strip_suffix('\r'))
        .or_else(|| content.strip_suffix('\u{2028}'))
        .or_else(|| content.strip_suffix('\u{2029}'))
        .unwrap_or(content);
    !candidate.is_empty()
        && candidate.chars().all(|ch| {
            matches!(ch, '-' | '+' | '_' | '=' | '*') || ('\u{2500}'..='\u{257f}').contains(&ch)
        })
}

fn shell_inspection_key(call: &ToolCallRequestInfo) -> Option<&'static str> {
    if call.name != "run_shell_command" {
        return None;
    }
    let command = call.args.get("command")?.as_str()?;
    is_git_overview_inspection_command(command).then_some("run_shell_command:git-inspection")
}

fn is_git_overview_inspection_command(command: &str) -> bool {
    let segments: Vec<&str> = command
        .split([';', '&', '|', '\n'])
        .map(str::trim)
        .filter(|segment| !segment.is_empty())
        .collect();
    if segments.is_empty() {
        return false;
    }
    let git_command = git_command_regex();
    segments.iter().all(|segment| {
        let Some(captures) = git_command.captures(segment) else {
            return false;
        };
        let subcommand = captures.get(1).map(|value| value.as_str()).unwrap_or("");
        !subcommand.eq_ignore_ascii_case("diff")
            || is_overview_git_diff(&segment[captures.get(0).unwrap().end()..])
    })
}

fn git_command_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(r"(?i)^git(?:\s+(?:-C\s+\S+|--no-pager))*\s+(status|diff|ls-files)\b")
            .expect("valid git inspection regex")
    })
}

fn is_overview_git_diff(args: &str) -> bool {
    let trimmed = args.trim();
    if trimmed.is_empty() {
        return true;
    }
    let tokens: Vec<&str> = trimmed.split_whitespace().collect();
    if let Some(separator) = tokens.iter().position(|token| *token == "--") {
        if separator < tokens.len() - 1 {
            return false;
        }
    }
    tokens
        .iter()
        .all(|token| token.starts_with('-') || is_git_revision_token(token))
}

fn is_git_revision_token(token: &str) -> bool {
    token == "HEAD"
        || token == "@"
        || revision_expression_regex().is_match(token)
        || hex_revision_regex().is_match(token)
        || range_revision_regex().is_match(token)
}

fn revision_expression_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| Regex::new(r"^(?:HEAD|@)(?:[~^]\d*)+$").expect("valid revision regex"))
}

fn hex_revision_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| Regex::new(r"(?i)^[0-9a-f]{7,40}$").expect("valid hex regex"))
}

fn range_revision_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| Regex::new(r"^\S+\.{2,3}\S+$").expect("valid range regex"))
}

#[cfg(test)]
fn make_tool_call(name: &str, args: Value) -> TurnEvent {
    TurnEvent::ToolCallRequest {
        value: ToolCallRequestInfo {
            call_id: "test-id".to_owned(),
            provider_call_id: None,
            name: name.to_owned(),
            args,
            is_client_initiated: false,
            prompt_id: "test-prompt-id".to_owned(),
            response_id: None,
            was_output_truncated: None,
            goal_context: None,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn service() -> LoopDetectionService {
        LoopDetectionService::new(LoopDetectionConfig::default())
    }

    fn args(value: u64) -> Value {
        serde_json::json!({ "value": value })
    }

    #[test]
    fn repeat_key_sorts_nested_keys_and_canonicalizes_legacy_aliases() {
        let left = serde_json::json!({"outer":{"a":1,"b":[2,3]}});
        let right = serde_json::json!({"outer":{"b":[2,3],"a":1}});
        assert_eq!(
            get_tool_call_repeat_key("task", &left),
            get_tool_call_repeat_key("agent", &right)
        );
        assert_ne!(
            get_tool_call_repeat_key("agent", &serde_json::json!([1, 2])),
            get_tool_call_repeat_key("agent", &serde_json::json!([2, 1]))
        );
    }

    #[test]
    fn repeat_key_uses_javascript_number_spelling() {
        assert_eq!(js_number_string(-0.0), "0");
        assert_eq!(js_number_string(1.0), "1");
        assert_eq!(js_number_string(1e21), "1e+21");
        assert_eq!(js_number_string(1e-6), "0.000001");
        assert_eq!(js_number_string(1e-7), "1e-7");
    }

    #[test]
    fn canonical_json_uses_javascript_integer_key_enumeration() {
        let value: Value = serde_json::from_str(r#"{"2":"two","10":"ten","a":1}"#).unwrap();
        let mut encoded = String::new();
        write_js_json(&value, &mut encoded);
        assert_eq!(encoded, r#"{"2":"two","10":"ten","a":1}"#);

        let value: Value = serde_json::from_str(r#"{"01":1,"2":2,"10":10,"__proto__":3}"#).unwrap();
        let mut encoded = String::new();
        write_js_json(&value, &mut encoded);
        assert_eq!(encoded, r#"{"2":2,"10":10,"01":1}"#);
    }

    #[test]
    fn consecutive_guard_fires_on_fifth_call_and_retry_clears_streak() {
        let mut detector = service();
        let call = make_tool_call("custom", args(1));
        for _ in 0..4 {
            assert!(!detector.check_always_on_safeties(&call));
        }
        assert_eq!(detector.consecutive_tool_call_count(), 4);
        assert!(!detector.check_always_on_safeties(&TurnEvent::Retry {
            retry_info: None,
            is_continuation: None,
        }));
        for _ in 0..4 {
            assert!(!detector.check_always_on_safeties(&call));
        }
        assert!(detector.check_always_on_safeties(&call));
        assert_eq!(
            detector.last_loop_type(),
            Some(LoopType::ConsecutiveIdenticalToolCalls)
        );
    }

    #[test]
    fn consecutive_guard_treats_reordered_args_as_identical() {
        let mut detector = service();
        for index in 0..5 {
            let json = if index % 2 == 0 {
                r#"{"a":1,"b":2}"#
            } else {
                r#"{"b":2,"a":1}"#
            };
            let call = make_tool_call("custom", serde_json::from_str(json).unwrap());
            if detector.check_always_on_safeties(&call) {
                assert_eq!(
                    detector.last_loop_type(),
                    Some(LoopType::ConsecutiveIdenticalToolCalls)
                );
                return;
            }
        }
        panic!("expected fifth identical call to halt");
    }

    #[test]
    fn session_disable_suppresses_semantic_guard_but_keeps_backstop() {
        let mut detector = service();
        detector.disable_for_session();
        let call = make_tool_call("custom", args(1));
        for _ in 0..UNBOUNDED_TOOL_CALLS_PER_TURN_BACKSTOP {
            assert!(!detector.check_always_on_safeties(&call));
        }
        assert!(detector.check_always_on_safeties(&call));
        assert_eq!(detector.last_loop_type(), Some(LoopType::TurnToolCallCap));
    }

    #[test]
    fn explicit_cap_is_hard_while_default_cap_is_adaptive() {
        let mut hard = LoopDetectionService::new(LoopDetectionConfig {
            max_tool_calls_per_turn: Some(2),
            max_tool_calls_per_turn_explicit: true,
            skip_loop_detection: false,
        });
        for index in 0..2 {
            assert!(!hard.check_always_on_safeties(&make_tool_call("t", args(index))));
        }
        assert!(hard.check_always_on_safeties(&make_tool_call("t", args(3))));
        assert_eq!(hard.last_loop_type(), Some(LoopType::TurnToolCallCap));

        let mut productive = LoopDetectionService::new(LoopDetectionConfig {
            max_tool_calls_per_turn: Some(2),
            max_tool_calls_per_turn_explicit: false,
            skip_loop_detection: false,
        });
        for index in 0..20 {
            assert!(!productive.check_always_on_safeties(&make_tool_call("t", args(index))));
        }
    }

    #[test]
    fn adaptive_cap_halts_on_repetition_or_only_after_the_hard_backstop() {
        assert!(!should_halt_on_turn_tool_call_cap(100, 6, Some(100), false));
        assert!(!should_halt_on_turn_tool_call_cap(101, 5, Some(100), false));
        assert!(should_halt_on_turn_tool_call_cap(101, 6, Some(100), false));
        assert!(!should_halt_on_turn_tool_call_cap(
            1000,
            1,
            Some(100),
            false
        ));
        assert!(should_halt_on_turn_tool_call_cap(1001, 1, Some(100), false));

        let mut detector = LoopDetectionService::new(LoopDetectionConfig {
            max_tool_calls_per_turn: Some(10),
            max_tool_calls_per_turn_explicit: false,
            skip_loop_detection: false,
        });
        let repeated = make_tool_call("repeat", serde_json::json!({}));
        let others: Vec<TurnEvent> = (1..=5)
            .map(|index| make_tool_call(&format!("other_{index}"), serde_json::json!({})))
            .collect();
        let mut last = false;
        for index in 0..11 {
            let event = if index % 2 == 0 {
                &repeated
            } else {
                // Unique names keep the consecutive guard from replacing the
                // adaptive soft-cap's stuck-repetition signal.
                &others[index / 2]
            };
            last = detector.check_always_on_safeties(event);
            if last {
                break;
            }
        }
        assert!(last);
        assert_eq!(detector.last_loop_type(), Some(LoopType::TurnToolCallCap));
    }

    #[test]
    fn retry_rolls_cap_total_back_to_last_finished_round_trip() {
        let mut detector = LoopDetectionService::new(LoopDetectionConfig {
            max_tool_calls_per_turn: Some(2),
            max_tool_calls_per_turn_explicit: true,
            skip_loop_detection: false,
        });
        assert!(!detector.check_always_on_safeties(&make_tool_call("t", args(1))));
        assert!(!detector.check_always_on_safeties(&TurnEvent::Finished {
            value: crate::turn::FinishedEventValue {
                reason: Some("STOP".to_owned()),
                usage_metadata: None,
            },
        }));
        assert!(!detector.check_always_on_safeties(&make_tool_call("t", args(2))));
        assert!(!detector.check_always_on_safeties(&TurnEvent::Retry {
            retry_info: None,
            is_continuation: None,
        }));
        assert!(!detector.check_always_on_safeties(&make_tool_call("t", args(3))));
        assert!(detector.check_always_on_safeties(&make_tool_call("t", args(4))));
        assert_eq!(detector.last_loop_type(), Some(LoopType::TurnToolCallCap));
    }

    #[test]
    fn alternating_calls_fire_after_three_ab_cycles() {
        let mut detector = service();
        let a = make_tool_call("a", serde_json::json!({}));
        let b = make_tool_call("b", serde_json::json!({}));
        for index in 0..5 {
            let event = if index % 2 == 0 { &a } else { &b };
            assert!(!detector.add_and_check_heuristic_loops(event));
        }
        assert!(detector.add_and_check_heuristic_loops(&b));
        assert_eq!(
            detector.last_loop_type(),
            Some(LoopType::AlternatingToolCallPattern)
        );
    }

    #[test]
    fn global_duplicate_guard_counts_nonconsecutive_calls() {
        let mut detector = service();
        let repeated = make_tool_call("target", serde_json::json!({"same": true}));
        for index in 0..GLOBAL_DUPLICATE_THRESHOLD {
            if index > 0 {
                let interleaved = make_tool_call(
                    &format!("other_{index}"),
                    serde_json::json!({"index": index}),
                );
                assert!(!detector.add_and_check_heuristic_loops(&interleaved));
            }
            let detected = detector.add_and_check_heuristic_loops(&repeated);
            assert_eq!(detected, index + 1 == GLOBAL_DUPLICATE_THRESHOLD);
        }
        assert_eq!(
            detector.last_loop_type(),
            Some(LoopType::GlobalToolCallDuplicate)
        );
    }

    #[test]
    fn action_stagnation_counts_same_tool_even_when_arguments_change() {
        let mut detector = service();
        for index in 0..STAGNATION_THRESHOLD {
            let call = make_tool_call("update", serde_json::json!({"index": index}));
            assert_eq!(
                detector.add_and_check_heuristic_loops(&call),
                index + 1 == STAGNATION_THRESHOLD
            );
        }
        assert_eq!(detector.last_loop_type(), Some(LoopType::ActionStagnation));
    }

    #[test]
    fn thoughts_require_three_contiguous_matching_signatures() {
        let mut detector = service();
        let thought = |subject: &str| TurnEvent::Thought {
            value: ThoughtSummary {
                subject: subject.to_owned(),
                description: "DETAIL".to_owned(),
            },
        };
        assert!(!detector.add_and_check_heuristic_loops(&thought("other")));
        assert!(!detector.add_and_check_heuristic_loops(&thought(" Step ")));
        assert!(!detector.add_and_check_heuristic_loops(&thought("step")));
        assert!(detector.add_and_check_heuristic_loops(&thought(" STEP ")));
        assert_eq!(
            detector.last_loop_type(),
            Some(LoopType::RepetitiveThoughts)
        );
    }

    #[test]
    fn repeated_content_fires_but_markdown_list_and_code_do_not_accumulate() {
        let mut detector = service();
        let repeated = TurnEvent::Content {
            value: "x".repeat(500),
            parts: None,
        };
        assert!(detector.add_and_check_heuristic_loops(&repeated));
        assert_eq!(
            detector.last_loop_type(),
            Some(LoopType::ChantingIdenticalSentences)
        );

        let mut markdown = service();
        let mut sentence = String::new();
        while sentence.len() < CONTENT_CHUNK_SIZE {
            sentence.push_str("This is a unique sentence, id=1. ");
        }
        sentence.truncate(CONTENT_CHUNK_SIZE);
        for _ in 0..CONTENT_LOOP_THRESHOLD * 2 {
            let list = TurnEvent::Content {
                value: format!("- {sentence}\n"),
                parts: None,
            };
            assert!(!markdown.add_and_check_heuristic_loops(&list));
        }
        let mut code = service();
        for _ in 0..12 {
            let opening = TurnEvent::Content {
                value: "```\n".to_owned(),
                parts: None,
            };
            let body = TurnEvent::Content {
                value: "x".repeat(80),
                parts: None,
            };
            let closing = TurnEvent::Content {
                value: "\n```".to_owned(),
                parts: None,
            };
            assert!(!code.add_and_check_heuristic_loops(&opening));
            assert!(!code.add_and_check_heuristic_loops(&body));
            assert!(!code.add_and_check_heuristic_loops(&closing));
        }
    }

    #[test]
    fn shell_stagnation_collapses_overview_variants_and_fails_open_for_paths() {
        let commands = [
            "git status --short",
            "git status --short && git diff --stat",
            "git diff --name-only HEAD",
            "git status --porcelain=v1",
            "git diff --stat HEAD",
            "git -C . status --short",
            "git --no-pager diff --stat",
            "git ls-files --modified",
        ];
        let mut detector = service();
        for command in &commands[..7] {
            let call = make_tool_call("run_shell_command", serde_json::json!({"command": command}));
            assert!(!detector.check_always_on_safeties(&call));
        }
        let last = make_tool_call(
            "run_shell_command",
            serde_json::json!({"command": commands[7]}),
        );
        assert!(detector.check_always_on_safeties(&last));
        assert_eq!(
            detector.last_loop_type(),
            Some(LoopType::ShellCommandStagnation)
        );

        let mut path_diff = service();
        for index in 0..8 {
            let call = make_tool_call(
                "run_shell_command",
                serde_json::json!({"command": format!("git diff -- src/file_{index}.rs")}),
            );
            assert!(!path_diff.check_always_on_safeties(&call));
        }
    }

    #[test]
    fn shell_stagnation_streak_resets_on_noninspection_tool_and_retry() {
        let overview = |index: usize| {
            make_tool_call(
                "run_shell_command",
                serde_json::json!({
                    "command": "git status --short",
                    "description": format!("inspection {index}")
                }),
            )
        };
        let mut detector = service();
        for index in 0..4 {
            assert!(!detector.check_always_on_safeties(&overview(index)));
        }
        assert!(!detector.check_always_on_safeties(&make_tool_call(
            "run_shell_command",
            serde_json::json!({"command": "cargo test"}),
        )));
        for index in 4..11 {
            assert!(!detector.check_always_on_safeties(&overview(index)));
        }
        assert!(detector.check_always_on_safeties(&overview(11)));
        assert_eq!(
            detector.last_loop_type(),
            Some(LoopType::ShellCommandStagnation)
        );

        let mut retry = service();
        for index in 0..4 {
            assert!(!retry.check_always_on_safeties(&overview(index)));
        }
        assert!(!retry.check_always_on_safeties(&TurnEvent::Retry {
            retry_info: None,
            is_continuation: None,
        }));
        for index in 4..11 {
            assert!(!retry.check_always_on_safeties(&overview(index)));
        }
        assert!(retry.check_always_on_safeties(&overview(11)));
    }

    #[test]
    fn read_only_cold_start_is_exempt_but_read_churn_after_progress_fires() {
        let mut cold = service();
        for index in 0..18 {
            let name = if index % 2 == 0 {
                "read_file"
            } else {
                "list_directory"
            };
            assert!(!cold.add_and_check_heuristic_loops(&make_tool_call(name, args(index))));
        }
        let mut after_progress = service();
        assert!(
            !after_progress.add_and_check_heuristic_loops(&make_tool_call("write_file", args(999)))
        );
        for index in 0..7 {
            let name = if index % 2 == 0 {
                "read_file"
            } else {
                "list_directory"
            };
            assert!(
                !after_progress.add_and_check_heuristic_loops(&make_tool_call(name, args(index)))
            );
        }
        assert!(
            after_progress
                .add_and_check_heuristic_loops(&make_tool_call("list_directory", args(7)))
        );
        assert_eq!(
            after_progress.last_loop_type(),
            Some(LoopType::ReadFileLoop)
        );
    }

    #[test]
    fn reset_clears_prompt_state_but_not_session_disable() {
        let mut detector = service();
        for _ in 0..5 {
            if detector.check_always_on_safeties(&make_tool_call("t", serde_json::json!({}))) {
                break;
            }
        }
        detector.reset("next-prompt");
        assert_eq!(detector.last_loop_type(), None);
        assert_eq!(detector.consecutive_tool_call_count(), 0);
        detector.disable_for_session();
        detector.reset("third-prompt");
        let call = make_tool_call("t", serde_json::json!({}));
        for _ in 0..6 {
            assert!(!detector.check_always_on_safeties(&call));
        }
    }

    #[test]
    fn git_inspection_classifier_handles_paths_and_chains() {
        assert!(is_git_overview_inspection_command(
            "git status --short && git diff --stat"
        ));
        assert!(is_git_overview_inspection_command("git diff HEAD~2..HEAD"));
        assert!(!is_git_overview_inspection_command(
            "git diff -- src/main.rs"
        ));
        assert!(!is_git_overview_inspection_command(
            "git status; cargo test"
        ));
        assert!(!is_git_overview_inspection_command(""));
    }
}
